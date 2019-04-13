from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging
import math
import sys
import time
import types

import ray
import ray.experimental.signal as signal
from ray.experimental.streaming.benchmarks.macro.nexmark.event import Record
from ray.experimental.streaming.benchmarks.macro.nexmark.event import Watermark

logger = logging.getLogger(__name__)
logger.setLevel("INFO")

#
# Each Ray actor corresponds to an operator instance in the physical dataflow
# Actors communicate using batched queues as data channels (no standing TCP
# connections)
# Currently, batched queues are based on Eric's implementation (see:
# batched_queue.py)

def _identity(element):
    return element


# Signal denoting that a streaming actor finished processing
# and returned after all its input channels have been closed
class ActorExit(signal.Signal):
    def __init__(self, value=None):
        self.value = value

# Signal denoting that a streaming actor started spinning
class ActorStart(signal.Signal):
    def __init__(self, value=None):
        self.value = value

class OperatorInstance(object):
    """A streaming operator instance.

    Attributes:
        instance_id (UUID): The id of the instance.
        input (DataInput): The input gate that manages input channels of
        the instance (see: DataInput in communication.py).
        input (DataOutput): The output gate that manages output channels of
        the instance (see: DataOutput in communication.py).
        config (EnvironmentConfig): The environment's configuration.
    """
    # TODO (john): Keep all operator metadata and environment's configuration
    # for debugging purposes
    def __init__(self,
                 instance_id,
                 operator_metadata,
                 input_gate,
                 output_gate,
                 config=None):
        self.metadata = operator_metadata
        self.key_index = None       # Index for key selection
        self.key_attribute = None   # Attribute name for key selection
        self.instance_id = instance_id
        self.input = input_gate
        self.output = output_gate
        self.this_actor = None  # A handle to self

        # Parameters for periodic rescheduling in queue-based execution
        if not config:  # Actor will spin continuously until termination
            self.records_limit = float("inf")   # Unlimited
            self.scheduling_timeout = None      # No timeout
        else:  # Actor will be rescheduled periodically
            self.records_limit = config.scheduling_period_in_records
            self.scheduling_timeout = config.scheduling_timeout
        self.records_processed = 0
        self.previous_scheduling_time = time.time()

        # Logging-related attributes
        self.logging = self.metadata.logging
        if self.logging:
            self.input.enable_logging()
            self.output.enable_logging()

        # Enable writes to all output channels
        for channel in self.output.forward_channels:
            channel.queue.enable_writes()
        for channels in self.output.shuffle_channels:
            for channel in channels:
                channel.queue.enable_writes()
        for channels in self.output.shuffle_key_channels:
            for channel in channels:
                channel.queue.enable_writes()
        for channels in self.output.round_robin_channels:
            for channel in channels:
                channel.queue.enable_writes()
        for channels in self.output.custom_partitioning_channels:
            for channel in channels:
                channel.queue.enable_writes()

    # Used for index-based key extraction, e.g. for tuples
    def _index_based_selector(self,record):
        return record[self.key_index]

    # Used for attribute-based key extraction, e.g. for classes
    def _attribute_based_selector(self,record):
        return vars(record)[self.key_attribute]

    # Used to register own handle so that the actor can schedule itself
    def _register_handle(self, actor_handle):
        self.this_actor = actor_handle

    # Used to register the handle of a destination actor to a channel
    def _register_destination_handle(self, actor_handle, channel_id):
        for channel in self.output.forward_channels:
            if channel.id == channel_id:
                channel.register_destination_actor(actor_handle)
                return
        for channels in self.output.shuffle_channels:
            for channel in channels:
                if channel.id == channel_id:
                    channel.register_destination_actor(actor_handle)
                    return
        for channels in self.output.shuffle_key_channels:
            for channel in channels:
                if channel.id == channel_id:
                    channel.register_destination_actor(actor_handle)
                    return
        for channels in self.output.round_robin_channels:
            for channel in channels:
                if channel.id == channel_id:
                    channel.register_destination_actor(actor_handle)
                    return
        for channels in self.output.custom_partitioning_channels:
            for channel in channels:
                if channel.id == channel_id:
                    channel.register_destination_actor(actor_handle)
                    return

    # Used to periodically stop spinning and reschedule an actor
    # in order to call other methods on it from the outside world
    # Returns True if the actor should stop spinning, False otherwise
    def _reschedule(self):
        reschedule = False
        # Check if number of processed records reached the limit
        if self.records_processed >= self.records_limit:
            reschedule = True
        # Check if there is a timeout
        if self.scheduling_timeout:
            elapsed_time = time.time() - self.previous_scheduling_time
            if elapsed_time >= self.scheduling_timeout:
                reschedule = True
        if reschedule:
            self.this_actor.start.remote()  # Submit task to scheduler
            self.previous_scheduling_time = time.time()
            self.records_processed = 0
        return reschedule

    # Returns the logged rates (if any)
    def logs(self):
        return (self.instance_id, self.input.rates,
                self.output.rates)

    # Starts the spinning actor (implemented by the subclasses)
    def start(self):  # Used in queue-based execution
        pass

    # Applies the actor's logic once (implemented by the subclasses)
    def apply(self):  # Used in task-based execution
        pass


# A monitoring actor used to keep track of the execution's progress
@ray.remote
class ProgressMonitor(object):
    """A monitoring actor used to track the progress of
    the dataflow execution.

    Attributes:
        running_actors list(actor handles): A list of handles to all actors
        executing the physical dataflow.
    """

    def __init__(self,running_actors):
        self.running_actors = running_actors  # Actor handles
        self.start_signals = []
        self.exit_signals = []
        logger.debug("Running actors: {}".format(self.running_actors))

    # Returns when the dataflow execution is over
    def all_exit_signals(self):
        while True:
            # Block until the ActorExit signal has been
            # received from each actor executing the dataflow
            signals = signal.receive(self.running_actors)
            for _, received_signal in signals:
                if isinstance(received_signal, ActorExit):
                    self.exit_signals.append(received_signal)
            if len(self.exit_signals) == len(self.running_actors):
                return

    # Returns after receiving all ActorStart signals for a list of actors
    def start_signals(self, actor_handles):
        while True:
            # Block until the ActorStart signal has been
            # received from each actor in the given list
            signals = signal.receive(actor_handles)
            for _, received_signal in signals:
                if isinstance(received_signal, ActorStart):
                    self.start_signals.append(received_signal)
            if len(self.start_signals) == len(actor_handles):
                return

# A source actor that reads a text file line by line
@ray.remote
class ReadTextFile(OperatorInstance):
    """A source operator instance that reads a text file line by line.

    Attributes:
        filepath (string): The path to the input file.
    """

    def __init__(self,
                 instance_id,
                 operator_metadata,
                 input_gate,
                 output_gate):
        OperatorInstance.__init__(self, instance_id,
                                  operator_metadata, input_gate, output_gate)
        self.filepath = operator_metadata.filepath
        # TODO (john): Handle possible exception here
        self.reader = open(self.filepath, "r")

    # Read input file line by line
    def start(self):
        while True:
            record = self.reader.readline()
            # Reader returns empty string ('') on EOF,
            # so a 'record is None' condition doesn't work here
            if not record:
                # Flush any remaining records to plasma and close the file
                self.output._flush(close=True)
                self.reader.close()
                signal.send(ActorExit(self.instance_id))
                return
            # Push after removing newline characters
            self.output._push(record[:-1])


# Map actor
@ray.remote
class Map(OperatorInstance):
    """A map operator instance that applies a user-defined
    stream transformation.

    A map produces exactly one output record for each record in
    the input stream.

    Attributes:
        map_fn (function): The user-defined function.
    """

    def __init__(self, instance_id, operator_metadata, input_gate,
                 output_gate):
        OperatorInstance.__init__(self, instance_id,
                                  operator_metadata, input_gate, output_gate)
        self.map_fn = operator_metadata.logic

    # Applies the map to each record of the input stream(s)
    # and pushes resulting records to the output stream(s)
    def start(self):
        signal.send(ActorStart(self.instance_id))
        while True:
            record = self.input._pull()
            if record is None:
                self.output._flush(close=True)
                signal.send(ActorExit(self.instance_id))
                return
            self.output._push(self.map_fn(record))

    # Task-based map execution on a set of batches
    def apply(self, batches, channel_id, _source_operator_id):
        records = 0
        for batch in batches:
            for record in batch:
                if record is None:
                    if self.input._close_channel(channel_id):
                        self.output._flush(close=True)
                        signal.send(ActorExit(self.instance_id))
                        records += len(batch)
                    return records
                self.output._push(self.map_fn(record))
            records += len(batch)
        return records


# Flatmap actor
@ray.remote
class FlatMap(OperatorInstance):
    """A map operator instance that applies a user-defined
    stream transformation.

    A flatmap produces one or more output records for each record in
    the input stream.

    Attributes:
        flatmap_fn (function): The user-defined function.
    """

    def __init__(self, instance_id, operator_metadata, input_gate,
                 output_gate):
        OperatorInstance.__init__(self, instance_id,
                                  operator_metadata, input_gate, output_gate)
        self.flatmap_fn = operator_metadata.logic

    # Applies the flatmap logic to the records of the input stream(s)
    # and pushes resulting records to the output stream(s)
    def start(self):
        signal.send(ActorStart(self.instance_id))
        while True:
            record = self.input._pull()
            if record is None:
                self.output._flush(close=True)
                signal.send(ActorExit(self.instance_id))
                return
            self.output._push_all(self.flatmap_fn(record))

    # Task-based flatmap execution on a set of batches
    def apply(self, batches, channel_id, _source_operator_id):
        records = 0
        for batch in batches:
            for record in batch:
                if record is None:
                    if self.input._close_channel(channel_id):
                        self.output._flush(close=True)
                        signal.send(ActorExit(self.instance_id))
                        records += len(batch)
                    return records
                self.output._push_all(self.flatmap_fn(record))
            records += len(batch)
        return records

# Filter actor
@ray.remote
class Filter(OperatorInstance):
    """A filter operator instance that applies a user-defined filter to
    each record of the stream.

    Output records are those that pass the filter, i.e. those for which
    the filter function returns True.

    Attributes:
        filter_fn (function): The user-defined boolean function.
    """

    def __init__(self, instance_id, operator_metadata, input_gate,
                 output_gate):
        OperatorInstance.__init__(self, instance_id,
                                  operator_metadata, input_gate, output_gate)
        self.filter_fn = operator_metadata.logic

    # Applies the filter to the records of the input stream(s)
    # and pushes resulting records to the output stream(s)
    def start(self):
        while True:
            record = self.input._pull()
            if record is None:  # Close channel and return
                self.output._flush(close=True)
                signal.send(ActorExit(self.instance_id))
                return
            if self.filter_fn(record):
                self.output._push(record)

    # Task-based filter execution on a set of batches
    def apply(self, batches, channel_id, _source_operator_id):
        records = 0
        for batch in batches:
            for record in batch:
                if record is None:
                    if self.input._close_channel(channel_id):
                        self.output._flush(close=True)
                        signal.send(ActorExit(self.instance_id))
                        records += len(batch)
                    return records
                if self.filter_fn(record):
                    self.output._push(record)
            records += len(batch)
        return records

# Union actor
@ray.remote
class Union(OperatorInstance):
    """A union operator instance that concatenates two or more streams."""

    def __init__(self, instance_id, operator_metadata, input_gate,
                 output_gate):
        OperatorInstance.__init__(self, instance_id,
                                  operator_metadata, input_gate, output_gate)

    # Repeatedly moves records from input to output
    def start(self):
        while True:
            record = self.input._pull()
            if record is None:  # Close channel and return
                self.output._flush(close=True)
                signal.send(ActorExit(self.instance_id))
                return
            self.output._push(record)

    # Task-based union execution on a set of batches
    def apply(self, batches, channel_id, _source_operator_id):
        records = 0
        for batch in batches:
            for record in batch:
                if record is None:
                    if self.input._close_channel(channel_id):
                        self.output._flush(close=True)
                        signal.send(ActorExit(self.instance_id))
                        records += len(batch)
                    return records
                self.output._push(record)
            records += len(batch)
        return records

# Join actor
@ray.remote
class Join(OperatorInstance):
    """A join operator instance that joins two streams."""

    def __init__(self, instance_id, operator_metadata, input_gate,
                 output_gate):
        OperatorInstance.__init__(self, instance_id,
                                  operator_metadata, input_gate, output_gate)
        self.join_logic = operator_metadata.logic
        assert(self.join_logic is not None), (self.join_logic)
        self.left = operator_metadata.left_input_operator_id
        self.right = operator_metadata.right_input_operator_id
        self.process_logic = None

    # Repeatedly pulls and joins records from both inputs
    def start(self):
        sys.exit("Queue-based join is not supported yet.")

    # Task-based join execution on a set of batches
    def apply(self, batches, channel_id, source_operator_id):
        # Distringuish between left and right input and set the right logic
        # We expect this to be as cheap as setting a pointer
        if source_operator_id == self.left:
            self.process_logic = self.join_logic.process_left
        else:
            assert source_operator_id == self.right
            self.process_logic = self.join_logic.process_right
        records = 0
        for batch in batches:
            for record in batch:
                if record is None:
                    if self.input._close_channel(channel_id):
                        self.output._flush(close=True)
                        signal.send(ActorExit(self.instance_id))
                        records += len(batch)
                    return records
                if isinstance(record, Watermark):  # It is a watermark
                    self.output._push(record)
                else:
                    self.output._push_all(self.process_logic(record))
            records += len(batch)
        return records

# Event-time window actor
@ray.remote
class EventTimeWindow(OperatorInstance):
    """An event time window operator instance (tumbling or sliding)."""

    def __init__(self, instance_id, operator_metadata, input_gate,
                 output_gate):
        OperatorInstance.__init__(self, instance_id,
                                  operator_metadata, input_gate, output_gate)
        self.window_length_ms =operator_metadata.window_length_ms
        self.slide_ms = operator_metadata.slide_ms
        aggregator = operator_metadata.aggregation_logic
        self.aggregator = aggregator  # The user-defined aggregator
        # Assignment of records to windows is done based on the type of state
        self.__assign = self.__assigner_2 if aggregator else self.__assigner_1
        # Local state is organized beased on whether the user has given an
        # aggregation function or not. In the former case, state is a list
        # of widnow ids, each one associated with a list of tuples. This is
        # allows for fast pre-aggregation. In the latter case, state is a list
        # of tuples, each one associated with a list of window ids. This is
        # better as we avoid replicating (possibly large) records to windows
        self.state = []
        # An (optional) offset that serves as the min window (bucket) id
        self.offset = operator_metadata.offset
        # Range of window ids (used to find the windows a records belongs to)
        self.range = math.trunc(math.ceil(
                                self.window_length_ms / self.slide_ms))
        # Register channel_ids to the output in order to forward watermarks
        for channel in self.input.input_channels:
            # Keep the last seen watermark from each input channel
            self.output.input_channels[channel.id] = 0

    # Collects windows that have expired
    def __collect_expired_windows(self, watermark):
        result = []
        event_time = watermark.event_time
        min_open_window = (event_time // self.slide_ms) - self.range + 1
        # logger.info("Min window: {}".format(min_open_window))
        if self.aggregator is not None:  # window id -> state
            indexes = []
            for i, (window, state) in enumerate(self.state):
                if window < min_open_window: # Window has expired
                    record = Record(content=(window, state),
                                    system_time=watermark.system_time)
                    result.append(record)
                    indexes.append(i)
            for window_index in indexes:  # Update state
                self.state.pop(window_index)
        else:  # record -> window ids
            for i, (record, windows) in enumerate(self.state):
                indexes = []
                for j, window in enumerate(windows):
                    if window < min_open_window:  # Window has expired
                        r = Record(content=(window, record),
                                   system_time=watermark.system_time)
                        result.append(r)
                        indexes.append(j)
                for window_index in indexes:  # Update state
                    self.state[i][1].pop(window_index)
        return result

    # Finds the windows a record belongs to
    def __find_windows(self, record):
        windows = []
        event_time = record.dateTime
        slot = -1
        slot = event_time // self.slide_ms
        window_end = (slot * self.slide_ms) + self.window_length_ms
        # logger.info("Window end: {}".format(window_end))
        if event_time > window_end:  # Can happen when slide is bigger
            return windows           # than the window
        min = slot - self.range + 1
        # TODO (john): Check if this is the correct semantics for offset
        min_window_id = min if min >= self.offset else self.offset
        max_window_id = slot if slot >= self.offset else self.offset
        windows = [i for i in range(min_window_id, max_window_id + 1)]
        return windows

    # Updates the list of windows a record belongs to
    def __assigner_1(self, record):
        # This type of state is not a good fit for efficient pre-aggregation
        assert self.aggregator is None, (self.aggregator)
        windows = self.__find_windows(record)
        if len(windows) > 0:  # Handle special case where some records do not
            # fall in any window because the slide is larger than the window
            self.state.append((record, windows))

    # Replicates the result of the aggregation to all
    # windows (buckets) affected by the input record
    def __assigner_2(self, record):
        assert self.aggregator is not None, (self.aggregator)
        windows = self.__find_windows(record)
        # logger.info("Windows for input record with timestamp {}: {}".format(
        #             record.dateTime, windows))
        for window in windows:
            slot, state = next(((w, s) for w, s in self.state
                               if w == window), (None, None))
            if slot is None:  # New window
                init_state = [self.aggregator.initialize(record)]
                # logger.info("Init state for window {} is {}".format(window,
                #             init_state))
                self.state.append((window, init_state))
            else:  # Apply pre-aggregation and update state
                self.aggregator.update(state, record)

    # Repeatedly pulls and joins records from both inputs
    def start(self):
        sys.exit("Queue-based event time window is not supported yet.")

    # Task-based window execution on a set of batches
    def apply(self, batches, channel_id, _source_operator_id):
        records = 0
        for batch in batches:
            for record in batch:
                if record is None:
                    if self.input._close_channel(channel_id):
                        self.output._flush(close=True)
                        signal.send(ActorExit(self.instance_id))
                        records += len(batch)
                    return records
                if isinstance(record, Watermark):  # It is a watermark
                    # Fire windows that have expired
                    windows = self.__collect_expired_windows(record)
                    # for win in windows:
                    #     logger.info("Firing windows {} on {}".format(
                    #             win.content[0], record.event_time))
                    self.output._push_all(windows)  # Push records
                    self.output._push(record)       # Propagate watermark
                else:  # It is a data record
                    self.__assign(record)
            records += len(batch)
        return records

# Inspect actor
@ray.remote
class Inspect(OperatorInstance):
    """A inspect operator instance that inspects the content of the stream.

    Inspect is useful for printing the records in the stream.

    Attributes:
         inspect_fn (function): The user-defined inspect logic.
    """

    def __init__(self, instance_id, operator_metadata, input_gate,
                 output_gate):
        OperatorInstance.__init__(self, instance_id,
                                   operator_metadata, input_gate, output_gate)
        self.inspect_fn = operator_metadata.logic

    # Applies the inspect logic (e.g. print) to the records of
    # the input stream(s) and leaves stream unaffected by simply
    # pushing the records to the output stream(s)
    def start(self):
        while True:
            record = self.input._pull()
            if record is None:
                self.output._flush(close=True)
                signal.send(ActorExit(self.instance_id))
                return
            self.inspect_fn(record)
            self.output._push(record)

    # Task-based inspect execution on a set of batches
    def apply(self, batches, channel_id, _source_operator_id):
        records = 0
        for batch in batches:
            for record in batch:
                if record is None:
                    if self.input._close_channel(channel_id):
                        self.output._flush(close=True)
                        signal.send(ActorExit(self.instance_id))
                        records += len(batch)
                    return records
                self.inspect_fn(record)
                self.output._push(record)
            records += len(batch)
        return records

# Reduce actor
@ray.remote
class Reduce(OperatorInstance):
    """A reduce operator instance that combines a new value for a key
    with the last reduced one according to a user-defined logic.

    Attributes:
        reduce_fn (function): The user-defined reduce logic.
        value_attribute (int): The index of the value to reduce
        (assuming tuple records).
        state (dict): A mapping from keys to values.
    """

    def __init__(self, instance_id, operator_metadata, input_gate,
                 output_gate, config=None):
        OperatorInstance.__init__(self, instance_id,
                                  operator_metadata, input_gate,
                                  output_gate, config)
        self.state = {}                             # key -> value
        self.reduce_fn = operator_metadata.logic    # Reduce function
        # Set the attribute selector
        self.attribute_selector = operator_metadata.attribute_selector
        if self.attribute_selector is None:
            self.attribute_selector = _identity
        elif isinstance(self.attribute_selector, int):
            self.key_index = self.attribute_selector
            self.attribute_selector = self._index_based_selector
        elif isinstance(self.attribute_selector, str):
            self.key_attribute = self.attribute_selector
            self.attribute_selector = self._attribute_based_selector
        elif not isinstance(self.attribute_selector, types.FunctionType):
            sys.exit("Unrecognized or unsupported key selector.")

    # Combines the input value for a key with the last reduced
    # value for that key to produce a new value.
    # Outputs the result as a tuple of the form (key,new value)
    def start(self):
        while True:
            if self._reschedule():
                # Stop spinning so that other methods can be
                # called on this actor from the outside world
                return
            record = self.input._pull()
            if record is None:
                self.output._flush(close=True)
                signal.send(ActorExit(self.instance_id))
                return
            key, rest = record
            new_value = self.attribute_selector(rest)
            # TODO (john): Is there a way to update state with
            # a single dictionary lookup?
            try:
                old_value = self.state[key]
                new_value = self.reduce_fn(old_value, new_value)
                self.state[key] = new_value
            except KeyError:  # Key does not exist in state
                self.state.setdefault(key, new_value)
            self.output._push((key, new_value))
            self.records_processed += 1

    # Returns the local state of the actor
    def state(self):
        return self.state

    # Task-based reduce execution on a set of batches
    def apply(self, batches, channel_id, _source_operator_id):
        records = 0
        for batch in batches:
            for record in batch:
                if record is None:
                    if self.input._close_channel(channel_id):
                        self.output._flush(close=True)
                        signal.send(ActorExit(self.instance_id))
                        records += len(batch)
                    return records
                key, rest = record
                new_value = self.attribute_selector(rest)
                # TODO (john): Is there a way to update state with
                # a single dictionary lookup?
                try:
                    old_value = self.state[key]
                    new_value = self.reduce_fn(old_value, new_value)
                    self.state[key] = new_value
                except KeyError:  # Key does not exist in state
                    self.state.setdefault(key, new_value)
                self.output._push((key, new_value))
            records += len(batch)
        return records


@ray.remote
class KeyBy(OperatorInstance):
    """A key_by operator instance that physically partitions the
    stream based on a key.

    Attributes:
        key_attribute (int): The index of the value to reduce
        (assuming tuple records).
    """

    def __init__(self, instance_id, operator_metadata, input_gate,
                 output_gate):
        OperatorInstance.__init__(self, instance_id,
                                  operator_metadata, input_gate, output_gate)
        # Set the key selector
        self.key_selector = operator_metadata.key_selector
        if isinstance(self.key_selector, int):
            self.key_index = self.key_selector
            self.key_selector = self._index_based_selector
        elif isinstance(self.key_selector, str):
            self.key_attribute = self.key_selector
            self.key_selector = self._attribute_based_selector
        elif not isinstance(self.key_selector, types.FunctionType):
            sys.exit("Unrecognized or unsupported key selector.")

    # The actual stream partitioning is done by the output gate
    def start(self):
        while True:
            record = self.input._pull()
            if record is None:
                self.output._flush(close=True)
                signal.send(ActorExit(self.instance_id))
                return
            key = self.key_selector(record)
            self.output._push((key,record))

    # Task-based keyby execution on a set of batches
    def apply(self, batches, channel_id, _source_operator_id):
        records = 0
        for batch in batches:
            for record in batch:
                if record is None:
                    if self.input._close_channel(channel_id):
                        self.output._flush(close=True)
                        signal.send(ActorExit(self.instance_id))
                        records += len(batch)
                    return records
                key = self.key_selector(record)
                self.output._push((key,record))
            records += len(batch)
        return records

# A custom source actor
@ray.remote
class Source(OperatorInstance):
    def __init__(self, instance_id, operator_metadata, input_gate,
                 output_gate):
        OperatorInstance.__init__(self, instance_id,
                                  operator_metadata, input_gate, output_gate)
        # The user-defined source with a get_next() method
        _, local_instance_id = instance_id
        source_objects = operator_metadata.sources
        self.source = source_objects[local_instance_id] if isinstance(
                            source_objects, list) else source_objects
        self.source.init()
        self.watermark_interval = operator_metadata.watermark_interval
        self.max_event_time = 0


    def __watermark(self, record):
        event_time = record.dateTime  # TODO (john): Make this general
        max_timestamp = max(event_time, self.max_event_time)
        if (max_timestamp >=
                self.max_event_time + self.watermark_interval):
            # Emit watermark
            # logger.info("Source emitting watermark {} due to {}".format(
            #             self.max_event_time, max_timestamp))
            self.output._push(Watermark(self.max_event_time, time.time()))
            self.max_event_time = max_timestamp

    # Starts the source by calling get_next() repeatedly
    def start(self):
        signal.send(ActorStart(self.instance_id))
        while True:
            record = self.source.get_next()
            if record is None:
                self.output._flush(close=True)
                signal.send(ActorExit(self.instance_id))
                return
            self.output._push(record)
            if self.watermark_interval > 0:
                # Check if watermark should be emitted
                self.__watermark(record)


# A custom sink actor
@ray.remote
class Sink(OperatorInstance):
    def __init__(self, instance_id, operator_metadata, input_gate,
                 output_gate):
        OperatorInstance.__init__(self, instance_id,
                                  operator_metadata, input_gate, output_gate)
        # The user-defined sink with an evict() method
        self.sink = operator_metadata.sink

    # Starts the sink by calling process() repeatedly
    def start(self):
        signal.send(ActorStart(self.instance_id))
        while True:
            record = self.input._pull()
            if record is None:
                self.sink.close()
                signal.send(ActorExit(self.instance_id))
                return
            self.sink.evict(record)

    # Task-based sink execution on a set of batches
    def apply(self, batches, channel_id, _source_operator_id):
        records = 0
        for batch in batches:
            for record in batch:
                if record is None:
                    if self.input._close_channel(channel_id):
                        self.sink.close()
                        signal.send(ActorExit(self.instance_id))
                        records += len(batch)
                    return records
                self.sink.evict(record)
            records += len(batch)
        return records

    # Returns the sink's state
    def state(self):
        try:  # There might be no 'get_state()' method implemented
            return (self.instance_id, self.sink.get_state())
        except AttributeError:
            return None


# A sink actor that writes records to a distributed text file
@ray.remote
class WriteTextFile(OperatorInstance):
    def __init__(self, instance_id, operator_metadata, input_gate,
                 output_gate):
        OperatorInstance.__init__(self, instance_id,
                                  operator_metadata, input_gate, output_gate)
        # Common prefix for all instances of the sink as given by the user
        prefix = operator_metadata.filename_prefix
        # Suffix of self's filename
        operator_id, local_instance_id = instance_id
        suffix = str(operator_id) + "_" + str(local_instance_id)
        self.filename = prefix + "_" + suffix
        # TODO (john): Handle possible exception here
        self.writer = open(self.filename, "w")
        # User-defined logic applied to each record (optional)
        self.logic = operator_metadata.logic

    # Applies logic (if any) and writes result to a text file
    def _put_next(self, record):
        if self.logic is None:
            self.writer.write(str(record) + "\n")
        else:
            self.writer.write(str(self.logic(record)) + "\n")

    # Starts the sink
    def start(self):
        while True:
            record = self.input._pull()
            if record is None:
                self.writer.close()
                signal.send(ActorExit(self.instance_id))
                return
            self._put_next(record)

    # Task-based sink execution on a set of batches
    def apply(self, batches, channel_id, _source_operator_id):
        records = 0
        for batch in batches:
            for record in batch:
                if record is None:
                    if self.input._close_channel(channel_id):
                        signal.send(ActorExit(self.instance_id))
                        records += len(batch)
                    return records
                self._put_next(record)
            records += len(batch)
        return records

# TODO (john): Time window actor (uses system time)
@ray.remote
class TimeWindow(OperatorInstance):
    def __init__(self, instance_id, operator_metadata, input_gate,
                 output_gate):
        OperatorInstance.__init__(self, instance_id,
                                  operator_metadata, input_gate, output_gate)
        # The length of the window in ms
        self.length = operator_metadata.length
        self.window_state = []
        self.start = time.time()

    def start(self):
        while True:
            if self._reschedule():
                # Stop spinning so that other methods can be
                # called on this actor from the outside world
                return
            record = self.source.get_next()
            if record is None:
                self.output._flush(close=True)
                signal.send(ActorExit(self.instance_id))
                return
            self.window_state.append(record)
            elapsed_time = time.time() - self.start
            if elapsed_time >= self.length:
                self.output._push_all(self.window_state)
                self.window_state.clear()
                self.start = time.time()

    # Task-based time window execution
    def apply(self, batches, channel_id, _source_operator_id):
        sys.exit("Task-based time window execution not supported yet.")
