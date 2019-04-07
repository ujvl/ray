from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import time

from ray.experimental.streaming.benchmarks.macro.nexmark.event import Auction
from ray.experimental.streaming.benchmarks.macro.nexmark.event import Bid
from ray.experimental.streaming.benchmarks.macro.nexmark.event import Person

# A stream replayer that reads Nexmark events from files and
# replays them at given rates
class NexmarkEventGenerator(object):
    def __init__(self, event_file, event_type, event_rate,
                       sample_period=1000):
        self.event_file = event_file
        self.event_rate = event_rate  if event_rate > 0 else float("inf")
        self.event_type = event_type  # Auction, Bid, Person
        self.events = []
        # Read all events from the input file
        with open(self.event_file, "r") as ef:
            for event in ef:
                self.events.append(self.create_event(event))
        # Used for event replaying
        self.total_count = 0
        self.count = 0
        self.period = sample_period
        self.start = 0

    # Waits
    def __wait(self):
        while (self.total_count / (time.time() - self.start) >
               self.event_rate):
           time.sleep(0.0001)  # 100 us

    # Returns the next event
    def get_next(self):
        if not self.start:
            self.start = time.time()
        if not self.events:
            return None  # Exhausted
        event = self.events.pop(0)
        # print(event)
        self.total_count += 1
        # Wait if needed
        self.__wait()
        self.count += 1
        if self.count == self.period:
            self.count = 0
            # Assign the generation timestamp
            event.system_time = time.time()
        return event

    # Parses a nexmark event log and creates an event object
    def create_event(self, event):
        event = event.strip()[1:-1]  # Trim spaces and brackets
        if self.event_type == "Bid":
            bid = Bid()
            raw_attributes = event.split(",")
            attribute_value = []
            for attribute in raw_attributes:
                k_v = attribute.split(":")
                key = k_v[0][1:-1]
                value = int(k_v[1]) if k_v[1][0] != "\"" else str(k_v[1])
                setattr(bid, key, value)
            return bid
        elif self.event_type == "Person":
            person = Person()
            raw_attributes = event.split(",")
            attribute_value = []
            for attribute in raw_attributes:
                k_v = attribute.split(":")
                key = k_v[0][1:-1]
                value = int(k_v[1]) if k_v[1][0] != "\"" else str(k_v[1])
                setattr(person, key, value)
            return person
        else:
            assert self.event_type == "Auction", (event_type, "Auction")
            auction = Auction()
            raw_attributes = event.split(",")
            attribute_value = []
            for attribute in raw_attributes:
                k_v = attribute.split(":")
                key = k_v[0][1:-1]
                value = int(k_v[1]) if k_v[1][0] != "\"" else str(k_v[1])
                setattr(auction, key, value)
            return auction

# Used to measure per-record processing time in nexmark queries
def compute_elapsed_time(record):
    generation_time = record.system_time
    if generation_time is not None:
        # TODO (john): Clock skew might distort elapsed time
        return [time.time() - generation_time]
    else:
        return []

# A custom sink used to measure processing latency
class LatencySink(object):
    def __init__(self):
        self.state = []
        self.logic = compute_elapsed_time

    # Evicts next record
    def evict(self, record):
        self.state.extend(self.logic(record))

    # Closes the sink
    def close(self):
        pass

    # Returns sink's state
    def get_state(self):
        return self.state
