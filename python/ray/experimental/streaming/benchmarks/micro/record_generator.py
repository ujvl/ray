from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import time

# A record generator used in bechmarks
# The generator periodically assigns timestamps to records so that we can
# measure end-to-end per-record latency
class RecordGenerator(object):
    """Generates records of type int or str.

    Attributes:
        rounds (int): Number of rounds, each one generating 100K records
        record_type (str): The type of records to generate ("int" or "string")
        record_size (int): The size of string in case record_type="string"
        sample_period (int): The period to measure record latency
                             (every 'sample_period' records)
        fixed_rate (int): The source rate (unbounded by default)
        warm_up (bool): Whether to do a first warm-up round or not
    """
    def __init__(self, rounds, record_type="int",
                 record_size=None, sample_period=1,
                 fixed_rate=-1,
                 warm_up=False,
                 records_per_round=100000,
                 key=None):

        assert rounds > 0, rounds
        assert fixed_rate != 0, fixed_rate

        self.warm_up = warm_up
        if self.warm_up:
            rounds += 1
        self.records_per_round = records_per_round
        self.total_elements = records_per_round * rounds
        self.total_count = 0
        self.period = sample_period
        self.fixed_rate = fixed_rate if fixed_rate > 0 else float("inf")
        self.rate_count = 0
        self.start = time.time()
        self.count = 0
        self.current_round = 0
        self.record_type = record_type
        self.record_size = record_size
        self.record = -1
        self.key = key
        # Set the right function
        if self.key is not None:
            self.__get_next_record = self.__get_next_keyed_int
        elif self.record_type == "int":
            self.__get_next_record = self.__get_next_int
        elif self.record_type == "string":
            self.__get_next_record = self.__get_next_string
        else:
            message = "Unrecognized record type '{}'"
            logger.error(message.format(self.record_type))
            sys.exit()

    # Returns the next int
    def __get_next_int(self):
        self.record += 1
        return self.record

    # Returns the next int
    def __get_next_keyed_int(self):
        self.record += 1
        return (self.key, self.record)

    # Returns the next (random) string
    def __get_next_string(self):
        return "".join(random.choice(
                string.ascii_letters + string.digits) for _ in range(
                                                            self.record_size))

    # Waits
    def __wait(self):
        while (self.rate_count / (time.time() - self.start) >
               self.fixed_rate):
           time.sleep(0.0001)  # 100 us

    # Returns the next record (either int or string depending on record_type)
    def get_next(self):
        if self.total_count == self.total_elements:
            return None  # Exhausted
        record = self.__get_next_record()
        self.total_count += 1
        self.rate_count += 1
        # Wait if needed
        self.__wait()
        # Measure source rate per round
        if self.rate_count == self.records_per_round:
            self.rate_count = 0
            self.start = time.time()
            time.sleep(0.0001)  # 100 us
        # Do a first round without measuring latency just to warm up
        if self.warm_up and self.total_count <= self.records_per_round:
            if self.total_count == self.records_per_round:
                logger.info("Finished warmup.")
            return (-1,record)
        self.count += 1
        if self.count == self.period:
            self.count = 0
            # Assign the record generation timestamp
            return (time.time(),record)
        else:
            return(-1,record)

    def init(self):
        pass

    # Drains the generator and returns the total number of records produced
    def drain(self, no_wait=False):
        if no_wait:
            self.fixed_rate = float("inf")
        records = 0
        while self.get_next() is not None:
            records += 1
        return records
