#!/usr/bin/env python
"""playing around w/ MRJob"""

from mrjob.job import MRJob
from bluesmote.record import Record
import re

class TimeRate(MRJob):

    # output:
    # [date, HH:00:00] - bytes/second over a 1 hour period


    @Record.wrap
    def mapper(self, _, r):
        yield (r.date, r.time[:-6]+':00:00'), (r.sc_bytes, r.cs_bytes)

    def combiner(self, dt, result):
        outbound = inbound = 0
        for out_b, in_b in result:
            outbound += int(out_b)
            inbound += int(in_b)

        yield dt, (outbound, inbound)

    def reducer(self, dt, counts):
        outbound = inbound = 0
        for out_b, in_b in counts:
            outbound += int(out_b)
            inbound += int(in_b)

        key = "%sT%sZ"%tuple(dt)
        yield key, (outbound, inbound)

if __name__ == '__main__':
    TimeRate.run()