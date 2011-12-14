#!/usr/bin/env python
"""playing around w/ MRJob"""

from mrjob.job import MRJob
from bluesmote.record import Record
import re

class TimeRate(MRJob):
    
    # output:
    # [date, HH:M0:00] - bytes/second over a 10-minute period
    # [date] - total bytes (for the day)
    
    
    @Record.wrap
    def mapper(self, _, r):
        yield (r.date, ), (r.sc_bytes, r.cs_bytes)
        yield (r.date, r.time[:-4]+'0:00'), (r.sc_bytes, r.cs_bytes)
            
    def combiner(self, dt, result):
        outbound, inbound = 0
        for out_b, in_b in result:
            outbound += out_b
            inbound += in_b
            
        yield dt, (outbound, inbound)
                
    def reducer(self, dt, counts):
#        _, (outbound, inbound) = self.combiner(dt, counts).next()
        
        outbound, inbound = 0
        for out_b, in_b in result:
            outbound += out_b
            inbound += in_b
        
        if len(dt) == 2:            
            key = "%s %s"%tuple(dt)
            out_rate = outbound/60.0 
            in_rate = inbound/60.0
            yield key, (out_rate, in_rate)
        else:
            yield dt[0], (outbound, inbound)
            
if __name__ == '__main__':
    TimeRate.run()