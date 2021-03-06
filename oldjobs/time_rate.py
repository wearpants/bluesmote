#!/usr/bin/env python
"""playing around w/ MRJob"""

from mrjob.job import MRJob
from bluesmote.record import Record
import re

class TimeRate(MRJob):
    
    @Record.wrap
    def mapper(self, _, r):
        yield (r.date, ), r.sc_filter_result
        yield (r.date, r.time[:-4]+'0:00'), r.sc_filter_result
            
    def combiner(self, dt, result):
        yes = no = total = 0
        for r in result:
            total +=1
            if r in ("OBSERVED", "PROXIED"):
                yes += 1
            elif r == "DENIED":
                no += 1
            else:
                self.increment_counter("unknown_sc_filter_result", r)
        yield dt, (yes, no, total)
                
    def reducer(self, dt, counts):
        yes = no = total = 0
        for y, n, t in counts:
            yes += y
            no += n
            total += t
        
        rate = no/float(total)        
        key = "%s %s"%tuple(dt) if len(dt) == 2 else dt[0]
        yield key, rate
            
if __name__ == '__main__':
    TimeRate.run()