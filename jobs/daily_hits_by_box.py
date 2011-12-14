#!/usr/bin/env python
"""playing around w/ MRJob"""

from mrjob.job import MRJob
import os.path

from bluesmote.record import Record

class ProxyDailyCounter(MRJob):
    
    @Record.wrap
    def mapper(self, _, r):
        
        box = os.path.basename(os.path.dirname(r.filename))
        yield (r.date, "<TOTAL>"), 1
        yield (r.date, box), 1
    
    def combiner(self, dt_box, occurrences):
        yield dt_box, sum(occurrences)
                
    def reducer(self, dt_box, occurrences):
        yield dt_box, sum(occurrences)
            
if __name__ == '__main__':
    ProxyDailyCounter.run()