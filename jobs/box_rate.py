#!/usr/bin/env python
"""playing around w/ MRJob"""

from mrjob.job import MRJob
from bluesmote.record import Record

class BoxRate(MRJob):
    @Record.wrap
    def mapper(self, _, r):
        box = os.path.basename(r.filename)
        yield box, r.sc_filter_result
                
    def combiner(self, domain, result):
        yes = no = total = 0
        for r in result:
            total +=1
            if r in ("OBSERVED", "PROXIED"):
                yes += 1
            elif r == "DENIED":
                no += 1
            else:
                self.increment_counter("unknown_sc_filter_result", r)
        yield domain, (yes, no, total)
                
    def reducer(self, domain, counts):
        yes = no = total = 0
        for y, n, t in counts:
            yes += y
            no += n
            total += t
        
        yield domain, no/float(total)
            
if __name__ == '__main__':
    BoxRate.run()