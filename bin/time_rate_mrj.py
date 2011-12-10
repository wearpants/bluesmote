#!/usr/bin/env python
"""playing around w/ MRJob"""

from mrjob.job import MRJob
from mrjob.protocol import RawValueProtocol

from bluesmote.record import Record
import re

ip_re = re.compile(r"""^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$""")

class DomainCounter(MRJob):
    
    def mapper(self, key, line):
        r = Record.parse(line)
        if r: yield (r.date, r.time[:-4]+'0:00'), r.sc_filter_result
            
    def reducer(self, dt, result):
        yes = no = total = 0
        for r in result:
            total +=1
            if r in ("OBSERVED", "PROXIED"):
                yes += 1
            elif r == "DENIED":
                no += 1
            else:
                self.increment_counter("unknown_sc_filter_result", r)
                
        yield "%s %s"%tuple(dt), no/(total*1.0)
            
if __name__ == '__main__':
    DomainCounter.run()