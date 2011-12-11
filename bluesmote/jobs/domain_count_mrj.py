#!/usr/bin/env python
"""playing around w/ MRJob"""

from mrjob.job import MRJob

from bluesmote.record import Record
import re

ip_re = re.compile(r"""^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$""")

class DomainCounter(MRJob):
    
    def mapper(self, key, line):
        r = Record.parse(line)
        if r is not None:
            yield "<TOTAL>", 1
            if ip_re.match(r.cs_host):
                yield "<IP>", 1
                yield r.cs_host, 1
            else:
                yield "<HOST>", 1
                yield ".".join(r.cs_host.rsplit('.', 2)[-2:]), 1
    
    def combiner(self, domain, occurrences):
        yield domain, sum(occurrences)
                
    def reducer(self, domain, occurrences):
        yield domain, sum(occurrences)
            
if __name__ == '__main__':
    DomainCounter.run()