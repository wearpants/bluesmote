#!/usr/bin/env python
"""playing around w/ MRJob"""

from mrjob.job import MRJob

import bluesomete.parse2
import re

ip_re = re.compile(r"""^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$""")

class DomainCounter(MRJob):
    
    def mapper(self, key, line):
        r = bluesomete.parse2.parse(line)
        if r is not None:
            if ip_re.match(r.cs_host):
                yield r.cs_host, 1
            else:
                yield ".".join(r.cs_host.rsplit('.', 2)[-2:]), 1
            
    def reducer(self, domain, occurrences):
        yield domain, sum(occurrences)
            
if __name__ == '__main__':
    DomainCounter.run()