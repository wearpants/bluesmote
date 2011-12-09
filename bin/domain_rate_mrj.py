#!/usr/bin/env python
"""playing around w/ MRJob"""

from mrjob.job import MRJob
from mrjob.protocol import RawValueProtocol

import bluesomete.parse2
import re

ip_re = re.compile(r"""^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$""")

class DomainCounter(MRJob):
    DEFAULT_OUTPUT_PROTOCOL = 'raw_value'
    
    def mapper(self, key, line):
        r = bluesomete.parse2.parse(line)
        if r is not None:
            if ip_re.match(r.cs_host):
                yield r.cs_host, r.sc_filter_result
            else:
                yield ".".join(r.cs_host.rsplit('.', 2)[-2:]), r.sc_filter_result
            
    def reducer(self, domain, result):
        yes = no = total = 0
        for r in result:
            total +=1
            if r in ("OBSERVED", "PROXIED"):
                yes += 1
            elif r == "DENIED":
                no += 1
            else:
                self.increment_counter("unknown_sc_filter_result", r)
                
        #yield domain, (total, no/(total*1.0))
        yield None, "%s\t%d\t%r"%(domain, total, no/(total*1.0))
            
if __name__ == '__main__':
    DomainCounter.run()