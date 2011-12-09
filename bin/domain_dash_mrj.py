#!/usr/bin/env python
"""playing around w/ MRJob"""

from mrjob.job import MRJob

import bluesomete.parse2
import re

ip_re = re.compile(r"""^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$""")

class DomainCounter(MRJob):
    
    def mapper(self, key, line):
        r = bluesomete.parse2.parse(line)
        if r is not None and r.cs_host == '-':
            yield line, None
                       
if __name__ == '__main__':
    DomainCounter.run()