#!/usr/bin/env python
"""playing around w/ MRJob"""

from mrjob.job import MRJob
from bluesmote.record import Record
from mrjob.protocol import RawValueProtocol, PickleProtocol
import re
from itertools import imap, chain
from collections import defaultdict

ip_re = re.compile(r"""^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$""")

class DomainRate(MRJob):

    INTERNAL_PROTOCOL = PickleProtocol
    OUTPUT_PROTOCOL = RawValueProtocol

    @Record.wrap
    def mapper(self, _, r):
        yield "<TOTAL>",r.sc_filter_result
        if ip_re.match(r.cs_host):
            yield r.cs_host, r.sc_filter_result
            yield "<IP>", r.sc_filter_result
        else:
            yield ".".join(r.cs_host.rsplit('.', 2)[-2:]), r.sc_filter_result
            yield "<HOST>", r.sc_filter_result

    def combiner(self, domain, result):
        d = defaultdict(int)
        for r in result:
            d['TOTAL'] +=1
            d[r] += 1
        yield domain, d

    def reducer(self, domain, counts):
        d = defaultdict(int)

        for c in counts:
            for k, v in c.iteritems():
                d[k]+=v

        yield domain, "\t".join(chain([domain], ("%s\t%d"%i for i in sorted(d.iteritems()))))

if __name__ == '__main__':
    DomainRate.run()