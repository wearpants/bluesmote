#!/usr/bin/env python
"""playing around w/ MRJob"""

from mrjob.job import MRJob
from bluesmote.record import Record
from mrjob.protocol import RawValueProtocol, PickleProtocol
import re
from itertools import imap, chain

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
        yes = no = total = 0
        for r in result:
            total +=1
            if r in ("OBSERVED", "PROXIED"):
                yes += 1
            elif r == "DENIED":
                no += 1
            else:
                self.increment_counter("unknown_sc_filter_result", r)
        yield domain, [yes, no, total]

    def reducer(self, domain, counts):
        yes = no = total = 0
        for y, n, t in counts:
            yes += y
            no += n
            total += t

        yield domain, "\t".join(imap(str, (domain, yes, no, total, no/float(total))))

if __name__ == '__main__':
    DomainRate.run()