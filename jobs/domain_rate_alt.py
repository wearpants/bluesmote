#!/usr/bin/env python
"""playing around w/ MRJob"""

from mrjob.job import MRJob
from mrjob.protocol import RawValueProtocol, PickleProtocol
from bluesmote.record import Record
import re
from itertools import imap, chain, izip_longest

ip_re = re.compile(r"""^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$""")

class DomainRateAlt(MRJob):

    INTERNAL_PROTOCOL = PickleProtocol
    OUTPUT_PROTOCOL = RawValueProtocol

    # output: tab separated
    # key domain.com isodate
    # fields: accessed blocked in_bytes out_bytes

    @Record.wrap
    def mapper(self, _, r):
        if r.sc_filter_result in ("OBSERVED", "PROXIED"):
            accessed = 1
            blocked = 0
        elif r.sc_filter_result == "DENIED":
            accessed = 0
            blocked = 1
        else:
            self.increment_counter("unknown_sc_filter_result", r.sc_filter_result)

        values = (accessed, blocked, int(r.sc_bytes), int(r.cs_bytes))
        if ip_re.match(r.cs_host):
            yield r.cs_host, values
        else:
            yield ".".join(r.cs_host.rsplit('.', 2)[-2:]), values

    def combiner(self, key, values):
        yield key, list(imap(sum, izip_longest(*values)))

    def reducer(self, key, values):
        arr = imap(sum, izip_longest(*values))
        yield key, "\t".join(chain((key,), imap(str, arr)))


if __name__ == '__main__':
    DomainRateAlt.run()