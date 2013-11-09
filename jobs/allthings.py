#!/usr/bin/env python
"""playing around w/ MRJob"""

from mrjob.job import MRJob
from mrjob.protocol import RawValueProtocol, PickleProtocol
from bluesmote.record import Record
import re
from itertools import imap, chain, izip

ip_re = re.compile(r"""^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$""")

class AllThings(MRJob):

    INTERNAL_PROTOCOL = PickleProtocol
    OUTPUT_PROTOCOL = RawValueProtocol

    # output: tab separated
    # key domain.com isodate
    # fields: accessed blocked in_bytes out_bytes

    @Record.wrap
    def mapper(self, _, r):
        accessed = blocked = 0
        if r.sc_filter_result in ("OBSERVED", "PROXIED"):
            accessed += 1
        elif r.sc_filter_result == "DENIED":
            blocked += 1
        else:
            self.increment_counter("unknown_sc_filter_result", r.sc_filter_result)

        values = (accessed, blocked, int(r.sc_bytes), int(r.cs_bytes))

        isodate = "%sT%s:00:00Z"%(r.date, r.time[:-6])
        if ip_re.match(r.cs_host):
            yield (isodate, r.cs_host), values
        else:
            yield (isodate, ".".join(r.cs_host.rsplit('.', 2)[-2:])), values

    def combiner2(self, key, values):
        yield key, list(imap(sum, izip(*values)))

    def reducer(self, key, values):
        arr = imap(sum, izip(*values))
        yield key, "\t".join(chain(key, imap(str, arr)))


if __name__ == '__main__':
    AllThings.run()