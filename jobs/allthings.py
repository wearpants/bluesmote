#!/usr/bin/env python
"""playing around w/ MRJob"""

from mrjob.job import MRJob
from mrjob.protocol import RawValueProtocol, PickleProtocol
from bluesmote.record import Record
import re

ip_re = re.compile(r"""^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$""")

class AllThings(MRJob):

    OUTPUT_PROTOCOL = RawValueProtocol

    # output: tab separated
    # key domain.com isodate
    # fields: accessed blocked in_bytes out_bytes

    @Record.wrap
    def mapper(self, _, r):
        isodate = "%sT%s:00:00Z"%(r.date, r.time[:-6])
        value = (r.sc_filter_result, int(r.sc_bytes), int(r.cs_bytes))

        if ip_re.match(r.cs_host):
            yield "%s\t%s"%(isodate, r.cs_host), value
        else:
            yield "%s\t%s"%(isodate, ".".join(r.cs_host.rsplit('.', 2)[-2:])), value

    def combiner(self, key, value):
        accessed = blocked = in_bytes = out_bytes = 0
        for r, i, o in value:
            in_bytes += i
            out_bytes += o
            if r in ("OBSERVED", "PROXIED"):
                accessed += 1
            elif r == "DENIED":
                blocked += 1
            else:
                self.increment_counter("unknown_sc_filter_result", r)
        yield key, (accessed, blocked, in_bytes, out_bytes)

    def reducer(self, key, value):
        accessed = blocked = in_bytes = out_bytes = 0
        for a, b, i, o in value:
            accessed += a
            blocked += b
            in_bytes += i
            out_bytes += o

        yield key, "\t".join((key, str(accessed), str(blocked), str(in_bytes), str(out_bytes)))


if __name__ == '__main__':
    AllThings.run()