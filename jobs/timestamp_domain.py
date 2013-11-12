#!/usr/bin/env python
"""playing around w/ MRJob"""

from mrjob.job import MRJob
from mrjob.protocol import RawValueProtocol, PickleProtocol
from bluesmote.record import Record
import re
from itertools import imap, chain

ip_re = re.compile(r"""^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$""")

class TimestampDomain(MRJob):

    INTERNAL_PROTOCOL = PickleProtocol
    OUTPUT_PROTOCOL = RawValueProtocol

    # output: tab separated
    # key domain.com isodate
    # fields: accessed blocked in_bytes out_bytes

    @Record.wrap
    def mapper(self, _, r):
        isodate = "%sT%s:00:00Z"%(r.date, r.time[:-6])
        value = (str(r.sc_filter_result), int(r.sc_bytes), int(r.cs_bytes))

        if ip_re.match(r.cs_host):
            yield (isodate, r.cs_host), value
        else:
            yield (isodate, ".".join(r.cs_host.rsplit('.', 2)[-2:])), value

    def combiner(self, key, value):
        accessed = blocked = other = error = in_bytes = out_bytes = 0
        for _ in value:
            try:
                r, i, o = _
            except ValueError:
                self.increment_counter("combiner_value_error", "XXX"+repr(_)+"XXX")
                error += 1
            else:
                in_bytes += i
                out_bytes += o
                if r in ("OBSERVED", "PROXIED"):
                    accessed += 1
                elif r == "DENIED":
                    blocked += 1
                else:
                    other += 1
                    self.increment_counter("unknown_sc_filter_result", r)

        assert accessed + blocked + other + error > 0, "WTF %r ==> %r"%(key, value)
        yield key, (accessed, blocked, other, error, in_bytes, out_bytes)

    def reducer(self, key, value):
        accessed = blocked = other = error = in_bytes = out_bytes = 0
        for a, b, x, e, i, o in value:
            accessed += a
            blocked += b
            other += x
            error += e
            in_bytes += i
            out_bytes += o

        yield key, "\t".join(chain(key, imap(str, (accessed, blocked, other, error, in_bytes, out_bytes))))


if __name__ == '__main__':
    TimestampDomain.run()