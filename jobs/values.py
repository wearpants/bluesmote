#!/usr/bin/env python
"""playing around w/ MRJob"""

from mrjob.job import MRJob
from mrjob.protocol import RawValueProtocol, PickleProtocol
from bluesmote.record import Record
import re
from itertools import imap, chain
import string

ip_re = re.compile(r"""^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$""")

# TODO
# 'cs_uri_extension'
# 'rs_content_type'

class Values(MRJob):

    INTERNAL_PROTOCOL = PickleProtocol
    OUTPUT_PROTOCOL = RawValueProtocol

    # output: tab separated
    # key domain.com isodate
    # fields: accessed blocked in_bytes out_bytes

    @Record.wrap
    def mapper(self, _, r):
        for k in ['x_exception_id', 'sc_filter_result', 'sc_status', 's_action',
                  'cs_uri_scheme']:
            v = getattr(r, k)
            yield (k, v), 1

        method = r.cs_method
        try:
            method.decode()
        except UnicodeDecodeError:
            pass
        else:
            if len(method) < 10 and \
               '%' not in method and \
               not method.translate(None, string.uppercase+string.lowercase):
                yield ('cs_method', method), 1


    def combiner(self, pair, counts):
        yield pair, sum(counts)

    def reducer(self, pair, counts):
        yield pair, "%s\t%s\t%d"%(pair[0], pair[1], sum(counts))


if __name__ == '__main__':
    Values.run()