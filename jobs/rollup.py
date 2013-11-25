#!/usr/bin/env python
"""playing around w/ MRJob"""

from mrjob.job import MRJob
from mrjob.protocol import RawValueProtocol, PickleProtocol
from bluesmote.record import Record
import re
from itertools import imap, chain, izip_longest

ip_re = re.compile(r"""^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$""")

fields = {
    'x_exception_id': set(['-', 'tcp_error', 'internal_error', 'policy_denied', 'invalid_request', 'unsupported_protocol', 'dns_unresolved_hostname', 'dns_server_failure', 'policy_redirect']),
     'sc_filter_result': set(['OBSERVED', 'DENIED', 'PROXIED']),
     'sc_status': set(['200', '304', '206', '503', '403', '302', '0', '204', '404', '400', '500', '301', '410', '401', '406', '504', '412', '416', '501', '303', '502', '408', '307', '405', '100', '202', '201', '761', '999', '509', '300']),
     's_action': set(['TCP_NC_MISS', 'TCP_HIT', 'TCP_MISS', 'TCP_ERR_MISS', 'TCP_CLIENT_REFRESH', 'TCP_DENIED', 'TCP_REFRESH_MISS', 'TCP_PARTIAL_MISS', 'TCP_TUNNELED', 'TCP_NC_MISS_RST', '-', 'TCP_AUTH_HIT', 'TCP_AUTH_MISS', 'TCP_POLICY_REDIRECT', 'TCP_MISS_RST']),
     'cs_uri_scheme': set(['http', '-', 'tcp', 'ftp', 'rtsp'])
     }


def build_dict():
    result = {"%s/%s"%(k, v): 0 for k, L in fields.iteritems() for v in L }
    for k in fields.iterkeys():
        result["%s/RARE"%k] = 0

    result['sc_bytes'] = 0
    result['cs_bytes'] = 0
    return result

def headers():
    return "\t".join(chain(('timestamp', 'domain'), sorted(build_dict().iterkeys())))


class Rollup(MRJob):

    INTERNAL_PROTOCOL = PickleProtocol
    OUTPUT_PROTOCOL = RawValueProtocol

    # output: tab separated
    # key domain.com isodate
    # fields: accessed blocked in_bytes out_bytes

    @Record.wrap
    def mapper(self, _, r):
        result = build_dict()

        for k, L in fields.iteritems():
            x = getattr(r, k)
            if x in L:
                result['%s/%s'%(k, x)] += 1
            else:
                result['%s/RARE'%k] += 1

        result['sc_bytes'] = int(r.sc_bytes)
        result['cs_bytes'] = int(r.cs_bytes)

        values = [v for k, v in sorted(result.iteritems())]

        isodate = "%sT%s:00Z"%(r.date, r.time[:-3])
        if ip_re.match(r.cs_host):
            yield (isodate, r.cs_host), values
        else:
            yield (isodate, ".".join(r.cs_host.rsplit('.', 2)[-2:])), values

    def combiner(self, key, values):
        yield key, list(imap(sum, izip_longest(*values)))

    def reducer(self, key, values):
        arr = imap(sum, izip_longest(*values))
        yield key, "\t".join(chain(key, imap(str, arr)))


if __name__ == '__main__':
    Rollup.run()