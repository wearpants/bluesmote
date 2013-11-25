#!/usr/bin/env python
"""playing around w/ MRJob"""

from mrjob.job import MRJob
from mrjob.protocol import RawValueProtocol, PickleProtocol
from bluesmote.record import Record
import re
from itertools import imap, chain, izip_longest

ip_re = re.compile(r"""^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$""")

fields = {
    'x_exception_id': set(['-', 'tcp_error', 'internal_error', 'policy_denied', 'invalid_request', 'unsupported_protocol', 'dns_unresolved_hostname', 'dns_server_failure', 'policy_redirect', 'RARE']),
     'sc_filter_result': set(['OBSERVED', 'DENIED', 'PROXIED']),
     'sc_status': set(['200', '304', '206', '503', '403', '302', '0', '204', '404', '400', '500', '301', '410', '401', '406', '504', '412', '416', '501', '303', '502', '408', '307', '405', '100', '202', '201', '761', '999', '509', '300', 'RARE']),
     's_action': set(['TCP_NC_MISS', 'TCP_HIT', 'TCP_MISS', 'TCP_ERR_MISS', 'TCP_CLIENT_REFRESH', 'TCP_DENIED', 'TCP_REFRESH_MISS', 'TCP_PARTIAL_MISS', 'TCP_TUNNELED', 'TCP_NC_MISS_RST', '-', 'TCP_AUTH_HIT', 'TCP_AUTH_MISS', 'TCP_POLICY_REDIRECT', 'TCP_MISS_RST', 'RARE']),
     'cs_uri_scheme': set(['http', '-', 'tcp', 'ftp', 'rtsp']),
     'proxy_id': set(['42', '43', '44', '45', '46', '47', '48']),
     }


def build_dict():
    result = dict(("%s/%s"%(k, v), 0) for k, L in fields.iteritems() for v in L)

    result['sc_bytes'] = 0
    result['cs_bytes'] = 0
    result['requests'] = 0
    return result

def headers():
    return list(chain(('timestamp', 'domain'), sorted(build_dict().iterkeys())))

class Rollup(MRJob):

    INTERNAL_PROTOCOL = PickleProtocol
    OUTPUT_PROTOCOL = RawValueProtocol

    # output: tab separated
    # key domain.com isodate
    # fields: accessed blocked in_bytes out_bytes

    @Record.wrap
    def mapper(self, _, r):
        self.increment_counter('lines', 'lines')
        result = build_dict()

        for k, L in fields.iteritems():
            x = getattr(r, k)
            if callable(x):
                # handle proxy_id
                x = x()
            if x in L:
                result['%s/%s'%(k, x)] += 1
            elif 'RARE' in L:
                result['%s/RARE'%k] += 1
            else:
                # we got an unknown value for a field in which we don't record RAREs. Bad.
                self.increment_counter("%s/%s"%(k, x), "%s/%s"%(k, x))

        result['requests'] += 1
        result['sc_bytes'] += int(r.sc_bytes)
        result['cs_bytes'] += int(r.cs_bytes)

        isodate = "%sT%s:00Z"%(r.date, r.time[:-3])
        if ip_re.match(r.cs_host):
            yield (isodate, r.cs_host), result
        else:
            yield (isodate, ".".join(r.cs_host.rsplit('.', 2)[-2:])), result

    def combiner(self, key, values):
        result = build_dict()
        for d in values:
            for k, v in d.iteritems():
                result[k] += v
        yield key, result

    def reducer(self, key, values):
        result = build_dict()
        for d in values:
            for k, v in d.iteritems():
                result[k] += v

        arr = (str(x[1]) for x in sorted(result.iteritems()))
        yield key, "\t".join(chain(key, arr))


if __name__ == '__main__':
    Rollup.run()