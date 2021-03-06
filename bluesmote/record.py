from functools import wraps
from collections import namedtuple

Record = namedtuple("Record",
["filename",
 "lineno",
 "date",
 "time",
 "time_taken",
 "c_ip", # obfuscated IP
 "cs_username", # blank or HTTP username?
 "cs_auth_group", # blank?
 "x_exception_id", # what filter stopped processing?
 "sc_filter_result", # filter decision
 "cs_categories", # always "unavailable"?
 "cs_referer", # Referrer header
 "sc_status", # HTTP Response code
 "s_action", # how retrieved (cache, etc)
 "cs_method", # request method - GET, POST, etc.
 "rs_content_type", # response Content-Type
 "cs_uri_scheme",
 "cs_host", # Request Host: header
 "cs_uri_port",
 "cs_uri_path",
 "cs_uri_query",
 "cs_uri_extension", # file extension
 "cs_user_agent",
 "s_ip", # IP of proxy OR destination IP
 "sc_bytes", # inbound bytes: internet -> proxy -> client
 "cs_bytes", # outbound bytes: client -> proxy -> internet
 "x_virus_id"])

Record.__str__ = lambda self: "%s\n"%("\t".join(self))

@staticmethod
def parse(s):
    try:
        return Record._make(s.rstrip().split('\t'))
    except:
        return None

Record.parse = parse

def proxy_id(s):
    return s.filename[10:12]

Record.proxy_id= proxy_id

@staticmethod
def wrap(f):
    @wraps(f)
    def wrapped(self, _, line):
        r = Record.parse(line)
        if r is not None:
            for i in f(self, _, r): yield i
    return wrapped

Record.wrap = wrap
