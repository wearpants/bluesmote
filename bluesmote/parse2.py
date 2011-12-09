from collections import namedtuple
import re

Record = namedtuple("Record",
["date",
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
 "sc_bytes",
 "cs_bytes",
 "x_virus_id"])

Record.__str__ = lambda self: "%s\r\n"%(" ".join(self))

## IGNORE THIS
#####################
dash_or_any = "-|\w+"
ip_address = "\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}" #good enough

rfc3986_uri = """(?:(?:[^:/?#]+):)?(?://(?:[^/?#]*))?(?:[^?#]*)(?:\?(?:[^#]*))?(?:#(?:.*))?"""
rfc3986_scheme = """[^:/?#]+"""
rfc3986_host = """[^/?#]*"""
rfc3986_path = """[^?#]*"""
rfc3986_query = """\?(?:[^#]*)"""

#rfc3986_uri = """(([^:/?#]+):)?(//([^/?#]*))?([^?#]*)(\?([^#]*))?(#(.*))?"""
#rfc3986_scheme = """([^:/?#]+)"""
#rfc3986_host = """([^/?#]*)"""
#rfc3986_path = """([^?#]*)"""
#rfc3986_query = """(\?([^#]*))"""


regexes = [
    "2011-\d\d-\d\d", #date
    "\d\d:\d\d:\d\d", #time
    "\d{1,5}", #time_taken
    #"[0-9a-f]{16}|(0\.0\.0\.0)", #c_ip # XXX non-capture!
    "\w+",
    dash_or_any, #cs_username
    dash_or_any, #cs_auth_group
    dash_or_any, #x_exception_id
    "[A-Z]+", #sc_filter_result
    '"\w+"', #cs_categories
    rfc3986_uri, #cs_referer
    "\d\d\d", #sc_status
    "[A-Z_]+", #s_action
    "\w+", #cs_method
    "[-\w/+;%=]+", #rs_content_type - dash when DENIED XXX better regex?
    rfc3986_scheme, #cs_uri_scheme
    rfc3986_host, #cs_host
    "\d+", #cs_uri_port 
    rfc3986_path, #cs_uri_path
    rfc3986_query, #cs_uri_query
    "\w+", #cs_uri_extension
    '".+"', #cs_user_agent
    ip_address, #s_ip
    "\d+", #sc_bytes
    "\d+", #cs_bytes
    dash_or_any, #x_virus_id
]

record_regex = " ".join("({})".format(r) for r in regexes)
record_re = re.compile(record_regex)
############################################################

def slow_parse(s):
    """a slow and more accurate parser"""
    m = record_re.match(s)
    return Record(*m.groups()) if m else None

    
x = ["(\S+)"]*25
x[9] = "(\S+) ?" # cs_referrer has an extra trailing space (optional, b/c our output doesn't)
x[20] = '((?:".*?")|-)' # user-agent
cheap_regex = " ".join(x)#+r'''\r\n'''
cheap_re = re.compile(cheap_regex)

def parse(s):
    """parse a single line"""
    m = cheap_re.match(s)
    return Record(*m.groups()) if m else None

def iterparse(s):
    return (Record(*m.groups()) for m in cheap_re.finditer(s))
