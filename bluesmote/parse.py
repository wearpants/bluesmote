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
 "cs_method",
 "rs_content_type", # rsponse Content-Type
 "cs_uri_scheme", 
 "cs_host", # Request Host: header
 "cs_uri_port",
 "cs_uri_path",
 "cs_uri_query",
 "cs_uri_extension", # file extension
 "cs_user_agent",
 "s_ip", # IP of proxy
 "sc_bytes",
 "cs_bytes",
 "x_virus_id"])

dash_or_any = "-|\w+"
ip_address = "\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}" #good enough

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
    "[^ ]+ ", #cs_referer XXX need proper URI regex!
    "\d\d\d", #sc_status
    "[A-Z_]+", #s_action
    "\w+", #cs_method
    "[-\w/+;%]+", #rs_content_type - dash when DENIED
    "\w+", #cs_uri_scheme
    "[\w.]+", #cs_host
    "\d+", #cs_uri_port 
    "[-_./\w]+", #cs_uri_path XXX need proper path regex
    "[-_?&=\w]+", #cs_uri_query
    "\w+", #cs_uri_extension
    '".+"', #cs_user_agent
    ip_address, #s_ip
    "\d+", #sc_bytes
    "\d+", #cs_bytes
    dash_or_any, #x_virus_id
]

record_regex = " ".join("({})".format(r) for r in regexes)
record_re = re.compile(record_regex)

def parse(s):
    m = record_re.match(s)
    return Record(*m.groups()) if m else s
    