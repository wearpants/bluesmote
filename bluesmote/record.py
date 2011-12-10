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
 "sc_bytes",
 "cs_bytes",
 "x_virus_id"])

Record.__str__ = lambda self: "%s\n"%("\t".join(self))
Record.parse = lambda s: Record(*s.rstrip().split('\t'))