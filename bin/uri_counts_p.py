#!/usr/bin/env python
"""output all lines where sc_filter_result != OBSERVED"""

from bluesmote import reader, pipeline, util
import sys
import os
import operator
import csv
from collections import defaultdict, OrderedDict, Counter
import urlparse
import string
import re
import sys

def make_counts():
    return {
        "domain_parts": Counter(),
        #"raw_path": Counter(),
        #"raw_query": Counter(),
        #"path": Counter(),
        #"query": Counter(),
        "query_parts": Counter(),
        "path_parts": Counter(),
        "path_query_parts": Counter(),
        "all_parts": Counter(),
    }

def dump_counts(counts, path):
    for field, counter in counts.iteritems():
        with open(os.path.join(path, field+'.csv'), 'w') as f:
            w = csv.writer(f, dialect="excel-tab")
            w.writerows(sorted(counter.iteritems(), key=operator.itemgetter(1), reverse=True))

global_counts = make_counts()
def global_reducer(partial_counts):
    global global_counts
    for name, counter in global_counts.iteritems():
        counter.update(partial_counts[name])

def local_reducer(it):
    local_counts = make_counts()
    for d in it:
        for field, l in d.iteritems():
            c = local_counts[field]              
            for i in l:
                if i is None: continue
                c[i] += 1
    return local_counts

def mapper(it):
    return (doit(r) for r in it)

split_re = re.compile("["+string.punctuation+"]")

def split(s):
    return split_re.split(s) 

ip_re = re.compile(r"""^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$""")

def is_ip(s):
    return ip_re.match(s) is not None

num_dot=set(str(i) for i in xrange(10))
num_dot.add('.')

def is_ip2(s):
    return not set(s) - num_dot

def doit(r):
    if r.cs_host == '-' or is_ip2(r.cs_host):
        domain_parts = []
        domain_base = None        
    else:
        domain_parts = split_re.split(r.cs_host)    
        domain_base = ".".join(r.cs_host.rsplit('.', 2)[-2:])
     
    #path = urlparse.unquote(r.cs_uri_path)
    path = r.cs_uri_path
    path_parts = split_re.split(path)
    
    if r.cs_uri_query != '-':
        #query = urlparse.unquote(r.cs_uri_query)
        query = r.cs_uri_query
        query_parts = split_re.split(query)
    else:
        query = None
        query_parts = []
    
    return {
        "domain_parts": domain_parts,
        # XXX worried this may blow up memory
        #"raw_path": [r.cs_uri_path],
        #"raw_query": [r.cs_uri_query[1:]],
        #"path": [path],
        #"query": [query],
        "path_parts": path_parts,
        "query_parts": query_parts,
        "path_query_parts": path_parts + query_parts,
        "all_parts": path_parts + query_parts + domain_parts,
    }

def main(input, output, num_workers):
    fnames = reader.smart_find_logs(input)
    
    pipeline.fakepool(fnames, mapper, local_reducer, global_reducer, num_workers)
    dump_counts(global_counts, output)

if __name__ == '__main__':
    main(sys.argv[1], sys.argv[2], int(sys.argv[3]))