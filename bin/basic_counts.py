#!/usr/bin/env python
"""output all lines where sc_filter_result != OBSERVED"""

from bluesmote import reader, pipeline, util
import sys
import os
import operator
import csv
from collections import defaultdict, OrderedDict, Counter

counts = {
    "x_exception_id": Counter(),
    "sc_filter_result": Counter(),
    "sc_status": Counter(),
    "s_action": Counter(),
    "cs_method": Counter(),
    "cs_uri_scheme": Counter(),
    "cs_host": Counter(),
    "cs_uri_port": Counter(),
    "cs_uri_extension": Counter(),
}

def do_increment(r):
    for field, counter in counts.iteritems():
        counter[getattr(r, field)] += 1

def increment(it):
    return (do_increment(r) for r in it)

def dump_counts(path):
    for field, counter in counts.iteritems():
        with open(os.path.join(path, field+'.csv'), 'w') as f:
            w = csv.writer(f, dialect="excel")
            w.writerows(sorted(counter.iteritems(), key=operator.itemgetter(1), reverse=True))

def main(input, output):
    for fname in reader.smart_find_logs(input):
        print "Processing", fname
        with open(fname, 'rb') as f:        
            results = pipeline.inline(f, increment)
            util.exhaust(results)
    dump_counts(output)

if __name__ == '__main__':
    main(sys.argv[1], sys.argv[2])