#!/usr/bin/env python
"""output all lines where sc_filter_result != OBSERVED"""

from bluesmote import reader, pipeline, util
import sys
import os
import operator
import csv
from collections import defaultdict, OrderedDict, Counter

def make_counts():
    return {
        "date": Counter(),
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
    for r in it:
        for field, counter in local_counts.iteritems():
            counter[getattr(r, field)] += 1
    return local_counts
        
def main(input, output, num_workers):
    fnames = reader.smart_find_logs(input)
    
    pipeline.pool(fnames, util.identity, local_reducer, global_reducer, num_workers)
        
    dump_counts(global_counts, output)

if __name__ == '__main__':
    main(sys.argv[1], sys.argv[2], int(sys.argv[3]))