#!/usr/bin/env python
"""output all lines where sc_filter_result != OBSERVED"""

from bluesmote import reader, pipeline, util
import sys

intern("OBSERVED")
def filt(it):
    return (r for r in it if r.sc_filter_result != "OBSERVED")
    

def main(input, output):
    with open(output, 'wb') as outfile:
        for fname in reader.smart_find_logs(input):
            print "Processing", fname
            with open(fname, 'rb') as f:
                results = pipeline.inline(f, filt)
                outfile.writelines(util.stringify(results))

if __name__ == '__main__':
    main(sys.argv[1], sys.argv[2])