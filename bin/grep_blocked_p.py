#!/usr/bin/env python
"""output all lines where sc_filter_result != OBSERVED"""

from bluesmote import reader, pipeline, util
import sys
                
intern("OBSERVED")
def filt(it):
    return (r for r in it if r.sc_filter_result != "OBSERVED")

def mapper(it):
    return util.stringify(filt(it))

def main(input, output, num_workers):
    fnames = reader.smart_find_logs(input)        
    with open(output, 'wb') as outfile:
        pipeline.pool(fnames, mapper, util.identity, outfile.writelines, num_workers)

if __name__ == '__main__':
    main(sys.argv[1], sys.argv[2], int(sys.argv[3]))