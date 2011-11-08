#!/usr/bin/env python
"""output all lines where sc_filter_result != OBSERVED"""

from bluesmote import reader, pipeline, util
import sys
                
intern("OBSERVED")
def filt(it):
    return (r for r in it if r.sc_filter_result != "OBSERVED")

def func(it):
    return util.stringify(filt(it))
    
def main(input, output):
    with open(output, 'wb') as outfile:
        fnames = ['SG_main__420722212535.log.gz', 'SG_main__420723084209.log.gz']
        pipeline.pool(fnames, func, outfile.writelines, 2)

if __name__ == '__main__':
    main(sys.argv[1], sys.argv[2])