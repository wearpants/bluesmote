#!/usr/bin/env python
"""output all lines where sc_filter_result != OBSERVED"""

import sys
import csv
import random
                
intern("OBSERVED")
def filt(it):
    return (r for r in it if r.sc_filter_result != "OBSERVED")

def mapper(it):
    return util.stringify(filt(it))

def main(input, uname, passwd):
    dns = opendns.Client(uname, password, random.randint(10000, 99999)) 
    
    fnames = reader.smart_find_logs(input)        
    roller = util.rollover(output)
    pipeline.pool(fnames, mapper, util.identity, roller.writelines, num_workers)

if __name__ == '__main__':
    import cProfile, os
    cProfile.run('main(sys.argv[1], sys.argv[2], int(sys.argv[3]))', 'stats.'+os.getpid())