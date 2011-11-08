#!/usr/bin/env python
"""output all lines where sc_filter_result != OBSERVED"""

from bluesmote import reader, pipeline, util
from bluesmote.util import chunk
import sys

import multiprocessing

DONE = "DONEasflkdsafjldsaf 5 u0843tuu5ut0t5uthysifdsjf n vnkDONE"

def worker(wrap, fnames, q):
    return multiprocessing.Process(target = _work, args=(wrap, fnames, q))

def _work(wrap, fnames, q):
    for fname in fnames:
        with open(fname, 'rb') as f:
            for c in chunk(pipeline.inline(f, wrap), 16384):
                q.put(c)
    q.put(DONE)
                
intern("OBSERVED")
def filt(it):
    return (r for r in it if r.sc_filter_result != "OBSERVED")

def func(it):
    return util.stringify(filt(it))
    
def main(input, output):
    q = multiprocessing.Queue(100)
    w1 = worker(func, ['SG_main__420722212535.log.gz'], q)
    w2 = worker(func, ['SG_main__420723084209.log.gz'], q)

    w1.daemon = True
    w2.daemon = True
    
    w1.start()
    w2.start()
    print "starting"
    dones = 0
    count = 0
    with open(output, 'wb') as outfile:
        while dones < 2:
            x = q.get()
            if x != DONE:                
                outfile.writelines(x)
            else:
                dones += 1
    
    w1.terminate()
    w2.terminate()

if __name__ == '__main__':
    main(sys.argv[1], sys.argv[2])