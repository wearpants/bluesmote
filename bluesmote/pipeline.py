from itertools import chain, imap

from bluesmote.util import chunk

from . import reader, parse

import multiprocessing

def inline(f, mapper):
    dc = reader.decompress(reader.read_blocks(f))
    raw = chain.from_iterable(imap(parse.iterparse, dc))
    return mapper(raw)

DONE = "DONE_asflkdsafjldsaf_5_u0843tuu5ut0t5uthysifdsjf_n_vnk_DONE"

def _worker(mapper, local_reducer, fnames, q):
    return multiprocessing.Process(target = _work, args=(mapper, local_reducer, fnames, q))

def _work(mapper, local_reducer, fnames, q):
    for fname in fnames:
        with open(fname, 'rb') as f:
            for c in chunk(inline(f, mapper), 16384):
                q.put(local_reducer(c))
    q.put(DONE)

def pool(fnames, mapper, local_reducer, global_reducer, workers):
    q = multiprocessing.Queue(100)
    
    workers = [_worker(mapper, local_reducer, [fnames[i]], q) for i in xrange(workers)]
    for w in workers:
        w.daemon = True
        w.start()
    
    
    print "starting"
    dones = 0
    while dones < len(workers):
        x = q.get()
        if x != DONE:                
            global_reducer(x)
        else:
            dones += 1
    
    for w in workers:
        w.terminate()
