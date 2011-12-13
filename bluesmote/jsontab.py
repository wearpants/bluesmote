"""utilities for working with MRJob's tab-separated JSON data"""

import json

def iterload(f):
    for line in f:
        k, v = line.rstrip().split('\t', 1)
        yield json.loads(k), json.loads(v)

def load(f):
    return list(iterload(f))

def dump(x, f):
    try:
        it = x.iteritems()
    except AttributeError:
        it = iter(x)

    f.writelines("%s\t%s\n"%(json.dumps(k), json.dumps(v)) for k, v in it)

