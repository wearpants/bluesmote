from bluesmote import jsontab
import os.path
import itertools
import numpy as np
import pandas as pd
from itertools import chain, groupby

from jobs.rollup import headers

dtype = np.dtype([('timestamp', 'datetime64[s]'), ('domain', 'U128')] + [(k, np.uint64) for k in headers()][2:])

def load(dirname):
    files = [os.path.join(dirname, f) for f in os.listdir(dirname)]
    lines = itertools.chain(*(open(f) for f in files))
    raw  = (l.rstrip().split('\t') for l in lines)
    fields = (tuple(chain((r[0], np.string_(r[1])), r[2:])) for r in raw)
    return pd.DataFrame(np.fromiter(fields, dtype))


def sanity_check(data):
    """sum data across similar columns"""
    d = {}
    for k, group in groupby((c for c in data.columns
                             if c not in ('timestamp', 'domain', 'sc_bytes', 'cs_bytes')),
                            lambda c: c.split('/')[0]):
        d[k] = 0
        for c in group:
            d[k] += data[c].sum()
    return d