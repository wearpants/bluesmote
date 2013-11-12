from bluesmote import jsontab
import os.path
import itertools
import numpy as np
import pandas as pd

dtype = np.dtype([('timestamp', 'datetime64[s]'),
                  ('domain', 'U128'),
                  ('accessed', np.uint64),
                  ('blocked', np.uint64),
                  ('in_bytes', np.uint64),
                  ('out_bytes', np.uint64)])

def load(dirname):
    files = [os.path.join(dirname, f) for f in os.listdir(dirname)]
    lines = itertools.chain(*(open(f) for f in files))
    raw  = (l.rstrip().split('\t') for l in lines)
    fields = ((ts, np.string_(d), a, b, i, o) for ts, d, a, b, i, o in raw)
    return pd.DataFrame(np.fromiter(fields, dtype))