from bluesmote import jsontab
import numpy as np

dtype = np.dtype([('timestamp', 'datetime64[s]'), ('outbound', np.uint64), ('inbound', np.uint64)])

def load(fname):
    """load numpy data from jsontab file"""
    with open(fname) as f:
        it = (((i[0]), i[1][0], i[1][1]) for i in jsontab.iterload(f))
        return np.fromiter(it, dtype = dtype)