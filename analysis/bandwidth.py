from bluesmote import jsontab
import numpy as np

dtype = np.dtype([('timestamp', 'datetime64[s]'), ('outbound', np.uint64), ('inbound', np.uint64)])

def load(dirname):
    """load numpy data from jsontab files"""
    it = (((i[0]), i[1][0], i[1][1]) for i in jsontab.iterload_dir(dirname))
    return np.fromiter(it, dtype = dtype)