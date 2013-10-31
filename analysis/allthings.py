from bluesmote import jsontab
import numpy as np

dtype = np.dtype([('timestamp', 'datetime64[s]'),
                  ('domain', 'S100'),
                  ('accessed', np.uint32),
                  ('blocked', np.uint32),
                  ('outbound', np.uint64),
                  ('inbound', np.uint64)])

def load(dirname):
    """load numpy data from jsontab files"""
    it = ((k[:20],
           np.string_(k[21:]),
           v[0],
           v[1],
           v[2],
           v[3])
          for k, v in jsontab.iterload_dir(dirname))
    return np.fromiter(it, dtype = dtype)