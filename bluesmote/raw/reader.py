import gzip
import os

def find_logs(root=None):
    root = root if root is not None else os.getcwd()
    for path, dirs, files in os.walk(root):
        for filename in (os.path.abspath(os.path.join(path, filename)) for filename in files
                         if filename.endswith('.log.gz')):
            yield filename

def smart_find_logs(path):
    path = os.path.abspath(path)
    if os.path.isdir(path):
        return list(find_logs(path))
    else:
        return [path]

factor = 5.5
bytes_per_line = 400
compressed_bytes_per_line = bytes_per_line / factor
blocksize = int(16384 * bytes_per_line / factor)

def iter_rawlines(path):
    for fname in smart_find_logs(path):
        with gzip.open(fname) as f:
            #if 0: assert isinstance(f, gzip.GzipFile)
            i = 0
            for i, l in enumerate(f.readlines(blocksize), i):
                yield "%s %d %s"%(fname, i, l)