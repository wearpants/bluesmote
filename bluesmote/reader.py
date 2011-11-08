import zlib
import parse
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

BLOCKSIZE=16384

def read_blocks(f):
    x = f.read(BLOCKSIZE)
    while x:
        yield x
        x = f.read(BLOCKSIZE)

def decompress(blocks):
    dc = zlib.decompressobj(16+zlib.MAX_WBITS)
    for x in blocks:
        yield dc.decompress(x)
    yield dc.flush()