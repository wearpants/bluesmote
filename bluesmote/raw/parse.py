import sys
import re
import gzip
import os
from itertools import * 
import operator

from ..record import Record
assert len(Record._fields == 27)

# some knobs
compression_factor = 5.5
bytes_per_line = 400
compressed_bytes_per_line = int(bytes_per_line / compression_factor)
read_blocksize = int(16384 * bytes_per_line / compression_factor)
write_filesize = 10 * 1024 * 1024   # 10Mb
write_blocksize = write_filesize/bytes_per_line

x = ["(\S+)"]*27
x[9] = "(\S+) " # cs_referrer has an extra trailing space
x[20] = '((?:".*?")|-)' # user-agent
raw_regex = " ".join(x)+r'''\r\n'''
raw_re = re.compile(raw_regex)

def parse(s):
    """parse a single line"""
    m = raw_re.match(s)
    return Record(*m.groups()) if m else None

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

def iter_rawlines(path):
    for fname in smart_find_logs(path):
        with gzip.open(fname) as f:
            #if 0: assert isinstance(f, gzip.GzipFile)
            i = 0
            for i, l in enumerate(f.readlines(read_blocksize), i):
                yield "%s %d %s"%(fname, i, l)

def iter_records(path):
    return imap(parse, iter_rawlines(path))


def writefiles(inpath, outpath):
    records = iter_records(inpath)
    for i, lines in chunk(records, write_blocksize):
        fname = os.path.join(outpath, 'block-%(08d).log.gz'%i)    
        with gzip.open(fname, 'wb') as f:
            f.writelines(stringify(lines))
            
def chunk(iterable, size):
    """return items from iterable in chunks of size items"""
    if isinstance(iterable, (list, tuple)):
        # XXX islice does the wrong thing
        raise NotImplementedError
    
    # XXX need to manifest to detect end of underlying iterator, it seems
    x = tuple(islice(iterable, 0, size))
    while x:
        yield x
        x = tuple(islice(iterable, 0, size))
        
def stringify(it):
    """call str() on each element of it"""
    return imap(str, it)

def exhaust(it):
    """call an iterable to exhaustion"""
    for i in it:
        pass

if __name__ == '__main__':
    writefiles(sys.argv[1], sys.argv[2])