import sys
import re
import gzip
import os
from itertools import * 
import operator
from collections import namedtuple
from bluesmote.util import chunk, stringify

from bluesmote.record import Record
assert len(Record._fields) == 27

RawRecord = namedtuple("RawRecord", Record._fields[2:])

# some knobs
compression_factor = 5.8
in_bytes_per_line = 380
in_compressed_bytes_per_line = int(in_bytes_per_line / compression_factor)
read_blocksize = int(16384 * in_bytes_per_line / compression_factor)
write_filesize = 10 * 1024 * 1024   # 10Mb
out_bytes_per_line = 427
out_compressed_bytes_per_line = out_bytes_per_line / compression_factor
write_blocksize = write_filesize/out_compressed_bytes_per_line

x = ["(\S+)"]*25
x[9] = "(\S+) " # cs_referrer has an extra trailing space
x[20] = '((?:".*?")|-)' # user-agent
raw_regex = " ".join(x)+r'''\r\n'''
raw_re = re.compile(raw_regex)

def parse(s):
    """parse a single line"""
    m = raw_re.match(s)
    if m:
        r = RawRecord(*m.groups())
        return r._replace(cs_user_agent=r.cs_user_agent.strip('"'), cs_categories=r.cs_categories.strip('"'))
    else:
        return None

def find_logs(root=None):
    root = root if root is not None else os.getcwd()
    for path, dirs, files in os.walk(root):
        for filename in (os.path.abspath(os.path.join(path, filename)) for filename in files
                         if filename.endswith('.log.gz')):
            yield filename

def smart_find_logs(path):
    #path = os.path.abspath(path)
    if os.path.isdir(path):
        return list(find_logs(path))
    else:
        return [path]

def iter_records(path):
    for fname in smart_find_logs(path):
        with gzip.open(fname) as f:
            #if 0: assert isinstance(f, gzip.GzipFile)
            lineno = count()
            lines = f.readlines(read_blocksize)
            while lines:
                for i, l in izip(lineno, lines):
                    r = parse(l)
                    if r:
                        yield Record(fname, str(i), *r) # needs to be ~basename(fname), not abspath
                lines = f.readlines(read_blocksize)
                        
def writefiles(inpath, outpath):
    records = iter_records(inpath)
    for i, lines in enumerate(chunk(records, write_blocksize)):
        fname = os.path.join(outpath, 'block-%04d.log.gz'%i)    
        with gzip.open(fname, 'wb') as f:
            f.writelines(stringify(lines))

if __name__ == '__main__':
    writefiles(sys.argv[1], sys.argv[2])