#!/usr/bin/env python
"""output all lines where sc_filter_result != OBSERVED"""

from bluesmote import parse, reader
from bluesmote.util import chunk 
import sys
import os
from itertools import chain, imap

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

def main(input, output):
    intern("OBSERVED")
    with open(output, 'wb') as outfile:
        for fname in smart_find_logs(input):
            print "Processing", fname
            with open(fname, 'rb') as f:
                dc = reader.decompress(reader.read_blocks(f))
                raw = chain.from_iterable(imap(parse.iterparse, dc))
                results = (r for r in raw if r.sc_filter_result != "OBSERVED")
                strings = ("%s\r\n"%str(r) for r in results)
                outfile.writelines(strings)

if __name__ == '__main__':
    main(sys.argv[1], sys.argv[2])