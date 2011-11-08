from itertools import chain, imap
from . import reader, parse

def inline(f, wrap):
    dc = reader.decompress(reader.read_blocks(f))
    raw = chain.from_iterable(imap(parse.iterparse, dc))
    return wrap(raw)