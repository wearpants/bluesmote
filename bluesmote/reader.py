import zlib
import parse

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