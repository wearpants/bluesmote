from itertools import islice

def chunk(iterable, size):
    """return items from iterable in chunks of size items"""
    if isinstance(iterable, (list, tuple)):
        # XXX islice does the wrong thing
        raise NotImplementedError
    x = islice(iterable, 0, size)
    while x:
        yield x
        x = islice(iterable, 0, size)