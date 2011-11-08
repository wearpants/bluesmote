from itertools import islice, imap

def chunk(iterable, size):
    """return items from iterable in chunks of size items"""
    if isinstance(iterable, (list, tuple)):
        # XXX islice does the wrong thing
        raise NotImplementedError
    x = islice(iterable, 0, size)
    while x:
        yield x
        x = islice(iterable, 0, size)

def stringify(it):
    """call str() on each element of it"""
    return imap(str, it)

def exhaust(it):
    """call an iterable to exhaustion"""
    for i in it:
        pass