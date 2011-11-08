from itertools import islice, imap

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

identity = lambda x: x