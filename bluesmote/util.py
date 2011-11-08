from itertools import islice, imap
import operator

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

def knapsackize(l, N):
    """group tuples of (str, int) so that groups have equal sums
    
    XXX this is not actually the knapsack problem
    XXX this an embarassingly ineffecient and sub-optimal solution
    """
    l = sorted(l, key=operator.itemgetter(1), reverse=True)
    out = [[] for i in xrange(N)]
    
    total = sum(i[1] for i in l)
    target = total / float(N)
    
    head = 0
    
    
    def smallest():
        ret = (out[0], sum(i[1] for i in out[0]))
        for x in out[1:]:
            s = sum(i[1] for i in x)
            if s < ret[1]:
                ret = (x, s)
        
        return ret[0]
                
    for x in l:
        smallest().append(x)

    return out