import os
import re

from UserDict import DictMixin

def avg(seq):
    return sum(seq) / float(len(seq))

def same(x): return x

def scale1k(x) : return x / 1000.0

def merge():
    """grab all *.out, extract statistics from there and merge into TSV file """
    # each string is inherently a regex, and those regexes should be mutually
    # exclusive. The order of putting items in fold_functions defines the order
    # of columns
    fold_functions = OrderedDict()
    fold_functions['RunTime']               = max, same
    fold_functions['Throughput']            = sum, same
    fold_functions['Operations']            = sum, same
    fold_functions['Retries']               = sum, same
    fold_functions['Return=0']              = sum, same
    fold_functions['Return=[^0].*']         = sum, same
    fold_functions['AverageLatency']        = avg, scale1k
    fold_functions['MinsLatency']            = min, scale1k
    fold_functions['MaxLatency']            = max, scale1k
    fold_functions['95thPercentileLatency'] = max, same
    fold_functions['99thPercentileLatency'] = max, same
    metrics = fold_functions.keys()
    # specify order and columns for the operation codes
    overall_ops = ['RunTime', 'Throughput']
    other_ops = [x for x in metrics if x not in overall_ops]
    ops = OrderedDict()
    ops['OVERALL'] = overall_ops
    ops['INSERT']  = other_ops
    ops['READ']    = other_ops
    ops['UPDATE']  = other_ops
    ops['CLEANUP']  = other_ops
    ops_keys = ops.keys()
    regexps = map(re.compile, metrics)
    cns = []
    # trying each regexp is slow, therefore
    # we need to obtain searchable prefix to make preprocessing
    prefixes = map(lambda mt: str(re.search('\w+', mt).group(0)), metrics)
    stats = NestedDict()
    # Gets all items in the current directory that end with .out
    items = filter(lambda x: str(x).endswith('.out'), os.listdir('.'))
    pcn = re.compile(r'.*?-c(\d)\.out')
    pln = re.compile(r'\[(\w+)\], (.*?), (\d+(\.\d+)?([eE]\d+)?)')
    # gather stats from all files=items
    for item in items:
        with open(item) as file:
            for line in file:
                for i in range(len(prefixes)):
                    pr = prefixes[i]
                    if pr in line:
                        m1 = (regexps[i]).search(line)
                        m2 = pln.search(line)
                        if m1 and m2:
                            oc = m2.group(1) # operation code
                            # cl = m2.group(2) # column
                            mt = metrics[i]
                            transform = fold_functions[mt][1]
                            if stats[oc][mt]:
                                stats[oc][mt] += transform(float(m2.group(3)))
                            else:
                                stats[oc][mt] = transform(float(m2.group(3)))
    cns.sort()

    for key in stats:
        print(key, '->', stats[key])

def tab_str(seq):
    return '\t'.join(map(str, seq))

class OrderedDict(dict, DictMixin):

    def __init__(self, *args, **kwds):
        if len(args) > 1:
            raise TypeError('expected at most 1 arguments, got %d' % len(args))
        try:
            self.__end
        except AttributeError:
            self.clear()
        self.update(*args, **kwds)

    def clear(self):
        self.__end = end = []
        end += [None, end, end]         # sentinel node for doubly linked list
        self.__map = {}                 # key --> [key, prev, next]
        dict.clear(self)

    def __setitem__(self, key, value):
        if key not in self:
            end = self.__end
            curr = end[1]
            curr[2] = end[1] = self.__map[key] = [key, curr, end]
        dict.__setitem__(self, key, value)

    def __delitem__(self, key):
        dict.__delitem__(self, key)
        key, prev, next = self.__map.pop(key)
        prev[2] = next
        next[1] = prev

    def __iter__(self):
        end = self.__end
        curr = end[2]
        while curr is not end:
            yield curr[0]
            curr = curr[2]

    def __reversed__(self):
        end = self.__end
        curr = end[1]
        while curr is not end:
            yield curr[0]
            curr = curr[1]

    def popitem(self, last=True):
        if not self:
            raise KeyError('dictionary is empty')
        if last:
            key = reversed(self).next()
        else:
            key = iter(self).next()
        value = self.pop(key)
        return key, value

    def __reduce__(self):
        items = [[k, self[k]] for k in self]
        tmp = self.__map, self.__end
        del self.__map, self.__end
        inst_dict = vars(self).copy()
        self.__map, self.__end = tmp
        if inst_dict:
            return (self.__class__, (items,), inst_dict)
        return self.__class__, (items,)

    def keys(self):
        return list(self)

    setdefault = DictMixin.setdefault
    update = DictMixin.update
    pop = DictMixin.pop
    values = DictMixin.values
    items = DictMixin.items
    iterkeys = DictMixin.iterkeys
    itervalues = DictMixin.itervalues
    iteritems = DictMixin.iteritems

    def __repr__(self):
        if not self:
            return '%s()' % (self.__class__.__name__,)
        return '%s(%r)' % (self.__class__.__name__, self.items())

    def copy(self):
        return self.__class__(self)

    @classmethod
    def fromkeys(cls, iterable, value=None):
        d = cls()
        for key in iterable:
            d[key] = value
        return d

    def __eq__(self, other):
        if isinstance(other, OrderedDict):
            if len(self) != len(other):
                return False
            for p, q in  zip(self.items(), other.items()):
                if p != q:
                    return False
            return True
        return dict.__eq__(self, other)

    def __ne__(self, other):
        return not self == other

class NestedDict(dict):
    def __getitem__(self, key):
        if key in self: return self.get(key)
        return self.setdefault(key, NestedDict())

if __name__=='__main__':
    merge()