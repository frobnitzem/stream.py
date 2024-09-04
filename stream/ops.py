from typing import (
    Optional,
    List,
    TypeVar,
    Generic,
    Union,
    #ParamSpec,
    #Concatenate,
)
from collections.abc import (
    Iterable,
    Iterator,
    Callable,
)
import functools
import collections
import itertools
import functools
import operator
import queue
import heapq

from .core import Source, Stream, Sink, source, stream, sink

_filter = filter
_map = map
_reduce = functools.reduce
_zip = zip

S = TypeVar('S')
T = TypeVar('T')
R = TypeVar('R')

#_______________________________________________________________________
# Process streams by element indices

def take(n : int) -> Stream:
    """Take the first n items of the input stream, return a Stream.

    Params:
        n: the number of elements to be taken
    
    >>> seq(1, 2) >> take(10) >> list
    [1, 3, 5, 7, 9, 11, 13, 15, 17, 19]
    """
    return Stream(itertools.islice, n)


#negative = lambda x: x and x < 0    ### since None < 0 == True

# TODO: turn this back into a strict evaluator
# and put slice-syntax into takes[1:2]
class _ItemTaker:
    """Slice the input stream, return a new Stream.

    >>> i = itertools.count()
    >>> i >> item[:10:2] >> list
    [0, 2, 4, 6, 8]
    >>> i >> item[:5] >> list
    [10, 11, 12, 13, 14]

    >>> range(20) >> item[::-2] >> list
    Traceback (most recent call last):
     ...
    ValueError: Step for islice() must be a positive integer or None.
    """
    @staticmethod
    def __getitem__(key) -> Union[Stream, Callable[[Iterable[S]],S]]:
        if isinstance(key, int):
            if key < 0:
                return last(key)
            return drop(key) >> next
        assert isinstance(key, slice), 'key must be an integer or a slice'
        return Stream(itertools.islice, key.start, key.stop, key.step)

    def __repr__(self):
        return '<itemtaker at %s>' % hex(id(self))

def last(n : Optional[int] = -1) -> Stream:
    """ Return the item n, indexed from the end.

    Params:
        n: index from end of list (0 == end of list, equivalent to n=-1)

    Raises:
        IndexError if the list does not contain enough elements.

    >>> Source(range(5)) >> last(-1)
    4
    >>> Source('abcd') >> last(0) # 0 back from the end
    'd'
    """
    if n >= 0: # last(0) == last(-1), last(1) == last(-2), etc.
        n = n+1
    else:
        n = -n # keep the last -n items
    # since we don't know beforehand when the stream stops
    def ans(inp):
        items = collections.deque(itertools.islice(inp, None), maxlen=n)
        if len(items) == n:
            # items[-n] == items[0]
            return items[0]
        else:
            raise IndexError('list index out of range')
    return ans

item = _ItemTaker()

@stream
def takei(iterator : Iterator[S], indices : Iterable[int]) -> Iterable[S]:
    """Take elements of the input stream by indices.

    Params:
        indices: an iterable of indices to be taken, should yield
                 non-negative integers in monotonically increasing order

    >>> seq() >> takei(range(2, 43, 4)) >> list
    [2, 6, 10, 14, 18, 22, 26, 30, 34, 38, 42]
    """

    indexiter = iter(indices)
    try:
        old_idx = -1
        idx = next(indexiter)                # next value to yield
        for c, elem in enumerate(iterator):
            while idx <= old_idx:            # ignore bad values
                idx = next(indexiter)
            if c == idx:
                yield elem
                old_idx = idx
                idx = next(indexiter)
    except StopIteration:
        pass

def drop(n) -> Stream: # forall S. Stream[S,S]
    """Drop the first n elements of the input stream.

    Args:
        n: the number of elements to be dropped

    >>> seq(0, 2) >> drop(1) >> take(5) >> list
    [2, 4, 6, 8, 10]
    """
    return Stream(itertools.islice, n, None)

@stream
def dropi(iterator : Iterator[S], indices : Iterable[int]) -> Iterator[S]:
    """Drop elements of the input stream by indices.

    Params:
        indices: an iterable of indices to be dropped, should yield
                 non-negative integers in monotonically increasing order

    >>> seq() >> dropi(seq(0,3)) >> item[:10] >> list
    [1, 2, 4, 5, 7, 8, 10, 11, 13, 14]
    >>> "abcd" >> dropi(range(1,3)) >> reduce(lambda a,b: a+b)
    'ad'
    """
    indexiter = iter(indices)

    def next_idx(old_idx):
        # Advance idx until indexiter runs out or we have idx > old_idx
        for idx in indexiter:
            if idx > old_idx:
                return idx, False
        return -1, True

    idx, exhausted = next_idx(-1)
    if not exhausted:
        for c, elem in enumerate(iterator):
            if c != idx:
                yield elem
            else:
                idx, exhausted = next_idx(idx)
                if exhausted:
                    break
    yield from iterator


#_______________________________________________________________________
# Process streams with functions and higher-order ones


def apply(function) -> Stream:
    """Invoke a function using each element of the input stream unpacked as
    its argument list, a la itertools.starmap.

    Params:
        function: to be called with each stream element unpacked as its
                  argument list

    >>> vectoradd = lambda u,v: _zip(u, v) >> apply(lambda x,y: x+y) >> list
    >>> vectoradd([1, 2, 3], [4, 5, 6])
    [5, 7, 9]
    """

    return Stream(functools.partial(itertools.starmap, function))

def map(function) -> Stream:
    """Invoke a function using each element of the input stream as its only
    argument, a la `map`

    Params:
        function: to be called with each stream element as its
                  only argument

    >>> square = lambda x: x*x
    >>> range(10) >> map(square) >> list
    [0, 1, 4, 9, 16, 25, 36, 49, 64, 81]
    """
    return Stream(functools.partial(_map, function))


def filter(function) -> Stream:
    """Filter the input stream, selecting only values which evaluates to True
    by the given function, a la `filter`.

    Params:
        function: to be called with each stream element as its
                  only argument

    >>> even = lambda x: x%2 == 0
    >>> range(10) >> filter(even) >> list
    [0, 2, 4, 6, 8]
    """

    return Stream(functools.partial(_filter, function))


def takewhile(function) -> Stream:
    """Take items from the input stream that come before the first item to
    evaluate to False by the given function, a la itertools.takewhile.

    Params:
        function: to be called with each stream element as its
        only argument
    """
    return Stream(functools.partial(itertools.takewhile, function))


def dropwhile(function) -> Stream:
    """Drop items from the input stream that come before the first item to
    evaluate to False by the given function, a la itertools.dropwhile.

    Params:
        function: to be called with each stream element as its
        only argument
    """
    return Stream(functools.partial(itertools.dropwhile, function))


@stream
def fold(iterator : Iterator[S],
         function : Callable[[T,S], T],
         *initval : T) -> Iterable[T]:
    """Combines the elements of the input stream by applying a function of two
    argument to a value and each element in turn.  At each step, the value is
    set to the value returned by the function, thus it is, in effect, an
    accumulation.
    
    Intermediate values are yielded (similar to Haskell `scanl`).

    This example calculate partial sums of the series 1 + 1/2 + 1/4 +...
    
    >>> gseq(0.5) >> fold(operator.add) >> item[:5] >> list
    [1, 1.5, 1.75, 1.875, 1.9375]
    """
    if len(initval) > 0:
        x = initval[0]
    else:
        x = next(iterator)
    yield x
    for val in iterator:
        x = function(x, val)
        yield x


#_____________________________________________________________________
# Special purpose stream processors


@stream
def chop(iterator : Iterator[S], n : int) -> Iterable[List[S]]:
    """Chop the input stream into segments of length n.

    Params:
        n: the length of the segments

    >>> range(10) >> chop(3) >> list
    [[0, 1, 2], [3, 4, 5], [6, 7, 8], [9]]
    """
    while True:
        #s = Source(iterator) >> item[:n]
        s = iterator >> take(n) >> list
        if s:
            yield s
        else:
            break


class itemcutter:
    """Slice each element of the input stream.

    >>> [range(10), range(10, 20)] >> cut[::2] >> map(list) >> list
    [[0, 2, 4, 6, 8], [10, 12, 14, 16, 18]]
    """

    @staticmethod
    def __getitem__(idx) -> Stream: #[List[T], List | T]:
        return map(operator.methodcaller('__getitem__', idx))
        #return map(methodcaller('__getitem__', *args))
        #return map(lambda x: x[*args])

    def __repr__(self):
        return '<itemcutter at %s>' % hex(id(self))

cut = itemcutter()


#_____________________________________________________________________
# Useful generator functions


@source
def seq(start : T = 0, step : T = 1) -> Iterable[T]:
    """An arithmetic sequence generator.  Works with any type with + defined.

    >>> seq(1, 0.25) >> item[:10] >> list
    [1, 1.25, 1.5, 1.75, 2.0, 2.25, 2.5, 2.75, 3.0, 3.25]
    """
    #return Source(itertools.count(start, step))
    while True:
        yield start
        start += step


@source
def gseq(ratio : T, initval : T = 1) -> Iterable[T]:
    """A geometric sequence generator.  Works with any type with * defined.

    >>> from decimal import Decimal
    >>> gseq(Decimal('.2')) >> item[:4] >> list
    [1, Decimal('0.2'), Decimal('0.04'), Decimal('0.008')]
    """
    x = initval
    while True:
        yield x
        x *= ratio


def repeatcall(func, *args):
    """Repeatedly call func(*args) and yield the result.
    
    Useful when func(*args) returns different results, esp. randomly.
    """
    return Source(itertools.starmap(func, itertools.repeat(args)))


@source
def chaincall(func : Callable[[T],T], initval : T) -> Iterable[T]:
    """Yield initval, func(initval), func(func(initval)), etc.
    
    >>> chaincall(lambda x: 3*x, 2) >> take(10) >> list
    [2, 6, 18, 54, 162, 486, 1458, 4374, 13122, 39366]
    """
    x = initval
    while True:
        yield x
        x = func(x)

#_____________________________________________________________________
# Useful curried versions of __builtin__.{max, min, reduce}


def maximum(key : Optional[Callable[[T],...]] = None) -> Sink[T,T]:
    """
    Curried version of the built-in max.
    
    >>> Source([3, 5, 28, 42, 7]) >> maximum(lambda x: x%28) 
    42
    """
    return Sink(max, key=key)


def minimum(key : Optional[Callable[[T],...]] = None) -> Sink[T,T]:
    """
    Curried version of the built-in min.
    
    >>> Source([[13, 52], [28, 35], [42, 6]]) >> minimum(lambda v: v[0] + v[1])
    [42, 6]
    """
    return Sink(min, key=key)


def reduce(function : Callable[[T,S],T], *arg : T) -> Sink[S,T]:
    """
    Curried version of the built-in reduce.
    
    >>> reduce(lambda x,y: x+y)( [1, 2, 3, 4, 5] )
    15
    """
    return Sink(lambda s: _reduce(function, s, *arg))

# FIXME: create conditional criteria to descent an encapsulation level
@stream
def flatten(iterator):
    """Flatten a nested stream of arbitrary depth.

    >>> (range(i) for i in seq(step=3)) >> flatten >> item[:18] >> list
    [0, 1, 2, 0, 1, 2, 3, 4, 5, 0, 1, 2, 3, 4, 5, 6, 7, 8]
    """
    ## Maintain a LIFO stack of iterators
    stack = []
    i = iterator
    while True:
        try:
            e = next(i)
            if hasattr(e, "__iter__") and not isinstance(e, str):
                stack.append(i)
                i = iter(e)
            else:
                yield e
        except StopIteration:
            try:
                i = stack.pop()
            except IndexError:
                break


#_______________________________________________________________________
# Combine multiple streams

@stream
def prepend(iterator : Iterator[S], addl : Iterable[S]) -> Iterable[S]:
    """Inject values at the beginning of the input stream.

    >>> seq(7, 7) >> prepend(range(0, 10, 2)) >> item[:10] >> list
    [0, 2, 4, 6, 8, 7, 14, 21, 28, 35]
    """
    return itertools.chain(addl, iterator)

@stream
def dup(iterator : Iterator[S], new_source : Source[S]) -> Iterable[S]:
    """Duplicate the source stream onto `new_source`.

    The duplication happens only when the this
    stream segment is connected to a Source.

    Params:
        new_source: Source whose iterator will be replaced.

    >>> foo = Source()
    >>> bar = seq(0, 2) >> dup(foo)
    >>> bar >> item[:5] >> list
    [0, 2, 4, 6, 8]
    >>> foo >> filter(lambda x: x%3 == 0) >> item[:5] >> list
    [0, 6, 12, 18, 24]
    """

    branch1, branch2 = itertools.tee(iterator)
    new_source.setup(branch2)
    yield from branch1


def append(ans : List):
    """Append the contents of the iterator to `ans`.

    Params:
        ans: list to extend with the iterator values.

    >>> ans = []
    >>> "abc" >> append(ans)
    >>> ans
    ['a', 'b', 'c']
    """
    return Sink(ans.extend)


@stream
def tee(iterator, new_sink : Sink):
    """Make a T-split of the input stream, sending
    a copy through to `new_sink`.

    Params:
        new_sink: a function consuming an iterator and performing some action
                  (since the return value is lost)

    >>> ans = []
    >>> foo = filter(lambda x: x%3==0) >> take(5) >> append(ans)
    >>> [1,2,3] >> foo
    >>> ans
    [3]
    >>> bar = seq(0, 2) >> tee(foo)
    >>> bar >> item[:5] >> list
    FIXME: Tee forces stream evaluation.
    [0, 2, 4, 6, 8]
    >>> ans
    [3, 0, 6, 12, 18, 24]
    """
    # TODO: use a pipe here so that there is a chance of
    # both consumers running simultaneously
    #class TeeIter:
    #    def __iter__(self):
    #        return self
    #    def __next__(self):
    #        # next needs to suspend the calling thread...
    #        return next(iterator)
    #return TeeIter()
    print('FIXME: Tee forces stream evaluation.')

    branch1, branch2 = itertools.tee(iterator)
    Source(branch2) >> new_sink # evaluate effect.
    yield from branch1


#_____________________________________________________________________
# iterqueue and iterrecv

@source
def iterqueue(queue) -> Iterable:
    # Turn a either a threading.Queue or a multiprocessing.SimpleQueue
    # into an thread-safe iterator which will exhaust when StopIteration is
    # put into it.
    while True:
        item = queue.get()
        if item is StopIteration:
            # Re-broadcast, in case there is another listener blocking on
            # queue.get().  That listener will receive StopIteration and
            # re-broadcast to the next one in line.
            try:
                queue.put(StopIteration)
            except IOError:
                # Could happen if the Queue is based on a system pipe,
                # and the other end was closed.
                pass
            break
        else:
            yield item

@source
def iterrecv(pipe) -> Iterable:
    # Turn a the receiving end of a multiprocessing.Connection object
    # into an iterator which will exhaust when StopIteration is
    # put into it.  _iterrecv is NOT safe to use by multiple threads.
    while True:
        try:
            item = pipe.recv()
        except EOFError:
            break
        else:
            if item is StopIteration:
                break
            else:
                yield item

def sorter(*inputs : Iterable[Source[S]]) -> Source[S]:
    """Stream / source combinator.
    Merge sorted iterates (smallest to largest) coming from many sources.

    >>> s = sorter(range(13) >> filter(lambda i: i%4==0), range(5,20,2))
    >>> s >> take(5) >> list
    [0, 4, 5, 7, 8]
    >>> s >> list
    [9, 11, 12, 13, 15, 17, 19]
    """
    return Source( heapq.merge(*inputs) )
