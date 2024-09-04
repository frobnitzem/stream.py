"""Lazily-evaluated, parallelizable pipeline.

Overview
========

Streams are iterables with a pipelining mechanism to enable
data-flow programming and easy parallelization.

The idea is to create a `Stream` class with 4 states: 
 - "pure" (no source or sink connected) 
 - "consumer" (sink but no source) 
 - "live" (source but no sink) 
 - "final" (source and sink connected) 

Both "pure" and "consumer" Stream-s maintain no internal
state.  They can be re-used with multiple input streams
to make different results.

Sources can be any iterable.  Sinks can be any callable.
When a source is connected to a sink, it becomes "final".
The sink (or its `__call__` method) will be called, with
the source as its argument.
The sink should iterate over the source
-- advancing the state of the source.
The "final" result evaluates to the return value of the sink.

Data sources can be created with `Source(iterable)`
pure transformers can be created with `Stream(generator, *args, **kws)`,
(where generator does not have an `__iter__` method),
and sinks are any callable taking a stream as an argument.
If you want to repeatedly call `callable(x, *args, **kws)` for each
element in the stream, there is a helper, `SinkFn(callable, *args, **kws)`.

If the sink does not completely consume
the source, then the source can be connected to further
sinks to yield the remaining values from the stream.

Sources and sinks can be distinguished by whether they contain
`__iter__` or `__call__` methods.  Only "live" sources define
`__iter__`.  All Streams and Sinks define `__call__`.  However, calling
a "pure" stream returns another Stream, whereas calling
a "sink" consumes the input and returns the result.

The idea is to take the output of a function that turn an iterable into
another iterable and plug that as the input of another such function.
While you can already do this using function composition, this package
provides an elegant notation for it by overloading the '>>' operator.

This approach focuses the programming on processing streams of data, step
by step.  A pipeline usually starts with a producer, then passes through
a number of filters.  Multiple streams can be branched and combined.
Finally, the output is fed to an accumulator, which can be any function
of one iterable argument.

Producers:  anything iterable
    + from this module:  seq, gseq, repeatcall, chaincall

Filters:
    + by index:  take, drop, takei, dropi
    + by condition:  filter, takewhile, dropwhile
    + by transformation:  apply, map, fold
    + by combining streams:  prepend, tee
    + for special purpose:  chop, cut, flatten

Accumulators:  item, maximum, minimum, reduce
    + from Python:  list, sum, dict, max, min ...
    (anything you can call with an iterable)

Values are computed only when an accumulator forces some or all evaluation
(not when the stream are set up).


Parallelization
===============

All parts of a pipeline can be parallelized using multiple threads or processes.

When a producer is doing blocking I/O, it is possible to use a ThreadedFeeder
or ForkedFeeder to improve performance.  The feeder will start a thread or a
process to run the producer and feed generated items back to the pipeline, thus
minimizing the time that the whole pipeline has to wait when the producer is
blocking in system calls.

If the order of processing does not matter, an ThreadPool or ProcessPool
can be used.  They both utilize a number of workers in other theads
or processes to work on items pulled from the input stream.  Their output
are simply iterables respresented by the pool objects which can be used in
pipelines.  Alternatively, an Executor can perform fine-grained, concurrent job
control over a thread/process pool.

Multiple streams can be piped to a single PCollector or QCollector, which
will gather generated items whenever they are avaiable.  PCollectors
can collect from ForkedFeeder's or ProcessPool's (via system pipes) and
QCollector's can collect from ThreadedFeeder's and ThreadPool's (via queues).
PSorter and QSorter are also collectors, but given multiples sorted input
streams (low to high), a Sorter will output items in sorted order.

Using multiples Feeder's and Collector's, one can implement many parallel
processing patterns:  fan-in, fan-out, many-to-many map-reduce, etc.


Articles
========

Articles written about this module by the author can be retrieved from
<http://blog.onideas.ws/tag/project:stream.py>.

* [SICP](https://mitp-content-server.mit.edu/books/content/sectbyfn/books_pres_0/6515/sicp.zip/index.html)
* [pointfree](https://github.com/mshroyer/pointfree)
* [Generator Tricks for Systems Programmers](http://www.dabeaz.com/generators/Generators.pdf)
"""

import pkg_resources
try:
    __version__ = pkg_resources.get_distribution('stream').version
except Exception:
    __version__ = 'unknown'

from typing import Optional, Callable, Iterable, Iterator, TypeVar, Generic, List
# Note: Iterable has __iter__,
# while Iterator has __next__ (and also __iter__ ~ self)
# so iter : Iterable[T] -> Iterator[T]

import copy
import collections
import heapq
import itertools
import functools
import operator
import queue
import re
import select
import sys
import threading
import time

from operator import itemgetter, attrgetter

_filter = filter
_map = map
_reduce = functools.reduce
_zip = zip

try:
    import multiprocessing
    _nCPU = multiprocessing.cpu_count()
except ImportError:
    _nCPU = 1

from operator import methodcaller


#_____________________________________________________________________
# Base class


class BrokenPipe(Exception):
    pass

S = TypeVar('S')
R = TypeVar('R')
class BaseStream:
    """The operator >> is a synonym for BaseStream.pipe.

    The expression `a >> b` means
      - `b(iter(a)) if hasattr(a, '__iter__')`
      - `Stream(lambda it: b(a(it))) if isinstance(b, Stream)`
      - `Sink(lambda it: b(a(it)))` otherwise
    
    >>> [1, 2, 3] >> map(lambda x: x+1) >> list
    [2, 3, 4]
    """
    @staticmethod
    def pipe(inp, out):
        """Connect inp and out.  If out is not a Stream instance,
        it should be a sink (function callable on an iterable).

        Inp must be either an iterable (i.e. Source) or a Stream (i.e. pure).
        """

        assert hasattr(out, "__call__"), "Cannot compose a stream with a non-callable."
        if hasattr(inp, "__iter__"): # source stream
            return out(iter(inp))    # connect streams
        assert isinstance(inp, Stream), "Input to >> Stream/Sink should be a stream."

        # compose generates only these closures
        @functools.wraps(out)
        def close(iterator):
            return out(iter(inp(iterator)))

        if isinstance(out, Stream):
            return Stream(close)
        return Sink(close)

    def __rshift__(self, outpipe):
        return Stream.pipe(self, outpipe)

    def __rrshift__(self, inpipe):
        return Stream.pipe(inpipe, self)


class Source(Generic[R], BaseStream):
    """A Source is a BaseStream with a connected source.
    It represents a lazy list.  That is stored internally as
    the iterator attribute.

    It defines __iter__(self) for consumers to use.
    """
    iterator : Iterator[R]

    def __init__(self, iterable : Iterable[R] = []) -> None:
        self.setup(iterable)

    def setup(self, iterable : Iterable[R]) -> None:
        self.iterator = iter(iterable)

    def __iter__(self) -> Iterator[R]:
        return self.iterator

    #def __reverse__(self) -> Iterator[R]:
    #    # Using this function is discouraged.
    #    return reverse(self.iterator)

    def extend(self, *other) -> None:
        self.setup( itertools.chain(self.iterator, *other) )

    #def __call__(self, iterator):
    #    #"""Append to the end of iterator."""
    #    return itertools.chain(iterator, self.iterator)

    def __repr__(self):
        return "Source(%s)" % self.iterator

def source(fn):
    """ Handy source decorator that wraps fn with a Source
    class so that it can be used in >> expressions.

    e.g.

    #>>> @source
    #>>> def to_source(rng):
    #>>>    yield from rng
    #>>> to_source(range(4, 6)) >> tuple
    #(4, 5)
    """
    @functools.wraps(fn)
    def gen(*args, **kws):
        return Source(fn(*args, **kws))
    return gen


class Stream(Generic[S,R], BaseStream, Callable[[Iterable[S]], Iterable[R]]):
    """A stream is an iterator-processing function.
    When connected to a data source, it is also a lazy list.
    The lazy list is represented by a Source.
    
    The iterator-processing function is represented by the method
    __call__(iterator).  Since this method is only called
    when a source is actually defined, it always returns
    a Source object.
    
    The `>>` operator is defined from BaseStream.
    """
    def __init__(self,
                 fn : Callable[[Iterable[S]], Iterable[R]],
                 *args, **kws) -> None:
        if not hasattr(fn, "__call__"):
            assert not hasattr(fn, "__iter__"), "Use Source() instead."
            assert hasattr(fn, "__call__"), "Stream function must be callable."
        self.fn = fn
        self.args = args
        self.kws = kws

    def __call__(self, iterator : Iterator[S]) -> Source[R]:
        """Consume the iterator to return a new Source (iterable)."""
        return Source(self.fn(iterator, *self.args, **self.kws))
        # Note: The function should be a generator, so this is equivalent to
        # yield from self.fn(gen, *self.args, **self.kws)

    def __repr__(self):
        if len(self.kws) > 0:
            return 'Stream(%s, *%s, **%s)' % (repr(self.fn),
                                            repr(self.args),
                                            repr(self.kws))
        if len(self.args) > 0:
            return 'Stream(%s, *%s)' % (repr(self.fn), repr(self.args))
        return 'Stream(%s)' % (repr(self.fn),)


class Sink(Generic[S, R], BaseStream):
    def __init__(self, consumer, *args, **kws):
        self.consumer = consumer
        self.args = args
        self.kws = kws

    def __call__(self, iterator : Iterable[S]) -> R:
        """Consume the iterator to yield a final value."""
        return self.consumer(iterator, *self.args, **self.kws)

    def __repr__(self):
        if len(self.kws) > 0:
            return 'Sink(%s, *%s, **%s)' % (repr(self.consumer),
                                            repr(self.args),
                                            repr(self.kws))
        if len(self.args) > 0:
            return 'Sink(%s, *%s)' % (repr(self.consumer), repr(self.args))
        return 'Sink(%s)' % (repr(self.consumer),)

def stream(fn):
    """ Handy stream decorator that wraps fn with a Stream
    class so that it can be used in >> expressions.

    Basically, the first argument to the function becomes implicit.

    e.g.

    #>>> @stream
    #>>> def add_n(it : Iterator[int], n : int):
    #>>>    for i in it:
    #>>>        yield i+n
    #>>> [1] >> add_n(7) >> tuple
    #(8,)
    """
    @functools.wraps(fn)
    def gen(*args, **kws):
        return Stream(fn, *args, **kws)
    return gen


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
    def __getitem__(key) -> Stream | Callable[[Iterable[S]],S]:
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

# TODO: make this a @stream decorator pattern
def takei(indices : Iterable[int]) -> Stream:
    """Take elements of the input stream by indices.

    Params:
        indices: an iterable of indices to be taken, should yield
                 non-negative integers in monotonically increasing order

    >>> seq() >> takei(range(2, 43, 4)) >> list
    [2, 6, 10, 14, 18, 22, 26, 30, 34, 38, 42]
    """

    def gen(iterator, indices):
        indexiter = iter(indices)
        try:
            old_idx = -1
            idx = next(indexiter)                # next value to yield
            counter = iter(seq())
            while 1:
                c = next(counter)
                elem = next(iterator)
                while idx <= old_idx:               # ignore bad values
                    idx = next(indexiter)
                if c == idx:
                    yield elem
                    old_idx = idx
                    idx = next(indexiter)
        except StopIteration:
            pass
    return Stream(gen, indices)

def drop(n) -> Stream:
    """Drop the first n elements of the input stream.

    Args:
        n: the number of elements to be dropped

    >>> seq(0, 2) >> drop(1) >> take(5) >> list
    [2, 4, 6, 8, 10]
    """
    return Stream(itertools.islice, n, None)


def dropi(indices):
    """Drop elements of the input stream by indices.

    Params:
        indices: an iterable of indices to be dropped, should yield
                 non-negative integers in monotonically increasing order

    >>> seq() >> dropi(seq(0,3)) >> item[:10] >> list
    [1, 2, 4, 5, 7, 8, 10, 11, 13, 14]
    >>> "abcd" >> dropi(range(1,3)) >> reduce(lambda a,b: a+b)
    'ad'
    """
    def gen(iterator, indices):
        indexiter = iter(indices)

        counter = iter(seq())
        def try_next_idx():
            ## so that the stream keeps going
            ## after the discard iterator is exhausted
            try:
                return next(indexiter), False
            except StopIteration:
                return -1, True
        old_idx = -1
        idx, exhausted = try_next_idx()                  # next value to discard
        while not exhausted:
            c = next(counter)
            elem = next(iterator)
            while not exhausted and idx <= old_idx:    # ignore bad values
                idx, exhausted = try_next_idx()
            if c != idx:
                yield elem
            elif not exhausted:
                old_idx = idx
                idx, exhausted = try_next_idx()
        yield from iterator
    return Stream(gen, indices)


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


def fold(function, initval=None) -> Stream:
    """Combines the elements of the input stream by applying a function of two
    argument to a value and each element in turn.  At each step, the value is
    set to the value returned by the function, thus it is, in effect, an
    accumulation.
    
    Intermediate values are yielded (similar to Haskell `scanl`).

    This example calculate partial sums of the series 1 + 1/2 + 1/4 +...
    
    >>> gseq(0.5) >> fold(operator.add) >> item[:5] >> list
    [1, 1.5, 1.75, 1.875, 1.9375]
    """
    def gen(iterator, x):
        if x is None:
            x = next(iterator)
        yield x
        for val in iterator:
            x = function(x, val)
            yield x
    return Stream(gen, initval)


#_____________________________________________________________________
# Special purpose stream processors


def chop(n) -> Stream:
    """Chop the input stream into segments of length n.

    Params:
        n: the length of the segments

    >>> range(10) >> chop(3) >> list
    [[0, 1, 2], [3, 4, 5], [6, 7, 8], [9]]
    """
    def gen(iterator, n):
        while True:
            #s = Source(iterator) >> item[:n]
            s = iterator >> take(n) >> list
            if s:
                yield s
            else:
                break
    return Stream(gen, n)

class itemcutter:
    """Slice each element of the input stream.

    >>> [range(10), range(10, 20)] >> cut[::2] >> map(list) >> list
    [[0, 2, 4, 6, 8], [10, 12, 14, 16, 18]]
    """

    @staticmethod
    def __getitem__(args) -> Stream:
        return map(methodcaller('__getitem__', args))
        #return map(methodcaller('__getitem__', *args))
        #return map(lambda x: x[*args])

    def __repr__(self):
        return '<itemcutter at %s>' % hex(id(self))

cut = itemcutter()

#_____________________________________________________________________
# Useful generator functions


def seq(start=0, step=1):
    """An arithmetic sequence generator.  Works with any type with + defined.

    >>> seq(1, 0.25) >> item[:10] >> list
    [1, 1.25, 1.5, 1.75, 2.0, 2.25, 2.5, 2.75, 3.0, 3.25]
    """
    #return Source(itertools.count(start, step))
    def gen(a, d):
        while True:
            yield a
            a += d
    return Source(gen(start, step))


def gseq(ratio, initval=1):
    """A geometric sequence generator.  Works with any type with * defined.

    >>> from decimal import Decimal
    >>> gseq(Decimal('.2')) >> item[:4] >> list
    [1, Decimal('0.2'), Decimal('0.04'), Decimal('0.008')]
    """
    def gen(r, x):
        while True:
            yield x
            x *= r
    return Source(gen(ratio, initval))

def repeatcall(func, *args):
    """Repeatedly call func(*args) and yield the result.
    
    Useful when func(*args) returns different results, esp. randomly.
    """
    return Source(itertools.starmap(func, itertools.repeat(args)))


def chaincall(func, initval):
    """Yield func(initval), func(func(initval)), etc.
    
    >>> chaincall(lambda x: 3*x, 2) >> take(10) >> list
    [2, 6, 18, 54, 162, 486, 1458, 4374, 13122, 39366]
    """
    def gen(x):
        while 1:
            yield x
            x = func(x)
    return Source(gen(initval))

#_____________________________________________________________________
# Useful curried versions of __builtin__.{max, min, reduce}


def maximum(key = None):
    """
    Curried version of the built-in max.
    
    >>> Source([3, 5, 28, 42, 7]) >> maximum(lambda x: x%28) 
    42
    """
    return Sink(max, key=key)


def minimum(key):
    """
    Curried version of the built-in min.
    
    >>> Source([[13, 52], [28, 35], [42, 6]]) >> minimum(lambda v: v[0] + v[1])
    [42, 6]
    """
    return Sink(min, key=key)


def reduce(function, *arg):
    """
    Curried version of the built-in reduce.
    
    >>> reduce(lambda x,y: x+y)( [1, 2, 3, 4, 5] )
    15
    """
    return Sink(lambda s: _reduce(function, s, *arg))

# FIXME: create conditional criteria to descent an encapsulation level
def flattener(iterator):
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

flatten = Stream(flattener)


#_______________________________________________________________________
# Combine multiple streams

@stream
def prepend(it, iterator):
    """Inject values at the beginning of the input stream.

    >>> seq(7, 7) >> prepend(range(0, 10, 2)) >> item[:10] >> list
    [0, 2, 4, 6, 8, 7, 14, 21, 28, 35]
    """
    return itertools.chain(iterator, it)

@stream
def dup(iterator, new_source : Source):
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
# _iterqueue and _iterrecv

@source
def iterqueue(queue):
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
def iterrecv(pipe):
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

# TODO: more functionality here.

#_____________________________________________________________________
# main


if __name__ == "__main__":
    import doctest
    if doctest.testmod()[0]:
        import sys
        sys.exit(1)
