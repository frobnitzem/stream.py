""" Parallel patterns are created by
    sending stream output to pipes / queues / sockets
    and receiving stream inputs from same.

    
"""
from typing import (
    Optional,
    TypeVar,
    Union,
    #ParamSpec,
    #Concatenate,
)
from collections.abc import (
    Iterable,
    Iterator,
    Callable,
)

import itertools
import heapq
import queue

import select
import sys
import threading
import time

_map = map

from .core import Source, Stream, Sink, source, stream, sink
from .ops import map, iterrecv, iterqueue

#try:
import multiprocessing
_nCPU = multiprocessing.cpu_count()
Pipe = multiprocessing.Pipe
AnyQueue = Union[queue.Queue, multiprocessing.SimpleQueue]
#except ImportError:
#    _nCPU = 1

S = TypeVar('S')
T = TypeVar('T')

#_____________________________________________________________________
# Threaded/forked feeder

@source
def ThreadedSource(generator : Callable[..., Iterator[S]],
                   *args, maxsize=1024, **kwargs
                  ) -> Iterable[S]:
    """Create a Source that starts the given generator with
    *args and **kwargs in a separate thread.  The thread will
    eagerly evaluate the stream and send it to consumers.

    Note: generator, args, and kwargs are all pickled
    and sent to the thread for processing, so this will only
    speedup the actual operations done during stream generation,
    not creation of the stream.
    
    This should improve performance when the generator often
    blocks in system calls.
    """
    outqueue = queue.Queue(maxsize)
    def feeder():
        for x in generator(*args, **kwargs):
            outqueue.put(x)
        outqueue.put(StopIteration)
    thread = threading.Thread(target=feeder)
    thread.start()
    
    yield from iterqueue(outqueue)

    thread.join()

# TODO: exchange data using shared memory
# TODO: read ACK-s from the receiver to guard against sending too much data to a slow receiver
@source
def ProcessSource(generator : Callable[..., Iterator[S]], 
                 *args, maxsize=0, **kwargs
                ) -> Iterable[S]:
    """Create a feeder that starts the given generator with
    *args and **kwargs in a child process. The feeder will
    act as an eagerly evaluating proxy of the generator.
    
    The feeder can then be streamed to other processes.
        
    This should improve performance when the generator often
    blocks in system calls.  Note that serialization could
    be costly.
    """
    outpipe, inpipe = Pipe(duplex=False)
    def feed():
        for x in generator(*args, **kwargs):
            inpipe.send(x)
        inpipe.send(StopIteration)
    process = multiprocessing.Process(target=feed)
    process.start()
    
    yield from iterrecv(outpipe)

    process.join()


#_____________________________________________________________________
# Asynchronous stream processing using a pool of threads or processes

@stream
def parallel(iterator : Iterator[S],
             worker_stream : Stream[S,T],
             typ="thread", poolsize=_nCPU) -> Iterable[T]:
    """
    Stream combinator taking a worker_stream and executing it in parallel.

    Params:
        worker_stream: a Stream object to be run on each thread/process
        typ: either "thread" or "process" indicating whether to use
             thread or process-based parallelism

    >>> range(10) >> ThreadStream(map(lambda x: x*x)) >> sum
    285
    >>> range(10) >> ProcessStream(map(lambda x: x*x)) >> sum
    285
    """

    if typ == "thread":
        Qtype = queue.Queue
        recvfrom = iterqueue
        start = threading.Thread
    else:
        Qtype = multiprocessing.SimpleQueue
        recvfrom = iterqueue
        start = multiprocessing.Process
    
    inqueue  = Qtype()
    outqueue = Qtype()
    #failqueue = Qtype()
    #failure = Source(recvfrom(failqueue))
    def work():
        try:
            for ans in (recvfrom(inqueue) >> worker_stream):
                #yield ans
                outqueue.put(ans)
        except Exception as e:
            #failqueue.put((next(dupinput), e))
            #outqueue.put(e)
            raise
    workers = []
    for _ in range(poolsize):
        t = start(target=work)
        workers.append(t)
        t.start()
    def cleanup():
        # Wait for all workers to finish,
        # then signal the end of outqueue and failqueue.
        for t in workers:
            t.join()
        outqueue.put(StopIteration)
    cleaner_thread = threading.Thread(target=cleanup)
    cleaner_thread.start()
    
    def feed():
        for item in iterator:
            inqueue.put(item)
        inqueue.put(StopIteration)
    feeder_thread = threading.Thread(target=feed)
    feeder_thread.start()

    yield from iterqueue(outqueue)

    feeder_thread.join()
    cleaner_thread.join()

def ThreadStream(worker_stream : Stream[S,T], poolsize=_nCPU*4) -> Stream[S,T]:
    return parallel(worker_stream, typ="thread", poolsize=poolsize)

def ProcessStream(worker_stream : Stream[S,T], poolsize=_nCPU) -> Stream[S,T]:
    return parallel(worker_stream, typ="process", poolsize=poolsize)


#___________
# Collectors

@source
def PCollector(inpipes : Iterable[Pipe]) -> Iterator:
    """Collect items from many ProcessSource's or ProcessStream's.
    """
    inpipes = [p for p in inpipes]
    while inpipes:
        ready, _, _ = select.select(self.inpipes, [], [])
        for inpipe in ready:
            item = inpipe.recv()
            if item is StopIteration:
                inpipes.pop(inpipes.index(inpipe))
            else:
                yield item


@source
def _PCollector(inpipes : Iterable[Pipe], waittime : float = 0.1) -> Iterator:
    """Collect items from many ProcessSource's or ProcessStream's.

    All input pipes are polled individually.  When none is ready, the
    collector sleeps for a fix duration before polling again.

    Params:
        waitime: the duration that the collector sleeps for
                 when all input pipes are empty
    """
    inpipes = [p for p in inpipes]
    while inpipes:
        ready = [p for p in inpipes if p.poll()]
        if not ready:
            time.sleep(waittime)
        for inpipe in ready:
            item = inpipe.recv()
            if item is StopIteration:
                inpipes.pop(inpipes.index(inpipe))
            else:
                yield item


if sys.platform == "win32":
    PCollector = _PCollector


@source
def QCollector(inqueues : Iterable[AnyQueue], waittime : float = 0.1) -> Iterator:
    """Collect items from many ThreadedSource's or ThreadStream's.
    
    All input queues are polled individually.  When none is ready, the
    collector sleeps for a fix duration before polling again.

    Params:
        waitime: the duration that the collector sleeps for
                 when all input pipes are empty
    """
    inqueues = [q for q in inqueues]
    while inqueues:
        ready = [q for q in inqueues if not q.empty()]
        if not ready:
            time.sleep(waittime)
        for q in ready:
            item = q.get()
            if item is StopIteration:
                inqueues.pop(inqueues.index(inqueues))
            else:
                yield item
