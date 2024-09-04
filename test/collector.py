#!/usr/bin/env python3

import os, sys

from pprint import pprint

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from stream import ForkedFeeder, ThreadedFeeder, PCollector, QCollector


## A trivial multiple producers -- single consumer scenario

N = 1000

def producer():
    # TODO: throw an error here to test how robust the collector is
	for x in range(N):
		yield x

def collect(pipe, feeder_class, collector_class, n):
	for _ in range(n):
		producer >> feeder_class(pipe)

	results = collector_class(pipe) >> list
	pprint(results)
	assert len(results) == N * n
	assert set(results) == set(range(N))


## Test cases

def test_PCollector():
	for i in [1, 2, 3, 4]:
		collect(Pipe(), PipeSink, PipeSource, i )

def test_QCollector():
	for i in [1, 2, 3, 4]:
		collect(Queue(), QueueSink, QueueSource, i )


if __name__ == '__main__':
	import nose
	nose.main()
