#!/usr/bin/env python

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

def collect(feeder_class, collector_class, n):
	consumer = collector_class()
	for _ in range(n):
		feeder_class(producer) >> consumer
	results = consumer >> list
	pprint(results)
	assert len(results) == N * n
	assert set(results) == set(range(N))


## Test cases

def test_PCollector():
	for i in [1, 2, 3, 4]:
		collect( ForkedFeeder, PCollector, i )

def test_QCollector():
	for i in [1, 2, 3, 4]:
		collect( ThreadedFeeder, QCollector, i )


if __name__ == '__main__':
	import nose
	nose.main()
