#!/usr/bin/env python3

import os
import sys

from pprint import pprint
from random import randint

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from stream import filter, map, ThreadStream, ProcessStream, append


## The test data

dataset = []

def alternating(n):
    values = []
    for i in range(1, n+1):
        values.append(i)
        values.append(-i)
    return values

def randomized(n):
    maxint = (1<<64)-1
    values = []
    for _ in range(n):
        values.append(randint(-maxint, maxint))
    return values

[10, 100, 1000] >> map(alternating) >> append(dataset)

[10, 100, 1000] >> map(randomized) >> append(dataset)

func = filter(lambda x: x&1)

resultset = dataset >> map(lambda s: s >> func >> set) >> list


## Test scenario

def threadpool(i):
    result = dataset[i] >> ThreadStream(func, poolsize=2) >> set
    pprint(result)
    assert result == resultset[i]

def processpool(i):
    result = dataset[i] >> ProcessStream(func, poolsize=2) >> set
    pprint(result)
    assert result == resultset[i]


## Test cases

def test_ThreadStream():
    for i in range(len(dataset)):
        threadpool(i)

def test_ProcessStream():
    for i in range(len(dataset)):
        processpool(i)


if __name__ == '__main__':
    import nose
    nose.main()
