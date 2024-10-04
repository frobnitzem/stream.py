#!/usr/bin/env python3

import os, sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from stream import ForkedFeeder, ThreadedFeeder, PSorter, QSorter


def test_PSorter():
	sorter = PSorter()
    # TODO: throw an error in the iter function to test robust PSorter behavior
	#ForkedFeeder(lambda: iter(yrange(10))) >> sorter
	ForkedFeeder(lambda: iter(range(10))) >> sorter
	ForkedFeeder(lambda: iter(range(0, 20, 2))) >> sorter
	assert sorter >> list == [0, 0, 1, 2, 2, 3, 4, 4, 5, 6, 6, 7, 8, 8, 9, 10, 12, 14, 16, 18]

def test_QSorter():
	sorter = QSorter()
    # TODO: throw an error in the iter function to test robust QSorter behavior
	#ThreadedFeeder(lambda: iter(zrange(10))) >> sorter
	ThreadedFeeder(lambda: iter(range(10))) >> sorter
	ThreadedFeeder(lambda: iter(range(0, 20, 2))) >> sorter
	assert sorter >> list == [0, 0, 1, 2, 2, 3, 4, 4, 5, 6, 6, 7, 8, 8, 9, 10, 12, 14, 16, 18]


if __name__ == '__main__':
	import nose
	nose.main()
