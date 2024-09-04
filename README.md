ABOUT
=====

`streams` gives you types for iterable `Source`-s, iterable-to-iterable
transformations (`Stream`-s), and iterable consumers (`Sink`-s).
These enable data-flow programming, easy parallelization,
and network operations.

See the reference documentation in [doc](doc/index.rst).


INSTALL
=======

This module requires Python 3.6.

    $ pip install stream

or, for development,

    $ pip install -e .


TEST
====
    
    pytest --doctest-modules stream # run doctests
    pytest test/*.py # runs test_* functions



RELEASES
========

See the [changelog](CHANGELOG.md) for a description of new features
in each release.


ROADMAP
=======

1.3: Exception handling for stream.parallel functions.

1.4: stream.parallel.ForkedFeeder Pipe-s read ACK-s from the receiver to guard against sending too much data to a slow receiver

1.5: exchange data between stream.parallel processes using shared memory
