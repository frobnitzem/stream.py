#!/usr/bin/env python2.6
# -*- coding: utf-8 -*-

import operator

from decimal import Decimal, getcontext
from stream import Stream, stream, seq, gseq, apply, map, fold, item, drop, take

"""
Compute digits of pi using the Gregory series, and its accelerated variants.

Inspired by this section of the wizard book (SICP):
<http://mitpress.mit.edu/sicp/full-text/sicp/book/node72.html>
"""

def _alt_sign(s):
    """Alternate the sign of numbers of the input stream by multiply it with
    the unit alternating series 1, -1, ...
    """
    return zip(s, gseq(-1, initval=1)) >> apply(operator.mul)
alt_sign = Stream(_alt_sign)

def Gregory(dtype=float):
    """Return partial sums of the Gregory series converging to atan(1) == pi/4.

    Yield 1 - 1/3 + 1/5 - 1/7 + ... computed with the given type.
    """
    return seq(dtype(1), step=2) >> map(lambda x: 1/x) >> alt_sign >> fold(operator.add)


series1 = Gregory()

# @Processor
def _Aitken(s):
    """Accelerate the convergence of the a series
    using Aitken's delta-squared process (SICP calls it Euler).
    """
    s0, s1, s2 = s >> item[:3] >> tuple
    while True:
        yield s2 - (s2 - s1)**2 / (s0 - 2*s1 + s2)
        s0, s1, s2 = s1, s2, next(s)

Aitken = Stream(_Aitken)

series2 = Gregory() >> Aitken

@stream
def recur_transform(series, accelerator):
    """Apply a accelerator recursively."""
    s = series
    while True:
        yield next(s)
        s = iter(s >> accelerator)


series3 = Gregory(Decimal) >> recur_transform(Aitken)

# The effect of the recursive transformation is:
#   i = iter(series3)
#   next(i) == Gregory(Decimal) >> item[0]
#   next(i) == Gregory(Decimal) >> drop(1) >> Aitken >> item[0]
#   next(i) == Gregory(Decimal) >> drop(1) >> Aitken >> drop(1) >> Aitken >> item[0]
#   next(i) == Gregory(Decimal) >> drop(1) >> Aitken >> drop(1) >> Aitken >> drop(1) >> Aitken >> item[0]
#   ...
# Note that it is possible to account for the dropped values using
# stream.prepend, but it's not necessary.


if __name__ == '__main__':
    getcontext().prec = 33
    #print(Gregory() >> item[4])
    print('π =', 4 * (series3 >> item[13]))
