#!/usr/bin/env python

from setuptools import setup, find_packages

classifiers = """
Development Status :: 3 - Alpha
Intended Audience :: Developers
License :: OSI Approved :: MIT License
Operating System :: OS Independent
Programming Language :: Python :: 3
Topic :: Software Development :: Libraries :: Python Modules
Topic :: Utilities
"""

__doc__ = """Streams are iterables with a pipelining mechanism to enable data-flow
programming and easy parallelization.

See the reference documentation in the doc/ subdirectory.

The code repository is located at <http://github.com/aht/stream.py>.
"""

requires = [
    "importlib-metadata>=6.8.0"
]

setup(
    name='stream',
    version='1.0.0',
    description=__doc__.split('\n', 1)[0],
	long_description = __doc__,
    author=['Anh Hai Trinh', 'David M. Rogers'],
    author_email=['moc.liamg@hnirt.iah.hna:otliam'[::-1], 'moc.liamg@hcemtatsevitciderp'[::-1]],
	keywords='lazy iterator generator stream pipe parallellization data flow functional list processing',
	url = 'http://github.com/aht/stream.py',
    install_requires = requires,
    #packages=find_packages(),       # Automatically find packages in the directory
    packages=['stream'],             # List of packages to include
    #py_modules=['stream'],          # single python module to include
	classifiers=filter(None, classifiers.split("\n")),
	platforms=['any'],
    python_requires='>=3.3',
)
