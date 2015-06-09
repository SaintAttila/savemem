#!/usr/bin/env python

"""Setup script for savemem."""

__author__ = 'Aaron Hosford'

from setuptools import setup
from os import path

from savemem import __version__

here = path.abspath(path.dirname(__file__))

long_description = """
Disk-backed container types that permit limits on RAM use to be specified.
Container types defined here are not meant to be persistent, but rather to be
memory efficient by effectively taking advantage disk storage. They should act
as drop-in replacements for containers in situations where RAM is at a premium
and disk space is not.
"""

setup(
    name='savemem',
    version=__version__,
    description='savemem - Disk-backed non-persistent containers for memory efficiency',
    long_description=long_description,
    url='https://github.com/SaintAttila/savemem',
    author=__author__,
    author_email='hosford42@gmail.com',
    license='MIT',

    # See https://pypi.python.org/pypi?%3Aaction=list_classifiers
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Topic :: Utilities',
        'License :: OSI Approved :: MIT License',

        # Specify the Python versions you support here. In particular, ensure
        # that you indicate whether you support Python 2, Python 3 or both.
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
    ],

    keywords='memory usage containers dictionary dict resource management RAM',
    py_modules=['savemem'],
)
