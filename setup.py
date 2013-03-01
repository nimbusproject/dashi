#!/usr/bin/env python

import os
import codecs

# STOP! you MUST also update this in dashi/__init__.py
VERSION = "0.3.0"  # SEE ABOVE LINE BEFORE EDITING THIS
# HEY! did you see the above two lines?

if os.path.exists("README.rst"):
    long_description = codecs.open('README.rst', "r", "utf-8").read()
else:
    long_description = "See http://github.com/nimbusproject/dashi"

setupdict = {
    'name' : 'dashi',
    'version' : VERSION,
    'description' : 'simple RPC layer on top of kombu',
    'long_description' : long_description,
    'license' : 'Apache 2.0',
    'author' : 'Nimbus team',
    'author_email' : 'nimbus@mcs.anl.gov',
    'keywords': ['nimbus','amqp', 'kombu'],
    'classifiers' : [
    'Development Status :: 4 - Beta',
    'Intended Audience :: Developers',
    'License :: OSI Approved :: Apache Software License',
    'Operating System :: OS Independent',
    'Programming Language :: Python',
    "Topic :: Communications",
    "Topic :: System :: Distributed Computing",
    "Topic :: System :: Networking",
    "Topic :: Software Development :: Libraries :: Python Modules"],
    "url" : "http://github.com/nimbusproject/dashi",
    "download_url" : "http://www.nimbusproject.org/downloads/dashi-%s.tar.gz" % VERSION,
}

install_requires = ['kombu>=2.5.0', 'pyyaml']
tests_require = ["nose", "mock"]

from setuptools import setup, find_packages
setupdict['packages'] = find_packages()
setupdict['install_requires'] = install_requires
setupdict['tests_require'] = tests_require
setupdict['extras_require'] = {'test': tests_require}
setupdict['test_suite'] = 'nose.collector'

setup(**setupdict)
