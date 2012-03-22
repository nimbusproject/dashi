#!/usr/bin/env python

setupdict = {
    'name' : 'dashi',
    'version' : '0.1',
    'description' : 'simple RPC layer on top of kombu',
    'license' : 'Apache 2.0',
    'author' : 'Nimbus team',
    'author_email' : 'nimbus@mcs.anl.gov',
    'keywords': ['nimbus','amqp', 'kombu'],
    'classifiers' : [
    'Development Status :: 3 - Alpha',
    'Intended Audience :: Developers',
    'License :: OSI Approved :: Apache Software License',
    'Operating System :: OS Independent',
    'Programming Language :: Python',
    "Topic :: Communications",
    "Topic :: System :: Distributed Computing",
    "Topic :: System :: Networking",
    "Topic :: Software Development :: Libraries :: Python Modules"],
}

from setuptools import setup, find_packages
setupdict['packages'] = find_packages()
setupdict['install_requires'] = ['kombu>=2.1.2', 'pyyaml']
setupdict['tests_require'] = ['nose']
setupdict['test_suite'] = 'nose.collector'

setup(**setupdict)
