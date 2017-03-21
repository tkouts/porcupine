"""
Porcupine
"""
import codecs
import os
import re
from setuptools import setup


version = 'dev'
with codecs.open(os.path.join(os.path.abspath(os.path.dirname(
        __file__)), 'porcupine', '__init__.py'), 'r', 'latin1') as fp:
    try:
        version = re.findall(r"^__version__ = '([^']+)'\r?$",
                             fp.read(), re.M)[0]
    except IndexError:
        raise RuntimeError('Unable to determine version.')

setup(
    name='porcupine',
    version=version,
    url='http://github.com/tkouts/porcupine/',
    license='MIT',
    author='Tassos Koutsovassilis',
    author_email='tkouts@innoscript.org',
    description='A web framework',
    packages=['porcupine'],
    scripts=['bin/porcupine'],
    platforms='any',
    install_requires=[
        'sanic>=0.4.1',
        'couchbase>=2.2.2',
        'PyYAML>=3.12',
        'lru-dict>=1.1.6',
    ],
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Environment :: Web Environment',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
    ],
)
