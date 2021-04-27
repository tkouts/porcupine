"""
Porcupine
"""
import codecs
import os
import re
from setuptools import setup, find_packages


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
    packages=find_packages(exclude=['tests', 'tests.*']),
    package_data={'porcupine.apps': ['*.yml']},
    scripts=['bin/porcupine'],
    platforms='any',
    install_requires=[
        'sanic==19.6.0',
        'couchbase==3.0.10',
        'PyYAML==5.1.1',
        'lru-dict==1.1.6',
        'namedlist==1.7',
        'cbor==1.0.0',
        'pendulum==2.1.2',
        'orjson==3.4.5',
        'aiofiles==0.6.0',
        'aiostream==0.3.1',
        'aiocron==1.3',
        'mmh3==2.5.1',
        'validate-email==1.3',
        'sly==0.3',
        'sortedcontainers==2.1.0',
    ],
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Environment :: Web Environment',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
    ],
)
