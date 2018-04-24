import codecs
import os
import re

from setuptools import setup


def open_local(paths: list, mode='r', encoding='utf-8'):
    path = os.path.join(
        os.path.abspath(os.path.dirname(__file__)),
        *paths
    )
    return codecs.open(path, mode, encoding)


with open_local(['README.md']) as f:
    long_description = f.read()

with open_local(['cannondb', '__init__.py']) as fp:
    try:
        version = re.findall(r"^__version__ = '([^']+)'\r?$",
                             fp.read(), re.M)[0]
    except IndexError:
        raise RuntimeError('Unable to determine version.')

setup(
    name='cannondb',
    version=version,
    packages=['cannondb'],
    zip_safe=True,
    url='https://github.com/SimonCqk/cannondb',
    license='MIT',
    author='SimonCqk',
    author_email='cqk0100@gmail.com',
    description='CannonDB is a lightweight but powerful key-value database created for human beings.',
    keywords='database key-value python nosql',
    long_description=long_description,
    long_description_content_type='text/markdown',
    install_requires=[
        'rwlock'
    ],
    platforms='any',
    python_requires=">=3.5",
    classifiers=[
        'Development Status :: 3 - Alpha',
        'License :: OSI Approved :: MIT License',
        'Topic :: Database',
        'Topic :: Communications :: Email',
        'Topic :: Database :: Database Engines/Servers',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Programming Language :: Python :: 3',
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: Implementation :: CPython",
        "Programming Language :: Python :: Implementation :: PyPy",
        'Operating System :: MacOS :: MacOS X',
        'Operating System :: Microsoft :: Windows',
        'Operating System :: POSIX :: Linux',
        'Natural Language :: English'
    ]
)
