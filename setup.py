import os

from setuptools import setup


def read(f_name):
    path = os.path.join(os.path.dirname(__file__), f_name)
    return open(path, encoding='utf-8')


with read('README.md') as f:
    long_description = f.read()

with read('cannondb/version.py') as f:
    version = f.read()

setup(
    name='cannondb',
    version=version,
    packages=['cannondb'],
    url='https://github.com/SimonCqk/cannondb',
    license='MIT',
    author='SimonCqk',
    author_email='cqk0100@gmail.com',
    description='CannonDB is a lightweight but powerful key-value database created for human beings.',
    install_requires=['rwlock']
)
