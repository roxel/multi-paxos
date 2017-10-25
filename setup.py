# -*- coding: utf-8 -*-
import os
import sys
from setuptools import setup, find_packages
from setuptools.command.test import test as TestCommand


README = open(os.path.join(os.path.dirname(__file__), 'README.md')).read()


class PyTest(TestCommand):
    user_options = [
        ('pytest-args=', 'a', "[string] Arguments to pass to py.test")
    ]

    def initialize_options(self):
        self.pytest_args = ""

    def finalize_options(self):
        self.pytest_args = self.pytest_args.split(" ")

    def run(self):
        if self.distribution.install_requires:
            self.distribution.fetch_build_eggs(
                self.distribution.install_requires)
        if self.distribution.tests_require:
            self.distribution.fetch_build_eggs(self.distribution.tests_require)
        self.run_tests()

    def run_tests(self):
        # import here, cause outside the eggs aren't loaded
        import pytest
        errno = pytest.main(self.pytest_args)
        sys.exit(errno)


NAME = 'multi-paxos'
DESCRIPTION = 'Simple distributed key-value store using multi-paxos consensus protocol.'

install_requires = []

tests_require = [
    'pytest==3.2.3',
]

version = '0.0.0.1'
__VERSION__ = version

setup(
    name=NAME,
    version=version,
    packages=find_packages(),
    include_package_data=True,
    license='MIT License',
    description=DESCRIPTION,
    long_description=README,
    url='https://github.com/KamilBugno/multi-paxos.git',
    author='Piotr Roksela',
    classifiers=[
        'Environment :: Web Environment',
        'Intended Audience :: Developers',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3.6',
    ],
    install_requires=install_requires,
    tests_require=tests_require,
    test_suite='tests.test_basic',
)
