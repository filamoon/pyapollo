# encoding: utf-8
"""
apollo python 客户端

"""
from setuptools import setup, find_packages

import pyapollo

SHORT = u'pyapollo'

setup(
    name='apollo-client-python',
    version=pyapollo.__version__,
    packages=find_packages(),
    install_requires=[
        'requests'
    ],
    url='https://www.fenqifamily.com',
    author=pyapollo.__author__,
    author_email=pyapollo.__email__,
    classifiers=[
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.7',
    ],
    include_package_data=True,
    package_data={'': ['*.py', '*.pyc']},
    zip_safe=False,
    platforms='any',

    description=SHORT,
    long_description=__doc__,
)
