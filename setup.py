# encoding: utf-8
"""
pyapollo 常用工具包


"""
from setuptools import setup, find_packages
import pyapollo

SHORT = u'a client for apollo'

setup(
    name='apollo-client',
    version=pyapollo.__version__,
    packages=find_packages(),
    install_requires=[
        'requests'
    ],
    url='',
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
