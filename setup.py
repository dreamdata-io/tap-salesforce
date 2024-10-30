#!/usr/bin/env python

from setuptools import setup

setup(
    name="tap-salesforce",
    version="1.6.0",
    description="Singer.io tap for extracting data from the Salesforce API",
    author="Stitch",
    url="https://singer.io",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["tap_salesforce"],
    install_requires=[
        "requests==2.32.0",
        "singer-python==5.8.1",
        "pydantic==2.4.0",
        "backoff==1.8.0",
        "simplejson",
        "singer",
    ],
    entry_points="""
          [console_scripts]
          tap-salesforce=tap_salesforce:main
      """,
    packages=["tap_salesforce"],
)
