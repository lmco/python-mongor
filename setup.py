#!/usr/bin/python
from setuptools import setup, find_packages

setup(
    name = "python-pymongor",
    version = "0.4",
    maintainer = "Daniel Bauman",
    maintainer_email = "Daniel.Bauman@lmco.com",
    description = ("A utility to curate mongo databases"),
    license = "",
    keywords = "",
    url = "",
    install_requires = ["pymongo>=3.0"],
    packages=find_packages(),
)
