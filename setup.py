#!/usr/bin/env python
# The Github repo for this project is:
# https://github.com/4dn-dcic/submitpipeline
# IMPORTANT: use Python 2.7 or above for this package

from setuptools import setup, find_packages

setup(
    name="submitpipeline",
    version=open("_version.py").readlines()[-1].split()[-1].strip("\"'"),
    description="scripts for submitting pipelines to 4DN portal",
    url="https://github.com/4dn-dcic/submitpipeline/",
    author="Soo Lee",
    author_email="duplexa@gmail.com",
    license="MIT",
    keywords=['pipeline', 'cwl', '4dn', 'common workflow language',
              'docker', 'tibanna', 'bioinformatics'],
    packages=find_packages(),
    classifiers=[
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3",
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: Scientific/Engineering :: Bio-Informatics"
    ]
)
