#!/bin/bash
set -e

tox
python setup.py sdist
twine upload dist/*
python setup.py clean
rm -rf dist
