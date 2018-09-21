#!/bin/bash
set -e

tox
python setup.py sdist
twine upload dist/*
rm -rf build dist .egg kylinpy.egg-info
