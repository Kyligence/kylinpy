#!/bin/sh

# Runs flake8 in the configuration used for kylinpy.
#
# E501 is "line longer than 80 chars" but the automated fix is ugly.
flake8 --ignore=E501
