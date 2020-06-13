# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals


class KylinError(Exception):
    """ Base Exception for Kylin Error """


class KylinUnauthorizedError(KylinError):
    """ Raise when unauthorized error"""


class KylinUserDisabled(KylinError):
    """ Raise when user is disabled """


class NoSuchTableError(KylinError):
    """ Raise when no such table """


class KylinQueryError(KylinError):
    """ Raise when Kylin query error """


class KylinUnsupportedType(KylinError):
    """ Raise when unsupport type in Kylin """


class KylinCubeError(KylinError):
    """ Raise when cube relative operation error """


class KylinJobError(KylinError):
    """ Raise when job relative error """


class KylinModelError(KylinError):
    """ Raise when model relative operation error """


class UnsupportApiError(KylinError):
    """ Raise when use an un-support function"""
