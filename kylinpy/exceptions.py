# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals


class KylinError(Exception):
    pass


class KylinUnauthorizedError(KylinError):
    pass


class KylinUserDisabled(KylinError):
    pass


class NoSuchTableError(KylinError):
    pass


class KylinQueryError(KylinError):
    pass
