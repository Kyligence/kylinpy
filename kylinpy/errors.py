# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals


class _BaseError(Exception):
    def __init__(self, msg):
        self.msg = msg


class KylinUnauthorizedError(_BaseError):
    pass


class KylinUserDisabled(_BaseError):
    pass


class KylinConnectionError(_BaseError):
    pass


class KylinError(_BaseError):
    pass


class KylinQueryError(_BaseError):
    pass


class KylinConfusedResponse(_BaseError):
    pass


class KylinDBAPIError(_BaseError):
    pass


class KAPOnlyError(_BaseError):
    def __init__(self):
        super(KAPOnlyError, self).__init__(
            'No such API in Apache Kylin, this is KAP only API')
