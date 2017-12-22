from __future__ import absolute_import

from .logger import logger


class _BaseError(Exception):
    def __init__(self, msg):
        logger.debug('''
==========================[RAW ERROR]===============================
 ERROR: %s \n
==========================[RAW ERROR]===============================
        ''', msg)
        self.msg = msg or ''
        super(_BaseError, self).__init__(msg)

    def __str__(self):
        return self.msg


class KylinUnauthorizedError(_BaseError):
    """Exception raised by unauthorized"""


class KylinUserDisabled(_BaseError):
    """Exception raised by unauthorized"""


class KylinConnectionError(_BaseError):
    """Exception raised by connection refused"""


class KylinError(_BaseError):
    """Exception raised by otherwise"""


class KylinConfusedResponse(_BaseError):
    """Exception raised by confused response"""


class KylinDBAPIError(_BaseError):
    """Exception raised dbapi error"""


class KAPOnlyError(_BaseError):
    def __init__(self):
        super(KAPOnlyError, self).__init__(
            'No such API in Apache Kylin, this is KAP only API')
