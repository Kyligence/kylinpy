# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from kylinpy.logger import logger


def private_v4_api_warnings(fn):
    def wrapper(*args, **kwargs):
        logger.warning('Use internal v4-API call, this API-CALL is not in KE4 official manual')
        return fn(*args, **kwargs)
    return wrapper
