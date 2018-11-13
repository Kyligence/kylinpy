# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from datetime import datetime
import re

from .compat import text_type
from ..logger import logger

true_pattern = re.compile(r'true', flags=re.IGNORECASE)

KylinType = dict(
    CHAR=text_type,
    VARCHAR=text_type,
    STRING=text_type,
    DECIMAL=float,
    DOUBLE=float,
    FLOAT=float,
    BIGINT=int,
    LONG=int,
    INTEGER=int,
    INT=int,
    TINYINT=int,
    SMALLINT=int,
    INT4=int,
    LONG8=int,
    BOOLEAN=lambda x: bool(re.search(true_pattern, x)),
    DATE=lambda x: datetime.strptime(x, '%Y-%m-%d').date(),
    DATETIME=lambda x: datetime.strptime(x, '%Y-%m-%d %H:%M:%S'),
    TIMESTAMP=lambda x: datetime.strptime(x.split('.')[0], '%Y-%m-%d %H:%M:%S'),
)


def kylin_to_python(_type, s):
    try:
        return KylinType.get(str(_type).upper())(s) if s else s
    except KeyError:
        logger.error(
            'CONVERT ERROR, type: {}, raw string: {}'.format(_type, s))
