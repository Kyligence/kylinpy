# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from datetime import datetime
import re

from kylinpy.exceptions import KylinUnsupportedType
from kylinpy.logger import logger
from kylinpy.utils.compat import text_type

true_pattern = re.compile(r'^true$', flags=re.IGNORECASE)

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


def _convert_type(_type):
    # remove parenthesis, such as 'DECIMAL(2, 5)'
    _type = re.sub(r'\(.*\)', '', _type)
    # remove some decorator in type string, such as 'BIGINT NOT NULL'
    _type = re.sub(r'\s.*', '', _type)
    return str(_type).upper()


def kylin_to_python(_type, s):
    try:
        return KylinType[_convert_type(_type)](s) if s else s
    except KeyError:
        logger.error(
            'CONVERT ERROR, type: {}, raw string: {}'.format(_type, s))
        raise KylinUnsupportedType(_type)
