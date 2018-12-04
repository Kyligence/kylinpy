# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import re

from sqlalchemy.types import (
    BIGINT,
    BOOLEAN,
    CHAR,
    DATE,
    DATETIME,
    DECIMAL,
    FLOAT,
    INTEGER,
    SMALLINT,
    TIMESTAMP,
    VARCHAR,
)

KylinType = dict(
    CHAR=CHAR,
    STRING=VARCHAR,
    VARCHAR=VARCHAR,
    DECIMAL=DECIMAL,
    DOUBLE=FLOAT,
    FLOAT=FLOAT,
    BIGINT=BIGINT,
    LONG=BIGINT,
    INTEGER=INTEGER,
    INT=INTEGER,
    TINYINT=SMALLINT,
    SMALLINT=SMALLINT,
    INT4=BIGINT,
    LONG8=BIGINT,
    BOOLEAN=BOOLEAN,
    DATE=DATE,
    DATETIME=DATETIME,
    TIMESTAMP=TIMESTAMP,
)


def kylin_to_sqla(s):
    type_re = re.compile(  # noqa
        '^({})\(?(\d+)?,?(\d+)?\)?.*$'.format('|'.join(KylinType.keys())),  # noqa
        flags=re.IGNORECASE)   # noqa
    type_tuple = type_re.match(s).groups()
    _type, _args = type_tuple[0].upper(), [int(e) for e in type_tuple[1:] if e]
    return KylinType.get(_type)(*_args)
