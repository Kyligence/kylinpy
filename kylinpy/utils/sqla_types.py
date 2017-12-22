import re

from sqlalchemy.types import (
    CHAR,
    VARCHAR,
    INTEGER,
    BIGINT,
    SMALLINT,
    FLOAT,
    DECIMAL,
    BOOLEAN,
    DATE,
    DATETIME,
    TIMESTAMP,
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
    type_re = re.compile('^({})\(?(\d+)?,?(\d+)?\)?$'.format(
        '|'.join(KylinType.keys())
    ), flags=re.IGNORECASE)
    try:
        type_tuple = type_re.match(s).groups()
        _type, _args = type_tuple[0].upper(), [int(e)
                                               for e in type_tuple[1:] if e]
    except AttributeError:
        pass

    return KylinType.get(_type)(*_args)
