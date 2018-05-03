import re
import six
import time
from datetime import datetime
from ..logger import logger

true_pattern = re.compile(r"true", flags=re.IGNORECASE)

KylinType = dict(
    CHAR=six.text_type,
    VARCHAR=six.text_type,
    STRING=six.text_type,
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
    DATE=lambda x: datetime.strptime(x, "%Y-%m-%d").date(),
    DATETIME=lambda x: datetime.strptime(x, "%Y-%m-%d %H:%M:%S"),
    TIMESTAMP=lambda x: datetime.strptime(x.split('.')[0], "%Y-%m-%d %H:%M:%S"),
)


def kylin_to_python(_type, s):
    try:
        return KylinType.get(str(_type).upper())(s) if s else s
    except KeyError:
        logger.error(
            "CONVERT ERROR, type: {}, raw string: {}".format(_type, s))
