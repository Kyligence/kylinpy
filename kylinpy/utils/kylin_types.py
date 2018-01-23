import six
import time
from datetime import datetime
from ..logger import logger

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
    BOOLEAN=bool,
    DATE=lambda x: datetime.strptime(x, "%Y-%m-%d").date(),
    DATETIME=lambda x: datetime.strptime(x, "%Y-%m-%d %H:%M:%S"),
    TIMESTAMP=lambda x: time.mktime(datetime.strptime(
        x, "%Y-%m-%d %H:%M:%S").timetuple()),
)


def kylin_to_python(_type, s):
    try:
        return KylinType.get(str(_type).upper())(s) if s else s
    except KeyError:
        logger.error(
            "CONVERT ERROR, type: {}, raw string: {}".format(_type, s))
