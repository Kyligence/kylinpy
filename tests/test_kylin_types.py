# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from datetime import date, datetime

import pytest

from kylinpy.exceptions import KylinUnsupportedType
from kylinpy.utils.kylin_types import kylin_to_python


def test_kylin_types():
    with pytest.raises(KylinUnsupportedType):
        raise kylin_to_python('UnsupportedType', 'abc')

    assert kylin_to_python('varchar', 'abc') == 'abc'
    assert kylin_to_python('varchar(256)', 'abc') == 'abc'
    assert kylin_to_python('DECIMAL(20, 6)', '3.1415926') == 3.1415926
    assert kylin_to_python('BIGINT not null', '123456') == 123456
    assert kylin_to_python('INTEGER null', '123456') == 123456
    assert kylin_to_python('DECIMAL(20, 6) NOT null', '123456.123') == 123456.123

    assert kylin_to_python('CHAR', 'abc') == 'abc'
    assert kylin_to_python('VARCHAR', 'abc') == 'abc'
    assert kylin_to_python('STRING', 'abc') == 'abc'
    assert kylin_to_python('DECIMAL', '3.1415926') == 3.1415926
    assert kylin_to_python('DOUBLE', '3.1415926') == 3.1415926
    assert kylin_to_python('FLOAT', '3.1415926') == 3.1415926
    assert kylin_to_python('BIGINT', '3') == 3
    assert kylin_to_python('LONG', '3') == 3
    assert kylin_to_python('INTEGER', '3') == 3
    assert kylin_to_python('INT', '3') == 3
    assert kylin_to_python('TINYINT', '3') == 3
    assert kylin_to_python('SMALLINT', '3') == 3
    assert kylin_to_python('INT4', '3') == 3
    assert kylin_to_python('LONG8', '3') == 3
    assert kylin_to_python('BOOLEAN', 'true') is True
    assert kylin_to_python('BOOLEAN', 'false') is False
    assert kylin_to_python('DATE', '2001-03-10') == date(2001, 3, 10)
    assert kylin_to_python('DATETIME', '2001-03-10 12:12:12') \
        == datetime(2001, 3, 10, 12, 12, 12)
    assert kylin_to_python('TIMESTAMP', '2001-03-10 12:12:12.000000') \
        == datetime(2001, 3, 10, 12, 12, 12)
