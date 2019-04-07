# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from kylinpy.utils.sqla_types import kylin_to_sqla


def test_string():
    string_obj = kylin_to_sqla('CHAR(64) hello this is long descrition')
    assert string_obj.length == 64

    string_obj = kylin_to_sqla('STRING(64) hello this is long descrition')
    assert string_obj.length == 64

    string_obj = kylin_to_sqla('VARCHAR(64) hello this is long descrition')
    assert string_obj.length == 64


def test_float():
    decimal_obj = kylin_to_sqla('DECIMAL(20,6)')
    assert decimal_obj.precision == 20
    assert decimal_obj.scale == 6

    decimal_obj = kylin_to_sqla('DOUBLE(20)')
    assert decimal_obj.precision == 20

    decimal_obj = kylin_to_sqla('FLOAT(20)')
    assert decimal_obj.precision == 20


def test_int():
    assert str(kylin_to_sqla('BIGINT')) == 'BIGINT'
    assert str(kylin_to_sqla('INTEGER')) == 'INTEGER'
    assert str(kylin_to_sqla('INT')) == 'INTEGER'
    assert str(kylin_to_sqla('TINYINT')) == 'SMALLINT'
    assert str(kylin_to_sqla('SMALLINT')) == 'SMALLINT'
    assert str(kylin_to_sqla('INT4')) == 'BIGINT'
    assert str(kylin_to_sqla('LONG8')) == 'BIGINT'


def test_others():
    assert str(kylin_to_sqla('BOOLEAN')) == 'BOOLEAN'
    assert str(kylin_to_sqla('DATE')) == 'DATE'
    assert str(kylin_to_sqla('DATETIME')) == 'DATETIME'
    assert str(kylin_to_sqla('TIMESTAMP')) == 'TIMESTAMP'
