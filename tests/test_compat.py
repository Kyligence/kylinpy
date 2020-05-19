# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from datetime import datetime

from kylinpy.utils.compat import to_millisecond_timestamp, to_second_timestamp


def test_to_millisecond_timestamp():
    dt = datetime(2020, 1, 1)
    dt_1 = datetime(2020, 1, 31, 12, 12, 0)
    dt_2 = datetime(1970, 1, 1)
    assert to_millisecond_timestamp(dt) == 1577836800000
    assert to_millisecond_timestamp(dt_1) == 1580472720000
    assert to_millisecond_timestamp(dt_2) == 0


def test_to_second_timestamp():
    dt = datetime(2020, 1, 1)
    dt_1 = datetime(2020, 1, 31, 12, 12, 0)
    dt_2 = datetime(1970, 1, 1)
    assert to_second_timestamp(dt) == 1577836800
    assert to_second_timestamp(dt_1) == 1580472720
    assert to_second_timestamp(dt_2) == 0
