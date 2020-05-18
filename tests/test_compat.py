from datetime import datetime

from kylinpy.utils.compat import to_millisecond_timestamp


def test_to_millisecond_timestamp():
    dt = datetime(2020, 1, 1)
    dt_2 = datetime(2020, 1, 31, 12, 12, 0)
    dt_3 = datetime(1970, 1, 1)
    assert to_millisecond_timestamp(dt) == 1577836800000
    assert to_millisecond_timestamp(dt_2) == 1580472720000
    assert to_millisecond_timestamp(dt_3) == 0
