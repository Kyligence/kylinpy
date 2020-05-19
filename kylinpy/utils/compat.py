# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import sys
from datetime import datetime

try:
    # Python 3
    import urllib.request as urllib  # noqa
    from urllib.request import urlparse  # noqa
    from urllib.request import HTTPSHandler  # noqa
    from urllib.parse import urlencode  # noqa
    from urllib.parse import parse_qsl  # noqa
    from urllib.error import HTTPError  # noqa
except ImportError:
    # Python 2
    import urllib2 as urllib  # noqa
    from urlparse import urlparse  # noqa
    from urlparse import parse_qsl  # noqa
    from urllib2 import HTTPError  # noqa
    from urllib2 import HTTPSHandler  # noqa
    from urllib import urlencode  # noqa

PY3 = sys.version_info[0] == 3

if PY3:
    string_types = str
    integer_types = int
    class_types = type
    text_type = str
    binary_type = bytes

    def as_unicode(s):
        if isinstance(s, bytes):
            return s.decode('utf-8')
        return str(s)

else:
    import types
    string_types = basestring  # noqa
    integer_types = (int, long)  # noqa
    class_types = (type, types.ClassType)  # noqa
    text_type = unicode  # noqa
    binary_type = str

    def as_unicode(s):
        if isinstance(s, str):
            return s.decode('utf-8')
        return unicode(s)  # noqa


def to_second_timestamp(dt):
    epoch = datetime(1970, 1, 1)
    return int((dt - epoch).total_seconds())


def to_millisecond_timestamp(dt):
    epoch = datetime(1970, 1, 1)
    return int((dt - epoch).total_seconds() * 1000)


to_seconds = to_second_timestamp
