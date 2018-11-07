# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import sys

PY3 = sys.version_info[0] == 3

if PY3:
    string_types = str,
    integer_types = int,
    class_types = type,
    text_type = str
    binary_type = bytes

    def as_unicode(s):
        if isinstance(s, bytes):
            return s.decode('utf-8')
        return str(s)

else:
    string_types = basestring,
    integer_types = (int, long)
    class_types = (type, types.ClassType)
    text_type = unicode
    binary_type = str

    def as_unicode(s):
        if isinstance(s, str):
            return s.decode('utf-8')
        return unicode(s)  # noqa
