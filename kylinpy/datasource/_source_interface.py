# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals


class SourceInterface(object):
    @property
    def name(self):
        raise NotImplementedError

    @property
    def dimensions(self):
        raise NotImplementedError


class ColumnInterface(object):
    @property
    def name(self):
        raise NotImplementedError

    @property
    def datatype(self):
        raise NotImplementedError
