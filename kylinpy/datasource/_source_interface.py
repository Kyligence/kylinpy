# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals


class SourceInterface(object):
    source_type = None

    def initial(self, *args, **kwargs):
        raise NotImplementedError

    def reflect_datasource_names(self, *args, **kwargs):
        raise NotImplementedError

    @property
    def name(self):
        raise NotImplementedError

    @property
    def columns(self):
        raise NotImplementedError


class ColumnInterface(object):
    @property
    def name(self):
        raise NotImplementedError

    @property
    def datatype(self):
        raise NotImplementedError
