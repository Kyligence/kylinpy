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
    def identity(self):
        """ source identity """
        raise NotImplementedError

    @property
    def last_modified(self):
        """ timestamp for sync source """
        raise NotImplementedError


class ColumnInterface(object):
    @property
    def name(self):
        raise NotImplementedError

    @property
    def datatype(self):
        raise NotImplementedError


class DimensionInterface(object):
    @property
    def name(self):
        """ name for SQL query (unique)"""
        raise NotImplementedError

    @property
    def verbose(self):
        """ verbose for identity dimension (unique)"""
        raise NotImplementedError

    @property
    def datatype(self):
        raise NotImplementedError


class MeasureInterface(object):
    @property
    def name(self):
        """ name for SQL query as clause (unique)"""
        raise NotImplementedError

    @property
    def verbose(self):
        """ verbose for identity measure (unique)"""
        raise NotImplementedError

    @property
    def measure_type(self):
        """ type of measure, eg: SUM, COUNT """
        raise NotImplementedError

    @property
    def expression(self):
        """ expression for SQL aggregation"""
        raise NotImplementedError
