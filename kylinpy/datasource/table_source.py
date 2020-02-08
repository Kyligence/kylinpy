# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from ._source_interface import ColumnInterface
from ._source_interface import SourceInterface


class TableSource(SourceInterface):
    source_type = 'table'
    service_type = 'kylin'

    def __init__(self, name, table_desc):
        self._name = name
        self.table_desc = table_desc

    @classmethod
    def initial(cls, name, kylin_service, is_pushdown=False):
        if is_pushdown:
            cls(name, kylin_service.tables_in_hive.get(name))
        return cls(name, kylin_service.tables_and_columns.get(name))

    @staticmethod
    def reflect_datasource_names(kylin_service, is_pushdown=False):
        if is_pushdown:
            _full_names = list(kylin_service.tables_in_hive.keys())
        else:
            _full_names = list(kylin_service.tables_and_columns.keys())
        return _full_names

    @property
    def name(self):
        return self._name

    @property
    def schema(self):
        return self.name.split('.')[0]

    @property
    def columns_map(self):
        return self.table_desc.get('columns')

    @property
    def columns(self):
        return [_Column(col) for col in self.columns_map]

    @property
    def identity(self):
        return

    @property
    def last_modified(self):
        return

    def __repr__(self):
        return ('<Hive Instance '
                'table_name: {self.name}>').format(**locals())


class _Column(ColumnInterface):
    def __init__(self, description):
        self.description = description[1] if \
            isinstance(description, tuple) else description

    @property
    def name(self):
        return self.description.get('name') or self.description.get('column_NAME')

    @property
    def datatype(self):
        return self.description.get('datatype') or self.description.get('type_NAME')

    def __repr__(self):
        return ('<Column '
                'name: {self.name}>').format(**locals())
