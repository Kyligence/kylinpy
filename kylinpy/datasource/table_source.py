# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from kylinpy.exceptions import NoSuchTableError
from ._source_interface import ColumnInterface
from ._source_interface import SourceInterface


class TableSource(SourceInterface):
    source_type = 'table'

    def __init__(self, name, schema, table_desc):
        if not table_desc:
            raise NoSuchTableError
        self._name = name
        self._schema = schema
        self.table_desc = table_desc

    @property
    def name(self):
        return self._name

    @property
    def schema(self):
        return self._schema

    @property
    def columns_map(self):
        return self.table_desc.get('columns')

    @property
    def columns(self):
        return [_Column(col) for col in self.columns_map]

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
