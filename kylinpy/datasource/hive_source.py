# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from ._source_interface import ColumnInterface
from ._source_interface import SourceInterface


class HiveSource(SourceInterface):
    def __init__(self, name, tables_in_hive):
        self._name = name
        self._tables_in_hive = tables_in_hive

    @property
    def name(self):
        return self._name

    @property
    def schema(self):
        return self.name.split('.')[0]

    @property
    def dimensions(self):
        return [_Column(col) for col in self._tables_in_hive.get('columns')]

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
