from .source_interface import SourceInterface
from .source_interface import ColumnInterface


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
        self.description = description

    @property
    def name(self):
        return self.description.get('name')

    @property
    def datatype(self):
        return self.description.get('datatype')

    def __repr__(self):
        return ('<Column '
                'name: {self.name}>').format(**locals())
