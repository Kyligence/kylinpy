# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from ._source_interface import ColumnInterface
from ._source_interface import SourceInterface


class HiveSource(SourceInterface):
    def __init__(self, name, project, cluster):
        self._name = name
        self.project = project
        self.cluster = cluster
        self.client = cluster.get_client()
        self.__tables_and_columns = None
        self.__tables_in_hive = None

    @property
    def name(self):
        return self._name

    @property
    def schema(self):
        return self.name.split('.')[0]

    @property
    def _tables_and_columns(self):
        if self.__tables_and_columns is None:
            resp = self.client.tables_and_columns.get(
                query_params={'project': self.project},
            ).to_object
            tbl_pair = tuple(
                ('{}.{}'.format(tbl.get('table_SCHEM'), tbl.get('table_NAME')), tbl)
                for tbl in resp)
            for tbl in tbl_pair:
                tbl[1]['columns'] = [(col['column_NAME'], col)
                                     for col in tbl[1]['columns']]
            self.__tables_and_columns = tbl_pair
        return dict(self.__tables_and_columns)

    @property
    def _tables_in_hive(self):
        if self.__tables_in_hive is None:
            tables = self.client.tables.get(
                query_params={
                    'project': self.project,
                    'ext': True,
                },
            ).to_object

            self.__tables_in_hive = {}
            for tbl in tables:
                db = tbl['database']
                name = tbl['name']
                fullname = '{}.{}'.format(db, name)
                self.__tables_in_hive[fullname] = tbl

        return self.__tables_in_hive

    @property
    def columns_map(self):
        if self.cluster.is_pushdown:
            return self._tables_in_hive.get(self.name)
        else:
            return self._tables_and_columns.get(self.name)

    @property
    def columns(self):
        return [_Column(col) for col in self.columns_map.get('columns')]

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
