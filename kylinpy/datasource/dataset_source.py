# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import datetime
import hashlib

from kylinpy.exceptions import KylinError
from kylinpy.logger import logger
from ._source_interface import (
    DimensionInterface, MeasureInterface, SourceInterface,
)
from ..utils.sqla_types import kylin_to_sqla

try:
    from sqlalchemy import sql
except ImportError:
    pass


class LoadDatasetError(KylinError):
    pass


class DatasetSource(SourceInterface):
    source_type = 'dataset'

    def __init__(self, dataset_desc, model_desc):
        try:
            self.dataset_name = dataset_desc['dataset_name']
            self.project_name = dataset_desc['project']
            self.dataset_desc = dataset_desc['models'][0]
            self._lm = dataset_desc.get('last_modified')
            self._cm = dataset_desc['calculate_measures']
            self.model_desc = model_desc
        except (TypeError, KeyError, IndexError):
            logger.error('invalid dataset description: {}'.format(self.dataset_name))
            raise LoadDatasetError

    @property
    def name(self):
        return self.dataset_name

    @property
    def fact_table(self):
        fullname = self.dataset_desc.get('fact_table')
        alias = fullname.split('.')[1]
        return _Table(fullname, alias)

    @property
    def _model_lookups(self):
        # lookup tables in model
        return tuple((_.get('alias'), _) for _ in self.model_desc.get('lookups'))

    @property
    def lookups(self):
        # lookup-tables and middle-join tables (snowflake schema)
        lookups_and_joins = {}

        # lookup-tables in cube
        lookups_in_cube = set([d.table.alias for d in self.dimensions]) | self.measure_tables

        # adding index for model lookups
        model_lookups_map = {}
        for (idx, l) in enumerate(self._model_lookups):
            (alias, lookup) = l
            lookup['__idx'] = idx
            model_lookups_map[alias] = lookup

        for tbl in lookups_in_cube:
            join_table = model_lookups_map.get(tbl)
            while join_table:
                if join_table.get('alias') not in lookups_and_joins:
                    lookups_and_joins[join_table.get('alias')] = join_table

                foreign_table = join_table.get('join').get('foreign_key')[0].split('.')[0]
                if foreign_table == self.fact_table.alias:
                    join_table = None
                else:
                    join_table = model_lookups_map.get(foreign_table)

        # sorted by model lookups
        rv = lookups_and_joins.values()
        return tuple(sorted(rv, key=lambda x: x['__idx']))

    @property
    def dimensions(self):
        _dimensions = []
        for dim in self.dataset_desc.get('dimension_tables'):
            table_alias = dim.get('name')
            table = dict(self._model_lookups).get(table_alias)
            table = table.get('table') if table else self.fact_table.fullname
            table_clz = _Table(table, table_alias)

            for col in dim['dim_cols']:
                column = col['name']
                column_alias = col['alias']
                data_type = col['data_type']
                desc = col.get('desc')
                column_clz = _Column(column, column_alias, data_type, desc)
                _dimensions.append(_DatasetDimension(table_clz, column_clz))
        return _dimensions

    @property
    def hierarchies(self):
        _hierarchies = []
        _dim_map = {f'{dim.table.name}.{dim.column.name}': dim for dim in self.dimensions}
        for _table in self.dataset_desc.get('dimension_tables'):
            _hic = _table.get('hierarchys')
            if _hic is None:
                continue
            else:
                for leaf in _hic:
                    _item = {
                        'name': leaf.get('name'),
                        'desc': leaf.get('desc'),
                        'children': [_dim_map.get(f'{_table.get("name")}.{d}') for
                                     d in leaf.get('dim_cols')],
                    }
                    _hierarchies.append(_item)
        return _hierarchies

    @property
    def measures(self):
        _measures = []
        for measure in self.dataset_desc.get('measures'):
            _measures.append(_DatasetMeasure(measure))
        return _measures

    @property
    def calculate_measures(self):
        _measures = []
        for measure in self._cm:
            _measures.append(_DatasetCalculateMeasures(measure))
        return _measures

    @property
    def measure_tables(self):
        _tables = []
        for m in self.measures:
            if m.value_tables:
                _tables.extend(m.value_tables)
        return set(_tables)

    def _get_table_clause(self, tbl_clz):
        table_clause = sql.table(tbl_clz.name)
        table_clause.schema = tbl_clz.scheme
        return sql.alias(table_clause, tbl_clz.alias)

    @property
    def last_modified(self):
        if self._lm:
            return int(self._lm) * 1000
        else:
            return datetime.datetime.utcnow().timestamp() * 1000

    @property
    def identity(self):
        _uid = '{}.{}.SQL'.format(self.project_name, self.dataset_name).encode('utf-8')
        _uid = hashlib.sha1(_uid).hexdigest()
        return _uid

    @property
    def from_clause(self):
        _from_clause = self._get_table_clause(self.fact_table)

        for lookup in self.lookups:
            _join_clause_and = []
            for (idx, pk) in enumerate(lookup['join']['primary_key']):
                fk = lookup['join']['foreign_key'][idx]
                fk_table, fk_column = fk.split('.')
                pk_table, pk_column = pk.split('.')
                fk_table_quoted = sql.quoted_name(fk_table, True)
                fk_column_quoted = sql.quoted_name(fk_column, True)
                pk_table_quoted = sql.quoted_name(pk_table, True)
                pk_column_quoted = sql.quoted_name(pk_column, True)

                pk_column = sql.column(fk_column_quoted,
                                       _selectable=sql.table(fk_table_quoted))
                fk_column = sql.column(pk_column_quoted,
                                       _selectable=sql.table(pk_table_quoted))
                _join_clause_and.append(pk_column == fk_column)

            _lookup = _Table(lookup.get('table'), lookup.get('alias'))
            _is_left_join = lookup['join']['type'].lower() == 'left'
            _from_clause = sql.join(
                left=_from_clause,
                right=self._get_table_clause(_lookup),
                onclause=sql.and_(*_join_clause_and),
                isouter=_is_left_join,
            )
        return _from_clause

    def __repr__(self):
        return '<Dataset Instance: {}>'.format(self.name)


class _DatasetDimension(DimensionInterface):
    def __init__(self, table_clz, column_clz):
        self.table = table_clz
        self.column = column_clz

    @property
    def datatype(self):
        return self.column.datatype

    @property
    def name(self):
        return '{}.{}'.format(self.table.alias, self.column.name)

    @property
    def verbose(self):
        return self.column.alias

    @property
    def desc(self):
        return self.column.desc

    def __repr__(self):
        return '<Dimension: {}.{}>'.format(self.table.alias, self.column.alias)


class _DatasetMeasure(MeasureInterface):
    def __init__(self, description):
        self._description = description

    @property
    def name(self):
        return self._description.get('name')

    @property
    def verbose(self):
        return self._description.get('alias')

    @property
    def measure_type(self):
        return self._description.get('expression')

    @property
    def expression(self):
        column_value = self._description.get('dim_column')
        if column_value == 'constant':
            column_value = 1
        return self._get_aggregations_exp(self.measure_type, column_value)

    @property
    def value_tables(self):
        _column_value = self._description.get('dim_column')
        if _column_value and _column_value != 'constant':
            return [_column_value.split('.')[0]]

    @property
    def desc(self):
        return self._description.get('desc')

    def _get_aggregations_exp(self, aggregations_key, column_value):
        """return aggregations expression with the column value"""
        metrics_expression = {
            'COUNT_DISTINCT': 'COUNT (DISTINCT {})'.format(column_value),
            'COUNT': 'COUNT ({})'.format(column_value),
            'SUM': 'SUM ({})'.format(column_value),
            'AVG': 'AVG ({})'.format(column_value),
            'MIN': 'MIN ({})'.format(column_value),
            'MAX': 'MAX ({})'.format(column_value),
        }
        return metrics_expression.get(aggregations_key)

    def __repr__(self):
        return '<Measure: {}>'.format(self.name)


class _DatasetCalculateMeasures(MeasureInterface):
    def __init__(self, description):
        self._description = description

    @property
    def verbose(self):
        return self._description.get('name')

    @property
    def name(self):
        return self._description.get('name')

    @property
    def measure_type(self):
        return None

    @property
    def value_tables(self):
        return []

    @property
    def expression(self):
        return self._description.get('expression')

    @property
    def desc(self):
        return self._description.get('desc')


class _Table(object):
    def __init__(self, fullname, alias=None):
        self.fullname = fullname
        self.alias = alias

    @property
    def scheme(self):
        return self.fullname.split('.')[0]

    @property
    def name(self):
        return self.fullname.split('.')[1]

    def __repr__(self):
        return ('<Table '
                'name: {self.fullname}, '
                'alias: {self.alias}>').format(**locals())


class _Column(object):
    def __init__(self, name, alias, data_type, desc):
        self.name = name
        self.alias = alias
        self.data_type = data_type
        self.desc = desc

    @property
    def datatype(self):
        return str(kylin_to_sqla(self.data_type))

    def __repr__(self):
        return ('<Column '
                'name: {self.name}, '
                'alias: {self.alias}>').format(**locals())
