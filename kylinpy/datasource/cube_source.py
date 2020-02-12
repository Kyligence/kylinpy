# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from ._source_interface import (
    DimensionInterface, MeasureInterface, SourceInterface,
)
from ..utils.sqla_types import kylin_to_sqla

try:
    from sqlalchemy import sql
except ImportError:
    pass


class CubeSource(SourceInterface):
    source_type = 'cube'

    def __init__(self, cube_desc, model_desc, tables_and_columns):
        self.cube_desc = cube_desc
        self.model_desc = model_desc
        self.tables_and_columns = tables_and_columns

    @property
    def name(self):
        return self.cube_name

    @property
    def model_name(self):
        return self.model_desc.get('name')

    @property
    def cube_name(self):
        return self.cube_desc.get('name')

    @property
    def fact_table(self):
        fullname = self.model_desc.get('fact_table')
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
        for dim in self.cube_desc.get('dimensions'):
            table_alias = dim.get('table')
            table = dict(self._model_lookups).get(table_alias)
            table = table.get('table') if table else self.fact_table.fullname
            table_clz = _Table(table, table_alias)

            column = dim['column'] if dim['derived'] is None else dim['derived'][0]
            column_alias = dim['name']
            tbl_map = self.tables_and_columns
            description = dict(tbl_map[table_clz.fullname].get('columns')).get(column)
            column_clz = _Column(column, column_alias, description)

            _dimensions.append(_CubeDimension(table_clz, column_clz))
        return _dimensions

    @property
    def measures(self):
        _measures = []
        for measure in self.cube_desc.get('measures'):
            _measures.append(_CubeMeasure(measure))
        return _measures

    @property
    def measure_tables(self):
        _tables = []
        for m in self.measures:
            if m.value_tables:
                _tables.extend(m.value_tables)
        return set(_tables)

    @property
    def last_modified(self):
        return self.cube_desc.get('last_modified')

    @property
    def identity(self):
        return self.cube_desc.get('uuid')

    def _get_table_clause(self, tbl_clz):
        table_clause = sql.table(tbl_clz.name)
        table_clause.schema = tbl_clz.scheme
        return sql.alias(table_clause, tbl_clz.alias)

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
        return ('<Cube Instance by '
                'model_name: {self.model_name}, '
                'cube_name: {self.cube_name}>').format(**locals())


class _CubeDimension(DimensionInterface):
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

    def __repr__(self):
        return '<Dimension: {}.{}>'.format(self.table.alias, self.column.alias)


class _CubeMeasure(MeasureInterface):
    def __init__(self, description):
        self._description = description
        self._function = description.get('function')
        self._paramter_stack = []

    @property
    def name(self):
        return self._description.get('name')

    @property
    def verbose(self):
        return self._description.get('name')

    @property
    def measure_type(self):
        return self._function.get('expression')

    @property
    def expression(self):
        _params = self._get_parameter_values(self._function.get('parameter'))
        return self._get_aggregations_exp(self.measure_type, _params)

    @property
    def value_tables(self):
        _values_columns = self._get_parameter_values(
            self._function.get('parameter'), 'column')
        if _values_columns:
            _columns = _values_columns.split(', ')
            return set(c.split('.')[0] for c in _columns)
        return None

    def _get_parameter_values(self, paramter_obj, param_type=None):
        """ return a parameter string from cube metrics function object. """
        if paramter_obj.get('next_parameter'):
            self._get_parameter_values(paramter_obj.get('next_parameter'), param_type)
        else:
            if param_type is None:
                self._paramter_stack.append(paramter_obj.get('value'))
            else:
                if paramter_obj.get('type') == param_type:
                    self._paramter_stack.append(paramter_obj.get('value'))
        return ', '.join(self._paramter_stack)

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
    def __init__(self, name, alias, description):
        self.name = name
        self.alias = alias
        self.description = description

    @property
    def datatype(self):
        return str(kylin_to_sqla(self.description.get('type_NAME')))

    def __repr__(self):
        return ('<Column '
                'name: {self.name}, '
                'alias: {self.alias}>').format(**locals())
