# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import inspect

from kylinpy.exceptions import KylinModelError
from kylinpy.utils.compat import to_millisecond_timestamp
from ._source_interface import (
    DimensionInterface, MeasureInterface, SourceInterface,
)
from ..utils.sqla_types import kylin_to_sqla

try:
    from sqlalchemy import sql
except ImportError:
    pass


class KE4ModelSource(SourceInterface):
    source_type = 'model'

    support_invoke_command = {
        'fullbuild', 'build', 'merge', 'refresh', 'delete', 'list_segment', 'refresh_catalog_cache',
    }

    def __init__(self, model_desc, tables_and_columns, service):
        self.model_desc = model_desc
        self.tables_and_columns = tables_and_columns
        self.service = service

    @property
    def name(self):
        return self.model_name

    @property
    def model_name(self):
        return self.model_desc.get('name')

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
        for dim in self.model_desc.get('simplified_dimensions'):
            table_alias = dim.get('column').split('.')[0]
            table = dict(self._model_lookups).get(table_alias)
            table = table.get('table') if table else self.fact_table.fullname
            table_clz = _Table(table, table_alias)

            column = dim['column'].split('.')[1]
            column_alias = dim['name']
            tbl_map = self.tables_and_columns
            description = dict(tbl_map[table_clz.fullname].get('columns')).get(column)
            if description:
                ke4_dim_id = dim.get('id')
                ke4_dim_status = dim.get('status')
                column_clz = _Column(column, column_alias, description)
                _dimensions.append(_CubeDimension(table_clz, column_clz, ke4_dim_id, ke4_dim_status))
        return _dimensions

    @property
    def measures(self):
        _measures = []
        for measure in self.model_desc.get('simplified_measures'):
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
        return self.model_desc.get('last_modified')

    @property
    def identity(self):
        return self.model_desc.get('uuid')

    @property
    def uuid(self):
        return self.model_desc.get('uuid')

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

    def fullbuild(self):
        return self.service.fullbuild(self.model_name)

    def build(self, start, end):
        _start = to_millisecond_timestamp(start)
        _end = to_millisecond_timestamp(end)
        return self.service.build(self.model_name, str(_start), str(_end))

    def list_segment(self):
        _segments = self.service.list_segment(model_name=self.model_name).get("value")
        if len(_segments) > 0:
            return _segments
        else:
            return []

    def merge(self, ids):
        return self.service.merge(self.model_name, ids)

    def refresh(self, ids):
        return self.service.refresh(self.model_name, ids)

    def delete(self, ids, **kwargs):
        return self.service.delete_segment(self.model_name, ids, **kwargs)

    def invoke_command(self, command, **kwargs):
        fn = getattr(self, str(command), None)
        if (
            fn is None
            or not inspect.ismethod(fn)
            or fn.__name__ not in self.support_invoke_command
        ):
            raise KylinModelError('Unsupported invoke command for datasource: {}'.format(command))

        eager_args = [arg for arg in inspect.getargspec(fn).args if arg != 'self']
        args = {key: kwargs[key] for key in kwargs.keys() if key in eager_args}
        return fn(**args)

    def list_indexes(self):
        return self.service.get_indexes_by_model_uuid(self.uuid)

    def build_indexes(self):
        return self.service.build_indexes(self.model_name)

    def delete_index(self, index_id):
        return self.service.delete_index(self.uuid, index_id)

    def list_index_rules(self):
        return self.service.get_index_rules_by_model_uuid(self.uuid)

    def add_index_rule(self, load_data=False, include=None, mandatory=None, hierarchy=None, joint=None, measure=None):
        # todo: to implement hierarchy/joint/measure
        include = include if include else []
        mandatory = mandatory if mandatory else []
        hierarchy = hierarchy if hierarchy else []
        joint = joint if joint else []
        measure = measure if measure else []
        rules = self.list_index_rules()
        agg_list = rules.get('aggregation_groups', [])
        current_agg = {
            'includes': [m.id for m in self.dimensions if m.name in include],
            'measures': [],
            'select_rule': {
                'hierarchy_dims': [],
                'mandatory_dims': [m.id for m in self.dimensions if m.name in mandatory],
                'joint_dims': [],
            },
        }
        agg_list.append(current_agg)
        return self.service.put_index_rules_by_model_uuid(self.uuid, load_data=load_data, aggregation_groups=agg_list)

    def clear_up_index_rules(self):
        return self.service.put_index_rules_by_model_uuid(self.uuid)

    def refresh_catalog_cache(self, tables):
        return self.service.refresh_catalog_cache(tables)

    def __repr__(self):
        return ('<Model Instance by '
                'model_name: {self.model_name}>').format(**locals())


class _CubeDimension(DimensionInterface):
    def __init__(self, table_clz, column_clz, id, status):
        self.table = table_clz
        self.column = column_clz
        # only in ke4 api
        self.id = id
        # only in ke4 api
        self.status = status

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
        # unused property in KE4
        self._function = description
        self._paramter_stack = []

    @property
    def name(self):
        return self._description.get('name')

    @property
    def verbose(self):
        return self._description.get('name')

    @property
    def measure_type(self):
        return self._description.get('expression')

    @property
    def expression(self):
        _params = self._get_parameter_values(self._description.get('parameter_value'))
        return self._get_aggregations_exp(self.measure_type, _params)

    @property
    def value_tables(self):
        _values_columns = self._get_parameter_values(self._description.get('parameter_value'), 'column')
        if _values_columns:
            _columns = _values_columns.split(', ')
            return set(c.split('.')[0] for c in _columns)
        return None

    def _get_parameter_values(self, paramter_list, param_type=None):
        """ return a parameter string from cube metrics function object. """
        param = paramter_list[0]
        if param_type is None:
            return param.get('value')
        else:
            return param.get('value') if param.get('type') == param_type else None

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
