from .source_interface import SourceInterface
from .source_interface import ColumnInterface


class CubeSource(SourceInterface):
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
        return self.model_desc.get('fact_table')

    @property
    def lookups(self):
        return tuple((_.get('alias'), _) for _ in self.model_desc.get('lookups'))

    @property
    def dimensions(self):
        _dimensions = []
        for dim in self.cube_desc.get('dimensions'):
            table_alias = dim.get('table')
            table = dict(self.lookups).get(table_alias)
            table = table.get('table') if table else self.fact_table
            table_clz = _Table(table, table_alias)

            column = dim['column'] if dim['derived'] is None else dim['derived'][0]
            column_alias = dim['name']
            tbl_map = dict(self.tables_and_columns)
            description = tbl_map[table_clz.fullname].get('columns').get(column)
            column_clz = _Column(column, column_alias, description)

            _dimensions.append(_CubeDimension(table_clz, column_clz))
        return _dimensions

    def __repr__(self):
        return ('<Cube Instance by '
                'model_name: {self.model_name}, '
                'cube_name: {self.cube_name}>').format(**locals())


class _CubeDimension(ColumnInterface):
    def __init__(self, table_clz, column_clz):
        self.table = table_clz
        self.column = column_clz

    @property
    def datatype(self):
        return self.column.datatype

    @property
    def name(self):
        return self.column.name

    def __repr__(self):
        return '<Dimension: {}.{}>'.format(self.table_clz.alias, self.column_clz.alias)


class _CubeMeasure(object):
    def __init__(self):
        pass

    def __iter__(self):
        pass

    def __repr__(self):
        return '<Measure Instance: >'


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
        return self.description.get('type_NAME')

    def __repr__(self):
        return ('<Column '
                'name: {self.name}, '
                'alias: {self.alias}>').format(**locals())
