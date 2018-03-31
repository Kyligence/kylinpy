from __future__ import absolute_import

import re
import itertools

from sqlalchemy import pool
from sqlalchemy.sql import compiler
from sqlalchemy.engine import default

from .kylindb import KylinDB
from .utils.sqla_types import kylin_to_sqla
from .utils.keywords import CALCITE_KEYWORDS

SUPERSET_KEYWORDS = set([
    '__timestamp',
])


class KylinIdentifierPreparer(compiler.IdentifierPreparer):
    compiler.IdentifierPreparer.reserved_words = set(
        itertools.chain(*[[e.lower(), e] for e in CALCITE_KEYWORDS])
    )
    compiler.IdentifierPreparer.reserved_words.update(SUPERSET_KEYWORDS)

    def __init__(self, dialect, initial_quote='"',
                 final_quote=None, escape_quote='"', omit_schema=True):
        super(KylinIdentifierPreparer, self).__init__(
            dialect, initial_quote, final_quote, escape_quote, omit_schema
        )


class KylinSQLCompiler(compiler.SQLCompiler):

    def __init__(self, *args, **kwargs):
        super(KylinSQLCompiler, self).__init__(*args, **kwargs)

    def _compose_select_body(self, text, select, inner_columns, froms, byfrom, kwargs):
        text = super(KylinSQLCompiler, self)._compose_select_body(
            text, select, inner_columns, froms, byfrom, kwargs)
        # todo
        text = re.sub(' \d{2}:\d{2}:\d{2}', '', text)
        return text


class KylinDialect(default.DefaultDialect):
    name = 'kylin'
    driver = 'kylin'

    statement_compiler = KylinSQLCompiler
    preparer = KylinIdentifierPreparer

    preexecute_pk_sequences = True
    supports_pk_autoincrement = True
    supports_sequences = True
    sequences_optional = True
    supports_native_decimal = True
    supports_default_values = True
    supports_native_boolean = True
    poolclass = pool.SingletonThreadPool
    supports_unicode_statements = True

    default_paramstyle = 'pyformat'

    def __init__(self, *args, **kwargs):
        super(KylinDialect, self).__init__(*args, **kwargs)

    @classmethod
    def dbapi(cls):
        return KylinDB

    def initialize(self, connection):
        self.server_version_info = None
        self.default_schema_name = None
        self.default_isolation_level = None
        self.returns_unicode_strings = True

    def create_connect_args(self, url):
        args = {
            'username': url.username,
            'password': url.password,
            'host': url.host,
            'port': url.port,
            'project': re.sub('/$', '', url.database or 'default'),
            'version': url.query.get('version'),
            'prefix': url.query.get('prefix'),
        }
        return [], dict([e for e in args.items() if e[1]])

    def get_table_names(self, connection, schema=None, **kw):
        conn = connection.connect()
        return conn.connection.connection.get_table_names().get('data')

    def get_schema_names(self, connection, schema=None, **kw):
        conn = connection.connect()
        return conn.connection.connection.list_schemas().get('data')

    def has_table(self, connection, table_name, schema=None):
        return table_name in self.get_table_names(connection, schema)

    def has_sequence(self, connection, sequence_name, schema=None):
        return False

    def get_columns(self, connection, table_name, schema=None, **kw):
        conn = connection.connect()
        columns = conn.connection.connection.get_table_columns(
            table_name).get('data')
        return [{
            'name': col['column_NAME'],
            'type': kylin_to_sqla(col['datatype'])
        } for col in columns]

    def get_foreign_keys(self, connection, table_name, schema=None, **kw):
        return []

    def get_indexes(self, connection, table_name, schema=None, **kw):
        return []

    def get_view_names(self, connection, schema=None, **kw):
        return []

    def get_pk_constraint(self, conn, table_name, schema=None, **kw):
        return {}

    def get_unique_constraints(self, connection, table_name, schema=None, **kw):
        return []
