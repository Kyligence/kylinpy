# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import base64
import logging
try:
    # Python 3
    from urllib.request import urlparse
    from urllib.parse import parse_qsl
except ImportError:
    # Python 2
    from urlparse import urlparse
    from urlparse import parse_qsl

from kylinpy.client import Client as HTTPClient
from kylinpy.service import KylinService, KE3Service, KE4Service
from kylinpy.datasource import TableSource, CubeSource
from kylinpy.utils.compat import as_unicode

SERVICES = {
    'v1': KylinService,
    'v2': KE3Service,
    'v4': KE4Service,
}


class Cluster(object):
    def __init__(self, host, username=None, password=None, port=7070, **connect_args):
        if host.startswith(('http://', 'https://')):
            _, self.host = host.split('://')
        else:
            self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.auth = connect_args.get('auth', 'basic')
        self.is_ssl = connect_args.get('is_ssl', None)
        self.prefix = connect_args.get('prefix', '/kylin/api')
        self.timeout = connect_args.get('timeout', 30)
        self.unverified = bool(connect_args.get('unverified', True))
        self.version = connect_args.get('version', 'v1')
        self.is_pushdown = bool(connect_args.get('is_pushdown', False))
        self.is_debug = bool(connect_args.get('is_debug', False))
        self.scheme = 'https' if self.is_ssl else 'http'
        if self.is_debug:
            logging.basicConfig(level=logging.DEBUG)
        self.service = SERVICES[self.version](self._get_client())

    def _get_client(self):
        headers = {
            'User-Agent': 'Kylin Python Client',
        }

        if self.version == 'v2':
            headers.update({'Accept': 'application/vnd.apache.kylin-v2+json'})

        if self.version == 'v4':
            headers.update({'Accept': 'application/vnd.apache.kylin-v4+json'})

        if self.username and self.password:
            headers.update(self.basic_auth(self.username, self.password))

        return HTTPClient(
            host='{self.scheme}://{self.host}:{self.port}'.format(**locals()),
            prefix=self.prefix,
            timeout=self.timeout,
            request_headers=headers,
            unverified=self.unverified,
            mask_auth=(not self.is_debug),
        )

    def set_headers(self, headers):
        self.service.client._update_headers(headers)

    def set_user(self, username, password):
        self.username = username
        self.password = password
        auth = self.basic_auth(username, password)
        self.set_headers(auth)

    @staticmethod
    def basic_auth(username, password):
        _auth = as_unicode('{}:{}').format(username, password)
        _auth = base64.b64encode(_auth.encode('utf-8')).decode('ascii')
        return {'Authorization': 'Basic {}'.format(_auth)}

    @property
    def projects(self):
        return self.service.projects

    def __repr__(self):
        dsn = ('<kylinpy instance '
               '{self.scheme}://'
               '{self.username}:{self.password}@'
               '{self.host}:{self.port}>')
        return dsn.format(**locals())


class Project(object):
    def __init__(self, cluster, project):
        self.cluster = cluster
        self.cluster.service.project = project
        self.service = self.cluster.service
        self.is_pushdown = self.cluster.is_pushdown
        self.project = project

    def query(self, sql, **parameters):
        return self.service.query(sql, **parameters)

    def get_all_tables(self, schema=None):
        if self.is_pushdown:
            _full_names = sorted(list(self.service.tables_in_hive.keys()))
        else:
            _full_names = sorted(list(self.service.tables_and_columns.keys()))

        if schema:
            _full_names = [t for t in _full_names if t.split('.')[0] == schema]
        return [t.split('.')[1] for t in _full_names]

    def get_all_schemas(self):
        if self.is_pushdown:
            _full_names = sorted(list(self.service.tables_in_hive.keys()))
        else:
            _full_names = sorted(list(self.service.tables_and_columns.keys()))
        return list(set(t.split('.')[0] for t in _full_names))

    def get_table_source(self, name, schema=None):
        if '.' in name:
            schema, name = name.split('.', 1)
        fullname = '{}.{}'.format(schema, name)
        if self.is_pushdown:
            return TableSource(name, schema, self.service.tables_in_hive.get(fullname))
        else:
            return TableSource(name, schema, self.service.tables_and_columns.get(fullname))

    def get_cube_source(self, name):
        cube_desc = self.service.cube_desc(name)
        model_name = cube_desc.get('model_name')
        return CubeSource(
            cube_desc=cube_desc,
            model_desc=self.service.model_desc(model_name),
            tables_and_columns=self.service.tables_and_columns,
        )

    def __str__(self):
        return str(self.cluster) + '/' + self.project

    def __repr__(self):
        return '<Kylin Project Instance: {}>'.format(self.project)


def dsn_proxy(dsn):
    url = urlparse(dsn)
    project = url.path.lstrip('/')
    port = url.port or 7070
    query = dict(parse_qsl(url.query) or {})
    cluster = Cluster(url.hostname, url.username, url.password, port, **query)
    if project:
        return Project(cluster, project)
    return cluster
