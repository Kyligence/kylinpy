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
from kylinpy.service import KylinService, KE3Service
from kylinpy.datasource import TableSource, CubeSource
from kylinpy.utils.compat import as_unicode

SERVICES = {
    'v1': KylinService,
    'v2': KE3Service,
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
        self.session = connect_args.get('session', '')
        self.version = connect_args.get('version', 'v1')
        self.is_pushdown = bool(connect_args.get('is_pushdown', False))
        self.is_debug = bool(connect_args.get('is_debug', False))
        self.scheme = 'https' if self.is_ssl else 'http'
        if self.is_debug:
            logging.basicConfig(level=logging.DEBUG)
        self.service = SERVICES[self.version](self.get_client())

    def set_user(self, username, password=None, session=None):
        self.username = username
        if password:
            self.password = password
        else:
            self.session = session

    def get_client(self):
        headers = {
            'User-Agent': 'Kylin Python Client',
        }

        if self.auth == 'basic':
            headers = self.basic_auth(headers)
        else:
            headers = self.session_auth(headers)

        if self.version == 'v2':
            headers = self.set_v2_api(headers)

        return HTTPClient(
            host='{self.scheme}://{self.host}:{self.port}'.format(**locals()),
            prefix=self.prefix,
            timeout=self.timeout,
            request_headers=headers,
            unverified=self.unverified,
            mask_auth=(not self.is_debug),
        )

    def basic_auth(self, headers):
        _headers = headers.copy()
        _auth = as_unicode('{}:{}').format(self.username, self.password)
        _auth = base64.b64encode(_auth.encode('utf-8')).decode('ascii')
        _headers.update({'Authorization': 'Basic {}'.format(_auth)})
        return _headers

    def session_auth(self, headers):
        _headers = headers.copy()
        _headers.update({'Cookie': '{}'.format(self.session)})
        return _headers

    def set_v2_api(self, headers):
        _headers = headers.copy()
        _headers.update({'Accept': 'application/vnd.apache.kylin-v2+json'})
        return _headers

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

    def query(self, sql):
        return self.service.query(sql)

    def get_tables_with_schema(self, scheme=None):
        _full_names = self.get_all_tables()
        if scheme is None:
            return _full_names
        else:
            return list(filter(lambda tbl: tbl.split('.')[0] == scheme, _full_names))

    def get_all_tables(self):
        if self.is_pushdown:
            return list(self.service.tables_in_hive.keys())
        else:
            return list(self.service.tables_and_columns.keys())

    def get_table_source(self, name):
        if self.is_pushdown:
            return TableSource(name, self.service.tables_in_hive.get(name))
        else:
            return TableSource(name, self.service.tables_and_columns.get(name))

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
