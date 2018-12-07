# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import base64
try:
    # Python 3
    import urllib.request as urllib
except ImportError:
    # Python 2
    import urllib2 as urllib

from .client import Client as HTTPClient
from .datasource import CubeSource, HiveSource
from .utils.compat import as_unicode


class KylinClient(object):
    def __init__(self, host, username, password, port=7070, **connect_args):
        if host.startswith(('http://', 'https://')):
            _, self.host = host.split('://')
        else:
            self.host = host
        self.port = port
        self.username = username
        self.password = password
        auth = connect_args.get('auth', 'basic')
        is_ssl = connect_args.get('is_ssl', None)
        prefix = connect_args.get('prefix', 'kylin/api')
        timeout = connect_args.get('timeout', 30)
        unverified = connect_args.get('unverified', True)
        self.version = connect_args.get('version', 'v1')
        self.is_pushdown = connect_args.get('is_pushdown', False)
        self.scheme = 'https' if is_ssl else 'http'

        headers = {
            'User-Agent': 'Kylin Python Client',
        }

        if auth == 'basic':
            headers = self.basic_auth(headers)

        if self.version == 'v2':
            headers = self.set_v2_api(headers)

        self.client = HTTPClient(
            host='{self.scheme}://{self.host}:{self.port}'.format(**locals()),
            prefix=prefix,
            timeout=timeout,
            request_headers=headers,
            unverified=unverified,
        )

    def basic_auth(self, headers):
        _headers = headers.copy()
        _auth = as_unicode('{}:{}').format(self.username, self.password)
        _auth = base64.b64encode(_auth.encode('utf-8')).decode('ascii')
        _headers.update({'Authorization': 'Basic {}'.format(_auth)})
        return _headers

    def set_v2_api(self, headers):
        _headers = headers.copy()
        _headers.update({'Accept': 'application/vnd.apache.kylin-v2+json'})
        return _headers

    def __repr__(self):
        dsn = ('<kylinpy instance '
               '{self.scheme}://'
               '{self.username}:{self.password}@'
               '{self.host}:{self.port}>')
        return dsn.format(**locals())


class Project(object):
    def __init__(self, host, username, password, port=7070, project='default',
                 **connect_args):
        self._client = KylinClient(host, username, password, port, **connect_args)
        self.client = self._client.client
        self.is_pushdown = self._client.is_pushdown
        self.project = project
        self.__tables_and_columns = None
        self.__tables_in_hive = None
        self.__cubes = None
        self.__models = None

    def query(self, sql, limit=50000, offset=0, acceptPartial=False):
        request_body = {
            'acceptPartial': acceptPartial,
            'limit': limit,
            'offset': offset,
            'project': self.project,
            'sql': sql,
        }
        response = self.client.query.post(request_body=request_body)
        return response

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
    def _models(self):
        if self.__models is None:
            self.__models = self.client.models.get(
                query_params={'projectName': self.project},
            ).to_object
        return self.__models

    @property
    def _cubes(self):
        if self.__cubes is None:
            self.__cubes = self.client.cubes.get(
                query_params={
                    'offset': 0,
                    'limit': 50000,
                    'pageSize': 200,
                    'projectName': self.project,
                },
            ).to_object
        if self._client.version == 'v2':
            return self.__cubes.get('cubes')
        return self.__cubes

    @property
    def cube_names(self):
        return tuple(cube.get('name') for cube in self._cubes
                     if cube['status'] == 'READY')

    @property
    def model_names(self):
        return tuple(model.get('name') for model in self._models)

    def get_source_tables(self, scheme=None):
        if self.is_pushdown:
            _full_names = list(self._tables_in_hive.keys())
        else:
            _full_names = list(self._tables_and_columns.keys())

        if scheme is None:
            return _full_names
        else:
            return list(filter(lambda tbl: tbl.split('.')[0] == scheme, _full_names))

    def _cube_desc(self, name):
        if self._client.version == 'v2':
            return self.client.cube_desc._(self.project)._(name).get()\
                .to_object.get('cube')
        return self.client.cube_desc._(name).desc.get().to_object

    def _model_desc(self, name):
        return [_ for _ in self._models if _.get('name') == name][0]

    def get_datasource(self, name):
        if name in self.get_source_tables():
            if self.is_pushdown:
                return HiveSource(name, self._tables_in_hive.get(name))
            else:
                return HiveSource(name, self._tables_and_columns.get(name))
        if name in self.cube_names:
            cube_desc = self._cube_desc(name)
            model_desc = self._model_desc(cube_desc.get('model_name'))
            return CubeSource(cube_desc, model_desc, self._tables_and_columns)

    def __str__(self):
        return str(self._client) + '/' + self.project

    def __repr__(self):
        return '<Kylin Project Instance: {}>'.format(self.project)


def dsn_proxy(dsn, connect_args={}):
    _ = urllib.urlparse(dsn)
    project = _.path.lstrip('/')
    _port = _.port or 7070
    if project:
        return Project(_.hostname, _.username, _.password, _port, project, **connect_args)
    else:
        return KylinClient(_.hostname, _.username, _.password, _port, **connect_args)
