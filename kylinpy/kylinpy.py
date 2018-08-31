# -*- coding: utf-8 -*-

from __future__ import division
from __future__ import absolute_import

import base64
import contextlib
import datetime
import json
import re
import time
from collections import namedtuple

from six.moves import urllib
import ssl

from .errors import (
    KylinUnauthorizedError,
    KylinUserDisabled,
    KylinConnectionError,
    KylinError,
    KylinConfusedResponse,
    KAPOnlyError
)
from .logger import logger
from .utils._compat import as_unicode


class Client(object):
    def __init__(self, scheme, host, port, username, **kwargs):
        self.scheme = scheme
        self.host = re.sub('/$', '', host)
        self.port = port
        self.username = username
        self.password = kwargs.get('password', None)
        self.version = kwargs.get('version', 'v1')
        self.prefix = re.sub('(^/|/$)', '', kwargs.get('prefix', 'kylin/api'))
        self.session = kwargs.get('session', None)

        if not self.password and not self.session:
            raise KylinError('Need password or session')

    def _prepare_url(self, endpoint, query=None):
        if endpoint.startswith('/'):
            endpoint = endpoint[1:]

        url = '{self.scheme}://{self.host}:{self.port}/{self.prefix}/{endpoint}'.format(
            **locals())

        if query:
            url = '{}?{}'.format(url, urllib.parse.urlencode(query))

        return url

    def _prepare_headers(self, method='GET'):
        headers = {
            'User-Agent': 'Kylin Python Client'
        }

        if not self.session:
            _auth_str = as_unicode('{}:{}').format(
                self.username, self.password)
            _auth = base64.b64encode(
                _auth_str.encode('utf-8')
            ).decode('ascii')
            headers.update({'Authorization': 'Basic {}'.format(_auth)})
        else:
            headers.update({'Cookie': self.session})

        if method in ['POST', 'PUT']:
            headers['Content-Type'] = 'application/json'

        return headers

    def _prepare_body(self, body=None):
        if body:
            body = json.dumps(body).encode('utf-8')

        return body

    def fetch(self, endpoint, method='GET', body=None, params=None):
        try:
            method = method.upper()
            url = self._prepare_url(endpoint, params)
            headers = self._prepare_headers(method)
            if self.version == 'v2':
                headers.update(
                    {'Accept': 'application/vnd.apache.kylin-v2+json'})
            body = self._prepare_body(body)

            req = urllib.request.Request(url, headers=headers)
            req.get_method = lambda: method
            logger.debug("""
==========================[QUERY]===============================
 method: %s \n url: %s \n headers: %s \n body: %s
==========================[QUERY]===============================
            """, method, url, headers, body)

            Response = namedtuple('Response', ['headers', 'body'])
            with contextlib.closing(urllib.request.urlopen(req, body, context=ssl._create_unverified_context())) as resp:
                try:
                    response_headers = dict(resp.info())
                    response_body = json.loads(resp.read().decode("utf-8"))
                except ValueError:
                    raise KylinError('KYLIN JSON object could not decoded')

            return Response(response_headers, response_body)

        except urllib.error.HTTPError as e:
            err = e.read()
            try:
                err = json.loads(err.decode("utf-8"))['msg']
            except(ValueError, AttributeError, KeyError):
                logger.debug(err)
                raise KylinConfusedResponse('Confused Response')

            if e.code == 401 and 'User is disabled' in err:
                raise KylinUserDisabled(err)

            if e.code == 401:
                raise KylinUnauthorizedError(err)

            if e.code == 500:
                raise KylinError(err)

        except urllib.error.URLError as e:
            raise KylinConnectionError("{}".format(str(e.args[0])))


def _is_v2(obj):
    return isinstance(obj, dict) and {'code', 'data', 'msg'} == set(obj.keys())


def compact_response(extract_v2=None, extract_v1=None):
    def fn(request):
        def wrapper(self, *args, **kwargs):
            # todo refactor here
            obj = request(self, *args, **kwargs)
            body = obj if isinstance(obj, list) or isinstance(obj, dict) else obj.body
            headers = None if isinstance(obj, list) or isinstance(obj, dict) else obj.headers
            # always return {'data': ENTITY}
            if _is_v2(body):
                body = body['data'].get(
                    extract_v2, None) if extract_v2 else body['data']
            else:
                body = body.get(extract_v1, None) if extract_v1 else body

            return {'data': body, 'headers': headers}
        return wrapper
    return fn


def only_kap_api(fn):
    def wrapper(self, *args, **kwargs):
        if self.client.version == 'v1':
            raise KAPOnlyError
        return fn(self, *args, **kwargs)
    return wrapper


class _OriginalAPIMixin(object):
    @compact_response(extract_v1='userDetails')
    def authentication(self):
        info = self.client.fetch(endpoint='user/authentication', method='POST')
        if info is None or not info.body:
            raise KylinUnauthorizedError('Unauthorized error')
        else:
            return info

    @compact_response(extract_v2='projects')
    def projects(self):
        _params = {
            'pageSize': 1000,
            'pageOffset': 0
        }
        return self.client.fetch(endpoint='projects', params=_params)

    @compact_response()
    def query(self, sql, **body):
        _body = {
            'acceptPartial': False,
            'limit': 50000,
            'offset': 0,
            'project': self.project,
            'sql': sql
        }
        _body.update(body)

        return self.client.fetch(endpoint='query', method='POST', body=_body)

    @compact_response()
    def tables_and_columns(self):
        params = {'project': self.project}
        return self.client.fetch(endpoint='tables_and_columns', params=params)

    @compact_response()
    def tables(self, ext=False):
        _params = {
            'project': self.project,
            'ext': ext
        }
        return self.client.fetch(endpoint='tables', params=_params)

    @compact_response(extract_v2='cubes')
    def cubes(self, **params):
        _params = {
            'offset': 0,
            'limit': 50000,
            'pageSize': 200,
            'projectName': self.project
        }
        _params.update(params)
        return self.client.fetch(endpoint='cubes', params=_params)

    @compact_response()
    @only_kap_api
    def cube_sql(self, cube_name):
        _endpoint = 'cubes/{}/sql'.format(cube_name)
        return self.client.fetch(endpoint=_endpoint)

    @compact_response(extract_v2='cube')
    def cube_desc(self, name):
        if self.client.version == 'v2':
            return self.client.fetch('cube_desc/{}/{}'.format(self.project, name))
        if self.client.version == 'v1':
            return self.client.fetch('cube_desc/{}/desc'.format(name))

    @compact_response(extract_v2="users")
    @only_kap_api
    def users(self):
        _params = {
            'pageSize': 1000,
            'pageOffset': 0
        }
        return self.client.fetch('kap/user/users', params=_params)

    @compact_response(extract_v2="model")
    def model_desc(self, name):
        if self.client.version == 'v2':
            return self.client.fetch('model_desc/{self.project}/{name}'.format(**locals()))
        # v1 did not implement model_desc/<project>/<model_name>
        if self.client.version == 'v1':
            models = self.client.fetch(
                'models', params={'projectName': self.project})
            return [e for e in models.body if e['name'] == name][0]

    @compact_response(extract_v2="models")
    @only_kap_api
    def list_models(self):
        return self.client.fetch('models')

    @only_kap_api
    def create_model(self, model_desc):
        _body = {
            'modelDescData': model_desc,
            'project': self.project
        }
        return self.client.fetch('models', method='PUT', body=_body)

    @only_kap_api
    def create_cube(self, cube_desc):
        _body = {
            'cubeDescData': cube_desc,
            'project': self.project
        }
        return self.client.fetch('cubes', method='PUT', body=_body)

    @only_kap_api
    def build_cube(self, cube_name):
        _body = {
            'buildType': "BUILD",
            'endTime': int(time.mktime(datetime.datetime.now().timetuple())),
            'mpValues': "",
            'startTime': 0
        }
        return self.client.fetch('cubes/{}/rebuild'.format(cube_name), method='PUT', body=_body)

    @only_kap_api
    def disable_cube(self, cube_name):
        return self.client.fetch('cubes/{}/disable'.format(cube_name), method='PUT')

    @only_kap_api
    def purge_cube(self, cube_name):
        _body = {
            'mpValues': "",
        }
        return self.client.fetch('cubes/{}/purge'.format(cube_name), method='PUT', body=_body)


class _ExtendedAPIMixin(object):
    def _get_column_datatype(self, column_name, table_name):
        schema_name, table_name = table_name.split('.')
        try:
            table = [t for t in self.tables()['data']
                     if t['name'] == table_name and t['database'] == schema_name][0]
            return [c for c in table['columns'] if c['name'] == column_name][0]['datatype']
        except IndexError:
            logger.error("column {} not found on {}.{}".format(
                column_name,
                schema_name,
                table_name))

    @compact_response()
    def get_table_names(self, schema=None):
        return [t['table_NAME'] for t in self.tables_and_columns()['data']
                if t['table_SCHEM'] == schema]

    @compact_response()
    def list_schemas(self):
        table_schemas = [t['table_SCHEM'] for t in self.tables_and_columns()['data']]
        table_schemas = list(set(table_schemas))
        return table_schemas

    @compact_response()
    def get_table_columns(self, table):
        """
        table structure:
        [
          {
            u'table': u'CUSTOMER',
            u'column_NAME': u'C_NAME'
            u'datatype': "varchar(256)"
          },
          ...
        ]
        """
        table = [t for t in self.tables_and_columns()['data']
                 if t['table_NAME'] == table]
        columns = table[0]['columns'] if table else []

        return [
            {
                'table_NAME': col['table_NAME'],
                'table_SCHEM': col['table_SCHEM'],
                'column_NAME': col['column_NAME'],
                'datatype': self._get_column_datatype(
                    col['column_NAME'],
                    '{}.{}'.format(col['table_SCHEM'], col['table_NAME']))
            }
            for col in columns
        ]

    @compact_response()
    def get_cube_names(self):
        return [cube['name'] for cube in self.cubes()['data'] if cube['status'] == 'READY']

    @compact_response()
    def get_cube_columns(self, cube_name):
        """cube describe"""
        Row = namedtuple('Row', ['schema', 'table', 'column',
                                 'datatype', 'is_derived',
                                 'table_label', 'column_label'])

        cube_desc = self.cube_desc(cube_name)['data']
        dimensions = cube_desc['dimensions']
        model_name = cube_desc['model_name']
        model_desc = self.model_desc(model_name)['data']
        rowkey_columns = cube_desc['rowkey'].get('rowkey_columns', [])

        def _get_origin_table(table_label):
            """
            return original table name with schema
            """
            lookup_table = ([e for e in model_desc['lookups']
                             if e['alias'] == table_label] or [None])[0]
            if lookup_table:
                return lookup_table['table']
            else:
                # fact table
                return model_desc['fact_table']

        cube_describe = []
        has_dimensions = set([])
        for dim in dimensions:
            column = dim['column'] if dim['derived'] is None else dim['derived'][0]
            column_label = dim['name']
            table_label = dim['table']
            schema, table = _get_origin_table(table_label).split('.')
            datatype = self._get_column_datatype(column, '{}.{}'.format(schema, table))
            is_derived = bool(dim['derived'])
            label = "{}.{}".format(table_label, column)
            has_dimensions.add(label)
            _row = Row(schema, table, column, datatype, is_derived,
                       table_label, column_label,)
            cube_describe.append(_row._asdict())

        for rowkey in rowkey_columns:
            if rowkey['column'] in has_dimensions:
                continue

            table_label, column_label = rowkey['column'].split('.')
            column = column_label
            schema, table = _get_origin_table(table_label).split('.')
            datatype = self._get_column_datatype(column, '{}.{}'.format(schema, table))
            label = "{}.{}".format(table_label, column)
            has_dimensions.add(label)
            _row = Row(schema, table, column, datatype, False,
                       table_label, column_label,)
            cube_describe.append(_row._asdict())

        return cube_describe

    @compact_response()
    def get_cube_measures(self, cube_name):
        cube_measures = self.cube_desc(cube_name)['data']['measures']
        return cube_measures


class Kylinpy(_OriginalAPIMixin, _ExtendedAPIMixin):
    def __init__(self, host, username, port=7070, project='default', **kwargs):
        if host.startswith(('http://', 'https://')):
            _, host = host.split('://')
        scheme = kwargs.get('scheme', 'http')
        kwargs.pop('scheme', None)

        self.client = Client(scheme, host, port, as_unicode(username), **kwargs)
        self.project = project
        self.is_debug = kwargs.get('is_debug', False)

    def __str__(self):
        c = self.client
        return 'kylin://{c.username}:{c.password}@{c.host}:{c.port}/{self.project}'\
            .format(**locals())
