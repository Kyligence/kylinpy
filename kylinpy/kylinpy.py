# -*- coding: utf-8 -*-

from __future__ import division
from __future__ import absolute_import

import re
import json
import base64
import contextlib

from six.moves import urllib

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
    def __init__(self, scheme, host, port, username, password, **kwargs):
        self.scheme = scheme
        self.host = re.sub('/$', '', host)
        self.port = port
        self.username = username
        self.password = password
        self.version = kwargs.get('version', 'v1')
        self.prefix = re.sub('(^/|/$)', '', kwargs.get('prefix', 'kylin/api'))

    def _prepare_url(self, endpoint, query=None):
        if endpoint.startswith('/'):
            endpoint = endpoint[1:]

        url = '{self.scheme}://{self.host}:{self.port}/{self.prefix}/{endpoint}'.format(
            **locals())

        if query:
            url = '{}?{}'.format(url, urllib.parse.urlencode(query))

        return url

    def _prepare_headers(self, method='GET', session=False):
        headers = {
            'User-Agent': 'Kylin Python Client'
        }

        if not session:
            _auth_str = as_unicode('{}:{}').format(
                self.username, self.password)
            _auth = base64.b64encode(
                _auth_str.encode('utf-8')
            ).decode('ascii')
            headers.update({'Authorization': 'Basic {}'.format(_auth)})
        else:
            # todo session
            pass

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
            logger.debug('''
==========================[QUERY]===============================
 method: %s \n url: %s \n headers: %s \n body: %s
==========================[QUERY]===============================
            ''', method, url, headers, body)

            with contextlib.closing(urllib.request.urlopen(req, body)) as fd:
                try:
                    dumps = json.loads(fd.read())
                except ValueError:
                    raise KylinError('KYLIN JSON object could not decoded')

            return dumps

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
            obj = request(self, *args, **kwargs)
            # todo:
            # if self.is_debug
            #     return obj

            # always return {'data': ENTITY}
            if _is_v2(obj):
                obj = obj['data'].get(
                    extract_v2, None) if extract_v2 else obj['data']
            else:
                obj = obj.get(extract_v1, None) if extract_v1 else obj

            return {'data': obj}
        return wrapper
    return fn


def only_kap_api(fn):
    def wrapper(self, *args, **kwargs):
        if self.client.version == 'v1':
            raise KAPOnlyError
        return fn(self, *args, **kwargs)
    return wrapper


def cache_resp(fn):
    cached = {}

    def wrapper(*args):
        if fn.__name__ not in cached:
            cached[fn.__name__] = fn(*args)
        return cached[fn.__name__]
    return wrapper


class OriginalAPIMixin(object):
    @compact_response(extract_v1='userDetails')
    def authentication(self):
        return self.client.fetch(endpoint='user/authentication', method='POST')

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
            return [e for e in models if e['name'] == name][0]


class ExtendAPIMixin(object):
    def _get_column_datatype(self, column_name, table_name):
        table = [t for t in self.tables()['data'] if t['name'] == table_name][0]
        return [c for c in table['columns'] if c['name'] == column_name][0]['datatype']

    @compact_response()
    def get_table_names(self):
        return [t['table_NAME'] for t in self.tables_and_columns()['data']]

    @compact_response()
    def get_table_columns(self, table):
        '''
        columns list like this:
        [
          {
            u'table': u'CUSTOMER',
            u'column_NAME': u'C_NAME'
            u'datatype': "varchar(256)"
          },
          ...
        ]
        '''
        table = [t for t in self.tables_and_columns()['data']
                 if t['table_NAME'] == table]
        columns = table[0]['columns'] if table else []

        return [
            {
                'table_NAME': col['table_NAME'],
                'column_NAME': col['column_NAME'],
                'datatype': self._get_column_datatype(col['column_NAME'], col['table_NAME'])
            }
            for col in columns
        ]

    @compact_response()
    def get_cube_names(self):
        return [cube['name'] for cube in self.cubes()['data'] if cube['status'] == 'READY']

    @compact_response()
    def get_cube_columns(self, cube_name):
        '''
        columns list like this:
        [
          {
            u'column': u'C_NAME',
            u'table': u'CUSTOMER',
            u'derived': None,
            u'name': u'C_NAME',
            u'column_NAME': u'C_NAME' <---- tabename_columnname
            u'datatype': "varchar(256)"
          },
          ...
        ]
        '''
        cube_desc = self.cube_desc(cube_name)['data']
        dimensions = cube_desc['dimensions']
        model_name = cube_desc['model_name']
        model_desc = self.model_desc(model_name)['data']

        # collect dimension and derived
        collect_dimensions = [dict(dim, **{
            'column_NAME': dim['column'] if dim['derived'] is None else dim['derived'][0]
        }) for dim in dimensions]

        def _get_origin_table(table_name):
            # lookup table name -> fact table name
            origin_table = ([e for e in model_desc['lookups']
                             if e['alias'] == table_name] or [None])[0]
            if origin_table:
                return origin_table['table'].split('.')[1]
            else:
                return table_name

        return [
            dict(dim, **{
                'datatype': self._get_column_datatype(dim['column_NAME'], _get_origin_table(dim['table'])),
                'column_NAME': '_'.join([dim['table'], dim['column_NAME']])
            })
            for dim in collect_dimensions
        ]

    @compact_response()
    def get_cube_measures(self, cube_name):
        cube_measures = self.cube_desc(cube_name)['data']['measures']
        return cube_measures

class Kylinpy(OriginalAPIMixin, ExtendAPIMixin):
    def __init__(self, host, username, password, port=7070, project='default', **kwargs):
        if host.startswith(('http://', 'https://')):
            scheme, host = host.split('://')
        else:
            scheme, host = ('http', host)

        self.client = Client(scheme, host, port, as_unicode(
            username), as_unicode(password), **kwargs)
        self.project = project
        self.is_debug = kwargs.get('is_debug', False)

    def __str__(self):
        c = self.client
        return 'kylin://{c.username}:{c.password}@{c.host}:{c.port}/{self.project}'.format(**locals())
