# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from kylinpy.client import InternalServerError, UnauthorizedError
from kylinpy.exceptions import KylinQueryError


class _Api(object):
    """
    For testing convenience
    """
    @staticmethod
    def query(client, endpoint, **kwargs):
        return client.post(endpoint=endpoint, **kwargs).json().get('data')

    @staticmethod
    def tables_and_columns(client, endpoint, **kwargs):
        return client.get(endpoint=endpoint, **kwargs).json().get('data')

    @staticmethod
    def projects(client, endpoint, **kwargs):
        return client.get(endpoint=endpoint, **kwargs).json().get('data')

    @staticmethod
    def tables(client, endpoint, **kwargs):
        return client.get(endpoint=endpoint, **kwargs).json().get('data')

    @staticmethod
    def cube_desc(client, endpoint, **kwargs):
        return client.get(endpoint=endpoint, **kwargs).json().get('data')

    @staticmethod
    def models(client, endpoint, **kwargs):
        return client.get(endpoint=endpoint, **kwargs).json().get('data')

    @staticmethod
    def cubes(client, endpoint, **kwargs):
        return client.get(endpoint=endpoint, **kwargs).json().get('data')

    @staticmethod
    def authentication(client, endpoint, **kwargs):
        rv = client.get(endpoint=endpoint, **kwargs).json().get('data')
        if rv == {}:
            raise UnauthorizedError
        return rv


class KE3Service(object):
    api = _Api

    def __init__(self, client, project=None):
        self.client = client
        self.project = project

    def query(self, sql, limit=50000, offset=0, acceptPartial=False):
        request_body = {
            'acceptPartial': acceptPartial,
            'limit': limit,
            'offset': offset,
            'project': self.project,
            'sql': sql,
        }
        try:
            response = self.api.query(self.client, '/query', json=request_body)
        except InternalServerError as err:
            raise KylinQueryError(err)

        err_message = response.get('exceptionMessage')
        if err_message:
            raise KylinQueryError(err_message)

        return response

    @property
    def projects(self):
        params = {
            'pageOffset': 0,
            'pageSize': 1000,
        }
        _projects = self.api.projects(self.client, '/projects', params=params)
        return _projects.get('projects')

    @property
    def tables_and_columns(self):
        resp = self.api.tables_and_columns(self.client, '/tables_and_columns', params={'project': self.project})
        tbl_pair = tuple(('{}.{}'.format(tbl.get('table_SCHEM'), tbl.get('table_NAME')), tbl) for tbl in resp)
        for tbl in tbl_pair:
            tbl[1]['columns'] = [(col['column_NAME'], col) for col in tbl[1]['columns']]
        return dict(tbl_pair)

    @property
    def tables_in_hive(self):
        tables = self.api.tables(self.client, '/tables', params={'project': self.project, 'ext': True})

        __tables_in_hive = {}
        for tbl in tables:
            db = tbl['database']
            name = tbl['name']
            fullname = '{}.{}'.format(db, name)
            __tables_in_hive[fullname] = tbl

        return __tables_in_hive

    def cube_desc(self, name):
        _cube_desc = self.api.cube_desc(self.client, '/cube_desc/{}/{}'.format(self.project, name))
        return _cube_desc.get('cube')

    def model_desc(self, name):
        return [_ for _ in self.models if _.get('name') == name][0]

    @property
    def models(self):
        params = {
            'projectName': self.project,
            'pageOffset': 0,
            'pageSize': 1000,
        }
        _models = self.api.models(self.client, '/models', params=params)
        return _models.get('models')

    @property
    def cubes(self):
        params = {
            'pageOffset': 0,
            'offset': 0,
            'limit': 50000,
            'pageSize': 1000,
            'projectName': self.project,
        }
        _cubes = self.api.cubes(self.client, '/cubes', params=params)
        return _cubes.get('cubes')

    @property
    def get_authentication(self):
        return self.api.authentication(self.client, '/user/authentication')
