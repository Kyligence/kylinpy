# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from kylinpy.client import InternalServerError, UnauthorizedError
from kylinpy.exceptions import KylinQueryError
from ._service_interface import ServiceInterface


class _Api(object):
    """
    For testing convenience
    """
    @staticmethod
    def query(client, endpoint, **kwargs):
        return client.post(endpoint=endpoint, **kwargs).json()

    @staticmethod
    def tables_and_columns(client, endpoint, **kwargs):
        return client.get(endpoint=endpoint, **kwargs).json()

    @staticmethod
    def projects(client, endpoint, **kwargs):
        return client.get(endpoint=endpoint, **kwargs).json()

    @staticmethod
    def tables(client, endpoint, **kwargs):
        return client.get(endpoint=endpoint, **kwargs).json()

    @staticmethod
    def cube_desc(client, endpoint, **kwargs):
        return client.get(endpoint=endpoint, **kwargs).json()

    @staticmethod
    def models(client, endpoint, **kwargs):
        return client.get(endpoint=endpoint, **kwargs).json()

    @staticmethod
    def cubes(client, endpoint, **kwargs):
        return client.get(endpoint=endpoint, **kwargs).json()

    @staticmethod
    def authentication(client, endpoint, **kwargs):
        rv = client.get(endpoint=endpoint, **kwargs).json()
        if rv == {}:
            raise UnauthorizedError
        return rv


class KylinService(ServiceInterface):
    api = _Api

    def __init__(self, client, project=None):
        self.client = client
        self.project = project

    def query(self, sql, limit=50000, offset=0, acceptPartial=False, **kwargs):
        json_data = {
            'acceptPartial': acceptPartial,
            'limit': limit,
            'offset': offset,
            'project': self.project,
            'sql': sql,
        }
        kwargs.setdefault('json', json_data)
        try:
            response = self.api.query(self.client, '/query', **kwargs)
        except InternalServerError as err:
            raise KylinQueryError(err)

        err_message = response.get('exceptionMessage')
        if err_message:
            raise KylinQueryError(err_message)

        return response

    def projects(self, **kwargs):
        params = {
            'pageOffset': 0,
            'pageSize': 1000,
        }
        kwargs.setdefault('params', params)
        _projects = self.api.projects(self.client, '/projects', **kwargs)
        return _projects

    def tables_and_columns(self, **kwargs):
        params = {
            'project': self.project,
        }
        kwargs.setdefault('params', params)
        resp = self.api.tables_and_columns(self.client, '/tables_and_columns', **kwargs)
        tbl_pair = tuple(('{}.{}'.format(tbl.get('table_SCHEM'), tbl.get('table_NAME')), tbl) for tbl in resp)
        for tbl in tbl_pair:
            tbl[1]['columns'] = [(col['column_NAME'], col) for col in tbl[1]['columns']]
        return dict(tbl_pair)

    def tables_in_hive(self, **kwargs):
        params = {
            'project': self.project,
            'ext': True,
        }
        kwargs.setdefault('params', params)
        tables = self.api.tables(self.client, '/tables', **kwargs)

        __tables_in_hive = {}
        for tbl in tables:
            db = tbl['database']
            name = tbl['name']
            fullname = '{}.{}'.format(db, name)
            __tables_in_hive[fullname] = tbl

        return __tables_in_hive

    def cube_desc(self, name, **kwargs):
        return self.api.cube_desc(self.client, '/cube_desc/{}/desc'.format(name), **kwargs)

    def model_desc(self, name, **kwargs):
        return [_ for _ in self.models(**kwargs) if _.get('name') == name][0]

    def models(self, **kwargs):
        params = {
            'projectName': self.project,
            'pageOffset': 0,
            'pageSize': 1000,
        }
        kwargs.setdefault('params', params)
        return self.api.models(self.client, '/models', **kwargs)

    def cubes(self, **kwargs):
        params = {
            'pageOffset': 0,
            'offset': 0,
            'limit': 50000,
            'pageSize': 1000,
            'projectName': self.project,
        }
        kwargs.setdefault('params', params)
        return self.api.cubes(self.client, '/cubes', **kwargs)

    def get_authentication(self, **kwargs):
        rv = self.api.authentication(self.client, '/user/authentication', **kwargs)
        return rv.get('userDetails')
