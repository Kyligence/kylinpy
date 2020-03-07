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
    def authentication(client, endpoint, **kwargs):
        rv = client.get(endpoint=endpoint, **kwargs).json().get('data')
        if rv == {}:
            raise UnauthorizedError
        return rv


class KE4Service(ServiceInterface):
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
        return _projects.get('value')

    def tables_and_columns(self, **kwargs):
        params = {
            'project': self.project,
        }
        kwargs.setdefault('params', params)
        resp = self.api.tables_and_columns(self.client, '/query/tables_and_columns', **kwargs)
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
        tables = tables.get('tables')

        __tables_in_hive = {}
        for tbl in tables:
            db = tbl['database']
            name = tbl['name']
            fullname = '{}.{}'.format(db, name)
            __tables_in_hive[fullname] = tbl

        return __tables_in_hive

    def get_authentication(self, **kwargs):
        return self.api.authentication(self.client, '/user/authentication', **kwargs)
