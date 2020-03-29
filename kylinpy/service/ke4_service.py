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
    def jobs(client, endpoint, **kwargs):
        return client.get(endpoint=endpoint, **kwargs).json().get('data')

    @staticmethod
    def resume_job(client, endpoint, **kwargs):
        return client.put(endpoint=endpoint, **kwargs).json().get('data')

    @staticmethod
    def tables(client, endpoint, **kwargs):
        return client.get(endpoint=endpoint, **kwargs).json().get('data')

    @staticmethod
    def model_desc(client, endpoint, **kwargs):
        return client.get(endpoint=endpoint, **kwargs).json().get('data')

    @staticmethod
    def models(client, endpoint, **kwargs):
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
            'page_offset': 0,
            'page_size': 1000,
        }
        kwargs.setdefault('params', params)
        _projects = self.api.projects(self.client, '/projects', **kwargs)
        return _projects.get('value')

    def jobs(self, **kwargs):
        params = {
            'page_offset': 0,
            'page_size': 20,
        }
        kwargs.setdefault('params', params)
        _jobs = self.api.jobs(self.client, '/jobs', **kwargs)
        return _jobs.get('value')

    def resume_job(self, job_id, **kwargs):
        params = {
            'jobId': job_id,
        }
        kwargs.setdefault('params', params)
        res = self.api.resume_job(self.client, '/jobs/{0}/resume'.format(job_id), **kwargs)
        return res

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

    def model_desc(self, name, **kwargs):
        _url = {
            'project': self.project,
            'model_name': name,
        }
        _model_desc = self.api.model_desc(
            self.client,
            '/models/{project}/{model_name}/model_desc'.format(**_url), **kwargs)
        return _model_desc

    def models(self, **kwargs):
        params = {
            'project': self.project,
            'page_offset': 0,
            'page_size': 1000,
        }
        kwargs.setdefault('params', params)
        _models = self.api.models(self.client, '/models', **kwargs)
        return _models.get('value')

    def get_authentication(self, **kwargs):
        return self.api.authentication(self.client, '/user/authentication', **kwargs)
