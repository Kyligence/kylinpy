# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from kylinpy.client import InternalServerError, UnauthorizedError
from kylinpy.exceptions import KylinQueryError, KylinJobError
from kylinpy.utils.helper import private_v4_api_warnings
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
    def jobs(client, endpoint, params=None, **kwargs):
        return client.get(endpoint=endpoint, params=params, **kwargs).json().get('data')

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

    @staticmethod
    def build(client, endpoint, **kwargs):
        return client.post(endpoint=endpoint, **kwargs).json().get('data')

    @staticmethod
    def build_indexes(client, endpoint, **kwargs):
        return client.post(endpoint=endpoint, **kwargs).json().get('data')

    @staticmethod
    def list_index_rules(client, endpoint, **kwargs):
        return client.get(endpoint=endpoint, **kwargs).json().get('data')

    @staticmethod
    def put_index_rules(client, endpoint, **kwargs):
        return client.put(endpoint=endpoint, **kwargs).json().get('data')

    @staticmethod
    def list_indexes(client, endpoint, **kwargs):
        return client.get(endpoint=endpoint, **kwargs).json().get('data')

    @staticmethod
    def delete_index(client, endpoint, **kwargs):
        return client.delete(endpoint=endpoint, **kwargs).json()

    @staticmethod
    def refresh(client, endpoint, **kwargs):
        return client.put(endpoint=endpoint, **kwargs).json().get('data')

    @staticmethod
    def merge(client, endpoint, **kwargs):
        return client.put(endpoint=endpoint, **kwargs).json().get('data')

    @staticmethod
    def list_segment(client, endpoint, **kwargs):
        return client.get(endpoint=endpoint, **kwargs).json().get('data')

    @staticmethod
    def delete_segment(client, endpoint, **kwargs):
        return client.delete(endpoint=endpoint, **kwargs).json()

    @staticmethod
    def refresh_catalog_cache(client, endpoint, **kwargs):
        return client.put(endpoint=endpoint, **kwargs).json()


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

    def jobs(self, **params):
        _params = {
            'time_filter': 0,
        }
        _params.update(params)
        _jobs = self.api.jobs(self.client, '/jobs', params=_params)
        return _jobs.get('value')

    def job_desc(self, job_id):
        try:
            params = {
                'time_filter': 0,
                'key': str(job_id),
                'project': self.project,
            }
            rv = self.api.jobs(self.client, '/jobs', params=params).get('value')
            if len(rv) > 0:
                return rv[0]
            else:
                raise KylinJobError("Not Find this job {}".format(job_id))
        except InternalServerError as e:
            raise KylinJobError(e)

    def tables_and_columns(self, **kwargs):
        params = {'project': self.project}
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

    def fullbuild(self, model_name):
        json = {
            'project': self.project,
            'start': '1',
            'end': '9223372036854775806',
        }
        endpoint = '/models/{}/segments'.format(model_name)
        return self.api.build(self.client, endpoint, json=json)

    def build(self, model_name, start, end):
        json = {
            'project': self.project,
            'start': str(start),
            'end': str(end),
        }
        endpoint = '/models/{}/segments'.format(model_name)
        return self.api.build(self.client, endpoint, json=json)

    @private_v4_api_warnings
    def get_index_rules_by_model_uuid(self, model_uuid):
        params = {
            'project': self.project,
            'model': model_uuid,
            'page_offset': 0,
            'page_size': 10000,
        }
        return self.api.list_index_rules(self.client, '/index_plans/rule', params=params)

    @private_v4_api_warnings
    def put_index_rules_by_model_uuid(self, model_uuid, load_data=False, aggregation_groups=None):
        _json = {
            'aggregation_groups': aggregation_groups if aggregation_groups else [],
            'load_data': load_data,
            'model_id': model_uuid,
            'project': self.project,
        }
        return self.api.put_index_rules(self.client, '/index_plans/rule', json=_json)

    @private_v4_api_warnings
    def get_indexes_by_model_uuid(self, model_uuid):
        params = {
            'project': self.project,
            'model': model_uuid,
            'page_offset': 0,
            'page_size': 10000,
        }
        rv = self.api.list_indexes(self.client, '/index_plans/index', params=params)
        return rv.get('value', [])

    def build_indexes(self, model_name):
        json = {
            'project': self.project,
        }
        endpoint = '/models/{}/indexes'.format(model_name)
        return self.api.build_indexes(self.client, endpoint, json=json)

    @private_v4_api_warnings
    def delete_index(self, model_uuid, index_id):
        params = {
            'project': self.project,
            'model': model_uuid,
        }
        return self.api.delete_index(self.client, '/index_plans/index/{}'.format(index_id), params=params)

    def refresh(self, model_name, ids):
        json = {
            'project': self.project,
            'type': 'REFRESH',
            'ids': ids,
        }
        res = self.api.refresh(self.client, '/models/{}/segments'.format(model_name), json=json)
        return res

    def merge(self, model_name, ids):
        json = {
            'project': self.project,
            'type': 'MERGE',
            'ids': ids,
        }
        res = self.api.merge(self.client, '/models/{}/segments'.format(model_name), json=json)
        return res

    def list_segment(self, model_name):
        params = {
            'project': self.project,
            'model_name': model_name,
            'page_size': 2147483646,
        }
        endpoint = '/models/{}/segments'.format(model_name)
        return self.api.list_segment(self.client, endpoint, params=params)

    def delete_segment(self, model_name, ids, **kwargs):
        params = {
            'project': self.project,
            'ids': ids,
        }
        params.update(kwargs)
        endpoint = '/models/{}/segments'.format(model_name)
        return self.api.delete_segment(self.client, endpoint, params=params)

    @private_v4_api_warnings
    def refresh_catalog_cache(self, tables):
        params = {
            'tables': tables,
        }
        endpoint = '/tables/catalog_cache'
        return self.api.refresh_catalog_cache(self.client, endpoint, params=params)
