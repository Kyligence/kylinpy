# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from kylinpy.client import InternalServerError, UnauthorizedError
from kylinpy.exceptions import KylinQueryError, KylinCubeError, KylinJobError
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
    def jobs(client, endpoint, **kwargs):
        return client.get(endpoint=endpoint, **kwargs).json()

    @staticmethod
    def maintain_job(client, endpoint, **kwargs):
        return client.put(endpoint=endpoint, **kwargs).json()

    @staticmethod
    def drop_job(client, endpoint, **kwargs):
        return client.delete(endpoint=endpoint, **kwargs).json()

    @staticmethod
    def job_desc(client, endpoint, **kwargs):
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

    @staticmethod
    def build(client, endpoint, **kwargs):
        return client.put(endpoint=endpoint, **kwargs).json()

    @staticmethod
    def build_streaming(client, endpoint, **kwargs):
        return client.put(endpoint=endpoint, **kwargs).json()

    @staticmethod
    def delete_segment(client, endpoint, **kwargs):
        return client.delete(endpoint=endpoint, **kwargs).json()

    @staticmethod
    def maintain_cube(client, endpoint, **kwargs):
        return client.put(endpoint=endpoint, **kwargs).json()

    @staticmethod
    def drop_cube(client, endpoint, **kwargs):
        return client.delete(endpoint=endpoint, **kwargs).json()


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

    def jobs(self, **params):
        _params = {
            'projectName': self.project,
            'limit': 15,
            'offset': 0,
        }
        _params.update(params)
        return self.api.jobs(self.client, '/jobs', params=_params)

    def maintain_job(self, job_id, maintain_type):
        if maintain_type not in ('resume', 'cancel', 'pause'):
            raise KylinJobError(
                "Unsupported maintain type: {}, "
                "The maintain type must be 'resume', 'cancel',  'pause'".format(maintain_type))

        try:
            return self.api.maintain_job(self.client, '/jobs/{}/{}'.format(job_id, maintain_type))
        except InternalServerError as e:
            raise KylinJobError(e)

    def drop_job(self, job_id):
        try:
            return self.api.drop_job(self.client, '/jobs/{}/drop'.format(job_id))
        except InternalServerError as e:
            raise KylinJobError(e)

    def job_desc(self, job_id):
        try:
            return self.api.job_desc(self.client, '/jobs/{}'.format(job_id))
        except InternalServerError as e:
            raise KylinJobError(e)

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

    def cubes(self, name=None, **kwargs):
        params = {
            'pageOffset': 0,
            'offset': 0,
            'limit': 50000,
            'pageSize': 1000,
            'projectName': self.project,
        }
        if name:
            params.update({'cubeName': name})
        kwargs.setdefault('params', params)
        return self.api.cubes(self.client, '/cubes', **kwargs)

    def get_authentication(self, **kwargs):
        rv = self.api.authentication(self.client, '/user/authentication', **kwargs)
        return rv.get('userDetails')

    def fullbuild(self, cube_name):
        json = {
            'startTime': 0,
            'endTime': 0,
            'buildType': 'BUILD',
        }
        endpoint = '/cubes/{}/build'.format(cube_name)
        return self.api.build(self.client, endpoint, json=json)

    def build(self, cube_name, build_type, start, end):
        if build_type not in ('BUILD', 'MERGE', 'REFRESH'):
            raise KylinCubeError(
                "Unsupported build type: {}, The build type must be 'BUILD', 'MERGE', 'REFRESH'".format(build_type))

        json = {
            'startTime': start,
            'endTime': end,
            'buildType': build_type,
        }
        endpoint = '/cubes/{}/build'.format(cube_name)
        return self.api.build(self.client, endpoint, json=json)

    def build_streaming(self, cube_name, build_type, offset_start, offset_end):
        if build_type not in ('BUILD', 'MERGE', 'REFRESH'):
            raise KylinCubeError(
                "Unsupported build type: {}, The build type must be 'BUILD', 'MERGE', 'REFRESH'".format(build_type))

        json = {
            'sourceOffsetStart': offset_start,
            'sourceOffsetEnd': offset_end,
            'buildType': build_type,
        }
        endpoint = '/cubes/{}/build2'.format(cube_name)
        return self.api.build_streaming(self.client, endpoint, json=json)

    def delete_segment(self, cube_name, segment_name):
        endpoint = '/cubes/{}/segs/{}'.format(cube_name, segment_name)
        return self.api.delete_segment(self.client, endpoint)

    def maintain_cube(self, cube_name, maintain_type):
        if maintain_type not in ('disable', 'enable', 'purge', 'clone'):
            raise KylinCubeError(
                "Unsupported maintain type: {}, "
                "The maintain type must be 'BUILD', 'MERGE', 'REFRESH'".format(maintain_type))

        endpoint = '/cubes/{}/{}'.format(cube_name, maintain_type)
        json = {}
        if maintain_type == 'clone':
            json = {
                'cubeName': '{}_clone'.format(cube_name),
                'project': self.project,
            }
        try:
            return self.api.maintain_cube(self.client, endpoint, json=json)
        except InternalServerError as e:
            raise KylinCubeError(e)

    def drop_cube(self, cube_name):
        endpoint = '/cubes/{}'.format(cube_name)
        try:
            return self.api.drop_cube(self.client, endpoint)
        except InternalServerError as e:
            raise KylinCubeError(e)
