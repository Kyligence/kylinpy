# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from kylinpy.client import InternalServerError
from kylinpy.exceptions import KylinQueryError


class KylinService(object):
    service_type = 'kylin'

    def __init__(self, client, project=None):
        self.client = client
        self.project = project

    @property
    def is_v2(self):
        return 'application/vnd.apache.kylin-v2+json' \
               in self.client.request_headers.values()

    def query(self, sql, limit=50000, offset=0, acceptPartial=False):
        request_body = {
            'acceptPartial': acceptPartial,
            'limit': limit,
            'offset': offset,
            'project': self.project,
            'sql': sql,
        }
        try:
            response = self.client.query.post(request_body=request_body)
        except InternalServerError as err:
            raise KylinQueryError(err)

        response = response.to_object
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
        _projects = self.client.get(endpoint='/projects', params=params).to_object
        if self.is_v2:
            return _projects.get('projects')
        return _projects

    @property
    def tables_and_columns(self):
        resp = self.client.get(
            endpoint='/tables_and_columns',
            params={'project': self.project},
        ).to_object
        tbl_pair = tuple(
            ('{}.{}'.format(tbl.get('table_SCHEM'), tbl.get('table_NAME')), tbl)
            for tbl in resp)
        for tbl in tbl_pair:
            tbl[1]['columns'] = [(col['column_NAME'], col)
                                 for col in tbl[1]['columns']]
        return dict(tbl_pair)

    @property
    def tables_in_hive(self):
        tables = self.client.tables.get(
            params={
                'project': self.project,
                'ext': True,
            },
        ).to_object

        __tables_in_hive = {}
        for tbl in tables:
            db = tbl['database']
            name = tbl['name']
            fullname = '{}.{}'.format(db, name)
            __tables_in_hive[fullname] = tbl

        return __tables_in_hive

    def cube_desc(self, name):
        if self.is_v2:
            return self.client.cube_desc._(self.project)._(name).get()\
                .to_object.get('cube')
        return self.client.cube_desc._(name).desc.get().to_object

    def model_desc(self, name):
        return [_ for _ in self.models if _.get('name') == name][0]

    @property
    def models(self):
        _models = self.client.models.get(
            params={
                'projectName': self.project,
                'pageOffset': 0,
                'pageSize': 1000,
            },
        ).to_object
        if self.is_v2:
            return _models.get('models')
        return _models

    @property
    def cubes(self):
        _cubes = self.client.cubes.get(
            params={
                'pageOffset': 0,
                'offset': 0,
                'limit': 50000,
                'pageSize': 1000,
                'projectName': self.project,
            },
        ).to_object
        if self.is_v2:
            return _cubes.get('cubes')
        return _cubes
