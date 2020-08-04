# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import pytest

from kylinpy.job import KylinJob
from kylinpy.kylinpy import create_kylin, SERVICES
from kylinpy.client import Client
from kylinpy.exceptions import NoSuchTableError


class TestProject(object):
    @property
    def project(self):
        return create_kylin('kylin://username:password@example/foobar')

    def test_init(self):
        cluster = create_kylin('kylin://name@45中文:pwd12@%+@example.com:9000/foobar')
        assert cluster.host == 'example.com'
        assert cluster.port == 9000
        assert cluster.username == 'name@45中文'
        assert cluster.password == 'pwd12@%+'
        assert cluster.is_ssl is False
        assert cluster.prefix == '/kylin/api'
        assert cluster.timeout == 30
        assert cluster.unverified is True
        assert cluster.version == 'v1'
        assert cluster.is_pushdown is False
        assert cluster.is_pushdown is False
        assert cluster.is_debug is False
        assert cluster.scheme == 'http'
        assert cluster.project == 'foobar'

    def test_service(self):
        cluster = create_kylin('kylin://foo:bar@example.com:9000/foobar?prefix=/aaa/bbb')
        assert isinstance(cluster.service, SERVICES['v1'])
        assert cluster.service.client.request_headers.get('Accept') is None
        assert cluster.service.client.request_headers.get('Authorization') == 'Basic Zm9vOmJhcg=='
        assert cluster.service.project == 'foobar'
        assert cluster.version == 'v1'
        cluster.username = 'yongjie'
        cluster.password = 'zhao'
        cluster.host = 'eager_host.com'
        cluster.port = 1234
        cluster.scheme = 'https'
        cluster.timeout = 1000
        cluster.unverified = False
        cluster.prefix = '/hello/world'
        cluster.is_debug = True
        eager_client = cluster.service.client
        assert eager_client.host == 'https://eager_host.com:1234'
        assert eager_client.prefix == '/hello/world'
        assert eager_client.unverified is False
        assert eager_client.timeout == 1000
        assert eager_client.is_debug is True
        assert cluster.service.client.request_headers.get('Authorization') == 'Basic eW9uZ2ppZTp6aGFv'

        cluster2 = create_kylin('kylin://foo:bar@example.com:9000/?version=v2')
        assert cluster2.version == 'v2'
        assert cluster2.service.client.request_headers.get('Accept') == 'application/vnd.apache.kylin-v2+json'
        assert isinstance(cluster2.service, SERVICES['v2'])

        cluster4 = create_kylin('kylin://foo:bar@example.com:9000/?version=v4')
        assert cluster4.version == 'v4'
        assert cluster4.service.client.request_headers.get('Accept') == 'application/vnd.apache.kylin-v4-public+json'
        assert isinstance(cluster4.service, SERVICES['v4'])

    def test_get_client(self):
        cluster = create_kylin('kylin://username:password@example')
        assert isinstance(cluster._get_client(), Client)

    def test_basic_auth_dump(self):
        cluster = create_kylin('kylin://username:password@example')
        assert cluster.basic_auth_dump('foo', 'bar') == {'Authorization': 'Basic Zm9vOmJhcg=='}

    def test_query(self, v1_api):
        rv = self.project.query('select count(*) from kylin_sales')
        assert 'columnMetas' in rv
        assert rv['results'] == [['10000']]

    def test_get_all_tables(self, v1_api):
        assert self.project.get_all_tables() == [
            'KYLIN_ACCOUNT',
            'KYLIN_CAL_DT',
            'KYLIN_CATEGORY_GROUPINGS',
            'KYLIN_COUNTRY',
            'KYLIN_SALES',
        ]

        pushdown = create_kylin('kylin://username:password@example/foobar?is_pushdown=1')
        assert pushdown.is_pushdown is True
        assert pushdown.get_all_tables() == [
            'KYLIN_ACCOUNT',
            'KYLIN_CAL_DT',
            'KYLIN_CATEGORY_GROUPINGS',
            'KYLIN_COUNTRY',
            'KYLIN_SALES',
            'KYLIN_STREAMING_TABLE',
        ]

    def test_get_all_schema(self, v1_api):
        assert self.project.get_all_schemas() == ['DEFAULT']

    def test_get_table_source(self, v1_api):
        table = self.project.get_table_source('KYLIN_SALES', 'DEFAULT')
        assert table.name == 'KYLIN_SALES'
        assert table.schema == 'DEFAULT'

    def test_get_table_source_with_schema(self, v1_api):
        table = self.project.get_table_source('DEFAULT.KYLIN_SALES')
        assert table.name == 'KYLIN_SALES'
        assert table.schema == 'DEFAULT'

    def test_get_table_source_error(self, v1_api):
        with pytest.raises(NoSuchTableError):
            self.project.get_table_source('foo.bar.zee.hello.world')

    def test_get_cube_source(self, v1_api):
        cube = self.project.get_cube_source('kylin_sales_cube')
        assert cube.name == 'kylin_sales_cube'
        assert cube.model_name == 'kylin_sales_model'

    def test_get_datasource(self, v1_api):
        cube = self.project.get_cube_source('kylin_sales_cube')
        assert cube.name == 'kylin_sales_cube'
        assert cube.model_name == 'kylin_sales_model'

    def test_list_job(self, v1_api):
        job_list = self.project.list_job()
        assert isinstance(job_list[0], KylinJob)
