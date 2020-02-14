# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from kylinpy.kylinpy import dsn_proxy, SERVICES
from kylinpy.client import Client


class TestCluster(object):
    def test_init(self):
        cluster = dsn_proxy('kylin://name@45中文:pwd12@%+@example.com:9000/')
        assert cluster.host == 'example.com'
        assert cluster.port == 9000
        assert cluster.username == 'name@45中文'
        assert cluster.password == 'pwd12@%+'
        assert cluster.auth == 'basic'
        assert cluster.is_ssl is None
        assert cluster.prefix == '/kylin/api'
        assert cluster.timeout == 30
        assert cluster.unverified is True
        assert cluster.session == ''
        assert cluster.version == 'v1'
        assert cluster.is_pushdown is False
        assert cluster.is_debug is False
        assert cluster.scheme == 'http'
        assert isinstance(cluster.service, SERVICES['v1'])

        cluster2 = dsn_proxy('kylin://name@45中文:pwd12@%+@example.com:9000/?version=v2')
        assert cluster2.version == 'v2'
        assert isinstance(cluster2.service, SERVICES['v2'])

    def test_set_user(self):
        cluster = dsn_proxy('kylin://username:password@example')
        cluster.set_user(username='foo', password='bar')
        assert cluster.username == 'foo'
        assert cluster.password == 'bar'

    def test_get_client(self):
        cluster = dsn_proxy('kylin://username:password@example')
        assert isinstance(cluster.get_client(), Client)

    def test_basic_auth(self):
        cluster = dsn_proxy('kylin://username:password@example')
        cluster.set_user(username='foo', password='bar')
        assert cluster.basic_auth({}) == {'Authorization': 'Basic Zm9vOmJhcg=='}

    def test_session_auth(self):
        cluster = dsn_proxy('kylin://username:password@example')
        assert cluster.session_auth({}) == {'Cookie': ''}

    def test_set_v2_api(self):
        cluster = dsn_proxy('kylin://username:password@example')
        assert cluster.set_v2_api({}) == {'Accept': 'application/vnd.apache.kylin-v2+json'}


class TestProject(object):
    @property
    def project(self):
        return dsn_proxy('kylin://username:password@example/foobar')

    def test_kylinpy_project(self):
        assert isinstance(self.project.service, SERVICES['v1'])
        assert self.project.service.project == 'foobar'
        assert self.project.is_pushdown is False
        assert self.project.project == 'foobar'

    def test_get_table_source(self, v1_api):
        table = self.project.get_table_source('kylin_sales')
        assert table.name == 'kylin_sales'

    def test_get_cube_source(self, v1_api):
        cube = self.project.get_cube_source('kylin_sales_cube')
        assert cube.name == 'kylin_sales_cube'
        assert cube.model_name == 'kylin_sales_model'
