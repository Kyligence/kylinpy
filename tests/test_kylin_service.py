# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import pytest

from kylinpy.client import HTTPError
from kylinpy.kylinpy import create_kylin
from kylinpy.exceptions import KylinQueryError
from .test_client import MockException


class TestKylinService(object):
    @property
    def cluster(self):
        return create_kylin('kylin://ADMIN:KYLIN@example')

    @property
    def project(self):
        return create_kylin('kylin://ADMIN:KYLIN@example/learn_kylin')

    def test_projects(self, v1_api):
        rv = self.project.service.projects(headers={})
        assert [e['name'] for e in rv] == ['learn_kylin']

    def test_jobs(self, v1_api):
        rv = self.project.service.jobs(headers={}, params={'offset': 0, 'limit': 10, 'timeFilter': 4})
        assert [e['related_cube'] for e in rv] == ['Sample_Cube']

    def test_tables_and_columns(self, v1_api):
        rv = self.project.service.tables_and_columns(headers={})
        assert sorted(list(rv.keys())) == [
            'DEFAULT.KYLIN_ACCOUNT',
            'DEFAULT.KYLIN_CAL_DT',
            'DEFAULT.KYLIN_CATEGORY_GROUPINGS',
            'DEFAULT.KYLIN_COUNTRY',
            'DEFAULT.KYLIN_SALES',
        ]

    def test_tables_in_hive(self, v1_api):
        rv = self.project.service.tables_in_hive()
        assert sorted(list(rv.keys())) == [
            'DEFAULT.KYLIN_ACCOUNT',
            'DEFAULT.KYLIN_CAL_DT',
            'DEFAULT.KYLIN_CATEGORY_GROUPINGS',
            'DEFAULT.KYLIN_COUNTRY',
            'DEFAULT.KYLIN_SALES',
            'DEFAULT.KYLIN_STREAMING_TABLE',
        ]

    def test_cubes(self, v1_api):
        rv = self.project.service.cubes(headers={})
        assert [e['name'] for e in rv] == ['kylin_sales_cube', 'kylin_streaming_cube']

    def test_models(self, v1_api):
        rv = self.project.service.models(headers={})
        assert [e['name'] for e in rv] == ['kylin_sales_model', 'kylin_streaming_model']

    def test_cube_desc(self, v1_api):
        rv = self.project.service.cube_desc('kylin_sales_cube', headers={})
        assert 'dimensions' in rv
        assert 'measures' in rv
        assert rv['model_name'] == 'kylin_sales_model'
        assert rv['name'] == 'kylin_sales_cube'

    def test_model_desc(self, v1_api):
        rv = self.project.service.model_desc('kylin_sales_model', headers={})
        assert 'dimensions' in rv
        assert 'lookups' in rv
        assert 'metrics' in rv
        assert rv['name'] == 'kylin_sales_model'

    def test_query(self, v1_api):
        rv = self.project.service.query(sql='select count(*) from kylin_sales', headers={})
        assert 'columnMetas' in rv
        assert 'results' in rv

    def test_error_query(self, mocker):
        mocker.patch('kylinpy.service.KylinService.api.query', return_value={'exceptionMessage': 'foobar'})

        with pytest.raises(KylinQueryError):
            self.project.service.query(sql='select count(*) from kylin_sales')

    def test_http_error_query(self, mocker):
        mc = mocker.patch('kylinpy.client.client.Client._make_request')
        mc.side_effect = MockException(500)
        with pytest.raises(HTTPError):
            self.project.service.query(sql='select count(*) from kylin_sales')

    def test_get_authentication(self, v1_api):
        rv = self.project.service.get_authentication(headers={})
        assert 'username' in rv
        assert 'authorities' in rv
