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


class TestKE4Service(object):
    @property
    def cluster(self):
        return create_kylin('kylin://ADMIN:KYLIN@example?version=v4')

    @property
    def project(self):
        return create_kylin('kylin://ADMIN:KYLIN@example/ssb?version=v4')

    def test_projects(self, v4_api):
        rv = self.project.service.projects(headers={})
        assert [e['name'] for e in rv] == ['ssb']

    def test_jobs(self, v4_api):
        rv = self.project.service.jobs(headers={}, params={'time_filter': 4})
        assert [e['project'] for e in rv] == ['test']

    def test_tables_and_columns(self, v4_api):
        rv = self.project.service.tables_and_columns(headers={})
        assert sorted(list(rv.keys())) == [
            'DEFAULT.KYLIN_ACCOUNT',
            'DEFAULT.KYLIN_CAL_DT',
            'DEFAULT.KYLIN_CATEGORY_GROUPINGS',
            'DEFAULT.KYLIN_COUNTRY',
            'DEFAULT.KYLIN_SALES',
        ]

    def test_tables_in_hive(self, v4_api):
        rv = self.project.service.tables_in_hive(headers={})
        assert sorted(list(rv.keys())) == [
            'SSB.CUSTOMER',
            'SSB.DATES',
            'SSB.LINEORDER',
            'SSB.PART',
            'SSB.P_LINEORDER',
            'SSB.SUPPLIER',
        ]

    def test_query(self, v4_api):
        rv = self.project.service.query(sql='select count(*) from P_LINEORDER', headers={})
        assert 'columnMetas' in rv
        assert 'results' in rv

    def test_error_query(self, mocker):
        mocker.patch('kylinpy.service.KE4Service.api.query', return_value={'exceptionMessage': 'foobar'})

        with pytest.raises(KylinQueryError):
            self.project.service.query(sql='select count(*) from P_LINEORDER')

    def test_http_error_query(self, mocker):
        mc = mocker.patch('kylinpy.client.client.Client._make_request')
        mc.side_effect = MockException(500)
        with pytest.raises(HTTPError):
            self.project.service.query(sql='select count(*) from P_LINEORDER')

    def test_get_authentication(self, v4_api):
        rv = self.project.service.get_authentication(headers={})
        assert 'username' in rv
        assert 'authorities' in rv

    def test_models(self, v4_api):
        rv = self.project.service.models()
        assert 'kylin_sales_model' == rv[0]['alias']
        assert 'kylin_sales_model' == rv[0]['name']
        assert 'lookups' in rv[0]
        assert 'join_tables' in rv[0]

    def test_model_desc(self, v4_api):
        rv = self.project.service.model_desc('kylin_sales_model')
        assert rv['name'] == 'kylin_sales_model'
        assert 'measures' in rv
        assert 'dimensions' in rv
        assert 'aggregation_groups' in rv
