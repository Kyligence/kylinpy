# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import pytest

from kylinpy.client import HTTPError
from kylinpy.kylinpy import dsn_proxy
from kylinpy.exceptions import KylinQueryError
from .test_client import MockException


class TestKE4Service(object):
    @property
    def cluster(self):
        return dsn_proxy('kylin://ADMIN:KYLIN@example?version=v4')

    @property
    def project(self):
        return dsn_proxy('kylin://ADMIN:KYLIN@example/ssb?version=v4')

    def test_projects(self, v4_api):
        rv = self.project.service.projects(headers={})
        assert [e['name'] for e in rv] == ['ssb']

    def test_tables_and_columns(self, v4_api):
        rv = self.project.service.tables_and_columns(headers={})
        assert sorted(list(rv.keys())) == [
            'SSB.CUSTOMER',
            'SSB.DATES',
            'SSB.LINEORDER',
            'SSB.PART',
            'SSB.P_LINEORDER',
            'SSB.SUPPLIER',
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
