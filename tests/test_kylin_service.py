import os.path
import json

import pytest

from kylinpy.client import HTTPError
from kylinpy.kylinpy import dsn_proxy
from kylinpy.exceptions import KylinQueryError

from .test_client import MockException


def read(filename):
    here = os.path.abspath(os.path.dirname(__file__))
    return json.load(open(os.path.join(here, 'fixtures', 'v1', filename)))


class TestKylinService(object):
    def test_projects(self, mocker):
        cluster = dsn_proxy('kylin://ADMIN:KYLIN@example')
        mc = mocker.patch('kylinpy.client.client.Client.get')
        mc.return_value.json.return_value = read('projects.json')

        rv = cluster.projects
        assert [e['name'] for e in rv] == ['learn_kylin']

    def test_tables_and_columns(self, mocker):
        project = dsn_proxy('kylin://ADMIN:KYLIN@example/learn_kylin')
        mc = mocker.patch('kylinpy.client.client.Client.get')
        mc.return_value.json.return_value = read('tables_and_columns.json')

        rv = project.service.tables_and_columns
        assert list(rv.keys()) == [
            'DEFAULT.KYLIN_ACCOUNT',
            'DEFAULT.KYLIN_CAL_DT',
            'DEFAULT.KYLIN_CATEGORY_GROUPINGS',
            'DEFAULT.KYLIN_COUNTRY',
            'DEFAULT.KYLIN_SALES',
        ]

    def test_cubes(self, mocker):
        project = dsn_proxy('kylin://ADMIN:KYLIN@example/learn_kylin')
        mc = mocker.patch('kylinpy.client.client.Client.get')
        mc.return_value.json.return_value = read('cubes.json')

        rv = project.service.cubes
        assert [e['name'] for e in rv] == ['kylin_sales_cube', 'kylin_streaming_cube']

    def test_models(self, mocker):
        project = dsn_proxy('kylin://ADMIN:KYLIN@example/learn_kylin')
        mc = mocker.patch('kylinpy.client.client.Client.get')
        mc.return_value.json.return_value = read('models.json')

        rv = project.service.models
        assert [e['name'] for e in rv] == ['kylin_sales_model', 'kylin_streaming_model']

    def test_cube_desc(self, mocker):
        project = dsn_proxy('kylin://ADMIN:KYLIN@example/learn_kylin')
        mc = mocker.patch('kylinpy.client.client.Client.get')
        mc.return_value.json.return_value = read('cube_desc.json')

        rv = project.service.cube_desc('kylin_sales_cube')
        assert 'dimensions' in rv
        assert 'measures' in rv
        assert rv['model_name'] == 'kylin_sales_model'
        assert rv['name'] == 'kylin_sales_cube'

    def test_model_desc(self, mocker):
        project = dsn_proxy('kylin://ADMIN:KYLIN@example/learn_kylin')
        mc = mocker.patch('kylinpy.client.client.Client.get')
        mc.return_value.json.return_value = read('models.json')

        rv = project.service.model_desc('kylin_sales_model')
        assert 'dimensions' in rv
        assert 'lookups' in rv
        assert 'metrics' in rv
        assert rv['name'] == 'kylin_sales_model'

    def test_query(self, mocker):
        project = dsn_proxy('kylin://ADMIN:KYLIN@example/learn_kylin')
        mc = mocker.patch('kylinpy.client.client.Client.post')
        mc.return_value.json.return_value = read('query.json')
        rv = project.service.query(sql='select count(*) from kylin_sales')
        assert 'columnMetas' in rv
        assert 'results' in rv

        project = dsn_proxy('kylin://ADMIN:KYLIN@example/learn_kylin')
        mc = mocker.patch('kylinpy.client.client.Client.post')
        response = read('query.json')
        response['exceptionMessage'] = 'foobar'
        mc.return_value.json.return_value = response
        with pytest.raises(KylinQueryError):
            project.service.query(sql='select count(*) from kylin_sales')

    def test_http_error_query(self, mocker):
        project = dsn_proxy('kylin://ADMIN:KYLIN@example/learn_kylin')
        mc = mocker.patch('kylinpy.client.client.Client._make_request')
        mc.side_effect = MockException(500)
        with pytest.raises(HTTPError):
            project.service.query(sql='select count(*) from kylin_sales')
