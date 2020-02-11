import os.path
import json

from kylinpy.kylinpy import dsn_proxy


def read(filename):
    here = os.path.abspath(os.path.dirname(__file__))
    return json.load(open(os.path.join(here, 'fixtures', 'v1', filename)))


class TestKylinService(object):
    def test_projects(self, mocker):
        cluster = dsn_proxy('kylin://ADMIN:KYLIN@example')
        mc = mocker.patch('kylinpy.client.client.Client.get')
        mc.return_value.to_object = read('projects.json')

        rv = cluster.projects
        assert [e['name'] for e in rv] == ['learn_kylin']

    def test_tables_and_columns(self, mocker):
        project = dsn_proxy('kylin://ADMIN:KYLIN@example/learn_kylin')
        mc = mocker.patch('kylinpy.client.client.Client.get')
        mc.return_value.to_object = read('tables_and_columns.json')

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
        mc.return_value.to_object = read('cubes.json')

        rv = project.service.cubes
        assert [e['name'] for e in rv] == ['kylin_sales_cube', 'kylin_streaming_cube']
