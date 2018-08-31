import os
import json

import pytest
from mock import patch
from kylinpy import kylinpy
from kylinpy.errors import (  # noqa
    KylinUnauthorizedError,
    KylinUserDisabled,
    KylinConnectionError,
    KylinError,
    KylinConfusedResponse,
    KAPOnlyError
)


class MockResponse(object):
    def __init__(self, json_data):
        self.json_data = json.dumps(json_data, sort_keys=True)

    def close(self):
        pass

    def getcode(self):
        pass

    def info(self):
        return {}

    def read(self):
        return self.json_data


@patch('six.moves.urllib.request.urlopen')
def test_authentication(response):
    response.side_effect = [
        MockResponse({'userDetails': {'authorities': {}}}),
        MockResponse({'data': {'authorities': {}}, 'code': 000, 'msg': ''})
    ]
    v1 = kylinpy.Kylinpy(host='host', username='username', password='password', version='v1').authentication()
    v2 = kylinpy.Kylinpy(host='host', username='username', password='password', version='v2').authentication()
    assert v1== v2


@patch('six.moves.urllib.request.urlopen')
def test_projects(response):
    response.side_effect = [
        MockResponse([]),
        MockResponse({'data': {'projects': []}, 'code': 000, 'msg': ''})
    ]
    v1 = kylinpy.Kylinpy(host='host', username='username', password='password', version='v1').projects()
    v2 = kylinpy.Kylinpy(host='host', username='username', password='password', version='v2').projects()
    assert v1 == v2


@patch('six.moves.urllib.request.urlopen')
def test_query(response):
    response.side_effect = [
        MockResponse({}),
        MockResponse({'data': {}, 'code': 000, 'msg': ''})
    ]
    v1 = kylinpy.Kylinpy(host='host', username='username', password='password', version='v1').query('sql')
    v2 = kylinpy.Kylinpy(host='host', username='username', password='password', version='v2').query('sql')
    assert v1 == v2


@patch('six.moves.urllib.request.urlopen')
def test_tables_and_columns(response):
    response.side_effect = [
        MockResponse([]),
        MockResponse({'data': [], 'code': 000, 'msg': ''})
    ]
    v1 = kylinpy.Kylinpy(host='host', username='username', password='password', version='v1').tables_and_columns()
    v2 = kylinpy.Kylinpy(host='host', username='username', password='password', version='v2').tables_and_columns()
    assert v1 == v2


@patch('six.moves.urllib.request.urlopen')
def test_tables(response):
    response.side_effect = [
        MockResponse({}),
        MockResponse({'data': {}, 'code': 000, 'msg': ''})
    ]
    v1 = kylinpy.Kylinpy(host='host', username='username', password='password', version='v1').tables()
    v2 = kylinpy.Kylinpy(host='host', username='username', password='password', version='v2').tables()
    assert v1 == v2


@patch('six.moves.urllib.request.urlopen')
def test_cubes(response):
    response.side_effect = [
        MockResponse([]),
        MockResponse({'data': {'cubes': []}, 'code': 000, 'msg': ''})
    ]

    v1 = kylinpy.Kylinpy(host='host', username='username', password='password', version='v1').cubes()
    v2 = kylinpy.Kylinpy(host='host', username='username', password='password', version='v2').cubes()
    assert v1 == v2


@patch('six.moves.urllib.request.urlopen')
def test_cube_desc(response):
    response.side_effect = [
        MockResponse({}),
        MockResponse({'data': {'cube': {}}, 'code': 000, 'msg': ''})
    ]

    v1 = kylinpy.Kylinpy(host='host', username='username', password='password', version='v1').cube_desc('cube_name')
    v2 = kylinpy.Kylinpy(host='host', username='username', password='password', version='v2').cube_desc('cube_name')
    assert v1 == v2


# @patch('six.moves.urllib.request.urlopen')
# def test_users(response):
#     with pytest.raises(KAPOnlyError):
#         kylinpy.Kylinpy(host='host', username='username', password='password', version='v1').users()

#     response.side_effect = [
#         MockResponse({'data': {'users': []}, 'code': 000, 'msg': ''})
#     ]
#     v2 = kylinpy.Kylinpy(host='host', username='username', password='password', version='v2').users()
#     assert v2 == {'data': []}


# @patch('six.moves.urllib.request.urlopen')
# def test_model_desc(response):
#     response.side_effect = [
#         MockResponse([{'name': 'model_name'}]),
#         MockResponse({'data': {'model': {'name': 'model_name'}}, 'code': 000, 'msg': ''})
#     ]

#     v1 = kylinpy.Kylinpy(host='host', username='username', password='password', version='v1').model_desc('model_name')
#     v2 = kylinpy.Kylinpy(host='host', username='username', password='password', version='v2').model_desc('model_name')
#     assert v1 == v2


# # =================================================


# @patch('six.moves.urllib.request.urlopen')
# def test_get_table_names(response):
#     response.side_effect = [
#         MockResponse([{'table_NAME': 'table1'}]),
#         MockResponse({'data': [{'table_NAME': 'table1'}], 'code': 000, 'msg': ''})
#     ]
#     v1 = kylinpy.Kylinpy(host='host', username='username', password='password', version='v1').get_table_names()
#     v2 = kylinpy.Kylinpy(host='host', username='username', password='password', version='v2').get_table_names()
#     assert v1.body == v2.body


# @patch('six.moves.urllib.request.urlopen')
# def test_list_schemas(response):
#     response.side_effect = [
#         MockResponse([{'table_NAME': 'table1', 'table_SCHEM': 'schema1'}]),
#         MockResponse({'data': [{'table_NAME': 'table1', 'table_SCHEM': 'schema1'}], 'code': 000, 'msg': ''})
#     ]
#     v1 = kylinpy.Kylinpy(host='host', username='username', password='password', version='v1').list_schemas()
#     v2 = kylinpy.Kylinpy(host='host', username='username', password='password', version='v2').list_schemas()
#     assert v1.body == v2.body

