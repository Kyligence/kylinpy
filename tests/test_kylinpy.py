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


def clean_json(s):
    return json.dumps(json.loads(s), sort_keys=True)


def to_str(s):
    return json.dumps(s, sort_keys=True)


class MockResponse(object):
    def __init__(self, json_data):
        self.json_data = clean_json(json_data)

    def close(self):
        pass

    def getcode(self):
        pass

    def read(self):
        return self.json_data


def f(filename):
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), 'mock_response', filename)


def check_response(resp):
    return isinstance(resp, dict) and 'data' in resp and len(list(resp)) == 1


@patch('six.moves.urllib.request.urlopen')
def test_authentication(response):
    with open(f('auth_v1.json')) as fd:
        v1_str = clean_json(fd.read())

    with open(f('auth_v2.json')) as fd:
        v2_str = clean_json(fd.read())

    response.side_effect = [MockResponse(v1_str), MockResponse(v2_str)]

    assert check_response(kylinpy.Kylinpy(
        'host', 7070, 'username', 'password', version='v1').authentication())
    assert check_response(kylinpy.Kylinpy(
        'host', 7070, 'username', 'password', version='v2').authentication())


@patch('six.moves.urllib.request.urlopen')
def test_projects(response):
    with open(f('projects_v1.json')) as fd:
        v1_str = clean_json(fd.read())

    with open(f('projects_v2.json')) as fd:
        v2_str = clean_json(fd.read())

    response.side_effect = [MockResponse(v1_str), MockResponse(v2_str)]

    assert check_response(kylinpy.Kylinpy(
        'host', 7070, 'username', 'password', version='v1').projects())
    assert check_response(kylinpy.Kylinpy(
        'host', 7070, 'username', 'password', version='v2').projects())


@patch('six.moves.urllib.request.urlopen')
def test_query(response):
    with open(f('query_v1.json')) as fd:
        v1_str = clean_json(fd.read())

    with open(f('query_v2.json')) as fd:
        v2_str = clean_json(fd.read())

    sql = "select count(*) from KYLIN_ACCOUNT"

    response.side_effect = [MockResponse(v1_str), MockResponse(v2_str)]
    assert check_response(kylinpy.Kylinpy('host', 7070, 'username',
                                          'password', project='learn_kylin', version='v1').query(sql))
    assert check_response(kylinpy.Kylinpy('host', 7070, 'username',
                                          'password', project='learn_kylin', version='v2').query(sql))


@patch('six.moves.urllib.request.urlopen')
def test_tables_and_columns(response):
    with open(f('tables_and_columns_v1.json')) as fd:
        v1_str = clean_json(fd.read())

    with open(f('tables_and_columns_v2.json')) as fd:
        v2_str = clean_json(fd.read())

    response.side_effect = [MockResponse(v1_str), MockResponse(v2_str)]
    assert check_response(kylinpy.Kylinpy('host', 7070, 'username', 'password',
                                          version='v1', project='learn_kylin').tables_and_columns())
    assert check_response(kylinpy.Kylinpy('host', 7070, 'username', 'password',
                                          version='v2', project='learn_kylin').tables_and_columns())


@patch('six.moves.urllib.request.urlopen')
def test_tables(response):
    with open(f('tables_v1.json')) as fd:
        v1_str = clean_json(fd.read())

    with open(f('tables_v2.json')) as fd:
        v2_str = clean_json(fd.read())

    response.side_effect = [MockResponse(v1_str), MockResponse(v2_str)]
    assert check_response(kylinpy.Kylinpy(
        'host', 7070, 'username', 'password', version='v1', project='learn_kylin').tables())
    assert check_response(kylinpy.Kylinpy(
        'host', 7070, 'username', 'password', version='v2', project='learn_kylin').tables())


@patch('six.moves.urllib.request.urlopen')
def test_cubes(response):
    with open(f('cubes_v1.json')) as fd:
        v1_str = clean_json(fd.read())

    with open(f('cubes_v2.json')) as fd:
        v2_str = clean_json(fd.read())

    response.side_effect = [MockResponse(v1_str), MockResponse(v2_str)]
    assert check_response(kylinpy.Kylinpy(
        'host', 7070, 'username', 'password', version='v1', project='learn_kylin').cubes())
    assert check_response(kylinpy.Kylinpy(
        'host', 7070, 'username', 'password', version='v2', project='learn_kylin').cubes())


@patch('six.moves.urllib.request.urlopen')
def test_cube_sql(response):
    with open(f('cube_sql_v1.json')) as fd:
        v1_str = clean_json(fd.read())

    with open(f('cube_sql_v2.json')) as fd:
        v2_str = clean_json(fd.read())

    response.side_effect = [MockResponse(v1_str), MockResponse(v2_str)]
    # assert check_response(kylinpy.Kylinpy('host', 7070, 'username', 'password',
    #                                       version='v1', project='learn_kylin').cube_sql('kylin_sales_cube'))
    assert check_response(kylinpy.Kylinpy('host', 7070, 'username', 'password',
                                          version='v2', project='learn_kylin').cube_sql('kylin_sales_cube'))


@patch('six.moves.urllib.request.urlopen')
def test_cube_desc(response):
    with open(f('cube_desc_v1.json')) as fd:
        v1_str = clean_json(fd.read())

    with open(f('cube_desc_v2.json')) as fd:
        v2_str = clean_json(fd.read())

    response.side_effect = [MockResponse(v1_str), MockResponse(
        v2_str), MockResponse(v1_str), MockResponse(v2_str)]
    assert check_response(kylinpy.Kylinpy('host', 7070, 'username', 'password',
                                          version='v1', project='learn_kylin').cube_desc('kylin_sales_cube'))
    assert check_response(kylinpy.Kylinpy('host', 7070, 'username', 'password',
                                          version='v2', project='learn_kylin').cube_desc('kylin_sales_cube'))


@patch('six.moves.urllib.request.urlopen')
def test_users(response):
    with open(f('users_v2.json')) as fd:
        v2_str = clean_json(fd.read())

    with pytest.raises(KAPOnlyError):
        kylinpy.Kylinpy('host', 7070, 'username',
                        'password', version='v1').users()

    response.side_effect = [MockResponse(v2_str)]
    assert check_response(kylinpy.Kylinpy(
        'host', 7070, 'username', 'password', version='v2').users())


# =================================================


@patch('six.moves.urllib.request.urlopen')
def test_get_table_names_v1(response):
    with open(f('tables_and_columns_v1.json')) as fd:
        v1_str = clean_json(fd.read())

    with open(f('get_table_names_v1.json')) as fd:
        get_table_names_str = clean_json(fd.read())

    response.side_effect = [MockResponse(v1_str)]
    assert to_str(kylinpy.Kylinpy('host', 7070, 'username', 'password', version='v1',
                                  project='learn_kylin').get_table_names()) == get_table_names_str


@patch('six.moves.urllib.request.urlopen')
def test_get_table_names_v2(response):
    with open(f('tables_and_columns_v2.json')) as fd:
        v2_str = clean_json(fd.read())

    with open(f('get_table_names_v2.json')) as fd:
        get_table_names_str = clean_json(fd.read())

    response.side_effect = [MockResponse(v2_str)]
    assert to_str(kylinpy.Kylinpy('host', 7070, 'username', 'password', version='v2',
                                  project='learn_kylin').get_table_names()) == get_table_names_str


@patch('six.moves.urllib.request.urlopen')
def test_get_table_columns_v1(response):
    with open(f('tables_and_columns_v1.json')) as fd:
        v1_step1 = clean_json(fd.read())
    with open(f('tables_v1.json')) as fd:
        v1_step2 = clean_json(fd.read())
    with open(f('get_table_columns_v1.json')) as fd:
        get_table_columns_v1_str = clean_json(fd.read())

    response.side_effect = [MockResponse(v1_step1), MockResponse(v1_step2)]
    assert to_str(kylinpy.Kylinpy('host', 7070, 'username', 'password', version='v1',
                                  project='learn_kylin').get_table_columns('KYLIN_SALES')) == get_table_columns_v1_str


@patch('six.moves.urllib.request.urlopen')
def test_get_table_columns_v2(response):
    with open(f('tables_and_columns_v2.json')) as fd:
        v2_step1 = clean_json(fd.read())
    with open(f('tables_v2.json')) as fd:
        v2_step2 = clean_json(fd.read())
    with open(f('get_table_columns_v2.json')) as fd:
        get_table_columns_v2_str = clean_json(fd.read())

    response.side_effect = [MockResponse(v2_step1), MockResponse(v2_step2)]
    assert to_str(kylinpy.Kylinpy('host', 7070, 'username', 'password', version='v2',
                                  project='learn_kylin').get_table_columns('KYLIN_SALES')) == get_table_columns_v2_str

# @patch('six.moves.urllib.request.urlopen')
# def test_get_cube_names(response):
#     with open(f('cubes_v1.json')) as fd:
#         v1_str = clean_json(fd.read())

#     with open(f('cubes_v2.json')) as fd:
#         v2_str = clean_json(fd.read())

#     with open(f('get_cube_names.json')) as fd:
#         get_cube_names_str = clean_json(fd.read())

#     response.side_effect = [MockResponse(v1_str), MockResponse(v2_str)]
#     assert to_str(kylinpy.Kylinpy('host', 7070, 'username', 'password', version='v1', project='learn_kylin').get_cube_names()) == get_cube_names_str
#     assert to_str(kylinpy.Kylinpy('host', 7070, 'username', 'password', version='v2', project='learn_kylin').get_cube_names()) == get_cube_names_str

# @patch('six.moves.urllib.request.urlopen')
# def test_get_cube_columns(response):
#     with open(f('cube_desc_v1.json')) as fd:
#         v1_step1 = clean_json(fd.read())
#     with open(f('tables_v1.json')) as fd:
#         v1_step2 = clean_json(fd.read())

#     with open(f('cube_desc_v2.json')) as fd:
#         v2_step1 = clean_json(fd.read())
#     with open(f('tables_v2.json')) as fd:
#         v2_step2 = clean_json(fd.read())

#     with open(f('get_cube_columns_v1.json')) as fd:
#         get_cube_columns_v1_str = clean_json(fd.read())
#     with open(f('get_cube_columns_v2.json')) as fd:
#         get_cube_columns_v2_str = clean_json(fd.read())

#     response.side_effect = [MockResponse(v1_step1), MockResponse(v1_step2), MockResponse(v2_step1), MockResponse(v2_step2)]
#     assert to_str(kylinpy.Kylinpy('host', 7070, 'username', 'password', version='v1', project='learn_kylin').get_cube_columns('kylin_sales_cube')) == get_cube_columns_v1_str
#     assert to_str(kylinpy.Kylinpy('host', 7070, 'username', 'password', version='v2', project='learn_kylin').get_cube_columns('kylin_sales_cube')) == get_cube_columns_v2_str
