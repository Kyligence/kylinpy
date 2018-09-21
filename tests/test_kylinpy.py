# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import json

from mock import patch

from kylinpy import kylinpy
from kylinpy.errors import (  # noqa
    KylinUnauthorizedError,
    KylinUserDisabled,
    KylinConnectionError,
    KylinError,
    KylinConfusedResponse,
    KAPOnlyError,
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
        MockResponse({'data': {'authorities': {}}, 'code': 000, 'msg': ''}),
    ]
    v1 = kylinpy.Kylinpy(host='host', username='username',
                         password='password', version='v1').authentication()
    v2 = kylinpy.Kylinpy(host='host', username='username',
                         password='password', version='v2').authentication()
    assert v1 == v2


@patch('six.moves.urllib.request.urlopen')
def test_projects(response):
    response.side_effect = [
        MockResponse([]),
        MockResponse({'data': {'projects': []}, 'code': 000, 'msg': ''}),
    ]
    v1 = kylinpy.Kylinpy(host='host', username='username',
                         password='password', version='v1').projects()
    v2 = kylinpy.Kylinpy(host='host', username='username',
                         password='password', version='v2').projects()
    assert v1 == v2


@patch('six.moves.urllib.request.urlopen')
def test_query(response):
    response.side_effect = [
        MockResponse({}),
        MockResponse({'data': {}, 'code': 000, 'msg': ''}),
    ]
    v1 = kylinpy.Kylinpy(host='host', username='username',
                         password='password', version='v1').query('sql')
    v2 = kylinpy.Kylinpy(host='host', username='username',
                         password='password', version='v2').query('sql')
    assert v1 == v2


@patch('six.moves.urllib.request.urlopen')
def test_tables_and_columns(response):
    response.side_effect = [
        MockResponse([]),
        MockResponse({'data': [], 'code': 000, 'msg': ''}),
    ]
    v1 = kylinpy.Kylinpy(host='host', username='username',
                         password='password', version='v1').tables_and_columns()
    v2 = kylinpy.Kylinpy(host='host', username='username',
                         password='password', version='v2').tables_and_columns()
    assert v1 == v2


@patch('six.moves.urllib.request.urlopen')
def test_tables(response):
    response.side_effect = [
        MockResponse({}),
        MockResponse({'data': {}, 'code': 000, 'msg': ''}),
    ]
    v1 = kylinpy.Kylinpy(host='host', username='username',
                         password='password', version='v1').tables()
    v2 = kylinpy.Kylinpy(host='host', username='username',
                         password='password', version='v2').tables()
    assert v1 == v2


@patch('six.moves.urllib.request.urlopen')
def test_cubes(response):
    response.side_effect = [
        MockResponse([]),
        MockResponse({'data': {'cubes': []}, 'code': 000, 'msg': ''}),
    ]

    v1 = kylinpy.Kylinpy(host='host', username='username',
                         password='password', version='v1').cubes()
    v2 = kylinpy.Kylinpy(host='host', username='username',
                         password='password', version='v2').cubes()
    assert v1 == v2


@patch('six.moves.urllib.request.urlopen')
def test_cube_desc(response):
    response.side_effect = [
        MockResponse({}),
        MockResponse({'data': {'cube': {}}, 'code': 000, 'msg': ''}),
    ]

    v1 = kylinpy.Kylinpy(host='host', username='username',
                         password='password', version='v1').cube_desc('cube_name')
    v2 = kylinpy.Kylinpy(host='host', username='username',
                         password='password', version='v2').cube_desc('cube_name')
    assert v1 == v2
