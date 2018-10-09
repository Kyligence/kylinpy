# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from mock import Mock

from kylinpy import kylinpy
from kylinpy.kylinpy import Client
from kylinpy.errors import (  # noqa
    KylinUnauthorizedError,
    KylinUserDisabled,
    KylinConnectionError,
    KylinError,
    KylinConfusedResponse,
    KAPOnlyError,
)


def test_authentication():
    response = Mock()
    response.body = {'userDetails': {'authorities': {}}}
    response.headers = {}
    Client.fetch = Mock(return_value=response)
    v1 = kylinpy.Kylinpy(host='host', username='username',
                         password='password', version='v1').authentication()

    response = Mock()
    response.body = {'data': {'authorities': {}}, 'code': 000, 'msg': ''}
    response.headers = {}
    Client.fetch = Mock(return_value=response)
    v2 = kylinpy.Kylinpy(host='host', username='username',
                         password='password', version='v2').authentication()
    assert v1 == v2


def test_projects():
    response = Mock()
    response.body = []
    response.headers = {}
    Client.fetch = Mock(return_value=response)
    v1 = kylinpy.Kylinpy(host='host', username='username',
                         password='password', version='v1').projects()

    response = Mock()
    response.body = {'data': {'projects': []}, 'code': 000, 'msg': ''}
    response.headers = {}
    Client.fetch = Mock(return_value=response)
    v2 = kylinpy.Kylinpy(host='host', username='username',
                         password='password', version='v2').projects()
    assert v1 == v2


def test_query():
    response = Mock()
    response.body = {}
    response.headers = {}
    Client.fetch = Mock(return_value=response)
    v1 = kylinpy.Kylinpy(host='host', username='username',
                         password='password', version='v1').query('sql')

    response = Mock()
    response.body = {'data': {}, 'code': 000, 'msg': ''}
    response.headers = {}
    Client.fetch = Mock(return_value=response)
    v2 = kylinpy.Kylinpy(host='host', username='username',
                         password='password', version='v2').query('sql')
    assert v1 == v2


def test_tables_and_columns():
    response = Mock()
    response.body = []
    response.headers = {}
    Client.fetch = Mock(return_value=response)
    v1 = kylinpy.Kylinpy(host='host', username='username',
                         password='password', version='v1').tables_and_columns()

    response = Mock()
    response.body = {'data': [], 'code': 000, 'msg': ''}
    response.headers = {}
    Client.fetch = Mock(return_value=response)
    v2 = kylinpy.Kylinpy(host='host', username='username',
                         password='password', version='v2').tables_and_columns()
    assert v1 == v2


def test_tables():
    response = Mock()
    response.body = {}
    response.headers = {}
    Client.fetch = Mock(return_value=response)
    v1 = kylinpy.Kylinpy(host='host', username='username',
                         password='password', version='v1').tables()

    response = Mock()
    response.body = {'data': {}, 'code': 000, 'msg': ''}
    response.headers = {}
    Client.fetch = Mock(return_value=response)
    v2 = kylinpy.Kylinpy(host='host', username='username',
                         password='password', version='v2').tables()
    assert v1 == v2


def test_cubes():
    response = Mock()
    response.body = []
    response.headers = {}
    Client.fetch = Mock(return_value=response)
    v1 = kylinpy.Kylinpy(host='host', username='username',
                         password='password', version='v1').cubes()

    response = Mock()
    response.body = {'data': {'cubes': []}, 'code': 000, 'msg': ''}
    response.headers = {}
    Client.fetch = Mock(return_value=response)
    v2 = kylinpy.Kylinpy(host='host', username='username',
                         password='password', version='v2').cubes()
    assert v1 == v2


def test_cube_desc():
    response = Mock()
    response.body = []
    response.headers = {}
    Client.fetch = Mock(return_value=response)
    v1 = kylinpy.Kylinpy(host='host', username='username',
                         password='password', version='v1').cube_desc('cube_name')

    response = Mock()
    response.body = {'data': {'cube': []}, 'code': 000, 'msg': ''}
    response.headers = {}
    v2 = kylinpy.Kylinpy(host='host', username='username',
                         password='password', version='v2').cube_desc('cube_name')
    assert v1 == v2
