# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import json

import pytest

from kylinpy.client import Client
from kylinpy.client.exceptions import HTTPError
from kylinpy.utils.compat import urllib


class MockException(HTTPError):
    def __init__(self, code):
        self.code = code
        self.reason = 'REASON'
        self.hdrs = 'HEADERS'

    def read(self):
        return 'BODY'


class MockResponse(urllib.HTTPSHandler):
    def __init__(self, response_code):
        self.response_code = response_code
        self.url = '/foo/bar'

    def getcode(self):
        return self.response_code

    def info(self):
        return 'HEADERS'

    def read(self):
        return json.dumps({'hello': 'world'}).encode('utf-8')


class TestClient(object):
    @pytest.fixture
    def client(self):
        host = 'http://example/'
        request_headers = {'Content-Type': 'application/json'}
        client = Client(host=host, request_headers=request_headers)
        yield client

    def test_init(self, client):
        assert client.host == 'http://example'
        assert client.request_headers == {'Content-Type': 'application/json'}
        assert client.prefix is None
        assert client.timeout is None
        assert client.unverified is None
        assert client.is_debug is False

    def test_get(self, client, mocker):
        mocker.patch('kylinpy.client.client.Client._make_request', return_value=MockResponse(200))

        rv = client.get('/foo/bar', params={'param1': 'x', 'param2': 'y'})
        assert rv.url == '/foo/bar'
        assert rv.status_code == 200
        assert rv.headers == 'HEADERS'
        assert rv.json() == {'hello': 'world'}

    def test_post(self, client, mocker):
        mocker.patch('kylinpy.client.client.Client._make_request', return_value=MockResponse(200))

        rv = client.post('/foo/bar', params={'param1': 'x', 'param2': 'y'})
        assert rv.url == '/foo/bar'
        assert rv.status_code == 200
        assert rv.headers == 'HEADERS'
        assert rv.json() == {'hello': 'world'}

    def test_put(self, client, mocker):
        mocker.patch('kylinpy.client.client.Client._make_request', return_value=MockResponse(200))

        rv = client.post('/foo/bar', params={'param1': 'x', 'param2': 'y'})
        assert rv.url == '/foo/bar'
        assert rv.status_code == 200
        assert rv.headers == 'HEADERS'
        assert rv.json() == {'hello': 'world'}

    def test_delete(self, client, mocker):
        mocker.patch('kylinpy.client.client.Client._make_request', return_value=MockResponse(200))

        rv = client.delete('/foo/bar', params={'param1': 'x', 'param2': 'y'})
        assert rv.url == '/foo/bar'
        assert rv.status_code == 200
        assert rv.headers == 'HEADERS'
        assert rv.json() == {'hello': 'world'}
