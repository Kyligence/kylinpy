# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import pickle
import unittest

from kylinpy.client import Client
from kylinpy.client.exceptions import (
    BadRequestsError,
    handle_error,
    HTTPError,
    NotFoundError,
    ServiceUnavailableError,
    UnsupportedMediaTypeError,
)

try:
    # Python 3
    import urllib.request as urllib
except ImportError:
    # Python 2
    import urllib2 as urllib

try:
    basestring
except NameError:
    basestring = str


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

    def getcode(self):
        return self.response_code

    def info(self):
        return 'HEADERS'

    def read(self):
        return 'RESPONSE BODY'

    def url(self):
        return 'URL'


class MockClient(Client):

    def __init__(self, host, response_code, timeout=None):
        self.response_code = 200
        Client.__init__(self, host)

    def _make_request(self, opener, request, timeout=None):
        if 200 <= self.response_code < 299:  # if successful code
            return MockResponse(self.response_code)
        else:
            raise handle_error(MockException(self.response_code))


class TestClient(unittest.TestCase):

    def setUp(self):
        self.host = 'http://sandbox'
        self.request_headers = {
            'Content-Type': 'application/json',
        }
        self.client = Client(host=self.host,
                             request_headers=self.request_headers)

    def test__init__(self):
        default_client = Client(host=self.host)
        self.assertEqual(default_client.host, self.host)
        self.assertEqual(default_client.request_headers, {})
        self.assertIs(default_client.timeout, None)
        methods = ['delete', 'get', 'patch', 'post', 'put']
        self.assertEqual(default_client.methods, methods)
        self.assertEqual(default_client._url_path, [])

        request_headers = {'X-Test': 'test', 'X-Test2': 1}
        client = Client(host=self.host,
                        request_headers=request_headers,
                        timeout=10)
        self.assertEqual(client.host, self.host)
        self.assertEqual(client.request_headers, request_headers)
        methods = ['delete', 'get', 'patch', 'post', 'put']
        self.assertEqual(client.methods, methods)
        self.assertEqual(client._url_path, [])
        self.assertEqual(client.timeout, 10)

    def test__build_url(self):
        self.client._url_path = self.client._url_path + ['here']
        self.client._url_path = self.client._url_path + ['there']
        self.client._url_path = self.client._url_path + [1]
        url = '{}{}'.format(
            self.host,
            '/here/there/1?hello=0&world=1&ztest=0&ztest=1',
        )
        query_params = {'hello': 0, 'world': 1, 'ztest': [0, 1]}
        built_url = self.client._build_url(query_params)
        self.assertEqual(built_url, url)

    def test__update_headers(self):
        request_headers = {'X-Test': 'Test'}
        self.client._update_headers(request_headers)
        self.assertIn('X-Test', self.client.request_headers)
        self.client.request_headers.pop('X-Test', None)

    def test__build_client(self):
        new_client = self.client._build_client('test')
        self.assertTrue(new_client)

    def test__(self):
        self.assertEqual(self.client._url_path, [])
        client = self.client._('hello')
        url_path = ['hello']
        self.assertEqual(client._url_path[0], url_path[0])

    def test__getattr__(self):
        mock_client = MockClient(self.host, 200)
        client = mock_client.__getattr__('hello')
        url_path = ['hello']
        self.assertEqual(client._url_path, url_path)
        self.assertEqual(client.__getattr__('get').__name__, 'http_request')

        # Test GET
        mock_client._url_path + ['test']
        r = mock_client.get()
        self.assertEqual(r.status_code, 200)

        # Test POST
        r = mock_client.put()
        self.assertEqual(r.status_code, 200)

        # Test PATCH
        r = mock_client.patch()
        self.assertEqual(r.status_code, 200)

        # Test POST
        mock_client.response_code = 201
        r = mock_client.post()
        self.assertEqual(r.status_code, 201)

        # Test DELETE
        mock_client.response_code = 204
        r = mock_client.delete()
        self.assertEqual(r.status_code, 204)

        mock_client.response_code = 400
        self.assertRaises(BadRequestsError, mock_client.get)

        mock_client.response_code = 404
        self.assertRaises(NotFoundError, mock_client.post)

        mock_client.response_code = 415
        self.assertRaises(UnsupportedMediaTypeError, mock_client.patch)

        mock_client.response_code = 503
        self.assertRaises(ServiceUnavailableError, mock_client.delete)

        mock_client.response_code = 523
        self.assertRaises(HTTPError, mock_client.delete)

    def test_client_pickle_unpickle(self):
        pickled_client = pickle.dumps(self.client)
        unpickled_client = pickle.loads(pickled_client)
        self.assertDictEqual(
            self.client.__dict__,
            unpickled_client.__dict__,
            'original client and unpickled client must have the same state',
        )


if __name__ == '__main__':
    unittest.main()
