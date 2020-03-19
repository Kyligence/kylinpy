# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import json as _json
import ssl

from kylinpy.utils.compat import urllib
from kylinpy.utils.compat import HTTPSHandler
from kylinpy.utils.compat import urlencode
from kylinpy.utils.compat import HTTPError
from kylinpy.client.exceptions import handle_error
from kylinpy.logger import logger


class Response(object):
    def __init__(self, response):
        self._status_code = response.getcode()
        self._body = response.read()
        self._headers = response.info()
        self._url = response.url

    @property
    def status_code(self):
        return self._status_code

    @property
    def url(self):
        return self._url

    @property
    def body(self):
        return self._body

    @property
    def headers(self):
        return self._headers

    def json(self):
        if self.body:
            return _json.loads(self.body.decode('utf-8'))
        else:
            return None


class Client(object):
    """Quickly and easily access any REST or REST-like API."""

    def __init__(
        self,
        host,
        request_headers=None,
        prefix=None,
        timeout=None,
        unverified=None,
        is_debug=False,
    ):
        self.host = host.rstrip('/')
        self.request_headers = request_headers or {}
        self.prefix = prefix
        self.timeout = timeout
        self.unverified = unverified
        self.is_debug = is_debug

    def _build_url(self, endpoint=None, params=None):
        if self.prefix:
            url = '{}/{}'.format(self.host, self.prefix.strip('/'))
        else:
            url = self.host

        if endpoint:
            url = '{}/{}'.format(url, endpoint.strip('/'))

        if params:
            params = {k: v for k, v in params.items() if v is not None}
            url_values = urlencode(sorted(params.items()), True)
            url = '{}?{}'.format(url, url_values)

        return url

    def _update_headers(self, request_headers):
        self.request_headers.update(request_headers)

    def _mask_auth_headers(self, headers):
        if self.is_debug:
            return headers

        _headers = {}
        for (key, value) in headers.items():
            if key.lower() == 'authorization':
                _headers[key] = '******'
            else:
                _headers[key] = value
        return _headers

    def _make_request(self, opener, request, timeout=None):
        timeout = timeout or self.timeout
        logger.debug("""
==========================[QUERY]===============================
method: {} 
url: {} 
headers: {} 
body: {}
==========================[QUERY]===============================""".format(  # noqa
            request.get_method(),
            request.get_full_url(),
            self._mask_auth_headers(dict(request.header_items())),
            request.data))
        try:
            return opener.open(request, timeout=timeout)
        except HTTPError as err:
            exc = handle_error(err)
            exc.__cause__ = None
            raise exc

    def _request(self, method, endpoint,
                 params=None, json=None, headers=None, timeout=None):
        method = method.upper()
        request_data = None

        if headers:
            self._update_headers(headers)
        if json:
            request_data = _json.dumps(json).encode('utf-8')

        if self.unverified:
            opener = urllib.build_opener(
                HTTPSHandler(context=ssl._create_unverified_context()),
            )
        else:
            opener = urllib.build_opener()
        request = urllib.Request(self._build_url(endpoint, params), data=request_data)
        if self.request_headers:
            for key, value in self.request_headers.items():
                request.add_header(key, value)
        if request_data and ('Content-Type' not in self.request_headers):
            request.add_header('Content-Type', 'application/json')

        request.get_method = lambda: method
        return Response(self._make_request(opener, request, timeout=timeout))

    def get(self, endpoint, params=None, **kwargs):
        return self._request('get', endpoint, params=params, **kwargs)

    def delete(self, endpoint, **kwargs):
        return self._request('delete', endpoint, **kwargs)

    def post(self, endpoint, json=None, **kwargs):
        return self._request('post', endpoint, json=json, **kwargs)

    def put(self, endpoint, json=None, **kwargs):
        return self._request('put', endpoint, json=json, **kwargs)
