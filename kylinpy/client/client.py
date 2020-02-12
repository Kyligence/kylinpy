# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import json as _json
import ssl

from .exceptions import handle_error
from ..logger import logger

try:
    # Python 3
    import urllib.request as urllib
    from urllib.request import HTTPSHandler
    from urllib.parse import urlencode
    from urllib.error import HTTPError
except ImportError:
    # Python 2
    import urllib2 as urllib
    from urllib2 import HTTPError
    from urllib2 import HTTPSHandler
    from urllib import urlencode


class Response(object):
    """Holds the response from an API call."""

    def __init__(self, response):
        """
        :param response: The return value from a open call
                         on a urllib.build_opener()
        :type response:  urllib response object
        """
        self._status_code = response.getcode()
        self._body = response.read()
        self._headers = response.info()
        self._url = response.url

    @property
    def status_code(self):
        """
        :return: integer, status code of API call
        """
        return self._status_code

    @property
    def url(self):
        """
        :return: string, url of API call
        """
        return self._url

    @property
    def body(self):
        """
        :return: response from the API
        """
        return self._body

    @property
    def headers(self):
        """
        :return: dict of response headers
        """
        return self._headers

    def json(self):
        """
        :return: object of response from the API
        """
        if self.body:
            ct = dict(self.headers).get('Content-Type')
            if ct and str(ct).startswith('application/vnd.apache.kylin-v2+json'):
                return _json.loads(self.body.decode('utf-8')).get('data')
            return _json.loads(self.body.decode('utf-8'))
        else:
            return None


class Client(object):
    """Quickly and easily access any REST or REST-like API."""

    def __init__(self,
                 host,
                 request_headers=None,
                 prefix=None,
                 timeout=None,
                 unverified=None,
                 mask_auth=True):
        """
        :param host: Base URL for the api. (e.g. https://api.sendgrid.com)
        :type host:  string
        :param request_headers: A dictionary of the headers you want
                                applied on all calls
        :type request_headers: dictionary
        :param prefix: The prefix of the API.
                        Subclass _build_prefix_url for custom behavior.
        :type prefix: string
        """
        self.host = host.rstrip('/')
        self.request_headers = request_headers or {}
        self.prefix = prefix.strip('/') if prefix else None
        self.timeout = timeout
        self.unverified = unverified
        self.mask_auth = mask_auth

    def _build_url(self, endpoint=None, params=None):
        """Build the final URL to be passed to urllib

        :param params: A dictionary of all the query parameters
        :type params: dictionary
        :return: string
        """
        if self.prefix:
            url = '{}/{}'.format(self.host, self.prefix)
        else:
            url = self.host

        if endpoint:
            url = '{}/{}'.format(url, endpoint.strip('/'))

        if params:
            url_values = urlencode(sorted(params.items()), True)
            url = '{}?{}'.format(url, url_values)

        return url

    def _update_headers(self, request_headers):
        """Update the headers for the request

        :param request_headers: headers to set for the API call
        :type request_headers: dictionary
        :return: dictionary
        """
        self.request_headers.update(request_headers)

    def _mask_auth_headers(self, headers):
        if not self.mask_auth:
            return headers

        _headers = {}
        for (key, value) in headers.items():
            if key.lower() == 'authorization':
                _headers[key] = '******'
            else:
                _headers[key] = value
        return _headers

    def _make_request(self, opener, request, timeout=None):
        """Make the API call and return the response. This is separated into
           it's own function, so we can mock it easily for testing.

        :param opener:
        :type opener:
        :param request: url payload to request
        :type request: urllib.Request object
        :param timeout: timeout value or None
        :type timeout: float
        :return: urllib response
        """
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
