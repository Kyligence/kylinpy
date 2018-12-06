# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import json
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

    @property
    def to_object(self):
        """
        :return: object of response from the API
        """
        if self.body:
            ct = dict(self.headers).get('Content-Type')
            if ct and str(ct).startswith('application/vnd.apache.kylin-v2+json'):
                return json.loads(self.body.decode('utf-8')).get('data')
            return json.loads(self.body.decode('utf-8'))
        else:
            return None


class Client(object):
    """Quickly and easily access any REST or REST-like API."""

    def __init__(self,
                 host,
                 request_headers=None,
                 prefix=None,
                 url_path=None,
                 append_slash=False,
                 timeout=None,
                 unverified=None):
        """
        :param host: Base URL for the api. (e.g. https://api.sendgrid.com)
        :type host:  string
        :param request_headers: A dictionary of the headers you want
                                applied on all calls
        :type request_headers: dictionary
        :param prefix: The prefix of the API.
                        Subclass _build_prefix_url for custom behavior.
        :type prefix: string
        :param url_path: A list of the url path segments
        :type url_path: list of strings
        """
        self.host = host
        self.request_headers = request_headers or {}
        self._prefix = prefix.strip('/') if prefix else None
        # _url_path keeps track of the dynamically built url
        self._url_path = url_path or []
        # These are the supported HTTP verbs
        self.methods = ['delete', 'get', 'patch', 'post', 'put']
        # APPEND SLASH set
        self.append_slash = append_slash
        self.timeout = timeout
        self.unverified = unverified

    def _build_prefix_url(self, url):
        """pass the prefix of the URL
        :param url: URI portion of the full URL being requested
        :type url: string
        :return: string
        """
        return '{}/{}{}'.format(self.host, str(self._prefix), url)

    def _build_url(self, query_params):
        """Build the final URL to be passed to urllib

        :param query_params: A dictionary of all the query parameters
        :type query_params: dictionary
        :return: string
        """
        url = ''
        count = 0
        while count < len(self._url_path):
            url += '/{}'.format(self._url_path[count])
            count += 1

        # add slash
        if self.append_slash:
            url += '/'

        if query_params:
            url_values = urlencode(sorted(query_params.items()), True)
            url = '{}?{}'.format(url, url_values)

        if self._prefix:
            url = self._build_prefix_url(url)
        else:
            url = self.host + url
        return url

    def _update_headers(self, request_headers):
        """Update the headers for the request

        :param request_headers: headers to set for the API call
        :type request_headers: dictionary
        :return: dictionary
        """
        self.request_headers.update(request_headers)

    def _build_client(self, name=None):
        """Make a new Client object

        :param name: Name of the url segment
        :type name: string
        :return: A Client object
        """
        url_path = self._url_path + [name] if name else self._url_path
        return Client(host=self.host,
                      prefix=self._prefix,
                      request_headers=self.request_headers,
                      url_path=url_path,
                      append_slash=self.append_slash,
                      timeout=self.timeout,
                      unverified=self.unverified)

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
==========================[QUERY]===============================
""".format(  # noqa
            request.get_method(),
            request.get_full_url(),
            dict(request.header_items()),
            request.data))
        try:
            return opener.open(request, timeout=timeout)
        except HTTPError as err:
            exc = handle_error(err)
            exc.__cause__ = None
            raise exc

    def _(self, name):
        """Add variable values to the url.
           (e.g. /your/api/{variable_value}/call)
           Another example: if you have a Python reserved word, such as global,
           in your url, you must use this method.

        :param name: Name of the url segment
        :type name: string
        :return: Client object
        """
        return self._build_client(name)

    def __getattr__(self, name):
        """Dynamically add method calls to the url, then call a method.
           (e.g. client.name.name.method())

        :param name: Name of the url segment or method call
        """
        # We have reached the end of the method chain, make the API call
        if name in self.methods:
            method = name.upper()

            def http_request(*_, **kwargs):
                """Make the API call
                :param args: unused
                :param kwargs:
                :return: Client object
                """
                if 'request_headers' in kwargs:
                    self._update_headers(kwargs['request_headers'])
                if 'request_body' not in kwargs:
                    data = None
                else:
                    # Don't serialize to a JSON formatted str
                    # if we don't have a JSON Content-Type
                    if 'Content-Type' in self.request_headers:
                        if self.request_headers['Content-Type'] != 'application\
                        /json':
                            data = kwargs['request_body'].encode('utf-8')
                        else:
                            data = json.dumps(
                                kwargs['request_body']).encode('utf-8')
                    else:
                        data = json.dumps(
                            kwargs['request_body']).encode('utf-8')

                if 'query_params' in kwargs:
                    params = kwargs['query_params']
                else:
                    params = None

                if self.unverified:
                    opener = urllib.build_opener(
                        HTTPSHandler(context=ssl._create_unverified_context()),
                    )
                else:
                    opener = urllib.build_opener()
                request = urllib.Request(self._build_url(params), data=data)
                if self.request_headers:
                    for key, value in self.request_headers.items():
                        request.add_header(key, value)
                if data and ('Content-Type' not in self.request_headers):
                    request.add_header('Content-Type', 'application/json')
                request.get_method = lambda: method
                timeout = kwargs.pop('timeout', None)
                return Response(self._make_request(opener, request, timeout=timeout))
            return http_request
        else:
            # Add a segment to the URL
            return self._(name)

    def __getstate__(self):
        return self.__dict__

    def __setstate__(self, state):
        self.__dict__ = state
