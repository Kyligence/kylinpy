# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import base64
import logging
try:
    # Python 3
    import urllib.request as urllib
except ImportError:
    # Python 2
    import urllib2 as urllib

from kylinpy.client import Client as HTTPClient
from kylinpy.exceptions import NoSuchTableError
from kylinpy.service import KylinService, V2Service
from kylinpy.datasource import CubeSource, TableSource
from kylinpy.utils.compat import as_unicode

SERVICES = {
    'v1': KylinService,
    'v2': V2Service,
}


class Cluster(object):
    def __init__(self, host, username=None, password=None, port=7070, **connect_args):
        if host.startswith(('http://', 'https://')):
            _, self.host = host.split('://')
        else:
            self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.auth = connect_args.get('auth', 'basic')
        self.is_ssl = connect_args.get('is_ssl', None)
        self.prefix = connect_args.get('prefix', 'kylin/api')
        self.timeout = connect_args.get('timeout', 30)
        self.unverified = bool(connect_args.get('unverified', True))
        self.session = connect_args.get('session', '')
        self.version = connect_args.get('version', 'v1')
        self.is_pushdown = bool(connect_args.get('is_pushdown', False))
        self.is_debug = bool(connect_args.get('is_debug', False))
        self.scheme = 'https' if self.is_ssl else 'http'
        if self.is_debug:
            logging.basicConfig(level=logging.DEBUG)
        self.service = SERVICES[self.version](self.get_client())

    def set_user(self, username, password=None, session=None):
        self.username = username
        if password:
            self.password = password
        else:
            self.session = session

    def get_client(self):
        headers = {
            'User-Agent': 'Kylin Python Client',
        }

        if self.auth == 'basic':
            headers = self.basic_auth(headers)
        else:
            headers = self.session_auth(headers)

        if self.is_v2:
            headers = self.set_v2_api(headers)

        return HTTPClient(
            host='{self.scheme}://{self.host}:{self.port}'.format(**locals()),
            prefix=self.prefix,
            timeout=self.timeout,
            request_headers=headers,
            unverified=self.unverified,
            mask_auth=(not self.is_debug),
        )

    def basic_auth(self, headers):
        _headers = headers.copy()
        _auth = as_unicode('{}:{}').format(self.username, self.password)
        _auth = base64.b64encode(_auth.encode('utf-8')).decode('ascii')
        _headers.update({'Authorization': 'Basic {}'.format(_auth)})
        return _headers

    def session_auth(self, headers):
        _headers = headers.copy()
        _headers.update({'Cookie': '{}'.format(self.session)})
        return _headers

    def set_v2_api(self, headers):
        _headers = headers.copy()
        _headers.update({'Accept': 'application/vnd.apache.kylin-v2+json'})
        return _headers

    @property
    def is_v2(self):
        return self.version == 'v2'

    @property
    def projects(self):
        return self.service.projects

    def __repr__(self):
        dsn = ('<kylinpy instance '
               '{self.scheme}://'
               '{self.username}:{self.password}@'
               '{self.host}:{self.port}>')
        return dsn.format(**locals())


class Project(object):
    def __init__(self, cluster, project):
        self.cluster = cluster
        self.cluster.service.project = project
        self.service = self.cluster.service
        self.is_pushdown = self.cluster.is_pushdown
        self.project = project

    def query(self, sql):
        return self.service.query(sql)

    def get_source_tables(self, scheme=None):
        _full_names = [s for s in self.get_all_sources('table')]
        if scheme is None:
            return _full_names
        else:
            return list(filter(lambda tbl: tbl.split('.')[0] == scheme, _full_names))

    def get_all_sources(self, source_type):
        if source_type == 'table':
            return TableSource.reflect_datasource_names(self.service, self.is_pushdown)
        elif source_type == 'cube':
            return CubeSource.reflect_datasource_names(self.service, self.is_pushdown)
        else:
            raise NoSuchTableError

    def get_datasource(self, name, source_type='table'):
        if source_type == 'table':
            _source = TableSource.initial(name, self.service, self.is_pushdown)
        elif source_type == 'cube':
            _source = CubeSource.initial(name, self.service, self.is_pushdown)
        else:
            raise NoSuchTableError
        return _source

    def __str__(self):
        return str(self.cluster) + '/' + self.project

    def __repr__(self):
        return '<Kylin Project Instance: {}>'.format(self.project)


def dsn_proxy(dsn, connect_args={}):
    _ = urllib.urlparse(dsn)
    project = _.path.lstrip('/')
    _port = _.port or 7070
    cluster = Cluster(_.hostname, _.username, _.password, _port, **connect_args)
    if project:
        return Project(cluster, project)
    return cluster
