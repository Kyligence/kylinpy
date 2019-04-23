# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import base64
try:
    # Python 3
    import urllib.request as urllib
except ImportError:
    # Python 2
    import urllib2 as urllib

from kylinpy.client import Client as HTTPClient
from kylinpy.exceptions import NoSuchTableError
from kylinpy.kylin_service import KylinService
from kylinpy.source_factory import SourceFactory
from kylinpy.utils.compat import as_unicode


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
        self.unverified = connect_args.get('unverified', True)
        self.session = connect_args.get('session', '')
        self.version = connect_args.get('version', 'v1')
        self.is_pushdown = connect_args.get('is_pushdown', False)
        self.scheme = 'https' if self.is_ssl else 'http'

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
        return KylinService.initial_from_cluster(self.get_client()).projects

    def __repr__(self):
        dsn = ('<kylinpy instance '
               '{self.scheme}://'
               '{self.username}:{self.password}@'
               '{self.host}:{self.port}>')
        return dsn.format(**locals())


class Project(object):
    def __init__(self, cluster, project):
        self.cluster = cluster
        self.kylin_service = KylinService.initial_from_project(
            self.cluster.get_client(),
            project,
        )
        self.is_pushdown = self.cluster.is_pushdown
        self.project = project

    def query(self, sql):
        return self.kylin_service.query(sql)

    def get_source_tables(self, scheme=None):
        _full_names = [s for s in self.get_all_sources().get('hive', [])]
        if scheme is None:
            return _full_names
        else:
            return list(filter(lambda tbl: tbl.split('.')[0] == scheme, _full_names))

    def get_all_sources(self):
        return SourceFactory.get_sources(self.kylin_service, self.is_pushdown)

    def get_datasource(self, name, source_type='hive'):
        _source = SourceFactory(
            name,
            source_type,
            self.kylin_service,
            self.is_pushdown,
        ).source
        if _source is None:
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
