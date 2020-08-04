# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import base64
import logging
import warnings

from kylinpy.client import Client as HTTPClient
from kylinpy.exceptions import KylinCubeError
from kylinpy.service import KylinService, KE3Service, KE4Service
from kylinpy.datasource import TableSource, CubeSource, KE4ModelSource
from kylinpy.job import KylinJob, Ke3Job, Ke4Job
from kylinpy.utils.compat import as_unicode, urlparse, parse_qsl

SERVICES = {
    'v1': KylinService,
    'v2': KE3Service,
    'v4': KE4Service,
}

Job = {
    'v1': KylinJob,
    'v2': Ke3Job,
    'v4': Ke4Job,
}


class Kylin(object):
    def __init__(self, host, username=None, password=None, port=7070, project=None, **connect_args):
        if host.startswith(('http://', 'https://')):
            _, self.host = host.split('://')
        else:
            self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.is_ssl = connect_args.get('is_ssl', False)
        self.prefix = connect_args.get('prefix', '/kylin/api')
        self.timeout = int(connect_args.get('timeout', 30))
        self.unverified = bool(connect_args.get('unverified', True))
        self.version = connect_args.get('version', 'v1')
        self.is_pushdown = bool(connect_args.get('is_pushdown', False))
        self.is_debug = bool(connect_args.get('is_debug', False))
        self.scheme = 'https' if self.is_ssl else 'http'
        self.project = project
        if self.is_debug:
            logging.basicConfig(level=logging.DEBUG)

    @property
    def service(self):
        return SERVICES[self.version](self._get_client(), self.project)

    def _get_client(self):
        headers = {
            'User-Agent': 'Kylin Python Client',
        }

        if self.version == 'v2':
            headers.update({'Accept': 'application/vnd.apache.kylin-v2+json'})

        if self.version == 'v4':
            headers.update({'Accept': 'application/vnd.apache.kylin-v4-public+json'})

        if self.username and self.password:
            headers.update(self.basic_auth_dump(self.username, self.password))

        return HTTPClient(
            host='{self.scheme}://{self.host}:{self.port}'.format(**locals()),
            prefix=self.prefix,
            timeout=self.timeout,
            request_headers=headers,
            unverified=self.unverified,
            is_debug=self.is_debug,
        )

    def basic_auth_dump(self, username, password):
        _auth = as_unicode('{}:{}').format(username, password)
        _auth = base64.b64encode(_auth.encode('utf-8')).decode('ascii')
        return {'Authorization': 'Basic {}'.format(_auth)}

    def projects(self):
        return self.service.projects()

    def query(self, sql, **parameters):
        return self.service.query(sql, **parameters)

    def get_all_tables(self, schema=None):
        if self.is_pushdown:
            _full_names = sorted(list(self.service.tables_in_hive().keys()))
        else:
            _full_names = sorted(list(self.service.tables_and_columns().keys()))

        if schema:
            _full_names = [t for t in _full_names if t.split('.')[0] == schema]
        return [t.split('.')[1] for t in _full_names]

    def get_all_schemas(self):
        if self.is_pushdown:
            _full_names = sorted(list(self.service.tables_in_hive().keys()))
        else:
            _full_names = sorted(list(self.service.tables_and_columns().keys()))
        return list(set(t.split('.')[0] for t in _full_names))

    def get_table_source(self, name, schema=None):
        if '.' in name:
            schema, name = name.split('.', 1)
        fullname = '{}.{}'.format(schema, name)
        if self.is_pushdown:
            return TableSource(name, schema, self.service.tables_in_hive().get(fullname))
        else:
            return TableSource(name, schema, self.service.tables_and_columns().get(fullname))

    def get_cube_source(self, name):
        warnings.warn('This method is deprecated. Please use `get_datsource()`.')
        return self.get_datasource(name)

    def get_datasource(self, name):
        if self.version == 'v4':
            _params = {
                'project': self.project,
                'page_offset': 0,
                'page_size': 1000,
                'model_name': name,
                'exact': True,
            }
            model_desc = self.service.models(params=_params)[0]
            return KE4ModelSource(
                model_desc=model_desc,
                tables_and_columns=self.service.tables_and_columns(),
                service=self.service,
            )

        cube_desc = self.service.cube_desc(name)
        if cube_desc is None:
            raise KylinCubeError('No Cube found: {}'.format(name))
        model_name = cube_desc.get('model_name')
        return CubeSource(
            cube_desc=cube_desc,
            model_desc=self.service.model_desc(model_name),
            tables_and_columns=self.service.tables_and_columns(),
            service=self.service,
        )

    def list_datasources(self):
        if self.version == 'v4':
            return [models.get('name') for models in self.service.models()]
        return [cube.get('name') for cube in self.service.cubes()]

    def get_job(self, job_id):
        return Job[self.version](job_id=job_id, service=self.service)

    def list_job(self, **query_params):
        jobs = self.service.jobs(params=query_params)
        if self.version == 'v4':
            return [self.get_job(job_id=job['id']) for job in jobs]
        else:
            return [self.get_job(job_id=job['uuid']) for job in jobs]

    def __str__(self):
        if self.project:
            dsn = ('{self.scheme}://'
                   '{self.username}:{self.password}@'
                   '{self.host}:{self.port}'
                   '/{self.project}')
        else:
            dsn = ('{self.scheme}://'
                   '{self.username}:{self.password}@'
                   '{self.host}:{self.port}')
        return dsn.format(**locals())

    def __repr__(self):
        return '<Kylinpy instance: {}>'.format(str(self))


def create_kylin(dsn):
    url = urlparse(dsn)
    project = url.path.lstrip('/')
    port = url.port or 7070
    query = dict(parse_qsl(url.query) or {})
    return Kylin(url.hostname, url.username, url.password, port, project, **query)


# For compatibility with older versions
KylinCluster = Kylin
dsn_proxy = create_kylin
