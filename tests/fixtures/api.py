# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import os.path
import json

import pytest


def read(version, filename):
    here = os.path.abspath(os.path.dirname(__file__))
    return json.load(open(os.path.join(here, version, filename)))


@pytest.fixture
def v1_api(mocker):
    cube_desc = read('v1', 'cube_desc.json')
    cubes = read('v1', 'cubes.json')
    models = read('v1', 'models.json')
    projects = read('v1', 'projects.json')
    jobs = read('v1', 'jobs.json')
    query = read('v1', 'query.json')
    tables = read('v1', 'tables.json')
    tables_and_columns = read('v1', 'tables_and_columns.json')
    authentication = read('v1', 'authentication.json')

    mocker.patch('kylinpy.service.KylinService.api.cube_desc', return_value=cube_desc)
    mocker.patch('kylinpy.service.KylinService.api.cubes', return_value=cubes)
    mocker.patch('kylinpy.service.KylinService.api.models', return_value=models)
    mocker.patch('kylinpy.service.KylinService.api.projects', return_value=projects)
    mocker.patch('kylinpy.service.KylinService.api.jobs', return_value=jobs)
    mocker.patch('kylinpy.service.KylinService.api.query', return_value=query)
    mocker.patch('kylinpy.service.KylinService.api.tables', return_value=tables)
    mocker.patch('kylinpy.service.KylinService.api.tables_and_columns', return_value=tables_and_columns)
    mocker.patch('kylinpy.service.KylinService.api.authentication', return_value=authentication)
    mocker.patch('kylinpy.service.KylinService.api.build', return_value={'build': 'success'})
    mocker.patch('kylinpy.service.KylinService.api.build_streaming', return_value={'success': 'success'})
    mocker.patch('kylinpy.service.KylinService.api.delete_segment', return_value={'success': 'success'})
    mocker.patch('kylinpy.service.KylinService.api.maintain_cube', return_value={'success': 'success'})
    mocker.patch('kylinpy.service.KylinService.api.drop_cube', return_value={'success': 'success'})
    mocker.patch('kylinpy.service.KylinService.api.maintain_job', return_value={'success': 'success'})
    mocker.patch('kylinpy.service.KylinService.api.drop_job', return_value={'success': 'success'})
    mocker.patch('kylinpy.service.KylinService.api.job_desc', return_value={})
    yield mocker


@pytest.fixture
def v2_api(mocker):
    cube_desc = read('v2', 'cube_desc.json').get('data')
    cubes = read('v2', 'cubes.json').get('data')
    models = read('v2', 'models.json').get('data')
    projects = read('v2', 'projects.json').get('data')
    jobs = read('v2', 'jobs.json').get('data')
    query = read('v2', 'query.json').get('data')
    tables = read('v2', 'tables.json').get('data')
    tables_and_columns = read('v2', 'tables_and_columns.json').get('data')
    authentication = read('v2', 'authentication.json').get('data')

    mocker.patch('kylinpy.service.KE3Service.api.cube_desc', return_value=cube_desc)
    mocker.patch('kylinpy.service.KE3Service.api.cubes', return_value=cubes)
    mocker.patch('kylinpy.service.KE3Service.api.models', return_value=models)
    mocker.patch('kylinpy.service.KE3Service.api.projects', return_value=projects)
    mocker.patch('kylinpy.service.KE3Service.api.jobs', return_value=jobs)
    mocker.patch('kylinpy.service.KE3Service.api.query', return_value=query)
    mocker.patch('kylinpy.service.KE3Service.api.tables', return_value=tables)
    mocker.patch('kylinpy.service.KE3Service.api.tables_and_columns', return_value=tables_and_columns)
    mocker.patch('kylinpy.service.KE3Service.api.authentication', return_value=authentication)
    yield mocker


@pytest.fixture
def v4_api(mocker):
    projects = read('v4', 'projects.json').get('data')
    jobs = read('v4', 'jobs.json').get('data')
    query = read('v4', 'query.json').get('data')
    tables = read('v4', 'tables.json').get('data')
    tables_and_columns = read('v4', 'tables_and_columns.json').get('data')
    authentication = read('v4', 'authentication.json').get('data')
    models = read('v4', 'models.json').get('data')
    model_desc = read('v4', 'model_desc.json').get('data')
    segments = read('v4', 'segments.json').get('data')

    mocker.patch('kylinpy.service.KE4Service.api.projects', return_value=projects)
    mocker.patch('kylinpy.service.KE4Service.api.jobs', return_value=jobs)
    mocker.patch('kylinpy.service.KE4Service.api.query', return_value=query)
    mocker.patch('kylinpy.service.KE4Service.api.tables', return_value=tables)
    mocker.patch('kylinpy.service.KE4Service.api.tables_and_columns', return_value=tables_and_columns)
    mocker.patch('kylinpy.service.KE4Service.api.authentication', return_value=authentication)
    mocker.patch('kylinpy.service.KE4Service.api.models', return_value=models)
    mocker.patch('kylinpy.service.KE4Service.api.model_desc', return_value=model_desc)
    mocker.patch('kylinpy.service.KE4Service.api.build', return_value={'build': 'success'})
    mocker.patch('kylinpy.service.KE4Service.api.refresh', return_value={'build': 'success'})
    mocker.patch('kylinpy.service.KE4Service.api.merge', return_value={'build': 'success'})
    mocker.patch('kylinpy.service.KE4Service.api.list_segment', return_value=segments)
    mocker.patch('kylinpy.service.KE4Service.api.delete_segment', return_value={'success': 'success'})
    mocker.patch('kylinpy.service.KE4Service.api.list_indexes', return_value={'value': [1, 2, 3, 4]})
    mocker.patch('kylinpy.service.KE4Service.api.build_indexes', return_value={'build_indexes': 'success'})
    mocker.patch('kylinpy.service.KE4Service.api.delete_index', return_value={'delete_index': 'success'})
    mocker.patch('kylinpy.service.KE4Service.api.list_index_rules', return_value=[1, 2, 3, 4])
    mocker.patch('kylinpy.service.KE4Service.api.put_index_rules', return_value={'put_index_rules': 'success'})
    yield mocker
