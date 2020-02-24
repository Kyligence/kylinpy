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
    query = read('v1', 'query.json')
    tables = read('v1', 'tables.json')
    tables_and_columns = read('v1', 'tables_and_columns.json')
    authentication = read('v1', 'authentication.json')

    mocker.patch('kylinpy.service.KylinService.api.cube_desc', return_value=cube_desc)
    mocker.patch('kylinpy.service.KylinService.api.cubes', return_value=cubes)
    mocker.patch('kylinpy.service.KylinService.api.models', return_value=models)
    mocker.patch('kylinpy.service.KylinService.api.projects', return_value=projects)
    mocker.patch('kylinpy.service.KylinService.api.query', return_value=query)
    mocker.patch('kylinpy.service.KylinService.api.tables', return_value=tables)
    mocker.patch('kylinpy.service.KylinService.api.tables_and_columns', return_value=tables_and_columns)
    mocker.patch('kylinpy.service.KylinService.api.authentication', return_value=authentication)
    yield mocker


@pytest.fixture
def v2_api(mocker):
    cube_desc = read('v2', 'cube_desc.json').get('data')
    cubes = read('v2', 'cubes.json').get('data')
    models = read('v2', 'models.json').get('data')
    projects = read('v2', 'projects.json').get('data')
    query = read('v2', 'query.json').get('data')
    tables = read('v2', 'tables.json').get('data')
    tables_and_columns = read('v2', 'tables_and_columns.json').get('data')
    authentication = read('v2', 'authentication.json').get('data')

    mocker.patch('kylinpy.service.KE3Service.api.cube_desc', return_value=cube_desc)
    mocker.patch('kylinpy.service.KE3Service.api.cubes', return_value=cubes)
    mocker.patch('kylinpy.service.KE3Service.api.models', return_value=models)
    mocker.patch('kylinpy.service.KE3Service.api.projects', return_value=projects)
    mocker.patch('kylinpy.service.KE3Service.api.query', return_value=query)
    mocker.patch('kylinpy.service.KE3Service.api.tables', return_value=tables)
    mocker.patch('kylinpy.service.KE3Service.api.tables_and_columns', return_value=tables_and_columns)
    mocker.patch('kylinpy.service.KE3Service.api.authentication', return_value=authentication)
    yield mocker


@pytest.fixture
def v4_api(mocker):
    projects = read('v4', 'projects.json').get('data')
    query = read('v4', 'query.json').get('data')
    tables = read('v4', 'tables.json').get('data')
    tables_and_columns = read('v4', 'tables_and_columns.json').get('data')
    authentication = read('v4', 'authentication.json').get('data')

    mocker.patch('kylinpy.service.KE4Service.api.projects', return_value=projects)
    mocker.patch('kylinpy.service.KE4Service.api.query', return_value=query)
    mocker.patch('kylinpy.service.KE4Service.api.tables', return_value=tables)
    mocker.patch('kylinpy.service.KE4Service.api.tables_and_columns', return_value=tables_and_columns)
    mocker.patch('kylinpy.service.KE4Service.api.authentication', return_value=authentication)
    yield mocker
