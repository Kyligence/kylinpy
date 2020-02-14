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

    mocker.patch('kylinpy.service.KylinService.api.cube_desc', return_value=cube_desc)
    mocker.patch('kylinpy.service.KylinService.api.cubes', return_value=cubes)
    mocker.patch('kylinpy.service.KylinService.api.models', return_value=models)
    mocker.patch('kylinpy.service.KylinService.api.projects', return_value=projects)
    mocker.patch('kylinpy.service.KylinService.api.query', return_value=query)
    mocker.patch('kylinpy.service.KylinService.api.tables', return_value=tables)
    mocker.patch('kylinpy.service.KylinService.api.tables_and_columns', return_value=tables_and_columns)
    yield mocker
