# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from sqlalchemy.dialects import registry

registry.register('kylin', 'kylinpy.sqla_dialect', 'KylinDialect')

pytest_plugins = [
    'tests.fixtures.api',
]
