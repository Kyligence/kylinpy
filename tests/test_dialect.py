# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import json
import os
import unittest

from mock import Mock
from sqlalchemy import create_engine

from kylinpy.kylinpy import Client
from kylinpy.utils._compat import as_unicode


class TestDialect(unittest.TestCase):
    def json_loads(self, filename):
        here = os.path.abspath(os.path.dirname(__file__))
        f = open(os.path.join(here, 'fixtures', '{}.json'.format(filename)), 'r').read()
        return json.loads(f)

    def test_connection(self):
        dsn = 'kylin://ADMIN:KYLIN@sandbox:7070/project'
        kylin = create_engine(dsn)
        self.assertEqual(str(kylin.url), dsn)

        dsn = 'kylin://用户:密码@sandbox:7070/中文project'
        kylin = create_engine(dsn)
        self.assertEqual(as_unicode(kylin.url), dsn)

    def test_get_table_names(self):
        dsn = 'kylin://ADMIN:KYLIN@sandbox:7070/sample_data'
        kylin = create_engine(dsn)
        Client.fetch = Mock(return_value=self.json_loads('tables_and_columns'))
        self.assertEqual(kylin.table_names(), ['US_BIRTHS'])
        self.assertEqual(kylin.table_names('DEFAULT'), [])
