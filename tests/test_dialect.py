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

from kylinpy.errors import KylinQueryError
from kylinpy.kylindb import KylinDB
from kylinpy.kylinpy import Client, Kylinpy
from kylinpy.utils._compat import as_unicode


class TestDialect(unittest.TestCase):
    def json_loads(self, filename):
        here = os.path.abspath(os.path.dirname(__file__))
        f = open(os.path.join(here, 'fixtures', '{}.json'.format(filename)), 'r').read()
        return json.loads(f)

    def test_connection(self):
        dsn = 'kylin://ADMIN:KYLIN@sandbox:7070/learn_kylin'
        kylin = create_engine(dsn)
        self.assertEqual(as_unicode(kylin.url), dsn)
        self.assertTrue(isinstance(kylin.connect().connection.connection, KylinDB))
        self.assertTrue(isinstance(kylin.connect().connection.connection, Kylinpy))

        dsn = 'kylin://用户:密码@sandbox:7070/中文project'
        kylin = create_engine(dsn)
        self.assertEqual(as_unicode(kylin.url), dsn)

    def test_get_table_names(self):
        dsn = 'kylin://ADMIN:KYLIN@sandbox:7070/learn_kylin'
        kylin = create_engine(dsn)
        Client.fetch = Mock(return_value=self.json_loads('tables_and_columns'))
        self.assertEqual(
            kylin.table_names(),
            [
                'KYLIN_ACCOUNT',
                'KYLIN_CAL_DT',
                'KYLIN_CATEGORY_GROUPINGS',
                'KYLIN_COUNTRY',
                'KYLIN_SALES',
            ])
        self.assertEqual(kylin.table_names('NOT_EXISTS_SCHEMA'), [])

    def test_query(self):
        dsn = 'kylin://ADMIN:KYLIN@sandbox:7070/learn_kylin'
        kylin = create_engine(dsn)
        Client.fetch = Mock(return_value=self.json_loads('query'))
        results = kylin.execute('SELECT COUNT(*) FROM KYLIN_SALES').fetchall()
        self.assertEqual(results, [(10000,)])

    def test_bad_query(self):
        dsn = 'kylin://ADMIN:KYLIN@sandbox:7070/learn_kylin'
        kylin = create_engine(dsn)
        Client.fetch = Mock(return_value={'exception': 'exception info'})
        with self.assertRaises(KylinQueryError) as error:
            kylin.execute('SELECT NOT_EXISTS_COLUMN FROM KYLIN_SALES').fetchall()
        self.assertEqual('exception info', str(error.exception))

        Client.fetch = Mock(return_value={'exceptionMessage': 'exception info'})
        with self.assertRaises(KylinQueryError) as error:
            kylin.execute('SELECT NOT_EXISTS_COLUMN FROM KYLIN_SALES').fetchall()
        self.assertEqual('exception info', str(error.exception))
