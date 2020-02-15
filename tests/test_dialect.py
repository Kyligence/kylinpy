# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import json
import os

from sqlalchemy import create_engine, inspect

from kylinpy.utils.compat import as_unicode


class TestDialect(object):
    dsn = 'kylin://ADMIN:KYLIN@sandbox:7070/learn_kylin'
    cn_dsn = 'kylin://用户:密码@sandbox:7070/中文project'

    def json_loads(self, filename):
        here = os.path.abspath(os.path.dirname(__file__))
        f = open(os.path.join(here, 'fixtures', '{}.json'.format(filename)), 'r').read()
        return json.loads(f)

    @property
    def engine(self):
        return create_engine(self.dsn)

    def test_connection(self):
        engine = create_engine(self.cn_dsn)
        assert as_unicode(engine.url) == self.cn_dsn

        engine = create_engine('kylin://aa@bb123:aa@bb123@hello.world.com:1024/learn_kylin')
        assert engine.url.username == 'aa@bb123'
        assert engine.url.password == 'aa@bb123'
        assert engine.url.host == 'hello.world.com'
        assert engine.url.port == 1024
        assert engine.url.database == 'learn_kylin'

    def test_query(self, v1_api):
        engine = create_engine('kylin://ADMIN:KYLIN@sandbox/learn_kylin')
        rp = engine.execute('select count(*) from kylin_sales')
        assert [row[0] for row in rp.fetchall()] == [10000]

    def test_table_names(self, v1_api):
        engine = create_engine('kylin://ADMIN:KYLIN@sandbox/learn_kylin')
        assert engine.table_names() == [
            'KYLIN_ACCOUNT',
            'KYLIN_CAL_DT',
            'KYLIN_CATEGORY_GROUPINGS',
            'KYLIN_COUNTRY',
            'KYLIN_SALES',
        ]

    def test_get_schema_names(self, v1_api):
        engine = create_engine('kylin://ADMIN:KYLIN@sandbox/learn_kylin')
        insp = inspect(engine)
        assert insp.get_schema_names() == ['DEFAULT']

    def test_get_columns(self, v1_api):
        engine = create_engine('kylin://ADMIN:KYLIN@sandbox/learn_kylin')
        insp = inspect(engine)
        assert [c.get('name') for c in insp.get_columns('KYLIN_SALES', 'DEFAULT')] == [
            'TRANS_ID',
            'PART_DT',
            'LSTG_FORMAT_NAME',
            'LEAF_CATEG_ID',
            'LSTG_SITE_ID',
            'PRICE',
            'SELLER_ID',
            'BUYER_ID',
            'OPS_USER_ID',
            'OPS_REGION',
        ]
