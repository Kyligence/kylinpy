# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from kylinpy.kylinpy import dsn_proxy


class TestTableSource:
    @property
    def project(self):
        return dsn_proxy('kylin://username:password@example/foobar')

    def test_table_source(self, v1_api):
        table = self.project.get_table_source('KYLIN_SALES', 'DEFAULT')
        assert table.name == 'KYLIN_SALES'
        assert table.schema == 'DEFAULT'
        assert [c.name for c in table.columns] == [
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
