# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from kylinpy.kylinpy import create_kylin


class TestTableSource:
    def test_table_source(self, v1_api):
        project = create_kylin('kylin://username:password@example/kylin_sales')
        table = project.get_table_source('KYLIN_SALES', 'DEFAULT')
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

    def test_v2_table_source(self, v2_api):
        project = create_kylin('kylin://username:password@example/kylin_sales?version=v2')
        table = project.get_table_source('KYLIN_SALES', 'DEFAULT')
        assert table.name == 'KYLIN_SALES'
        assert table.schema == 'DEFAULT'
        assert [c.name for c in table.columns] == [
            'TRANS_ID',
            'PART_DT',
            'LSTG_FORMAT_NAME',
            'LEAF_CATEG_ID',
            'LSTG_SITE_ID',
            'PRICE',
            'ITEM_COUNT',
            'SELLER_ID',
            'BUYER_ID',
            'OPS_USER_ID',
            'OPS_REGION',
        ]
