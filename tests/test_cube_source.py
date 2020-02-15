# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from kylinpy.kylinpy import dsn_proxy


class TestCubeSource:
    @property
    def project(self):
        return dsn_proxy('kylin://username:password@example/foobar')

    def test_cube_source(self, v1_api):
        cube = self.project.get_cube_source('kylin_sales_cube')
        assert cube.name == 'kylin_sales_cube'
        assert cube.model_name == 'kylin_sales_model'
        assert cube.cube_name == 'kylin_sales_cube'

        # fact table testing
        assert cube.fact_table.alias == 'KYLIN_SALES'
        assert cube.fact_table.fullname == 'DEFAULT.KYLIN_SALES'
        assert cube.fact_table.scheme == 'DEFAULT'
        assert cube.fact_table.name == 'KYLIN_SALES'

        assert cube.identity == '0ef9b7a8-3929-4dff-b59d-2100aadc8dbf'
        assert cube.last_modified == 1560061109000

        assert [d.name for d in cube.dimensions] == [
            'KYLIN_SALES.TRANS_ID',
            'KYLIN_CAL_DT.YEAR_BEG_DT',
            'KYLIN_CAL_DT.MONTH_BEG_DT',
            'KYLIN_CAL_DT.WEEK_BEG_DT',
            'KYLIN_CATEGORY_GROUPINGS.USER_DEFINED_FIELD1',
            'KYLIN_CATEGORY_GROUPINGS.USER_DEFINED_FIELD3',
            'KYLIN_CATEGORY_GROUPINGS.META_CATEG_NAME',
            'KYLIN_CATEGORY_GROUPINGS.CATEG_LVL2_NAME',
            'KYLIN_CATEGORY_GROUPINGS.CATEG_LVL3_NAME',
            'KYLIN_SALES.LSTG_FORMAT_NAME',
            'KYLIN_SALES.SELLER_ID',
            'KYLIN_SALES.BUYER_ID',
            'BUYER_ACCOUNT.ACCOUNT_BUYER_LEVEL',
            'SELLER_ACCOUNT.ACCOUNT_SELLER_LEVEL',
            'BUYER_ACCOUNT.ACCOUNT_COUNTRY',
            'SELLER_ACCOUNT.ACCOUNT_COUNTRY',
            'BUYER_COUNTRY.NAME',
            'SELLER_COUNTRY.NAME',
            'KYLIN_SALES.OPS_USER_ID',
            'KYLIN_SALES.OPS_REGION',
        ]

        assert [m.name for m in cube.measures] == [
            'GMV_SUM',
            'BUYER_LEVEL_SUM',
            'SELLER_LEVEL_SUM',
            'TRANS_CNT',
            'SELLER_CNT_HLL',
            'TOP_SELLER',
        ]

        assert str(cube.from_clause) == (
            '"DEFAULT"."KYLIN_SALES" AS "KYLIN_SALES" '
            'JOIN "DEFAULT"."KYLIN_CAL_DT" AS "KYLIN_CAL_DT" ON "KYLIN_SALES"."PART_DT" = "KYLIN_CAL_DT"."CAL_DT" '  # noqa
            'JOIN "DEFAULT"."KYLIN_CATEGORY_GROUPINGS" AS "KYLIN_CATEGORY_GROUPINGS" ON "KYLIN_SALES"."LEAF_CATEG_ID" = "KYLIN_CATEGORY_GROUPINGS"."LEAF_CATEG_ID" AND "KYLIN_SALES"."LSTG_SITE_ID" = "KYLIN_CATEGORY_GROUPINGS"."SITE_ID" '  # noqa
            'JOIN "DEFAULT"."KYLIN_ACCOUNT" AS "BUYER_ACCOUNT" ON "KYLIN_SALES"."BUYER_ID" = "BUYER_ACCOUNT"."ACCOUNT_ID" '  # noqa
            'JOIN "DEFAULT"."KYLIN_ACCOUNT" AS "SELLER_ACCOUNT" ON "KYLIN_SALES"."SELLER_ID" = "SELLER_ACCOUNT"."ACCOUNT_ID" '  # noqa
            'JOIN "DEFAULT"."KYLIN_COUNTRY" AS "BUYER_COUNTRY" ON "BUYER_ACCOUNT"."ACCOUNT_COUNTRY" = "BUYER_COUNTRY"."COUNTRY" '  # noqa
            'JOIN "DEFAULT"."KYLIN_COUNTRY" AS "SELLER_COUNTRY" ON "SELLER_ACCOUNT"."ACCOUNT_COUNTRY" = "SELLER_COUNTRY"."COUNTRY"'  # noqa
        )
