# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from datetime import datetime

import pytest

from kylinpy import create_kylin
from kylinpy.exceptions import KylinCubeError


class TestCubeSource:
    def test_kylin_cube_source(self, v1_api):
        project = create_kylin('kylin://username:password@example/kylin_sales')
        assert hasattr(project, 'get_cube_source')
        cube = project.get_datasource('kylin_sales_cube')
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
        assert cube.support_invoke_command == {
            'fullbuild', 'build', 'merge', 'refresh',
            'delete', 'build_streaming', 'merge_streaming', 'refresh_streaming',
            'disable', 'enable', 'purge', 'clone', 'drop',
        }
        assert cube.fullbuild() == {'build': 'success'}
        assert cube.build(datetime(2000, 1, 1), datetime(2000, 1, 2)) == {'build': 'success'}
        assert cube.list_segment()[0].get('uuid') == 'b5999bec-2381-77c7-cafb-c59407a7a032'
        assert cube.merge(datetime(2000, 1, 1), datetime(2000, 1, 2)) == {'build': 'success'}
        assert cube.refresh(datetime(2000, 1, 1), datetime(2000, 1, 2)) == {'build': 'success'}
        assert cube.delete('segment_name') == {'success': 'success'}
        assert cube.build_streaming(123, 456) == {'success': 'success'}
        assert cube.merge_streaming(123, 456) == {'success': 'success'}
        assert cube.refresh_streaming(123, 456) == {'success': 'success'}
        assert cube.disable() == {'success': 'success'}
        assert cube.enable() == {'success': 'success'}
        assert cube.purge() == {'success': 'success'}
        assert cube.clone() == {'success': 'success'}
        assert cube.drop() == {'success': 'success'}
        with pytest.raises(KylinCubeError):
            cube.invoke_command('invalid command')
        build_cmd = cube.invoke_command('build', foo='unused', bar='unused',
                                        start=datetime(2000, 1, 1), end=datetime(2000, 1, 1))
        drop_cmd = cube.invoke_command('drop')
        assert build_cmd == {'build': 'success'}
        assert drop_cmd == {'success': 'success'}

    def test_v2_cube_source(self, v2_api):
        project = create_kylin('kylin://username:password@example/kylin_sales?version=v2')
        cube = project.get_cube_source('kylin_sales_cube')
        assert cube.name == 'kylin_sales_cube'
        assert cube.model_name == 'kylin_sales_model'
        assert cube.cube_name == 'kylin_sales_cube'

        # fact table testing
        assert cube.fact_table.alias == 'KYLIN_SALES'
        assert cube.fact_table.fullname == 'DEFAULT.KYLIN_SALES'
        assert cube.fact_table.scheme == 'DEFAULT'
        assert cube.fact_table.name == 'KYLIN_SALES'

        assert cube.identity == '0ef9b7a8-3929-4dff-b59d-2100aadc8dbf'
        assert cube.last_modified == 1573799855000

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
        assert [d.column.name for d in cube.dimensions] == [
            'TRANS_ID',
            'YEAR_BEG_DT',
            'MONTH_BEG_DT',
            'WEEK_BEG_DT',
            'USER_DEFINED_FIELD1',
            'USER_DEFINED_FIELD3',
            'META_CATEG_NAME',
            'CATEG_LVL2_NAME',
            'CATEG_LVL3_NAME',
            'LSTG_FORMAT_NAME',
            'SELLER_ID',
            'BUYER_ID',
            'ACCOUNT_BUYER_LEVEL',
            'ACCOUNT_SELLER_LEVEL',
            'ACCOUNT_COUNTRY',
            'ACCOUNT_COUNTRY',
            'NAME',
            'NAME',
            'OPS_USER_ID',
            'OPS_REGION',
        ]
        assert [d.column.alias for d in cube.dimensions] == [
            'TRANS_ID',
            'YEAR_BEG_DT',
            'MONTH_BEG_DT',
            'WEEK_BEG_DT',
            'USER_DEFINED_FIELD1',
            'USER_DEFINED_FIELD3',
            'META_CATEG_NAME',
            'CATEG_LVL2_NAME',
            'CATEG_LVL3_NAME',
            'LSTG_FORMAT_NAME',
            'SELLER_ID',
            'BUYER_ID',
            'ACCOUNT_BUYER_LEVEL',
            'ACCOUNT_SELLER_LEVEL',
            'BUYER_COUNTRY',
            'SELLER_COUNTRY',
            'BUYER_COUNTRY_NAME',
            'SELLER_COUNTRY_NAME',
            'OPS_USER_ID',
            'OPS_REGION',
        ]
        assert [d.column.datatype for d in cube.dimensions] == [
            'BIGINT',
            'DATE',
            'DATE',
            'DATE',
            'VARCHAR(4096)',
            'VARCHAR(4096)',
            'VARCHAR(4096)',
            'VARCHAR(4096)',
            'VARCHAR(4096)',
            'VARCHAR(4096)',
            'BIGINT',
            'BIGINT',
            'INTEGER',
            'INTEGER',
            'VARCHAR(4096)',
            'VARCHAR(4096)',
            'VARCHAR(4096)',
            'VARCHAR(4096)',
            'VARCHAR(4096)',
            'VARCHAR(4096)',
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

    def test_v4_cube_source(self, v4_api):
        project = create_kylin('kylin://username:password@example/kylin_sales?version=v4')
        model = project.get_cube_source('kylin_sales_model')
        assert model.name == 'kylin_sales_model'
        assert model.model_name == 'kylin_sales_model'

        # fact table testing
        assert model.fact_table.alias == 'KYLIN_SALES'
        assert model.fact_table.fullname == 'DEFAULT.KYLIN_SALES'
        assert model.fact_table.scheme == 'DEFAULT'
        assert model.fact_table.name == 'KYLIN_SALES'

        assert model.identity == '0e788bb6-d56c-44fd-8fe0-26bb77aa40c5'
        assert model.last_modified == 1581343542414

        assert [d.name for d in model.dimensions] == [
            'KYLIN_SALES.PART_DT',
            'KYLIN_SALES.LSTG_FORMAT_NAME',
            'KYLIN_SALES.OPS_REGION',
            'KYLIN_CAL_DT.MONTH_BEG_DT',
            'KYLIN_CAL_DT.CAL_DT',
            'KYLIN_CAL_DT.YEAR_BEG_DT',
            'KYLIN_CAL_DT.QTR_BEG_DT',
            'BUYER_ACCOUNT.ACCOUNT_COUNTRY',
            'BUYER_ACCOUNT.ACCOUNT_BUYER_LEVEL',
            'BUYER_COUNTRY.NAME',
            'BUYER_COUNTRY.COUNTRY',
            'SELLER_ACCOUNT.ACCOUNT_SELLER_LEVEL',
            'SELLER_ACCOUNT.ACCOUNT_COUNTRY',
            'SELLER_COUNTRY.NAME',
            'SELLER_COUNTRY.COUNTRY',
            'KYLIN_CATEGORY_GROUPINGS.CATEG_LVL3_NAME',
            'KYLIN_CATEGORY_GROUPINGS.CATEG_LVL2_NAME',
            'KYLIN_CATEGORY_GROUPINGS.LEAF_CATEG_NAME',
            'KYLIN_CATEGORY_GROUPINGS.META_CATEG_NAME',
        ]
        assert [d.verbose for d in model.dimensions] == [
            'KYLIN_SALES_PART_DT',
            'KYLIN_SALES_LSTG_FORMAT_NAME',
            'KYLIN_SALES_OPS_REGION',
            'KYLIN_CAL_DT_MONTH_BEG_DT',
            'KYLIN_CAL_DT_CAL_DT',
            'KYLIN_CAL_DT_YEAR_BEG_DT',
            'KYLIN_CAL_DT_QTR_BEG_DT',
            'BUYER_ACCOUNT_ACCOUNT_COUNTRY',
            'BUYER_ACCOUNT_ACCOUNT_BUYER_LEVEL',
            'BUYER_COUNTRY_NAME',
            'BUYER_COUNTRY_COUNTRY',
            'SELLER_ACCOUNT_ACCOUNT_SELLER_LEVEL',
            'SELLER_ACCOUNT_ACCOUNT_COUNTRY',
            'SELLER_COUNTRY_NAME',
            'SELLER_COUNTRY_COUNTRY',
            'KYLIN_CATEGORY_GROUPINGS_CATEG_LVL3_NAME',
            'KYLIN_CATEGORY_GROUPINGS_CATEG_LVL2_NAME',
            'KYLIN_CATEGORY_GROUPINGS_LEAF_CATEG_NAME',
            'KYLIN_CATEGORY_GROUPINGS_META_CATEG_NAME',
        ]
        assert [d.column.name for d in model.dimensions] == [
            'PART_DT',
            'LSTG_FORMAT_NAME',
            'OPS_REGION',
            'MONTH_BEG_DT',
            'CAL_DT',
            'YEAR_BEG_DT',
            'QTR_BEG_DT',
            'ACCOUNT_COUNTRY',
            'ACCOUNT_BUYER_LEVEL',
            'NAME',
            'COUNTRY',
            'ACCOUNT_SELLER_LEVEL',
            'ACCOUNT_COUNTRY',
            'NAME',
            'COUNTRY',
            'CATEG_LVL3_NAME',
            'CATEG_LVL2_NAME',
            'LEAF_CATEG_NAME',
            'META_CATEG_NAME',
        ]
        assert [d.column.alias for d in model.dimensions] == [
            'KYLIN_SALES_PART_DT',
            'KYLIN_SALES_LSTG_FORMAT_NAME',
            'KYLIN_SALES_OPS_REGION',
            'KYLIN_CAL_DT_MONTH_BEG_DT',
            'KYLIN_CAL_DT_CAL_DT',
            'KYLIN_CAL_DT_YEAR_BEG_DT',
            'KYLIN_CAL_DT_QTR_BEG_DT',
            'BUYER_ACCOUNT_ACCOUNT_COUNTRY',
            'BUYER_ACCOUNT_ACCOUNT_BUYER_LEVEL',
            'BUYER_COUNTRY_NAME',
            'BUYER_COUNTRY_COUNTRY',
            'SELLER_ACCOUNT_ACCOUNT_SELLER_LEVEL',
            'SELLER_ACCOUNT_ACCOUNT_COUNTRY',
            'SELLER_COUNTRY_NAME',
            'SELLER_COUNTRY_COUNTRY',
            'KYLIN_CATEGORY_GROUPINGS_CATEG_LVL3_NAME',
            'KYLIN_CATEGORY_GROUPINGS_CATEG_LVL2_NAME',
            'KYLIN_CATEGORY_GROUPINGS_LEAF_CATEG_NAME',
            'KYLIN_CATEGORY_GROUPINGS_META_CATEG_NAME',
        ]
        assert [d.column.datatype for d in model.dimensions] == [
            'DATE',
            'VARCHAR(4096)',
            'VARCHAR(4096)',
            'DATE',
            'DATE',
            'DATE',
            'DATE',
            'VARCHAR(4096)',
            'INTEGER',
            'VARCHAR(4096)',
            'VARCHAR(4096)',
            'INTEGER',
            'VARCHAR(4096)',
            'VARCHAR(4096)',
            'VARCHAR(4096)',
            'VARCHAR(4096)',
            'VARCHAR(4096)',
            'VARCHAR(4096)',
            'VARCHAR(4096)',
        ]

        assert [m.name for m in model.measures] == [
            'KYLIN_SALES_TRANS_ID_COUNT',
            'KYLIN_SALES_PRICE_SUM',
            'KYLIN_SALES_PRICE_MAX',
            'KYLIN_SALES_ITEM_COUNT_SUM',
            'BUYER_ID_COUNT_DISTINCT',
            'COUNT_ALL',
        ]
        assert [m.verbose for m in model.measures] == [
            'KYLIN_SALES_TRANS_ID_COUNT',
            'KYLIN_SALES_PRICE_SUM',
            'KYLIN_SALES_PRICE_MAX',
            'KYLIN_SALES_ITEM_COUNT_SUM',
            'BUYER_ID_COUNT_DISTINCT',
            'COUNT_ALL',
        ]
        assert [m.measure_type for m in model.measures] == [
            'COUNT',
            'SUM',
            'MAX',
            'SUM',
            'COUNT_DISTINCT',
            'COUNT',
        ]
        assert [m.expression for m in model.measures] == [
            'COUNT (KYLIN_SALES.TRANS_ID)',
            'SUM (KYLIN_SALES.PRICE)',
            'MAX (KYLIN_SALES.PRICE)',
            'SUM (KYLIN_SALES.ITEM_COUNT)',
            'COUNT (DISTINCT KYLIN_SALES.BUYER_ID)',
            'COUNT (1)',
        ]
        assert str(model.from_clause) == (
            '"DEFAULT"."KYLIN_SALES" AS "KYLIN_SALES" '
            'JOIN "DEFAULT"."KYLIN_CAL_DT" AS "KYLIN_CAL_DT" ON "KYLIN_SALES"."PART_DT" = "KYLIN_CAL_DT"."CAL_DT" '  # noqa
            'JOIN "DEFAULT"."KYLIN_ACCOUNT" AS "BUYER_ACCOUNT" ON "KYLIN_SALES"."SELLER_ID" = "BUYER_ACCOUNT"."ACCOUNT_ID" '  # noqa
            'JOIN "DEFAULT"."KYLIN_ACCOUNT" AS "SELLER_ACCOUNT" ON "KYLIN_SALES"."SELLER_ID" = "SELLER_ACCOUNT"."ACCOUNT_ID" '  # noqa
            'JOIN "DEFAULT"."KYLIN_CATEGORY_GROUPINGS" AS "KYLIN_CATEGORY_GROUPINGS" ON "KYLIN_SALES"."LEAF_CATEG_ID" = "KYLIN_CATEGORY_GROUPINGS"."LEAF_CATEG_ID" AND "KYLIN_SALES"."LSTG_SITE_ID" = "KYLIN_CATEGORY_GROUPINGS"."SITE_ID" '  # noqa
            'JOIN "DEFAULT"."KYLIN_COUNTRY" AS "BUYER_COUNTRY" ON "BUYER_ACCOUNT"."ACCOUNT_COUNTRY" = "BUYER_COUNTRY"."COUNTRY" '  # noqa
            'JOIN "DEFAULT"."KYLIN_COUNTRY" AS "SELLER_COUNTRY" ON "SELLER_ACCOUNT"."ACCOUNT_COUNTRY" = "SELLER_COUNTRY"."COUNTRY"'  # noqa
        )
