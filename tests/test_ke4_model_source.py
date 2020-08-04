# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from datetime import datetime

from kylinpy import create_kylin


class TestKe4ModelSource:
    def test_v4_model_source(self, v4_api):
        project = create_kylin('kylin://username:password@example/kylin_sales?version=v4')
        model = project.get_datasource('kylin_sales_model')
        assert model.name == 'kylin_sales_model'
        assert model.model_name == 'kylin_sales_model'

        # fact table testing
        assert model.fact_table.alias == 'KYLIN_SALES'
        assert model.fact_table.fullname == 'DEFAULT.KYLIN_SALES'
        assert model.fact_table.scheme == 'DEFAULT'
        assert model.fact_table.name == 'KYLIN_SALES'

        assert model.identity == '0e788bb6-d56c-44fd-8fe0-26bb77aa40c5'
        assert model.uuid == '0e788bb6-d56c-44fd-8fe0-26bb77aa40c5'
        assert model.last_modified == 1581343542414
        assert model.support_invoke_command == {
            'fullbuild', 'build', 'merge', 'refresh', 'delete', 'list_segment', 'refresh_catalog_cache',
        }

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

        assert [d.id for d in model.dimensions] == [
            1,
            9,
            11,
            28,
            53,
            84,
            110,
            114,
            116,
            117,
            118,
            122,
            123,
            126,
            127,
            133,
            146,
            149,
            160,
        ]

        assert [d.status for d in model.dimensions] == [
            'DIMENSION',
            'DIMENSION',
            'DIMENSION',
            'DIMENSION',
            'DIMENSION',
            'DIMENSION',
            'DIMENSION',
            'DIMENSION',
            'DIMENSION',
            'DIMENSION',
            'DIMENSION',
            'DIMENSION',
            'DIMENSION',
            'DIMENSION',
            'DIMENSION',
            'DIMENSION',
            'DIMENSION',
            'DIMENSION',
            'DIMENSION',
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

        assert model.list_segment()[0].get('id') == '6dcc77dd-8e9b-488a-a52e-ed7cb0245d79'
        assert model.delete('id') == {'success': 'success'}
        assert model.fullbuild() == {'build': 'success'}
        assert model.build(datetime(2000, 1, 1), datetime(2000, 1, 2)) == {'build': 'success'}
        assert model.merge(['123', '234']) == {'build': 'success'}
        assert model.refresh(['123', '234']) == {'build': 'success'}

    def test_list_indexes(self, v4_api):
        project = create_kylin('kylin://username:password@example/kylin_sales?version=v4')
        model = project.get_datasource('kylin_sales_model')
        assert model.list_indexes() == [1, 2, 3, 4]

    def test_build_indexes(self, v4_api):
        project = create_kylin('kylin://username:password@example/kylin_sales?version=v4')
        model = project.get_datasource('kylin_sales_model')
        assert model.build_indexes() == {'build_indexes': 'success'}

    def test_delete_index(self, v4_api):
        project = create_kylin('kylin://username:password@example/kylin_sales?version=v4')
        model = project.get_datasource('kylin_sales_model')
        assert model.delete_index(1) == {'delete_index': 'success'}

    def test_list_index_rules(self, v4_api):
        project = create_kylin('kylin://username:password@example/kylin_sales?version=v4')
        model = project.get_datasource('kylin_sales_model')
        assert model.list_index_rules() == [1, 2, 3, 4]

    def test_clear_up_index_rules(self, v4_api):
        project = create_kylin('kylin://username:password@example/kylin_sales?version=v4')
        model = project.get_datasource('kylin_sales_model')
        assert model.clear_up_index_rules() == {'put_index_rules': 'success'}
