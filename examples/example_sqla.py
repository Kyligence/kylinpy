# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from pprint import pprint

from sqlalchemy import create_engine, inspect


kylin = create_engine("kylin://ADMIN:KYLIN@sandbox/learn_kylin")
pprint(kylin.table_names())

insp = inspect(kylin)
pprint(insp.get_columns('DEFAULT.KYLIN_SALES'))
pprint(insp.get_schema_names())

result = kylin.execute('SELECT * FROM KYLIN_SALES LIMIT 10')
pprint(result.fetchall())
