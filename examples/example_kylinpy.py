# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from pprint import pprint

import kylinpy

kylin = kylinpy.Project('sandbox', 'ADMIN', 'KYLIN', project='learn_kylin')
pprint("====== Get source table names in hive =====>")
pprint(kylin.get_source_tables())

pprint("====== table name ======")
tbl = kylin.get_datasource('DEFAULT.KYLIN_SALES')
pprint(tbl.name)
pprint("====== table schema ======")
pprint(tbl.schema)
pprint("====== table dimensions ======>")
pprint(tbl.dimensions)
pprint("====== table dimensions info ======>")
pprint([(dim.name, dim.datatype) for dim in tbl.dimensions])


pprint("====== Get cube names in hive =====>")
pprint(kylin.cube_names)
cube = kylin.get_datasource('kylin_sales_cube')

pprint("====== cube infomation ======")
pprint(cube.name)
pprint(cube.cube_name)
pprint(cube.model_name)
pprint(cube.cube_desc)
pprint(cube.model_desc)
pprint(cube.dimensions)
pprint([(dim.name, dim.datatype) for dim in tbl.dimensions])
pprint(cube.measures)
pprint(cube.model_desc)
pprint(cube.last_modified)
pprint(str(cube.from_clause))
