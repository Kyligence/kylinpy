# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import logging
from datetime import datetime

from kylinpy import Kylin
logging.basicConfig(level=logging.DEBUG)

engine = Kylin(
    host='127.0.0.1',
    port=7070,
    username='ADMIN',
    password='KYLIN',
    version='v4',
    is_debug=True,
)

# get a abstruct datasource
model = engine.get_datasource('ssb_model')

# return model uuid
model.uuid

# return model name
model.name

# return all dimensions in model
model.dimensions

# get a specific dimension
dimension = model.dimensions[0]
dimension.name
dimension.datetype
dimension.verbose
dimension.id

# some uncommon property in Dimension object
dimension.table
dimension.table.name
dimension.table.schema
dimension.table.alias
dimension.table.fullname
dimension.column
dimension.column.name
dimension.column.alias
dimension.column.description
dimension.column.datatype

# return all measures in model
model.measures

# get a specific measure
measure = model.measures[0]
measure.name
measure.verbose
measure.measure_type
measure.expression

# build specific segment
model.build(datetime(2020, 1, 1), datetime(2020, 1, 2))

# list segments
model.list_segment()

# merge segments by segment_ids
model.merge([1, 2, 3])

# refresh segments by segment_ids
model.refresh([1, 2, 3])

# delete segments by segment_ids
model.delete([1, 2, 3])

# list indexes in current model
model.list_indexes()

# build all indexes in current model
model.build_indexes()

# delete index by specific index_id
model.delete_index(123)

# list index rules(agg groups) in current model
model.list_index_rules()

# append a index rule(agg groups) in current model
model.add_index_rule(
    load_data=False,  # whether or not auto build index
    include=['tbl.dim1', 'tbl.dim2'],  # dimenstion name in list
    mandatory=['tbl.dim1', 'tbl.dim2'],  # dimension name in list
)

# REMOVE ALL index rules(agg groups) and index in current model
model.clear_up_index_rules()
