# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from kylinpy.datasource import source_types


class SourceFactory(object):
    def __init__(
            self,
            name,
            source_type,
            kylin_service,
            is_pushdown,
    ):
        self.source = None
        self.source = source_types[source_type].initial(
            name,
            kylin_service,
            is_pushdown,
        )

    @staticmethod
    def get_sources(kylin_service, is_pushdown):
        return {
            source_type: source_types[source_type].reflect_datasource_names(
                kylin_service,
                is_pushdown,
            ) for source_type in source_types
        }
