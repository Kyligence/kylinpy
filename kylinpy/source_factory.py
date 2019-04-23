# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from kylinpy.datasource import source_types


def source_factory(
    name,
    source_type,
    services,
    is_pushdown,
):
    service_type = source_types[source_type].service_type
    return source_types[source_type].initial(
        name,
        services[service_type],
        is_pushdown,
    )


def get_sources(services, is_pushdown):
    _sources = dict()
    for source_type in source_types:
        service_type = source_types[source_type].service_type
        _sources[source_type] = source_types[source_type].reflect_datasource_names(
            services[service_type],
            is_pushdown,
        )
    return _sources
