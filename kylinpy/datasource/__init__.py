# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import inspect

from .cube_source import CubeSource  # noqa
from .hive_source import HiveSource  # noqa
from ._source_interface import SourceInterface

source_types = {
    o.source_type: o for o in globals().values()
    if (
        inspect.isclass(o) and
        issubclass(o, SourceInterface) and
        o.source_type is not None
    )
}
