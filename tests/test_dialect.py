# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from sqlalchemy import create_engine


def test_connection():
    dsn = 'kylin://ADMIN:KYLIN@sandbox:7070'
    kylin = create_engine(dsn)
    assert str(kylin.url) == dsn
