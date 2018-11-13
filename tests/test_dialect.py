# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import json
import os

from sqlalchemy import create_engine

from kylinpy.utils.compat import as_unicode


class TestDialect(object):
    dsn = 'kylin://ADMIN:KYLIN@sandbox:7070/learn_kylin'
    cn_dsn = 'kylin://用户:密码@sandbox:7070/中文project'

    def json_loads(self, filename):
        here = os.path.abspath(os.path.dirname(__file__))
        f = open(os.path.join(here, 'fixtures', '{}.json'.format(filename)), 'r').read()
        return json.loads(f)

    def test_connection(self):
        kylin = create_engine(self.dsn)
        assert as_unicode(kylin.url) == self.dsn
        assert kylin.url.host == 'sandbox'
        assert kylin.url.username == 'ADMIN'
        assert kylin.url.password == 'KYLIN'

        kylin = create_engine(self.cn_dsn)
        assert as_unicode(kylin.url) == self.cn_dsn
