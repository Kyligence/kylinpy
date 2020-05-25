# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals


class ServiceInterface(object):
    def query(self, sql, limit=50000, offset=0, acceptPartial=False, **kwargs):
        raise NotImplementedError()

    def projects(self, **kwargs):
        raise NotImplementedError()

    def jobs(self, **kwargs):
        raise NotImplementedError()

    def maintain_job(self, **kwargs):
        raise NotImplementedError()

    def tables_and_columns(self, **kwargs):
        raise NotImplementedError()

    def tables_in_hive(self, **kwargs):
        raise NotImplementedError()

    def cube_desc(self, name, **kwargs):
        raise NotImplementedError()

    def model_desc(self, name, **kwargs):
        raise NotImplementedError()

    def models(self, **kwargs):
        raise NotImplementedError()

    def cubes(self, name=None, **kwargs):
        raise NotImplementedError()

    def get_authentication(self, **kwargs):
        raise NotImplementedError()
