# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals


class JobInterface(object):

    def pause(self):
        raise NotImplementedError()

    def resume(self):
        raise NotImplementedError()

    def cancel(self):
        raise NotImplementedError()

    def get_desc(self):
        raise NotImplementedError()

    def drop(self):
        raise NotImplementedError()
