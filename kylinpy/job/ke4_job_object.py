# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from kylinpy.exceptions import UnsupportApiError
from ._job_interface import JobInterface


class Ke4Job(JobInterface):
    def __init__(self, job_id, service):
        self.job_id = job_id
        self.service = service

    def resume(self):
        raise UnsupportApiError("Now ke4 jobs does not support resume")

    def cancel(self):
        raise UnsupportApiError("Now ke4 jobs does not support cancel")

    def get_desc(self):
        return self.service.job_desc(self.job_id)

    def pause(self):
        raise UnsupportApiError("Now ke4 jobs does not support pause")

    def drop(self):
        raise UnsupportApiError("Now ke4 jobs does not support drop")

    @property
    def status(self):
        desc = self.get_desc()
        if desc:
            return desc.get('job_status')
        return None

    def __repr__(self):
        return '<KylinJob instance: {}>'.format(self.job_id)
