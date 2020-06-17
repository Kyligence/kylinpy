# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from ._job_interface import JobInterface


class Ke3Job(JobInterface):
    def __init__(self, job_id, service):
        self.job_id = job_id
        self.service = service

    def resume(self):
        return self.service.maintain_job(self.job_id, 'resume')

    def cancel(self):
        return self.service.maintain_job(self.job_id, 'cancel')

    def get_desc(self):
        return self.service.job_desc(self.job_id)

    def pause(self):
        return self.service.maintain_job(self.job_id, 'pause')

    def drop(self):
        return self.service.drop_job(self.job_id)

    @property
    def status(self):
        desc = self.get_desc()
        if desc:
            return desc.get('job_status')
        return None

    def __repr__(self):
        return '<KylinJob instance: {}>'.format(self.job_id)
