# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from ._source_interface import (
    DimensionInterface, MeasureInterface, SourceInterface,
)

try:
    from sqlalchemy import sql
except ImportError:
    pass


class JobSource(SourceInterface):
    source_type = 'job'

    def __init__(self, job_desc):
        self.job_desc = job_desc

    @property
    def id(self):
        return self.job_desc.get('uuid')

    @property
    def name(self):
        return self.job_desc.get('name')

    @property
    def duration(self):
        return self.job_desc.get('duration')

    @property
    def project_name(self):
        return self.job_desc.get('project_name')

    @property
    def display_cube_name(self):
        return self.job_desc.get('display_cube_name')

    @property
    def related_cube(self):
        return self.job_desc.get('related_cube')

    @property
    def job_status(self):
        return self.job_desc.get('job_status')

    @property
    def submitter(self):
        return self.job_desc.get('submitter')

    @property
    def progress(self):
        return self.job_desc.get('progress')

    @property
    def info(self):
        return self.job_desc.get('info')

    @property
    def steps(self):
        return self.job_desc.get('steps')

    @property
    def type(self):
        return self.job_desc.get('type')

    @property
    def last_modified(self):
        return self.job_desc.get('last_modified')

    def __repr__(self):
        return ('<Job Instance by '
                'job_name: {self.name}, '
                'type: {self.type}>').format(**locals())

    def resume(self):
        pass

    def pause(self):
        pass

    def cancel(self):
        pass

    def drop(self):
        pass

    def output(self, step_id):
        _step = [step for step in self.steps if step['id'] == step_id]
        if _step:
            return _step[0]
