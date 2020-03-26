# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from kylinpy.client import HTTPError
from kylinpy.kylinpy import Kylin
from kylinpy.utils.compat import as_unicode
from kylinpy.utils.kylin_types import kylin_to_python


class Cursor(object):
    def __init__(self, connection):
        self.connection = connection
        self._arraysize = 1
        self.rowcount = -1
        self.results = []
        self.fetched_rows = 0
        self._column_metas = []

    def callproc(self):
        pass

    def close(self):
        pass

    @property
    def description(self):
        return tuple([
            as_unicode(c['label']),
            c['columnTypeName'].lower(),
            c['displaySize'],
            None,
            c['precision'],
            c['scale'],
            c['isNullable'],
        ] for c in self._column_metas)

    def execute(self, query, parameters={}):
        resp = self.connection.query(query, **parameters)

        self._column_metas = resp.get('columnMetas')
        self.results = [tuple([
            kylin_to_python(self.description[col][1], cell)
            for (col, cell) in enumerate(row)
        ]) for row in resp['results']]
        self.rowcount = len(self.results)
        self.fetched_rows = 0

    def executemany(self, query, seq_params=[]):
        results = []
        for param in seq_params:
            self.execute(query, param)
            results.extend(self.results)

        self.results = results
        self.rowcount = len(self.results)
        self.fetched_rows = 0

    def fetchone(self):
        if self.fetched_rows < self.rowcount:
            row = self.results[self.fetched_rows]
            self.fetched_rows += 1
            return row
        else:
            return None

    def fetchmany(self, size=None):
        fetched_rows = self.fetched_rows
        size = size or self.arraysize
        self.fetched_rows = fetched_rows + size
        return self.results[fetched_rows: self.fetched_rows]

    def fetchall(self):
        fetched_rows = self.fetched_rows
        self.fetched_rows = self.rowcount
        return self.results[fetched_rows:]

    def nextset(self):
        pass

    @property
    def arraysize(self):
        return self._arraysize

    @arraysize.setter
    def arraysize(self, array_size):
        self._arraysize = array_size

    def setinputsizes(self):
        pass

    def setoutputsize(self):
        pass


class Connection(Kylin):
    paramstyle = 'pyformat'
    threadsafety = 2
    apilevel = '2.0'
    Error = HTTPError

    def __init__(self, **kwargs):
        super(Connection, self).__init__(**kwargs)

    @classmethod
    def connect(cls, **kwargs):
        return cls(**kwargs)

    def close(self):
        pass

    def commit(self):
        pass

    def rollback(self):
        pass

    def cursor(self):
        return Cursor(self)
