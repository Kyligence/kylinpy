from .kylinpy import Kylinpy
from .utils.kylin_types import kylin_to_python
from .logger import logger
from .errors import KylinDBAPIError, KylinConnectionError


class Cursor(object):
    def __init__(self, connection):
        self.connection = connection
        self._arraysize = 1

        self.description = None
        self.rowcount = -1
        self.results = None
        self.fetched_rows = 0

    def callproc(self):
        logger.warn('Stored procedures not supported in Kylin')

    def close(self):
        logger.debug('Cursor close called')

    def execute(self, query, *params, **kwargs):
        # todo query params
        def get_col(x):
            for l in kwargs.get('labels', set()):
                if l.lower() == x.lower():
                    return l
            return x

        resp = self.connection.query(query).get('data')
        self.description = [[
            get_col(c['label']),
            c['columnTypeName'].lower(),
            c['displaySize'],
            0,
            c['precision'],
            c['scale'],
            c['isNullable']
        ] for c in resp['columnMetas']]

        self.results = [[
            kylin_to_python(resp['columnMetas'][idx]['columnTypeName'], cell) for (idx, cell) in enumerate(row)
        ] for row in resp['results']]
        self.rowcount = len(self.results)
        self.fetched_rows = 0
        return self.rowcount

    def executemany(self, query, seq_params=[]):
        results = []
        for param in seq_params:
            self.execute(query, param)
            results.extend(self.results)

        self.results = results
        self.rowcount = len(self.results)
        self.fetched_rows = 0
        return self.rowcount

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
        logger.warn('Nextset operation not supported in Kylin')

    @property
    def arraysize(self):
        return self._arraysize

    @arraysize.setter
    def arraysize(self, array_size):
        self._arraysize = array_size

    def setinputsizes(self):
        logger.warn('setinputsize not supported in Kylin')

    def setoutputsize(self):
        logger.warn('setoutputsize not supported in Kylin')


class KylinDB(Kylinpy):
    paramstyle = 'pyformat'
    threadsafety = 2
    apilevel = '2.0'
    Error = KylinDBAPIError

    def __init__(self, **kwargs):
        super(KylinDB, self).__init__(**kwargs)

    @classmethod
    def connect(cls, **kwargs):
        try:
            return cls(**kwargs)
        except TypeError:
            raise KylinConnectionError

    def close(self):
        return

    def commit(self):
        return

    def rollback(self):
        return

    def cursor(self):
        return Cursor(self)
