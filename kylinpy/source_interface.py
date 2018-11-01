class SourceInterface(object):
    @property
    def name(self):
        raise NotImplementedError

    @property
    def dimensions(self):
        raise NotImplementedError


class ColumnInterface(object):
    @property
    def name(self):
        raise NotImplementedError

    @property
    def datatype(self):
        raise NotImplementedError
