
class Crawler():
    def __init__(self):
        self.return_value = None
        self.filter = None
        self.query_return = None

    @property
    def reader(self):
        return self._reader

    @reader.setter
    def reader(self, value):
        self._reader = value

    @property
    def filter(self):
        return self._filter

    @filter.setter
    def filter(self, value):
        self._filter = value

    @property
    def query_return(self):
        return self._query_return

    @query_return.setter
    def query_return(self, value):
        self._query_return = value
