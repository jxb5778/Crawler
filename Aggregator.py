
class Aggregator():
    def __init__(self):
        self.return_value = None

    @property
    def return_value(self):
        return self._return_value

    @return_value.setter
    def return_value(self, value):
        self._return_value = value
