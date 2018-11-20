
class Buffer():
    def __init__(self):
        self.value = None

    @property
    def value(self):
        return self._value

    @value.setter
    def value(self, value):
        self._value = value
