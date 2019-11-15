class Result():
    def __init__(self, **kargs):
        self.total = kargs.get('total', 0)
        self.records = kargs.get('records', [])
        self._areResultChanged = kargs.get('areResultChanged', False)

    @property
    def areResultChanged(self):
        return self._areResultChanged

    @age.setter
    def areResultChanged(self, boolVal):
        self._areResultChanged = boolVal
