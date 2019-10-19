class Result():
    def __init__(self, **kargs):
        self.total = kargs.get('total', 0);
        self.records = kargs.get('records', []);
        self.areResultChanged = kargs.get('areResultChanged', False);

     # a getter function
     @property
     def areResultChanged(self):
         return self.areResultChanged
       
     # a setter function
     @age.setter
     def areResultChanged(self, boolVal):
         self.areResultChanged = boolVal

