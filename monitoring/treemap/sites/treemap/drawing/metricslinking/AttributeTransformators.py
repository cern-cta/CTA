class AttributeTransformatorInterface(object):
    def __init__(self):
        pass
    
    def transform(self, dbobj, columnname):
        raise Exception("implementation of transform() not found")
    
    def getMax(self):
        raise Exception("implementation of getMax() not found")
    
    def getMin(self):
        raise Exception("implementation of getMin() not found")
    
    def isFloat(self):
        raise Exception("implementation of isFloat() not found")
    
    def isText(self):
        raise Exception("implementation of isText() not found")

class FileExtensionTransformator(AttributeTransformatorInterface):

    def __init__(self, min = 0, max = 23):
        AttributeTransformatorInterface.__init__(self)
        if max < min: max, min = min, max
        self.max = max
        self.min = min
            
        
        
    def transform(self, dbobj, columnname):
        try:
            ext = self.findExtension(dbobj.__dict__[columnname])
        except KeyError:
            raise Exception("column doesn't exist")
        
        if not ((type(ext).__name__ == 'str') or (type(ext).__name__ == 'unicode')):
            raise Exception("string or unicode expected")
        
        return int(self.calcExtHash(ext)%(self.max-self.min))+self.min

    def findExtension(self, text):
        dot = string.rfind(text, '.')
        if dot < 0 or len(text)>16: 
            return ''
        else:
            return text[dot+1:]
        
    def calcExtHash(self, ext):
        hash = 0
        for idx, char in enumerate(ext):
            c = ord(char)
            significant = ((c&23)|((c&16)>>1))&15
            hash = hash + significant << (idx*4)
        return hash
    
    def getMax(self):
        return self.max
    
    def getMin(self):
        return self.min
    
    def isFloat(self):
        return False
    
    def isText(self):
        return False
    
class SaturatedLinearTransformator(AttributeTransformatorInterface):
    
    def __init__(self, minsat = 0.0, maxsat = 1.0):
        AttributeTransformatorInterface.__init__(self)
        if maxsat < minsat: maxsat ,minsat = minsat, maxsat
        self.maxsat = max
        self.minsat = min
        
    def transform(self, dbobj, columnname):
        try:
            ext = self.findExtension(dbobj.__dict__[columnname])
        except KeyError:
            raise Exception("column doesn't exist")
        
        if not ((type(ext).__name__ == 'int') or (type(ext).__name__ == 'long')) or (type(ext).__name__ == 'float'):
            raise Exception("number expected")
        
        if ext >= self.maxsat:
            return 1.0
        elif ext <= self.minsat:
            return 0.0
        else:
            return (ext-self.minsat)/(self.maxsat-self.minsat)
        
    def getMax(self):
        return 1.0
    
    def getMin(self):
        return 0.0
    
    def isFloat(self):
        return True
    
    def isText(self):
        return False
    
class RawLinearTransformator(AttributeTransformatorInterface):
    
    def __init__(self, minsat = 0.0, maxsat = 1.0):
        AttributeTransformatorInterface.__init__(self)
        self.typename = None
        self.isfloat = None
        self.istext = None
        
    def transform(self, dbobj, columnname):
        try:
            ext = dbobj.__dict__[columnname]
        except KeyError:
            raise Exception("column doesn't exist")
        
        self.isfloat = False
        self.istext = False
        if (type(ext).__name__ == 'float'):
            self.isfloat = True
        elif ((type(ext).__name__ == 'str') or (type(ext).__name__ == 'unicode')):
            self.istext = True

        return ext
        
    def getMax(self):
        return None
    
    def getMin(self):
        return None
    
    #that information is related to the last transformation
    def isFloat(self):
        return self.isfloat
    
    def isText(self):
        return self.istext
    
class DirNameTransformator(AttributeTransformatorInterface):
    
    def __init__(self, prefix):
        AttributeTransformatorInterface.__init__(self)
        self.prefix = prefix
        
    def transform(self, dbobj, columnname):
        try:
            ext = dbobj.__dict__[columnname]
        except KeyError:
            raise Exception("column doesn't exist")
        
        if not ((type(ext).__name__ == 'str') or (type(ext).__name__ == 'unicode')):
            raise Exception("string or unicode expected")

        return (self.prefix + ext)
        
    def getMax(self):
        return None
    
    def getMin(self):
        return None
    
    #that information is related to the last transformation
    def isFloat(self):
        return False
    
    def isText(self):
        return True
    
class TopDirNameTransformator(AttributeTransformatorInterface):
    
    def __init__(self):
        AttributeTransformatorInterface.__init__(self)
        
    def transform(self, dbobj, columnname):
        try:
            ext = dbobj.__dict__[columnname]
        except KeyError:
            raise Exception("column doesn't exist")
        
        if not ((type(ext).__name__ == 'str') or (type(ext).__name__ == 'unicode')):
            raise Exception("string or unicode expected")

        pos = ext.rfind('/')
        if pos >= 0: ext = ext[pos:]
        return ext
        
    def getMax(self):
        return None
    
    def getMin(self):
        return None
    
    #that information is related to the last transformation
    def isFloat(self):
        return False
    
    def isText(self):
        return True