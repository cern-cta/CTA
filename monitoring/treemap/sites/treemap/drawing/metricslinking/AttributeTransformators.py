import string

class AttributeTransformatorInterface(object):
    def __init__(self):
        pass
    
    def transform(self, dbobj, columnname):
        raise Exception("implementation of transform() not found")

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
        
        ext.isfloat = False
        ext.istext = False
        if (type(ext).__name__ == 'float'):
            ext.isfloat = True
        elif ((type(ext).__name__ == 'str') or (type(ext).__name__ == 'unicode')):
            ext.istext = True

        return ext
    
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