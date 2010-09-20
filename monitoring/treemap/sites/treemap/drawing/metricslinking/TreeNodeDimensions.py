'''
Created on Jul 14, 2010

@author: kblaszcz
'''
import exceptions
from sites.treemap.viewtree.ViewNode  import ViewNode
from sites.tools.Inspections import ColumnFinder
import sys
import inspect
import string
from sites.errors.ConfigError import ConfigError
import math
from sites.dirs.models import *
from sites.treemap.objecttree.Annex import Annex
from sites.tools.TextTools import sizeInBytes

class ViewNodeDimensionBase(object):
    '''
    classdocs
    '''
    def __init__(self, name = 'None', min = 0.0, max = 0.0, isfloat = True, istext = False):
        '''
        Constructor
        '''
        self.min = min
        self.max = max
        self.isfloat = isfloat
        self.istext = istext
        
        self.name = name
        
    def getValue(self, tnode):
        raise Exception("implementation of getValue() not found")
    
    def getName(self):
        return self.name
    
    def getMax(self):
        return self.max

    def getMin(self):
        return self.min
    
    def isFloat(self):
        return self.isfloat
    
    def isText(self):
        return self.istext
    
    

#outputs the level number
class LevelDimension(ViewNodeDimensionBase):
    '''
    classdocs
    '''
    def __init__(self):
        ViewNodeDimensionBase.__init__(self, 'level', 0, None, False, False)
        
    def getValue(self, tnode):
        assert(tnode is not None and isinstance(tnode, ViewNode))
        ret = tnode.getProperty('level')
        
        #convert to integer in case it is not supposed to be float
        if self.isfloat == False: ret = ret - ret%1
        
        #None as min or/and max means there is no limitation of the number
        if not (((self.min is not None) and (self.min > ret)) or ((self.max is not None) and (self.max < ret))):
            return ret
        else:
            raise Exception("invalid level in LevelDimension")
        
class ConstantDimension(ViewNodeDimensionBase):
    '''
    classdocs
    '''
    def __init__(self, constant):
        ViewNodeDimensionBase.__init__(self, 'level', constant, constant, isinstance(constant, float), isinstance(constant, str) or isinstance(constant, unicode))
        
    def getValue(self, tnode):
        return self.min
    
        
class ColumnTransformatorInterface(object):
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

class FileExtensionTransformator(ColumnTransformatorInterface):

    def __init__(self, min = 0, max = 23):
        ColumnTransformatorInterface.__init__(self)
        if max < min: max, min = min, max
        self.max = max
        self.min = min
            
        
        
    def transform(self, dbobj, columnname):
        try:
            ext = self.findExtension(dbobj.__dict__[columnname])
        except KeyError:
            raise ConfigError("column doesn't exist")
        
        if not ((type(ext).__name__ == 'str') or (type(ext).__name__ == 'unicode')):
            raise ConfigError("string or unicode expected")
        
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
    
class SaturatedLinearTransformator(ColumnTransformatorInterface):
    
    def __init__(self, minsat = 0.0, maxsat = 1.0):
        ColumnTransformatorInterface.__init__(self)
        if maxsat < minsat: maxsat ,minsat = minsat, maxsat
        self.maxsat = max
        self.minsat = min
        
    def transform(self, dbobj, columnname):
        try:
            ext = self.findExtension(dbobj.__dict__[columnname])
        except KeyError:
            raise ConfigError("column doesn't exist")
        
        if not ((type(ext).__name__ == 'int') or (type(ext).__name__ == 'long')) or (type(ext).__name__ == 'float'):
            raise ConfigError("number expected")
        
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
    
class RawLinearTransformator(ColumnTransformatorInterface):
    
    def __init__(self, minsat = 0.0, maxsat = 1.0):
        ColumnTransformatorInterface.__init__(self)
        self.typename = None
        self.isfloat = None
        self.istext = None
        
    def transform(self, dbobj, columnname):
        try:
            ext = dbobj.__dict__[columnname]
        except KeyError:
            raise ConfigError("column doesn't exist")
        
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

#evaluates any column to number   
class ColumnDimension(ViewNodeDimensionBase):
    '''
    classdocs
    '''
    def __init__(self, columnname, transformation): #transformation.transform (db object)
        
        assert(transformation is not None)
        ViewNodeDimensionBase.__init__(self, 'column', transformation.getMin(), transformation.getMax(), transformation.isFloat(), transformation.isText())
        self.transformation = transformation
        self.columnname = columnname

        
    def getValue(self, tnode):
        assert(tnode is not None and isinstance(tnode, ViewNode))
        dbobj = tnode.getProperty('treenode').getObject()
        
        ret = self.transformation.transform(dbobj, self.columnname)
        
        #convert to integer in case it is not supposed to be float
        if not self.isfloat and not self.istext: ret = ret - ret%1
        
        #None as min or/and max means there is no limitation of the number
        if not self.istext:
            if not (((self.min is not None) and (self.min > ret)) or ((self.max is not None) and (self.max < ret))):
                return ret
            else:
                raise Exception("invalid level in LevelDimension")
        
        return ret
    
class RawColumnDimension(ViewNodeDimensionBase):
    '''
    classdocs
    '''
    def __init__(self, columnname, transformation = RawLinearTransformator()): #transformation.transform (db object)
        ViewNodeDimensionBase.__init__(self)
        self.transformation = transformation
        self.columnname = columnname
        self.name = 'rawcolumn'

        
    def getValue(self, tnode):
        assert(tnode is not None and isinstance(tnode, ViewNode))
        dbobj = tnode.getProperty('treenode').getObject()
        
        ret = self.transformation.transform(dbobj, self.columnname)
        self.min = self.transformation.getMin()
        self.max = self.transformation.getMax()
        self.isfloat = self.transformation.isFloat()
        self.istext = self.transformation.isText()
        
        return ret

class DirNameTransformator(ColumnTransformatorInterface):
    
    def __init__(self, prefix):
        ColumnTransformatorInterface.__init__(self)
        self.prefix = prefix
        
    def transform(self, dbobj, columnname):
        try:
            ext = dbobj.__dict__[columnname]
        except KeyError:
            raise ConfigError("column doesn't exist")
        
        if not ((type(ext).__name__ == 'str') or (type(ext).__name__ == 'unicode')):
            raise ConfigError("string or unicode expected")

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
    
class DirHtmlInfoDimension(ViewNodeDimensionBase):
    '''
    classdocs
    '''
    def __init__(self): #transformation.transform (db object)
        
        ViewNodeDimensionBase.__init__(self, 'dirhtmlinfo', None, None, False, True)

        
    def getValue(self, tnode):
        assert(tnode is not None and isinstance(tnode, ViewNode))
        dbobj = tnode.getProperty('treenode').getObject()
        
        assert(isinstance(dbobj, Dirs))
        
        ret = []
        size = float(tnode.getProperty('treenode').getEvalValue())
        psize = tnode.getProperty('treenode').getSiblingsSum()
        bytesize = long(dbobj.totalsize)
        
        ret.append("<b>Directory:</b> ")
        ret.append(splitText(dbobj.fullname.__str__(), 50, 39))
        
        ret.append("<br><b> Evaluation of ")
        ret.append(tnode.getProperty('treenode').getColumnname())
        ret.append(":</b> ")
        ret.append("%.2f"%(float(tnode.getProperty('treenode').getEvalValueNoPostProcess())))
        
        ret.append("<br><b>Processed value:</b> ")
        ret.append("%.2f"%(float(tnode.getProperty('treenode').getEvalValue())))
        
        ret.append("<br><br><b>size:</b> ")
        
        ret.append(sizeInBytes(bytesize))
            
        ret.append(" (")
        ret.append(long(bytesize).__str__())
        ret.append(" Bytes)")
        
        ret.append("<br><b>parent percentage:</b> ")
        if(psize == 0):
            ret.append("%.2f"%(100))
        else:
            ret.append("%.2f"%(size/psize*100.0))
            
        ret.append("%")
        
        ret.append("<br><b>number of files: </b> ")
        ret.append(dbobj.countFiles().__str__())
        ret.append("<br><b>number of directories: </b> ")
        ret.append(dbobj.countDirs().__str__())
        ret.append("<br><br><b>number of files in subtree: </b> ")
        ret.append(dbobj.nbfiles.__str__())
        ret.append("<br><b>number of directories in subtree: </b> ")
        ret.append(dbobj.nbsubdirs.__str__())
        
        return ''.join([bla for bla in ret])
        
class FileHtmlInfoDimension(ViewNodeDimensionBase):
    '''
    classdocs
    '''
    def __init__(self): #transformation.transform (db object)
        
        ViewNodeDimensionBase.__init__(self, 'filehtmlinfo', None, None, False, True)

        
    def getValue(self, tnode):
        assert(tnode is not None and isinstance(tnode, ViewNode))
        dbobj = tnode.getProperty('treenode').getObject()
        parent = tnode.getProperty('treenode').getNakedParent()
        assert(isinstance(dbobj, CnsFileMetadata))
        
        ret = []
        size = float(tnode.getProperty('treenode').getEvalValue())
        psize = tnode.getProperty('treenode').getSiblingsSum()
        bytesize = long(dbobj.filesize)
        dirname = parent.__str__()
        
        ret.append("<b>File name:</b> ")
        ret.append(splitText(dbobj.name.__str__(), 50, 39))
        ret.append("<br><b>Directory:</b> ")
        ret.append(splitText(dirname, 50, 39))
        
        ret.append("<br><b> Evaluation of ")
        ret.append(tnode.getProperty('treenode').getColumnname())
        ret.append(":</b> ")
        ret.append("%.2f"%(float(tnode.getProperty('treenode').getEvalValueNoPostProcess())))
        
        ret.append("<br><b>Processed value:</b> ")
        ret.append("%.2f"%(float(tnode.getProperty('treenode').getEvalValue())))
        
        ret.append("<br><br><b>size:</b> ")
        
        ret.append(sizeInBytes(bytesize))
            
        ret.append(" (")
        ret.append(long(bytesize).__str__())
        ret.append(" Bytes)")
        
        ret.append("<br><b>parent percentage:</b> ")
        if(psize == 0):
            ret.append("%.2f"%(100))
        else:
            ret.append("%.2f"%(size/psize*100.0))
            
        ret.append("%")

        
        return ''.join([bla for bla in ret])
    
class AnnexHtmlInfoDimension(ViewNodeDimensionBase):
    '''
    classdocs
    '''
    def __init__(self): #transformation.transform (db object)
        
        ViewNodeDimensionBase.__init__(self, 'annexhtmlinfo', None, None, False, True)

        
    def getValue(self, tnode):
        assert(tnode is not None and isinstance(tnode, ViewNode))
        tnobj = tnode.getProperty('treenode')
        dbobj = tnobj.getObject()
        parent = tnode.getProperty('treenode').getNakedParent()
        
        assert(isinstance(dbobj, Annex))
        
        ret = []
        size = tnobj.getEvalValue()
        psize = tnobj.getSiblingsSum()
        dirname = parent.__str__()
        
        ret.append("<b>The rest of the items</b> ")
        ret.append("<br><b>Directory:</b> ")
        ret.append(splitText(dirname, 50, 39))
        ret.append("<br><b>Processed size:</b> ")
        
        ret.append(sizeInBytes(size))
            
        ret.append(" (")
        ret.append((long(size)).__str__())
        ret.append(" Bytes)")
        
        ret.append("<br><b>parent percentage:</b> ")
        if(psize == 0):
            ret.append("%.2f"%(100))
        else:
            ret.append("%.2f"%(size/psize*100.0))
            
        ret.append("%")
        
        ret.append("<br><b>number of items: </b> ")
        ret.append(dbobj.countItems().__str__())

        return ''.join([bla for bla in ret])
    
def splitText(text, limit, firstlimit):
    limitold = limit
    limit = firstlimit
    assert(limit > 1)
    ret = []
    while len(text) > 0:
        if(len(text)<=limit):
            ret.append(text)
            break
        else:
            bestlimit = limit
            alternativelimit = text[:limit].rfind('/')
            if alternativelimit > 1: bestlimit = alternativelimit
            
            ret.append(text[:bestlimit])
            ret.append('<br>')
            text = text[bestlimit:]
        limit = limitold
            
    return ''.join([bla for bla in ret])
        
        