'''
Created on Jul 14, 2010

@author: kblaszcz
'''
import exceptions
from sites.treemap.viewtree import ViewNode
from sites.tools.ColumnFinder import ColumnFinder
import sys
import inspect
import string

class ViewNodeDimensionBase(object):
    '''
    classdocs
    '''
    def __init__(self, name = 'None', min = 0.0, max = 0.0, isfloat = True):
        '''
        Constructor
        '''
        self.min = min
        self.max = max
        self.isfloat = isfloat
        
        self.name = name
        
    def getValue(self, tnode):
        raise Exception("implementation of getValue() not found")
    
    def getName(self):
        return self.name
    
    def getMax(self):
        return max

    def getMin(self):
        return min
    
    def isFloat(self):
        return self.isfloat

#outputs the level number
class LevelDimension(ViewNodeDimensionBase):
    '''
    classdocs
    '''
    def __init__(self):
        ViewNodeDimensionBase.__init__(self, 'level', 0, None, False)
        
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

class FileExtensionTransfomator(object):
    def __init__(self):
        self.extensions = {str(''): 0}
        self.indexcounter = 1
        
    def transform(self, dbobj):
        
        modulename = inspect.getmodule(dbobj).__name__
        if modulename != 'sites.dirs.models':
            raise Exception("This transformator is only for sites.dirs.models.CnsFileMetadata")
        
        if dbobj.__class__.__name__ != 'CnsFileMetadata':
            raise Exception("This transformator is only for sites.dirs.models.CnsFileMetadata")
        
        ext = self.findExtension(dbobj.name)
        
        try:
            return self.extensions[ext]
        except KeyError:
            self.extensions[ext] = self.indexcounter
            self.indexcounter = self.indexcounter + 1
            return self.indexcounter - 1
        
    def findExtension(self, text):
        dot = string.rfind(text, '.')
        if dot < 0: 
            return ''
        else:
            return text[dot+1:]
        

        


#evaluates any column to number   
class ColumnDimension(ViewNodeDimensionBase):
    '''
    classdocs
    '''
    def __init__(self, modulename, classname, transformation = None, min=None, max=None, isfloat = True): #transformation.transform (db object)
        
        ViewNodeDimensionBase.__init__(self, 'column', min, max, isfloat)
        
        #check modulename
        if not self.modulename in sys.modules.keys():
            try:
                module = __import__( self.modulename )
            except ImportError, e:
                raise Exception( 'Unable to load module: ' + self.modulename )
        else:
            self.module = sys.modules[self.modulename]
        
        #check classname
        classes = dict(inspect.getmembers( module, inspect.isclass ))
        if not classname in classes:
            raise Exception( 'Unable to find ' + classname + ' class in ' + modulename )
            
        self.modulename = modulename
        self.classname = classname
        
        assert(transformation is not None)
        
        self.transformation = transformation

        
    def getValue(self, tnode):
        assert(tnode is not None and isinstance(tnode, ViewNode))
        dbobj = tnode.getProperty('treenode').getObject()
        
        ret = self.transformation.transform(dbobj)
        
        #convert to integer in case it is not supposed to be float
        if self.isfloat == False: ret = ret - ret%1
        
        #None as min or/and max means there is no limitation of the number
        if not (((self.min is not None) and (self.min > ret)) or ((self.max is not None) and (self.max < ret))):
            return ret
        else:
            raise Exception("invalid level in LevelDimension")