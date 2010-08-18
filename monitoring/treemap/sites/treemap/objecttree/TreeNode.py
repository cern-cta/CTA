'''
Created on May 10, 2010

@author: kblaszcz
'''
from sites.treemap.objecttree.Wrapper import Wrapper
from sites.errors import ConfigError
from sites.tools.ObjectCreator import createObject
import inspect

class TreeNode(object):
    '''
    classdocs
    '''
    def __init__(self, theobject, evalcolumnname, parentmethodname, fpar = None, dpt = -1, siblings_sum = 1.0):
        if dpt >= -1:
            self.depth = dpt
        else:
            raise ConfigError( 'invalid depth: ' + dpt)
        
        self.fparam = fpar
        self.wrapped = Wrapper(theobject, evalcolumnname)
        self.evaled = False
        self.evalvalue = 0
        
        if siblings_sum > 0.0:
            self.siblings_sum = siblings_sum
        else:
            self.siblings_sum = 1.0
            
        self.parentmethodname = parentmethodname
    
    def __str__(self):
        return self.__class__.__name__ + ': ' + self.wrapped.__str__()
    
    def evaluate(self):
        if not self.evaled:
            self.evalvalue = self.wrapped.evaluate(self.fparam)
            self.evaled = True
        return self.evalvalue/self.siblings_sum
    
    def getEvalValue(self):
        if not self.evaled:
            self.evalvalue = self.wrapped.evaluate(self.fparam)
            self.evaled = True
        return self.evalvalue/1.0
    
    def getNakedParent(self):
        nested_object = self.wrapped.getObject()
        
        modulename = inspect.getmodule(nested_object).__name__
        classname = nested_object.__class__.__name__
        
        try:
            instance = createObject(modulename, classname)
        except ConfigError:
            return nested_object 
        
        #check methodname
        found = False
        members = instance.__class__.__dict__.keys()
        for membername in members:
            if ((type(instance.__class__.__dict__[membername]).__name__) == 'function') and membername == self.parentmethodname:
                found = True
                break
            
        if not found:
            return nested_object
        else:
            return nested_object.__class__.__dict__[self.parentmethodname](nested_object)
    
    def setColumnname(self, newname):
        self.wrapped.setColumnname(newname)
        self.evaled = False
        
    def setFparam(self, fparam):
        self.fparam = fparam
        self.evaled = False
        
    def setSiblingsSum(self, sum):
        if sum > 0.0:
            self.siblings_sum = sum
            
    def getSiblingsSum(self):
            return self.siblings_sum 
    
    def setDepth(self, value): 
        if value >= 0:
            self.depth = value
        else:
            raise ConfigError( 'depth cannot be less than 0: ' + value)
        
    def getDepth(self):
        return self.depth

    def getColumnname(self):
        return self.wrapped.getColumnname()
    
    def getObject(self):
        return self.wrapped.getObject()
        
    def depthIsUnknown(self):
        if self.depth == -1:
            return True
        elif self.depth >= 0:
            return False
        else:
            raise ConfigError( 'invalid value detected: depth = ' + self.depth)
        
    def setToUnknown(self):
        self.depth = -1
        
    def isRoot(self):
        if self.depthIsUnknown() or self.depth != 0:
            return False
        elif self.depth == 0:
            return True
        else:
            return False
        
        