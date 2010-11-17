'''
Created on May 10, 2010

@author: kblaszcz
'''
from sites.tools.ObjectCreator import createObject
from sites.treemap.objecttree.Postprocessors import PostProcessorBase
from sites.treemap.objecttree.Wrapper import Wrapper
import inspect

class TreeNode(object):
    '''
    classdocs
    '''
    def __init__(self, theobject, evalcolumnname, parentmethodname, fpar = None, dpt = -1, siblings_sum = 1.0, postprocessobject = None):
        if dpt >= -1:
            self.depth = dpt
        else:
            raise Exception( 'invalid depth: ' + dpt)
        
        self.fparam = fpar
        self.postprocessobject = postprocessobject
        self.wrapped = Wrapper(theobject, evalcolumnname)
        self.evaled = False
        self.evalvalue = 0
        
        if siblings_sum > 0.0:
            self.siblings_sum = siblings_sum
        else:
            self.siblings_sum = 1.0
            
        self.parentmethodname = parentmethodname
        #additional attribute "metainfo" can be added from outside
    
    def __str__(self):
        return self.__class__.__name__ + ': ' + self.wrapped.__str__()
    
    def evaluate(self):
        if not self.evaled:
            self.evalvalue = self.wrapped.evaluate(self.fparam, self.postprocessobject)
            self.evaled = True
        return self.evalvalue/self.siblings_sum
    
    def setPostProcessorObject(self, ppobj):
        self.evaled = False
        assert((ppobj is None) or isinstance(ppobj,PostProcessorBase))
        self.postprocessobject = ppobj
    
    def getEvalValue(self):
        if not self.evaled:
            self.evalvalue = self.wrapped.evaluate(self.fparam, self.postprocessobject)
            self.evaled = True
        return self.evalvalue/1.0
    
    def getEvalValueNoPostProcess(self):
        return self.wrapped.evaluateNoPostProcess(self.fparam)
    
    def getNakedParent(self):
        nested_object = self.wrapped.getObject()
        
        modulename = inspect.getmodule(nested_object).__name__
        classname = nested_object.__class__.__name__
        
        try:
            instance = createObject(modulename, classname)
        except:
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
            raise Exception( 'depth cannot be less than 0: ' + value)
        
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
            raise Exception( 'invalid value detected: depth = ' + self.depth)
        
    def setToUnknown(self):
        self.depth = -1
        
    def isRoot(self):
        if self.depthIsUnknown() or self.depth != 0:
            return False
        elif self.depth == 0:
            return True
        else:
            return False
        
        