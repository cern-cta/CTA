'''
Created on May 5, 2010
@author: kblaszcz
'''
import warnings
import inspect
from sites.treemap.objecttree.columntransformation.ColumnTransformator import ColumnTransformator
from sites.tools.Inspections import *
from sites.treemap.objecttree.Postprocessors import *

class Wrapper(object):
    '''
    Wraps a Database object to make it usable for a Treemap by providing an attribute specific evaluate() function
    '''
    column_transformators = {}

    def __init__(self, wrapped, evalattrname):
        '''
        Constructor
        '''
        object.__init__(self)
        self.wrapped = wrapped
        self.modulename = inspect.getmodule(wrapped).__name__
        
        self.classname = wrapped.__class__.__name__
        self.attrname = ""

        if self.classname not in self.__class__.column_transformators:
            self.__class__.column_transformators[self.classname] = ColumnTransformator(self.classname)
        
        self.setAttrName(evalattrname)
    
    def __str__(self):
        return self.modulename.__str__() + '.' + self.classname.__str__() + ': ' + self.wrapped.__str__()
    
    def evaluate(self, fparam = None, postprocessobject = None):
        result = None
        command = "result = self.__class__.column_transformators[self.classname]." + self.__class__.column_transformators[self.classname].getFnprefix() + self.attrname + "(self.wrapped."+ self.attrname
        
        if fparam:
            command = command + ", fparam)"
        else:
            command = command + ")"
        
        exec(command)
        
        if (postprocessobject is not None):
            result = postprocessobject.process(result)
        
        return result
    
    def evaluateNoPostProcess(self, fparam = None):
        result = None
        command = "result = self.__class__.column_transformators[self.classname]." + self.__class__.column_transformators[self.classname].getFnprefix() + self.attrname + "(self.wrapped."+ self.attrname
        
        if fparam:
            command = command + ", fparam)"
        else:
            command = command + ")"
        
        exec(command)
        
        return result
    
    def setAttrName(self, newname):
        if newname == None:
            raise Exception( 'Wrapper was not able to find any default column for ' + self.classname)
        
        allnames = self.__class__.column_transformators[self.classname].getColumnAndAtrributeNames()
        if allnames:
            found = False
            for name in allnames:
                if name == newname:
                    found = True
                    break
            if not found:
                raise Exception( 'Wrapper was not able to find the given Column "' + newname + '" in ' + self.classname)
            self.attrname = newname
            
        else:
            raise Exception( 'Wrapper was not able to find any Columns for ' + self.classname)
        
    def getAttrName(self):
        return self.attrname
    
    def getObject(self):
        return self.wrapped
        