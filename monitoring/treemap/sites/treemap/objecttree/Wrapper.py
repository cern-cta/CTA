'''
Created on May 5, 2010

@author: kblaszcz
'''
import warnings
import inspect
from sites.treemap.objecttree.columntransformation.ColumnTransformator import ColumnTransformator
from sites.errors import ConfigError
from sites.tools.Inspections import *
from sites.treemap.objecttree.Postprocessors import *

class Wrapper(object):
    '''
    Wraps a Database object to make it usable for a Treemap by providing an column specific evaluate() function
    '''
    column_transformators = {}

    def __init__(self, wrapped, evalcolumnname):
        '''
        Constructor
        '''
        object.__init__(self)
        self.wrapped = wrapped
        self.modulename = inspect.getmodule(wrapped).__name__
        
        self.classname = wrapped.__class__.__name__
        self.columnname = ""

        if self.classname not in self.__class__.column_transformators:
            self.__class__.column_transformators[self.classname] = ColumnTransformator(self.classname)
        
        self.setColumnname(evalcolumnname)
    
    def __str__(self):
        return self.modulename.__str__() + '.' + self.classname.__str__() + ': ' + self.wrapped.__str__()
    
    def evaluate(self, fparam = None, postprocessobject = None):
        result = None
        command = "result = self.__class__.column_transformators[self.classname]." + self.__class__.column_transformators[self.classname].getFnprefix() + self.columnname + "(self.wrapped."+ self.columnname
        
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
        command = "result = self.__class__.column_transformators[self.classname]." + self.__class__.column_transformators[self.classname].getFnprefix() + self.columnname + "(self.wrapped."+ self.columnname
        
        if fparam:
            command = command + ", fparam)"
        else:
            command = command + ")"
        
        exec(command)
        
        return result
    
    def setColumnname(self, newname):
        if newname == None:
#            columnt = self.__class__.column_transformators[self.classname]
#            newname = columnt.getColumnFinder().guessPk().name
#            if newname == None:
            raise ConfigError( 'Wrapper was not able to find any default column for ' + self.classname)
#            warnings.warn('cloumnname = None: Wrapper decided to evaluate using columnname = ' + newname + ' , model: ' + self.classname, Warning)
            
        if self.__class__.column_transformators[self.classname].getColumns():
            found = False
            for column in self.__class__.column_transformators[self.classname].getColumns():
                if column.name == newname:
                    found = True
                    break
            if not found:
                raise ConfigError( 'Wrapper was not able to find the given Column "' + newname + '" in ' + self.classname)
            self.columnname = newname
            
        else:
            raise ConfigError( 'Wrapper was not able to find any Columns for ' + self.classname)
        
    def getColumnname(self):
        return self.columnname
    
    def getObject(self):
        return self.wrapped
        