'''
Created on May 5, 2010

@author: kblaszcz
'''
import warnings
import inspect
from sites.treemap.objecttree.columntransformation.ColumnTransformator import ColumnTransformator
from sites.errors import ConfigError

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

        
        self.fullmodule = self.modulename.__str__() + '.' + self.classname

        if self.fullmodule not in self.__class__.column_transformators:
            self.__class__.column_transformators[self.fullmodule] = ColumnTransformator(self.modulename, self.classname)
        
        self.setColumnname(evalcolumnname)
    
    def __str__(self):
        return self.modulename.__str__() + '.' + self.classname.__str__() + ': ' + self.wrapped.__str__()
    
    def evaluate(self, fparam = None):
        command = "result = self.__class__.column_transformators[self.fullmodule]." + self.__class__.column_transformators[self.fullmodule].getFnprefix() + self.columnname + "(self.wrapped."+ self.columnname
        
        if fparam:
            command = command + ", fparam)"
        else:
            command = command + ")"
        
#        if(self.classname == 'Annex'):
#           result = self.__class__.column_transformators[self.fullmodule].transform_evaluation(self.wrapped.evaluation, fparam)
        
        exec(command)
        return result
    
    def setColumnname(self, newname):
        if newname == None:
            columnt = self.__class__.column_transformators[self.fullmodule]
            newname = columnt.getColumnFinder().guessPk().name
            if newname == None:
                raise ConfigError( 'Wrapper was not able to find any default column for ' + self.fullmodule)
            warnings.warn('cloumnname = None: Wrapper decided to evaluate using columnname = ' + newname + ' , model: ' + self.fullmodule, Warning)
            
        if self.__class__.column_transformators[self.fullmodule].getColumns():
            found = False
            for column in self.__class__.column_transformators[self.fullmodule].getColumns():
                if column.name == newname:
                    found = True
                    break
            if not found:
                raise ConfigError( 'Wrapper was not able to find the given Column "' + newname + '" in ' + self.fullmodule)
            self.columnname = newname
            
        else:
            raise ConfigError( 'Wrapper was not able to find any Columns for ' + self.fullmodule)
        
    def getColumnname(self):
        return self.columnname
    
    def getObject(self):
        return self.wrapped
        