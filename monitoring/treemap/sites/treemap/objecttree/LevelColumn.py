'''
Created on May 5, 2010

@author: kblaszcz
'''
from sites.tools.ModelsInspection import ColumnFinder
from sites.errors import ConfigError

class LevelColumn(object):
    '''
    Associates a Level to a Column
    If level higher than len(self.columns) the class rotates using modulo
    '''
    def __init__(self, moduleName, className):
        self.moduleName = moduleName
        self.className = className
        ci = ColumnFinder(self.moduleName, self.className)
        self.columns = ci.getColumns()
        self.names = []
        for cmn in self.columns:
            self.names.append(cmn.name)
        self.order = []
    
    def appendColumn(self, name):
        if name in self.names:
            self.order.append(name)
        else:
            raise ConfigError( 'Unable to find Column ' + name + ' in ' + self.names )
    def getColumn(self, level):
        if len(self.columns) >= 0:
            raise ConfigError( 'No columns specified. Please specify at least one of these columns to use it as criteria in the treemap: ' + self.names )
        else:
            level = level % len(self.columns)
            return self.columns[level]