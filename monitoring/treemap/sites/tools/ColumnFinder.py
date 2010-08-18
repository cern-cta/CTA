'''
Created on May 4, 2010

@author: kblaszcz
'''
from sites.errors import ConfigError
from sites.tools.ObjectCreator import createObject
from django.db.models.fields import *
from sites.treemap.objecttree.columntransformation.TransFunctions import evalStringBySimilarity

class ColumnFinder:

    def __init__(self, moduleName, className):
        """Determines all Django Columns of given model class"""
        self.moduleName = moduleName
        self.className = className
        self.columns = []
        self.column_names = []
    
        instance = createObject(self.moduleName, self.className)
        try:
            for key in instance._meta.fields:
                value = key
                self.columns.append(key)
                self.column_names.append(key.name)
        except:
            raise ConfigError( 'Unable to read Columns' )
    
        self.columns.sort()
        
    def getColumns(self):
        return self.columns
    
    def getColumnnames(self):
        return self.column_names
    
    def getPk(self):
        for column in self.columns:
            if column.primary_key:
                return column
        return None
    
    def guessPk(self):
        pk=self.getPk()
        if pk is None:
            max = 0  
            for column in self.columns:
                if column.rel is None:
                    evl = evalStringBySimilarity(column.name, 'id')
                    if (evl>max):
                        max = evl
                        pk = column
        return pk

                
        