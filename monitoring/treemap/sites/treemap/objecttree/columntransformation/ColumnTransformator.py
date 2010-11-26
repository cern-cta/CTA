'''
Created on Apr 27, 2010

@author: kblaszcz
'''
import inspect
import sys
import new
import warnings
from django.db.models import *
from django.db.models.fields import *
import datetime
#import math
from sites.tools.ObjectCreator import createObject
from sites.treemap.objecttree.columntransformation.TransFunctions import *
from sites.tools.Inspections import *

#USAGE EXAMPLE:
#from sites.dirs.models import Dirs
#from sites.treemap.node.object_extender.object_extender import *
#t = ColumnTransformator('Dirs')
#p = Dirs.objects.get(pk=4)
#totalsize_value = t.transform_totalsize(p.totalsize)

class ColumnTransformator(dict):
    
    """The class will extend itself to provide functions for each column of common type from the given model"""
    """The purpose of that functions is to define how to turn a column to a number that can be used for a tree map"""
    """Just overwrite the function if the standard definition is not what you want"""
    """Why:"""   
    """It is convenient for DateFields and separates the interpretation of the column from the rest of the code."""   
    """You might also have the idea to do some fancy stuff like defining a function that increases very small values to display more files in you treemap""" 
    
    fnprefix = 'transform_'
    
    def __init__(self, className):
        self.moduleName = getModelsModuleName(className)
        self.className = className
        self.ci = ModelAttributeFinder(self.className)
        self.columns = self.ci.getColumns()
        self.metricattributenames = self.ci.getMetricAttributeNames()
        self.generateDefaultFunctions()
        
        if not self.moduleName in sys.modules.keys():
            try:
                module = __import__( self.moduleName )
            except ImportError, e:
                raise Exception( 'Unable to load module: ' + self.moduleName )
        else:
            self.module = sys.modules[self.moduleName]

    def generateDefaultFunctions(self):
        """extend yourself with new functions"""
        for column in self.columns:
            name = self.__class__.fnprefix + column.name
            #only basic Field types here. To find all field classes see django.db.models.fields.__init__
            if isinstance(column, DecimalField):
                self.__dict__[name] = evalDecimalField
            if isinstance(column, IntegerField):
                self.__dict__[name] = evalIntegerField
            if isinstance(column, CharField):
                self.__dict__[name] = evalStringBySimilarity
            if isinstance(column, DateField):
                self.__dict__[name] = evalStringBySimilarity
            if isinstance(column, DateTimeField):
                self.__dict__[name] = evalDateField
            if isinstance(column, FloatField):
                self.__dict__[name] = evalFloatField
                
        for attributename in self.metricattributenames:
            name = self.__class__.fnprefix + attributename
            self.__dict__[name] = evalAttribute

    def getClassname(self):
        return self.className
    
    def getModulename(self):
        return self.moduleName
    
    def getColumnAndAtrributeNames(self):
        return self.ci.getColumnAndAtrributeNames()
    
    def getModelAttributeFinder(self):
        return self.ci
    
    def getFnprefix(self):
        return self.fnprefix