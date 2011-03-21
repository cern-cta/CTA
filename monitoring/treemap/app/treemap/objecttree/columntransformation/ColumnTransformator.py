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
from app.tools.ObjectCreator import createObject
from app.treemap.objecttree.columntransformation.TransFunctions import *
import app.tools

#USAGE EXAMPLE:
#from app.dirs.models import Dirs
#from app.treemap.node.object_extender.object_extender import *
#t = ColumnTransformator('Dirs')
#p = Dirs.objects.get(pk=4)
#totalsize_value = t.transform_totalsize(p.totalsize)

class ColumnTransformator(dict):
    
    """The class will extend itself to provide functions for each attribute of common type from the given model"""
    """The purpose of that functions is to define how to turn an attribute to a number that can be used for a treemap"""
    """Just overwrite the function if the standard definition is not what you want"""
    """Why:"""   
    """DB Attributes are Objects that have to be converted into a number before they can be used (ie. DecimalField)"""   
    
    fnprefix = 'transform_'
    
    def __init__(self, className):
        self.moduleName = app.tools.Inspections.getModelsModuleName(className)
        self.className = className
        self.af = app.tools.Inspections.ModelAttributeFinder(self.className)
        self.columns = self.af.getColumns()
        self.metricattributenames = self.af.getMetricAttributeNames()
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
        return self.af.getColumnAndAtrributeNames()
    
    def getModelAttributeFinder(self):
        return self.af
    
    def getFnprefix(self):
        return self.fnprefix