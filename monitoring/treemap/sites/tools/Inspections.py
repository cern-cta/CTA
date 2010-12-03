'''
Created on May 4, 2010
set of useful functions enabling to easily scan for model attributes, available models etc
@author: kblaszcz
'''
from django.db.models.base import ModelBase
from django.db.models.fields import *
from sites.tools.ObjectCreator import createObject
from sites.treemap.objecttree.columntransformation.TransFunctions import evalStringBySimilarity
import inspect
import sys
from sites.dirs.models import *

class ModelAttributeFinder:

    def __init__(self, className):
        """Determines all Django Columns of given model class"""
        self.moduleName = getModelsModuleName(className)
        self.className = className
        self.columns = []
        self.column_names = []
        self.metricattributenames = []
    
        instance = createObject(self.moduleName, self.className)
        try:
            for key in instance._meta.fields:
                value = key
                self.columns.append(key)
                self.column_names.append(key.name)
        except:
            raise Exception( 'Unable to read Columns' )
    
        self.metricattributenames = instance.__class__.metricattributes
        
        self.metricattributenames.sort()
        self.columns.sort()
        
    def getMetricAttributeNames(self):
        return self.metricattributenames
    
    def getColumnAndAtrributeNames(self):
        return self.column_names + self.metricattributenames
        
    def getColumns(self):
        return self.columns
    
    def getAttrNames(self):
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
    
    

    
def getAvailableModels():
    modulename = None
    availablemodels = []
    if settings.MODELS_LOCATION in settings.INSTALLED_APPS:
        modulename = settings.MODELS_LOCATION + '.models'
        if not modulename in sys.modules.keys():
            try:
                module = __import__( modulename )
            except ImportError, e:
                raise Exception( 'Unable to load module: ' + module )
        else:
            module = sys.modules[modulename]
            
        classes = dict(inspect.getmembers( module, inspect.isclass ))
        
        for classname in classes:
            cls = classes[classname]
            if isinstance(cls, ModelBase):
                modelname = cls._base_manager.model._meta.object_name
                availablemodels.append(modelname)
                
    return availablemodels

def getCountMethodFor(themodel, childrenmethodname):
    modulename = getModelsModuleName(themodel)
    instance = createObject(modulename, themodel)  
    methodname = None
    methodpairs = instance.childrenMethodsPairs()
    for pair in methodpairs:
        try:
            if(pair['childrenmethod'] == childrenmethodname):
                methodname = pair['childrencounter']
                break
        except:
            raise Exception("unexpected childrenmethod pair structure")
        
    return methodname

def getModelsModuleName(themodel):
    modulename = ""
    if(themodel == 'Annex'): return 'sites.treemap.objecttree.Annex'
    
    if settings.MODELS_LOCATION in settings.INSTALLED_APPS:
        modulename = settings.MODELS_LOCATION + '.models'
    else:
        raise Exception("Configuration Error in settings.py: settings.MODELS_LOCATION must contain a value which is in settings.INSTALLED_APPS. The models file must be called models.py")
    
    return modulename

def getPostProcessorNames(): 
    ret = []
    
    modulename = settings.POSTPROCESSORS_LOCATION
    if not modulename in sys.modules.keys():
        try:
            module = __import__( modulename )
        except ImportError, e:
            return False
    else:
        module = sys.modules[modulename]

    classnames = dict(inspect.getmembers( module, inspect.isclass ))
    for classname in classnames.keys():
        if classname != 'PostProcessorBase':
            ret.append(classname)
    
    return ret

def getDefaultNumberOfLevels():
    return 8

def getModelsNotToCache():
    return [];

def getFunctionNames(instance):
    functions = []
    for membername in instance.__class__.__dict__.keys():
        if ((type(instance.__class__.__dict__[membername]).__name__) == 'function'):
                functions.append(membername)
    return functions
    
            
