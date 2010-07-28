'''
Created on May 18, 2010

@author: kblaszcz
'''
from sites.errors import ConfigError
from sites.tools.ColumnFinder import ColumnFinder
from sites.tools.ObjectCreator import createObject
import exceptions
import inspect
import sys
import warnings

class ChildRules(object):
    '''
    classdocs
    '''

    def __init__(self):
        '''
        Constructor
        '''
        self.methods = {}
        self.countmethods = {}
        self.parentmethods = {}
        self.fparams = {}
        self.columnnames = {}
    
    def addRule(self, modulename, classname, methodname, countmethodname, parentmethodname, columnname, fparam = None):
        if self.ruleDataCorrect(modulename, classname, methodname, countmethodname, parentmethodname, columnname):
            index = modulename.__str__() + '.' + classname
            self.methods[index] = methodname
            self.countmethods[index] = countmethodname
            self.fparams[index] = fparam
            self.columnnames[index] = columnname
            self.parentmethods[index] = parentmethodname
        else: 
            raise Warning('Rule for ' + modulename + '.' + classname + ' could not be added. Methodname is ' + methodname)
            
    def getMethodNameFor(self, modulename, classname):
        return self.methods[modulename.__str__() + '.' + classname]
    
    def getParentMethodNameFor(self, modulename, classname):
        return self.parentmethods[modulename.__str__() + '.' + classname]
    
    def getParamFor(self, modulename, classname):
        return self.fparams[modulename.__str__() + '.' + classname]
    
    def getColumnNameFor(self, modulename, classname):
        return self.columnnames[modulename.__str__() + '.' + classname]

    def getCountMethodNameFor(self, modulename, classname):
        return self.countmethods[modulename.__str__() + '.' + classname]

    
    def ruleDataCorrect(self, modulename, classname, methodname, countmethodname, parentmethodname, columnname):
        #check modulename and  classname
        try:
            instance = createObject(modulename, classname)
        except ConfigError:
            return False 
        
        #check methodname
        found = False
        for membername in instance.__class__.__dict__.keys():
            if ((type(instance.__class__.__dict__[membername]).__name__) == 'function') and membername == methodname:
                found = True
                break
        
        if not found: return False
        
        #check count methodname
        found = False
        for membername in instance.__class__.__dict__.keys():
            if ((type(instance.__class__.__dict__[membername]).__name__) == 'function') and membername == countmethodname:
                found = True
                break
        
        if not found: return False
        
        #check parent methodname
        found = False
        for membername in instance.__class__.__dict__.keys():
            if ((type(instance.__class__.__dict__[membername]).__name__) == 'function') and membername == parentmethodname:
                found = True
                break
        
        if not found: return False
        
        #check columnname
        cf = ColumnFinder(modulename, classname)
        if columnname in cf.getColumnnames():
            return True
        else:
            return False
        
    
class LevelRules(object):
    
    def __init__(self):
        self.rules = []
        
    def addRules(self, modulename, classname, methodname, countmethodname, parentmethodname, columnname, level, fparam = None):
        try:
            self.rules[level]
        except IndexError:   
            if level <= len(self.rules):               
                r = ChildRules()
                r.addRule(modulename, classname, methodname, countmethodname, parentmethodname, columnname, fparam)
                self.rules.append(r) 
                return
            else:
                raise ConfigError('Before you create Rule for level ' + level + ' you need to have all rules up to level ' + (len(self.rules)-1) + ' defined.')
            
        self.rules[level].addRule(modulename, classname, methodname, countmethodname, parentmethodname, columnname, fparam)
            

        
    def getMethodNameFor(self, level, modulename, classname):
        try:
            return self.rules[level].getMethodNameFor(modulename, classname)
        except IndexError:
            return None
        
    def getCountMethodNameFor(self, level, modulename, classname):
        try:
            return self.rules[level].getCountMethodNameFor(modulename, classname)
        except IndexError:
            return None
        
    def getParentMethodNameFor(self, level, modulename, classname):
        try:
            return self.rules[level].getParentMethodNameFor(modulename, classname)
        except IndexError:
            return None
    
    def getParamFor(self, level, modulename, classname):
        try:
            return self.rules[level].getParamFor(modulename, classname)
        except IndexError:
            return None
        
    def getColumnNameFor(self, level, modulename, classname):
        try:
            return self.rules[level].getColumnNameFor(modulename, classname)
        except IndexError:
            return None

    def indexIsValid(self,index):
        try:
            self.rules[index]
            return True
        except IndexError:
            return False
    
