'''
Created on May 18, 2010

@author: kblaszcz
'''
from sites.errors import ConfigError
from sites.tools.ModelsInspection import *
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
    
    def createOrUpdate(self, modulename, classname, methodname, parentmethodname, columnname, fparam = None):
        countmethodname = getCountMethodFor(classname, methodname)
        if self.ruleDataCorrect(modulename, classname, methodname, countmethodname, parentmethodname, columnname) and countmethodname is not None:
            index = modulename.__str__() + '.' + classname
            self.methods[index] = methodname
            self.countmethods[index] = countmethodname
            self.fparams[index] = fparam
            self.columnnames[index] = columnname
            self.parentmethods[index] = parentmethodname
        else: 
            raise Warning('Rule for ' + modulename + '.' + classname + ' could not be added. Methodname is ' + methodname)
        
    def getUsedClassNames(self):
        fullmodels = self.methods.keys()
        ret = {}
        for index in fullmodels:
            ret[index[(index.rfind(".")+1):]] = True
            
        return ret.keys()
    
    def infoDict(self):
        ret = []
        indexkeys = self.methods.keys()
        for index in indexkeys:
            ret.append({'model': index, 'method': self.methods[index], 'countmethod': self.countmethods[index], 'parentmethod': self.parentmethods[index], 'columnname': self.columnnames[index], 'fparam': self.fparams[index]})
        return ret
            
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
        childrenmethods = []
        for membername in instance.__class__.__dict__.keys():
            if ((type(instance.__class__.__dict__[membername]).__name__) == 'function') and membername == methodname:
                try:
                    if instance.__class__.__dict__[membername].__dict__['methodtype'] == 'children':
                        found = True
                        childrenmethods.append(membername)
                except KeyError:
                    continue
        
        if not found: return False
        
        #check count methodname
        found = False
        for membername in instance.__class__.__dict__.keys():
            if ((type(instance.__class__.__dict__[membername]).__name__) == 'function') and membername == countmethodname:
                try:
                    if instance.__class__.__dict__[membername].__dict__['methodtype'] == 'childrencount' and \
                    instance.__class__.__dict__[membername].__dict__['countsfor'] in childrenmethods:
                        found = True
                        break
                except KeyError:
                    continue
        
        if not found: return False
        
        #check parent methodname
        found = False
        for membername in instance.__class__.__dict__.keys():
            if ((type(instance.__class__.__dict__[membername]).__name__) == 'function') and membername == parentmethodname:
                if instance.__class__.__dict__[membername].__dict__['methodtype'] == 'parent':
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
        
    def addRules(self, modulename, classname, methodname, parentmethodname, columnname, level, fparam = None):
        try:
            self.rules[level]
        except IndexError:   
            if level <= len(self.rules):               
                r = ChildRules()
                r.createOrUpdate(modulename, classname, methodname, parentmethodname, columnname, fparam)
                self.rules.append(r) 
                return
            else:
                raise ConfigError('Before you create Rule for level ' + level + ' you need to have all rules up to level ' + (len(self.rules)-1) + ' defined.')
            
        self.rules[level].createOrUpdate(modulename, classname, methodname, parentmethodname, columnname, fparam)
            
    def appendRuleObject(self, obj):
        self.rules.append(obj)
        
    def getRuleObject(self, level):
        return self.rules[level]
        
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
        
    def getUniqueLevelRulesId(self):
        idparts = []
        oldcontent = None
        for nr, rule in enumerate(self.rules):
            idparts.append(nr.__str__())
            
            content = rule.infoDict()
            if content != oldcontent:
                for entry in content:
                    idparts.append(entry['model'])
                    idparts.append(entry['method'])
                    idparts.append(entry['countmethod'])
                    idparts.append(entry['parentmethod'])
                    idparts.append(entry['columnname'])
                    idparts.append(entry['fparam'].__str__())
            else:
                idparts.append('h8gates') #some fixed random number  
            oldcontent = content
        
        text = (''.join([bla for bla in idparts]))
        hashvalue = str(hash(text[:len(text)/2])) + str(hash(text[len(text)/2:])) 
        return hashvalue
     
    def describeRuleToUser(self, level):
        models = self.rules[level].getUsedClassNames()
        description = []

        for model in models:
            if model == 'Annex': continue
            description.append("All items on level ")
            description.append(level.__str__())
            description.append(" being the model ")
            description.append(model)
            
            description.append(" are evaluated by ")
            description.append(self.rules[level].getColumnNameFor(getModelsModuleName(model), model))
            description.append("<br>")
            
            description.append("Children of ")
            description.append(model) 
            description.append(" in level ")
            description.append(level.__str__())
            description.append(" will be requested using the method ")
            description.append(self.rules[level].getMethodNameFor(getModelsModuleName(model), model))
            description.append("<br>")
            description.append("<br>")

        return (''.join([bla for bla in description ]))
         
    
#    #returns data structured like this: [level][ruleindex]{rulesdict}    
#    def infoDict(self):
#        ret = []
#        for rule in self.rules:
#            ret.append(rule.infoDict())
#        return ret
    
