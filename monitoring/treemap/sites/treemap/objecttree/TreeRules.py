'''
Created on May 18, 2010

@author: kblaszcz
'''
from sites.errors import ConfigError
from sites.tools.ModelsInspection import *
from sites.tools.ObjectCreator import createObject
from sites.treemap.objecttree.Postprocessors import *
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
        self.ppnames = {}
    
    def createOrUpdate(self, classname, methodname, parentmethodname, columnname, fparam = None, postprocessornm = 'DafaultPostProcessor'):
        countmethodname = getCountMethodFor(classname, methodname)

        modulename = getModelsModuleName(classname)
        if self.ruleDataCorrect(modulename, classname, methodname, countmethodname, parentmethodname, columnname, postprocessornm) and countmethodname is not None:
            index = classname
            self.methods[index] = methodname
            self.countmethods[index] = countmethodname
            self.fparams[index] = fparam
            self.columnnames[index] = columnname
            self.parentmethods[index] = parentmethodname
            self.ppnames[index] = postprocessornm
        else: 
            raise Warning('Rule for ' + classname + ' could not be added. Methodname is ' + methodname)
        
    def getUsedClassNames(self):  
        return self.methods.keys()
    
    def getModelToMethod(self):  
        ret = []
        
        for classname, methodname in self.methods.items():
            ret.append((classname, methodname))
        return ret
    
    def getModelToColumn(self):  
        ret = []
        
        for classname, columnname in self.columnnames.items():
            ret.append((classname, columnname))
        return ret
    
    def getModelToPostProcessor(self):
        ret = []
        
        for classname, postprocessorname in self.ppnames.items():
            ret.append((classname, postprocessorname))
        return ret
    
    def infoDict(self):
        ret = []
        indexkeys = self.methods.keys()
        for index in indexkeys:
            ret.append({'model': index, 'method': self.methods[index], 'countmethod': self.countmethods[index], 'parentmethod': self.parentmethods[index], 'columnname': self.columnnames[index], 'fparam': self.fparams[index], 'postprocessorname': self.ppnames[index]})
        return ret
            
    def getMethodNameFor(self, classname):
        return self.methods[classname]
    
    def getParentMethodNameFor(self, classname):
        return self.parentmethods[classname]
    
    def getParamFor(self, classname):
        return self.fparams[classname]
    
    def getColumnNameFor(self, classname):
        return self.columnnames[classname]

    def getCountMethodNameFor(self, classname):
        return self.countmethods[classname]   
    
    def getPostProcessorNameFor(self, classname):
        return self.ppnames[classname] 
    
    def ruleDataCorrect(self, modulename, classname, methodname, countmethodname, parentmethodname, columnname, postprocessornm):
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
        cf = ColumnFinder(classname)
        if columnname not in cf.getColumnnames():
            return False
        
        #check postprocessornm
        if postprocessornm is not None:
            if postprocessornm not in getPostProcessorNames(): return False
        
        return True
        
    def attributesAreValid(self):
        
        childrenmethods = {}
        for classname, methodname in self.methods.items():
            #check modulename and  classname
            modulename = getModelsModuleName(classname)
            try:
                instance = createObject(modulename, classname)
            except ConfigError:
                return False 
            
            #check methodname
            childrenmethods[classname] = []
            found = False
            for membername in instance.__class__.__dict__.keys():
                if ((type(instance.__class__.__dict__[membername]).__name__) == 'function') and membername == methodname:
                    try:
                        if instance.__class__.__dict__[membername].__dict__['methodtype'] == 'children':
                            found = True
                            childrenmethods[classname].append(membername)
                    except KeyError:
                        continue
            if not found: 
                print "not found"
                return False

            
        for classname, countmethodname in self.countmethods.items():
            #check modulename and  classname
            modulename = getModelsModuleName(classname)
            try:
                instance = createObject(modulename, classname)
            except ConfigError:
                return False 
            
            #check count methodname
            found = False
            for membername in instance.__class__.__dict__.keys():
                if ((type(instance.__class__.__dict__[membername]).__name__) == 'function') and membername == countmethodname:
                    try:
                        if instance.__class__.__dict__[membername].__dict__['methodtype'] == 'childrencount':
                            if instance.__class__.__dict__[membername].__dict__['countsfor'] in childrenmethods[classname]:
                                found = True
                                break
                    except KeyError:
                        continue
            
            if not found: 
                print "not found"
                return False
        
        for classname, parentmethodname in self.parentmethods.items():
            #check modulename and  classname
            modulename = getModelsModuleName(classname)
            try:
                instance = createObject(modulename, classname)
            except ConfigError:
                return False 
            
            #check parent methodname
            found = False
            for membername in instance.__class__.__dict__.keys():
                if ((type(instance.__class__.__dict__[membername]).__name__) == 'function') and membername == parentmethodname:
                    if instance.__class__.__dict__[membername].__dict__['methodtype'] == 'parent':
                        found = True
                        break
            
            if not found: 
                print "not found"
                return False
        
        for classname, columnname in self.columnnames.items():
            #check modulename and  classname
            modulename = getModelsModuleName(classname)
            try:
                instance = createObject(modulename, classname)
            except ConfigError:
                return False 
        
            #check columnname
            cf = ColumnFinder(classname)
            if columnname not in cf.getColumnnames():
                return False
            
        #check postprocessornm
        for classname, ppn in self.ppnames.items():
            if ppn is not None:
                modulename = 'sites.treemap.objecttree.Postprocessors'
                if not modulename in sys.modules.keys():
                    try:
                        module = __import__( modulename )
                    except ImportError, e:
                        return False
                else:
                    module = sys.modules[modulename]
            
                classes = dict(inspect.getmembers( module, inspect.isclass ))
                if not ppn in classes:
                    return False
            
        return True
    
class LevelRules(object):
    
    def __init__(self):
        self.rules = []
        
    def addRules(self, classname, methodname, parentmethodname, columnname, level, fparam = None, postprocessorname = None):
        try:
            self.rules[level]
        except IndexError:   
            if level <= len(self.rules):               
                r = ChildRules()
                r.createOrUpdate(classname, methodname, parentmethodname, columnname, fparam, postprocessorname)
                self.rules.append(r) 
                return
            else:
                raise ConfigError('Before you create Rule for level ' + level + ' you need to have all rules up to level ' + (len(self.rules)-1) + ' defined.')
            
        self.rules[level].createOrUpdate(classname, methodname, parentmethodname, columnname, fparam)
            
    def appendRuleObject(self, obj):
        self.rules.append(obj)
        
    def getRuleObject(self, level):
        return self.rules[level]
    
    def getRules(self):
        return self.rules
        
    def getMethodNameFor(self, level, classname):
        try:
            return self.rules[level].getMethodNameFor(classname)
        except IndexError:
            return None
        
    def getCountMethodNameFor(self, level, classname):
        try:
            return self.rules[level].getCountMethodNameFor(classname)
        except IndexError:
            return None
        
    def getParentMethodNameFor(self, level, classname):
        try:
            return self.rules[level].getParentMethodNameFor(classname)
        except IndexError:
            return None
    
    def getParamFor(self, level, classname):
        try:
            return self.rules[level].getParamFor(classname)
        except IndexError:
            return None
        
    def getColumnNameFor(self, level, classname):
        try:
            return self.rules[level].getColumnNameFor(classname)
        except IndexError:
            return None
        
    def getPostProcessorNameFor(self, level,  classname):
        try:
            return self.rules[level].getPostProcessorNameFor(classname)
        except IndexError:
            return None

    def indexIsValid(self,index):
        try:
            self.rules[index]
            return True
        except IndexError:
            return False
        
    def countDefinedLevels(self):
        return len(self.rules)
        
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
                    idparts.append(entry['postprocessorname'].__str__())
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
            description.append(self.rules[level].getColumnNameFor(model))
            description.append("<br>")
            
            description.append("Children of ")
            description.append(model) 
            description.append(" in level ")
            description.append(level.__str__())
            description.append(" will be requested using the method ")
            description.append(self.rules[level].getMethodNameFor(model))
            description.append("<br>")
            description.append("<br>")

        return (''.join([bla for bla in description ]))
    
    def rulesAreValid(self):
        for rule in self.rules:
            if not rule.attributesAreValid():
                return False
        return True
         
    
#    #returns data structured like this: [level][ruleindex]{rulesdict}    
#    def infoDict(self):
#        ret = []
#        for rule in self.rules:
#            ret.append(rule.infoDict())
#        return ret
    
