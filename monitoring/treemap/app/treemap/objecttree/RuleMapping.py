'''
Created on May 18, 2010

RuleMapping define per level and model what count/children/parent methods to use and attributes to evaluate.
fparams define additional parameters that will be passed to a TransFunction.
A TransFunction translates object attributes into numbers.

@author: kblaszcz
'''
import app.tools.Inspections
from app.tools.ObjectCreator import createObject
from app.treemap.objecttree.Postprocessors import *
import exceptions
import inspect
import sys
import warnings
import app.tools

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
        self.attrnames = {}
        self.ppnames = {}
    
    def createOrUpdate(self, classname, methodname, parentmethodname, attrname, fparam = None, postprocessornm = 'DafaultPostProcessor'):
        countmethodname = app.tools.Inspections.getCountMethodFor(classname, methodname)

        modulename = app.tools.Inspections.getModelsModuleName(classname)
        if self.ruleDataCorrect(modulename, classname, methodname, countmethodname, parentmethodname, attrname, postprocessornm) and countmethodname is not None:
            index = classname
            self.methods[index] = methodname
            self.countmethods[index] = countmethodname
            self.fparams[index] = fparam
            self.attrnames[index] = attrname
            self.parentmethods[index] = parentmethodname
            self.ppnames[index] = postprocessornm
        else: 
            raise Warning('Rule for ' + classname + ' could not be added. Methodname is ' + methodname)
        
    def setPostProcessorName(self, classname, ppname):
        if ppname in app.tools.Inspections.getPostProcessorNames() and classname in self.getUsedClassNames():
            self.ppnames[classname] = ppname
        
        
    def getUsedClassNames(self):  
        return self.methods.keys()
    
    def getModelToMethod(self):  
        ret = []
        
        for classname, methodname in self.methods.items():
            ret.append((classname, methodname))
        return ret
    
    def getModelToColumn(self):  
        ret = []
        
        for classname, attrname in self.attrnames.items():
            ret.append((classname, attrname))
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
            ret.append({'model': index, 'method': self.methods[index], 'countmethod': self.countmethods[index], 'parentmethod': self.parentmethods[index], 'attrname': self.attrnames[index], 'fparam': self.fparams[index], 'postprocessorname': self.ppnames[index]})
        return ret
            
    def getMethodNameFor(self, classname):
        return self.methods[classname]
    
    def getParentMethodNameFor(self, classname):
        return self.parentmethods[classname]
    
    def getParamFor(self, classname):
        return self.fparams[classname]
    
    def getAttrNameFor(self, classname):
        return self.attrnames[classname]

    def getCountMethodNameFor(self, classname):
        return self.countmethods[classname]   
    
    def getPostProcessorNameFor(self, classname):
        return self.ppnames[classname] 
    
    def ruleDataCorrect(self, modulename, classname, methodname, countmethodname, parentmethodname, attrname, postprocessornm):
        #check modulename and  classname
        try:
            instance = createObject(modulename, classname)
        except Exception:
            return False 
        
        #check methodname and countmethodname
        found = False
        for dic in instance.childrenMethodsPairs(): 
            if dic['childrenmethod'] == methodname and dic['childrencounter'] == countmethodname:
                found = True
                break
        if not found: return False

        #check parent methodname
        found = parentmethodname in instance.parentMethods()
        if not found: return False
        
        #check attrname
        cf = app.tools.Inspections.ModelAttributeFinder(classname)
        if attrname not in cf.getColumnAndAtrributeNames():
            return False
        
        #check postprocessornm
        if postprocessornm is not None:
            if postprocessornm not in app.tools.Inspections.getPostProcessorNames(): return False
        
        return True
        
    def isValid(self):
        
        for classname, methodname in self.methods.items():
            #check modulename and  classname
            modulename = app.tools.Inspections.getModelsModuleName(classname)
            try:
                instance = createObject(modulename, classname)
            except Exception:
                return False 
            
            #check methodname
            found = False
            for dic in instance.childrenMethodsPairs(): 
                if dic['childrenmethod'] == methodname:
                    found = True

            if not found: 
                print "childmethod not found"
                return False

            
        for classname, countmethodname in self.countmethods.items():
            #check modulename and  classname
            modulename = app.tools.Inspections.getModelsModuleName(classname)
            try:
                instance = createObject(modulename, classname)
            except Exception:
                return False 
            
            #check count methodname
            found = False      
            for dic in instance.childrenMethodsPairs():
                if dic['childrencounter'] == countmethodname and dic['childrenmethod'] in self.methods:
                    found = True 
                    break
            
            if not found: 
                print "countmethod not found"
                return False
        
        for classname, parentmethodname in self.parentmethods.items():
            #check modulename and  classname
            modulename = app.tools.Inspections.getModelsModuleName(classname)
            try:
                instance = createObject(modulename, classname)
            except Exception:
                return False 
            
            #check parent parentmethod
            found = parentmethodname in instance.parentMethods() 
            
            if not found: 
                print "not found"
                return False
        
        for classname, attrname in self.attrnames.items():
            #check modulename and  classname
            modulename = app.tools.Inspections.getModelsModuleName(classname)
            try:
                instance = createObject(modulename, classname)
            except Exception:
                return False 
        
            #check attrname
            cf = app.tools.Inspections.ModelAttributeFinder(classname)
            if attrname not in cf.getColumnAndAtrributeNames():
                return False
            
        #check postprocessornm
        for classname, ppn in self.ppnames.items():
            if ppn is not None:
                modulename = 'app.treemap.objecttree.Postprocessors'
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
        
    def addRules(self, classname, methodname, parentmethodname, attrname, level, fparam = None, postprocessorname = None):
        try:
            self.rules[level]
        except IndexError:   
            if level <= len(self.rules):               
                r = ChildRules()
                r.createOrUpdate(classname, methodname, parentmethodname, attrname, fparam, postprocessorname)
                self.rules.append(r) 
                return
            else:
                raise Exception('Before you create Rule for level ' + level + ' you need to have all rules up to level ' + (len(self.rules)-1) + ' defined.')
            
        self.rules[level].createOrUpdate(classname, methodname, parentmethodname, attrname, fparam)
            
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
        
    def getAttrNameFor(self, level, classname):
        try:
            return self.rules[level].getAttrNameFor(classname)
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
                    idparts.append(entry['attrname'])
                    idparts.append(entry['fparam'].__str__())
                    idparts.append(entry['postprocessorname'].__str__())
            else:
                idparts.append('h8gates') #some fixed random number  
            oldcontent = content
        
        text = (''.join([bla for bla in idparts]))
        hashvalue = str(hash(text[:len(text)/2])) + str(hash(text[len(text)/2:])) 
        return hashvalue
    
    def rulesAreValid(self):
        for rule in self.rules:
            if not rule.isValid():
                return False
        return True
    
    def nbLevels(self):
        return len(self.rules)
         
    
#    #returns data structured like this: [level][ruleindex]{rulesdict}    
#    def infoDict(self):
#        ret = []
#        for rule in self.rules:
#            ret.append(rule.infoDict())
#        return ret
    
