'''
Created on Jun 10, 2010

Annex is a fake model, representing the sum of the Items which were too small to display.

@author: kblaszcz
'''


from django.db import connection, transaction, models
from app.tools.Inspections import *
import inspect
from app.dirs.ModelInterface import ModelInterface
import app.dirs.urls
import app.tools.Inspections

def buildAnnexId(rootmodel, depth, theid):
        if not(rootmodel in app.tools.Inspections.getAvailableModels()) and rootmodel !=  'Annex':
            raise Exception("model "+ rootmodel + " could not be found!")
        
        if depth < 0: depth = 0
        
        #findObjectByIdReplacementId
        try:
            if rootmodel != 'Annex':#to not to fail during id creation if annex constructor gets called without parameters
                app.dirs.models.__dict__[str(rootmodel)].findObjectByIdReplacementId(createObject(app.tools.Inspections.getModelsModuleName(rootmodel), rootmodel), theid, '')
        except:
            raise Exception("replacementid "+ theid + " could not be found!")
        
        correctedoptions = []
        correctedoptions.append("group_")
        correctedoptions.append(rootmodel)
        correctedoptions.append("_")
        correctedoptions.append(str(int(depth)))
        correctedoptions.append("_")
        correctedoptions.append(theid)
        
        return ''.join([bla for bla in correctedoptions]) 

#this class summarizes a group of itmes. It has the same kind of methods as the classes that map real db data
class Annex(models.Model, ModelInterface):
    
    #if you rename the id you have to rename it in the hack inside of constructor, too!
    id = models.CharField(unique=True, max_length=255, blank=True, primary_key=True)#models.DecimalField(unique=True, max_digits=0, decimal_places=-127, primary_key=True, default = -1)
    evaluation = models.IntegerField(default = 1.0)
    
    def __init__(self, rules = None, level = 0, parent = None, excludednodes = [], depth = 0):
        assert(hasattr(excludednodes,'__iter__'))
        assert(level >= 0)
        models.Model.__init__(self)
        self.excludednodes = excludednodes
        self.rules = rules
        self.level = level
        self.parent = parent
        self.depth = depth
        
        self.children_cache = []
        self.valid_cache = False
        
        self.corrected_evaluation = self.evaluation
        
        #how many exclusions were done before, used to have process independend annex id
        self.depth = depth
        
        def searchClassName(pr):
            try:
                cname = pr.getClassName()
            except:
                cname = pr.__class__.__name__
            return cname
            
        if parent == None:
            ppk = -1 #no parent
            classname = 'Annex'
        elif not isinstance(parent, Annex):
            ppk = self.parent.getIdReplacement()
            classname = searchClassName(self.parent)
        elif isinstance(parent, Annex): #parent is Annex
            ppk = self.parent.getAnnexParent().getIdReplacement()
            classname = searchClassName(self.parent.getAnnexParent())
        else:
            raise Exception("unexpected error")
            
        self.__dict__['id'] = ppk.__str__()
#        self.__dict__['id'] = "group_" + classname + "_" + self.depth.__str__() + "_"  + ppk.__str__()
    
    #DUAL is Oracle's fake Table
    class Meta:
        db_table = u'dual'
            
    def __unicode__(self):
        return unicode(self.__str__())
    
    def __str__(self):
        return "The rest [zoom = "+str(self.depth)+"]"
    
    def getItems(self):
        if not(self.valid_cache):
            chclassname = self.parent.__class__.__name__
            methodname = self.rules.getMethodNameFor(self.level + 1, chclassname)
            self.children_cache = list(self.parent.__class__.__dict__[methodname](self.parent))
            
            newcache = []
            #filter children to display the remaining ones
            for child in self.children_cache:
                remove = False
                for toexclude in self.excludednodes:
                    if child.pk == toexclude.getObject().pk:
                        remove = True
                        
                if(not remove): newcache.append(child)
                
            self.children_cache = newcache
            self.valid_cache = True

        return self.children_cache
    
    def hasItems(self):
        if len(self.children_cache) > 0:
            return True 
        else: 
            return False
            
    def countItems(self):
        if(self.valid_cache):
            return len(self.children_cache)
        else:
            chclassname = self.parent.__class__.__name__
            methodname = self.rules.getCountMethodNameFor(self.level + 1, chclassname)
            allcount = self.parent.__class__.__dict__[methodname](self.parent)
            return allcount - len(self.excludednodes)
    
    def getAnnexParent(self):
        return self.parent
    
    def getDepth(self):
        return self.depth
    
    def getCopyWithIncDepth(self, newexcluded, evaluation):
        ret = Annex(rules = self.rules, level = self.level, parent = self.parent, excludednodes = self.excludednodes[:], depth = (self.depth + 1))
        ret.__setattr__('children_cache', self.children_cache[:])
        ret.__setattr__('valid_cache', True)
        ret.addExcludedNodes(newexcluded)
        ret.evaluation = evaluation
        return ret
    
    def addExcludedNodes(self, newexcluded):
        if len(newexcluded) > 0:
            for excl in newexcluded:
                self.excludednodes.append(excl)
      
            if self.valid_cache:
                newcache = []
                #filter children to display the remaining ones
                for child in self.children_cache:
                    remove = False
                    for toexclude in newexcluded:
                        if child.pk == toexclude.getObject().pk:
                            remove = True
                            
                    if(not remove): newcache.append(child)
                
                del self.children_cache
                self.children_cache = newcache
                self.valid_cache = True
                
    def activateNode(self, activ):
        if (activ in self.excludednodes):
            self.excludednodes.remove(activ)
            self.children_cache.append(activ)
            self.corrected_evaluation = self.corrected_evaluation  + activ.getEvalValue()
            return
        #if object isn't anywhere, not in excluded and not in children_cache
        if not (activ in self.children_cache):
            self.children_cache.append(activ)
            self.corrected_evaluation = self.corrected_evaluation  + activ.getEvalValue()
            return
            
    def getCorrectedEvalValue(self):
        return self.corrected_evaluation

    
    def getExcludedNodes(self):
        return self.excludednodes
    
    def getDbParent(self):
        if self.parent == None:
            ppk = None
        elif not isinstance(self.parent, Annex):
            ppk = self.parent
        elif isinstance(self.parent, Annex): #parent is Annex
            ppk = self.parent.getDbParent()
        else:
            raise Exception("unexpected error")
        return ppk
    
    #defines how to find an object, no matter in what process or physical address
    def getIdReplacement(self):
        return self.pk.__str__()
    
    def getNaviName(self):
        return str(self)
    
    def childrenMethodsPairs(self):
        return [
                {'childrenmethod': 'getItems', 'childrencounter': 'countItems'},
               ]
    
    def parentMethods(self):
        return['getAnnexParent']
        
    def metricAttributes(self):
        return []
    
    def getClassName(self):
        return self.__class__.__name__
    
    def getVeryParentClassName(self):
        if self.parent.getClassName() == 'Annex':
            cname = self.parent.getVeryParentClassName()
        else:
            cname = self.parent.__class__.__name__
        return cname
    
    def setEvaluations(self, value):
        self.evaluation = value;
        self.corrected_evaluation = self.evaluation

    def setCorrectedEvaluation(self, value):
        self.corrected_evaluation = value