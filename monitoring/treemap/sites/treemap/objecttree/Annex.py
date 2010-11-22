from django.db import connection, transaction, models
import inspect
from sites.tools.GroupIdService import newGroupId
from sites.tools.Inspections import *


#this class summarizes a group of itmes. It has the same kind of methods as the classes that map real db data
class Annex(models.Model):
    
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
        
        #how many exclusions were done before, used to have process independend annex id
        self.depth = depth
        
        #hack to fake id's to django
        classname = self.parent.__class__.__name__
        if parent == None:
            ppk = -1 #no parent
        elif not isinstance(parent, Annex):
            ppk = self.parent.getIdReplacement()
        elif isinstance(parent, Annex): #parent is Annex
            ppk = self.parent.getAnnexParent().getIdReplacement()
            classname = self.parent.getAnnexParent().__class__.__name__
        else:
            raise Exception("unexpected error")
            
            
        self.__dict__['id'] = newGroupId(self, ppk, classname, self.depth)
    
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
    
    def getExcludedNodes(self):
        return self.excludednodes
    
    def getDbParent(self):
        if self.parent == None:
            ppk = -1
        elif not isinstance(self.parent, Annex):
            ppk = self.parent.pk
        elif isinstance(self.parent, Annex): #parent is Annex
            ppk = self.parent.getDbParent().pk
        else:
            raise Exception("unexpected error")
        return ppk
    
    #defines how to find an object, no matter in what process or physical address
    def getIdReplacement(self):
        return self.pk.__str__()
    
    def getNaviName(self):
        return str(self)

Annex.nonmetrics = ['id']
Annex.metricattributes = []

#mark children Methods   
Annex.getItems.__dict__['methodtype'] = 'children'

#mark count Methods
Annex.countItems.__dict__['methodtype'] = 'childrencount'
Annex.countItems.__dict__['countsfor'] = 'getItems'

#mark parent Methods
Annex.getAnnexParent.__dict__['methodtype'] = 'parent'
Annex.getAnnexParent.__dict__['returntype'] = getAvailableModels()
        