from django.db import connection, transaction, models
import inspect

class TheFakeIdService(object):
    id = 0

def newfakeid(self):  
    TheFakeIdService.id = TheFakeIdService.id - 1
    return TheFakeIdService.id.__str__()

#this class summarizes a group of itmes. It has the same kind of methods as the classes that map real db data
class Annex(models.Model):
    
    #if you rename the id you have to rename it in the hack inside of constructor, too!
    id = models.CharField(unique=True, max_length=255, blank=True, primary_key=True)#models.DecimalField(unique=True, max_digits=0, decimal_places=-127, primary_key=True, default = -1)
    evaluation = models.IntegerField(default = 1.0)
    
    def __init__(self, rules = None, level = 0, parent = None, excludedchildren = []):
        assert(hasattr(excludedchildren,'__iter__'))
        assert(level >= 0)
        models.Model.__init__(self)
        self.excludedchildren = excludedchildren
        self.rules = rules
        self.level = level
        self.parent = parent
        
        self.children_cache = []
        self.valid_cache = False
        
        #hack to fake id's to django
        self.__dict__['id'] = newfakeid(self)
    
    #DUAL is Oracle's fake Table
    class Meta:
        db_table = u'dual'
            
    def __unicode__(self):
        return unicode(self.__str__())
    
    def __str__(self):
        return "The rest"
    
    def getItems(self):
        if not(self.valid_cache):
            chmodulename = inspect.getmodule(self.parent).__name__
            chclassname = self.parent.__class__.__name__
            methodname = self.rules.getMethodNameFor(self.level + 1, chmodulename, chclassname)
            self.children_cache = list(self.parent.__class__.__dict__[methodname](self.parent))
            
            #filter children to display the remaining ones
            for child in self.children_cache:
                for toexclude in self.excludedchildren:
                    if child == toexclude:
                        self.children_cache.remove(child)
            
            self.valid_cache = True
        else:
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
            chmodulename = inspect.getmodule(self.parent).__name__
            chclassname = self.parent.__class__.__name__
            methodname = self.rules.getCountMethodNameFor(self.level + 1, chmodulename, chclassname)
            allcount = self.parent.__class__.__dict__[methodname](self.parent)
            return allcount - len(self.excludedchildren)
    
    def getAnnexParent(self):
        return self.parent