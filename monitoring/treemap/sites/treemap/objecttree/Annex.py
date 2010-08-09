from django.db import connection, transaction, models
import inspect

#this class summarizes a group of itmes. It has the same kind of methods as the classes that map real db data
class Annex(models.Model):
    
    evaluation = models.IntegerField(default = 1.0)
    id = models.DecimalField(unique=True, max_digits=0, decimal_places=-127, primary_key=True, default = -1)
    
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
    
    #DUAL is Oracle's fake Table
    class Meta:
        db_table = u'dual'
            
    def __unicode__(self):
        return unicode(self.__str__())
    
    def __str__(self):
        return "rest of the items"
    
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
        return len(self.children_cache)
    
    def getAnnexParent(self):
        return self.parent