from django.db import connection, transaction, models

#this class summarizes a group of itmes. It has the same kind of methods as the classes that map real db data
class Annex(models.Model):
    
    evaluation = models.IntegerField(default = 1.0)
    id = models.DecimalField(unique=True, max_digits=0, decimal_places=-127, primary_key=True, default = -1)
    
    def __init__(self, children = [], parent = None):
        models.Model.__init__(self)
        assert(hasattr(children,'__iter__'))
        self.children = children
        self.parent = parent
    
    #DUAL is Oracle's fake Table
    class Meta:
        db_table = u'dual'
            
    def __unicode__(self):
        return unicode(self.__str__())
    
    def __str__(self):
        return "rest of the items"
    
    def getItems(self):
        return self.children
    
    def hasItems(self):
        if len(self.children) > 0:
            return True 
        else: 
            return False
            
    def countItems(self):
        return len(self.children)
    
    def getAnnexParent(self):
        return self.parent