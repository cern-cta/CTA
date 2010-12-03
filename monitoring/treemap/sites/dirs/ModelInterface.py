class ModelInterface(object):
    
    def __init__(self, *args, **kwargs):
        pass
        
    def __unicode__(self):
        raise Exception("__unicode__ not implemented!")
    
    def __str__(self):
        raise Exception("__str__ not implemented!")
    
    #This is needed because the networkx libraray used by BasicTree uses the hash function to tell if objects are different
    def __hash__(self):
        raise Exception("__hash__ not implemented!")
        
    def getUserFriendlyName(self):
        raise Exception("getUserFriendlyName not implemented!")
    
    #defines how to find an object, no matter in what process or physical address
    def getIdReplacement(self):
        raise Exception("getIdReplacement not implemented!")
    
    #finds the closest Object in the tree if the requested one doesn't exist
    def findObjectByIdReplacementSuffix(self, urlrest, statusfilename):
        raise Exception("findObjectByIdReplacementSuffix not implemented!")

    def getNaviName(self):
        raise Exception("getNaviName not implemented!")
    
    def childrenMethodPairs(self):
        raise Exception("childrenMethodPairs not implemented!")
    
    def parentMethods(self):
        raise Exception("parentMethods not implemented!")
