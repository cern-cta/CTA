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
    
    #defines how to find an object
    def getIdReplacement(self):
        raise Exception("getIdReplacement not implemented!")
    
    #returns the Object type
    def getClassName(self):
        raise Exception("getClassName not implemented!")
    
    #finds the closest proper root Object in the tree if the requested one doesn't exist
    #has to raise NoDataAvailableError if not found
    def findObjectByIdReplacementId(self, rid, statusfilename):
        raise Exception("findObjectByIdReplacementId not implemented!") 

    def getNaviName(self):
        raise Exception("getNaviName not implemented!")
    
    #defines how to get the children and how to count them (if possible without reading all of them)
    #childrenMethod pairs must return a list of dictionaries and use the keywords 'childrenmethod' and 'childrencounter'
    #example:
    #[
    # {'childrenmethod': 'getDirs', 'childrencounter': 'countDirs'},
    # {'childrenmethod': 'getFiles', 'childrencounter': 'countFiles'},
    # {'childrenmethod': 'getFilesAndDirectories', 'childrencounter': 'countFilesAndDirs'},
    #]
 
    def childrenMethodPairs(self):
        raise Exception("childrenMethodPairs not implemented!")
    
    #defines how to get the Parent
    def parentMethods(self):
        raise Exception("parentMethods not implemented!")
    
    #metrics which are not related to columns from the database but can be accessed with the dot operator
    def metricAttributes(self):
        raise Exception("metricAttributes not implemented!")