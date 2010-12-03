'''
Created on Jun 4, 2010

holds a dictionary collecting properties of a Node (key-value pairs)

@author: kblaszcz
'''

class ViewNode(object):
    '''
    classdocs
    '''


    def __init__(self):
        '''
        Constructor
        '''
        self.props = {}
        
    def setProperty (self, key, value):
        self.props[key] = value
        
    def getProperty (self, key):
        try:
            return self.props[key]
        except KeyError:
            return None
    
#    def getObject(self):
#        return self.obj
    
    def __str__(self):
        return self.obj.__str__()