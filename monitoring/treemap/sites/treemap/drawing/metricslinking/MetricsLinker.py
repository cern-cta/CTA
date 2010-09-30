'''
Created on Jul 22, 2010

@author: kblaszcz
'''

import exceptions

#at the moment following properties are available:
#fillcolor, strokecolor, inbordersize, headertextsize, radiallight.brightness, radiallight.hue, radiallight.opacity

class MetricsLinker(object):
    '''
    classdocs
    '''
    def __init__(self):
        '''
        Constructor
        '''
        self.proplink = {}
        
    
    def addPropertyLink(self, classname, propertyname, dimensionobject):
        #no check reuirements like min max and istext?
        self.proplink[(classname,propertyname)] = dimensionobject
        
    def getLinkedValue(self, propertyname, node):
        try:
            classname = node.getProperty('treenode').getObject().__class__.__name__
        except:
            raise Exception("Viewnode has no information about related Treenode")
        
        try:
            return self.proplink[(classname, propertyname)].getValue(node)
        except KeyError, e:
            raise Exception("That property is not linked")