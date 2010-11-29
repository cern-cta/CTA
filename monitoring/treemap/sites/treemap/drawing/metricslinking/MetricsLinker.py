'''
Created on Jul 22, 2010
stores relations between a node properties and Dimensions (which define values of that properties)

model the node is related to + specific node property (ie. text in the header) -> Dimension defining the value (ie. a db column)

MetricsLinker mlinker
mlinker.addPropertyLink('CnsFileMetadata', 'headertext', RawColumnDimension('name'))

All available ViewNode properties like 'headertext' are set by TreeBuilder and TreeCalculators.
See there for further description

@author: kblaszcz
'''

import exceptions

#at the moment following properties are available:
#fillcolor, strokecolor, inbordersize, headerfontsize, radiallight.brightness, radiallight.hue, radiallight.opacity

class MetricsLinker(object):
    '''
    classdocs
    '''
    def __init__(self):
        '''
        Constructor
        '''
        self.proplink = {}
        
    
    def addPropertyLink(self, modelname, propertyname, dimensionobject):
        #no check reuirements like min max and istext?
        self.proplink[(modelname,propertyname)] = dimensionobject
        
    def getLinkedValue(self, propertyname, node):
        try:
            modelname = node.getProperty('treenode').getObject().__class__.__name__
        except:
            raise Exception("Viewnode has no information about related Treenode")
        
        try:
            return self.proplink[(modelname, propertyname)].getValue(node)
        except Exception, e:
            raise Exception("That property is not linked")