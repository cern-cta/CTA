'''
Created on Jul 22, 2010
stores relations between a node properties and Dimensions (which define values of that properties)

model the node is related to + specific node property (ie. text in the label) -> Dimension defining the value (ie. a db column)

DimensionVisPropMapping mapping
mapping.mapVisPropetyToDimension('CnsFileMetadata', 'labeltext', RawColumnDimension('name'))

All available ViewNode properties like 'labeltext' are set by TreeBuilder and TreeCalculators.
See there for further description

@author: kblaszcz
'''

import exceptions

#at the moment following properties are available:
#fillcolor, strokecolor, strokesize, labelfontsize, radiallight.brightness, radiallight.hue, radiallight.opacity

class DimensionVisPropMapping(object):
    '''
    classdocs
    '''
    def __init__(self):
        '''
        Constructor
        '''
        self.proplink = {}
        
    
    def mapVisPropetyToDimension(self, modelname, propertyname, dimensionobject):
        #no check reuirements like min max and istext?
        self.proplink[(modelname,propertyname)] = dimensionobject
        
    def getMappedValue(self, propertyname, node):
        try:
            modelname = node.treenode.getObject().__class__.__name__
        except:
            raise Exception("Viewnode has no information about related Treenode")
        
        try:
            return self.proplink[(modelname, propertyname)].getValue(node)
        except Exception, e:
            raise Exception("That property is not linked")