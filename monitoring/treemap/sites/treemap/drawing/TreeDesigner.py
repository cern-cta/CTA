'''
Created on Jul 7, 2010

@author: kblaszcz
'''

import cairo
from sites.treemap.defaultproperties.SquaredViewProperties import BasicViewTreeProps, ViewTreeCalculationProps, ViewTreeDesignProps
from sites.treemap.viewtree.ViewTree import ViewTree
from sites.tools.HsvConverter import *
import random

class SquaredTreemapDesigner(object):
    '''
    classdocs
    '''

    def __init__(self, vtree, design_properties = None):
        '''
        Constructor
        '''
        assert (design_properties is None or isinstance(design_properties, ViewTreeDesignProps)) 
        assert (isinstance(vtree, ViewTree)) 
        self.vtree = vtree
        
        self.design_properties = vtree.getDesignProperties()
        
        if self.design_properties is None and design_properties is None:
            vtree.setDesignProperties(ViewTreeDesignProps(calc_properties = vtree.getCalcProperties(), tree = self.vtree))
        elif design_properties is not None:
            vtree.setDesignProperties(design_properties)
        #...otherwise design_properties are already set
        
        #update value
        self.design_properties = vtree.getDesignProperties()
        
        self.inbordersize = self.design_properties.getProperty('inbordersize')
        self.headertextsize = self.design_properties.getProperty('headertextsize')
        self.radiallightbrightness = self.design_properties.getProperty('radiallightbrightness')
        
    def designTreemap(self):
        inbordersize = self.inbordersize
        
        self.vtree.traveseToRoot()
        root = self.vtree.getCurrentObject()
        
        root.setProperty('inbordersize', inbordersize)
        root.setProperty('headertextsize',self.headertextsize)
        r,g,b = self.googleColors(root.getProperty('level'))
        root.setProperty('fillcolor', {'r':r, 'g':g,'b':b} )
        r,g,b = self.googleColors(root.getProperty('level') + 2)
        r,g,b = addValueToRgb(r,g,b,-0.3)
        root.setProperty('strokecolor', {'r':r, 'g':g,'b':b} )
        root.setProperty('radiallight', {'brightness': self.radiallightbrightness, 'hue':random.random(), 'opacity': 0.5 })
        
        if(inbordersize > 1):
            self.designRecursion(inbordersize-1, 0+1)
        else:
            self.designRecursion(inbordersize, 0+1)
            
    def designRecursion(self, inbordersize, level):
        children = self.vtree.getChildren()
        
        for child in children: #(self, text, x, y, max_text_width, max_text_height)
           
            child.setProperty('inbordersize', inbordersize)
            child.setProperty('headertextsize',self.headertextsize)
            r,g,b = self.googleColors(child.getProperty('level'))
            child.setProperty('fillcolor', {'r':r, 'g':g,'b':b} )
            r,g,b = self.googleColors(child.getProperty('level') + 2)
            child.setProperty('strokecolor', {'r':r, 'g':g,'b':b} )
            child.setProperty('radiallight', {'brightness': self.radiallightbrightness, 'hue':random.random(), 'opacity': 0.5 })

            self.vtree.traverseInto(child)
            
            if inbordersize > 1:
                self.designRecursion(inbordersize-1, level + 1)
            else:
                self.designRecursion(inbordersize, level + 1)
                
            self.vtree.traverseBack()
    def googleColors(self, number):
        number = number%4
        if number == 0:
            return 1.0, 0.67, 0.0
        if number == 1:
            return 0.0, 0.44, 0.055
        if number == 2:
            return 0.689, 0.0852, 0.155 
        if number == 3:
            return 0.191, 0.307, 0.66 
 
   