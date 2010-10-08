'''
Created on Jul 7, 2010

@author: kblaszcz
'''
from sites.tools.HsvConverter import *
from sites.treemap.defaultproperties.SquaredViewProperties import \
    BasicViewTreeProps, ViewTreeCalculationProps, ViewTreeDesignProps
from sites.treemap.drawing.metricslinking.MetricsLinker import MetricsLinker
from sites.treemap.viewtree.ViewTree import ViewTree
import cairo
import random


class SquaredTreemapDesigner(object):
    '''
    classdocs
    '''

    def __init__(self, vtree, design_properties = None, metricslinkage = None):
        '''
        Constructor
        '''
        assert (design_properties is None or isinstance(design_properties, ViewTreeDesignProps)) 
        assert (metricslinkage is not None or isinstance(metricslinkage, MetricsLinker))
        
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
        self.metricslinkage = metricslinkage
        
        self.inbordersize = self.design_properties.getProperty('inbordersize')
        self.headertextsize = self.design_properties.getProperty('headertextsize')
        self.radiallightbrightness = self.design_properties.getProperty('radiallightbrightness')
        self.headertextisbold = self.design_properties.getProperty('headertext.isbold')

    def designTreemap(self):
        inbordersize = self.inbordersize
        
        self.vtree.traveseToRoot()
        root = self.vtree.getCurrentObject()
        
        self.setInBorderSize(root)
        self.setHeaderTextsize(root)
        self.setFillColor(root)
        self.setStrokeColor(root)
        self.setRadialLight(root)
        self.setHeaderText(root)
        self.setHtmlInfoText(root)
        self.setHeaderTextIsbold(root)
        
        self.designRecursion(0+1)
            
    def designRecursion(self, level):
        children = self.vtree.getChildren()
        
        for child in children: #(self, text, x, y, max_text_width, max_text_height)
           
            self.setInBorderSize(child)
            self.setHeaderTextsize(child)
            self.setFillColor(child)
            self.setStrokeColor(child)          
            self.setRadialLight(child)
            self.setHeaderText(child)
            self.setHtmlInfoText(child)
            self.setHeaderTextIsbold(child)
            
            self.vtree.traverseInto(child)
            self.designRecursion(level + 1)
            self.vtree.traverseBack()
            
    def googleColors(self, number):
        #strokecolor
        if number == -1: return 0.5, 0.5, 0.5, 1.0
        #fillcolor of Annex
        if number == -2: return 0.0, 0.0, 0.0, 0.0
        
        number = number%4
        
        if number == 0:
            return 1.0, 0.67, 0.0, 1.0
        if number == 1:
            return 0.0, 0.44, 0.055, 1.0
        if number == 2:
            return 0.689, 0.0852, 0.155, 1.0
        if number == 3:
            return 0.191, 0.307, 0.66, 1.0
        #extra
        if number == 4:
            return 0.08235, 0.6901, 0.6901, 1.0
        
    def setHeaderText(self, vnode):
        try:
            vnode.setProperty('headertext', self.metricslinkage.getLinkedValue('headertext', vnode))
        except Exception, e:
            vnode.setProperty('headertext', vnode.getProperty('treenode').getObject().__str__())
            
    def setHeaderTextIsbold(self, vnode):
        try:
            vnode.setProperty('headertext.isbold', self.metricslinkage.getLinkedValue('headertext.isbold', vnode))
        except Exception, e:
            vnode.setProperty('headertext.isbold', self.headertextisbold)
            
    def setHtmlInfoText(self, vnode):
        try:
            vnode.setProperty('htmlinfotext', self.metricslinkage.getLinkedValue('htmlinfotext', vnode))
        except Exception, e:
            vnode.setProperty('htmlinfotext', vnode.getProperty('treenode').getObject().__str__())
        
    def setFillColor(self, vnode):
        try:
            r,g,b,a = self.googleColors(self.metricslinkage.getLinkedValue('fillcolor', vnode))
        except:
            try:
                r,g,b,a = self.googleColors(vnode.getProperty('level'))
            except KeyError:
                raise Exception("Level information for node is missing")
            
        vnode.setProperty('fillcolor', {'r':r, 'g':g, 'b':b, 'a':a} )
        
    def setStrokeColor(self,vnode):
        try:
            r,g,b,a = self.googleColors(self.metricslinkage.getLinkedValue('strokecolor', vnode))
        except:
            try:
                r,g,b,a = self.googleColors(vnode.getProperty('level') + 2)
            except KeyError:
                raise Exception("Level information for node is missing")
#        r,g,b = addValueToRgb(r,g,b,-0.3)
        vnode.setProperty('strokecolor', {'r':r, 'g':g, 'b':b, 'a':a} )
        
    def setInBorderSize(self,vnode):
        try:
            vnode.setProperty('inbordersize', self.metricslinkage.getLinkedValue('inbordersize', vnode))
        except:
            try:
                inbsize = self.inbordersize - vnode.getProperty('level')
                if inbsize < 0: inbsize = 0
                vnode.setProperty('inbordersize', inbsize)
            except KeyError:
                raise Exception("Level information for node is missing")
            
    def setHeaderTextsize(self,vnode):
        try:
            vnode.setProperty('headertextsize', self.metricslinkage.getLinkedValue('headertextsize', vnode))
        except:
            vnode.setProperty('headertextsize', self.headertextsize)
            
    def setRadialLight(self, vnode):
        try:
            b = self.metricslinkage.getLinkedValue('radiallight.brightness', vnode)
        except:
            b = self.radiallightbrightness
         
        try:   
            h = self.metricslinkage.getLinkedValue('radiallight.hue', vnode)
        except:
            h = random.random()
            
        try:
            o = self.metricslinkage.getLinkedValue('radiallight.opacity', vnode)
        except:
            o = 0.5
            
        vnode.setProperty('radiallight', {'brightness': b, 'hue':h, 'opacity': o })
 
   