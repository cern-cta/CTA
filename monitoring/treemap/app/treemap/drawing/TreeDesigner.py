'''
Created on Jul 7, 2010
Sets design and web interface related properties for each node:

headertext - text string to show in the rectangle header
headertextisbold - defines if the header text should be bold
htmltooltiptext - defines the tooltip text for that node
fillcolor - color the rectangle should be filled with
level - level inside of the data tree (root is 0, ascending)
radiallight {'brightness': b, 'hue':h, 'opacity': o } - defines brightness, hue and opacity of the "light" shining at the rectangle

If a property is not available, there is a hardcoded default value

@author: kblaszcz
'''
from app.tools.ColorFunctions import *
from app.treemap.defaultproperties.TreeMapProperties import \
    BasicViewTreeProps, ViewTreeCalculationProps, ViewTreeDesignProps
from app.treemap.drawing.metricslinking.MetricsLinker import MetricsLinker
from app.treemap.viewtree.ViewTree import ViewTree
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
        self.headerfontsize = self.design_properties.getProperty('headerfontsize')
        self.radiallightbrightness = self.design_properties.getProperty('radiallightbrightness')
        self.headertextisbold = self.design_properties.getProperty('headertextisbold')

    def designTreemap(self):
        inbordersize = self.inbordersize
        
        self.vtree.traveseToRoot()
        root = self.vtree.getCurrentObject()
        
        self.setInBorderSize(root)
        self.setHeaderFontSize(root)
        self.setFillColor(root)
        self.setStrokeColor(root)
        self.setRadialLight(root)
        self.setHeaderText(root)
        self.setToolTipInfoText(root)
        self.setHeaderTextIsbold(root)
        
        self.designRecursion(0+1)
            
    def designRecursion(self, level):
        children = self.vtree.getChildren()
        
        for child in children: #(self, text, x, y, max_text_width, max_text_height)
           
            self.setInBorderSize(child)
            self.setHeaderFontSize(child)
            self.setFillColor(child)
            self.setStrokeColor(child)          
            self.setRadialLight(child)
            self.setHeaderText(child)
            self.setToolTipInfoText(child)
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
            vnode.setProperty('headertextisbold', self.metricslinkage.getLinkedValue('headertextisbold', vnode))
        except Exception, e:
            vnode.setProperty('headertextisbold', self.headertextisbold)
            
    def setToolTipInfoText(self, vnode):
        try:
            vnode.setProperty('htmltooltiptext', self.metricslinkage.getLinkedValue('htmltooltiptext', vnode))
        except Exception, e:
            vnode.setProperty('htmltooltiptext', vnode.getProperty('treenode').getObject().__str__())
        
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
            
    def setHeaderFontSize(self,vnode):
        try:
            vnode.setProperty('headerfontsize', self.metricslinkage.getLinkedValue('headerfontsize', vnode))
        except:
            vnode.setProperty('headerfontsize', self.headerfontsize)
            
    def setRadialLight(self, vnode):
        try:
            fillc = vnode.getProperty('fillcolor')
            sensitivityfactor = 7.0
            human_eye_sensitivity = ((0.299*fillc['r'] + 0.114 * fillc['b'] + 0.587 * fillc['g'])/3) * sensitivityfactor
        except:
            human_eye_sensitivity = 1.0
        
        try:
            b = self.metricslinkage.getLinkedValue('radiallight.brightness', vnode)* human_eye_sensitivity
        except:
            b = human_eye_sensitivity*self.radiallightbrightness
         
        try:   
            h = self.metricslinkage.getLinkedValue('radiallight.hue', vnode)
            if(h is None):
                fillc = vnode.getProperty('fillcolor')
                h,s,v = rgbToHsv(fillc['r'], fillc['g'], fillc['b'])
            else:
                fillc = vnode.getProperty('fillcolor')
                ch,s,v = rgbToHsv(fillc['r'], fillc['g'], fillc['b'])
                if(math.fabs(ch-h)) < 0.1: h=1.0-h
                red,green,blue = hsvToRgb(h,s,v)
                b = b/human_eye_sensitivity #remove old sensitivity
                human_eye_sensitivity = ((0.299*red + 0.114 * blue + 0.587 * green)/3) * sensitivityfactor
                b = b*self.radiallightbrightness #calculate with the new sensitivity
        except:
            h = random.random()
            
        try:
            o = self.metricslinkage.getLinkedValue('radiallight.opacity', vnode)
        except:
            o = 0.5
            
        vnode.setProperty('radiallight', {'brightness': b, 'hue':h, 'opacity': o })
 
   