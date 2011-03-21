'''
Created on Jul 7, 2010
Sets design and web interface related properties for each node:

labeltext - text string to show in the rectangle label
labeltextisbold - defines if the label text should be bold
htmltooltiptext - defines the tooltip text for that node
fillcolor - color the rectangle should be filled with
level - level inside of the data tree (root is 0, ascending)
radiallight {'brightness': b, 'hue':h, 'opacity': o } - defines brightness, hue and opacity of the "light" shining at the rectangle

If a property is not available, there is a hardcoded default value

@author: kblaszcz
'''
from app.tools.ColorFunctions import *
from app.treemap.defaultproperties.TreeMapProperties import treemap_props
from app.treemap.drawing.metricslinking.MetricsLinker import MetricsLinker
from app.treemap.viewtree.ViewTree import ViewTree
import cairo
import random


class SquaredTreemapDesigner(object):
    '''
    classdocs
    '''

    def __init__(self, vtree, treemap_props, metricslinkage = None):
        '''
        Constructor
        '''
        assert (metricslinkage is not None or isinstance(metricslinkage, MetricsLinker))
        
        assert (isinstance(vtree, ViewTree)) 
        self.vtree = vtree
        self.metricslinkage = metricslinkage
        
        self.strokesize = treemap_props['strokesize']
        self.labelfontsize = treemap_props['labelfontsize']
        self.radiallightbrightness = treemap_props['radiallightbrightness']
        self.labeltextisbold = treemap_props['labeltextisbold']
        self.strokesizedecrease = treemap_props['strokesizedecrease']
        self.minstrokesize = treemap_props['minstrokesize']
        self.icons = treemap_props['icons']

    def designTreemap(self):
        strokesize = self.strokesize
        
        self.vtree.traveseToRoot()
        root = self.vtree.getCurrentObject()
        
        self.setStrokeSize(root)
        self.setLabelFontSize(root)
        self.setFillColor(root)
        self.setStrokeColor(root)
        self.setRadialLight(root)
        self.setLabelText(root)
        self.setToolTipInfoText(root)
        self.setLabelTextIsbold(root)
        self.setIcon(root)
        self.setIconFile(root)
        
        self.designRecursion(0+1)
            
    def designRecursion(self, level):
        children = self.vtree.getChildren()
        
        for child in children: #(self, text, x, y, max_text_width, max_text_height)
            self.setStrokeSize(child)
            self.setLabelFontSize(child)
            self.setFillColor(child)
            self.setStrokeColor(child)          
            self.setRadialLight(child)
            self.setLabelText(child)
            self.setToolTipInfoText(child)
            self.setLabelTextIsbold(child)
            self.setIcon(child)
            self.setIconFile(child)
            
            self.vtree.traverseIntoChild(child)
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
        
    def setLabelText(self, vnode):
        try:
            vnode.setProperty('labeltext', self.metricslinkage.getLinkedValue('labeltext', vnode))
        except Exception, e:
            vnode.setProperty('labeltext', vnode.getProperty('treenode').getObject().__str__())
            
    def setIcon(self, vnode):
        icon = False
        try:
            icon = self.metricslinkage.getLinkedValue('icon', vnode)
            vnode.setProperty('icon', icon)
                
        except Exception, e:
            icon = vnode.getProperty('hasannex')
            vnode.setProperty('icon', icon)
            
        if icon:
            iconx = vnode.getProperty('x') + vnode.getProperty('labelwidth') - vnode.getProperty('labelheight')
            icony = vnode.getProperty('y')
            iconwidth = vnode.getProperty('labelheight')
            iconheight = vnode.getProperty('labelheight')
            labelwidth = vnode.getProperty('labelwidth')-iconwidth
            
            if labelwidth > 0.0:
                vnode.setProperty('iconcoords', {'x':iconx,'y':icony,'width':iconwidth,'height':iconheight})
                vnode.setProperty('labelwidth', labelwidth)
            else:
                vnode.setProperty('iconcoords', {'x':0,'y':0,'width':0,'height':0})
                vnode.setProperty('icon', False)
        else:
            vnode.setProperty('iconcoords', {'x':0,'y':0,'width':0,'height':0})
            
    def setIconFile(self, vnode):
        iconfile = False
        try:
            iconfile = self.metricslinkage.getLinkedValue('iconfile', vnode)
            vnode.setProperty('iconfile', iconfile)
                
        except Exception, e:
            iconfile = treemap_props["defaulticonfile"]
            vnode.setProperty('iconfile', iconfile)
            
    def setLabelTextIsbold(self, vnode):
        try:
            vnode.setProperty('labeltextisbold', self.metricslinkage.getLinkedValue('labeltextisbold', vnode))
        except Exception, e:
            vnode.setProperty('labeltextisbold', self.labeltextisbold)
            
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
        
    def setStrokeSize(self,vnode):
        try:
            strokesize = self.metricslinkage.getLinkedValue('strokesize', vnode)
            if strokesize < self.minstrokesize: strokesize = self.minstrokesize
            vnode.setProperty('strokesize', strokesize)
        except:
            try:
                strokesize = self.strokesize - vnode.getProperty('level') * self.strokesizedecrease
                if strokesize < self.minstrokesize: strokesize = self.minstrokesize
                vnode.setProperty('strokesize', strokesize)
            except KeyError:
                raise Exception("Level information for node is missing")
            
    def setLabelFontSize(self,vnode):
        try:
            vnode.setProperty('labelfontsize', self.metricslinkage.getLinkedValue('labelfontsize', vnode))
        except:
            vnode.setProperty('labelfontsize', self.labelfontsize)
            
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
 
   