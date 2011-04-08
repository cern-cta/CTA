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
from app.tools.ColorFunctions import rgbToHsv, hsvToRgb
from app.treemap.defaultproperties.TreeMapProperties import treemap_props
from app.treemap.drawing.dimensionmapping.DimensionVisPropMapping import \
    DimensionVisPropMapping
from app.treemap.viewtree.ViewTree import ViewTree
import random
import math


class SquaredTreemapDesigner(object):
    '''
    classdocs
    '''

    def __init__(self, vtree, treemap_props, mapping = None):
        '''
        Constructor
        '''
        assert (mapping is not None or isinstance(mapping, DimensionVisPropMapping))
        
        assert (isinstance(vtree, ViewTree)) 
        self.vtree = vtree
        self.mapping = mapping
        
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
            vnode.labeltext = self.mapping.getMappedValue('labeltext', vnode)
        except Exception, e:
            vnode.labeltext = vnode.treenode.getObject().__str__()
            
    def setIcon(self, vnode):
        icon = False
        try:
            icon = self.mapping.getMappedValue('icon', vnode)
            vnode.icon = icon
                
        except Exception, e:
            icon = vnode.hasannex
            vnode.icon = icon
            
        if icon:
            iconx = vnode.x + vnode.labelwidth - vnode.labelheight
            icony = vnode.y
            iconwidth = vnode.labelheight
            iconheight = vnode.labelheight
            labelwidth = vnode.labelwidth -iconwidth
            
            if labelwidth > 0.0:
                vnode.iconcoords = {'x':iconx,'y':icony,'width':iconwidth,'height':iconheight}
                vnode.labelwidth =  labelwidth
            else:
                vnode.iconcoords = {'x':0,'y':0,'width':0,'height':0}
                vnode.icon = False
        else:
            vnode.iconcoords = {'x':0,'y':0,'width':0,'height':0}
            
    def setIconFile(self, vnode):
        iconfile = False
        try:
            iconfile = self.mapping.getMappedValue('iconfile', vnode)
            vnode.iconfile = iconfile
                
        except Exception, e:
            iconfile = treemap_props["defaulticonfile"]
            vnode.iconfile = iconfile
            
    def setLabelTextIsbold(self, vnode):
        try:
            vnode.labeltextisbold = self.mapping.getMappedValue('labeltextisbold', vnode)
        except Exception, e:
            vnode.labeltextisbold = self.labeltextisbold
            
    def setToolTipInfoText(self, vnode):
        try:
            vnode.htmltooltiptext = self.mapping.getMappedValue('htmltooltiptext', vnode)
        except Exception, e:
            vnode.htmltooltiptext = vnode.treenode.getObject().__str__()
        
    def setFillColor(self, vnode):
        try:
            r,g,b,a = self.googleColors(self.mapping.getMappedValue('fillcolor', vnode))
        except:
            try:
                r,g,b,a = self.googleColors(vnode.level)
            except KeyError:
                raise Exception("Level information for node is missing")
            
        vnode.fillcolor = {'r':r, 'g':g, 'b':b, 'a':a} 
        
    def setStrokeColor(self,vnode):
        try:
            r,g,b,a = self.googleColors(self.mapping.getMappedValue('strokecolor', vnode))
        except:
            try:
                r,g,b,a = self.googleColors(vnode.level + 2)
            except KeyError:
                raise Exception("Level information for node is missing")
#        r,g,b = addValueToRgb(r,g,b,-0.3)
        vnode.strokecolor = {'r':r, 'g':g, 'b':b, 'a':a}
        
    def setStrokeSize(self,vnode):
        try:
            strokesize = self.mapping.getMappedValue('strokesize', vnode)
            if strokesize < self.minstrokesize: strokesize = self.minstrokesize
            vnode.strokesize = strokesize
        except:
            try:
                strokesize = self.strokesize - vnode.level * self.strokesizedecrease
                if strokesize < self.minstrokesize: strokesize = self.minstrokesize
                vnode.strokesize = strokesize
            except KeyError:
                raise Exception("Level information for node is missing")
            
    def setLabelFontSize(self,vnode):
        try:
            vnode.labelfontsize = self.mapping.getMappedValue('labelfontsize', vnode)
        except:
            vnode.labelfontsize = self.labelfontsize
            
    def setRadialLight(self, vnode):
        try:
            fillc = vnode.fillcolor
            sensitivityfactor = 7.0
            human_eye_sensitivity = ((0.299*fillc['r'] + 0.114 * fillc['b'] + 0.587 * fillc['g'])/3) * sensitivityfactor
        except:
            human_eye_sensitivity = 1.0
        
        try:
            b = self.mapping.getMappedValue('radiallight.brightness', vnode)* human_eye_sensitivity
        except:
            b = human_eye_sensitivity*self.radiallightbrightness
         
        try:   
            h = self.mapping.getMappedValue('radiallight.hue', vnode)
            if(h is None):
                fillc = vnode.fillcolor
                h,s,v = rgbToHsv(fillc['r'], fillc['g'], fillc['b'])
            else:
                fillc = vnode.fillcolor
                ch,s,v = rgbToHsv(fillc['r'], fillc['g'], fillc['b'])
                if(math.fabs(ch-h)) < 0.1: h=1.0-h
                red,green,blue = hsvToRgb(h,s,v)
                b = b/human_eye_sensitivity #remove old sensitivity
                human_eye_sensitivity = ((0.299*red + 0.114 * blue + 0.587 * green)/3) * sensitivityfactor
                b = b*self.radiallightbrightness #calculate with the new sensitivity
        except:
            h = random.random()
            
        try:
            o = self.mapping.getMappedValue('radiallight.opacity', vnode)
        except:
            o = 0.5
            
        vnode.radiallight = {'brightness': b, 'hue':h, 'opacity': o }
 
   