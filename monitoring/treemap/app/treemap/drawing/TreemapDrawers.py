'''
Created on Jun 10, 2010

This class reads ViewNodes from a ViewTree and draws them depending on their properties.
The resulting output is written into a png file.

@author: kblaszcz
'''
import cairo
import math
from app.tools.ColorFunctions import *
from app.treemap.defaultproperties.TreeMapProperties import BasicViewTreeProps, ViewTreeCalculationProps, ViewTreeDesignProps
from app.treemap.viewtree.ViewTree import ViewTree

class SquaredTreemapDrawer(object):
    '''
    classdocs
    '''

    def __init__(self, vtree, basic_properties = None, design_properties = None):
        '''
        Constructor
        '''
        assert (design_properties is None or isinstance(design_properties, ViewTreeDesignProps))
        assert(basic_properties is None or isinstance(basic_properties, BasicViewTreeProps)), "basic_properties wrong"
        assert (isinstance(vtree, ViewTree)) 
        
        saved_basic_properties = vtree.getBasicProperties()
        
        if saved_basic_properties is None and basic_properties is None:
            vtree.setBasicProperties(BasicViewTreeProps(tree = self.tree))
        elif basic_properties is not None:
            vtree.setBasicProperties(basic_properties)
        #...otherwise basic_properties are already set
        
        self.vtree = vtree
        
        self.basic_properties = vtree.getBasicProperties()
        assert (self.basic_properties is not None)
        self.mapwidth = self.basic_properties.getProperty('width')
        self.mapheight = self.basic_properties.getProperty('height')
    
        self.surface = cairo.ImageSurface (cairo.FORMAT_ARGB32, self.mapwidth, self.mapheight)
        self.ctx = cairo.Context (self.surface)
        self.ctx.scale (self.mapwidth/1.0, self.mapheight/1.0) # Normalizing the canvas
        
        #setting background color
        pat = cairo.LinearGradient (0, 0, 1, 1)
        pat.add_color_stop_rgba (0.0, 0.7137, 0.9294, 0.0, 1.0) # last number is opacity
        pat.add_color_stop_rgba (1.0, 0.6666, 0.9294, 0.0, 1.0) # last number is opacity
        self.ctx.rectangle (0,0,1,1) # Rectangle(x0, y0, x1, y1)
        self.ctx.set_source (pat)
        self.ctx.fill ()
        
        #enable antialiasing for text
        fo = cairo.FontOptions()
        fo.set_antialias(cairo.ANTIALIAS_GRAY)
        self.ctx.set_font_options(fo)
        
    def drawTreemap(self, filename):
        self.printText.__dict__['brokentextcount'] = 0
        self.printText.__dict__['brokentextletters'] = 0
        self.printText.__dict__['fulltextcount'] = 0
        self.printText.__dict__['fulltextletters'] = 0

        self.vtree.traveseToRoot()
        root = self.vtree.getCurrentObject()
        
        self.drawRect(root.getProperty('x'), root.getProperty('y'), root.getProperty('width'), root.getProperty('height'), root.getProperty('inbordersize'), root.getProperty('level'), root.getProperty('fillcolor'), root.getProperty('strokecolor'), root.getProperty('radiallight'))
        self.printText(root.getProperty('treenode').getObject().__str__(), root.getProperty('x') + root.getProperty('inbordersize'), root.getProperty('y') + root.getProperty('inbordersize'), root.getProperty('width')-2* root.getProperty('inbordersize'), root.getProperty('headerfontsize'), root.getProperty('headertextisbold'))
        
        self.drawRecursion()
        
        self.surface.write_to_png (filename)
        
        print "brokentextcount ", self.printText.__dict__['brokentextcount']
        print "fulltextcount ", self.printText.__dict__['fulltextcount']
        print "brokentextletters ", self.printText.__dict__['brokentextletters']
        print "fulltextletters ", self.printText.__dict__['fulltextletters']
        
    def drawRecursion(self):
        children = self.vtree.getChildren()
        
        for child in children: #(self, text, x, y, max_text_width, max_text_height)
            inbordersize = child.getProperty('inbordersize')
            
            self.drawRect(child.getProperty('x'), child.getProperty('y'), child.getProperty('width'), child.getProperty('height'), inbordersize, child.getProperty('level'), child.getProperty('fillcolor'), child.getProperty('strokecolor'), child.getProperty('radiallight'))
            
            if child.getProperty('headersize') > 0.0:
                txt = child.getProperty('headertext')
                self.printText(txt, child.getProperty('x') + inbordersize, child.getProperty('y') + inbordersize, child.getProperty('width')-2*inbordersize, child.getProperty('headerfontsize'), child.getProperty('headertextisbold'))
                
            self.vtree.traverseInto(child)
            self.drawRecursion()
            self.vtree.traverseBack()
        
        
    def drawRect(self, x, y, subwidth, subheight, bordersize, level, fillcolor, strokecolor, radiallight):
        pix_x = 1.0/self.mapwidth
        pix_y = 1.0/self.mapheight
        
        x0 = pix_x*x
        y0 = pix_y*y
        width = pix_x*(subwidth)
        height = pix_y*(subheight)

        #smaller rectangle coordinates
#        x0 = x0+pix_x*bordersize
#        y0 = y0+pix_y*bordersize
#        width = width-2*pix_x*bordersize
#        height = height-2*pix_y*bordersize
        
        #gradient inside of smaller rectangle
        r,g,b,a = fillcolor['r'], fillcolor['g'], fillcolor['b'], fillcolor['a']

        pat = cairo.LinearGradient (x0, y0, x0+width, y0)
#        pat.add_color_stop_rgba (0.0, r, g, b, 1.0)
#        r,g,b = fillcolor['r'], fillcolor['g'], fillcolor['b']
        pat.add_color_stop_rgba (0.0, r, g ,b, a) # last number is opacity
        pat.add_color_stop_rgba (1.0, r, g ,b, a) # last number is opacity
        
        
        self.ctx.rectangle (x0,y0,width,height) # Rectangle(x0, y0, x1, y1)
        self.ctx.set_source (pat)
        self.ctx.fill ()
        
        #radial gradient over smaller rectangle
        ratio = height/width
        
        maxsize = 0
        if width>height: 
            maxsize = width 
            minsize = height
        else: 
            maxsize = height
            minsize = width
             
        radial = cairo.RadialGradient(x0+width/2.0, y0+height/2.0, pix_x/2.0, x0+width/2.0, y0+height/2.0, maxsize*1.41)
        r,g,b = fillcolor['r'], fillcolor['g'], fillcolor['b']
        r,g,b = addValueToRgb (r,g,b,radiallight['brightness'])
        
        try:
            radiallighthue = radiallight['hue']
            r,g,b = setHueToRgb (r,g,b,radiallighthue)
        except KeyError:
            pass
        
        a = 1.0
        try:
            a = radiallight['opacity']
        except KeyError:
            pass
        
        radial.add_color_stop_rgba(0.0, r, g, b, a)
        radial.add_color_stop_rgba(0.5, r, g, b, 0.0)
#        
#        mx = radial.get_matrix()
#        if maxsize == width:
#            mx.scale(1,ratio )
#            mx.translate(x0, y0*ratio)
#        elif maxsize == height:
#            mx.scale(1.0/ratio,1)
#            mx.translate(x0/ratio, y0)
#
#        radial.set_matrix(mx)
#        
        self.ctx.rectangle (x0,y0,width,height) # Rectangle(x0, y0, x1, y1)
        self.ctx.set_source (radial)
        self.ctx.fill ()
        
        x0 = pix_x*(x+bordersize*0.5)
        y0 = pix_y*(y+bordersize*0.5)
        width = pix_x*(subwidth-bordersize)
        height = pix_y*(subheight-bordersize)
        
        #rectangle border
        r,g,b,a = strokecolor['r'], strokecolor['g'], strokecolor['b'], strokecolor['a']
        
        self.ctx.move_to (x0, y0 )
        
        self.ctx.set_line_width (pix_y*bordersize)
        self.ctx.line_to (x0 + width, y0)
        
        self.ctx.set_line_width (pix_x*bordersize)
        self.ctx.line_to (x0 + width, y0 + height)
        
        self.ctx.set_line_width (pix_y*bordersize)
        self.ctx.line_to (x0 , y0 + height)
        
        self.ctx.set_line_width (pix_x*bordersize)
        self.ctx.line_to (x0 , y0)
        
        self.ctx.close_path ()

        self.ctx.set_source_rgba(r,g,b,a) 
        self.ctx.stroke ()
        
    def printText(self, text, x, y, max_text_width, max_text_height, isbold): #text, x, y, subwidth, subheight, bordersize, level):
        pix_x = 1.0/self.mapwidth
        pix_y = 1.0/self.mapheight
#        
#        x0 = pix_x*x
#        y0 = pix_y*y
#        width = pix_x*(subwidth)
#        height = pix_y*(subheight)
        scalex = 0.0
        scaley = 0.0
        
        if self.mapwidth > self.mapheight:
            scalex = self.mapheight/self.mapwidth
            scaley = 1.0
        else:
            scalex = 1.0
            scaley = self.mapwidth/self.mapheight
            
        pix_x = pix_x / scalex
        pix_y = pix_y / scaley
        self.ctx.scale(scalex, scaley)

        self.ctx.set_source_rgb(0.0, 0.0, 0.0)
        if isbold:
            self.ctx.select_font_face("Arial", cairo.FONT_SLANT_NORMAL, cairo.FONT_WEIGHT_BOLD)
        else:
            self.ctx.select_font_face("Arial", cairo.FONT_SLANT_NORMAL, cairo.FONT_WEIGHT_NORMAL)
            
        self.ctx.set_font_size(pix_y*max_text_height)
        
        #trying to fit the text into the defined area
        x_bearing, y_bearing, width, height = self.ctx.text_extents(text)[:4]
        if width > max_text_width/(self.mapwidth*scalex): 
            x_bearing, y_bearing, width, height = self.ctx.text_extents(text[0]+"..")[:4]
            if width > max_text_width/(self.mapwidth*scalex): 
                self.ctx.scale(1.0/scalex, 1.0/scaley)
                return
            else:
                tl = 0
                while width < max_text_width/(self.mapwidth*scalex):
                    tl = tl + 1
                    x_bearing, y_bearing, width, height = self.ctx.text_extents(text[0:tl]+"..")[:4]
                
                text = text[0:(tl-1)] + ".."
                x_bearing, y_bearing, width, height = self.ctx.text_extents(text)[:4]
#        self.ctx.move_to(pix_x * x, pix_y * (y+height))
#        self.ctx.move_to(x - width / 2 - x_bearing, y - height / 2 - y_bearing)
        self.ctx.move_to(x*pix_x - x_bearing, y*pix_y - y_bearing)
        self.ctx.show_text(text)
        self.ctx.scale(1.0/scalex, 1.0/scaley)
#        self.surface.write_to_png ("test1233.png") # Output to PNG
        if len(text) == 0:
            print "another notext"
        
        if text [-2:] == '..':
            self.printText.__dict__['brokentextcount'] = self.printText.__dict__['brokentextcount'] + 1
            self.printText.__dict__['brokentextletters'] = self.printText.__dict__['brokentextletters'] + len(text) - 2
        else:
            self.printText.__dict__['fulltextcount'] = self.printText.__dict__['fulltextcount'] + 1
            self.printText.__dict__['fulltextletters'] = self.printText.__dict__['fulltextletters'] + len(text)

        
    def rotateFuzzyValue(self, value, step):
        assert (value<=1.0), "cannot rotate value"
        value = value + step
        value = value % 1.0
        return value
        
