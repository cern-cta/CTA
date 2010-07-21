'''
Created on Jun 10, 2010

@author: kblaszcz
'''
import cairo
import math
from sites.tools.HsvConverter import *
from sites.treemap.defaultproperties.SquaredViewProperties import BasicViewTreeProps, ViewTreeCalculationProps, ViewTreeDesignProps
from sites.treemap.viewtree.ViewTree import ViewTree

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
        
    def drawTreemap(self, filename):

        self.vtree.traveseToRoot()
        root = self.vtree.getCurrentObject()
        
        self.drawRect(root.getProperty('x'), root.getProperty('y'), root.getProperty('width'), root.getProperty('height'), root.getProperty('inbordersize'), root.getProperty('level'), root.getProperty('fillcolor'), root.getProperty('strokecolor'), root.getProperty('radiallight'))
        self.printText(root.getProperty('treenode').getObject().__str__(), root.getProperty('x') + root.getProperty('inbordersize'), root.getProperty('y') + root.getProperty('inbordersize'), root.getProperty('width')-2* root.getProperty('inbordersize'), root.getProperty('headertextsize'))
        
        self.drawRecursion()
        
        self.surface.write_to_png (filename)
        
    def drawRecursion(self):
        children = self.vtree.getChildren()
        
        for child in children: #(self, text, x, y, max_text_width, max_text_height)
            inbordersize = child.getProperty('inbordersize')
            
            self.drawRect(child.getProperty('x'), child.getProperty('y'), child.getProperty('width'), child.getProperty('height'), inbordersize, child.getProperty('level'), child.getProperty('fillcolor'), child.getProperty('strokecolor'), child.getProperty('radiallight'))
            if child.getProperty('headersize') > 0.0:
                txt = child.getProperty('treenode').getObject().name.__str__()
                if child.getProperty('treenode').getObject().__class__.__name__ == 'Dirs':
                    txt = "/" + txt
                self.printText(txt, child.getProperty('x') + inbordersize, child.getProperty('y') + inbordersize, child.getProperty('width')-2*inbordersize, child.getProperty('headertextsize'))
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
        
        #bigger rectangle
        pat = cairo.LinearGradient (x0, y0, x0+width, y0+height)
        r,g,b = strokecolor['r'], strokecolor['g'], strokecolor['b']
        pat.add_color_stop_rgba (0, r, g, b, 1.0) #
        
        self.ctx.rectangle (x0,y0,width,height) # Rectangle(x0, y0, x1, y1)
        self.ctx.set_source (pat)
        self.ctx.fill ()

        #smaller rectangle coordinates
        x0 = x0+pix_x*bordersize
        y0 = y0+pix_y*bordersize
        width = width-2*pix_x*bordersize
        height = height-2*pix_y*bordersize
        
        #gradient inside of smaller rectangle
        r,g,b = fillcolor['r'], fillcolor['g'], fillcolor['b']
        a = 1.0
#        r,g,b = addValueToRgb(r,g,b,0.4)
        pat = cairo.LinearGradient (x0, y0, x0+width, y0)
#        pat.add_color_stop_rgba (0.0, r, g, b, 1.0)
#        r,g,b = fillcolor['r'], fillcolor['g'], fillcolor['b']
        pat.add_color_stop_rgba (0.0, r, g ,b , 1.0) # last number is opacity
        pat.add_color_stop_rgba (1.0, r, g ,b , 1.0) # last number is opacity
        
        
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
        
#        ctx.stroke ()

#        ctx.translate (0.1, 0.1) # Changing the current transformation matrix
#        
#        ctx.move_to (0, 0)
#        ctx.arc (0.2, 0.1, 0.1, -math.pi/2, 0) # Arc(cx, cy, radius, start_angle, stop_angle)
#        ctx.line_to (0.5, 0.1) # Line to (x,y)
#        ctx.curve_to (0.5, 0.2, 0.5, 0.4, 0.2, 0.8) # Curve(x1, y1, x2, y2, x3, y3)
#        ctx.close_path ()
#        
#        ctx.set_source_rgb (0.3, 0.2, 0.5) # Solid color
#        ctx.set_line_width (0.02)
#        ctx.stroke ()
        
#        self.surface.write_to_png ("test1233.png") # Output to PNG
        
    def printText(self, text, x, y, max_text_width, max_text_height): #text, x, y, subwidth, subheight, bordersize, level):
        pix_x = 1.0/self.mapwidth
        pix_y = 1.0/self.mapheight
#        
#        x0 = pix_x*x
#        y0 = pix_y*y
#        width = pix_x*(subwidth)
#        height = pix_y*(subheight)
        self.ctx.set_source_rgb(0.0, 0.0, 0.0)
        self.ctx.select_font_face("Serif", cairo.FONT_SLANT_NORMAL, cairo.FONT_WEIGHT_BOLD)
        self.ctx.set_font_size(pix_y*max_text_height)
        x_bearing, y_bearing, width, height = self.ctx.text_extents(text)[:4]
        if width > max_text_width/self.mapwidth: return
#        self.ctx.move_to(pix_x * x, pix_y * (y+height))
#        self.ctx.move_to(x - width / 2 - x_bearing, y - height / 2 - y_bearing)
        self.ctx.move_to(x*pix_x - x_bearing, y*pix_y - y_bearing)
        self.ctx.show_text(text)
        
#        self.surface.write_to_png ("test1233.png") # Output to PNG

        
    def rotateFuzzyValue(self, value, step):
        assert (value<=1.0), "cannot rotate value"
        value = value + step
        value = value % 1.0
        return value
        
