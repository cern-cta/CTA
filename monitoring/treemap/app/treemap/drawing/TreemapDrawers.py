'''
Created on Jun 10, 2010

This class reads ViewNodes from a ViewTree and draws them depending on their properties.
The resulting output is written into a png file.

@author: kblaszcz
'''
from app.tools.ColorFunctions import *
from app.treemap.defaultproperties.TreeMapProperties import treemap_props
from app.treemap.viewtree.ViewTree import ViewTree
import cairo
import math
import rsvg
import settings

class SquarifiedTreemapDrawer(object):
    '''
    classdocs
    '''

    def __init__(self, treemap_props):
        '''
        Constructor
        '''
        self.vtree = treemap_props['viewtree']
        assert (isinstance(self.vtree, ViewTree)) 
        
        self.mapwidth = treemap_props['pxwidth']
        self.mapheight = treemap_props['pxheight']
    
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
        
        self.drawRect(root.x, root.y, root.width, root.height, root.strokesize, root.level, root.fillcolor, root.strokecolor, root.radiallight)
        if treemap_props['labels']:
            iconx, icony, iconwidth, iconheight = root.iconcoords['x'],root.iconcoords['y'],root.iconcoords['width'], root.iconcoords['height']
            iconfile = root.iconfile
            self.printSVG(iconfile,iconx, icony, iconwidth, iconheight)
            self.printText(root.treenode.getObject().__str__(), root.x + root.strokesize, root.y + root.strokesize, root.width-2* root.strokesize - iconwidth, root.labelfontsize, root.labeltextisbold)
        self.drawRecursion()
        
        self.surface.write_to_png (filename)
        
        print "brokentextcount ", self.printText.__dict__['brokentextcount']
        print "fulltextcount ", self.printText.__dict__['fulltextcount']
        print "brokentextletters ", self.printText.__dict__['brokentextletters']
        print "fulltextletters ", self.printText.__dict__['fulltextletters']
        
    def drawRecursion(self):
        children = self.vtree.getChildren()
        
        for child in children: #(self, text, x, y, max_text_width, max_text_height)
            strokesize = child.strokesize
            
            self.drawRect(child.x, child.y, child.width, child.height, strokesize, child.level, child.fillcolor, child.strokecolor, child.radiallight)
                    
            if child.labelheight > 0.0:
                txt = child.labeltext
                iconx, icony, iconwidth, iconheight = child.iconcoords['x'],child.iconcoords['y'],child.iconcoords['width'],child.iconcoords['height']
                iconfile = child.iconfile
                self.printSVG(iconfile,iconx, icony, iconwidth, iconheight)
                self.printText(txt, child.x + strokesize, child.y + strokesize, child.width-2*strokesize - iconwidth, child.labelfontsize, child.labeltextisbold)
                
            
            self.vtree.traverseIntoChild(child)
            self.drawRecursion()
            self.vtree.traverseBack()
        
        
    def drawRect(self, x, y, subwidth, subheight, bordersize, level, fillcolor, strokecolor, radiallight):
#        x=round(x,0)
#        y=round(y,0)
#        subwidth=round(subwidth,0)
#        subheight=round(subheight,0)
#        
#        if subwidth == 0: return

        if subwidth <= 1.0 or subheight <= 1.0: return
        
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
#        self.ctx.set_source_surface(image.,0.0,0.0)
#        image.render_cairo(self.ctx)

#        s1.write_to_png("/home/kblaszcz/Desktop/atest.png")
#        self.ctx.set_source_surface(s1)     

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
        
    def printSVG(self, svgfilename = '', x = 0.0, y=0.0, width=0.0, height=0.0):
        if width == 0.0 or height == 0.0: return
        x = float(x)
        y = float(y)
        width = float(width)
        height = float(height)
        if svgfilename == '': return
        self.ctx.save()#backup transformation matrix, scale and translate will change it
        self.ctx.scale (1.0/self.mapwidth, 1.0/self.mapheight)
        
        image = rsvg.Handle(svgfilename)
        imgwidth, imgheight = image.get_dimension_data()[:2]
        imgwidth = float(imgwidth)
        imgheight = float(imgheight)
        assert(imgwidth>0 and imgheight>0)
        if (width == 0 and height == 0):
            scalex = 1.0
            scaley = 1.0
        else:
            scalex = width/imgwidth
            scaley = height/imgheight
        self.ctx.scale(scalex, scaley)
        self.ctx.translate(x/scalex,y/scaley)
        
        image.render_cairo(self.ctx)
        #restore transformation matrix
        self.ctx.restore()
        
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
        
