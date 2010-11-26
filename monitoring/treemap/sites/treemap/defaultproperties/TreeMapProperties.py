'''
Created on Jul 13, 2010
collections of key-value pairs which describe various parameters during processing stages of TreeMap generation
The classes perform some additional validiation beyond pure mapping them
All the properties will be set for every node individually, even if in some cases they don't differ

BasicViewTreeProps:
width - width of the TreeMap in px
height - height of the TreeMap in px
tree - calculated viewtree, set if already known, otherwise value is None

ViewTreeCalculationProps:
basic_properties - reference to BasicViewTreeProps
headersize - size of the rectangle head where text is displayed
spacesize - space between the rectangles in px (in the current implementation this value is decreased each level)
minspacesize - the minimum space between rectangles in px
tree - calculated viewtree, as soon as known

ViewTreeDesignProps:
calc_properties - reference to ViewTreeCalculationProps
inbordersize - size of the rectangle border line (default is 0, ie. it is greater than 0 for Annex)
headerfontsize - size of the font for the text displayed in rectangle header in px
radiallightbrightness - "the strength of the light source shining on the rectangle", value between 0.0 and 1.0
tree - calculated viewtree
htextisbold - defines if the text in the rectangle header should be bold

@author: kblaszcz
'''

class PropertiesBase(object):
    '''
    classdocs
    '''
    def __init__(self):
        '''
        Constructor
        '''
        self.props = {}
        
    def addProperties(self, propdict):
        assert(isinstance (propdict, dict))
        self.props.update(propdict)
        
    def setProperty (self, key, value):
        self.props[key] = value
        
    def getProperty (self, key):
        try:
            return self.props[key]
        except KeyError:
            return None
    
#    def getObject(self):
#        return self.obj
    
    def __str__(self):
        return self.props.__str__()


class BasicViewTreeProps(PropertiesBase):
    '''
    classdocs
    '''

    def __init__(self, width=800, height = 600, tree = None):
        '''
        Constructor
        '''
        PropertiesBase.__init__(self)
        self.props['width'] = width
        self.props['height'] = height
        self.props['viewtree'] = tree

class ViewTreeCalculationProps(PropertiesBase):
    '''
    classdocsheight
    '''

    def __init__(self, basic_properties, headersize = 12.0, spacesize = 3.0, minspacesize = 1.0, tree = None):
        '''
        Constructor
        '''
        assert (basic_properties is not None or isinstance(basic_properties, BasicViewTreeProps))
        
        PropertiesBase.__init__(self)
        self.props['basic_properties'] = basic_properties
        
        self.height = basic_properties.getProperty('height')
        self.width = basic_properties.getProperty('width')
        
        if headersize > self.height:
            self.props['headersize'] = self.height * 0.02
        else:
            self.props['headersize'] = headersize
            
        if spacesize > min(self.width, self.height) * 0.25:
            self.props['spacesize'] = 4.0
        else:
            self.props['spacesize'] = spacesize
            
        if minspacesize <= spacesize and minspacesize >= 0.0:
            self.props['minspacesize'] = minspacesize
        else:
            self.props['minspacesize'] = spacesize
            
        self.props['objecttree'] = tree

        
class ViewTreeDesignProps(PropertiesBase):
    '''
    classdocs
    '''

    def __init__(self, calc_properties, inbordersize = 0.0, headerfontsize = 12.0, radiallightbrightness = 0.4, tree = None, htextisbold = True):
        '''
        Constructor
        '''
        assert(calc_properties is not None or isinstance(calc_properties, ViewTreeCalculationProps)), "calc_properties wrong"
        PropertiesBase.__init__(self)
        self.props['calc_properties'] = calc_properties
        
        self.props['basic_properties'] = calc_properties.getProperty('basic_properties')
        basic_properties = self.props['basic_properties']
        assert(basic_properties is not None or isinstance(self.basic_properties, BasicViewTreeProps)), "basic_properties wrong"
        
        self.headersize  = self.props['calc_properties'].getProperty('headersize')
        self.width = self.props['basic_properties'].getProperty('width')
        self.height = self.props['basic_properties'].getProperty('height')
            
        if inbordersize > min(self.width, self.height) * 0.1:
            self.props['inbordersize'] = 1.0
        else:
            self.props['inbordersize'] = inbordersize
            
        if headerfontsize > self.headersize - inbordersize:
            self.props['headerfontsize'] = self.headersize - inbordersize
        else:
            self.props['headerfontsize'] = headerfontsize
            
        if radiallightbrightness > 1.0 or radiallightbrightness < 0.0:
            self.props['radiallightbrightness'] = 0.4
        else:
            self.props['radiallightbrightness'] = radiallightbrightness
        
        self.props['viewtree'] = tree
        
        self.props['headertext.isbold'] = htextisbold

        