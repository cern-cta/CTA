class Properties(object):
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


class BasicViewTreeProps(Properties):
    '''
    classdocs
    '''

    def __init__(self, width=800, height = 600, tree = None):
        '''
        Constructor
        '''
        Properties.__init__(self)
        self.props['width'] = width
        self.props['height'] = height
        self.props['viewtree'] = tree

class ViewTreeCalculationProps(Properties):
    '''
    classdocsheight
    '''

    def __init__(self, basic_properties, headersize = 12.0, spacesize = 3.0, minspacesize = 1.0, correction_ratio = 2.0, tree = None):
        '''
        Constructor
        '''
        assert (basic_properties is not None or isinstance(basic_properties, BasicViewTreeProps))
        
        Properties.__init__(self)
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
            
        if correction_ratio >= 0.5:
            self.props['correction_ratio'] = correction_ratio
        else:
            self.props['correction_ratio'] = 2.0
            
        self.props['objecttree'] = tree

        
class ViewTreeDesignProps(Properties):
    '''
    classdocs
    '''

    def __init__(self, calc_properties, inbordersize = 0.0, headertextsize = 12.0, radiallightbrightness = 0.4, tree = None, htextisbold = True):
        '''
        Constructor
        '''
        assert(calc_properties is not None or isinstance(calc_properties, ViewTreeCalculationProps)), "calc_properties wrong"
        Properties.__init__(self)
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
            
        if headertextsize > self.headersize - inbordersize:
            self.props['headertextsize'] = self.headersize - inbordersize
        else:
            self.props['headertextsize'] = headertextsize
            
        if radiallightbrightness > 1.0 or radiallightbrightness < 0.0:
            self.props['radiallightbrightness'] = 0.4
        else:
            self.props['radiallightbrightness'] = radiallightbrightness
        
        self.props['viewtree'] = tree
        
        self.props['headertext.isbold'] = htextisbold

        