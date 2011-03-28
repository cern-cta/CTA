'''
Created on Jun 4, 2010

holds a dictionary collecting properties of a Node (key-value pairs)

@author: kblaszcz
'''

class ViewNode(object):
    def __init__(self):
        self.treenode = None
        self.x = 0.0
        self.y = 0.0
        self.width = 0.0
        self.height = 0.0
        self.level = 0.0
        self.hasannex = False
        self.paddingsize = 0.0
        
        self.labeltext = ''
        self.labelfontsize = 0.0
        self.labeltextisbold = True
        self.labelheight = 0.0
        self.labelwidth = 0.0
        
        self.icon = False
        self.iconfile = ''
        self.iconcoords = {'x':0.0,'y':0.0,'width':0.0,'height':0.0}
        
        self.htmltooltiptext = ''
        self.fillcolor = {'r':0.0, 'g':0.0, 'b':0.0, 'a':0.0}
        self.strokecolor = {'r':0.0, 'g':0.0, 'b':0.0, 'a':0.0}
        self.strokesize = 0.0
        self.radiallight = {'brightness': 0.0, 'hue':0.0, 'opacity': 0.0 }
