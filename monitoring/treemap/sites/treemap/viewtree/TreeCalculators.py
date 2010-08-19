'''
Created on May 25, 2010

@author: kblaszcz
'''
from sites.treemap.viewtree.ViewTree import ViewTree
import math
from sites.treemap.viewtree.ViewNode import ViewNode
from sites.errors import ConfigError
from sites.treemap.defaultproperties.SquaredViewProperties import BasicViewTreeProps, ViewTreeCalculationProps, ViewTreeDesignProps

class SquaredTreemapCalculator(object):
    '''
    classdocs
    '''
    def __init__(self, otree, basic_properties = None, calc_properties = None):
        '''
        Constructor
        '''
        self.otree = otree
        
        assert(basic_properties is None or isinstance(basic_properties, BasicViewTreeProps)), "basic_properties wrong"
        self.basic_properties = basic_properties
        
        assert(calc_properties is None or isinstance(calc_properties, ViewTreeCalculationProps)), "calc_properties wrong"
        self.calc_properties = calc_properties
        
    def calculate(self):
        
        if self.basic_properties is None:
            self.basic_properties = BasicViewTreeProps()
        if self.calc_properties is None:
            self.calc_properties = ViewTreeCalculationProps(basic_properties = self.basic_properties)
            
        self.spacesize = self.calc_properties.getProperty('spacesize')
        self.minspacesize = self.calc_properties.getProperty('minspacesize')
        self.headersize = self.calc_properties.getProperty('headersize')

        width = self.basic_properties.getProperty('width')
        height = self.basic_properties.getProperty('height')
        self.otree.traveseToRoot()
        root = self.otree.getCurrentObject()
        
        viewtree = ViewTree(basic_properties = self.basic_properties, calc_properties = self.calc_properties)
        self.basic_properties.setProperty('viewtree',viewtree)
        self.calc_properties.setProperty('objecttree',self.otree)
        
        vnode = ViewNode()
        vnode.setProperty('treenode', root)
        vnode.setProperty('headersize', self.headersize)
        vnode.setProperty('spacesize', self.spacesize)
        vnode.setProperty('x', 0)
        vnode.setProperty('y', 0)
        vnode.setProperty('width', width)
        vnode.setProperty('height', height)
        vnode.setProperty('level', self.otree.getCurrentObject().getDepth())
        vnode.setProperty('nbchildren', self.otree.countChildren())
        
        viewtree.setRoot(vnode)
        viewtree.traverseInto(vnode)
        
        x = self.spacesize
        y = self.spacesize + self.headersize
        width = width - 2*self.spacesize
        height = height - 2*self.spacesize-self.headersize
        
        if self.spacesize > self.minspacesize: self.spacesize = self.spacesize - 1
        self.calculateRecursion(x, y, width ,height , viewtree, self.spacesize, self.minspacesize, self.headersize)
        
        return viewtree
        
    #line: The items are ordered graphically in lines of equally tall squares
    #it is like a line of text but with rectangles instead of letters
    def calculateRecursion(self, startx, starty, width ,height, viewtree, spacesize, minspacesize, headersize):

        #calculate the area in square pixels
        pixels = float(height * width)
        
        #exclude everyhing that would be smaller than 1 pixel
        if pixels < (1.0+spacesize)*(1.0+spacesize): return
        startxold = startx
        startyold = starty
        nblines = 0 #counts how many lines are completed before the cleanup code starts
        
        #go deeper only if parent had a header
        parenthsize = viewtree.getCurrentObject().getProperty('headersize')
        if parenthsize <= 0.0:
            return
        
        #children at the current position, ordered from the biggest to smallest, the algorithm works only if children[i] <= children[i+1]
        children = self.otree.getSortedChildren()
        if len(children) <= 0: return
        
        #collect all nodes that can be displayed
        totalchildnodes = [] #for recursive traversal in the ObjectTree
        totalviewnodes = [] #to determine the size of each node
        
        linesum = 0.0 #accumulate line length sum of the rectangles until you go over parents rectangle border
        areasum = 0.0 #sum of the rectangle areas for one line
        percentagesum = 0.0 #needed to calculate how many percent of the parent area are 100% of the line area
        line_collection = [] #Nodes collected for current line
        
        VERTICAL, HORIZONTAL = range(2)
        if width > height:
            direction = VERTICAL
            linelen = height
        else:
            direction = HORIZONTAL
            linelen = width
            
        
        for child in children:
            
            percentage = child.evaluate() # evaluation must be percentage of the parent area
            
            if percentage <= 0: #if child invisible
                continue
            
            area = percentage*pixels #needed area for that child in square pixels
            sqwidth = math.sqrt(area) #in best case you wish to have a square, so calculate the dimension of the ideal square
            
            #if collecting items for current line is completed, because the next addition would go over the border
            #add all items to TreeView except of the current overflow one
            if (((linesum + sqwidth) > linelen)):
                #save the coordinates where the line starts
                xbeginning = startx
                ybeginning = starty
                #they all together have to be squeezed to same height and parent's size
                line_height = areasum/linelen 
                x,y,chheight,chwidth = 0.0, 0.0, 0.0, 0.0
         
                for ch in line_collection: #calculate the witdth of each child
                    percentage_of_line = ch.evaluate()/percentagesum #normalize percantage values relative to line
                    child_area = percentage_of_line * areasum #area in square pixels which the child will take in that line
                    child_width = child_area/line_height #height and area are known, now you can calculate width of the child
                    
                    #calculate final values
                    x = startx + spacesize
                    y = starty + spacesize
                    
                    if(direction == HORIZONTAL):
                        chwidth = child_width 
                        chheight = line_height
                    else:
                        chheight = child_width
                        chwidth = line_height
                    
                    #store the calculated values
                    vn = ViewNode()
                    if 2.0 * headersize > (chheight-2.0):
                        vn.setProperty('headersize', 0.0)
                    else:
                        vn.setProperty('headersize', headersize)
                        
                    vn.setProperty('treenode', ch)
                    vn.setProperty('spacesize', spacesize)
                    vn.setProperty('x', x)
                    vn.setProperty('y', y)
                    vn.setProperty('width', chwidth - 2.0 * spacesize)
                    vn.setProperty('height', chheight - 2.0 * spacesize)
                    vn.setProperty('level', self.otree.getCurrentObject().getDepth() + 1)
                    
                    currently_inscope = self.otree.getCurrentObject()
                    self.otree.traverseInto(child)
                    vn.setProperty('nbchildren', self.otree.countChildren())
                    self.otree.traverseInto(currently_inscope)
                    
                    totalchildnodes.append(ch)
                    totalviewnodes.append(vn)
                    viewtree.addChild(vn)
                    
                    #fix start position for next child
                    if(direction == HORIZONTAL):
                        startx = startx + child_width
                    else:
                        starty = starty + child_width
                
                #see what the ratio of the remaining area is
                remaining_ratio = 1.0        
                if(direction == HORIZONTAL):
                    remaining_ratio = (height-(starty-startyold)-chheight)/(width-(xbeginning - startxold))
                else:
                    remaining_ratio = (height-(ybeginning - startyold))/(width-(startx-startxold)-chwidth)
        
                #calculate start coordinates and direction for the next line
                if (direction == HORIZONTAL):
                    if remaining_ratio < 1.0:
                        direction = VERTICAL
                        linelen = height - (starty-startyold) -chheight 
                        startx = xbeginning
                        starty = starty + chheight
                    else:
                        starty = starty + chheight
                        startx = xbeginning
                        
                elif (direction == VERTICAL):
                    if remaining_ratio >= 1.0:
                        direction = HORIZONTAL
                        linelen = width - (startx-startxold) -chwidth
                        starty = ybeginning
                        startx = startx + chwidth
                    else:
                        starty = ybeginning
                        startx = startx + chwidth
                
                #take care of the remaining child which belongs to the next line
                linesum = sqwidth
                areasum = area
                percentagesum = percentage
                line_collection = [] #clear the children list for new line
                line_collection.append(child) #add the remaining child
                
                nblines = nblines + 1 #counts the number of lines 
                
            else: #number of items not reached yet, accumulate values and append child to collection 
                linesum = linesum + sqwidth 
                areasum = areasum + area
                percentagesum = percentagesum + percentage
                line_collection.append(child)
        
        #cleanup code: In case of unprocessed items in line_collection
        if line_collection:
            #assuming line_collection, areasum, percentagesum have correct values from the previous loop
            xbeginning = startx
            ybeginning = starty
            
            line_height = areasum/linelen
            x,y = 0.0, 0.0
            
            for ch in line_collection:
                percentage_of_line = ch.evaluate()/percentagesum
                child_area = percentage_of_line * areasum
                child_width = child_area/line_height
                
                #calculate final values
                x = startx+spacesize
                y = starty+spacesize
                
                if(direction == HORIZONTAL):
                    chwidth = child_width
                    chheight = line_height
                else:
                    chheight = child_width
                    chwidth = line_height
                
                if (((chwidth-2.0*spacesize) <= 0) or ((chheight-2.0*spacesize) <= 0)):
                    continue
                    
                #store the calculated values
                vn = ViewNode()
                if 2.0 * headersize > (chheight-2.0):
                    vn.setProperty('headersize', 0.0)
                else:
                    vn.setProperty('headersize', headersize)
                    
                vn.setProperty('treenode', ch)
                vn.setProperty('spacesize', spacesize)
                vn.setProperty('x', x)
                vn.setProperty('y', y)
                vn.setProperty('width', chwidth-2.0*spacesize)
                vn.setProperty('height', chheight-2.0*spacesize)
                vn.setProperty('level', self.otree.getCurrentObject().getDepth() + 1)
                
                currently_inscope = self.otree.getCurrentObject()
                self.otree.traverseInto(child)
                vn.setProperty('nbchildren', self.otree.countChildren())
                self.otree.traverseInto(currently_inscope)
                    
                totalchildnodes.append(ch)
                totalviewnodes.append(vn)
                viewtree.addChild(vn)
                    
                #fix start position for next child
                if(direction == HORIZONTAL):
                    startx = startx + child_width
                else:
                    starty = starty + child_width
            
        #now that parent is ready, do recursion
        
        if spacesize > minspacesize: spacesize = spacesize - 1
        
        for i in range(len(totalchildnodes)):
            self.otree.traverseInto(totalchildnodes[i])
            viewtree.traverseInto(totalviewnodes[i])
            
            #see if it is big enough to have a header and do recursion
            hsize = totalviewnodes[i].getProperty('headersize')
            if hsize <= 0.0:
                self.calculateRecursion(totalviewnodes[i].getProperty('x') + 1, totalviewnodes[i].getProperty('y')+1, totalviewnodes[i].getProperty('width')-2 ,totalviewnodes[i].getProperty('height')-2, viewtree, spacesize, minspacesize, headersize)
            else:
                self.calculateRecursion(totalviewnodes[i].getProperty('x') + 1, totalviewnodes[i].getProperty('y') + 1 + hsize, totalviewnodes[i].getProperty('width')-2 ,totalviewnodes[i].getProperty('height')-2 - hsize, viewtree, spacesize, minspacesize, hsize)
                
            self.otree.traverseBack()
            viewtree.traverseBack()