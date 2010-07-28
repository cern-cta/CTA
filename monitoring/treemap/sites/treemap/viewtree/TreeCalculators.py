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
        vnode.setProperty('level', self.otree.getLevel())
        
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
        if pixels < 1: return
        startxold = startx
        nblines = 0 #counts how many lines are completed before the cleanup code starts
        
        #children at the current position, ordered from the biggest to smallest, the algorithm works only if children[i] <= children[i+1]
        children = self.otree.getSortedChildren()
        if len(children) <= 0: return
        
        #collect all nodes that can be displayed
        totalchildnodes = [] #for recursive traversal in the ObjectTree
        totalviewnodes = [] #to determine the size of each node
        
        areasum = 0.0 #sum of the rectangle areas for one line
        percentagesum = 0.0 #needed to calculate how many percent of the parent area are 100% of the line area
        line_collection = [] #Nodes collected for current line
        
        line_limits =  self.calc_per_line(startx, starty, width , height, children) #tells the number of items in each line
        
        for child in children:
            
            percentage = child.evaluate() # evaluation must be percentage of the parent area
            
            if percentage <= 0: #if child invisible
                continue
            
            area = percentage*pixels #needed area for that child in square pixels
            
            #if collecting items for current line is completed, add all items to TreeView except of the current one, 
            #the current item is collected for the next line
            if line_limits[nblines] == len(line_collection):
                #they all together have to be squeezed to same height and parent's width
                line_height = areasum/width 
         
                for ch in line_collection: #calculate the witdth of each child
                    percentage_of_line = ch.evaluate()/percentagesum #normalize percantage values relative to line
                    child_area = percentage_of_line * areasum #area in square pixels which the child will take in that line
                    child_width = child_area/line_height #height and area are known, now you can calculate width of the child
                    
                    #calculate final values
                    x = startx+spacesize
                    y = starty+spacesize
                    chwidth = child_width-2*spacesize
                    chheight = line_height-2*spacesize
                    
                    #store the calculated values
                    vn = ViewNode()
                    if 2.0 * headersize > (chheight-2):
                        vn.setProperty('headersize', 0.0)
                    else:
                        vn.setProperty('headersize', headersize)
                    vn.setProperty('treenode', ch)
                    vn.setProperty('spacesize', spacesize)
                    vn.setProperty('x', x)
                    vn.setProperty('y', y)
                    vn.setProperty('width', chwidth)
                    vn.setProperty('height', chheight)
                    vn.setProperty('level', self.otree.getLevel() + 1)
                    
                    totalchildnodes.append(ch)
                    totalviewnodes.append(vn)
                    viewtree.addChild(vn)
                    
                    startx = startx + child_width #fix start position for next child
                
                #calculate coordinates for the next line
                startx = startxold
                starty = starty + line_height
                
                #take care of the remaining child which belongs to the next line
                areasum = area
                percentagesum = percentage
                line_collection = [] #clear the children list for new line
                line_collection.append(child) #add the remaining child
                
                nblines = nblines + 1 #counts the number of lines 
                
            else: #number of items not reached yet, accumulate values and append child to collection 
                areasum = areasum + area
                percentagesum = percentagesum + percentage
                line_collection.append(child)
        
        #cleanup code: In case of unprocessed items in line_collection
        #if the precalculation is good that condition should always evaluate to False
        
        if line_collection:
            #line_collection, areasum, percentagesum have correct values from the previous loop
            line_height = areasum/width
            
            for ch in line_collection:
                percentage_of_line = ch.evaluate()/percentagesum
                child_area = percentage_of_line * areasum
                child_width = child_area/line_height
                
                #calculate final values
                x = startx+spacesize
                y = starty+spacesize
                chwidth = child_width-2*spacesize
                chheight = line_height-2*spacesize
                
                if ((chwidth <= 0) or (chheight <= 0)):
                    continue
                    
                #store the calculated values
                vn = ViewNode()
                if 2.0 * headersize > (chheight-2):
                    vn.setProperty('headersize', 0.0)
                else:
                    vn.setProperty('headersize', headersize)
                    
                vn.setProperty('treenode', ch)
                vn.setProperty('spacesize', spacesize)
                vn.setProperty('x', x)
                vn.setProperty('y', y)
                vn.setProperty('width', chwidth)
                vn.setProperty('height', chheight)
                vn.setProperty('level', self.otree.getLevel() + 1)
                    
                totalchildnodes.append(ch)
                totalviewnodes.append(vn)
                viewtree.addChild(vn)
                    
                startx = startx + child_width
            
            startx = startxold   
            starty = starty + line_height
            
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
    
    #simulation of the core algorithm
    def simulate(self, startx, starty, width , height, children, trigger_correction_ratio):
        
        #items per line
        items_per_line = []
        
        #calculate the area in square pixels
        pixels = float(height * width)
        #exclude everything that would be smaller than 1 pixel
        if pixels < 1: 
            return False, items_per_line.append(0)
        startxold = startx
        nblines = 0 #counts how many lines are completed before the cleanup code starts
        
        if len(children) <= 0: 
            return False, items_per_line.append(0)
        
        widthsum = 0.0 #accumulate width sum of the rectangles until you go over parents rectangle border
        areasum = 0.0 #sum of the rectangle areas for one line
        percentagesum = 0.0 #needed to calculate how many percent of the parent area are 100% of the line area
        inside_rectangle = [] #Nodes which fit to the line = nodes collection for the line
        
        for child in children:
            
            percentage = child.evaluate() # evaluation must be percentage of the parent area
            if percentage <= 0: #if child invisible
                continue
            
            area = percentage*pixels #needed area for that child in square pixels
            sqwidth = math.sqrt(area) #in best case you wish to have a square, so calculate the dimension of the ideal square
            
            #if additional square crosses the line of the parent
            if ((widthsum + sqwidth) > width):
                #they all together have to be squeezed to same height and parent's width
                line_height = areasum/width
         
                for ch in inside_rectangle: #calculate the witdth of each child
                    percentage_of_line = ch.evaluate()/percentagesum #normalize percantage values relative to line
                    child_area = percentage_of_line * areasum #area in square pixels which the child will take in that line
                    child_width = child_area/line_height #height and area are known, now you can calculate width of the child
                    
                    startx = startx + child_width #fix start position for next child
                
                #calculate coordinates for the next line
                startx = startxold
                starty = starty + line_height
                
                #always one more child is read to have the overflow condition true
                #that child belongs already to the next line, the values are already calculated so just use them
                widthsum = sqwidth
                areasum = area
                percentagesum = percentage
                items_per_line.append(len(inside_rectangle)) #save the current number of items in that line
                inside_rectangle = [] #clear the children list for new line
                inside_rectangle.append(child) #add the overflow child
                
                nblines = nblines + 1 #counts the number of lines 
                
            else: #square still fits into the line: accumulate values and append child to collection
                widthsum = widthsum + sqwidth 
                areasum = areasum + area
                percentagesum = percentagesum + percentage
                inside_rectangle.append(child)
        
        #cleanup code: The last node usually won't create an overflow which prevents "if ((widthsum + sqwidth) > width)" to evaluate true 
        #and leaves some items in inside_rectangle unprocessed
        
        correction = False
        
        if inside_rectangle:
            items_per_line.append(len(inside_rectangle))
            #inside_rectangle, widthsum, areasum, percentagesum have correct values from the previous loop
            line_height = areasum/width
            
            for ch in inside_rectangle:
                percentage_of_line = ch.evaluate()/percentagesum
                child_area = percentage_of_line * areasum
                child_width = child_area/line_height
                
                #see if the item is too flat. If so correction will be needed (it is usually the case)
                if child_width/line_height > trigger_correction_ratio :
                    correction = True

                startx = startx + child_width
            
            startx = startxold   
            starty = starty + line_height
        
        
        #returns information about the items (if one of them is too flat + information how many items per line appeared)
        if correction:
            return True, items_per_line
        else:
            return False, items_per_line
    
    #optimizes the number of items per line by pushing some of them to the last cleanup line. It makes the last line taller and the items more visible
    #Imagine a text processing program with the option justified text enabled. You are typing a line of words and pressing the enter key before the line ends
    #The text will be stretched to fill the whole line. If the last line has not enough words the last line won't be stretched in a good looking way,
    # so push enough words forward to add some words to the last line and make it look better.
    
    #the last item must be None if you call it from outside!
    def calc_per_line(self, startx, starty, width , height, children, corrected = None, trigger_correction_ratio = None, items_per_line = None):
        
        #read the recommended width/height ratio for last line rectangles 
        if trigger_correction_ratio is None:
            trigger_correction_ratio = self.calc_properties.getProperty('correction_ratio')

        #pushing one additional item per call by defining how many items less per line there are
        #that increases the number of items which will remain for the last line
        #returns information if pushing was successful and the new correction 
        
        def increase_correction(corr):
            if len(corr) == 1: return False, corr
            allequal = True
            for i in range(len(corr)-2): #-2 because: -1 for valid index, additional -1 for last line which is not allowed to push to new line and collects the remaining items instead
                if (corr[i] > corr[i+1]) and ((items_per_line[i+1] - corr[i+1])>0):
                    corr[i+1] = corr[i+1] + 1
                    return True, corr
                if corr[i] != corr[i+1]:
                    allequal = False
                   
            if allequal and ((items_per_line[0] - corr[0])>0):
                corr[0] = corr[0] + 1
            else:
                return False, corr
            
            return True, corr
        
        #calculate the area in square pixels
        pixels = float(height * width)
        #exclude everyhing that would be smaller than 1 pixel
        if pixels < 1: return -1, 0.0
        startxold = startx
        nblines = 0 #counts how many lines are completed before the cleanup code starts
        
        if len(children) <= 0: return -1, 0.0
        
        #simulate core algorithm to determine if correction is needed
        needs_correction = True
        if items_per_line is None:
            needs_correction, items_per_line = self.simulate( startx, starty, width , height, children, trigger_correction_ratio)
        
        #if correction not needed return items_per_line from the core algorithm
        if not needs_correction:
            return items_per_line
            
        #initialize corrected which tells you how many items less per line to take, that many items will be removed from the right of the line
        #if corrected already has content then it was an recursive call and it already has the right values
        if corrected is None:
            corrected = []
            for i in range (len(items_per_line)):
                corrected.append(0)

        widthsum = 0.0 #accumulate width sum of the rectangles until you go over parents rectangle border
        areasum = 0.0 #sum of the rectangle areas for one line
        percentagesum = 0.0 #needed to calculate how many percent of the parent area are 100% of the line area
        inside_rectangle = [] #Nodes which fit to the line
        ret_item_count = [] # recalculated items per line to return

        for child in children:
            
            percentage = child.evaluate() # evaluation must be percentage of the parent area
            if percentage <= 0: #if child invisible
                continue
            
            area = percentage*pixels #needed area for that child in square pixels
            sqwidth = math.sqrt(area) #in best case you wish to have a square, so calculate the dimension of the ideal square
            
            #if additional square crosses the line of the parent 
            #or if currently corrected number of squares is reached and it is not the last line (which is meant to be processed in the cleanup code section)
            if (((widthsum + sqwidth) > width) or ((len(inside_rectangle) ==  items_per_line[nblines] - corrected[nblines]))) and ((len(items_per_line)-1) != nblines):
                #they all together have to be squeezed to same height and parent's width
                
#                if ((len(items_per_line)-1) != nblines):
#                    areasum = areasum + area
#                    percentagesum = percentagesum + percentage
                
                line_height = areasum/width 
         
                for ch in inside_rectangle: #calculate the witdth of each child
                    percentage_of_line = ch.evaluate()/percentagesum #normalize percantage values relative to line
                    child_area = percentage_of_line * areasum #area in square pixels which the child will take in that line
                    child_width = child_area/line_height #height and area are known, now you can calculate width of the child
                    
                    startx = startx + child_width #fix start position for next child
                
                #calculate coordinates for the next line
                startx = startxold
                starty = starty + line_height
                
                #read one more child to have the overflow condition true
                #that child belongs already to the next line, the values are already calculated so just use them
                widthsum = sqwidth
                areasum = area
                percentagesum = percentage
                ret_item_count.append(len(inside_rectangle))
                inside_rectangle = [] #clear the children list for new line
                inside_rectangle.append(child) #add the overflow child
                
                nblines = nblines + 1 #counts the number of lines 
                
            else: #square still fits into the line: accumulate values and append child to collection
                widthsum = widthsum + sqwidth 
                areasum = areasum + area
                percentagesum = percentagesum + percentage
                inside_rectangle.append(child)
        
        #cleanup code: The last node usually won't create an overflow which prevents completing calculations of last line of children above
        if inside_rectangle:
            ret_item_count.append(len(inside_rectangle))
            #inside_rectangle, widthsum, areasum, percentagesum have correct values from the previous loop
            line_height = areasum/width
            
            for ch in inside_rectangle:
                percentage_of_line = ch.evaluate()/percentagesum
                child_area = percentage_of_line * areasum
                child_width = child_area/line_height
                
                #see if the item is too flat. If so correction will be needed (it is usually the case)
                if child_width/line_height > trigger_correction_ratio :
                    success, corrected = increase_correction(corrected)
                    if not success: break #if not sucessful it can't be optimized further
                    return self.calc_per_line(startx, starty, width , height, children, corrected, trigger_correction_ratio)

                startx = startx + child_width
            
            startx = startxold   
            starty = starty + line_height
        print 'return value: ', ret_item_count
        return ret_item_count
        
