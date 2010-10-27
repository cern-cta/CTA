'''
Created on May 19, 2010

@author: kblaszcz
'''

from sites import settings
from sites.tools.StatusTools import generateStatusFile
from sites.treemap.objecttree.Annex import Annex
from sites.treemap.objecttree.ObjectTree import ObjectTree
from sites.treemap.objecttree.Postprocessors import *
from sites.treemap.objecttree.TreeNode import TreeNode
from sites.treemap.objecttree.TreeRules import LevelRules
import inspect

class TreeBuilder(object):
    '''
    classdocs
    '''


    def __init__(self, levelrules):
        '''
        Constructor
        '''
        self.rules = levelrules
        
        #imagine it would be possible to divide the root area in equally big squares
        #max_tree_leafes defines how many rectangles there are maximum allowed to divide that area
        self.max_tree_leafes = 1500
        
        #smallest accepted percentage of the area
        #if the node evaluates below that value the child will be considered as not worth going deeper
        self.lowest_area_factor = 1.0/self.max_tree_leafes
        
        #if the number of subitems is bigger than max_items_to_read_initial * the current area_factor
        #the child will be considered as not worth going deeper
        self.max_items_to_read_initial = 500
        
        #to avoid recursions over thousands of child items
        self.max_items_for_recursion = 80
        
    def generateObjectTree(self, rootobject, statusfilename):
        tree = ObjectTree()
		
        classname = rootobject.__class__.__name__
        level = 0
        rootisannex = False
        
        if classname == 'Annex':
            rootisannex = True
        try:
            parentmethodname =  self.rules.getParentMethodNameFor(level, classname)
        except KeyError:
            raise Exception("no parentmethod for " + classname + " found.")
        
        fparam = self.rules.getParamFor(level, classname)
        rootevalcolumn = self.rules.getColumnNameFor(0, classname)
        
        rootobjnode = TreeNode(rootobject, rootevalcolumn, parentmethodname, fparam, 0)
#        rootobjnode.metainfo = {'minsibling': rootobjnode.getEvalValue(), 'maxsibling':rootobjnode.getEvalValue()}
        
        tree.setRoot(rootobject, parentmethodname, fparam, rootevalcolumn, rootobjnode.getEvalValue())
        
        self.addChildrenRecursion(tree, level, tree.getRoot(), 1.0, rootisannex, statusfilename, None)
        
        return tree
    
#    count = 0
    chcount = 0
    
    def addChildrenRecursion(self, tree, level, parent, area_factor, rootisannex, statusfilename, nbrootchildren):
        max = float('inf')*(-1)
        min = float('inf')
#        metainfo = {}

        if area_factor < self.lowest_area_factor: return
        if not self.rules.indexIsValid(level + 1): return
        
        enableannex = True
        
#        self.count = self.count + 1
        #if not (self.count % 1000) :
        #print self.count, parent.getObject()
        
        nested_object = parent.getObject()

        classname = nested_object.__class__.__name__
        
        try:
            countmethodname = self.rules.getCountMethodNameFor(level, classname)
        except KeyError:
            return
        
        nbchildren = nested_object.__class__.__dict__[countmethodname](nested_object)
        if nbrootchildren is None: nbrootchildren= tree.getRoot().getObject().__class__.__dict__[countmethodname](tree.getRoot().getObject())

        if nbchildren <= 0 or (nbchildren > (self.max_items_to_read_initial * area_factor) and level > 0):
            return
        
        methodname = self.rules.getMethodNameFor(level, classname)

        children = list(nested_object.__class__.__dict__[methodname](nested_object))

        if len(children) == 0:
            return
        else:
            
            self.chcount = self.chcount + len(children)
            print level, self.chcount, parent.getObject()
            

            #this will always overestimate
            #self.max_tree_leafes doesn't apply to root
            if(nbrootchildren > self.max_tree_leafes):
                max_items = self.max_tree_leafes + nbrootchildren
            else:
                max_items = self.max_tree_leafes
                
            status = ((float(self.chcount)/float(max_items))) #adding nbchildren because root will always be read
            generateStatusFile(statusfilename, status)

            evalsum = 0.0
            thechild = None
            
            annexevalsum = 0.0
            
            treenodechildren = []
            
            #calculate sum of all entries and keep the generated Treenodes in treenodechildren
            for child in children:
                chclassname = child.__class__.__name__
                
                try:
                    chcolumnname = self.rules.getColumnNameFor(level + 1, chclassname)
                    chparam = self.rules.getParamFor(level + 1, chclassname)
                    parentmethodname =  self.rules.getParentMethodNameFor(level + 1, chclassname)
                    postprocessname =  self.rules.getPostProcessorNameFor(level + 1, chclassname)
                except KeyError:
                    return
                
                thechild = TreeNode(child, chcolumnname, parentmethodname, chparam, level + 1)
                evl = thechild.evaluate()
                if(evl <= 0): continue #ignore zeroes
                
                if evl < min: min = evl
                if evl > max: max = evl
                
                treenodechildren.append(thechild)

                evalsum = evalsum + evl
            
#            metainfo['minsibling'] = min
#            metainfo['maxsibling'] = max

            if evalsum <= 0: 
                tree.deleteChildren()
                return
            
            evalsum = 0
            #set PostProcessor to all children
            for thechild in treenodechildren:
                chclassname = thechild.getObject().__class__.__name__
                postprocessname =  self.rules.getPostProcessorNameFor(level + 1, chclassname)
                postprocessobject = None
                
                if postprocessname is not None:
                    if(postprocessname == 'SubstractMinPostProcessor'):
                        postprocessobject = SubstractMinPostProcessor(min)
#                    elif(postprocessname == 'InversePercentagePostProcessor'):
#                        postprocessobject = InversePercentagePostProcessor(max)
                    elif(postprocessname == 'LogAndSubstractMinPostProcessor'):
                        postprocessobject = LogAndSubstractMinPostProcessor(min) 
                    else:
                        command = "postprocessobject = " + postprocessname + "()"
                        exec(command)
                        pass
                
                thechild.setPostProcessorObject(postprocessobject)
                evalsum = evalsum + thechild.getEvalValue()
                
            if evalsum <= 0: 
                tree.deleteChildren()
                return
                
            for thechild in treenodechildren:     
                thechild.setSiblingsSum(evalsum)
            
            for child in treenodechildren:
                if (child.evaluate()*area_factor) >= self.lowest_area_factor:
                    tree.addTreeNodeChild(child)
                else:
                    annexevalsum = annexevalsum + child.getEvalValue()
                    
#            if annexevalsum == evalsum:
#                tree.deleteChildren()
#                return
            
            childnodes = tree.getChildren()
#            annexchildnodes = tree.getChildren()[:]
            
#            do the annex thing only if annex enabled      
            if not enableannex:
                evalsum = evalsum - annexevalsum
                annexevalsum = 0
                for thechild in childnodes:
                    thechild.setSiblingsSum(evalsum)
            
            #do the recursion if not too many children
            if len(childnodes) < self.max_items_for_recursion:
                for thechild in childnodes:
                    tree.traverseInto(thechild)
                    self.addChildrenRecursion(tree, level + 1, thechild, area_factor * thechild.evaluate(), False, statusfilename, nbrootchildren)
                    tree.traverseBack()
            
            #[:] to copy the content 
            #childnodes is a direct reference to the tree content, which we don't want to have in Annex
            #if there is no copy excludedchildren are the same as childnodes, so annexnode = tree.addChild(annexchild... would also add an item to Annex
            childnodes = tree.getChildren()[:]
            
            #add the annex object representing the rest of the children
            if(annexevalsum > 0):
                
                annexdepth = 0
                if(rootisannex):
                    annexdepth = tree.getRoot().getObject().getDepth() + 1
                    
                #create Annex as TreeNode
                annexchild = Annex(self.rules, level, nested_object, childnodes, annexdepth)
                annexchild.evaluation = annexevalsum
                chclassname = annexchild.__class__.__name__
                chcolumnname = self.rules.getColumnNameFor(level, chclassname)
                chparam = self.rules.getParamFor(level, chclassname)
                parentmethodname =  self.rules.getParentMethodNameFor(level, chclassname)
                
                annexnode = tree.addChild(annexchild, chcolumnname, parentmethodname, chparam)
                annexnode.setSiblingsSum(evalsum)
#                annexnode.metainfo = metainfo
                             
def print_tabs(n):
    for i in range(n):
        print "   ",
                
                    