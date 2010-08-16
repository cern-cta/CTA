'''
Created on May 19, 2010

@author: kblaszcz
'''

from sites.treemap.objecttree.Annex import Annex
from sites.treemap.objecttree.ObjectTree import ObjectTree
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
        max_tree_leafes = 1500
        
        #smallest accepted percentage of the area
        #if the node evaluates below that value the child will be considered as not worth going deeper
        self.lowest_area_factor = 1.0/max_tree_leafes
        
        #if the number of subitems is bigger than max_items_to_read_initial * the current area_factor
        #the child will be considered as not worth going deeper
        self.max_items_to_read_initial = 500
        
        #to avoid recursions over thousands of child items
        self.max_items_for_recursion = 80
        
    def generateObjectTree(self, rootobject, fparam = None):
        tree = ObjectTree()
        
        modulename = inspect.getmodule(rootobject).__name__
        classname = rootobject.__class__.__name__
        level = 0
        
        parentmethodname =  self.rules.getParentMethodNameFor(level, modulename, classname)
        
        tree.setRoot(rootobject, parentmethodname, fparam)
        
        self.addChildrenRecursion(tree, level, tree.getRoot(), 1.0)
        
        return tree
    
#    count = 0
    chcount = 0
    
    def addChildrenRecursion(self, tree, level, parent, area_factor):

        if area_factor < self.lowest_area_factor: return
        if not self.rules.indexIsValid(level + 1): return
        
        enableannex = True
        
#        self.count = self.count + 1
        #if not (self.count % 1000) :
        #print self.count, parent.getObject()
        
        nested_object = parent.getObject()

        
        modulename = inspect.getmodule(nested_object).__name__
        classname = nested_object.__class__.__name__
        
        try:
            countmethodname = self.rules.getCountMethodNameFor(level, modulename, classname)
        except KeyError:
            return
        
        print countmethodname, nested_object, nested_object.__class__
        nbchildren = nested_object.__class__.__dict__[countmethodname](nested_object)

        if nbchildren <= 0 or (nbchildren > (self.max_items_to_read_initial * area_factor) and level > 0):
            return
        
        methodname = self.rules.getMethodNameFor(level, modulename, classname)
        parentmethodname =  self.rules.getParentMethodNameFor(level, modulename, classname)

        children = list(nested_object.__class__.__dict__[methodname](nested_object))

        if len(children) == 0:
            return
        else:
            
            self.chcount = self.chcount + len(children)
            print level, self.chcount, parent.getObject()
        
            evalsum = 0.0
            thechild = None
            
            annexevalsum = 0.0
            
            treenodechildren = []
            
            #calculate sum of all entries and keep the generated Treenodes in treenodechildren
            for child in children:
                
                chmodulename = inspect.getmodule(child).__name__
                chclassname = child.__class__.__name__
                
                chcolumnname = self.rules.getColumnNameFor(level + 1, chmodulename, chclassname)
                chparam = self.rules.getParamFor(level + 1, chmodulename, chclassname)
                
                thechild = TreeNode(child, chcolumnname, parentmethodname, chparam, level + 1)
                treenodechildren.append(thechild)

                evalsum = evalsum + thechild.evaluate()

            if evalsum <= 0: 
                tree.deleteChildren()
                return
            
            #set siblingssum to all children
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
            
            #[:] to copy the content 
            #childnodes is a direct reference to the tree content, which we don't want to have in Annex
            #if there is no copy excludedchildren are the same as childnodes, so annexnode = tree.addChild(annexchild... would also add an item to Annex
            childnodes = tree.getChildren()[:] 
            
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
                    self.addChildrenRecursion(tree, level + 1, thechild, area_factor * thechild.evaluate() )
                    tree.traverseBack()
            
            #add the annex object representing the rest of the children
            if(annexevalsum > 0):
                #create Annex as TreeNode
                annexchild = Annex(self.rules, level, nested_object, childnodes)
                annexchild.evaluation = annexevalsum
                chmodulename = inspect.getmodule(annexchild).__name__
                chclassname = annexchild.__class__.__name__
                chcolumnname = self.rules.getColumnNameFor(level, chmodulename, chclassname)
                chparam = self.rules.getParamFor(level, chmodulename, chclassname)
                #annexnode = TreeNode(annexchild, chcolumnname, parentmethodname, chparam, level)
                
                annexnode = tree.addChild(annexchild, chcolumnname, parentmethodname, chparam)
                annexnode.setSiblingsSum(evalsum)
                             
def print_tabs(n):
    for i in range(n):
        print "   ",
                
                    