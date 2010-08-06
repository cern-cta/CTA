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
        self.lowest_area_factor = 0.01443375672974064411 #smallest accepted percentage of the area, if the node evaluates below that percentage the child will not be added
        self.max_children_to_read = 100
        
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

        if nbchildren <= 0:
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
            annexchildren = []
            annexrawchildren = []
            
            for i, child in enumerate(children):
                
                chmodulename = inspect.getmodule(child).__name__
                chclassname = child.__class__.__name__
                
                chcolumnname = self.rules.getColumnNameFor(level + 1, chmodulename, chclassname)
                chparam = self.rules.getParamFor(level + 1, chmodulename, chclassname)
                
                if i < self.max_children_to_read:
                    thechild = tree.addChild(child, chcolumnname, parentmethodname, chparam)
                else:
                    thechild = TreeNode(child, chcolumnname, parentmethodname, chparam, level + 1)
                    annexchildren.append(thechild)
                    annexrawchildren.append(child)
                    annexevalsum = annexevalsum + thechild.evaluate()
                
                evalsum = evalsum + thechild.evaluate()
#                print "   ", child

            if evalsum <= 0: 
                tree.deleteChildren()
                return
            
            childnodes = tree.getChildren()
            
            #add max_children_to_read number of children and go recursively for their children
            for thechild in childnodes:
                
                thechild.setSiblingsSum(evalsum)
                tree.traverseInto(thechild)
                self.addChildrenRecursion(tree, level + 1, thechild, area_factor * thechild.evaluate() )
                tree.traverseBack()
            
            #add the annex object representing the rest of the children
            if(annexevalsum > 0):
                #create Annex as TreeNode
                annexchild = Annex(annexrawchildren, nested_object)
                annexchild.evaluation = annexevalsum
                chmodulename = inspect.getmodule(annexchild).__name__
                chclassname = annexchild.__class__.__name__
                chcolumnname = self.rules.getColumnNameFor(level, chmodulename, chclassname)
                chparam = self.rules.getParamFor(level, chmodulename, chclassname)
                #annexnode = TreeNode(annexchild, chcolumnname, parentmethodname, chparam, level)
                
                annexnode = tree.addChild(annexchild, chcolumnname, parentmethodname, chparam)
                annexnode.setSiblingsSum(evalsum)
#                self.addChildrenRecursion(tree, level, annexnode, area_factor * annexnode.evaluate() )
                
#                tree.traverseInto(annexchild)
#                
#                for child in annexrawchildren:
#                    
#                    chmodulename = inspect.getmodule(child).__name__
#                    chclassname = child.__class__.__name__
#                    chcolumnname = self.rules.getColumnNameFor(level, chmodulename, chclassname)
#                    chparam = self.rules.getParamFor(level, chmodulename, chclassname)
#                        
#                    thechild = tree.addChild(child, chcolumnname, parentmethodname, chparam)
#                    thechild.setSiblingsSum(annexevalsum)
#                    tree.traverseInto(thechild)
#                    self.addChildrenRecursion(tree, level + 1, thechild, area_factor * thechild.evaluate() )
#                    tree.traverseBack()
#                    
#                tree.traverseBack()
                           
                
def print_tabs(n):
    for i in range(n):
        print "   ",
                
                    