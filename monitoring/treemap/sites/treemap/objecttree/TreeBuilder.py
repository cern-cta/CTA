'''
Created on May 19, 2010

@author: kblaszcz
'''

from sites.treemap.objecttree.ObjectTree import ObjectTree
import inspect
from sites.treemap.objecttree.TreeRules import LevelRules

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
#        if parent.getObject().__str__() == "/castor/cern.ch/grid/cms/HMM/x18751u11598":
#            print 'dbg:', self.count, parent.getObject(), level

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
        
#        print '3'
#        
#        if (nested_object.__str__() == "/castor/cern.ch/compass/data/2008/oracle_dst") or (nested_object.__str__() == "/castor/cern.ch/grid/lhcb") or (nested_object.__str__() == "/castor/cern.ch/grid/atlas") or (nested_object.__str__() == "/castor/cern.ch/compass/data/2006/raw") or (nested_object.__str__() == "/castor/cern.ch/grid") or (nested_object.__str__() == "/castor/cern.ch/cms"):
#            print "stop"
        print countmethodname, nested_object, nested_object.__class__
        nbchildren = nested_object.__class__.__dict__[countmethodname](nested_object)
        
#        print '4'
        if nbchildren <= 0 or (nbchildren > self.max_children_to_read and level > 0):
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
#            tree.addChildren(children, columnname, param)
            thechild = None
            for child in children:
                
                chmodulename = inspect.getmodule(child).__name__
                chclassname = child.__class__.__name__
                
                chcolumnname = self.rules.getColumnNameFor(level + 1, chmodulename, chclassname)
                chparam = self.rules.getParamFor(level + 1, chmodulename, chclassname)
                
                thechild = tree.addChild(child, chcolumnname, parentmethodname, chparam)
                evalsum = evalsum + thechild.evaluate()
#                print "   ", child

            if evalsum <= 0: 
                tree.deleteChildren()
                return
            
#            print "evalsum: ", evalsum
            
            childnodes = tree.getChildren()
            
            for thechild in childnodes:
                
                thechild.setSiblingsSum(evalsum)
                tree.traverseInto(thechild)
                self.addChildrenRecursion(tree, level + 1, thechild, area_factor * thechild.evaluate() )
                tree.traverseBack()
                
def print_tabs(n):
    for i in range(n):
        print "   ",
                
                    