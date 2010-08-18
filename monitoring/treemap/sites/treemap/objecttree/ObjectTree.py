'''
Created on May 10, 2010

@author: kblaszcz
'''
from sites.treemap.objecttree.TreeNode import TreeNode
import networkx as nx
from sites.errors import ConfigError
import warnings

class ObjectTree(object):
    '''
    you insert your model instances, but you receive TreeNode Objects!
    create and traverse the Object tree
    use traveseToRoot() to restore the traversal position
    '''


    def __init__(self):
        self.root = None
        
        self.node_inscope = self.root
        self.children_inscope = []
        self.graph = nx.DiGraph()
        self.depth_inscope = -1
        self.traversal_path = []
        
        self.children_sorted = False
        self.children_cached= False

    def setRoot(self, theobject, parentmethodname, fparam = None, evalcolumn = None, siblingssum = 1.0):
        self.graph.clear()
        theroot = TreeNode(theobject = theobject, evalcolumnname = evalcolumn, parentmethodname = parentmethodname, fpar = fparam, siblings_sum = siblingssum, dpt = 0) 
        self.graph.add_node(theroot, value = 1)
        self.root = theroot
        self.node_inscope = theroot
        self.depth_inscope = 0
        self.children_sorted = False
        self.children_cached= False
        self.traversal_path = []
        
    def getRoot(self):
        return self.root

    def countChildren(self):
        self.updateChildren()    
        return len(self.children_inscope)
    
    def getSortedChildren(self):
        def childrenCompare(child1, child2):
            #self.graph.neighbors doesn't guarantee the same order everytime you call it
            #so if you want to guarantee the same child order (for consistent access by index)
            #you need to avoid equality of evaluation. This is done here by using python's objectid
            if child1.evaluate() > child2.evaluate(): return 1
            elif child1.evaluate() < child2.evaluate(): return -1
            else: 
                if id(child1.getObject()) > (child2.getObject()):
                    return 1
                elif id(child1.getObject()) < (child2.getObject()):
                    return -1
            warnings.warn('you seem to have the same object at least 2 times on the same tree level')
            return 0
                
        if not self.children_sorted:
#            print '--'
#            for node in self.children_inscope:
#                print node.getObject(), node.evaluate()
#            self.children_sorted = True
            self.updateChildren()  
            self.children_inscope.sort(cmp=childrenCompare, key=None, reverse=True)
            self.children_sorted = True
#            print '--'
#            for node in self.children_inscope:
#                print node.getObject(), node.evaluate()
#            self.children_sorted = True
        return self.children_inscope
            
#    count = 0     
            
    def addChild(self, theobject, columnname, parentmethodname, fparam = None):
#        self.count = self.count + 1
#        print self.count
#        if self.count%13 == 0:
#            print "stop here"
        if self.root == None:
            raise ConfigError( 'Cannot add child if no root specified')
        
        if self.node_inscope is theobject:
            print "error!!!"
            raise ConfigError( 'Parent cannot contain itself as child')
        
        child = TreeNode(theobject, columnname, parentmethodname, fparam, self.depth_inscope + 1)
        self.graph.add_node(child, value = child.evaluate())
        self.graph.add_edge(self.node_inscope, child)
        if self.children_cached:
            self.children_inscope.append(child)
        self.children_sorted = False
        return child
    
    def addTreeNodeChild(self, theobject):
        if self.root == None:
            raise ConfigError( 'Cannot add child if no root specified')
        
        if self.node_inscope is theobject:
            print "error!!!"
            raise ConfigError( 'Parent cannot contain itself as child')
        
        assert(isinstance(theobject, TreeNode))
        
        self.graph.add_node(theobject, value = theobject.evaluate())
        self.graph.add_edge(self.node_inscope, theobject)
        if self.children_cached:
            self.children_inscope.append(theobject)
        self.children_sorted = False
        return theobject

    def addChildren(self, objects, columnname, parentmethodname, fparam = None):
        if self.root == None:
            raise ConfigError( 'Cannot add child if no root specified')
    
        if self.node_inscope in objects:
            raise ConfigError( 'Parent cannot contain itself as child')
    
        for theobject in objects:
            
            child = TreeNode(theobject, columnname, parentmethodname, fparam, self.depth_inscope + 1)
            self.graph.add_node(child, value = child.evaluate())
            self.graph.add_edge(self.node_inscope, child)
            if self.children_cached:
                self.children_inscope.append(child)
                
        self.children_sorted = False
        
    def getChildren(self):
        self.updateChildren()
        return self.children_inscope

    def deleteChildren(self):
        self.graph.remove_nodes_from(self.graph.neighbors(self.node_inscope))
        
        self.children_inscope = []
        
        self.children_sorted = True
        self.children_cached= True

    
    def getCurrentObject(self):
        return self.node_inscope
        

    def traverseInto(self, child):
        if not self.graph.has_node(child) and self.graph.has_edge(self.node_inscope, child):
            raise ConfigError( 'No child like ' + child.__str__() + ' found')
        if self.root == None:
            raise ConfigError( 'Cannot go deeper if no root specified')   

        self.traversal_path.append(self.node_inscope)
        self.node_inscope = child
        
        self.children_cached = False
        self.children_sorted = False
        self.depth_inscope = self.depth_inscope + 1

        
    def traverseBack(self):
        if self.depth_inscope <= 0:
            raise ConfigError( 'root has no parents, cannot traverse back')
        
        #find parent
#        parent_edge = self.graph.in_edges(self.node_inscope)
        if len(self.traversal_path) <= 0:
            raise ConfigError( 'no parents found, cannot traverse back')
        
        parent = self.traversal_path.pop()
#        parent = parent_edge[0][0] #first edge, first Node
        self.node_inscope = parent
        
        self.children_sorted = False
        self.children_cached = False

        self.depth_inscope = self.depth_inscope - 1
    
    def traveseToRoot(self):
        self.node_inscope = self.root
        self.children_sorted = False
        self.children_cached = False
        self.depth_inscope = 0
        self.traversal_path = []
    
    def updateChildren(self):
        if not self.children_cached:
            self.children_inscope = []
            self.children_inscope = self.graph.neighbors(self.node_inscope)
            if self.children_inscope:
                self.children_sorted = False
            else:
                self.children_inscope = []
                self.children_sorted = True
                
        self.children_cached = True
    
    def getLevel(self):
        return self.depth_inscope
        
        

        