'''
Created on May 10, 2010

holds ViewNodes in a tree structure, provides methods to access that tree

@author: kblaszcz
'''
import networkx as nx
import warnings
from  networkx import NetworkXError

class ViewTree(object):
    '''
    you insert your model instances, but you receive TreeNode Objects!
    create and traverse the Object tree
    use traveseToRoot() to restore the traversal position
    '''


    def __init__(self, treemap_props):
        
        self.width = treemap_props['pxwidth']
        self.height = treemap_props['pxheight']
            
        self.root = None
        
        self.node_inscope = self.root
        self.children_inscope = []
        self.graph = nx.DiGraph()
        self.depth_inscope = -1
        self.traversal_path = []
        
        self.children_sorted = False
        self.children_cached= False
        
        self.nodes_per_level = []

    def setRoot(self, theobject):
        self.graph.clear()
        theroot = theobject #(self, theobject, evalattrname, fpar = None, dpt = -1)
        self.graph.add_node(theroot, value = 1)
        self.root = theroot
        self.node_inscope = theroot
        self.depth_inscope = 0
        self.children_sorted = False
        self.children_cached= False
        self.traversal_path = []
        
        self.countNodesPerLevel(0,1)
        
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
            child1area = child1.getWidth()*child1.getHeight()
            child2area = child2.getWidth()*child2.getHeight()
            if child1area > child2area: return 1
            elif child1area < child2area: return -1
            else: 
                if id(child1) > (child2):
                    return 1
                elif id(child1) < (child2):
                    return -1
            warnings.warn('you seem to have the same object at least 2 times on the same tree level')
            return 0
                
        if not self.children_sorted:
            self.updateChildren()  
            self.children_inscope.sort(cmp=childrenCompare, key=None, reverse=True)
            self.children_sorted = True
        return self.children_inscope
            
#    count = 0     
            
    def addChild(self, theobject):
#        self.count = self.count + 1
#        print self.count
#        if self.count%13 == 0:
#            print "stop here"
        if self.root == None:
            raise Exception( 'Cannot add child if no root specified')
        
        if self.node_inscope is theobject:
            print "error!!!"
            raise Exception( 'Parent cannot contain itself as child')
        
        child = theobject
        self.graph.add_node(child)
        self.graph.add_edge(self.node_inscope, child)
        if self.children_cached:
            self.children_inscope.append(child)
        self.children_sorted = False
        
        self.countNodesPerLevel(self.depth_inscope,1)
        
        return child

    def addChildren(self, objects):
        if self.root == None:
            raise Exception( 'Cannot add child if no root specified')
    
        if self.node_inscope in objects:
            raise Exception( 'Parent cannot contain itself as child')
    
        for theobject in objects:
            
            child = theobject
            self.graph.add_node(child)
            self.graph.add_edge(self.node_inscope, child)
            if self.children_cached:
                self.children_inscope.append(child)
        
        self.children_sorted = False
        
        self.countNodesPerLevel(self.depth_inscope,len(objects))
        
    def getChildren(self):
        self.updateChildren()
        return self.children_inscope

    def deleteChildren(self):
        self.graph.remove_nodes_from(self.graph.neighbors(self.node_inscope))
        
        self.countNodesPerLevel(self.depth_inscope, len(self.children_inscope) * (-1))
        
        self.children_inscope = []
        
        self.children_sorted = True
        self.children_cached= True
        
    def nodeHasChildren(self, node):
        try:
            children = self.graph.out_degree(node)
            if children>0: 
                return True 
            else: 
                return False
        except NetworkXError, e:
            return False

    
    def getCurrentObject(self):
        return self.node_inscope
        

    def traverseInto(self, child):
        if not self.graph.has_node(child) and self.graph.has_edge(self.node_inscope, child):
            raise Exception( 'No child like ' + child.__str__() + ' found')
        if self.root == None:
            raise Exception( 'Cannot go deeper if no root specified')   
        self.traversal_path.append(self.node_inscope)
        self.node_inscope = child
        
        self.children_cached = False
        self.children_sorted = False
        self.depth_inscope = self.depth_inscope + 1

        
    def traverseBack(self):
        if self.depth_inscope <= 0:
            raise Exception( 'root has no parents, cannot traverse back')
        
        #find parent
#        parent_edge = self.graph.in_edges(self.node_inscope)
        if len(self.traversal_path) <= 0:
            raise Exception( 'no parents found, cannot traverse back')
        
        parent = self.traversal_path.pop()
#        parent = parent_edge[0][0] #first edge, first Node
        self.node_inscope = parent
        
        self.children_sorted = False
        self.children_cached = False

        self.depth_inscope = self.depth_inscope - 1
    
    def traveseToRoot(self):
        self.node_inscope = self.root#    def report_change(self):
#        self.tree.update()
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
        
    def getWidth(self):
        return self.width
    
    def getHeight(self):
        return self.height
    
    def getMaxDepth(self):
        return len(self.nodes_per_level)
    
    #for internal use only
    def countNodesPerLevel(self, level, number):
        if level < 0: raise IndexError
        if number == 0: return
        if level > 20: raise Warning("It is unusual to have more than 20 levels in ViewTree. Please check your code.")
        if level >= len(self.nodes_per_level):
                self.nodes_per_level.append(0)
        try:
            self.nodes_per_level[level] = self.nodes_per_level[level] + number
        except IndexError:
            raise Exception("Before level "+ level+ " can exist, level " + (level-1) + "must exist!")
        
        #cut if number of nodes is 0 now to make getMaxDepth = len(self.nodes_per_level)
        if self.nodes_per_level[level] <= 0:
            self.nodes_per_level = self.nodes_per_level[:level]
            
    def getLevel(self):
        return self.depth_inscope
    
    def getAllNodes(self):
        return self.graph.nodes()