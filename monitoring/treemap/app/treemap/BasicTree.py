'''
Basic implementation of a tree
@author: kblaszcz
'''

import networkx as nx

class BasicTree(object):
    '''
    you insert your model instances, but you read TreeNode Objects!
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
        
        self.children_cached= False

    def setRoot(self, theobject):
        self.graph.clear()
        self.graph.add_node(theobject)
        self.root = theobject
        self.node_inscope = theobject
        self.depth_inscope = 0
        self.children_cached= False
        self.traversal_path = []
        
    def getRoot(self):
        return self.root

    def countChildren(self):
        self.updateChildren()    
        return len(self.children_inscope)
    
    def addChild(self, theobject):
        if self.root == None:
            raise Exception( 'Cannot add child if no root specified')
        
        if self.node_inscope is theobject:
            print "error!!!"
            raise Exception( 'Parent cannot contain itself as child')
        
        self.graph.add_node(theobject)
        self.graph.add_edge(self.node_inscope, theobject)
            
        if self.children_cached:
            self.children_inscope.append(theobject)
        return theobject

    def addChildren(self, objects):
        if self.root == None:
            raise Exception( 'Cannot add child if no root specified')
    
        if self.node_inscope in objects:
            raise Exception( 'Parent cannot contain itself as child')
    
        for theobject in objects:
            
            self.graph.add_node(theobject)
            self.graph.add_edge(self.node_inscope, theobject)
            if self.children_cached:
                self.children_inscope.append(theobject)
        
    def getChildren(self):
        self.updateChildren()
        return self.children_inscope

    def deleteChildren(self):
        self.graph.remove_nodes_from(self.graph.neighbors(self.node_inscope))
        self.children_inscope = []
        self.children_cached= True

    
    def getCurrentObject(self):
        return self.node_inscope
        

    def traverseIntoChild(self, child):
        if not self.graph.has_node(child) and self.graph.has_edge(self.node_inscope, child):
            raise Exception( 'No child like ' + child.__str__() + ' found')
        if self.root == None:
            raise Exception( 'Cannot go deeper if no root specified')   

        self.traversal_path.append(self.node_inscope)
        self.node_inscope = child
        
        self.children_cached = False
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
        
        self.children_cached = False

        self.depth_inscope = self.depth_inscope - 1
    
    def traverseToRoot(self):
        self.node_inscope = self.root
        self.children_cached = False
        self.depth_inscope = 0
        self.traversal_path = []
    
    def updateChildren(self):
        if not self.children_cached:
            self.children_inscope = []
            #parent_edge = self.graph.out_edges(self.node_inscope)
            self.children_inscope = self.graph.neighbors(self.node_inscope)
            if self.children_inscope:
                pass
            else:
                self.children_inscope = []
                
        self.children_cached = True
    
    def getLevel(self):
        return self.depth_inscope
    
    def hasRoot(self):
        return self.root is not None