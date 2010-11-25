'''
Created on Jul 14, 2010

@author: kblaszcz
'''
import exceptions
from sites.treemap.viewtree.ViewNode  import ViewNode
from sites.tools.Inspections import ColumnFinder
import sys
import inspect
import string
import math
from sites.dirs.models import *
from sites.treemap.objecttree.Annex import Annex
from sites.tools.TextTools import sizeInBytes
from sites.treemap.drawing.metricslinking.AttributeTransformators import *

class ViewNodeDimensionBase(object):
    '''
    classdocs
    '''
    def __init__(self):
        pass
        
    def getValue(self, tnode):
        raise Exception("implementation of getValue() not found")

#outputs the level number
class LevelDimension(ViewNodeDimensionBase):
    '''
    classdocs
    '''
    def __init__(self):
        ViewNodeDimensionBase.__init__(self)
        
    def getValue(self, tnode):
        assert(tnode is not None and isinstance(tnode, ViewNode))
        ret = tnode.getProperty('level')
        #convert to integer in case it is a float
        ret = int(ret - ret%1)
        return ret

        
class ConstantDimension(ViewNodeDimensionBase):
    '''
    classdocs
    '''
    def __init__(self, constant):
        ViewNodeDimensionBase.__init__(self)
        self.constant = constant
        
    def getValue(self, tnode):
        return self.constant
    
#evaluates any column to number   
class ColumnDimension(ViewNodeDimensionBase):
    '''
    classdocs
    '''
    def __init__(self, columnname, transformation): #transformation.transform (db object)
        
        assert(transformation is not None)
        ViewNodeDimensionBase.__init__(self)
        self.transformation = transformation
        self.columnname = columnname

        
    def getValue(self, tnode):
        assert(tnode is not None and isinstance(tnode, ViewNode))
        dbobj = tnode.getProperty('treenode').getObject()
        
        ret = self.transformation.transform(dbobj, self.columnname)
        
        #convert to integer in case it is not supposed to be float
        if not ret.isfloat and not ret.istext: ret = ret - ret%1
        
        return ret
    
class RawColumnDimension(ViewNodeDimensionBase):
    '''
    classdocs
    '''
    def __init__(self, columnname, transformation = RawLinearTransformator()): #transformation.transform (db object)
        ViewNodeDimensionBase.__init__(self)
        self.transformation = transformation
        self.columnname = columnname

        
    def getValue(self, tnode):
        assert(tnode is not None and isinstance(tnode, ViewNode))
        dbobj = tnode.getProperty('treenode').getObject()
        ret = self.transformation.transform(dbobj, self.columnname)
        return ret
    
class DirToolTipDimension(ViewNodeDimensionBase):
    '''
    classdocs
    '''
    def __init__(self): #transformation.transform (db object)
        
        ViewNodeDimensionBase.__init__(self)

        
    def getValue(self, tnode):
        assert(tnode is not None and isinstance(tnode, ViewNode))
        dbobj = tnode.getProperty('treenode').getObject()
        
        assert(isinstance(dbobj, Dirs))
        
        ret = []
        size = float(tnode.getProperty('treenode').getEvalValue())
        psize = tnode.getProperty('treenode').getSiblingsSum()
        bytesize = long(dbobj.totalsize)
        
        ret.append("<b>Directory:</b> ")
        ret.append(splitText(dbobj.fullname.__str__(), 50, 39))
        
        ret.append("<br><b> Evaluation of ")
        ret.append(tnode.getProperty('treenode').getColumnname())
        ret.append(":</b> ")
        ret.append("%.2f"%(float(tnode.getProperty('treenode').getEvalValueNoPostProcess())))
        
        ret.append("<br><b>Processed value:</b> ")
        ret.append("%.2f"%(float(tnode.getProperty('treenode').getEvalValue())))
        
        ret.append("<br><br><b>size:</b> ")
        
        ret.append(sizeInBytes(bytesize))
            
        ret.append(" (")
        ret.append(long(bytesize).__str__())
        ret.append(" Bytes)")
        
        ret.append("<br><b>parent percentage:</b> ")
        if(psize == 0):
            ret.append("%.2f"%(100))
        else:
            ret.append("%.2f"%(size/psize*100.0))
            
        ret.append("%")
        
        ret.append("<br><b>number of files: </b> ")
        ret.append(dbobj.countFiles().__str__())
        ret.append("<br><b>number of directories: </b> ")
        ret.append(dbobj.countDirs().__str__())
        ret.append("<br><br><b>number of files in subtree: </b> ")
        ret.append(dbobj.nbfiles.__str__())
        ret.append("<br><b>number of directories in subtree: </b> ")
        ret.append(dbobj.nbsubdirs.__str__())
        
        return ''.join([bla for bla in ret])
        
class FileToolTipDimension(ViewNodeDimensionBase):
    '''
    classdocs
    '''
    def __init__(self): #transformation.transform (db object)
        
        ViewNodeDimensionBase.__init__(self)

        
    def getValue(self, tnode):
        assert(tnode is not None and isinstance(tnode, ViewNode))
        dbobj = tnode.getProperty('treenode').getObject()
        parent = tnode.getProperty('treenode').getNakedParent()
        assert(isinstance(dbobj, CnsFileMetadata))
        
        ret = []
        size = float(tnode.getProperty('treenode').getEvalValue())
        psize = tnode.getProperty('treenode').getSiblingsSum()
        bytesize = long(dbobj.filesize)
        dirname = parent.__str__()
        
        ret.append("<b>File name:</b> ")
        ret.append(splitText(dbobj.name.__str__(), 50, 39))
        ret.append("<br><b>Directory:</b> ")
        ret.append(splitText(dirname, 50, 39))
        
        ret.append("<br><b> Evaluation of ")
        ret.append(tnode.getProperty('treenode').getColumnname())
        ret.append(":</b> ")
        ret.append("%.2f"%(float(tnode.getProperty('treenode').getEvalValueNoPostProcess())))
        
        ret.append("<br><b>Processed value:</b> ")
        ret.append("%.2f"%(float(tnode.getProperty('treenode').getEvalValue())))
        
        ret.append("<br><br><b>size:</b> ")
        
        ret.append(sizeInBytes(bytesize))
            
        ret.append(" (")
        ret.append(long(bytesize).__str__())
        ret.append(" Bytes)")
        
        ret.append("<br><b>parent percentage:</b> ")
        if(psize == 0):
            ret.append("%.2f"%(100))
        else:
            ret.append("%.2f"%(size/psize*100.0))
            
        ret.append("%")

        
        return ''.join([bla for bla in ret])
    
class AnnexToolTipDimension(ViewNodeDimensionBase):
    '''
    classdocs
    '''
    def __init__(self): #transformation.transform (db object)
        
        ViewNodeDimensionBase.__init__(self)

        
    def getValue(self, tnode):
        assert(tnode is not None and isinstance(tnode, ViewNode))
        tnobj = tnode.getProperty('treenode')
        dbobj = tnobj.getObject()
        parent = tnode.getProperty('treenode').getNakedParent()
        
        assert(isinstance(dbobj, Annex))
        
        ret = []
        size = tnobj.getEvalValue()
        psize = tnobj.getSiblingsSum()
        dirname = parent.__str__()
        
        ret.append("<b>The rest of the items</b> ")
        ret.append("<br><b>Directory:</b> ")
        ret.append(splitText(dirname, 50, 39))
        ret.append("<br><b>Processed size:</b> ")
        
        ret.append(sizeInBytes(size))
            
        ret.append(" (")
        ret.append((long(size)).__str__())
        ret.append(" Bytes)")
        
        ret.append("<br><b>parent percentage:</b> ")
        if(psize == 0):
            ret.append("%.2f"%(100))
        else:
            ret.append("%.2f"%(size/psize*100.0))
            
        ret.append("%")
        
        ret.append("<br><b>number of items: </b> ")
        ret.append(dbobj.countItems().__str__())

        return ''.join([bla for bla in ret])
    
class RequestsToolTipDimension(ViewNodeDimensionBase):
    '''
    classdocs
    '''
    def __init__(self): #transformation.transform (db object)
        
        ViewNodeDimensionBase.__init__(self)

        
    def getValue(self, tnode):
        assert(tnode is not None and isinstance(tnode, ViewNode))
        dbobj = tnode.getProperty('treenode').getObject()
        parent = tnode.getProperty('treenode').getNakedParent()
        assert(isinstance(dbobj, Requestsatlas) or isinstance(dbobj, Requestscms) or isinstance(dbobj, Requestsalice) or isinstance(dbobj, Requestslhcb) or isinstance(dbobj, Requestspublic))
        
        ret = []
        size = float(tnode.getProperty('treenode').getEvalValue())
        psize = tnode.getProperty('treenode').getSiblingsSum()
        
        bytesize = dbobj.filesize
        if bytesize is None: 
            bytesize = 0
        else:
            bytesize = long(bytesize)
            
        nbreq = dbobj.requestscount
        
        ret.append("<b>Item:</b> ")
        ret.append(splitText(dbobj.filename, 50, 39))
        
        ret.append("<br><b>Number of requests:</b> ")
        ret.append(str(nbreq))
        
        ret.append("<br><b> Evaluation of ")
        ret.append(tnode.getProperty('treenode').getColumnname())
        ret.append(":</b> ")
        ret.append("%.2f"%(float(tnode.getProperty('treenode').getEvalValueNoPostProcess())))
        
        ret.append("<br><b>Processed value:</b> ")
        ret.append("%.2f"%(float(tnode.getProperty('treenode').getEvalValue())))
        
        ret.append("<br><br><b>parent percentage:</b> ")
        if(psize == 0):
            ret.append("%.2f"%(100))
        else:
            ret.append("%.2f"%(size/psize*100.0))
            
        ret.append("%")
        
        return ''.join([bla for bla in ret])   
    
    
def splitText(text, limit, firstlimit):
    limitold = limit
    limit = firstlimit
    assert(limit > 1)
    ret = []
    while len(text) > 0:
        if(len(text)<=limit):
            ret.append(text)
            break
        else:
            bestlimit = limit
            alternativelimit = text[:limit].rfind('/')
            if alternativelimit > 1: bestlimit = alternativelimit
            
            ret.append(text[:bestlimit])
            ret.append('<br>')
            text = text[bestlimit:]
        limit = limitold
            
    return ''.join([bla for bla in ret])
        
        