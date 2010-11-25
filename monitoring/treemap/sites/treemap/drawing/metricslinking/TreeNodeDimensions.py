'''
Created on Jul 14, 2010

@author: kblaszcz
'''
from django.template.loader import render_to_string
from sites.dirs.models import *
from sites.tools.Inspections import ColumnFinder
from sites.tools.TextTools import sizeInBytes
from sites.treemap.drawing.metricslinking.AttributeTransformators import *
from sites.treemap.objecttree.Annex import Annex
from sites.treemap.viewtree.ViewNode import ViewNode
import exceptions
import inspect
import math
import string
import sys

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
        
        size = float(tnode.getProperty('treenode').getEvalValue())
        psize = tnode.getProperty('treenode').getSiblingsSum()
        bytesize = long(dbobj.totalsize)
        
        itemnameparts = splitText(dbobj.fullname.__str__(), 50, 39)
        attrnname = tnode.getProperty('treenode').getColumnname()
        attrvalue = "%.2f"%(float(tnode.getProperty('treenode').getEvalValueNoPostProcess()))
        attrprocvalue = "%.2f"%(float(tnode.getProperty('treenode').getEvalValue()))
        sizestring = ''.join([bla for bla in (sizeInBytes(bytesize), " (", long(bytesize).__str__(), " Bytes)")])
        percentagestring = ''
        if(psize == 0):
            percentagestring = "%.2f"%(100)
        else:
            percentagestring = "%.2f"%(size/psize*100.0)
        
        nbfiles = dbobj.countFiles().__str__()
        nbdirs = dbobj.countDirs().__str__()
        nbsubtreefiles = dbobj.nbfiles.__str__()
        nbsubtreedirs = dbobj.nbsubdirs.__str__()
        
        return render_to_string('tooltipdimensions/dirtooltip.html', {'itemnameparts': itemnameparts, 'attrnname':attrnname, 'attrvalue':attrvalue, \
                                                'attrprocvalue':attrprocvalue, 'sizestring':sizestring, 'percentagestring':percentagestring, \
                                                'nbfiles':nbfiles, 'nbdirs':nbdirs, 'nbsubtreefiles':nbsubtreefiles, 'nbsubtreedirs':nbsubtreedirs}, \
                                context_instance=None)
        
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
        
        size = float(tnode.getProperty('treenode').getEvalValue())
        psize = tnode.getProperty('treenode').getSiblingsSum()
        bytesize = long(dbobj.filesize)
        dirname = parent.__str__()
        
        itemnameparts = splitText(dbobj.name.__str__(), 50, 39)
        dirnameparts = splitText(dirname, 50, 39)
        attrnname = tnode.getProperty('treenode').getColumnname()
        attrvalue = "%.2f"%(float(tnode.getProperty('treenode').getEvalValueNoPostProcess()))
        attrprocvalue = "%.2f"%(float(tnode.getProperty('treenode').getEvalValue()))
        sizestring = ''.join([bla for bla in (sizeInBytes(bytesize), " (", long(bytesize).__str__(), " Bytes)")])
        percentagestring = ''
        if(psize == 0):
            percentagestring = "%.2f"%(100)
        else:
            percentagestring = "%.2f"%(size/psize*100.0)

        return render_to_string('tooltipdimensions/filetooltip.html', {'itemnameparts': itemnameparts, 'attrnname':attrnname, 'attrvalue':attrvalue, \
                                                'attrprocvalue':attrprocvalue, 'sizestring':sizestring, 'percentagestring':percentagestring, \
                                                'dirnameparts': dirnameparts}, \
                                context_instance=None)
    
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
        
        size = tnobj.getEvalValue()
        psize = tnobj.getSiblingsSum()
        dirname = parent.__str__()
        
        dirnameparts = splitText(dirname, 50, 39)
        attrprocvalue = ''.join([bla for bla in (sizeInBytes(size), " (", long(size).__str__(), " Bytes)")])
        percentagestring = ''
        if(psize == 0):
            percentagestring = "%.2f"%(100)
        else:
            percentagestring = "%.2f"%(size/psize*100.0)
        nbitems = dbobj.countItems().__str__()

        return render_to_string('tooltipdimensions/annextooltip.html', {'attrprocvalue':attrprocvalue, 'percentagestring':percentagestring, \
                                                      'dirnameparts': dirnameparts, 'nbitems': nbitems}, \
                                context_instance=None)
    
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
        
        size = float(tnode.getProperty('treenode').getEvalValue())
        psize = tnode.getProperty('treenode').getSiblingsSum()
        
        bytesize = dbobj.filesize
        if bytesize is None: 
            bytesize = 0
        else:
            bytesize = long(bytesize)
            
        nbreq = dbobj.requestscount
        
        itemnameparts = splitText(dbobj.filename, 50, 39)
        attrnname = tnode.getProperty('treenode').getColumnname()
        attrvalue = "%.2f"%(float(tnode.getProperty('treenode').getEvalValueNoPostProcess()))
        attrprocvalue = "%.2f"%(float(tnode.getProperty('treenode').getEvalValue()))
        percentagestring = ''
        if(psize == 0):
            percentagestring = "%.2f"%(100)
        else:
            percentagestring = "%.2f"%(size/psize*100.0)
        
        nbrequests = str(nbreq)
        
        return render_to_string('tooltipdimensions/requesttooltip.html', {'itemnameparts': itemnameparts, 'attrnname':attrnname, 'attrvalue':attrvalue, \
                                                'attrprocvalue':attrprocvalue, 'percentagestring':percentagestring, 'nbrequests':nbrequests}, \
                                context_instance=None)
    
    
def splitText(text, limit = 50, firstlimit = 39):
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
            text = text[bestlimit:]
        limit = limitold
            
    return ret
        
        