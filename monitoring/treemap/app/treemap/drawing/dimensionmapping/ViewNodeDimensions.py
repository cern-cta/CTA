'''
Created on Jul 14, 2010

A ViewNodeDimension extracts specific information ("Dimension") from a ViewNode and provides a method getValue to access the value 
It is like watching at ViewNode from different points of view. Sometimes you want to see the level af a node, sometimes you want to see a description of a node.

This "Dimension" can then be associated with a graphical property by using DimensionVisPropMapping:

mapping = DimensionVisPropMapping()
mapping.addPropertyLink('Dirs', 'fillcolor', LevelDimension())

In the example above:
If LevelDimension.getValue() returns 1, TreeDesigner will decode and set the fillcolor associated with number 1.

@author: kblaszcz
'''
from django.template.loader import render_to_string
from app.dirs.models import *
from app.tools.Inspections import ModelAttributeFinder
from app.tools.TextTools import sizeInBytes, splitText
from app.treemap.drawing.dimensionmapping.AttributeTranslators import *
from app.treemap.objecttree.Annex import Annex
from app.treemap.viewtree.ViewNode import ViewNode
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
    
#evaluates any attribute to number   
class ColumnDimension(ViewNodeDimensionBase):
    '''
    classdocs
    '''
    def __init__(self, attrname, translation): #translation.translate (db object)
        
        assert(translation is not None)
        ViewNodeDimensionBase.__init__(self)
        self.translation = translation
        self.attrname = attrname

        
    def getValue(self, tnode):
        assert(tnode is not None and isinstance(tnode, ViewNode))
        modelinstance = tnode.getProperty('treenode').getObject()
        
        ret = self.translation.translate(modelinstance, self.attrname)
        
        #convert to integer in case it is not supposed to be float
        if not ret.isfloat and not ret.istext: ret = ret - ret%1
        
        return ret
    
class RawColumnDimension(ViewNodeDimensionBase):
    '''
    classdocs
    '''
    def __init__(self, attrname, translation = RawLinearTranslator()): #translation.translate (object)
        ViewNodeDimensionBase.__init__(self)
        self.translation = translation
        self.attrname = attrname

        
    def getValue(self, tnode):
        assert(tnode is not None and isinstance(tnode, ViewNode))
        modelinstance = tnode.getProperty('treenode').getObject()
        ret = self.translation.translate(modelinstance, self.attrname)
        return ret
    
class FileExtensionDimension(ViewNodeDimensionBase):
    '''
    classdocs
    '''
    def __init__(self, attrname, translation = FileExtensionTranslator()): #translation.translate (object)
        ViewNodeDimensionBase.__init__(self)
        self.translation = translation
        self.attrname = attrname

        
    def getValue(self, tnode):
        assert(tnode is not None and isinstance(tnode, ViewNode))
        modelinstance = tnode.getProperty('treenode').getObject()
        try:
            ret = self.translation.translate(modelinstance, self.attrname)/float(self.translation.max)
        except:
            return None
        return ret
    
class DirToolTipDimension(ViewNodeDimensionBase):
    '''
    classdocs
    '''
    def __init__(self): #translation.translate (object)
        
        ViewNodeDimensionBase.__init__(self)

        
    def getValue(self, tnode):
        assert(tnode is not None and isinstance(tnode, ViewNode))
        modelinstance = tnode.getProperty('treenode').getObject()
        assert(isinstance(modelinstance, Dirs))
        
        size = float(tnode.getProperty('treenode').getEvalValue())
        psize = tnode.getProperty('treenode').getSiblingsSum()
        bytesize = long(modelinstance.totalsize)
        
        itemnameparts = splitText(modelinstance.fullname.__str__(), 50, 39)
        attrname = tnode.getProperty('treenode').getAttrName()
        attrvalue = "%.2f"%(float(tnode.getProperty('treenode').getEvalValueNoPostProcess()))
        attrprocvalue = "%.2f"%(float(tnode.getProperty('treenode').getEvalValue()))
        sizestring = ''.join([bla for bla in (sizeInBytes(bytesize), " (", long(bytesize).__str__(), " Bytes)")])
        percentagestring = ''
        if(psize == 0):
            percentagestring = "%.2f"%(100)
        else:
            percentagestring = "%.2f"%(size/psize*100.0)
        
        nbfiles = modelinstance.countFiles().__str__()
        nbdirs = modelinstance.countDirs().__str__()
        nbsubtreefiles = modelinstance.nbfiles.__str__()
        nbsubtreedirs = modelinstance.nbsubdirs.__str__()
        
        return render_to_string('tooltipdimensions/dirtooltip.html', {'itemnameparts': itemnameparts, 'attrname':attrname, 'attrvalue':attrvalue, \
                                                'attrprocvalue':attrprocvalue, 'sizestring':sizestring, 'percentagestring':percentagestring, \
                                                'nbfiles':nbfiles, 'nbdirs':nbdirs, 'nbsubtreefiles':nbsubtreefiles, 'nbsubtreedirs':nbsubtreedirs}, \
                                context_instance=None)
        
class FileToolTipDimension(ViewNodeDimensionBase):
    '''
    classdocs
    '''
    def __init__(self): #translation.translate (object)
        
        ViewNodeDimensionBase.__init__(self)

        
    def getValue(self, tnode):
        assert(tnode is not None and isinstance(tnode, ViewNode))
        modelinstance = tnode.getProperty('treenode').getObject()
        parent = tnode.getProperty('treenode').getNakedParent()
        assert(isinstance(modelinstance, CnsFileMetadata))
        
        size = float(tnode.getProperty('treenode').getEvalValue())
        psize = tnode.getProperty('treenode').getSiblingsSum()
        bytesize = long(modelinstance.filesize)
        dirname = parent.__str__()
        
        itemnameparts = splitText(modelinstance.name.__str__(), 50, 39)
        dirnameparts = splitText(dirname, 50, 39)
        attrname = tnode.getProperty('treenode').getAttrName()
        attrvalue = "%.2f"%(float(tnode.getProperty('treenode').getEvalValueNoPostProcess()))
        attrprocvalue = "%.2f"%(float(tnode.getProperty('treenode').getEvalValue()))
        sizestring = ''.join([bla for bla in (sizeInBytes(bytesize), " (", long(bytesize).__str__(), " Bytes)")])
        percentagestring = ''
        if(psize == 0):
            percentagestring = "%.2f"%(100)
        else:
            percentagestring = "%.2f"%(size/psize*100.0)

        return render_to_string('tooltipdimensions/filetooltip.html', {'itemnameparts': itemnameparts, 'attrname':attrname, 'attrvalue':attrvalue, \
                                                'attrprocvalue':attrprocvalue, 'sizestring':sizestring, 'percentagestring':percentagestring, \
                                                'dirnameparts': dirnameparts}, \
                                context_instance=None)
    
class AnnexToolTipDimension(ViewNodeDimensionBase):
    '''
    classdocs
    '''
    def __init__(self): #translation.translate (object)
        
        ViewNodeDimensionBase.__init__(self)

        
    def getValue(self, tnode):
        assert(tnode is not None and isinstance(tnode, ViewNode))
        tnobj = tnode.getProperty('treenode')
        modelinstance = tnobj.getObject()
        parent = tnode.getProperty('treenode').getNakedParent()
        
        assert(isinstance(modelinstance, Annex))
        
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
        nbitems = modelinstance.countItems().__str__()

        return render_to_string('tooltipdimensions/annextooltip.html', {'attrprocvalue':attrprocvalue, 'percentagestring':percentagestring, \
                                                      'dirnameparts': dirnameparts, 'nbitems': nbitems}, \
                                context_instance=None)
    
class RequestsToolTipDimension(ViewNodeDimensionBase):
    '''
    classdocs
    '''
    def __init__(self): #translation.translate (object)
        
        ViewNodeDimensionBase.__init__(self)

        
    def getValue(self, tnode):
        assert(tnode is not None and isinstance(tnode, ViewNode))
        modelinstance = tnode.getProperty('treenode').getObject()
        parent = tnode.getProperty('treenode').getNakedParent()
        assert(isinstance(modelinstance, Requestsatlas) or isinstance(modelinstance, Requestscms) or isinstance(modelinstance, Requestsalice) or isinstance(modelinstance, Requestslhcb) or isinstance(modelinstance, Requestspublic))
        
        size = float(tnode.getProperty('treenode').getEvalValue())
        psize = tnode.getProperty('treenode').getSiblingsSum()
        
        bytesize = modelinstance.filesize
        if bytesize is None: 
            bytesize = 0
        else:
            bytesize = long(bytesize)
            
        nbreq = modelinstance.requestscount
        
        itemnameparts = splitText(modelinstance.filename, 50, 39)
        attrname = tnode.getProperty('treenode').getAttrName()
        attrvalue = "%.2f"%(float(tnode.getProperty('treenode').getEvalValueNoPostProcess()))
        attrprocvalue = "%.2f"%(float(tnode.getProperty('treenode').getEvalValue()))
        percentagestring = ''
        if(psize == 0):
            percentagestring = "%.2f"%(100)
        else:
            percentagestring = "%.2f"%(size/psize*100.0)
        
        nbrequests = str(nbreq)
        
        return render_to_string('tooltipdimensions/requesttooltip.html', {'itemnameparts': itemnameparts, 'attrname':attrname, 'attrvalue':attrvalue, \
                                                'attrprocvalue':attrprocvalue, 'percentagestring':percentagestring, 'nbrequests':nbrequests}, \
                                context_instance=None)
    
        
        