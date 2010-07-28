# Create your views here.
from django.http import Http404, HttpResponse
from django.shortcuts import render_to_response
from django.template import Context, loader
from sites.dirs.models import Dirs
from django.shortcuts import render_to_response, get_list_or_404, get_object_or_404
from django.http import HttpResponseRedirect
from django.core.urlresolvers import reverse
from django.conf.urls.defaults import *
from django.db.models.query import QuerySet
import datetime
from django.template.loader import render_to_string

from sites.treemap.objecttree.ObjectTree import ObjectTree
from sites.treemap.objecttree.TreeBuilder import TreeBuilder
from sites.treemap.viewtree.TreeCalculators import SquaredTreemapCalculator
from sites.treemap.objecttree.TreeRules import LevelRules
import profile
from sites.treemap.drawing.TreemapDrawers import SquaredTreemapDrawer
from sites.treemap.defaultproperties.SquaredViewProperties import *
from sites.treemap.drawing.TreeDesigner import SquaredTreemapDesigner
from sites.dirs.models import *
from sites.treemap.drawing.metricslinking.MetricsLinker import MetricsLinker
from sites.treemap.drawing.metricslinking.TreeNodeDimensions import *

def plain(request, depth):
    p = get_list_or_404(Dirs, depth = depth)
    return render_to_response('dirs/dir_list.html', {'object_list': p})

def xxxplainbydir(request, id):
    time = datetime.datetime.now()
    try:
        #Directory you want to show its content
        p = Dirs.objects.get(pk=id)
        
    except Dirs.DoesNotExist:
        raise Http404
        return render_to_response('dirs/dir_list.html', {'object_list': p})
        
    children = list(p.getDirs())#list(Dirs.objects.filter(parent=id))
    
    if (children):
        try:
            back = p.parent #Django knows from the model that this is a foreign key, so dirs.objects... is not needed
            back.fullname = '..'
            children.insert(0, back)
        except Dirs.DoesNotExist:
            return render_to_response('dirs/dir_list.html', {'object_list': children})
    totaltime = datetime.datetime.now() - time
    response = render_to_string('dirs/dir_list.html', {'object_list': children, 'time': totaltime, 'number_entries': children.__len__(),} , context_instance=None)
    totaltime = datetime.datetime.now() - time
    response = '<p> <blockquote> Execution and render time: ' + totaltime.__str__() + ' </blockquote> </p>' + response
    return HttpResponse(response)#render_to_response('dirs/dir_list.html', {'object_list': children, 'time': totaltime})

def plainbydir(request, theid):    
    time = datetime.datetime.now()
    try:
        #Directory you want to show its content
        root = Dirs.objects.get(pk=theid)   
    except Dirs.DoesNotExist:
        raise Http404
        return render_to_response('dirs/dir_list.html', {'object_list': root}) 
    
    imagewidth = 800.0
    imageheight = 600.0
    
    lr = LevelRules()
    
    lr.addRules('sites.dirs.models', 'Dirs', 'getFilesAndFolders', 'countFilesAndDirs', 'getDirParent', 'totalsize', 0)
    lr.addRules('sites.dirs.models', 'CnsFileMetadata', 'getChildren', 'countChildren', 'getDirParent', 'filesize', 0)
    
    lr.addRules('sites.dirs.models', 'Dirs', 'getFilesAndFolders', 'countFilesAndDirs', 'getDirParent', 'totalsize', 1)
    lr.addRules('sites.dirs.models', 'CnsFileMetadata', 'getChildren', 'countChildren', 'getDirParent', 'filesize', 1)
    
    lr.addRules('sites.dirs.models', 'Dirs', 'getFilesAndFolders', 'countFilesAndDirs', 'getDirParent', 'totalsize', 2)
    lr.addRules('sites.dirs.models', 'CnsFileMetadata', 'getChildren', 'countChildren', 'getDirParent', 'filesize', 2)
    
    lr.addRules('sites.dirs.models', 'Dirs', 'getFilesAndFolders', 'countFilesAndDirs', 'getDirParent', 'totalsize', 3)
    lr.addRules('sites.dirs.models', 'CnsFileMetadata', 'getChildren', 'countChildren', 'getDirParent', 'filesize', 3)
    
    lr.addRules('sites.dirs.models', 'Dirs', 'getFilesAndFolders', 'countFilesAndDirs', 'getDirParent', 'totalsize', 4)
    lr.addRules('sites.dirs.models', 'CnsFileMetadata', 'getChildren', 'countChildren', 'getDirParent', 'filesize', 4)

    lr.addRules('sites.dirs.models', 'Dirs', 'getFilesAndFolders', 'countFilesAndDirs', 'getDirParent', 'totalsize', 5)
    lr.addRules('sites.dirs.models', 'CnsFileMetadata', 'getChildren', 'countChildren', 'getDirParent', 'filesize', 5)
    
    lr.addRules('sites.dirs.models', 'Dirs', 'getFilesAndFolders', 'countFilesAndDirs', 'getDirParent', 'totalsize', 6)
    lr.addRules('sites.dirs.models', 'CnsFileMetadata', 'getChildren', 'countChildren', 'getDirParent', 'filesize', 6)
    
    lr.addRules('sites.dirs.models', 'Dirs', 'getFilesAndFolders', 'countFilesAndDirs', 'getDirParent', 'totalsize', 7)
    lr.addRules('sites.dirs.models', 'CnsFileMetadata', 'getChildren', 'countChildren', 'getDirParent', 'filesize', 7)
    
    start = datetime.datetime.now()
    
    print 'start generating object tree for ' + root.__str__()
    tb = TreeBuilder(lr)
    otree = None
    
    if root.__str__() == "/castor/cern.ch/totem/offlinea":
        print 'profiling'
        profile.runctx('otree = tb.generateObjectTree(root, \'bla\')', globals(), {'tb':tb, 'root':root})
    else:
        otree = tb.generateObjectTree(root, 'bla')
        
    print 'time was: ' + (datetime.datetime.now() - start ).__str__()
    print ''
    
    start = datetime.datetime.now()
    print 'start calculating rectangle sizes'
    if root.__str__() == "/castor/cern.ch/cms/store":
        print "debug here"
        
    props = BasicViewTreeProps(width = imagewidth, height = imageheight)    
    tc = SquaredTreemapCalculator(otree = otree, basic_properties = props)

    tree = tc.calculate()
        
    print 'time was: ' + (datetime.datetime.now() - start ).__str__()
    print ''
    
    print "linking metrics to graphical properties"
    mlinker = MetricsLinker()
    mlinker.addPropertyLink('Dirs', 'fillcolor', LevelDimension())
    mlinker.addPropertyLink('CnsFileMetadata', 'fillcolor', LevelDimension())
    mlinker.addPropertyLink('Dirs', 'headertext', RawColumnDimension('name', DirNameTransformator('/')))
    mlinker.addPropertyLink('CnsFileMetadata', 'headertext', RawColumnDimension('name'))
    mlinker.addPropertyLink('Dirs', 'htmlinfotext', DirHtmlInfoDimension())
    mlinker.addPropertyLink('CnsFileMetadata', 'htmlinfotext', FileHtmlInfoDimension())
    
    print "designing tree"
    designer = SquaredTreemapDesigner( vtree = tree, metricslinkage = mlinker)
    designer.designTreemap()
    
    print "drawing something"
    drawer = SquaredTreemapDrawer(tree)
    filenm = root.fileid.__str__().replace('/','') + "test_tree.png"
    print filenm
    drawer.drawTreemap(filenm)
    #------------------------------------------------------------
    print "preparing response"
    
    parentid = tree.getRoot().getProperty('treenode').getNakedParent().fileid
    print "PARENTID ", parentid
    parentidstr = parentid.__str__()
    
    nodes = tree.getAllNodes()
    mapparams = [None] * len(nodes)
    
    for (idx, node) in enumerate(nodes):
        x1 = int(round(node.getProperty('x'),0))
        y1 = int(round(node.getProperty('y'),0))
        x2 = int(round(node.getProperty('x') + node.getProperty('width'),0))  
        hsize = node.getProperty('headersize')
        if((not(tree.nodeHasChildren(node)))):
            hsize = node.getProperty('height')
        y2 = int(round(node.getProperty('y') + hsize,0))
        
#        coord =  (x1,y1,x2,y2).__str__()[1:-1]#(''.join([num for num in (x1,y1,x2,y2).__str__()]))[1:-1]    
        theid = node.getProperty('treenode').getObject().fileid
        info = node.getProperty('htmlinfotext')
        
        mapparams[idx] = (x1,y1,x2,y2,theid,info)
        
    rt = tree.getRoot().getProperty('treenode').getObject()
    parents = []
    
    while True:
        modulename = inspect.getmodule(rt).__name__
        classname = rt.__class__.__name__
        level = 0
        
        parentmethodname =  lr.getParentMethodNameFor(level, modulename, classname)
        pr = rt.__class__.__dict__[parentmethodname](rt)
        if rt is pr: break
        parents.append(pr)

        rt = pr

    
    parents.reverse()
    navlinkparts = []
    for pr in parents:  
        navlinkparts.append( (pr.name, pr.fullname, pr.fileid) )
        
    del otree
    del tree
    response = render_to_string('dirs/imagemap.html', {'nodes': nodes, 'parentid': parentidstr, 'filename': filenm, 'mapparams': mapparams, 'navilink': navlinkparts, 'imagewidth': imagewidth, 'imageheight': imageheight} , context_instance=None)
    totaltime = datetime.datetime.now() - time
    response = response + '<p> <blockquote> Execution and render time: ' + totaltime.__str__() + ' </blockquote> </p>'
    return HttpResponse(response)

