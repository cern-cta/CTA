# Create your views here.
from django.conf import settings
from django.conf.urls.defaults import *
from django.core.cache import cache
from django.core.urlresolvers import reverse
from django.db.models.query import QuerySet
from django.http import Http404, HttpResponse, HttpResponseRedirect
from django.shortcuts import render_to_response, render_to_response, \
    get_list_or_404, get_object_or_404
from django.template import Context, loader, resolve_variable
from django.template.loader import render_to_string
from django.utils.hashcompat import md5_constructor
from django.utils.http import urlquote
from django.views.decorators.cache import cache_page
from sites.dirs.models import *
from sites.tools.GroupIdService import resolveGroupId
from sites.treemap.defaultproperties.SquaredViewProperties import *
from sites.treemap.drawing.TreeDesigner import SquaredTreemapDesigner
from sites.treemap.drawing.TreemapDrawers import SquaredTreemapDrawer
from sites.treemap.drawing.metricslinking.MetricsLinker import MetricsLinker
from sites.treemap.drawing.metricslinking.TreeNodeDimensions import *
from sites.treemap.objecttree.Annex import Annex
from sites.treemap.objecttree.ObjectTree import ObjectTree
from sites.treemap.objecttree.TreeBuilder import TreeBuilder
from sites.treemap.objecttree.TreeNode import TreeNode
from sites.treemap.objecttree.TreeRules import LevelRules
from sites.treemap.viewtree.TreeCalculators import SquaredTreemapCalculator
import datetime
import profile
from django import forms
from django.db import models
from django.db.models.base import ModelBase
from sites.tools.ObjectCreator import createObject
from sites.tools.ColumnFinder import ColumnFinder


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

#@cache_page(60 *60 * 24 * 3) #cache for 3 days

#def plainbydirprofiling(request, theid): 
#    print 'profiling'
#    response = None
#    profile.runctx('response =  plainbydir1(request, theid)', globals(), {'request':request, 'theid': theid, 'response':response})
#    return response
#    #plainbydir1(request, theid)

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
    nbdefinedlevels = 8

    serverdict = settings.LOCAL_APACHE_DICT
    treemapdir = settings.REL_TREEMAP_DICT
    
    #caching part
    key_prefix = settings.CACHE_MIDDLEWARE_KEY_PREFIX
    fragment_name = theid.__str__()
    vary_on = [(2,3,5,7),(11,13,17,19,23,29)]
    args = md5_constructor(u':'.join([urlquote(resolve_variable("145", theid))]))                 
    cache_key = 'template.cache.%s.%s.%s' % (key_prefix, fragment_name, args.hexdigest())
    cache_expire = settings.CACHE_MIDDLEWARE_SECONDS
    
    value = cache.get(cache_key)
    
    #if already in cache
    if value is not None:
        return HttpResponse(value)

    lr = LevelRules()
    
    for i in range(nbdefinedlevels):
        lr.addRules('sites.dirs.models', 'Dirs', 'getFilesAndFolders', 'countFilesAndDirs', 'getDirParent', 'totalsize', i)
        lr.addRules('sites.dirs.models', 'CnsFileMetadata', 'getChildren', 'countChildren', 'getDirParent', 'filesize', i)
        lr.addRules('sites.treemap.objecttree.Annex', 'Annex', 'getItems', 'countItems', 'getAnnexParent', 'evaluation', i)
    
    start = datetime.datetime.now()
    
    print 'start generating object tree for ' + root.__str__()
    tb = TreeBuilder(lr)
    
    otree = tb.generateObjectTree(rootobject = root) 
    print 'time until now was: ' + (datetime.datetime.now() - start ).__str__()
    
    start = datetime.datetime.now()
    print 'start calculating rectangle sizes'
        
    props = BasicViewTreeProps(width = imagewidth, height = imageheight)    
    tc = SquaredTreemapCalculator(otree = otree, basic_properties = props)

    tree = tc.calculate()
    print 'time until now was: ' + (datetime.datetime.now() - start ).__str__()
    
    start = datetime.datetime.now()
    print "linking metrics to graphical properties"
    mlinker = MetricsLinker()
    mlinker.addPropertyLink('Annex', 'strokecolor', ConstantDimension(-1))
    mlinker.addPropertyLink('Annex', 'inbordersize', ConstantDimension(2))
    mlinker.addPropertyLink('Annex', 'htmlinfotext', AnnexHtmlInfoDimension())
    mlinker.addPropertyLink('Annex', 'fillcolor', ConstantDimension(-2))
    mlinker.addPropertyLink('Annex', 'radiallight.opacity', ConstantDimension(0.0))
    
    mlinker.addPropertyLink('Dirs', 'fillcolor', LevelDimension())
    mlinker.addPropertyLink('Dirs', 'htmlinfotext', DirHtmlInfoDimension())
    mlinker.addPropertyLink('CnsFileMetadata', 'fillcolor', LevelDimension())
    mlinker.addPropertyLink('Dirs', 'headertext', RawColumnDimension('name', DirNameTransformator('/')))
    mlinker.addPropertyLink('CnsFileMetadata', 'headertext', RawColumnDimension('name'))
    mlinker.addPropertyLink('Dirs', 'htmlinfotext', DirHtmlInfoDimension())
    mlinker.addPropertyLink('CnsFileMetadata', 'htmlinfotext', FileHtmlInfoDimension())
    
    mlinker.addPropertyLink('CnsFileMetadata', 'headertext.isbold', ConstantDimension(False))
    print 'time until now was: ' + (datetime.datetime.now() - start ).__str__()
    
    start = datetime.datetime.now()
    print "designing tree"
    designer = SquaredTreemapDesigner( vtree = tree, metricslinkage = mlinker)
#    profile.runctx('designer.designTreemap()', globals(), {'designer':designer})
    designer.designTreemap()
    print 'time until now was: ' + (datetime.datetime.now() - start ).__str__()
    
    start = datetime.datetime.now()
    print "drawing something"
    drawer = SquaredTreemapDrawer(tree)
    filenm = root.pk.__str__().replace('/','') + "treemap.png"
    print filenm
    drawer.drawTreemap(serverdict + treemapdir + "/" + filenm)
    print 'time until now was: ' + (datetime.datetime.now() - start ).__str__()
    #------------------------------------------------------------
    start = datetime.datetime.now()
    response = respond (vtree = tree, tooltipfontsize = 12, imagewidth = imagewidth, imageheight = imageheight,\
    filenm = filenm, lrules = lr, cache_key = cache_key, cache_expire = cache_expire, time = time, rootid = root.pk, nblevels = nbdefinedlevels)
    
    del tree
    del otree
    print 'time until now was: ' + (datetime.datetime.now() - start ).__str__()
    
    return response

#@cache_page(60 *60 * 24 * 3) #cache for 3 days
def groupView(request, parentpk, depth, model):    
    depth = int(depth)
    time = datetime.datetime.now()
    try:
        #Directory you want to show its content
        root = None
        command = 'root = ' + model + '.objects.get(pk=' + parentpk.__str__() + ')'
        exec(command)
    except:
        raise Http404
    
    imagewidth = 800.0
    imageheight = 600.0
    nbdefinedlevels = 8

    serverdict = settings.LOCAL_APACHE_DICT
    treemapdir = settings.REL_TREEMAP_DICT
    
    #caching part
    key_prefix = settings.CACHE_MIDDLEWARE_KEY_PREFIX
    fragment_name = parentpk.__str__() + '_'+ depth.__str__() + '_'+ ''.join([ord(bla).__str__() for bla in model.__str__()])
    vary_on = [(2,3,5,7),(11,17,19,23,29)]
    args = md5_constructor(u':'.join([urlquote(resolve_variable("185", parentpk.__str__() + '_'+ fragment_name))]))                 
    cache_key = 'template.cache.%s.%s.%s' % (key_prefix, fragment_name, args.hexdigest())
    cache_expire = settings.CACHE_MIDDLEWARE_SECONDS
    
    value = cache.get(cache_key)
    
    #if the response is cached
    if value is not None:
        return HttpResponse(value)
    
    #define LevelRules
    lr = LevelRules()
    
    #define only the first 2 levels because we only need the level 1 to see if there is an Annex in full size
    for i in range(2):
        lr.addRules('sites.dirs.models', 'Dirs', 'getFilesAndFolders', 'countFilesAndDirs', 'getDirParent', 'totalsize', i)
        lr.addRules('sites.dirs.models', 'CnsFileMetadata', 'getChildren', 'countChildren', 'getDirParent', 'filesize', i)
        lr.addRules('sites.treemap.objecttree.Annex', 'Annex', 'getItems', 'countItems', 'getAnnexParent', 'evaluation', i) 
    
    
    start = datetime.datetime.now()
    print 'start generating first object tree ' + root.__str__()
    tb = TreeBuilder(lr)

    otree = tb.generateObjectTree(rootobject = root)    
    print 'time until now was: ' + (datetime.datetime.now() - start ).__str__()
    
    #---------------------------------------------
    otree.traveseToRoot()
    anxnode = otree.getRootAnnex()
    anx = anxnode.getObject()
    
    #if there is an Annex then display it, otherwise you can just display the directory without it
    if anxnode is not None:
        for i in range(depth):
            print 'start generating object subtree ' + anx.__str__()
            otree = tb.generateObjectTree(anx)
            
            newanxnode = otree.getRootAnnex()
            
            if newanxnode is not None: 
                anx = anx.getCopyWithIncDepth(newexcluded = newanxnode.getObject().getExcludedNodes(), evaluation = newanxnode.getEvalValue())    
            else:
                raise Http404 
                
        for i in range(2,nbdefinedlevels):
            lr.addRules('sites.dirs.models', 'Dirs', 'getFilesAndFolders', 'countFilesAndDirs', 'getDirParent', 'totalsize', i)
            lr.addRules('sites.dirs.models', 'CnsFileMetadata', 'getChildren', 'countChildren', 'getDirParent', 'filesize', i)
            lr.addRules('sites.treemap.objecttree.Annex', 'Annex', 'getItems', 'countItems', 'getAnnexParent', 'evaluation', i)   
        
        otree = tb.generateObjectTree(anx) 
            
    else: #no Annex display needed
        for i in range(2,nbdefinedlevels):
            lr.addRules('sites.dirs.models', 'Dirs', 'getFilesAndFolders', 'countFilesAndDirs', 'getDirParent', 'totalsize', i)
            lr.addRules('sites.dirs.models', 'CnsFileMetadata', 'getChildren', 'countChildren', 'getDirParent', 'filesize', i)
            lr.addRules('sites.treemap.objecttree.Annex', 'Annex', 'getItems', 'countItems', 'getAnnexParent', 'evaluation', i)
            
            otree = tb.generateObjectTree(rootobject = root)     
        
     
    start = datetime.datetime.now()
    print 'start calculating rectangle sizes'
        
    props = BasicViewTreeProps(width = imagewidth, height = imageheight)    
    tc = SquaredTreemapCalculator(otree = otree, basic_properties = props)

    tree = tc.calculate()
        
    print 'time until now was: ' + (datetime.datetime.now() - start ).__str__()

    start = datetime.datetime.now()
    print "linking metrics to graphical properties"
    mlinker = MetricsLinker()
    mlinker.addPropertyLink('Annex', 'strokecolor', ConstantDimension(-1))
    mlinker.addPropertyLink('Annex', 'inbordersize', ConstantDimension(2))
    mlinker.addPropertyLink('Annex', 'htmlinfotext', AnnexHtmlInfoDimension())
    mlinker.addPropertyLink('Annex', 'fillcolor', ConstantDimension(-2))
    mlinker.addPropertyLink('Annex', 'radiallight.opacity', ConstantDimension(0.0))
    
    mlinker.addPropertyLink('Dirs', 'fillcolor', LevelDimension())
    mlinker.addPropertyLink('Dirs', 'htmlinfotext', DirHtmlInfoDimension())
    mlinker.addPropertyLink('CnsFileMetadata', 'fillcolor', LevelDimension())
    mlinker.addPropertyLink('Dirs', 'headertext', RawColumnDimension('name', DirNameTransformator('/')))
    mlinker.addPropertyLink('CnsFileMetadata', 'headertext', RawColumnDimension('name'))
    mlinker.addPropertyLink('Dirs', 'htmlinfotext', DirHtmlInfoDimension())
    mlinker.addPropertyLink('CnsFileMetadata', 'htmlinfotext', FileHtmlInfoDimension())
    
    mlinker.addPropertyLink('CnsFileMetadata', 'headertext.isbold', ConstantDimension(False))
    print 'time until now was: ' + (datetime.datetime.now() - start ).__str__()
    
    start = datetime.datetime.now()
    print "designing tree"
    designer = SquaredTreemapDesigner( vtree = tree, metricslinkage = mlinker)
    designer.designTreemap()
    print 'time until now was: ' + (datetime.datetime.now() - start ).__str__()
    
    start = datetime.datetime.now()
    print "drawing something"
    drawer = SquaredTreemapDrawer(tree)
    filenm = root.pk.__str__().replace('/','') + "annex"+depth.__str__()+ ".png"
    print filenm
    drawer.drawTreemap(serverdict + treemapdir + "/" + filenm)
    print 'time until now was: ' + (datetime.datetime.now() - start ).__str__()
    #------------------------------------------------------------
    start = datetime.datetime.now()
    response = respond (vtree = tree, tooltipfontsize = 12, imagewidth = imagewidth, imageheight = imageheight,\
    filenm = filenm, lrules = lr, cache_key = cache_key, cache_expire = cache_expire, time = time, rootid = root.pk, nblevels = nbdefinedlevels )
    
    del tree
    del otree
    print 'time until now was: ' + (datetime.datetime.now() - start ).__str__()
    return response

def changeMetrics(request, theid):

    if request.method == 'POST':
        posted = {}
        posted = request.POST
        
        response = "</p><b><u>This is not implemented yet</b></u></p><br><br><p>received POST data: <br><br>"
        for key, value in posted.items():
            response = response + "<b>Form Object:</b> " + key.__str__() + "<br><b>value:</b> " + value.__str__() + "<br><br>"
        response = response + "</p>"
        
    else:
        raise Http404
    
    return HttpResponse(response)

def respond(vtree, tooltipfontsize, imagewidth, imageheight, filenm, lrules, cache_key, cache_expire, time, rootid, nblevels, metric = ''):
    print "preparing response"
    
    apacheserver = settings.PUBLIC_APACHE_URL
    serverdict = settings.LOCAL_APACHE_DICT
    treemapdir = settings.REL_TREEMAP_DICT
    icondir = settings.REL_ICON_DICT
    
    parentid = vtree.getRoot().getProperty('treenode').getNakedParent().fileid
    print "PARENTID ", parentid
    parentidstr = parentid.__str__()
    
    nodes = vtree.getAllNodes()
    mapparams = [None] * len(nodes)
    tooltipshift = [None] * len(nodes)
    
    for (idx, node) in enumerate(nodes):
        x1 = int(round(node.getProperty('x'),0))
        y1 = int(round(node.getProperty('y'),0))
        x2 = int(round(node.getProperty('x') + node.getProperty('width'),0))  
        hsize = node.getProperty('headersize')
        if((not(vtree.nodeHasChildren(node)))):
            hsize = node.getProperty('height')
        y2 = int(round(node.getProperty('y') + hsize,0))
        
        theid = node.getProperty('treenode').getObject().pk.__str__()
        info = node.getProperty('htmlinfotext')
        
        mapparams[idx] = (x1,y1,x2,y2,theid,info)
        
        textlines = 1
        oldpos, fpos = 0 ,0
        fpos = info.find('<br>', fpos)
        maxtooltipwidth = 0
        
        while fpos != -1:
            textlines = textlines + 1
            
            tooltipwidth = 0.0
            for character in info[oldpos:fpos]:
                if character.islower():
                    tooltipwidth = tooltipwidth + tooltipfontsize * 0.61
                else:
                    tooltipwidth = tooltipwidth + tooltipfontsize * 0.66
            tooltipwidth = int(tooltipwidth)
                    
            if tooltipwidth > maxtooltipwidth: maxtooltipwidth = tooltipwidth
            oldpos = fpos
            fpos = info.find('<br>', fpos + 1)

        if maxtooltipwidth > 0: 
            tooltipwidth =  maxtooltipwidth
        else:
            tooltipwidth = 600
            
        tooltipheight = int(round(textlines * 12 * 1.6))

        itemwidth = int(round(x2-x1))
        itemheight = int(round(y2-y1)) 
        
        shiftx = 20
        shifty = 20
        
#        if(shifty) < 20:
#            shifty = shifty * 2
#        else:
#            shifty = shifty + 20
#            
#        if(shiftx) < 20:
#            shiftx = shiftx * 2
#        else:
#            shiftx = shiftx + 20
        
        if(x1 + shiftx + tooltipwidth) > imagewidth:
            shiftx = round(imagewidth - (x1 + tooltipwidth))
            
        if(y1 + shifty) > imageheight + 50:
            shifty = 50
#        elif (y1 + shifty) > imageheight and shifty <= 20:
#            shifty = 2*shifty
            
        tooltipshift[idx] = (shiftx, shifty, tooltipwidth, tooltipheight, theid)
        
    rt = vtree.getRoot().getProperty('treenode').getObject()
    parents = []
    
    while True:
        modulename = inspect.getmodule(rt).__name__
        classname = rt.__class__.__name__
        level = 0
        
        parentmethodname =  lrules.getParentMethodNameFor(level, modulename, classname)
        pr = rt.__class__.__dict__[parentmethodname](rt)
        if rt is pr: break
        parents.append(pr)

        rt = pr

    
    parents.reverse()
    navlinkparts = []
    for pr in parents:  
        navlinkparts.append( (pr.name, pr.fullname, pr.pk) )
          
    leveldropdown, modeldropdown, metricdropdown, childmethoddropdown = generateDropdownValues(nblevels, 'Dirs')
    
    response = render_to_string('dirs/imagemap.html', \
    {'nodes': nodes, 'parentid': parentidstr, 'filename': filenm, 'mapparams': mapparams, 'navilink': navlinkparts, 'imagewidth': int(imagewidth), 'imageheight': int(imageheight),\
     'tooltipfontsize': tooltipfontsize,'tooltipshift': tooltipshift, 'treemapdir': (apacheserver + treemapdir), 'icondir': apacheserver + icondir, \
     'rootid': rootid, 'leveldropdown': leveldropdown, 'modeldropdown': modeldropdown, 'metricdropdown': metricdropdown, 'childmethoddropdown': childmethoddropdown} , context_instance=None)
    
    totaltime = datetime.datetime.now() - time
    response = response + '<p> <blockquote> Execution and render time: ' + totaltime.__str__() + ' </blockquote> </p>'
    
    cache.add(cache_key, response, cache_expire)
    return HttpResponse(response)

def generateDropdownValues(nblevels, themodel):
    #generating drop down menu content
    #create level dropdown
    leveldropdown = []
    leveldropdown.append((-1, 'all levels'))
    for i in range(nblevels):
        leveldropdown.append((i, "level "+i.__str__()))
    
    #look for available models    
    #create model dropdown
    modulename = None
    themodelfound = False
    modeldropdown = []
    if settings.MODELS_LOCATION in settings.INSTALLED_APPS:
        modulename = settings.MODELS_LOCATION + '.models'
        if not modulename in sys.modules.keys():
            try:
                module = __import__( modulename )
            except ImportError, e:
                raise ConfigError( 'Unable to load module: ' + module )
        else:
            module = sys.modules[modulename]
            
        classes = dict(inspect.getmembers( module, inspect.isclass ))
        
        for classname in classes:
            cls = classes[classname]
            if isinstance(cls, ModelBase):
                modelname = cls._base_manager.model._meta.object_name
                if modelname == themodel: themodelfound = True
                modeldropdown.append((modelname, cls.getUserFriendlyName(createObject(modulename , classname))))
    else:
        raise Exception("Configuration Error in settings.py: settings.MODELS_LOCATION must contain a value which is in settings.INSTALLED_APPS ")
    
    if not themodelfound: raise Exception('the given model couldn\'t be found')
    
    metricdropdown = []
    cf = ColumnFinder(modulename, themodel)
    columns = cf.getColumnnames()
    for column in columns:
        metricdropdown.append((column, column))
        
    childmethoddropdown = []
    instance = createObject(modulename, themodel)  
    #check methodname
    found = False
    for membername in instance.__class__.__dict__.keys():
        if ((type(instance.__class__.__dict__[membername]).__name__) == 'function'):
            function = instance.__class__.__dict__[membername]
            try:
                print function.__dict__['methodtype']
                if function.__dict__['methodtype'] == 'children':
                    childmethoddropdown.append((membername, membername))
            except:
                continue
    
    return leveldropdown, modeldropdown, metricdropdown, childmethoddropdown