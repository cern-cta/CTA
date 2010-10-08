# Create your views here.
from django.conf import settings
from django.conf.urls.defaults import *
from django.core.cache import cache
from django.http import Http404, HttpResponse
from django.shortcuts import redirect, render_to_response
from django.template import resolve_variable
from django.template.loader import render_to_string
from django.utils.hashcompat import md5_constructor
from django.utils.http import urlquote
from sites.dirs.models import *
from sites.tools.Inspections import *
from sites.treemap.defaultproperties.SquaredViewProperties import *
from sites.treemap.drawing.TreeDesigner import SquaredTreemapDesigner
from sites.treemap.drawing.TreemapDrawers import SquaredTreemapDrawer
from sites.treemap.drawing.metricslinking.MetricsLinker import MetricsLinker
from sites.treemap.drawing.metricslinking.TreeNodeDimensions import *
from sites.treemap.objecttree.Postprocessors import *
from sites.treemap.objecttree.TreeBuilder import TreeBuilder
from sites.treemap.objecttree.TreeRules import LevelRules
from sites.treemap.viewtree.TreeCalculators import SquaredTreemapCalculator
import re
from sites.tools.Inspections import  getDefaultNumberOfLevels

import sites.dirs.Presets


def redirectOldLink(request, *args, **kwargs):
    return HttpResponse("Please use the new link: <a href = \"" + settings.PUBLIC_APACHE_URL + "/treemaps/\"> here </a>")
#    return treeView(request, 'Dirs', theid)

def redirectHome(request, *args, **kwargs):
    return redirect(to = settings.PUBLIC_APACHE_URL + '/treemaps/0_Dirs_')

def treeView(request, presetid, rootmodel, theid, refresh_cache = False):  
    time = datetime.datetime.now()
    presetid = int(presetid)
    
    imagewidth = 800.0
    imageheight = 600.0
    nbdefinedlevels = getDefaultNumberOfLevels()

    serverdict = settings.LOCAL_APACHE_DICT
    treemapdir = settings.REL_TREEMAP_DICT 
    
    #load levelRules from cookie, if cookie doesn't exist, load defaults
    lr = getCookieRules(request, nbdefinedlevels)
    #an import of getPresetByStaticId from Presets won't work! you have to give an full path here!
    #for some reason mod_python can't import Presets correctly and outputs useless error messages
    lr = sites.dirs.Presets.getPresetByStaticId(presetid).lr
    
    avmodels = lr.getRuleObject(0).getUsedClassNames()

    if rootmodel not in avmodels:
        avmodels.remove('Annex')
        if len(avmodels) == 0: raise Exception("no sufficient rules for level 0") 
        theid = ''
        rootmodel = avmodels[0]
        return treeView(request = request,rootmodel = rootmodel, theid = theid, refresh_cache = refresh_cache)
        
    if rootmodel in getModelsNotToCache(): refresh_cache = True
    
    cache_key = calcCacheKey(presetid = presetid, theid = theid, parentmodel = rootmodel, lr = lr)
    cache_expire = settings.CACHE_MIDDLEWARE_SECONDS
    value = cache.get(cache_key)
    #if already in cache
    #if already in cache
    cache_hit = False
    if (value is not None): cache_hit = True
    if cache_hit and not refresh_cache:
        return HttpResponse(value)
    
    root = getRootObjectForTreemap(rootmodel, theid)
    
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
    mlinker = getDefaultMetricsLinking()
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
    filenm = hash(root.getIdReplacement()).__str__() + str(presetid) + lr.getUniqueLevelRulesId() + "treemap.png"
    print filenm
    drawer.drawTreemap(serverdict + treemapdir + "/" + filenm)
    print 'time until now was: ' + (datetime.datetime.now() - start ).__str__()
    #------------------------------------------------------------
    start = datetime.datetime.now()
    response = respond (request = request, vtree = tree, tooltipfontsize = 12, imagewidth = imagewidth, imageheight = imageheight,\
    filenm = filenm, lrules = lr, cache_key = cache_key, cache_expire = cache_expire, time = time, rootsuffix = root.getIdReplacement(), nblevels = nbdefinedlevels, presetid = presetid)
    
    del tree
    del otree
    print 'time until now was: ' + (datetime.datetime.now() - start ).__str__()
    
    return response

#@cache_page(60 *60 * 24 * 3) #cache for 3 days
def groupView(request, presetid, model, depth, theid, refresh_cache = False):    
    time = datetime.datetime.now()
    depth = int(depth)
    presetid = int(presetid)
    
    prefix = theid[:(len(model)+1)]
    if(prefix == model + "_"):
        urlrest = theid[(len(model)+1):]
    
    if model in getModelsNotToCache(): refresh_cache = True
    
    imagewidth = 800.0
    imageheight = 600.0
    nbdefinedlevels = getDefaultNumberOfLevels()

    serverdict = settings.LOCAL_APACHE_DICT
    treemapdir = settings.REL_TREEMAP_DICT
    
    cookielr = getCookieRules(request, nbdefinedlevels)
    #an import of getPresetByStaticId from Presets won't work! you have to give an full path here!
    #for some reason mod_python can't import Presets correctly and outputs useless error messages
    cookielr = sites.dirs.Presets.getPresetByStaticId(presetid).lr
        
    cache_key = calcCacheKey(presetid = presetid, theid = theid, parentmodel = "Annex", depth = depth, lr = cookielr)
    cache_expire = settings.CACHE_MIDDLEWARE_SECONDS
    value = cache.get(cache_key)
    #if already in cache
    cache_hit = False
    if (value is not None): cache_hit = True
    if cache_hit and not refresh_cache:
        return HttpResponse(value)
    
    #define LevelRules
    lr = LevelRules()
    
    #define only the first 2 levels because we only need the level 1 to see if there is an Annex in full size
    for i in range(2):
        lr.appendRuleObject(cookielr.getRuleObject(i))
    
    root = getRootObjectForTreemap(model, urlrest)
    
    start = datetime.datetime.now()
    print 'start generating first object tree ' + root.__str__()
    tb = TreeBuilder(lr)

    otree = tb.generateObjectTree(rootobject = root)    
    print 'time until now was: ' + (datetime.datetime.now() - start ).__str__()
    
    #---------------------------------------------
    otree.traveseToRoot()
    anxnode = otree.getRootAnnex()
    
    #if there is an Annex then display it, otherwise you can just display the directory without it
    if anxnode is not None:
        anx = anxnode.getObject()
        for i in range(depth):
            print 'start generating object subtree ' + anx.__str__()
            otree = tb.generateObjectTree(anx)
            
            newanxnode = otree.getRootAnnex()
            
            if newanxnode is not None: 
                anx = anx.getCopyWithIncDepth(newexcluded = newanxnode.getObject().getExcludedNodes(), evaluation = newanxnode.getEvalValue())    
            else:
                raise Http404 
        
        lr = LevelRules()        
        for i in range(nbdefinedlevels):
            lr.appendRuleObject(cookielr.getRuleObject(i))  
        
        tb = TreeBuilder(lr)
        otree = tb.generateObjectTree(anx) 
            
    else: #no Annex display needed
        
        lr = LevelRules()
        for i in range(nbdefinedlevels):
            lr.appendRuleObject(cookielr.getRuleObject(i))  
            
        tb = TreeBuilder(lr)    
        otree = tb.generateObjectTree(rootobject = root)     
        
     
    start = datetime.datetime.now()
    print 'start calculating rectangle sizes'
        
    props = BasicViewTreeProps(width = imagewidth, height = imageheight)    
    tc = SquaredTreemapCalculator(otree = otree, basic_properties = props)

    tree = tc.calculate()
        
    print 'time until now was: ' + (datetime.datetime.now() - start ).__str__()

    start = datetime.datetime.now()
    print "linking metrics to graphical properties"
    mlinker = getDefaultMetricsLinking()
    print 'time until now was: ' + (datetime.datetime.now() - start ).__str__()
    
    start = datetime.datetime.now()
    print "designing tree"
    designer = SquaredTreemapDesigner( vtree = tree, metricslinkage = mlinker)
    designer.designTreemap()
    print 'time until now was: ' + (datetime.datetime.now() - start ).__str__()
    
    start = datetime.datetime.now()
    print "drawing something"
    drawer = SquaredTreemapDrawer(tree)
    filenm = "Annex" + hash(root.getIdReplacement()).__str__() + str(presetid) + str(depth) + lr.getUniqueLevelRulesId() + "treemap.png"
    print filenm
    drawer.drawTreemap(serverdict + treemapdir + "/" + filenm)
    print 'time until now was: ' + (datetime.datetime.now() - start ).__str__()
    #------------------------------------------------------------
    start = datetime.datetime.now()
    response = respond (request = request, vtree = tree, tooltipfontsize = 12, imagewidth = imagewidth, imageheight = imageheight,\
    filenm = filenm, lrules = lr, cache_key = cache_key, cache_expire = cache_expire, time = time, rootsuffix = root.getIdReplacement(), nblevels = nbdefinedlevels, presetid = presetid)
    
    del tree
    del otree
    print 'time until now was: ' + (datetime.datetime.now() - start ).__str__()
    return response

def redir(request, urlending, refreshcache, presetid, newmodel = None, idsuffix = ''):
    treeviewexpr = r'(?P<presetid>\d+)_(?P<rootmodel>\w+)_(?P<theid>.*)'
    groupviewexpr = r'(?P<presetid>\d+)_group_(?P<model>\w+)_(?P<depth>\d+)_(?P<theid>.*)', 'dirs.views.groupView'
    
    try:
        if re.match(treeviewexpr, urlending):
            match = re.match(treeviewexpr, urlending)
            theid = match.group('theid')
            model = match.group('rootmodel')
            if (newmodel is not None) and (newmodel in getAvailableModels()) and (model != newmodel):
                model = newmodel
                theid = idsuffix
            
            #generate the new link to redirect
            rediraddr = request.path
            pos = rediraddr.rfind(urlending)
            if pos == -1: raise Exception('invalid path in the response object')          
            rediraddr = rediraddr[:pos]
            rediraddr = rediraddr + str(presetid) + "_" + model  + "_" + theid
                
            return redirect(to = rediraddr, args = {'request':request, 'presetid':presetid, 'theid':theid, 'rootmodel': model, 'refresh_cache': refreshcache })
        elif re.match(groupviewexpr, urlending):
            match = re.match(treeviewexpr, urlending)
            theid = match.group('theid')
            depth = match.group('depth')
            model = match.group('model')
            if (newmodel is not None) and (newmodel in getAvailableModels()) and (model != newmodel):
                model = newmodel
                theid = idsuffix
                
            #generate the new link to redirect
            rediraddr = request.path
            pos = rediraddr.rfind(urlending)
            if pos == -1: raise Exception('invalid path in the response object')          
            rediraddr = rediraddr[:pos]
            rediraddr = rediraddr + str(presetid) + "_" + model  + "_" + theid
            
            return redirect(to = rediraddr, args = {'request':request,'theid':theid, 'presetid': str(presetid), 'depth':depth, 'model':model, 'refresh_cache': refreshcache })
    except:
        return redirect(to = '..', args = {'request':request, 'theid': '/castor', 'presetid':str(0), 'rootmodel': 'Dirs', 'refresh_cache': refreshcache })
    
    return Http404

def preset(request, urlending):
    nblevels = getDefaultNumberOfLevels()
    
    if request.method == 'POST':
        createCookieIfMissing(request, nblevels)
            
        posted = {}
        posted = request.POST
        
        try:
            presetname = posted['preset']
        except KeyError:
            redir(request, urlending, False, 0)
        
        flatview = False
        try:
            posted['flat_view']
            flatview = True
        except KeyError:
            pass
        
        smalltobig = False
        try:
            posted['smalltobig']
            smalltobig = True
        except KeyError:
            pass
        
        if presetname not in sites.dirs.Presets.getPresetNames():
            redir(request, urlending, False, 0)

        #an import of filterPreset from Presets won't work! you have to give an full path for everything from sites.dirs.Presets
        #for some reason mod_python can't import Presets correctly and outputs useless error messages
        lr = sites.dirs.Presets.filterPreset(sites.dirs.Presets.getPreset(presetname), flatview, smalltobig).lr
            
        request.session['defaultpreset'] = {'flat': flatview, 'presetname': presetname, 'smalltobig': smalltobig}
        request.session['levelrules'] = lr
        
    else:
        raise Http404

    return redir(request, urlending, sites.dirs.Presets.getPreset(presetname).cachingenabled, sites.dirs.Presets.getPreset(presetname).staticid, sites.dirs.Presets.getPreset(presetname).rootmodel, sites.dirs.Presets.getPreset(presetname).rootsuffix)

def respond(request, vtree, tooltipfontsize, imagewidth, imageheight, filenm, lrules, cache_key, cache_expire, time, rootsuffix, nblevels, presetid, metric = ''):
    print "preparing response"
    rootsuffix = str(presetid) + "_" + rootsuffix
    
    apacheserver = settings.PUBLIC_APACHE_URL
#    serverdict = settings.LOCAL_APACHE_DICT
    treemapdir = settings.REL_TREEMAP_DICT
    icondir = settings.REL_ICON_DICT
    
    parentid = vtree.getRoot().getProperty('treenode').getNakedParent().pk
    print "PARENTID ", parentid
    parentidstr = vtree.getRoot().getProperty('treenode').getNakedParent().getIdReplacement()
    
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
        
        linksuffix = str(presetid) + "_" + node.getProperty('treenode').getObject().getIdReplacement()
        info = node.getProperty('htmlinfotext')
        hash = node.getProperty('treenode').getObject().__hash__()
        
        mapparams[idx] = (x1,y1,x2,y2,hash,linksuffix,info)
        
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

#        itemwidth = int(round(x2-x1))
#        itemheight = int(round(y2-y1)) 
        
        shiftx = 20
        shifty = 20
        
        if(x1 + shiftx + tooltipwidth) > imagewidth:
            shiftx = round(imagewidth - (x1 + tooltipwidth))
            
        if(y1 + shifty) > imageheight + 50:
            shifty = 50
#        elif (y1 + shifty) > imageheight and shifty <= 20:
#            shifty = 2*shifty   
        tooltipshift[idx] = (shiftx, shifty, tooltipwidth, tooltipheight, hash, linksuffix)
        
    rt = vtree.getRoot().getProperty('treenode').getObject()
    parents = []
    
    while True:
        classname = rt.__class__.__name__
        level = 0
        
        parentmethodname =  lrules.getParentMethodNameFor(level, classname)
        pr = rt.__class__.__dict__[parentmethodname](rt)
        if rt is pr: break
        parents.append(pr)

        rt = pr

    
    parents.reverse()
    navlinkparts = []
    for pr in parents:  
        nvtext = pr.__dict__[getNaviName(pr.__class__.__name__)]
        pos = nvtext.rfind('/')
        if pos == -1: pos = 0
        nvtext = nvtext[pos:]   
           
        navlinkparts.append( (nvtext, str(pr), str(presetid) + "_" + pr.getIdReplacement()) )
    
    generationtime = datetime.datetime.now() - time
    cookierules = getCookieRules(request, nblevels).getRules()
    
    presetnames = sites.dirs.Presets.getPresetNames()
    presetnames.sort();
    
    response = render_to_string('dirs/imagemap.html', \
    {'nodes': nodes, 'parentid': parentidstr, 'filename': filenm, 'mapparams': mapparams, 'navilink': navlinkparts, 'imagewidth': int(imagewidth), 'imageheight': int(imageheight),\
     'tooltipfontsize': tooltipfontsize,'tooltipshift': tooltipshift, 'treemapdir': apacheserver + treemapdir, 'icondir': apacheserver + icondir, \
     'rootsuffix': rootsuffix, 'generationtime': generationtime, 'cookierules': cookierules, 'presetnames': presetnames, \
     'presetdefault':getCurrentPresetSelections(request)} , context_instance=None)
    
    totaltime = datetime.datetime.now() - time
#    response = response + '<!-- <p> <blockquote> Execution and render time: ' + totaltime.__str__() + ' </blockquote> </p> -->'
    print "total time: ", totaltime
    
    request.session.set_test_cookie()
    cache.add(cache_key, response, cache_expire)
    return HttpResponse(response)

class DropDownEntry(object):
    def __init__(self, text, value):
        self.text = text
        self.value = value

def createCookieIfMissing(request, nblevels):
    try:
        request.session['userhascookie']
        request.session['levelrules']
        request.session['levelrules'].getPostProcessorNameFor(0,'Annex')
        request.session['defaultpreset']
        request.session['defaultpreset']['smalltobig']
        if not request.session['levelrules'].rulesAreValid(): 
            raise AttributeError("Rules not valid or from an older version")
    except (KeyError, AttributeError):
        request.session['userhascookie'] = True
        request.session['levelrules'] = getDefaultRules(nblevels)
        request.session['defaultpreset'] = getDefaultPresets()     
    
def getCurrentPresetSelections(request):
    try:
        request.session['userhascookie']
        request.session['defaultpreset']['smalltobig']
        if request.session['defaultpreset']['presetname'] not in sites.dirs.Presets.getPresetNames():
            request.session['defaultpreset'] = getDefaultPresets()
            
        return request.session['defaultpreset']
    except KeyError:
        request.session['userhascookie'] = True
        return getDefaultPresets()

def getCookieRules(request, nbdefinedlevels):
    lr = None
    createCookieIfMissing(request, nbdefinedlevels)
    lr = request.session['levelrules']
    return lr

def calcCacheKey(theid, presetid, parentmodel, lr, depth = 0):
    key_prefix = settings.CACHE_MIDDLEWARE_KEY_PREFIX
    fragment_name = hash(theid).__str__() + '_'+ depth.__str__() + '_'+ ''.join([ord(bla).__str__() for bla in parentmodel.__str__()])
    args = md5_constructor(u':'.join([urlquote(resolve_variable("185", hash(theid).__str__() + '_'+ fragment_name))]))     
    lrkeypart = lr.getUniqueLevelRulesId()            
    cache_key = 'template.cache%s%s%s%s%s' % (key_prefix, fragment_name, args.hexdigest(), lrkeypart, str(presetid))
    return cache_key

def getDefaultRules(nblevels):
    lr = sites.dirs.Presets.getPreset("Default (Directory structure)").lr
    return lr

def getDefaultModel():
    return 'Dirs'

def getDefaultPresets():
    return{'flat': False, 'presetname': 'Default', 'smalltobig': False}
    
def getDefaultMetricsLinking():
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
    
    mlinker.addPropertyLink('Requestsatlas', 'fillcolor', LevelDimension())
    mlinker.addPropertyLink('Requestsatlas', 'htmlinfotext', RequestsHtmlInfoDimension())
    mlinker.addPropertyLink('Requestsatlas', 'headertext', RawColumnDimension('namepart', TopDirNameTransformator()))
    
    mlinker.addPropertyLink('Requestscms', 'fillcolor', LevelDimension())
    mlinker.addPropertyLink('Requestscms', 'htmlinfotext', RequestsHtmlInfoDimension())
    mlinker.addPropertyLink('Requestscms', 'headertext', RawColumnDimension('namepart', TopDirNameTransformator()))

    return mlinker

def getRootObjectForTreemap(rootmodel, urlrest):
    try:
        #Directory you want to show its content
        root = findObjectByIdReplacementSuffix(rootmodel, urlrest)
    except Dirs.DoesNotExist:
        raise Http404
        return render_to_response("Error") 
    
    return root
