'''
django views and helper functions
@author: kblaszcz
'''
from app.errors.NoDataAvailableError import NoDataAvailableError
from app.presets.options.BooleanOption import BooleanOption
from app.presets.options.DateOption import DateOption
from app.presets.options.OptionsReader import OptionsReader
from app.presets.options.SpinnerOption import SpinnerOption
from app.tools.ObjectCreator import createObject
from app.tools.StatusTools import deleteStatusFile, generateStatusFile
from app.treemap.defaultproperties.TreeMapProperties import treemap_props, \
    getDefaultDimensionMapping
from app.treemap.drawing.TreeDesigner import SquaredTreemapDesigner
from app.treemap.drawing.TreemapDrawers import SquarifiedTreemapDrawer
from app.treemap.objecttree.RuleMapping import LevelRules
from app.treemap.objecttree.TreeBuilder import TreeBuilder
from app.treemap.viewtree.TreeCalculators import DefaultTreemapCalculator
from django.conf import settings
from django.core.cache import cache
from django.core.exceptions import ObjectDoesNotExist
from django.http import HttpResponse, Http404
from django.shortcuts import redirect, render_to_response
from django.template import resolve_variable
from django.template.loader import render_to_string
from django.utils.hashcompat import md5_constructor
from django.utils.http import urlquote
from app.algorithmstudy.Analysis import doMeasurements
import app.dirs.urls
import app.presets.Presets
import app.tools.Inspections
import copy
import datetime
import os
import profile
import re
import time
from app.dirs.models import *

def redirectOldLink(request, *args, **kwargs):
    return HttpResponse("URL couldn't be resolved. Here is the main link: <a href = \"" + settings.PUBLIC_APACHE_URL + "/treemaps/\"> here </a>")
#    return treeView(request, 'Dirs', theid)

def redirectHome(request, *args, **kwargs):
    return redirect(to = settings.DJANGORESPONSE_URL + '/treemaps/0_Dirs_')

#@cache_page(60 *60 * 24 * 3) #cache for 3 days
def treeView(request, options, presetid, rootmodel, theid, refresh_cache = False, doprofile = False):   
    
    if doprofile:
        profile.runctx("treeView(request = request, options = options, presetid = presetid, rootmodel = rootmodel, theid = theid, refresh_cache = refresh_cache, doprofile = False)", globals(), {'request' : request, 'presetid' : presetid, 'options': options, 'rootmodel':rootmodel, 'theid':theid, 'refresh_cache': refresh_cache})
     
#    doMeasurements()
    thetime = datetime.datetime.now()
    presetid = int(presetid)
    if options is None: options = ''
    treemap_props_cp = copy.copy(treemap_props)
#    checkAndPartiallyCorrectTreemapProps(treemap_props_cp)
    
    if rootmodel in app.tools.Inspections.getModelsNotToCache(): refresh_cache = True
    
    imagewidth = treemap_props_cp['pxwidth']
    imageheight = treemap_props_cp['pxheight']

    serverdict = settings.LOCAL_APACHE_DICT
    treemapdir = settings.REL_TREEMAP_DICT
    
    optr = OptionsReader(options, presetid)
    depth = int(optr.getOption('annexzoom'))
    
    #whenever you see a full path like app.presets.Presets, it is a way to solve a circular dependecy 
    thepreset = app.presets.Presets.getPresetByStaticId(presetid)
    presetlr = app.presets.Presets.filterPreset(thepreset, optr.getOption('flatview'), optr.getOption('smalltobig')).lr
    nbdefinedlevels = presetlr.nbLevels()
        
    cache_key = calcCacheKey(presetid = presetid, theid = theid, parentmodel = "Annex", depth = depth, lr = presetlr, options = optr.getCorrectedOptions(presetid))
    cache_expire = settings.CACHE_MIDDLEWARE_SECONDS
    value = cache.get(cache_key)
    #if already in cache
    cache_hit = False
    if (value is not None): cache_hit = True
    
    #do not cache if no time and span is defined
    notime = False
    try:
        optr.includeasstring['time']
    except KeyError:
        notime = True
        
    if (notime and not optr.getOption('span')): cache_hit = False
    
    statusfilename = getStatusFileNameFromCookie(request)
    if cache_hit and not refresh_cache:
        deleteStatusFile(statusfilename)
        return HttpResponse(value)

    #define only the first 2 levels because we only need the level 1 to see if there is an Annex in full size
    setTruncatedLevelRulesCopy(treemap_props_cp, presetlr, 2)
    
    try:
        globals()[rootmodel].start
        globals()[rootmodel].stop
        globals()[rootmodel].start = optr.getOption('time')-datetime.timedelta(minutes = optr.getOption('span'))
        globals()[rootmodel].stop = optr.getOption('time')
    except AttributeError:
        pass
    
    try:
        root = getRootObjectForTreemap(rootmodel, theid, statusfilename)
        filenm = hash(optr.getCorrectedOptions(presetid)).__str__() + hash(root.getClassName()).__str__() + hash(root.getIdReplacement()).__str__() + str(presetid) + str(depth) + treemap_props_cp['levelrules'].getUniqueLevelRulesId() + ".png"
        
        start = datetime.datetime.now()
        print 'start generating first object tree ' + root.__str__()
        tb = TreeBuilder(treemap_props_cp)
    
        otree = tb.generateObjectTree(rootobject = root, statusfilename = statusfilename)    
        print 'time until now was: ' + (datetime.datetime.now() - start ).__str__()
    except NoDataAvailableError:
        deleteStatusFile(statusfilename)
        return HttpResponse(render_to_string('nodata.html', {'apacheserver': settings.PUBLIC_APACHE_URL} , context_instance=None))
    
    #---------------------------------------------
    otree.traveseToRoot()
    anxnode = otree.getRootAnnex()
    
    #if there is an Annex then display it, otherwise you can just display the directory without it
    if anxnode is not None and depth > 0:
        anx = anxnode.getObject()
        for i in range(depth):
            print 'start generating object subtree ' + anx.__str__()
            otree = tb.generateObjectTree(anx, statusfilename = statusfilename)
            
            newanxnode = otree.getRootAnnex()
            
            if newanxnode is not None: 
                anx = anx.getCopyWithIncDepth(newexcluded = newanxnode.getObject().getExcludedNodes(), evaluation = newanxnode.getObject().getCorrectedEvalValue())    
            else:
                optr.setOption('annexzoom', presetid, 0);
                options = optr.getCorrectedOptions(presetid)
                newurlpart = app.dirs.urls.UrlDefault().buildUrl(presetid = presetid, options = options, rootmodel = rootmodel, theid = theid)
                return redirect(to = settings.DJANGORESPONSE_URL + '/treemaps/' + newurlpart)
         
        treemap_props_cp['levelrules']=presetlr
        tb = TreeBuilder(treemap_props_cp)
        otree = tb.generateObjectTree(rootobject = anx, statusfilename = statusfilename) 
            
    else: #no Annex display needed 
        treemap_props_cp['levelrules']=presetlr
        tb = TreeBuilder(treemap_props_cp)    
        otree = tb.generateObjectTree(rootobject = root, statusfilename = statusfilename)     
        
     
    start = datetime.datetime.now()
    print 'start calculating rectangle sizes'
         
    tc = DefaultTreemapCalculator(treemap_props = treemap_props_cp)

    tree = tc.calculate(optr.getOption('optitext'))
        
    print 'time until now was: ' + (datetime.datetime.now() - start ).__str__()

    start = datetime.datetime.now()
    print "mapping metrics to graphical properties"
    dimmapping = getDefaultDimensionMapping()
    print 'time until now was: ' + (datetime.datetime.now() - start ).__str__()
    
    start = datetime.datetime.now()
    print "designing tree"
    designer = SquaredTreemapDesigner( vtree = tree, treemap_props = treemap_props_cp, mapping = dimmapping)
    designer.designTreemap()
    print 'time until now was: ' + (datetime.datetime.now() - start ).__str__()
    
    start = datetime.datetime.now()
    print "drawing something"
    drawer = SquarifiedTreemapDrawer(treemap_props_cp)

    fullfilepath= serverdict + treemapdir + "/" + filenm
    print fullfilepath

    #wait maximum 6 seconds for the file being unlocked, ignore the lock otherwise
    sleepcounter = 0
    while os.path.exists(fullfilepath + '.lock') and sleepcounter < 6:
        time.sleep(1)
        sleepcounter = sleepcounter + 1

    fileobject = open(fullfilepath + '.lock', 'w+b')
    drawer.drawTreemap(fullfilepath)
    os.remove(fullfilepath + '.lock')
    
    print 'time until now was: ' + (datetime.datetime.now() - start ).__str__()
    #------------------------------------------------------------
    start = datetime.datetime.now()
    response = respond (request = request, vtree = tree, tooltipfontsize = 12, imagewidth = imagewidth, imageheight = imageheight,\
    filenm = filenm, lrules = treemap_props_cp['levelrules'], cache_key = cache_key, cache_expire = cache_expire, thetime = thetime,\
    rootidreplacement = root.getIdReplacement(), rootmodel = root.getClassName(), nblevels = nbdefinedlevels, presetid = presetid,\
    options = optr.getCorrectedOptions(presetid), depth = depth+1)
    
    del tree
    del otree
    deleteStatusFile(statusfilename)
        
    print 'time until now was: ' + (datetime.datetime.now() - start ).__str__()
    return response

#@cache_page(60 *60 * 24 * 3) #cache for 3 days
def tableView(request, options, presetid, rootmodel, theid, refresh_cache = False, doprofile = False):    
#    app.algorithmstudy.Analysis.doMeasurements()
    thetime = datetime.datetime.now()
    presetid = int(presetid)
    if options is None: options = ''
    treemap_props_cp = copy.copy(treemap_props)
#    checkAndPartiallyCorrectTreemapProps(treemap_props_cp)
    
    if rootmodel in app.tools.Inspections.getModelsNotToCache(): refresh_cache = True
    statusfilename = getStatusFileNameFromCookie(request)
    
    imagewidth = treemap_props_cp['pxwidth']
    imageheight = treemap_props_cp['pxheight']

    serverdict = settings.LOCAL_APACHE_DICT
    treemapdir = settings.REL_TREEMAP_DICT
    
    optr = OptionsReader(options, presetid)
    depth = int(optr.getOption('annexzoom'))
    
    #whenever you see a full path like app.presets.Presets, it is a way to solve a circular dependecy 
    thepreset = app.presets.Presets.getPresetByStaticId(presetid)
    presetlr = app.presets.Presets.filterPreset(thepreset, optr.getOption('flatview'), optr.getOption('smalltobig')).lr
    nbdefinedlevels = presetlr.nbLevels()
        
    cache_key = calcCacheKey(presetid = presetid, theid = theid, parentmodel = "Annex", depth = depth, lr = presetlr, options = optr.getCorrectedOptions(presetid))
    cache_expire = settings.CACHE_MIDDLEWARE_SECONDS
    value = cache.get(cache_key)
    #if already in cache
    cache_hit = False
    if (value is not None): cache_hit = True
    
    #do not cache if no time and span is defined
    notime = False
    try:
        optr.includeasstring['time']
    except KeyError:
        notime = True
        
    if (notime and not optr.getOption('span')): cache_hit = False
    
    if cache_hit and not refresh_cache:
        deleteStatusFile(statusfilename)
        return HttpResponse(value)

    #define only the first 2 levels because we only need the level 1 to see if there is an Annex in full size
    setTruncatedLevelRulesCopy(treemap_props_cp, presetlr, 2)
    
    try:
        globals()[rootmodel].start
        globals()[rootmodel].stop
        globals()[rootmodel].start = optr.getOption('time')-datetime.timedelta(minutes = optr.getOption('span'))
        globals()[rootmodel].stop = optr.getOption('time')
    except AttributeError:
        pass

    statusfilename = getStatusFileNameFromCookie(request)
    
    try:
        root = getRootObjectForTreemap(rootmodel, theid, statusfilename)
        filenm = hash(optr.getCorrectedOptions(presetid)).__str__() + hash(root.getClassName()).__str__() + hash(root.getIdReplacement()).__str__() + str(presetid) + str(depth) + treemap_props_cp['levelrules'].getUniqueLevelRulesId() + ".png"
        
        start = datetime.datetime.now()
        print 'start generating first object tree ' + root.__str__()
        tb = TreeBuilder(treemap_props_cp)
    
        otree = tb.generateObjectTree(rootobject = root, statusfilename = statusfilename)    
        print 'time until now was: ' + (datetime.datetime.now() - start ).__str__()
    except NoDataAvailableError:
        deleteStatusFile(statusfilename)
        return HttpResponse(render_to_string('nodata.html', {'apacheserver': settings.PUBLIC_APACHE_URL} , context_instance=None))
    
    #---------------------------------------------
    otree.traveseToRoot()
    anxnode = otree.getRootAnnex()
    
    #if there is an Annex then display it, otherwise you can just display the directory without it
    if anxnode is not None and depth > 0:
        anx = anxnode.getObject()
        for i in range(depth):
            print 'start generating object subtree ' + anx.__str__()
            otree = tb.generateObjectTree(anx, statusfilename = statusfilename)
            
            newanxnode = otree.getRootAnnex()
            
            if newanxnode is not None: 
                anx = anx.getCopyWithIncDepth(newexcluded = newanxnode.getObject().getExcludedNodes(), evaluation = newanxnode.getObject().getCorrectedEvalValue())    
            else:
                optr.setOption('annexzoom', presetid, 0);
                options = optr.getCorrectedOptions(presetid)
                newurlpart = app.dirs.urls.UrlDefault().buildUrl(presetid = presetid, options = options, rootmodel = rootmodel, theid = theid)
                return redirect(to = settings.DJANGORESPONSE_URL + '/treemaps/' + newurlpart)
         
        treemap_props_cp['levelrules']=presetlr
        tb = TreeBuilder(treemap_props_cp)
        otree = tb.generateObjectTree(rootobject = anx, statusfilename = statusfilename) 
            
    else: #no Annex display needed 
        treemap_props_cp['levelrules']=presetlr
        tb = TreeBuilder(treemap_props_cp)    
        otree = tb.generateObjectTree(rootobject = root, statusfilename = statusfilename)     
        
     
    start = datetime.datetime.now()
    print 'start calculating rectangle sizes'
         
    tc = DefaultTreemapCalculator(treemap_props = treemap_props_cp)

    tree = tc.calculate(optr.getOption('optitext'))
            
    print 'time until now was: ' + (datetime.datetime.now() - start ).__str__()

    start = datetime.datetime.now()
    print "mapping metrics to graphical properties"
    dimmapping = getDefaultDimensionMapping()
    print 'time until now was: ' + (datetime.datetime.now() - start ).__str__()
    
    start = datetime.datetime.now()
    print "designing tree"
    designer = SquaredTreemapDesigner( vtree = tree, treemap_props = treemap_props_cp, mapping = dimmapping)
    designer.designTreemap()
    print 'time until now was: ' + (datetime.datetime.now() - start ).__str__()

    #------------------------------------------------------------
    start = datetime.datetime.now()
    response = respondTable (request = request, vtree = tree, tooltipfontsize = 12, imagewidth = imagewidth, imageheight = imageheight,\
    filenm = filenm, lrules = treemap_props_cp['levelrules'], cache_key = cache_key, cache_expire = cache_expire, thetime = thetime,\
    rootidreplacement = root.getIdReplacement(), rootmodel = root.getClassName(), nblevels = nbdefinedlevels, presetid = presetid,\
    options = optr.getCorrectedOptions(presetid), depth = depth+1)
    
    del tree
    del otree
    deleteStatusFile(statusfilename)
        
    print 'time until now was: ' + (datetime.datetime.now() - start ).__str__()
    return response

def preset(request, options,  urlending):
    if options is None: options = ''
    optr = None
    statusfilename = ''
    
    if request.method == 'POST':
            
        posted = {}
        posted = request.POST
        
        try:
            presetname = str(posted['preset'])
        except KeyError:
            redir(request, options, urlending, False, 0)
            
        try:
            statusfilename = str(posted['statusfilename'])
            generateStatusFile(statusfilename, 0)
        except:
            statusfilename = ''
            
        if presetname not in app.presets.Presets.getPresetNames():
            redir(request, options, urlending, False, 0)
            
        preset = app.presets.Presets.getPreset(presetname)
        optr = OptionsReader(options, preset.staticid)    
        validoptions = preset.optionsset
        thetime = None
        
        extractedid = app.dirs.urls.UrlDefault().searchReplacementId(urlending)
        
        for option in validoptions:
            try:
                posted[str(option.getName())]
                #if no error here, continue:
                if(isinstance(option,BooleanOption)):
                        optr.optdict[option.getName()] = True
                        optr.includeasstring[option.getName()] = True

                    
                if(isinstance(option,SpinnerOption)):
                    valuedict = {}
                    try:
                        try: #value posted?
                            valuedict = re.match(option.getValueExpression(), str(posted[option.getName()])).groupdict() 
                            value = int(valuedict['value'])
                        except:
                            try: #value in options?
                                valuedict = re.match(option.getFullExpression(), options).groupdict() 
                                value = int(valuedict['value'])
                            except Exception, e: #value is nowhere: take standard value
                                value = option.getStdVal();
                        

                        optr.optdict[option.getName()] = value
                        optr.includeasstring[option.getName()] = True
                        
                    except:
                        pass
                
                if(isinstance(option,DateOption)):
                    valuedict = {}
                    try:
                        try: #value posted?
                            valuedict = re.match(option.getValueExpression(), str(posted[option.getName()])).groupdict() 
                            thetime = datetime.datetime(int(valuedict['year']), int(valuedict['month']), int(valuedict['day']), int(valuedict['hour']), int(valuedict['minute']), int(valuedict['second']))
                        except:
                            try: #value in options?
                                valuedict = re.match(option.getFullExpression(), options).groupdict() 
                                thetime = datetime.datetime(int(valuedict['year']), int(valuedict['month']), int(valuedict['day']), int(valuedict['hour']), int(valuedict['minute']), int(valuedict['second']))
                            except Exception, e: #value is nowhere: take standard value
                                models = preset.lr.getRuleObject(0).getUsedClassNames()
                                for modelname in models:
                                    try:
                                        thetime = option.getStdVal();
                                        break
                                    except:
                                        pass
                        
                        optr.optdict[option.getName()] = thetime
                        optr.includeasstring[option.getName()] = True
                            
                    except:
                        pass
                    
            except KeyError:
                if(isinstance(option,BooleanOption)):
                    try:
                        optr.optdict[option.getName()]
                        optr.optdict[option.getName()] = False
                    except KeyError:
                        pass
    else:
        raise Http404
    
    options = optr.getCorrectedOptions(preset.staticid)
    return redir(request, options, urlending, app.presets.Presets.getPreset(presetname).cachingenabled, app.presets.Presets.getPreset(presetname).staticid, app.presets.Presets.getPreset(presetname).rootmodel, extractedid, statusfilename)

def respond(request, vtree, tooltipfontsize, imagewidth, imageheight, filenm, lrules, cache_key, cache_expire, thetime, rootidreplacement, rootmodel, nblevels, presetid, options = '', depth = 0):
    print "preparing response"
    optionsstring = '{'+ options +'}'
    #must fit to UrlDefault!
    rootsuffix = app.dirs.urls.UrlDefault().buildUrl(presetid, optionsstring, rootmodel, rootidreplacement)
    
    apacheserver = settings.PUBLIC_APACHE_URL
#    serverdict = settings.LOCAL_APACHE_DICT
    treemapdir = settings.REL_TREEMAP_DICT
    icondir = settings.REL_ICON_DICT
    
    parentid = vtree.getRoot().treenode.getNakedParent().pk
    print "PARENTID ", parentid
    #must fit to UrlDefault!
    parentoptionsstring = optionsstring
    parentor = OptionsReader(parentoptionsstring, presetid) 
    pzoom = parentor.getOption('annexzoom')
    if pzoom > 0:
        parentor.setOption('annexzoom',presetid, pzoom - 1)
        parentoptionsstring = parentor.getCorrectedOptions(presetid)
    parentidstr = app.dirs.urls.UrlDefault().buildUrl(presetid, parentoptionsstring, vtree.getRoot().treenode.getNakedParent().getClassName(), vtree.getRoot().treenode.getNakedParent().getIdReplacement())
    
    nodes = vtree.getAllNodes()
    mapparams = [None] * len(nodes)
    iconparams = []
    tooltipshift = [None] * len(nodes)
    
    for (idx, node) in enumerate(nodes):
        nodeisannex = node.treenode.getObject().getClassName() == 'Annex'
        #generate link suffix
        if(nodeisannex and (node.level == 1)):
            zoomoptionsstring = updateAnnexZoomInOptions(depth, options, presetid)
            linksuffix = app.dirs.urls.UrlDefault().buildUrl(presetid, zoomoptionsstring, rootmodel, node.treenode.getObject().getIdReplacement())
        elif (nodeisannex and (node.level == 0)):
            zoomoptionsstring = updateAnnexZoomInOptions(depth -1, options, presetid)
            linksuffix = app.dirs.urls.UrlDefault().buildUrl(presetid, zoomoptionsstring, rootmodel, node.treenode.getObject().getIdReplacement())
        else:
            if nodeisannex:
                zoomoptionsstring = updateAnnexZoomInOptions(0, options, presetid)
                linksuffix = app.dirs.urls.UrlDefault().buildUrl(presetid, zoomoptionsstring, node.treenode.getObject().getVeryParentClassName(), node.treenode.getObject().getIdReplacement())
            else:
                zoomoptionsstring = updateAnnexZoomInOptions(0, options, presetid)
                linksuffix = app.dirs.urls.UrlDefault().buildUrl(presetid, zoomoptionsstring, node.treenode.getObject().getClassName(), node.treenode.getObject().getIdReplacement())
       
        #generate label information like label coordinates, labelcontent
        info = node.htmltooltiptext
        thehash = node.treenode.getObject().__hash__() 
        x1,y1,x2,y2 = calcLinkCoordinates(vtree, node)
        mapparams[idx] = (x1,y1,x2,y2,thehash,linksuffix,info)

        #generate tooltip data
        tooltipwidth, tooltipheight = estimateToolTipSize(info, tooltipfontsize)
                
        shiftx = 20
        shifty = 20
        
        if(x1 + shiftx + tooltipwidth) > imagewidth:
            shiftx = round(imagewidth - (x1 + tooltipwidth))
            
        if(y1 + shifty) > imageheight + 50:
            shifty = 50
#        elif (y1 + shifty) > imageheight and shifty <= 20:
#            shifty = 2*shifty   
        tooltipshift[idx] = (shiftx, shifty, tooltipwidth, tooltipheight, thehash, linksuffix)
        
        #generate icon links 
        if node.icon:
            if(node.level == 0):
                if nodeisannex:
                    zoomoptionsstring = updateAnnexZoomInOptions(depth, options, presetid)
                    linksuffix = app.dirs.urls.UrlDefault().buildUrl(presetid, zoomoptionsstring, rootmodel, node.treenode.getObject().getIdReplacement())
                else:
                    zoomoptionsstring = updateAnnexZoomInOptions(depth-1, options, presetid)
                    linksuffix = app.dirs.urls.UrlDefault().buildUrl(presetid, zoomoptionsstring, node.treenode.getObject().getClassName(), node.treenode.getObject().getIdReplacement())
            else:
                zoomoptionsstring = updateAnnexZoomInOptions(0, options, presetid)
                linksuffix = app.dirs.urls.UrlDefault().buildUrl(presetid, zoomoptionsstring, node.treenode.getObject().getClassName(), node.treenode.getObject().getIdReplacement())
                
            thehash = node.treenode.getObject().__hash__() + 5
            x1,y1,x2,y2 = node.iconcoords['x'], node.iconcoords['y'], node.iconcoords['x'] + node.iconcoords['width'], node.iconcoords['y']+node.iconcoords['height']
            iconparams.append((x1,y1,x2,y2,thehash,linksuffix))
        
    rt = vtree.getRoot().treenode.getObject()
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
    urlending = ''
    for pr in parents:  
        nvtext = pr.getNaviName()
        zoomoptionsstring = updateAnnexZoomInOptions(0, options, presetid)
        urlending = app.dirs.urls.UrlDefault().buildUrl(presetid, zoomoptionsstring, pr.getClassName(), pr.getIdReplacement())   
        navlinkparts.append( (nvtext, str(pr), urlending) )
    
    generationtime = datetime.datetime.now() - thetime
    
    presetnames = app.presets.Presets.getPresetNames()
    presetnames.sort();
    
    optionshtml = []
    preset = app.presets.Presets.getPresetByStaticId(presetid)
    for option in preset.optionsset:
        optionshtml.append(option.toHtml(options))

    statusfilename = str(hash(str(cache_key)+str(request.session.session_key)+str(datetime.datetime.now()))) + "stat.html"
    relstatuspath = settings.REL_STATUS_DICT + "/" + statusfilename
    progressbardict = settings.PUBLIC_APACHE_URL + settings.REL_PBARIMG_DICT
    progressbarsizepx = "%.0f"%(200.0)
        
    response = render_to_string('treemap.html', \
    {'parentid': parentidstr, 'filename': filenm, 'mapparams': mapparams, 'navilink': navlinkparts, 'imagewidth': int(imagewidth), 'imageheight': int(imageheight),\
     'tooltipfontsize': tooltipfontsize,'tooltipshift': tooltipshift, 'treemapdir': treemapdir, 'icondir': icondir, \
     'rootsuffix': rootsuffix, 'generationtime': generationtime, 'presetnames': presetnames, 'progressbardict': progressbardict, 
     'progressbarsizepx': progressbarsizepx, 'presetdefault':getCurrentPresetSelections(request, presetid, options), 'optionshtml': optionshtml, 'statusfilename' : statusfilename, \
     'relstatuspath': relstatuspath, 'apacheserver': apacheserver, 'djangoresponseurl': settings.DJANGORESPONSE_URL, \
     'iconparams': iconparams} , context_instance=None,)
    
    #mapparams, tooltipshift
    totaltime = datetime.datetime.now() - thetime
#    response = response + '<!-- <p> <blockquote> Execution and render time: ' + totaltime.__str__() + ' </blockquote> </p> -->'
    print "total time: ", totaltime
    
    request.session.set_test_cookie()
    cache.add(cache_key, response, cache_expire)
    return HttpResponse(response)

def respondTable(request, vtree, tooltipfontsize, imagewidth, imageheight, filenm, lrules, cache_key, cache_expire, thetime, rootidreplacement, rootmodel, nblevels, presetid, options = '', depth = 0):
    print "preparing response"
    optionsstring = '{'+ options +'}'
    #must fit to UrlDefault!
    rootsuffix = app.dirs.urls.UrlDefault().buildUrl(presetid, optionsstring, rootmodel, rootidreplacement)
    
    apacheserver = settings.PUBLIC_APACHE_URL
#    serverdict = settings.LOCAL_APACHE_DICT
    treemapdir = settings.REL_TREEMAP_DICT
    icondir = settings.REL_ICON_DICT
    
    parentid = vtree.getRoot().treenode.getNakedParent().pk
    print "PARENTID ", parentid
    #must fit to UrlDefault!
    parentoptionsstring = optionsstring
    parentor = OptionsReader(parentoptionsstring, presetid) 
    pzoom = parentor.getOption('annexzoom')
    if pzoom > 0:
        parentor.setOption('annexzoom',presetid, pzoom - 1)
        parentoptionsstring = parentor.getCorrectedOptions(presetid)
    parentidstr = app.dirs.urls.UrlDefault().buildUrl(presetid, parentoptionsstring, vtree.getRoot().treenode.getNakedParent().getClassName(), vtree.getRoot().treenode.getNakedParent().getIdReplacement())
    
    nodes = vtree.getAllNodes()
    mapparams = [None] * len(nodes)
    iconparams = []
    tooltipshift = [None] * len(nodes)
    
    for (idx, node) in enumerate(nodes):
        nodeisannex = node.treenode.getObject().getClassName() == 'Annex'
        #generate link suffix
        if(nodeisannex and (node.level == 1)):
            zoomoptionsstring = updateAnnexZoomInOptions(depth, options, presetid)
            linksuffix = app.dirs.urls.UrlDefault().buildUrl(presetid, zoomoptionsstring, rootmodel, node.treenode.getObject().getIdReplacement())
        elif (nodeisannex and (node.level == 0)):
            zoomoptionsstring = updateAnnexZoomInOptions(depth -1, options, presetid)
            linksuffix = app.dirs.urls.UrlDefault().buildUrl(presetid, zoomoptionsstring, rootmodel, node.treenode.getObject().getIdReplacement())
        else:
            if nodeisannex:
                zoomoptionsstring = updateAnnexZoomInOptions(0, options, presetid)
                linksuffix = app.dirs.urls.UrlDefault().buildUrl(presetid, zoomoptionsstring, node.treenode.getObject().getVeryParentClassName(), node.treenode.getObject().getIdReplacement())
            else:
                zoomoptionsstring = updateAnnexZoomInOptions(0, options, presetid)
                linksuffix = app.dirs.urls.UrlDefault().buildUrl(presetid, zoomoptionsstring, node.treenode.getObject().getClassName(), node.treenode.getObject().getIdReplacement())
       
        #generate label information like label coordinates, labelcontent
        info = node.htmltooltiptext
        thehash = node.treenode.getObject().__hash__() 
        x1,y1,x2,y2 = calcLinkCoordinates(vtree, node)
        mapparams[idx] = (x1,y1,x2,y2,thehash,linksuffix,info)

        #generate tooltip data
        tooltipwidth, tooltipheight = estimateToolTipSize(info, tooltipfontsize)
                
        shiftx = 20
        shifty = 20
        
        if(x1 + shiftx + tooltipwidth) > imagewidth:
            shiftx = round(imagewidth - (x1 + tooltipwidth))
            
        if(y1 + shifty) > imageheight + 50:
            shifty = 50
#        elif (y1 + shifty) > imageheight and shifty <= 20:
#            shifty = 2*shifty   
        tooltipshift[idx] = (shiftx, shifty, tooltipwidth, tooltipheight, thehash, linksuffix)
        
        #generate icon links 
        if node.icon:
            if(node.level == 0):
                if nodeisannex:
                    zoomoptionsstring = updateAnnexZoomInOptions(depth, options, presetid)
                    linksuffix = app.dirs.urls.UrlDefault().buildUrl(presetid, zoomoptionsstring, rootmodel, node.treenode.getObject().getIdReplacement())
                else:
                    zoomoptionsstring = updateAnnexZoomInOptions(depth-1, options, presetid)
                    linksuffix = app.dirs.urls.UrlDefault().buildUrl(presetid, zoomoptionsstring, node.treenode.getObject().getClassName(), node.treenode.getObject().getIdReplacement())
            else:
                zoomoptionsstring = updateAnnexZoomInOptions(0, options, presetid)
                linksuffix = app.dirs.urls.UrlDefault().buildUrl(presetid, zoomoptionsstring, node.treenode.getObject().getClassName(), node.treenode.getObject().getIdReplacement())
                
            thehash = node.treenode.getObject().__hash__() + 5
            x1,y1,x2,y2 = node.iconcoords['x'], node.iconcoords['y'], node.iconcoords['x'] + node.iconcoords['width'], node.iconcoords['y']+node.iconcoords['height']
            iconparams.append((x1,y1,x2,y2,thehash,linksuffix))
        
    rt = vtree.getRoot().treenode.getObject()
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
    urlending = ''
    for pr in parents:  
        nvtext = pr.getNaviName()
        zoomoptionsstring = updateAnnexZoomInOptions(0, options, presetid)
        urlending = app.dirs.urls.UrlDefault().buildUrl(presetid, zoomoptionsstring, pr.getClassName(), pr.getIdReplacement())   
        navlinkparts.append( (nvtext, str(pr), urlending) )
    
    generationtime = datetime.datetime.now() - thetime
    
    presetnames = app.presets.Presets.getPresetNames()
    presetnames.sort();
    
    optionshtml = []
    preset = app.presets.Presets.getPresetByStaticId(presetid)
    for option in preset.optionsset:
        optionshtml.append(option.toHtml(options))

    statusfilename = str(hash(str(cache_key)+str(request.session.session_key)+str(datetime.datetime.now()))) + "stat.html"
    relstatuspath = settings.REL_STATUS_DICT + "/" + statusfilename
    progressbardict = settings.PUBLIC_APACHE_URL + settings.REL_PBARIMG_DICT
    progressbarsizepx = "%.0f"%(200.0)
        
    response = render_to_string('tableview.html', \
    {'parentid': parentidstr, 'filename': filenm, 'mapparams': mapparams, 'navilink': navlinkparts, 'imagewidth': int(imagewidth), 'imageheight': int(imageheight),\
     'tooltipfontsize': tooltipfontsize,'tooltipshift': tooltipshift, 'treemapdir': treemapdir, 'icondir': icondir, \
     'rootsuffix': rootsuffix, 'generationtime': generationtime, 'presetnames': presetnames, 'progressbardict': progressbardict, 
     'progressbarsizepx': progressbarsizepx, 'presetdefault':getCurrentPresetSelections(request, presetid, options), 'optionshtml': optionshtml, 'statusfilename' : statusfilename, \
     'relstatuspath': relstatuspath, 'apacheserver': apacheserver, 'djangoresponseurl': settings.DJANGORESPONSE_URL, \
     'iconparams': iconparams} , context_instance=None,)
    
    #mapparams, tooltipshift
    totaltime = datetime.datetime.now() - thetime
#    response = response + '<!-- <p> <blockquote> Execution and render time: ' + totaltime.__str__() + ' </blockquote> </p> -->'
    print "total time: ", totaltime
    
    request.session.set_test_cookie()
    cache.add(cache_key, response, cache_expire)
    return HttpResponse(response)

def redir(request, options, urlending, refreshcache, presetid, newmodel = None, idsuffix = '', statusfilename = ''):
    defurl = app.dirs.urls.UrlDefault('{' + options + '}' + urlending)
    
    try:
        if defurl.match():
            theid = defurl.getParameter('theid')
            model = defurl.getParameter('rootmodel')
            if (newmodel is not None) and (newmodel in app.tools.Inspections.getAvailableModels()) and (model != newmodel):
                model = newmodel
                #theid = idsuffix
            
            #generate the new link to redirect
            rediraddr = request.path
            pos = rediraddr.rfind(urlending)
            if pos == -1: raise Exception('invalid path in the response object')          
            rediraddr = rediraddr[:pos]
            pos = rediraddr.rfind('/{')
            if pos != -1: rediraddr = rediraddr[:pos+1]
            
            newurlpart = defurl.buildUrl(presetid = presetid, options = options, rootmodel = model, theid = theid)
            rediraddr = rediraddr + newurlpart
            defurl = app.dirs.urls.UrlDefault(newurlpart) #new url with the new values
            # = (statusfilename, already)
            request.session['statusfile'] = {'name': statusfilename}
                
            return redirect(to = rediraddr, args = {'request':request, 'options':  defurl.getParameter('options'), 'presetid':defurl.getParameter('presetid'), 'theid': defurl.getParameter('theid'), 'rootmodel': defurl.getParameter('rootmodel'), 'refresh_cache': refreshcache})
    except Exception, e:
        return redirect(to = '..', args = {'request':request, 'options':'', 'theid': '/castor', 'presetid':str(0), 'rootmodel': 'Dirs', 'refresh_cache': refreshcache})
    
    return Http404

def setStatusFileInCookie(request, statusfilename):
    request.session['statusfile'] = {'name': statusfilename}
    generateStatusFile(statusfilename, 0)
    return HttpResponse('done', mimetype='text/plain')
    
def getCurrentPresetSelections(request, presetid, options = ''):
    try:
        optrd = OptionsReader(options, presetid)
        return {'flat': optrd.getOption('flatview'), 'presetname': app.presets.Presets.presetIdToName(presetid), 'smalltobig': optrd.getOption('smalltobig')}
    except KeyError:
        return getDefaultPresets()

def calcCacheKey(theid, presetid, parentmodel, lr, depth = 0, options = ''):
    key_prefix = settings.CACHE_MIDDLEWARE_KEY_PREFIX
    fragment_name = hash(theid).__str__() + '_'+ depth.__str__() + '_'+ ''.join([ord(bla).__str__() for bla in parentmodel.__str__()])
    args = md5_constructor(u':'.join([urlquote(resolve_variable("185", hash(theid).__str__() + '_'+ fragment_name))]))     
    lrkeypart = lr.getUniqueLevelRulesId()            
    cache_key = 'treemap%s%s%s%s%s%s' % (key_prefix, fragment_name, args.hexdigest(), lrkeypart, str(presetid), hash(options))
    return cache_key

def getDefaultRules(nblevels):
    lr = app.presets.Presets.getPreset("Default (Directory structure)").lr
    return lr

def getDefaultModel():
    return 'Dirs'

def getDefaultPresets():
    return{'flat': False, 'presetname': "Default (Directory structure)", 'smalltobig': False}

def getRootObjectForTreemap(rootmodel, rid, statusfilename):
    try:
        #Directory you want to show its content
        root = globals()[rootmodel].__dict__['findObjectByIdReplacementId'](createObject(app.tools.Inspections.getModelsModuleName(rootmodel), rootmodel), rid, statusfilename)
    except ObjectDoesNotExist:
        raise Http404
        return render_to_response("Error") 
    
    return root
        
def getStatusFileNameFromCookie(request):
    statusfilename = ''
    try:
        statusfilename = request.session['statusfile']['name']
    except:
        pass
    return statusfilename
        
def estimateToolTipSize(htmltext, tooltipfontsize):
        textlines = 1
        oldpos, fpos = 0 ,0
        fpos = htmltext.find('<br>', fpos)
        maxtooltipwidth = 0
        
        while fpos != -1:
            textlines = textlines + 1
            
            tooltipwidth = 0.0
            for character in htmltext[oldpos:fpos]:
                if character.islower():
                    tooltipwidth = tooltipwidth + tooltipfontsize * 0.61
                else:
                    tooltipwidth = tooltipwidth + tooltipfontsize * 0.66
            tooltipwidth = int(tooltipwidth)
                    
            if tooltipwidth > maxtooltipwidth: maxtooltipwidth = tooltipwidth
            oldpos = fpos
            fpos = htmltext.find('<br>', fpos + 1)

        if maxtooltipwidth > 0: 
            tooltipwidth =  maxtooltipwidth
        else:
            tooltipwidth = 600
            
        tooltipheight = int(round(textlines * 12.0 * 1.7))
        return tooltipwidth, tooltipheight
    
def updateAnnexZoomInOptions(depth, options, presetid):
        optr = OptionsReader(options, presetid) 
        optr.extendOptions("annexzoom",presetid)
        optr.setOption('annexzoom',presetid, depth)
        return''.join([bla for bla in ["{",  optr.getCorrectedOptions(presetid), "}"]]) 
    
def setTruncatedLevelRulesCopy(treemap_props, presetlr, levels):
    nbdefinedlevels = presetlr.nbLevels()
    if levels > nbdefinedlevels : levels = nbdefinedlevels
    
    lr = LevelRules()
    #define only the first 2 levels because we only need the level 1 to see if there is an Annex in full size
    for i in range(levels):
        lr.appendRuleObject(presetlr.getRuleObject(i))
    treemap_props['levelrules'] = lr
    
def calcLinkCoordinates(vtree, node):
    x1 = int(round(node.x,0))
    y1 = int(round(node.y,0)) 
    x2 = 0.0
    csize = 0.0
    if((not(vtree.nodeHasChildren(node)))):
        csize = node.height
        x2 = int(round(node.x + node.width,0))
    else:
        csize = node.labelheight
        x2 = int(round(node.x + node.labelwidth,0))
    y2 = int(round(node.y + csize,0))
    
    return x1,y1,x2,y2
        
    y2 = int(round(node.y + csize,0))