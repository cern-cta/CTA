# Create your views here.
from django.conf import settings
from django.conf.urls.defaults import *
from django.core import serializers
from django.core.cache import cache
from django.http import Http404, HttpResponse
from django.shortcuts import redirect, render_to_response
from django.template import resolve_variable
from django.template.loader import render_to_string
from django.utils.hashcompat import md5_constructor
from django.utils.http import urlquote
from sites.dirs.BooleanOption import BooleanOption
from sites.dirs.DateOption import DateOption
from sites.dirs.OptionsReader import OptionsReader
from sites.dirs.SpinnerOption import SpinnerOption
from sites.dirs.models import *
from sites.tools.Inspections import *
from sites.tools.StatusTools import *
from sites.treemap.defaultproperties.SquaredViewProperties import *
from sites.treemap.drawing.TreeDesigner import SquaredTreemapDesigner
from sites.treemap.drawing.TreemapDrawers import SquaredTreemapDrawer
from sites.treemap.drawing.metricslinking.MetricsLinker import MetricsLinker
from sites.treemap.drawing.metricslinking.TreeNodeDimensions import *
from sites.treemap.objecttree.Postprocessors import *
from sites.treemap.objecttree.TreeBuilder import TreeBuilder
from sites.treemap.objecttree.TreeRules import LevelRules
from sites.treemap.viewtree.TreeCalculators import SquaredTreemapCalculator
import datetime
import re
import sites.dirs.Presets
import time



def redirectOldLink(request, *args, **kwargs):
    return HttpResponse("URL couldn't be resolved. Here is the main link: <a href = \"" + settings.PUBLIC_APACHE_URL + "/treemaps/\"> here </a>")
#    return treeView(request, 'Dirs', theid)

def redirectHome(request, *args, **kwargs):
    return redirect(to = settings.PUBLIC_APACHE_URL + '/treemaps/0_Dirs_')

def treeView(request, options, presetid, rootmodel, theid, refresh_cache = False):  
    print options
    time = datetime.datetime.now()
    presetid = int(presetid)
    if options is None: options = ''
    
    imagewidth = 800.0
    imageheight = 600.0
    nbdefinedlevels = getDefaultNumberOfLevels()

    serverdict = settings.LOCAL_APACHE_DICT
    treemapdir = settings.REL_TREEMAP_DICT 
    
    optr = OptionsReader(options, presetid)

    #an import of getPresetByStaticId from Presets won't work! you have to give an full path here!
    #for some reason mod_python can't import Presets correctly and outputs useless error messages
    thepreset = sites.dirs.Presets.getPresetByStaticId(presetid)
    lr = sites.dirs.Presets.filterPreset(thepreset, optr.getOption('flatview'), optr.getOption('smalltobig')).lr
    
    avmodels = lr.getRuleObject(0).getUsedClassNames()

    if rootmodel not in avmodels:
        avmodels.remove('Annex')
        if len(avmodels) == 0: raise Exception("no sufficient rules for level 0") 
        theid = ''
        rootmodel = avmodels[0]
        return treeView(request = request,rootmodel = rootmodel, theid = theid, refresh_cache = refresh_cache)
        
    if rootmodel in getModelsNotToCache(): refresh_cache = True
    statusfilename = getStatusFileNameFromCookie(request)
    generateStatusFile(statusfilename, 0)
    
    cache_key = calcCacheKey(presetid = presetid, theid = theid, parentmodel = rootmodel, lr = lr, options =  optr.getCorrectedOptions(presetid))
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
    
    try:
        globals()[rootmodel].start
        globals()[rootmodel].stop
        globals()[rootmodel].start = optr.getOption('time')-datetime.timedelta(minutes = optr.getOption('span'))
        globals()[rootmodel].stop = optr.getOption('time')
    except AttributeError:
        pass
    
    try:
        root = getRootObjectForTreemap(rootmodel, theid, statusfilename)
        filenm = hash(optr.getCorrectedOptions(presetid)).__str__() + hash(root.getIdReplacement()).__str__() + str(presetid) + lr.getUniqueLevelRulesId() + ".png"  
        
        start = datetime.datetime.now()
        print 'start generating object tree for ' + root.__str__()
        tb = TreeBuilder(lr)
        
        otree = tb.generateObjectTree(rootobject = root, statusfilename = statusfilename) 
        print 'time until now was: ' + (datetime.datetime.now() - start ).__str__()
    except NoDataAvailableError:
        deleteStatusFile(statusfilename)
        return HttpResponse(render_to_string('dirs/nodata.html', {'apacheserver': settings.PUBLIC_APACHE_URL} , context_instance=None))
    
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
    
    fullfilepath= serverdict + treemapdir + "/" + filenm
    print fullfilepath

    #wait maximum 6 seconds for the file being unlocked, ignore the lock otherwise
    sleepcounter = 0
    sleeptime = datetime.datetime.now()
    while os.path.exists(fullfilepath + '.lock') and sleepcounter < 6:
        while (datetime.datetime.now() - sleeptime).seconds < 1:
            pass
        sleepcounter = sleepcounter + 1

    fileobject = open(fullfilepath + '.lock', 'w+b')
    drawer.drawTreemap(fullfilepath)
    os.remove(fullfilepath + '.lock')
    
    print 'time until now was: ' + (datetime.datetime.now() - start ).__str__()
    #------------------------------------------------------------
    start = datetime.datetime.now()
    response = respond (request = request, vtree = tree, tooltipfontsize = 12, imagewidth = imagewidth, imageheight = imageheight,\
    filenm = filenm, lrules = lr, cache_key = cache_key, cache_expire = cache_expire, time = time, rootsuffix = root.getIdReplacement(), nblevels = nbdefinedlevels, presetid = presetid, options = optr.getCorrectedOptions(presetid))
    
    del tree
    del otree
    deleteStatusFile(statusfilename)
        
    print 'time until now was: ' + (datetime.datetime.now() - start ).__str__()
    
    return response

#@cache_page(60 *60 * 24 * 3) #cache for 3 days
def groupView(request, options, presetid, rootmodel, depth, theid, refresh_cache = False):    
    time = datetime.datetime.now()
    depth = int(depth)
    presetid = int(presetid)
    if options is None: options = ''
    
    prefix = theid[:(len(rootmodel)+1)]
    if(prefix == rootmodel + "_"):
        urlrest = theid[(len(rootmodel)+1):]
    
    if rootmodel in getModelsNotToCache(): refresh_cache = True
    statusfilename = getStatusFileNameFromCookie(request)
    
    imagewidth = 800.0
    imageheight = 600.0
    nbdefinedlevels = getDefaultNumberOfLevels()

    serverdict = settings.LOCAL_APACHE_DICT
    treemapdir = settings.REL_TREEMAP_DICT
    
    optr = OptionsReader(options, presetid)
    
    #an import of getPresetByStaticId from Presets won't work! you have to give an full path here!
    #for some reason mod_python can't import Presets correctly and outputs useless error messages
    thepreset = sites.dirs.Presets.getPresetByStaticId(presetid)
    cookielr = sites.dirs.Presets.filterPreset(thepreset, optr.getOption('flatview'), optr.getOption('smalltobig')).lr
        
    cache_key = calcCacheKey(presetid = presetid, theid = theid, parentmodel = "Annex", depth = depth, lr = cookielr, options = optr.getCorrectedOptions(presetid))
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
    
    #define LevelRules
    lr = LevelRules()
    
    #define only the first 2 levels because we only need the level 1 to see if there is an Annex in full size
    for i in range(2):
        lr.appendRuleObject(cookielr.getRuleObject(i))
    try:
        globals()[rootmodel].start
        globals()[rootmodel].stop
        globals()[rootmodel].start = optr.getOption('time')-datetime.timedelta(minutes = optr.getOption('span'))
        globals()[rootmodel].stop = optr.getOption('time')
    except AttributeError:
        pass

    try:
        if request.session['statusfile']['isvalid']:
            statusfilename = request.session['statusfile']['name']
            request.session['statusfile']['isvalid'] = False
        else:
            statusfilename = ''
    except:
        statusfilename = ''
    
    try:
        root = getRootObjectForTreemap(rootmodel, urlrest, statusfilename)
        filenm = "Annex" + hash(optr.getCorrectedOptions(presetid)).__str__() + hash(root.getIdReplacement()).__str__() + str(presetid) + str(depth) + lr.getUniqueLevelRulesId() + ".png"
        
        start = datetime.datetime.now()
        print 'start generating first object tree ' + root.__str__()
        tb = TreeBuilder(lr)
    
        otree = tb.generateObjectTree(rootobject = root, statusfilename = statusfilename)    
        print 'time until now was: ' + (datetime.datetime.now() - start ).__str__()
    except NoDataAvailableError:
        deleteStatusFile(statusfilename)
        return HttpResponse(render_to_string('dirs/nodata.html', {'apacheserver': settings.PUBLIC_APACHE_URL} , context_instance=None))
    
    #---------------------------------------------
    otree.traveseToRoot()
    anxnode = otree.getRootAnnex()
    
    #if there is an Annex then display it, otherwise you can just display the directory without it
    if anxnode is not None:
        anx = anxnode.getObject()
        for i in range(depth):
            print 'start generating object subtree ' + anx.__str__()
            otree = tb.generateObjectTree(anx, statusfilename = statusfilename)
            
            newanxnode = otree.getRootAnnex()
            
            if newanxnode is not None: 
                anx = anx.getCopyWithIncDepth(newexcluded = newanxnode.getObject().getExcludedNodes(), evaluation = newanxnode.getEvalValue())    
            else:
                raise Http404 
        
        lr = LevelRules()        
        for i in range(nbdefinedlevels):
            lr.appendRuleObject(cookielr.getRuleObject(i))  
        
        tb = TreeBuilder(lr)
        otree = tb.generateObjectTree(rootobject = anx, statusfilename = statusfilename) 
            
    else: #no Annex display needed
        
        lr = LevelRules()
        for i in range(nbdefinedlevels):
            lr.appendRuleObject(cookielr.getRuleObject(i))  
            
        tb = TreeBuilder(lr)    
        otree = tb.generateObjectTree(rootobject = root, statusfilename = statusfilename)     
        
     
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
    filenm = filenm, lrules = lr, cache_key = cache_key, cache_expire = cache_expire, time = time, rootsuffix = root.getIdReplacement(), nblevels = nbdefinedlevels, presetid = presetid, options = optr.getCorrectedOptions(presetid))
    
    del tree
    del otree
    deleteStatusFile(statusfilename)
        
    print 'time until now was: ' + (datetime.datetime.now() - start ).__str__()
    return response

def redir(request, options, urlending, refreshcache, presetid, newmodel = None, idsuffix = '', statusfilename = ''):
    if options is None: options = ''
    optrd = OptionsReader(options, presetid)
    
    #look for the patterns 
    patterns = sites.dirs.urls.urlpatterns
    groupv = r'.*?.groupView$'
    normalv = r'.*?.treeView$'
    treeviewexpr = r''
    groupviewexpr = r''
    for pattern in patterns:
        regex = str(pattern.regex.pattern)
        if(re.match(groupv, pattern._callback_str)):
            groupviewexpr = pattern.regex.pattern
        if(re.match(normalv, pattern._callback_str)):
            treeviewexpr = pattern.regex.pattern
    
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
            pos = rediraddr.rfind('/{')
            if pos != -1: rediraddr = rediraddr[:pos+1]
            rediraddr = rediraddr + '{' + optrd.getCorrectedOptions(presetid) + '}' + str(presetid) + "_" + model  + "_" + theid
            # = (statusfilename, already)
            request.session['statusfile'] = {'name': statusfilename, 'isvalid': True}
                
            return redirect(to = rediraddr, args = {'request':request, 'options':  optrd.getCorrectedOptions(presetid), 'presetid':presetid, 'theid':theid, 'rootmodel': model, 'refresh_cache': refreshcache})
        elif re.match(groupviewexpr, urlending):
            match = re.match(treeviewexpr, urlending)
            theid = match.group('theid')
            depth = match.group('depth')
            model = match.group('rootmodel')
            if (newmodel is not None) and (newmodel in getAvailableModels()) and (model != newmodel):
                model = newmodel
                theid = idsuffix
                
            #generate the new link to redirect
            rediraddr = request.path
            pos = rediraddr.rfind(options) - 1
            if pos == -1: raise Exception('invalid path in the response object')          
            rediraddr = rediraddr[:pos]
            pos = rediraddr.rfind('/{')
            if pos != -1: rediraddr = rediraddr[:pos+1]
            rediraddr = rediraddr + '{' + optrd.getCorrectedOptions(presetid) + '}' + str(presetid) + "_" + model  + "_" + theid
            
            return redirect(to = rediraddr, args = {'request':request,'options':  optrd.getCorrectedOptions(presetid), 'theid':theid, 'presetid': str(presetid), 'depth':depth, 'rootmodel':model, 'refresh_cache': refreshcache})
    except:
        return redirect(to = '..', args = {'request':request, 'options':'', 'theid': '/castor', 'presetid':str(0), 'rootmodel': 'Dirs', 'refresh_cache': refreshcache})
    
    return Http404

def preset(request, options,  urlending):
    if options is None: options = ''
    nblevels = getDefaultNumberOfLevels()
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
            
        if presetname not in sites.dirs.Presets.getPresetNames():
            redir(request, options, urlending, False, 0)
            
        preset = sites.dirs.Presets.getPreset(presetname)
        optr = OptionsReader(options, preset.staticid)    
        validoptions = preset.optionsset
        thetime = None
        
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
                                models = preset.lr.getRuleObject(0).getUsedClassNames(self)
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
    return redir(request, options, urlending, sites.dirs.Presets.getPreset(presetname).cachingenabled, sites.dirs.Presets.getPreset(presetname).staticid, sites.dirs.Presets.getPreset(presetname).rootmodel, sites.dirs.Presets.getPreset(presetname).rootsuffix, statusfilename)

def setStatusFileInCookie(request, statusfilename):
    request.session['statusfile'] = {'name': statusfilename, 'isvalid': True}
    generateStatusFile(statusfilename, 0)
    return HttpResponse('done', mimetype='text/plain')

def respond(request, vtree, tooltipfontsize, imagewidth, imageheight, filenm, lrules, cache_key, cache_expire, time, rootsuffix, nblevels, presetid, options = ''):
    print "preparing response"
    optionsstring = '{'+ options +'}'
    rootsuffix = optionsstring + str(presetid) + "_" + rootsuffix
    
    apacheserver = settings.PUBLIC_APACHE_URL
#    serverdict = settings.LOCAL_APACHE_DICT
    treemapdir = settings.REL_TREEMAP_DICT
    icondir = settings.REL_ICON_DICT
    
    parentid = vtree.getRoot().getProperty('treenode').getNakedParent().pk
    print "PARENTID ", parentid
    parentidstr = optionsstring + str(presetid) + "_" + vtree.getRoot().getProperty('treenode').getNakedParent().getIdReplacement()
    
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
        
        linksuffix = optionsstring + str(presetid) + "_" + node.getProperty('treenode').getObject().getIdReplacement()
        info = node.getProperty('htmlinfotext')
        thehash = node.getProperty('treenode').getObject().__hash__()
        
        mapparams[idx] = (x1,y1,x2,y2,thehash,linksuffix,info)
        
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
            
        tooltipheight = int(round(textlines * 12.0 * 1.7))

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
        tooltipshift[idx] = (shiftx, shifty, tooltipwidth, tooltipheight, thehash, linksuffix)
        
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
        nvtext = pr.getNaviName()     
        navlinkparts.append( (nvtext, str(pr), optionsstring + str(presetid) + "_" + pr.getIdReplacement()) )
    
    generationtime = datetime.datetime.now() - time
    
    presetnames = sites.dirs.Presets.getPresetNames()
    presetnames.sort();
    
    optionshtml = []
    preset = sites.dirs.Presets.getPresetByStaticId(presetid)
    for option in preset.optionsset:
        optionshtml.append(option.toHtml(options))

    statusfilename = str(hash(str(cache_key)+str(request.session.session_key)+str(datetime.datetime.now()))) + "stat.html"
    relstatuspath = settings.REL_STATUS_DICT + "/" + statusfilename
    progressbardict = settings.PUBLIC_APACHE_URL + settings.REL_PBARIMG_DICT
    progressbarsizepx = "%.0f"%(200.0)
        
    response = render_to_string('dirs/imagemap.html', \
    {'parentid': parentidstr, 'filename': filenm, 'mapparams': mapparams, 'navilink': navlinkparts, 'imagewidth': int(imagewidth), 'imageheight': int(imageheight),\
     'tooltipfontsize': tooltipfontsize,'tooltipshift': tooltipshift, 'treemapdir': treemapdir, 'icondir': icondir, \
     'rootsuffix': rootsuffix, 'generationtime': generationtime, 'presetnames': presetnames, 'progressbardict': progressbardict, 
     'progressbarsizepx': progressbarsizepx, 'presetdefault':getCurrentPresetSelections(request, presetid, options), 'optionshtml': optionshtml, 'statusfilename' : statusfilename, \
     'relstatuspath': relstatuspath, 'apacheserver': apacheserver, 'djangoresponseurl': settings.DJANGORESPONSE_URL} , context_instance=None)
    
    #mapparams, tooltipshift
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
    
def getCurrentPresetSelections(request, presetid, options = ''):
    try:
        optrd = OptionsReader(options, presetid)
        return {'flat': optrd.getOption('flatview'), 'presetname': sites.dirs.Presets.presetIdToName(presetid), 'smalltobig': optrd.getOption('smalltobig')}
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
    lr = sites.dirs.Presets.getPreset("Default (Directory structure)").lr
    return lr

def getDefaultModel():
    return 'Dirs'

def getDefaultPresets():
    return{'flat': False, 'presetname': "Default (Directory structure)", 'smalltobig': False}
    
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
    mlinker.addPropertyLink('Requestsatlas', 'headertext', RawColumnDimension('filename', TopDirNameTransformator()))
    
    mlinker.addPropertyLink('Requestscms', 'fillcolor', LevelDimension())
    mlinker.addPropertyLink('Requestscms', 'htmlinfotext', RequestsHtmlInfoDimension())
    mlinker.addPropertyLink('Requestscms', 'headertext', RawColumnDimension('filename', TopDirNameTransformator()))
    
    mlinker.addPropertyLink('Requestsalice', 'fillcolor', LevelDimension())
    mlinker.addPropertyLink('Requestsalice', 'htmlinfotext', RequestsHtmlInfoDimension())
    mlinker.addPropertyLink('Requestsalice', 'headertext', RawColumnDimension('filename', TopDirNameTransformator()))
    
    mlinker.addPropertyLink('Requestslhcb', 'fillcolor', LevelDimension())
    mlinker.addPropertyLink('Requestslhcb', 'htmlinfotext', RequestsHtmlInfoDimension())
    mlinker.addPropertyLink('Requestslhcb', 'headertext', RawColumnDimension('filename', TopDirNameTransformator()))
    
    mlinker.addPropertyLink('Requestspublic', 'fillcolor', LevelDimension())
    mlinker.addPropertyLink('Requestspublic', 'htmlinfotext', RequestsHtmlInfoDimension())
    mlinker.addPropertyLink('Requestspublic', 'headertext', RawColumnDimension('filename', TopDirNameTransformator()))

    return mlinker

def getRootObjectForTreemap(rootmodel, urlrest, statusfilename):
    try:
        #Directory you want to show its content
        root = findObjectByIdReplacementSuffix(rootmodel, urlrest, statusfilename)
    except Dirs.DoesNotExist:
        raise Http404
        return render_to_response("Error") 
    
    return root
        
def getStatusFileNameFromCookie(request):
    statusfilename = ''
    try:
        if request.session['statusfile']['isvalid']:
            statusfilename = request.session['statusfile']['name']
            request.session['statusfile']['isvalid'] = False
        else:
            statusfilename = ''
    except:
        pass
    return statusfilename