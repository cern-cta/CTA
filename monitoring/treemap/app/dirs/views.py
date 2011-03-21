'''
django views and helper functions
@author: kblaszcz
'''


# Create your views here.
from django.conf import settings
from django.conf.urls.defaults import *
from django.core import serializers
from django.core.cache import cache
from django.core.exceptions import ObjectDoesNotExist
from django.http import Http404, HttpResponse
from django.shortcuts import redirect, render_to_response
from django.template import resolve_variable
from django.template.loader import render_to_string
from django.utils.hashcompat import md5_constructor
from django.utils.http import urlquote
from app.presets.options.BooleanOption import BooleanOption
from app.presets.options.DateOption import DateOption
from app.presets.options.OptionsReader import OptionsReader
from app.presets.options.SpinnerOption import SpinnerOption
from app.dirs.models import *
from app.tools.GarbageDeleters import deleteOldImageFiles, deleteOldStatusFiles
from app.tools.Inspections import getAvailableModels, getDefaultNumberOfLevels, getModelsNotToCache
from app.tools.StatusTools import *
from app.treemap.defaultproperties.TreeMapProperties import *
from app.treemap.drawing.TreeDesigner import SquaredTreemapDesigner
from app.treemap.drawing.TreemapDrawers import SquarifiedTreemapDrawer
from app.treemap.drawing.metricslinking.AttributeTranslators import *
from app.treemap.drawing.metricslinking.MetricsLinker import MetricsLinker
from app.treemap.drawing.metricslinking.ViewNodeDimensions import *
from app.treemap.objecttree.Postprocessors import *
from app.treemap.objecttree.TreeBuilder import TreeBuilder
from app.treemap.objecttree.TreeRules import LevelRules
from app.treemap.viewtree.TreeCalculators import SquaredTreemapCalculator
import datetime
import re
import app.presets
import time
import app.dirs.urls
import app.algorithmstudy.Analysis
from app.treemap.defaultproperties.TreeMapProperties import treemap_props



def redirectOldLink(request, *args, **kwargs):
    return HttpResponse("URL couldn't be resolved. Here is the main link: <a href = \"" + settings.PUBLIC_APACHE_URL + "/treemaps/\"> here </a>")
#    return treeView(request, 'Dirs', theid)

def redirectHome(request, *args, **kwargs):
    return redirect(to = settings.PUBLIC_APACHE_URL + '/treemaps/0_Dirs_')

#@cache_page(60 *60 * 24 * 3) #cache for 3 days
def treeView(request, options, presetid, rootmodel, theid, refresh_cache = False, doprofile = False):    
    thetime = datetime.datetime.now()
    presetid = int(presetid)
    if options is None: options = ''
    treemap_props_cp = copy.copy(treemap_props)
#    checkAndPartiallyCorrectTreemapProps(treemap_props_cp)
    
    if rootmodel in getModelsNotToCache(): refresh_cache = True
    statusfilename = getStatusFileNameFromCookie(request)
    
    imagewidth = treemap_props_cp['pxwidth']
    imageheight = treemap_props_cp['pxheight']
    
    nbdefinedlevels = getDefaultNumberOfLevels()

    serverdict = settings.LOCAL_APACHE_DICT
    treemapdir = settings.REL_TREEMAP_DICT
    
    optr = OptionsReader(options, presetid)
    depth = int(optr.getOption('annexzoom'))
    
    #whenever you see a full path like app.presets.Presets, it is a way to solve a circular dependecy 
    thepreset = app.presets.Presets.getPresetByStaticId(presetid)
    presetlr = app.presets.Presets.filterPreset(thepreset, optr.getOption('flatview'), optr.getOption('smalltobig')).lr
        
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
    
    #define LevelRules
    lr = LevelRules()
    
    #define only the first 2 levels because we only need the level 1 to see if there is an Annex in full size
    for i in range(2):
        lr.appendRuleObject(presetlr.getRuleObject(i))
    treemap_props_cp['levelrules'] = lr
    
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
        root = getRootObjectForTreemap(rootmodel, theid, statusfilename)
        filenm = hash(optr.getCorrectedOptions(presetid)).__str__() + hash(root.getClassName()).__str__() + hash(root.getIdReplacement()).__str__() + str(presetid) + str(depth) + lr.getUniqueLevelRulesId() + ".png"
        
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
        
        lr = LevelRules()        
        for i in range(nbdefinedlevels):
            lr.appendRuleObject(presetlr.getRuleObject(i)) 
        treemap_props_cp['levelrules']=lr
        
        tb = TreeBuilder(treemap_props_cp)
        otree = tb.generateObjectTree(rootobject = anx, statusfilename = statusfilename) 
            
    else: #no Annex display needed
        
        lr = LevelRules()
        for i in range(nbdefinedlevels):
            lr.appendRuleObject(presetlr.getRuleObject(i))  
        treemap_props_cp['levelrules']=lr 
           
        tb = TreeBuilder(treemap_props_cp)    
        otree = tb.generateObjectTree(rootobject = root, statusfilename = statusfilename)     
        
     
    start = datetime.datetime.now()
    print 'start calculating rectangle sizes'
         
    tc = SquaredTreemapCalculator(treemap_props = treemap_props_cp)

    tree = tc.calculate(optr.getOption('optitext'))
        
    print 'time until now was: ' + (datetime.datetime.now() - start ).__str__()

    start = datetime.datetime.now()
    print "linking metrics to graphical properties"
    mlinker = getDefaultMetricsLinking()
    print 'time until now was: ' + (datetime.datetime.now() - start ).__str__()
    
    start = datetime.datetime.now()
    print "designing tree"
    designer = SquaredTreemapDesigner( vtree = tree, treemap_props = treemap_props_cp, metricslinkage = mlinker)
    designer.designTreemap()
    print 'time until now was: ' + (datetime.datetime.now() - start ).__str__()
    
    start = datetime.datetime.now()
    print "drawing something"
    drawer = SquarifiedTreemapDrawer(tree, treemap_props_cp)

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

def redir(request, options, urlending, refreshcache, presetid, newmodel = None, idsuffix = '', statusfilename = ''):
    defurl = app.dirs.urls.UrlDefault('{' + options + '}' + urlending)
    
    try:
        if defurl.match():
            theid = defurl.getParameter('theid')
            model = defurl.getParameter('rootmodel')
            if (newmodel is not None) and (newmodel in getAvailableModels()) and (model != newmodel):
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
            request.session['statusfile'] = {'name': statusfilename, 'isvalid': True}
                
            return redirect(to = rediraddr, args = {'request':request, 'options':  defurl.getParameter('options'), 'presetid':defurl.getParameter('presetid'), 'theid': defurl.getParameter('theid'), 'rootmodel': defurl.getParameter('rootmodel'), 'refresh_cache': refreshcache})
    except Exception, e:
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
    return redir(request, options, urlending, app.presets.Presets.getPreset(presetname).cachingenabled, app.presets.Presets.getPreset(presetname).staticid, app.presets.Presets.getPreset(presetname).rootmodel, extractedid, statusfilename)

def setStatusFileInCookie(request, statusfilename):
    request.session['statusfile'] = {'name': statusfilename, 'isvalid': True}
    generateStatusFile(statusfilename, 0)
    return HttpResponse('done', mimetype='text/plain')

def respond(request, vtree, tooltipfontsize, imagewidth, imageheight, filenm, lrules, cache_key, cache_expire, thetime, rootidreplacement, rootmodel, nblevels, presetid, options = '', depth = 0):
    print "preparing response"
    optionsstring = '{'+ options +'}'
    #must fit to UrlDefault!
    rootsuffix = app.dirs.urls.UrlDefault().buildUrl(presetid, optionsstring, rootmodel, rootidreplacement)
    
    apacheserver = settings.PUBLIC_APACHE_URL
#    serverdict = settings.LOCAL_APACHE_DICT
    treemapdir = settings.REL_TREEMAP_DICT
    icondir = settings.REL_ICON_DICT
    
    parentid = vtree.getRoot().getProperty('treenode').getNakedParent().pk
    print "PARENTID ", parentid
    #must fit to UrlDefault!
    parentoptionsstring = optionsstring
    parentor = OptionsReader(parentoptionsstring, presetid) 
    pzoom = parentor.getOption('annexzoom')
    if pzoom > 0:
        parentor.setOption('annexzoom',presetid, pzoom - 1)
        parentoptionsstring = parentor.getCorrectedOptions(presetid)
    parentidstr = app.dirs.urls.UrlDefault().buildUrl(presetid, parentoptionsstring, vtree.getRoot().getProperty('treenode').getNakedParent().getClassName(), vtree.getRoot().getProperty('treenode').getNakedParent().getIdReplacement())
    
    nodes = vtree.getAllNodes()
    mapparams = [None] * len(nodes)
    tooltipshift = [None] * len(nodes)
    
    for (idx, node) in enumerate(nodes):
        x1 = int(round(node.getProperty('x'),0))
        y1 = int(round(node.getProperty('y'),0)) 
        x2 = 0.0
        csize = 0.0
        if((not(vtree.nodeHasChildren(node)))):
            csize = node.getProperty('height')
            x2 = int(round(node.getProperty('x') + node.getProperty('width'),0))
        else:
            csize = node.getProperty('labelheight')
            x2 = int(round(node.getProperty('x') + node.getProperty('labelwidth'),0))
            
        y2 = int(round(node.getProperty('y') + csize,0))
        
        #must fit to UrlDefault!
        if(node.getProperty('treenode').getObject().getClassName() == 'Annex'):
            optr = OptionsReader(options, presetid) 
            optr.extendOptions("annexzoom",presetid)
            optr.setOption('annexzoom',presetid, node.getProperty('treenode').getObject().getDepth() + 1)
            zoomoptionsstring = "{" + optr.getCorrectedOptions(presetid) + "}"
            
            linksuffix = app.dirs.urls.UrlDefault().buildUrl(presetid, zoomoptionsstring, node.getProperty('treenode').getObject().getAnnexParent().getClassName(), node.getProperty('treenode').getObject().getIdReplacement())
        else:
            linksuffix = app.dirs.urls.UrlDefault().buildUrl(presetid, optionsstring, node.getProperty('treenode').getObject().getClassName(), node.getProperty('treenode').getObject().getIdReplacement())
        
        info = node.getProperty('htmltooltiptext')
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
    urlending = ''
    for pr in parents:  
        nvtext = pr.getNaviName()
        urlending = app.dirs.urls.UrlDefault().buildUrl(presetid, optionsstring, pr.getClassName(), pr.getIdReplacement())   
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
        
    response = render_to_string('imagemap.html', \
    {'parentid': parentidstr, 'filename': filenm, 'mapparams': mapparams, 'navilink': navlinkparts, 'imagewidth': int(imagewidth), 'imageheight': int(imageheight),\
     'tooltipfontsize': tooltipfontsize,'tooltipshift': tooltipshift, 'treemapdir': treemapdir, 'icondir': icondir, \
     'rootsuffix': rootsuffix, 'generationtime': generationtime, 'presetnames': presetnames, 'progressbardict': progressbardict, 
     'progressbarsizepx': progressbarsizepx, 'presetdefault':getCurrentPresetSelections(request, presetid, options), 'optionshtml': optionshtml, 'statusfilename' : statusfilename, \
     'relstatuspath': relstatuspath, 'apacheserver': apacheserver, 'djangoresponseurl': settings.DJANGORESPONSE_URL} , context_instance=None)
    
    #mapparams, tooltipshift
    totaltime = datetime.datetime.now() - thetime
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
    
def getDefaultMetricsLinking():
    mlinker = MetricsLinker()
    mlinker.addPropertyLink('Annex', 'strokecolor', ConstantDimension(-1))
    mlinker.addPropertyLink('Annex', 'strokesize', ConstantDimension(2))
    mlinker.addPropertyLink('Annex', 'htmltooltiptext',AnnexToolTipDimension())
    mlinker.addPropertyLink('Annex', 'fillcolor', ConstantDimension(-2))
    mlinker.addPropertyLink('Annex', 'radiallight.opacity', ConstantDimension(0.0))
    
    mlinker.addPropertyLink('Dirs', 'fillcolor', LevelDimension())
    mlinker.addPropertyLink('Dirs', 'htmltooltiptext', DirToolTipDimension())
    mlinker.addPropertyLink('CnsFileMetadata', 'fillcolor', LevelDimension())
    mlinker.addPropertyLink('Dirs', 'labeltext', RawColumnDimension('name', DirNameTranslator('/')))
    mlinker.addPropertyLink('CnsFileMetadata', 'labeltext', RawColumnDimension('name'))
    mlinker.addPropertyLink('Dirs', 'htmltooltiptext', DirToolTipDimension())
    mlinker.addPropertyLink('CnsFileMetadata', 'htmltooltiptext', FileToolTipDimension())
    mlinker.addPropertyLink('CnsFileMetadata', 'radiallight.hue', FileExtensionDimension(attrname = 'name'))
    mlinker.addPropertyLink('Dirs', 'radiallight.hue', FileExtensionDimension(attrname = 'name'))
    
    mlinker.addPropertyLink('CnsFileMetadata', 'labeltextisbold', ConstantDimension(False))
    
    mlinker.addPropertyLink('Requestsatlas', 'fillcolor', LevelDimension())
    mlinker.addPropertyLink('Requestsatlas', 'htmltooltiptext', RequestsToolTipDimension())
    mlinker.addPropertyLink('Requestsatlas', 'labeltext', RawColumnDimension('filename', TopDirNameTranslator()))
    mlinker.addPropertyLink('Requestsatlas', 'radiallight.hue', FileExtensionDimension(attrname = 'filename'))
    
    mlinker.addPropertyLink('Requestscms', 'fillcolor', LevelDimension())
    mlinker.addPropertyLink('Requestscms', 'htmltooltiptext', RequestsToolTipDimension())
    mlinker.addPropertyLink('Requestscms', 'labeltext', RawColumnDimension('filename', TopDirNameTranslator()))
    mlinker.addPropertyLink('Requestscms', 'radiallight.hue', FileExtensionDimension(attrname = 'filename'))
    
    mlinker.addPropertyLink('Requestsalice', 'fillcolor', LevelDimension())
    mlinker.addPropertyLink('Requestsalice', 'htmltooltiptext', RequestsToolTipDimension())
    mlinker.addPropertyLink('Requestsalice', 'labeltext', RawColumnDimension('filename', TopDirNameTranslator()))
    mlinker.addPropertyLink('Requestsalice', 'radiallight.hue', FileExtensionDimension(attrname = 'filename'))
    
    mlinker.addPropertyLink('Requestslhcb', 'fillcolor', LevelDimension())
    mlinker.addPropertyLink('Requestslhcb', 'htmltooltiptext', RequestsToolTipDimension())
    mlinker.addPropertyLink('Requestslhcb', 'labeltext', RawColumnDimension('filename', TopDirNameTranslator()))
    mlinker.addPropertyLink('Requestslhcb', 'radiallight.hue', FileExtensionDimension(attrname = 'filename'))
    
    mlinker.addPropertyLink('Requestspublic', 'fillcolor', LevelDimension())
    mlinker.addPropertyLink('Requestspublic', 'htmltooltiptext', RequestsToolTipDimension())
    mlinker.addPropertyLink('Requestspublic', 'labeltext', RawColumnDimension('filename', TopDirNameTranslator()))
    mlinker.addPropertyLink('Requestspublic', 'radiallight.hue', FileExtensionDimension(attrname = 'filename'))

    return mlinker

def getRootObjectForTreemap(rootmodel, rid, statusfilename):
    try:
        #Directory you want to show its content
        root = globals()[rootmodel].__dict__['findObjectByIdReplacementId'](createObject(getModelsModuleName(rootmodel), rootmodel), rid, statusfilename)
    except ObjectDoesNotExist:
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

#deletes old Files if the divisor results in 0 by accident (randomly)
#the more traffic, the higher the probability that someone hits the right time and triggers the cleaning
#the random cleaning is perfomed because 
#-it doesn't make sense to do it every time, it just should be done somewhen before the harddrive gets full
#-a cron job would need to know the directories, and if they change in settings.py, you could forget to change the cron job
def cleanGarbageFilesRandomly(imagetresholdage, statusfiletresholdage):
    if (datetime.datetime.now().second % 20) == 0:
        deleteOldImageFiles(imagetresholdage)
    #probability of cleaning status files should be low, because they only remain undeleted if a view doesn't finish execution
    if (datetime.datetime.now().second % 59) == 0:    
        deleteOldStatusFiles(statusfiletresholdage)