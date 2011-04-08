from app.presets.options.OptionsReader import OptionsReader
from app.tools.ObjectCreator import createObject
from app.tools.StatusTools import generateStatusFile
from app.treemap.objecttree.RuleMapping import LevelRules
from django.conf import settings
from django.core.exceptions import ObjectDoesNotExist
from django.http import Http404, HttpResponse
from django.shortcuts import redirect, render_to_response
from django.template import resolve_variable
from django.utils.hashcompat import md5_constructor
from django.utils.http import urlquote
import app.dirs.urls
import app.presets.Presets
import app.tools.Inspections
from app.dirs.models import *

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