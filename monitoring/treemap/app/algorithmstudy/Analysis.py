from app import settings
from app.dirs.models import *
from app.dirs.views import getRootObjectForTreemap, getDefaultDimensionMapping
from app.errors import NoDataAvailableError
from app.presets.options.OptionsReader import OptionsReader
from app.treemap.drawing.TreeDesigner import SquaredTreemapDesigner
from app.treemap.drawing.TreemapDrawers import SquarifiedTreemapDrawer
from app.treemap.objecttree.Annex import Annex
from app.treemap.objecttree.TreeBuilder import TreeBuilder
from app.treemap.viewtree.TreeCalculators import DefaultTreemapCalculator
from django.http import HttpResponse
from django.template.loader import render_to_string
import app.presets.Presets
import datetime
import os
from app.treemap.defaultproperties.TreeMapProperties import treemap_props

def doMeasurements():
    try:
        measurements = {'notextcount': 0, 'ratio':0.0, 'ratiocount':0, 'treemapcount':0, 'brokentextcount':0, 'brokentextletters':0, 'fulltextcount':0, 'fulltextletters':0, 'minratio':float("infinity"), 'maxratio':float("-infinity") }
        #generateTreemap('optitext=false',1,'Dirs', '/castor', measurements)
        generateTreemap('time=15.02.2011_15:26:26, span=1400, optitext=false',23,'Requestscms', '/castor', measurements)
        
#        measurementswithoptitext = {'notextcount': 0, 'ratio':0.0, 'ratiocount':0, 'treemapcount':0, 'brokentextcount':0, 'brokentextletters':0, 'fulltextcount':0, 'fulltextletters':0}
#        generateTreemap('optitext=true',1,'Dirs', '/castor', measurementswithoptitext)
        
        print "WITHOUT: ", measurements
        print "avg ratio: ", float(measurements['ratio'])/float(measurements['treemapcount'])
        print "avg nb without text: ", float(measurements['notextcount'])/float(measurements['treemapcount'])
        print "avg nb with broken text: ", float(measurements['brokentextcount'])/float(measurements['treemapcount'])
        print "avg nb with full text: ", float(measurements['fulltextcount'])/float(measurements['treemapcount'])
        print "avg total nb letters full text: ", float(measurements['fulltextletters'])/float(measurements['treemapcount'])
        print "avg total nb letters broken text: ", float(measurements['brokentextletters'])/float(measurements['treemapcount'])
        print "nb letters per one full text: ", (float(measurements['fulltextletters'])/float(measurements['fulltextcount']))
        print "nb letters per one broken text: ", (float(measurements['brokentextletters'])/float(measurements['brokentextcount']))
        print "total number letters per treemap: ", float(measurements['brokentextletters'] + measurements['fulltextletters'])/float(measurements['treemapcount'])
        print "min ratio: ", measurements['minratio']
        print "max ratio: ", measurements['maxratio']
        
#        print "WITH: ", measurementswithoptitext
#        print "avg ratio: ", float(measurementswithoptitext['ratio'])/float(measurementswithoptitext['treemapcount'])
#        print "avg nb without text: ", float(measurementswithoptitext['notextcount'])/float(measurementswithoptitext['treemapcount'])
#        print "avg nb with broken text: ", float(measurementswithoptitext['brokentextcount'])/float(measurementswithoptitext['treemapcount'])
#        print "avg nb with full text: ", float(measurementswithoptitext['fulltextcount'])/float(measurementswithoptitext['treemapcount'])
#        print "avg total nb letters full text: ", float(measurementswithoptitext['fulltextletters'])/float(measurementswithoptitext['treemapcount'])
#        print "avg total nb letters broken text: ", float(measurementswithoptitext['brokentextletters'])/float(measurementswithoptitext['treemapcount'])
#        print "nb letters per one full text: ", (float(measurementswithoptitext['fulltextletters'])/float(measurementswithoptitext['fulltextcount']))
#        print "nb letters per one broken text: ", (float(measurementswithoptitext['brokentextletters'])/float(measurementswithoptitext['brokentextcount']))
#        print "total number letters per treemap: ", float(measurementswithoptitext['brokentextletters'] + measurementswithoptitext['fulltextletters'])/float(measurementswithoptitext['treemapcount'])
        
    except NoDataAvailableError:
        print "No data"

def generateTreemap(options, presetid, rootmodel, theid, measurements, count = 0):  
    treemap_props_cp = copy.copy(treemap_props)
#    checkAndPartiallyCorrectTreemapProps(treemap_props_cp)
    if count == 1000: return
    print measurements
    print options
    time = datetime.datetime.now()
    presetid = int(presetid)
    if options is None: options = ''
    
#    imagewidth = treemap_props_cp['pxwidth']
#    treemap_props_cp['pxheight'] = treemap_props_cp['pxwidth']
#    imageheight = treemap_props_cp['pxheight']
    
    treemap_props_cp['pxwidth'] = 800.0
    treemap_props_cp['pxheight'] = 800.0

    
    serverdict = settings.LOCAL_APACHE_DICT
    treemapdir = settings.REL_TREEMAP_DICT 
    
    optr = OptionsReader(options, presetid)

    #whenever you see a full path like app.presets.Presets, it is a way to solve a circular dependecy 
    thepreset = app.presets.Presets.getPresetByStaticId(presetid)
    lr = app.presets.Presets.filterPreset(thepreset, optr.getOption('flatview'), optr.getOption('smalltobig')).lr
    
    avmodels = lr.getRuleObject(0).getUsedClassNames()

    if rootmodel not in avmodels:
        avmodels.remove('Annex')
        if len(avmodels) == 0: raise Exception("no sufficient rules for level 0") 
        theid = ''
        rootmodel = avmodels[0]
        return generateTreemap(rootmodel = rootmodel, theid = theid, measurements = measurements)
    
    #do not cache if no time and span is defined
    notime = False
    try:
        optr.includeasstring['time']
    except KeyError:
        notime = True
        
    if (notime and not optr.getOption('span')): cache_hit = False
    
    
    try:
        globals()[rootmodel].start
        globals()[rootmodel].stop
        globals()[rootmodel].start = optr.getOption('time')-datetime.timedelta(minutes = optr.getOption('span'))
        globals()[rootmodel].stop = optr.getOption('time')
    except AttributeError:
        pass
    
    root = getRootObjectForTreemap(rootmodel, theid, 'bla')
    filenm = "dummy.png"  
    
    collectcondition = root.countChildren() > 5
    
    treemap_props_cp['levelrules'] = lr
    tb = TreeBuilder(treemap_props_cp)
    
    otree = tb.generateObjectTree(rootobject = root, statusfilename = 'bla') 
    
    print 'start calculating rectangle sizes'
         
    tc = DefaultTreemapCalculator(treemap_props = treemap_props_cp)

    tree = tc.calculate(optr.getOption('optitext'))
    
    if collectcondition and (tc.calculateRecursion.__dict__['ratiocount'] != 0):
        measurements['notextcount'] = measurements['notextcount'] + tc.calculateRecursion.__dict__['notextcount']
        theratio = tc.calculateRecursion.__dict__['ratiosum']/tc.calculateRecursion.__dict__['ratiocount']
        
        if theratio > measurements['maxratio']:
            measurements['maxratio'] = theratio
        if theratio < measurements['minratio']:
            measurements ['minratio'] = theratio
            
        measurements['ratio'] = measurements['ratio'] + theratio
        measurements['ratiocount'] = measurements['ratiocount'] + 1
   
        dimmapping = getDefaultDimensionMapping() 
        start = datetime.datetime.now()
        designer = SquaredTreemapDesigner( vtree = tree, treemap_props = treemap_props_cp, mapping = dimmapping)
    #    profile.runctx('designer.designTreemap()', globals(), {'designer':designer})
        designer.designTreemap()
        start = datetime.datetime.now()
        drawer = SquarifiedTreemapDrawer(treemap_props_cp)
        
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
    

        print drawer.drawText.__dict__['brokentextcount']
        measurements['brokentextcount'] = measurements['brokentextcount'] + drawer.drawText.__dict__['brokentextcount']
        measurements['brokentextletters'] = measurements['brokentextletters'] + drawer.drawText.__dict__['brokentextletters']
        measurements['fulltextcount'] = measurements['fulltextcount'] + drawer.drawText.__dict__['fulltextcount']
        measurements['fulltextletters'] = measurements['fulltextletters'] + drawer.drawText.__dict__['fulltextletters'] 
    
        os.remove(fullfilepath + '.lock')
    
        measurements['treemapcount'] = measurements['treemapcount'] + 1
        
    tree.traveseToRoot()
    children = tree.getChildren();
    del otree
    
    for child in children:
        if not isinstance( child.treenode.getObject(), Annex):
            if child.treenode.getObject().countChildren() > 0:
                generateTreemap(options, presetid, child.treenode.getObject().getClassName(), child.treenode.getObject().getIdReplacement(), measurements, count+1)
            
    del tree
