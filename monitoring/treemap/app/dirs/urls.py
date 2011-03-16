'''
expressons to define URL's for django framework
@author: kblaszcz
'''

from app import dirs
import app.dirs.models
import app.presets.options
import app.tools.Inspections
from django.conf.urls.defaults import *
from django.http import Http404
import app.presets
import app.presets.options
import re
from app.tools.ObjectCreator import createObject

# Uncomment the next two lines to enable the admin:
# from django.contrib import admin
# admin.autodiscover()

urlpatterns = patterns('',
    # Example:
    # (r'^app/', include('app.foo.urls')),

    # Uncomment the admin/doc line below and add 'django.contrib.admindocs' 
    # to INSTALLED_APPS to enable admin documentation:
    # (r'^admin/doc/', include('django.contrib.admindocs.urls')),

    # Uncomment the next line to enable the admin:
#     (r'^admin/', include(admin.site.urls)),
    (r'bydir/(?P<theid>\d+)/$', 'dirs.views.redirectOldLink'),
    
#    'pprreesseett' to make it more unique because it is at the end of the url and could be misinterpreted as a part of the id(ie directory name)
    (r'({(?P<options>.*)}){0,1}(?P<urlending>.*)/pprreesseett/$', 'dirs.views.preset'),
    
#    (r'({(?P<options>.*)}){0,1}(?P<presetid>\d+)_group_(?P<rootmodel>\w+)_(?P<depth>\d+)_(?P<theid>.*)', 'dirs.views.groupView'),
    
    (r'({(?P<options>.*)}){0,1}(?P<presetid>\d+)_(?P<rootmodel>\w+)_(?P<theid>.*)', 'dirs.views.treeView'),
    
    (r'setstatusfile_(?P<statusfilename>.*)', 'dirs.views.setStatusFileInCookie'),
    
    (r'(?P<rootmodel>\w+)_(?P<theid>\d+)/$', 'dirs.views.redirectOldLink'),
    (r'(?P<rootmodel>\w+)_(?P<theid>\d+)$', 'dirs.views.redirectOldLink'),
)

class UrlReaderInterface(object):
    def getParameter(self, optionname):
        raise Exception("getParameter not implemented")
    
    def getParameterNames(self):
        raise Exception("getParameterNames not implemented")
    
    def getRank(self, optionname):
        raise Exception("getRank not implemented")
    
    def match(self):
        raise Exception("match not implemented")
    
    def getCorrectedUrlString(self, presetid):
        raise Exception("getCorrectedUrlString not implemented")
    
    def searchReplacementId(self, string):
        raise Exception("searchReplacementId not implemented")
    
    #buildUrl has variable parameters, therefore only as comment here:       
    #def buildUrl(self, someparameters):
        #raise Exception("buildUrl not implemented")


class UrlDefault(UrlReaderInterface):
    
    def __init__(self, urlstring = ''):
        self.paramdict = {}
        self.expression =  r'({(?P<options>.*)}){0,1}(?P<presetid>\d+)_(?P<rootmodel>\w+)_(?P<theid>.*)'
        self.urlstring = urlstring
        searchresults = re.match(self.expression, self.urlstring)
        if(searchresults is not None):
            self.paramdict = searchresults.groupdict() 
        else:
            self.paramdict = {}
        self.rank = 2
        
    def getParameter(self, optionname):
        return self.paramdict[optionname]
    
    def getParameterNames(self):
        return self.paramdict.keys()
    
    def getRank(self, optionname):
        return self.rank
    
    def match(self):
        if len(self.paramdict) == 0:
            return False
        else:
            return True
    
    def getOptionsString(self):
        pass
    
    def getIdReplacementString(self):
        pass
    
    def getCorrectedUrlString(self, presetid):    
        try:  
            presetid = self.paramdict['presetid'];
        except KeyError:
            presetid = "0"
            self.paramdict['rootmodel'] = 'Dirs'
            
        try:  
            options = self.paramdict['options'];
            options = "{" + app.presets.options.OptionsReader.OptionsReader(options, presetid).getCorrectedOptions(int(presetid)) + "}"
        except KeyError:
            options = ''
            
        try:  
            rootmodel = self.paramdict['rootmodel'];
        except KeyError:
            rootmodel = app.presets.Presets.getPresetByStaticId(int(presetid)).rootmodel
            
        try:  
            theid = self.paramdict['theid'];
            app.dirs.models.__dict__[str(rootmodel)].findObjectByIdReplacementId(createObject(app.tools.Inspections.getModelsModuleName(rootmodel), rootmodel), theid, '')
        except:
            theid = app.presets.Presets.getPresetByStaticId(int(presetid)).rootidreplacement
        
        correctedoptions = []
        correctedoptions.append(options)
        correctedoptions.append(str(presetid))
        correctedoptions.append("_")
        correctedoptions.append(rootmodel)
        correctedoptions.append("_")
        correctedoptions.append(theid)
        
        return ''.join([bla for bla in correctedoptions])      
    
    def buildUrl(self, presetid, options, rootmodel, theid):
        #check presetid
        if app.presets.Presets.getPresetByStaticId(int(presetid)) == None:
            presetid = '0'
            options = ''.join([bla for bla in ["{", app.presets.options.OptionsReader.OptionsReader(options, presetid).getCorrectedOptions(int(presetid)), "}"]])
            rootmodel = app.presets.Presets.getPresetByStaticId(int(presetid)).rootmodel
            theid = app.presets.Presets.getPresetByStaticId(int(presetid)).rootidreplacement
        else:
            options = ''.join([bla for bla in ["{" , app.presets.options.OptionsReader.OptionsReader(options, presetid).getCorrectedOptions(int(presetid)), "}"]])
            if not(rootmodel in app.tools.Inspections.getAvailableModelsWithAnnex()):
                rootmodel = app.presets.Presets.getPresetByStaticId(int(presetid)).rootmodel
                theid = app.presets.Presets.getPresetByStaticId(int(presetid)).rootidreplacement
            else:
                pass #the following lines are too expensive because this method here gets called frequently
#                try:
#                    app.dirs.models.__dict__[str(rootmodel)].findObjectByIdReplacementId(createObject(getModelsModuleName(rootmodel), rootmodel), theid, '')
#                except Exception, e:
#                    theid = app.presets.Presets.getPresetByStaticId(int(presetid)).rootidreplacement
                    
        if len(options) == 2:
            if options == '{}': options = ''
        
        correctedoptions = []
        correctedoptions.append(options)
        correctedoptions.append(str(presetid))
        correctedoptions.append("_")
        correctedoptions.append(rootmodel)
        correctedoptions.append("_")
        correctedoptions.append(theid)
        
        return ''.join([bla for bla in correctedoptions]) 
    
    def searchReplacementId(self, string):
        idexpression = r'({(?P<options>.*)}){0,1}(?P<presetid>\d+){0,1}_{0,1}(?P<rootmodel>\w+){0,1}_{0,1}(?P<theid>.*)'
        searchresults = re.match(idexpression, string)
        paramdict = searchresults.groupdict()
        try:
            return str(paramdict['theid'])
        except KeyError:
            return ''
    
#class UrlAnnex(UrlReaderInterface):
#    
#    def __init__(self, urlstring = ''):
#        self.paramdict = {} #({(?P<options>.*)}){0,1}(?P<presetid>\d+)_(?P<rootmodel>\w+)_(?P<theid>.*)'
#        self.expression =  r'({(?P<options>.*)}){0,1}(?P<presetid>\d+)_group_(?P<rootmodel>\w+)_(?P<depth>\d+)_(?P<theid>.*)'
#        self.urlstring = urlstring
#        searchresults = re.match(self.expression, self.urlstring)
#        if(searchresults is not None):
#            self.paramdict = searchresults.groupdict() 
#        else:
#            self.paramdict = {}
#        self.rank = 1
#        self.maxdepth = 32
#        #enforce depth between 0 and 32 which should be enough
#        try:
#            if(self.paramdict['depth'] > self.maxdepth):
#                self.paramdict['depth'] = self.maxdepth
#            if(self.paramdict['depth'] < 0):
#                self.paramdict['depth'] = 0
#        except KeyError:
#            pass
#    
#    def getParameter(self, optionname):
#        return self.paramdict[optionname]  
#    
#    def getParameterNames(self):
#        return self.paramdict.keys()
#    
#    def getRank(self, optionname):
#        return self.rank
#    
#    def match(self):
#        if len(self.paramdict) == 0:
#            return False
#        else:
#            return True
#    
#    def getCorrectedUrlString(self, presetid):    
#        try:  
#            presetid = self.paramdict['presetid'];
#        except KeyError:
#            presetid = "0"
#            self.paramdict['rootmodel'] = 'Dirs'
#            
#        try:  
#            options = self.paramdict['options'];
#            options = "{" + app.presets.options.OptionsReader.OptionsReader(options, presetid).getCorrectedOptions(int(presetid)) + "}"
#        except KeyError:
#            options = ''
#            
#        try:  
#            rootmodel = self.paramdict['rootmodel'];
#        except KeyError:
#            rootmodel = app.presets.Presets.getPresetByStaticId(int(presetid)).rootmodel
#        
#        try:  
#            theid = self.paramdict['theid'];
#            app.dirs.models.__dict__[str(rootmodel)].findObjectByIdReplacementId(createObject(app.tools.Inspections.getModelsModuleName(rootmodel), rootmodel), theid, '')
#        except:
#            theid = app.presets.Presets.getPresetByStaticId(int(presetid)).rootidreplacement
#            
#        try:  
#            depth = self.paramdict['depth'];
#        except KeyError:
#            depth = 0
#            
#        correctedoptions = []
#        correctedoptions.append(options)
#        correctedoptions.append(str(presetid))
#        correctedoptions.append("_group_")
#        correctedoptions.append(rootmodel)
#        correctedoptions.append("_")
#        correctedoptions.append(str(int(depth)))
#        correctedoptions.append("_")
#        correctedoptions.append(theid)
#        
#        return ''.join([bla for bla in correctedoptions])      
#    
#    def buildUrl(self, presetid, options, rootmodel, depth, theid):
#        #check presetid
#        if app.presets.Presets.getPresetByStaticId(int(presetid)) == None:
#            presetid = "0"
#            options = "{" + app.presets.options.OptionsReader.OptionsReader(options, presetid).getCorrectedOptions(int(presetid)) + "}"
#            rootmodel = app.presets.Presets.getPresetByStaticId(int(presetid)).rootmodel
#            theid = app.presets.Presets.getPresetByStaticId(int(presetid)).rootidreplacement
#        else:
#            #check rootmodel
#            options = "{" + app.presets.options.OptionsReader.OptionsReader(options, presetid).getCorrectedOptions(int(presetid)) + "}"
#            if not(rootmodel in app.tools.Inspections.getAvailableModels()):
#                rootmodel = app.presets.Presets.getPresetByStaticId(int(presetid)).rootmodel
#                theid = self.searchReplacementId(theid)
#            else:
#                theid = self.searchReplacementId(theid)
#                    
#        if depth > self.maxdepth: depth = self.maxdepth
#        if depth < 0: depth = 0
#        
#        if options == '{}': options = ''
#        
#        correctedoptions = []
#        correctedoptions.append(options)
#        correctedoptions.append(str(presetid))
#        correctedoptions.append("_group_")
#        correctedoptions.append(rootmodel)
#        correctedoptions.append("_")
#        correctedoptions.append(str(int(depth)))
#        correctedoptions.append("_")
#        correctedoptions.append(theid)
#        
#        return ''.join([bla for bla in correctedoptions]) 
#    
#    def buildAnnexId(self, rootmodel, depth, theid):
#        if not(rootmodel in app.tools.Inspections.getAvailableModels()) and rootmodel !=  'Annex':
#            raise Exception("model "+ rootmodel + " could not be found!")
#        
#        if depth < 0: depth = 0
#        
#        #findObjectByIdReplacementId
#        try:
#            if rootmodel != 'Annex':#to not to fail during id creation if annex constructor gets called without parameters
#                app.dirs.models.__dict__[str(rootmodel)].findObjectByIdReplacementId(createObject(app.tools.Inspections.getModelsModuleName(rootmodel), rootmodel), theid, '')
#        except:
#            raise Exception("replacementid "+ theid + " could not be found!")
#        
#        correctedoptions = []
#        correctedoptions.append("group_")
#        correctedoptions.append(rootmodel)
#        correctedoptions.append("_")
#        correctedoptions.append(str(int(depth)))
#        correctedoptions.append("_")
#        correctedoptions.append(theid)
#        
#        return ''.join([bla for bla in correctedoptions]) 
#    
#    def searchReplacementId(self, string):
#        idexpression = r'({(?P<options>.*)}){0,1}(?P<presetid>\d+){0,1}(_group_){0,1}(?P<rootmodel>\w+){0,1}_{0,1}(?P<depth>\d+){0,1}_{0,1}(?P<theid>.*)'
#        searchresults = re.match(idexpression, string)
#        paramdict = searchresults.groupdict()
#        try:
#            return str(paramdict['theid'])
#        except KeyError:
#            return ''
    

        