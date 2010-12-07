'''
expressons to define URL's for django framework
@author: kblaszcz
'''

from app import dirs
from app.presets.options.OptionsReader import OptionsReader
from app.tools.Inspections import getAvailableModels
from django.conf.urls.defaults import *
import app.presets.Presets
import re

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
    
    (r'({(?P<options>.*)}){0,1}(?P<presetid>\d+)_group_(?P<rootmodel>\w+)_(?P<depth>\d+)_(?P<theid>.*)', 'dirs.views.groupView'),
    
    (r'({(?P<options>.*)}){0,1}(?P<presetid>\d+)_(?P<rootmodel>\w+)_(?P<theid>.*)', 'dirs.views.treeView'),
    
    (r'setstatusfile_(?P<statusfilename>.*)', 'dirs.views.setStatusFileInCookie'),
    
    (r'(?P<rootmodel>\w+)_(?P<theid>\d+)/$', 'dirs.views.redirectOldLink'),
    (r'(?P<rootmodel>\w+)_(?P<theid>\d+)$', 'dirs.views.redirectOldLink'),
)

class UrlDefault(object):
    
    def __init__(self, urlstring = ''):
        self.optdict = {}
        self.expression =  r'({(?P<options>.*)}){0,1}(?P<presetid>\d+)_(?P<rootmodel>\w+)_(?P<theid>.*)'
        self.urlstring = urlstring
        self.optdict = re.match(self.expression, self.urlstring).groupdict() 
        self.rank = 2
    
    def getOption(self, optionname):
        return self.optdict[optionname]    
    
    def getRank(self, optionname):
        return self.rank
    
    def match(self):
        if len(self.optdict) == 0:
            return False
        else:
            return True
    
    def getCorrectedUrlString(self, presetid):    
        try:  
            presetid = self.optdict['presetid'];
        except KeyError:
            presetid = "0"
            self.optdict['rootmodel'] = 'Dirs'
            
        try:  
            options = self.optdict['options'];
            options = "{" + OptionsReader(options, presetid).getCorrectedOptions() + "}"
        except KeyError:
            options = ''
            
        try:  
            rootmodel = self.optdict['rootmodel'];
        except KeyError:
            rootmodel = app.presets.Presets.getPresetByStaticId(int(presetid)).rootmodel
            
        try:  
            theid = self.optdict['theid'];
        except KeyError:
            theid = app.presets.Presets.getPresetByStaticId(int(presetid)).rootidreplacement
        
        correctedoptions = []
        correctedoptions.append(options)
        correctedoptions.append(str(presetid))
        correctedoptions.append("_")
        correctedoptions.append(rootmodel)
        correctedoptions.append("_")
        correctedoptions.append(theid)
        
        return ''.join([bla for bla in correctedoptions])      
    
    def OptionsToUrl(self, presetid, options, rootmodel, theid):
        #check presetid
        if app.presets.Presets.getPresetByStaticId(int(presetid)) == None:
            presetid = "0"
            options = "{" + OptionsReader(options, presetid).getCorrectedOptions() + "}"
            rootmodel = app.presets.Presets.getPresetByStaticId(int(presetid)).rootmodel
            theid = app.presets.Presets.getPresetByStaticId(int(presetid)).rootidreplacement
        else:
            options = "{" + OptionsReader(options, presetid).getCorrectedOptions() + "}"
            if not(rootmodel in getAvailableModels()):
                rootmodel = app.presets.Presets.getPresetByStaticId(int(presetid)).rootmodel
                theid = app.presets.Presets.getPresetByStaticId(int(presetid)).rootidreplacement
            else:
                try:
                    globals()[rootmodel].findObjectByIdReplacementSuffix(theid)
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
    
        
    

        