'''
Created on Sep 10, 2010

A Preset contains the set of rules that defines the data tree.
 
It also defines available options which will be displayed on the web interface and received by a view.
It defines the default root model and idreplacment value of that root 
(id replacements: directory name is the idreplacement for directories, because the real id is the fileid, but you want to find a directory object by name instead by db id)

staticid is hardcoded and must be unique! staticid is included in the URL and chooses the preset
if you change the static id, you change the meaning of a URL to be a different preset!

disable or enable caching for a particular preset by using cachingenabled
@author: kblaszcz
'''
from django.conf import settings
from app.presets.options.BooleanOption import BooleanOption
from app.presets.options.DateOption import DateOption
from app.presets.options.SpinnerOption import SpinnerOption
from app.tools.Inspections import getDefaultNumberOfLevels
from app.treemap.objecttree.TreeRules import LevelRules
import copy
    
class Preset(object):
    def __init__(self, lr, cachingenabled, rootmodel, rootidreplacement, staticid, optionsset = []):
        self.lr = lr
        self.cachingenabled = cachingenabled
        self.rootmodel = rootmodel
        self.rootidreplacement = rootidreplacement
        self.staticid = staticid
        self.optionsset = optionsset
        
presetdict = {}
    
def getPreset(presetname):
    try:
        return presetdict[presetname]
    #if not found return some default
    except KeyError:
        lr = LevelRules()
        for i in range(getDefaultNumberOfLevels()):
            lr.addRules('Dirs', 'getFilesAndDirectories', 'getDirParent', 'totalsize', i)
            lr.addRules('CnsFileMetadata', 'getChildren', 'getDirParent', 'filesize', i)
            lr.addRules('Annex', 'getItems', 'getAnnexParent', 'evaluation', i)
        optionsset = getDefaultOptionSet()
        return Preset(lr, True, 'Dirs', '/castor', 0, optionsset)#0 must be default
  
def getDefaultOptionSet():
    return [BooleanOption('Lowest values', 'smalltobig', 'options/booleanoption.html', False), BooleanOption('Flat view', 'flatview', 'options/booleanoption.html', False), BooleanOption('More caption', 'optitext', 'options/booleanoption.html', False), SpinnerOption('Annex zoom', 'annexzoom', 'options/spinneroption.html', 0,0, 120,1,'iterations', False)]  
    
def getPresetByStaticId(staticid):
    if staticid == 0:
        return getPreset("blahblah")
    for preset in presetdict.values():
        if preset.staticid == staticid:
            return preset
    return None

def presetIdToName(staticid):
    if staticid == 0:
        return "Default (Directory structure)"
    for item in presetdict.items():
        if item[1].staticid == staticid:
            return item[0]
    return "Default (Directory structure)"
    
def getAllPresetIds():    
    ret = []
    for preset in presetdict.values():
        ret.append(preset.staticid)
    return ret
    
    
def getPresetNames():
    return presetdict.keys()
    
    
def filterPreset(preset, flatview, smalltobig):
    lr = copy.copy(preset.lr)
    
    newlr = None
    if flatview:
        newlr = LevelRules()
        for count, rule in enumerate(lr.getRules()):
            newlr.appendRuleObject(rule)
            if(count == 1): break
        
        for i in range(2, lr.countDefinedLevels()):
            newlr.addRules('Annex', 'getItems', 'getAnnexParent', 'evaluation', i)
            
        lr = newlr
        
    if smalltobig:
        newlr = LevelRules()
        for rule in lr.getRules():
            therule = copy.deepcopy(rule)
            for classname in rule.getUsedClassNames():
                if therule.getPostProcessorNameFor(classname) == 'SubstractMinPostProcessor':
                    therule.setPostProcessorName(classname, 'LogAndSubstractMinPostProcessor')
                else:
                    therule.setPostProcessorName(classname, 'DefaultInversePostProcessor')
            newlr.appendRuleObject(therule)
            
        lr = newlr
            
    retpreset = copy.copy(preset)
    retpreset.lr = copy.copy(lr)
    return retpreset

#Preset for Directory structure
lr = LevelRules()
for i in range(getDefaultNumberOfLevels()):
    lr.addRules('Dirs', 'getFilesAndDirectories', 'getDirParent', 'totalsize', i)
    lr.addRules('CnsFileMetadata', 'getChildren', 'getDirParent', 'filesize', i)
    lr.addRules('Annex', 'getItems', 'getAnnexParent', 'evaluation', i)
optionsset = getDefaultOptionSet()
presetdict["Directory structure"] = Preset(lr, True, 'Dirs', '/castor', 1, optionsset)

#Preset for default
lr = LevelRules()
for i in range(getDefaultNumberOfLevels()):
    lr.addRules('Dirs', 'getFilesAndDirectories', 'getDirParent', 'totalsize', i)
    lr.addRules('CnsFileMetadata', 'getChildren', 'getDirParent', 'filesize', i)
    lr.addRules('Annex', 'getItems', 'getAnnexParent', 'evaluation', i)
optionsset = getDefaultOptionSet()
presetdict["Default (Directory structure)"] = Preset(lr, True, 'Dirs', '/castor', 1, optionsset)

#Preset for Number of Files
lr = LevelRules()
for i in range(getDefaultNumberOfLevels()):
    lr.addRules('Dirs', 'getDirs', 'getDirParent', 'nbfiles', i)
    lr.addRules('CnsFileMetadata', 'getChildren', 'getDirParent', 'filesize', i) ##just to avoid errors if user applies on a file
    lr.addRules('Annex', 'getItems', 'getAnnexParent', 'evaluation', i)
optionsset = getDefaultOptionSet()
presetdict["Number of files"] = Preset(lr, True, 'Dirs', '/castor', 2, optionsset)

#Preset for Size on Tape
lr = LevelRules()
for i in range(getDefaultNumberOfLevels()):
    lr.addRules('Dirs', 'getFilesAndDirectories', 'getDirParent', 'sizeontape', i)
    lr.addRules('CnsFileMetadata', 'getChildren', 'getDirParent', 'filesize', i)
    lr.addRules('Annex', 'getItems', 'getAnnexParent', 'evaluation', i)
optionsset = getDefaultOptionSet()
presetdict["Size on tapes"] = Preset(lr, True, 'Dirs', '/castor', 3, optionsset)

#Preset for Data on Tape
lr = LevelRules()
for i in range(getDefaultNumberOfLevels()):
    lr.addRules('Dirs', 'getFilesAndDirectories', 'getDirParent', 'dataontape', i)
    lr.addRules('CnsFileMetadata', 'getChildren', 'getDirParent', 'filesize', i)
    lr.addRules('Annex', 'getItems', 'getAnnexParent', 'evaluation', i)
optionsset = getDefaultOptionSet()
presetdict["Data on tapes"] = Preset(lr, True, 'Dirs', '/castor', 4, optionsset)

#Preset for Number of tapes
lr = LevelRules()
for i in range(getDefaultNumberOfLevels()):
    lr.addRules('Dirs', 'getDirs', 'getDirParent', 'nbtapes', i)
    lr.addRules('CnsFileMetadata', 'getChildren', 'getDirParent', 'filesize', i) #just to avoid errors if user applies on a file
    lr.addRules('Annex', 'getItems', 'getAnnexParent', 'evaluation', i)
optionsset = getDefaultOptionSet()
presetdict["Number of tapes"] = Preset(lr, True, 'Dirs', '/castor', 5, optionsset)

#Preset for Number of files on tape
lr = LevelRules()
for i in range(getDefaultNumberOfLevels()):
    lr.addRules('Dirs', 'getFilesAndDirectories', 'getDirParent', 'nbfilesontape', i)
    lr.addRules('CnsFileMetadata', 'getChildren', 'getDirParent', 'filesize', i)
    lr.addRules('Annex', 'getItems', 'getAnnexParent', 'evaluation', i)
optionsset = getDefaultOptionSet()
presetdict["Number of files on tapes"] = Preset(lr, True, 'Dirs', '/castor', 6, optionsset)

#Preset for Number of File copies on Tape
lr = LevelRules()
for i in range(getDefaultNumberOfLevels()):
    lr.addRules('Dirs', 'getFilesAndDirectories', 'getDirParent', 'nbfilecopiesontape', i)
    lr.addRules('CnsFileMetadata', 'getChildren', 'getDirParent', 'filesize', i)
    lr.addRules('Annex', 'getItems', 'getAnnexParent', 'evaluation', i)
optionsset = getDefaultOptionSet()
presetdict["Number of file copies on tapes"] = Preset(lr, True, 'Dirs', '/castor', 7, optionsset)

#Preset for Number of directories
lr = LevelRules()
for i in range(getDefaultNumberOfLevels()):
    lr.addRules('Dirs', 'getDirs', 'getDirParent', 'nbsubdirs', i)
    lr.addRules('CnsFileMetadata', 'getChildren', 'getDirParent', 'filesize', i) #just to avoid errors if user applies on a file
    lr.addRules('Annex', 'getItems', 'getAnnexParent', 'evaluation', i)
optionsset = getDefaultOptionSet()
presetdict["Number of directories"] = Preset(lr, True, 'Dirs', '/castor', 8, optionsset)

#Preset for Time to migrate
lr = LevelRules()
for i in range(getDefaultNumberOfLevels()):
    lr.addRules('Dirs', 'getDirs', 'getDirParent', 'timetomigrate', i)
    lr.addRules('CnsFileMetadata', 'getChildren', 'getDirParent', 'filesize', i) #just to avoid errors if user applies on a file
    lr.addRules('Annex', 'getItems', 'getAnnexParent', 'evaluation', i)
optionsset = getDefaultOptionSet()
presetdict["Time to migrate"] = Preset(lr, True, 'Dirs', '/castor', 9, optionsset)

#Preset for Time lost in tape marks
lr = LevelRules()
for i in range(getDefaultNumberOfLevels()):
    lr.addRules('Dirs', 'getDirs', 'getDirParent', 'timelostintapemarks', i)
    lr.addRules('CnsFileMetadata', 'getChildren', 'getDirParent', 'filesize', i) #just to avoid errors if user applies on a file
    lr.addRules('Annex', 'getItems', 'getAnnexParent', 'evaluation', i)
optionsset = getDefaultOptionSet()
presetdict["Time lost in tape marks"] = Preset(lr, True, 'Dirs', '/castor', 10, optionsset)

#Preset for Optimal time to recall
lr = LevelRules()
for i in range(getDefaultNumberOfLevels()):
    lr.addRules('Dirs', 'getDirs', 'getDirParent', 'opttimetorecall', i)
    lr.addRules('CnsFileMetadata', 'getChildren', 'getDirParent', 'filesize', i) #just to avoid errors if user applies on a file
    lr.addRules('Annex', 'getItems', 'getAnnexParent', 'evaluation', i)
optionsset = getDefaultOptionSet()
presetdict["Optimal time to recall"] = Preset(lr, True, 'Dirs', '/castor', 11, optionsset)

#Preset for oldest file last modification
lr = LevelRules()
for i in range(getDefaultNumberOfLevels()):
    lr.addRules(classname = 'Dirs', methodname = 'getDirs', parentmethodname = 'getDirParent', attrname = 'oldestfilelastmod', level = i, postprocessorname = 'SubstractMinPostProcessor')
    lr.addRules('CnsFileMetadata', 'getChildren', 'getDirParent', 'filesize', i) #just to avoid errors if user applies on a file
    lr.addRules('Annex', 'getItems', 'getAnnexParent', 'evaluation', i)
optionsset = getDefaultOptionSet()
presetdict["Oldest - file last modification"] = Preset(lr, True, 'Dirs', '/castor/cern.ch', 12, optionsset)

#Preset for average file last modification
lr = LevelRules()
for i in range(getDefaultNumberOfLevels()):
    lr.addRules(classname = 'Dirs', methodname = 'getDirs', parentmethodname = 'getDirParent', attrname = 'avgfilelastmod', level = i, postprocessorname = 'SubstractMinPostProcessor')
    lr.addRules('CnsFileMetadata', 'getChildren', 'getDirParent', 'filesize', i) #just to avoid errors if user applies on a file
    lr.addRules('Annex', 'getItems', 'getAnnexParent', 'evaluation', i)
optionsset = getDefaultOptionSet()
presetdict["Average - file last modification"] = Preset(lr, True, 'Dirs', '/castor/cern.ch', 13, optionsset)

#Preset for sigma file last modification
lr = LevelRules()
for i in range(getDefaultNumberOfLevels()):
    lr.addRules(classname = 'Dirs', methodname = 'getDirs', parentmethodname = 'getDirParent', attrname = 'sigfilelastmod', level = i, postprocessorname = 'SubstractMinPostProcessor')
    lr.addRules('CnsFileMetadata', 'getChildren', 'getDirParent', 'filesize', i) #just to avoid errors if user applies on a file
    lr.addRules('Annex', 'getItems', 'getAnnexParent', 'evaluation', i)
optionsset = getDefaultOptionSet()
presetdict["Sigma - file last modification"] = Preset(lr, True, 'Dirs', '/castor/cern.ch', 16, optionsset)

#Preset for newest file last modification
lr = LevelRules()
for i in range(getDefaultNumberOfLevels()):
    lr.addRules(classname = 'Dirs', methodname = 'getDirs', parentmethodname = 'getDirParent', attrname = 'newestfilelastmod', level = i, postprocessorname = 'SubstractMinPostProcessor')
    lr.addRules('CnsFileMetadata', 'getChildren', 'getDirParent', 'filesize', i) #just to avoid errors if user applies on a file
    lr.addRules('Annex', 'getItems', 'getAnnexParent', 'evaluation', i)
optionsset = getDefaultOptionSet()
presetdict["Newest - file last modification"] = Preset(lr, True, 'Dirs', '/castor/cern.ch', 17, optionsset)

#Preset for oldest file on tape last modification
lr = LevelRules()
for i in range(getDefaultNumberOfLevels()):
    lr.addRules(classname = 'Dirs', methodname = 'getDirs', parentmethodname = 'getDirParent', attrname = 'oldestfileontapelastmod', level = i, postprocessorname = 'SubstractMinPostProcessor')
    lr.addRules('CnsFileMetadata', 'getChildren', 'getDirParent', 'filesize', i) #just to avoid errors if user applies on a file
    lr.addRules('Annex', 'getItems', 'getAnnexParent', 'evaluation', i)
optionsset = getDefaultOptionSet()
presetdict["Oldest - file on tape last modification"] = Preset(lr, True, 'Dirs', '/castor/cern.ch', 18, optionsset)

#Preset for average file on tape last modification
lr = LevelRules()
for i in range(getDefaultNumberOfLevels()):
    lr.addRules(classname = 'Dirs', methodname = 'getDirs', parentmethodname = 'getDirParent', attrname = 'avgfileontapelastmod', level = i, postprocessorname = 'SubstractMinPostProcessor')
    lr.addRules('CnsFileMetadata', 'getChildren', 'getDirParent', 'filesize', i) #just to avoid errors if user applies on a file
    lr.addRules('Annex', 'getItems', 'getAnnexParent', 'evaluation', i)
optionsset = getDefaultOptionSet()
presetdict["Average - file on tape last modification"] = Preset(lr, True, 'Dirs', '/castor/cern.ch', 19, optionsset)

#Preset for sigma file on tape last modification
lr = LevelRules()
for i in range(getDefaultNumberOfLevels()):
    lr.addRules(classname = 'Dirs', methodname = 'getDirs', parentmethodname = 'getDirParent', attrname = 'sigfileontapelastmod', level = i, postprocessorname = 'SubstractMinPostProcessor')
    lr.addRules('CnsFileMetadata', 'getChildren', 'getDirParent', 'filesize', i) #just to avoid errors if user applies on a file
    lr.addRules('Annex', 'getItems', 'getAnnexParent', 'evaluation', i)
optionsset = getDefaultOptionSet()
presetdict["Sigma - file on tape last modification"] = Preset(lr, True, 'Dirs', '/castor/cern.ch', 20, optionsset)

#Preset for newest file on tape last modification
lr = LevelRules()
for i in range(getDefaultNumberOfLevels()):
    lr.addRules(classname = 'Dirs', methodname = 'getDirs', parentmethodname = 'getDirParent', attrname = 'newestfileontapelastmod', level = i, postprocessorname = 'SubstractMinPostProcessor')
    lr.addRules('CnsFileMetadata', 'getChildren', 'getDirParent', 'filesize', i) #just to avoid errors if user applies on a file
    lr.addRules('Annex', 'getItems', 'getAnnexParent', 'evaluation', i)
optionsset = getDefaultOptionSet()
presetdict["Newest - file on tape last modification"] = Preset(lr, True, 'Dirs', '/castor/cern.ch', 21, optionsset)

#Preset for Requests Atlas
lr = LevelRules()
for i in range(getDefaultNumberOfLevels()):
    lr.addRules(classname = 'Requestsatlas', methodname = 'getChildren', parentmethodname = 'getParent', attrname = 'requestscount', level = i)
    lr.addRules('Annex', 'getItems', 'getAnnexParent', 'evaluation', i)  
optionsset = getDefaultOptionSet()                                                                                                                                                           
optionsset.extend([DateOption('Time offset', 'time', 'options/dateoption.html', 0), SpinnerOption('Span backwards', 'span', 'options/spinneroption.html', 120,0, 60*24,3,'minutes')])
presetdict["Requests ATLAS"] = Preset(lr, False, 'Requestsatlas', '/castor', 22, optionsset)

#Preset for Requests cms
lr = LevelRules()
for i in range(getDefaultNumberOfLevels()):
    lr.addRules(classname = 'Requestscms', methodname = 'getChildren', parentmethodname = 'getParent', attrname = 'requestscount', level = i)
    lr.addRules('Annex', 'getItems', 'getAnnexParent', 'evaluation', i)
optionsset = getDefaultOptionSet()                                                                                                                                                           
optionsset.extend([DateOption('Time offset', 'time', 'options/dateoption.html', 0), SpinnerOption('Span backwards', 'span', 'options/spinneroption.html', 120,0, 60*24,3,'minutes')])
presetdict["Requests CMS"] = Preset(lr, False, 'Requestscms', '/castor', 23, optionsset)

#Preset for Requests lhcb
lr = LevelRules()
for i in range(getDefaultNumberOfLevels()):
    lr.addRules(classname = 'Requestslhcb', methodname = 'getChildren', parentmethodname = 'getParent', attrname = 'requestscount', level = i)
    lr.addRules('Annex', 'getItems', 'getAnnexParent', 'evaluation', i)
optionsset = getDefaultOptionSet()                                                                                                                                                           
optionsset.extend([DateOption('Time offset', 'time', 'options/dateoption.html', 0), SpinnerOption('Span backwards', 'span', 'options/spinneroption.html', 120,0, 60*24,3,'minutes')])
presetdict["Requests LHCb"] = Preset(lr, False, 'Requestslhcb', '/castor', 24, optionsset)

#Preset for Requests alice
lr = LevelRules()
for i in range(getDefaultNumberOfLevels()):
    lr.addRules(classname = 'Requestsalice', methodname = 'getChildren', parentmethodname = 'getParent', attrname = 'requestscount', level = i)
    lr.addRules('Annex', 'getItems', 'getAnnexParent', 'evaluation', i)
optionsset = getDefaultOptionSet()                                                                                                                                                           
optionsset.extend([DateOption('Time offset', 'time', 'options/dateoption.html', 0), SpinnerOption('Span backwards', 'span', 'options/spinneroption.html', 120,0, 60*24,3,'minutes')])
presetdict["Requests ALICE"] = Preset(lr, False, 'Requestsalice', '/castor', 25, optionsset)

#Preset for Requests public
lr = LevelRules()
for i in range(getDefaultNumberOfLevels()):
    lr.addRules(classname = 'Requestspublic', methodname = 'getChildren', parentmethodname = 'getParent', attrname = 'requestscount', level = i)
    lr.addRules('Annex', 'getItems', 'getAnnexParent', 'evaluation', i)
optionsset = getDefaultOptionSet()                                                                                                                                                           
optionsset.extend([DateOption('Time offset', 'time', 'options/dateoption.html', 0), SpinnerOption('Span backwards', 'span', 'options/spinneroption.html', 120,0, 60*24,3,'minutes')])
presetdict["Requests public"] = Preset(lr, False, 'Requestspublic', '/castor', 26, optionsset)







