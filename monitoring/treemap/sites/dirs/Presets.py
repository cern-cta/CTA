'''
Created on Sep 10, 2010

@author: kblaszcz
'''
from django.conf import settings
from sites.dirs.BooleanOption import BooleanOption
from sites.dirs.DateOption import DateOption
from sites.tools.Inspections import getDefaultNumberOfLevels
from sites.treemap.objecttree.TreeRules import LevelRules
import copy
    
class Preset(object):
    def __init__(self, lr, cachingenabled, rootmodel, rootsuffix, staticid, optionsset = []):
        self.lr = lr
        self.cachingenabled = cachingenabled
        self.rootmodel = rootmodel
        self.rootsuffix = rootsuffix
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
        optionsset = [BooleanOption('Lowest values', 'smalltobig', False), BooleanOption('Flat view', 'flatview', False)]
        return Preset(lr, True, 'Dirs', '/castor', 0, optionsset)#0 must be default
    
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
optionsset = [BooleanOption('Lowest values', 'smalltobig', False), BooleanOption('Flat view', 'flatview', False)]
presetdict["Directory structure"] = Preset(lr, True, 'Dirs', '/castor', 1, optionsset)

#Preset for default
lr = LevelRules()
for i in range(getDefaultNumberOfLevels()):
    lr.addRules('Dirs', 'getFilesAndDirectories', 'getDirParent', 'totalsize', i)
    lr.addRules('CnsFileMetadata', 'getChildren', 'getDirParent', 'filesize', i)
    lr.addRules('Annex', 'getItems', 'getAnnexParent', 'evaluation', i)
optionsset = [BooleanOption('Lowest values', 'smalltobig', False), BooleanOption('Flat view', 'flatview', False)]
presetdict["Default (Directory structure)"] = Preset(lr, True, 'Dirs', '/castor', 1, optionsset)

#Preset for Number of Files
lr = LevelRules()
for i in range(getDefaultNumberOfLevels()):
    lr.addRules('Dirs', 'getDirs', 'getDirParent', 'nbfiles', i)
    lr.addRules('CnsFileMetadata', 'getChildren', 'getDirParent', 'filesize', i) ##just to avoid errors if user applies on a file
    lr.addRules('Annex', 'getItems', 'getAnnexParent', 'evaluation', i)
optionsset = [BooleanOption('Lowest values', 'smalltobig', False), BooleanOption('Flat view', 'flatview', False)]
presetdict["Number of files"] = Preset(lr, True, 'Dirs', '/castor', 2, optionsset)

#Preset for Size on Tape
lr = LevelRules()
for i in range(getDefaultNumberOfLevels()):
    lr.addRules('Dirs', 'getFilesAndDirectories', 'getDirParent', 'sizeontape', i)
    lr.addRules('CnsFileMetadata', 'getChildren', 'getDirParent', 'filesize', i)
    lr.addRules('Annex', 'getItems', 'getAnnexParent', 'evaluation', i)
optionsset = [BooleanOption('Lowest values', 'smalltobig', False), BooleanOption('Flat view', 'flatview', False)]
presetdict["Size on tapes"] = Preset(lr, True, 'Dirs', '/castor', 3, optionsset)

#Preset for Data on Tape
lr = LevelRules()
for i in range(getDefaultNumberOfLevels()):
    lr.addRules('Dirs', 'getFilesAndDirectories', 'getDirParent', 'dataontape', i)
    lr.addRules('CnsFileMetadata', 'getChildren', 'getDirParent', 'filesize', i)
    lr.addRules('Annex', 'getItems', 'getAnnexParent', 'evaluation', i)
optionsset = [BooleanOption('Lowest values', 'smalltobig', False), BooleanOption('Flat view', 'flatview', False)]
presetdict["Data on tapes"] = Preset(lr, True, 'Dirs', '/castor', 4, optionsset)

#Preset for Number of tapes
lr = LevelRules()
for i in range(getDefaultNumberOfLevels()):
    lr.addRules('Dirs', 'getDirs', 'getDirParent', 'nbtapes', i)
    lr.addRules('CnsFileMetadata', 'getChildren', 'getDirParent', 'filesize', i) #just to avoid errors if user applies on a file
    lr.addRules('Annex', 'getItems', 'getAnnexParent', 'evaluation', i)
optionsset = [BooleanOption('Lowest values', 'smalltobig', False), BooleanOption('Flat view', 'flatview', False)]
presetdict["Number of tapes"] = Preset(lr, True, 'Dirs', '/castor', 5, optionsset)

#Preset for Number of files on tape
lr = LevelRules()
for i in range(getDefaultNumberOfLevels()):
    lr.addRules('Dirs', 'getFilesAndDirectories', 'getDirParent', 'nbfilesontape', i)
    lr.addRules('CnsFileMetadata', 'getChildren', 'getDirParent', 'filesize', i)
    lr.addRules('Annex', 'getItems', 'getAnnexParent', 'evaluation', i)
optionsset = [BooleanOption('Lowest values', 'smalltobig', False), BooleanOption('Flat view', 'flatview', False)]
presetdict["Number of files on tapes"] = Preset(lr, True, 'Dirs', '/castor', 6, optionsset)

#Preset for Number of File copies on Tape
lr = LevelRules()
for i in range(getDefaultNumberOfLevels()):
    lr.addRules('Dirs', 'getFilesAndDirectories', 'getDirParent', 'nbfilecopiesontape', i)
    lr.addRules('CnsFileMetadata', 'getChildren', 'getDirParent', 'filesize', i)
    lr.addRules('Annex', 'getItems', 'getAnnexParent', 'evaluation', i)
optionsset = [BooleanOption('Lowest values', 'smalltobig', False), BooleanOption('Flat view', 'flatview', False)]
presetdict["Number of file copies on tapes"] = Preset(lr, True, 'Dirs', '/castor', 7, optionsset)

#Preset for Number of directories
lr = LevelRules()
for i in range(getDefaultNumberOfLevels()):
    lr.addRules('Dirs', 'getDirs', 'getDirParent', 'nbsubdirs', i)
    lr.addRules('CnsFileMetadata', 'getChildren', 'getDirParent', 'filesize', i) #just to avoid errors if user applies on a file
    lr.addRules('Annex', 'getItems', 'getAnnexParent', 'evaluation', i)
optionsset = [BooleanOption('Lowest values', 'smalltobig', False), BooleanOption('Flat view', 'flatview', False)]
presetdict["Number of directories"] = Preset(lr, True, 'Dirs', '/castor', 8, optionsset)

#Preset for Time to migrate
lr = LevelRules()
for i in range(getDefaultNumberOfLevels()):
    lr.addRules('Dirs', 'getDirs', 'getDirParent', 'timetomigrate', i)
    lr.addRules('CnsFileMetadata', 'getChildren', 'getDirParent', 'filesize', i) #just to avoid errors if user applies on a file
    lr.addRules('Annex', 'getItems', 'getAnnexParent', 'evaluation', i)
optionsset = [BooleanOption('Lowest values', 'smalltobig', False), BooleanOption('Flat view', 'flatview', False)]
presetdict["Time to migrate"] = Preset(lr, True, 'Dirs', '/castor', 9, optionsset)

#Preset for Time lost in tape marks
lr = LevelRules()
for i in range(getDefaultNumberOfLevels()):
    lr.addRules('Dirs', 'getDirs', 'getDirParent', 'timelostintapemarks', i)
    lr.addRules('CnsFileMetadata', 'getChildren', 'getDirParent', 'filesize', i) #just to avoid errors if user applies on a file
    lr.addRules('Annex', 'getItems', 'getAnnexParent', 'evaluation', i)
optionsset = [BooleanOption('Lowest values', 'smalltobig', False), BooleanOption('Flat view', 'flatview', False)]
presetdict["Time lost in tape marks"] = Preset(lr, True, 'Dirs', '/castor', 10, optionsset)

#Preset for Optimal time to recall
lr = LevelRules()
for i in range(getDefaultNumberOfLevels()):
    lr.addRules('Dirs', 'getDirs', 'getDirParent', 'opttimetorecall', i)
    lr.addRules('CnsFileMetadata', 'getChildren', 'getDirParent', 'filesize', i) #just to avoid errors if user applies on a file
    lr.addRules('Annex', 'getItems', 'getAnnexParent', 'evaluation', i)
optionsset = [BooleanOption('Lowest values', 'smalltobig', False), BooleanOption('Flat view', 'flatview', False)]
presetdict["Optimal time to recall"] = Preset(lr, True, 'Dirs', '/castor', 11, optionsset)

#Preset for oldest file last modification
lr = LevelRules()
for i in range(getDefaultNumberOfLevels()):
    lr.addRules(classname = 'Dirs', methodname = 'getDirs', parentmethodname = 'getDirParent', columnname = 'oldestfilelastmod', level = i, postprocessorname = 'SubstractMinPostProcessor')
    lr.addRules('CnsFileMetadata', 'getChildren', 'getDirParent', 'filesize', i) #just to avoid errors if user applies on a file
    lr.addRules('Annex', 'getItems', 'getAnnexParent', 'evaluation', i)
optionsset = [BooleanOption('Lowest values', 'smalltobig', False), BooleanOption('Flat view', 'flatview', False)]
presetdict["Oldest file last modification"] = Preset(lr, True, 'Dirs', '/castor', 12, optionsset)

#Preset for average file last modification
lr = LevelRules()
for i in range(getDefaultNumberOfLevels()):
    lr.addRules(classname = 'Dirs', methodname = 'getDirs', parentmethodname = 'getDirParent', columnname = 'avgfilelastmod', level = i, postprocessorname = 'SubstractMinPostProcessor')
    lr.addRules('CnsFileMetadata', 'getChildren', 'getDirParent', 'filesize', i) #just to avoid errors if user applies on a file
    lr.addRules('Annex', 'getItems', 'getAnnexParent', 'evaluation', i)
optionsset = [BooleanOption('Lowest values', 'smalltobig', False), BooleanOption('Flat view', 'flatview', False)]
presetdict["Average file last modification"] = Preset(lr, True, 'Dirs', '/castor', 13, optionsset)

#Preset for oldest file last modification
lr = LevelRules()
for i in range(getDefaultNumberOfLevels()):
    lr.addRules(classname = 'Dirs', methodname = 'getDirs', parentmethodname = 'getDirParent', columnname = 'oldestfilelastmod', level = i, postprocessorname = 'SubstractMinPostProcessor')
    lr.addRules('CnsFileMetadata', 'getChildren', 'getDirParent', 'filesize', i) #just to avoid errors if user applies on a file
    lr.addRules('Annex', 'getItems', 'getAnnexParent', 'evaluation', i)
optionsset = [BooleanOption('Lowest values', 'smalltobig', False), BooleanOption('Flat view', 'flatview', False)]
presetdict["Oldest file last modification"] = Preset(lr, True, 'Dirs', '/castor', 14, optionsset)

#Preset for oldest file last modification
lr = LevelRules()
for i in range(getDefaultNumberOfLevels()):
    lr.addRules(classname = 'Dirs', methodname = 'getDirs', parentmethodname = 'getDirParent', columnname = 'oldestfilelastmod', level = i, postprocessorname = 'SubstractMinPostProcessor')
    lr.addRules('CnsFileMetadata', 'getChildren', 'getDirParent', 'filesize', i) #just to avoid errors if user applies on a file
    lr.addRules('Annex', 'getItems', 'getAnnexParent', 'evaluation', i)
optionsset = [BooleanOption('Lowest values', 'smalltobig', False), BooleanOption('Flat view', 'flatview', False)]
presetdict["Oldest file last modification"] = Preset(lr, True, 'Dirs', '/castor', 15, optionsset)

#Preset for sigma file last modification
lr = LevelRules()
for i in range(getDefaultNumberOfLevels()):
    lr.addRules(classname = 'Dirs', methodname = 'getDirs', parentmethodname = 'getDirParent', columnname = 'sigfilelastmod', level = i, postprocessorname = 'SubstractMinPostProcessor')
    lr.addRules('CnsFileMetadata', 'getChildren', 'getDirParent', 'filesize', i) #just to avoid errors if user applies on a file
    lr.addRules('Annex', 'getItems', 'getAnnexParent', 'evaluation', i)
optionsset = [BooleanOption('Lowest values', 'smalltobig', False), BooleanOption('Flat view', 'flatview', False)]
presetdict["Sigma file last modification"] = Preset(lr, True, 'Dirs', '/castor', 16, optionsset)

#Preset for newest file last modification
lr = LevelRules()
for i in range(getDefaultNumberOfLevels()):
    lr.addRules(classname = 'Dirs', methodname = 'getDirs', parentmethodname = 'getDirParent', columnname = 'newestfilelastmod', level = i, postprocessorname = 'SubstractMinPostProcessor')
    lr.addRules('CnsFileMetadata', 'getChildren', 'getDirParent', 'filesize', i) #just to avoid errors if user applies on a file
    lr.addRules('Annex', 'getItems', 'getAnnexParent', 'evaluation', i)
optionsset = [BooleanOption('Lowest values', 'smalltobig', False), BooleanOption('Flat view', 'flatview', False)]
presetdict["Newest file last modification"] = Preset(lr, True, 'Dirs', '/castor', 17, optionsset)

#Preset for oldest file on tape last modification
lr = LevelRules()
for i in range(getDefaultNumberOfLevels()):
    lr.addRules(classname = 'Dirs', methodname = 'getDirs', parentmethodname = 'getDirParent', columnname = 'oldestfileontapelastmod', level = i, postprocessorname = 'SubstractMinPostProcessor')
    lr.addRules('CnsFileMetadata', 'getChildren', 'getDirParent', 'filesize', i) #just to avoid errors if user applies on a file
    lr.addRules('Annex', 'getItems', 'getAnnexParent', 'evaluation', i)
optionsset = [BooleanOption('Lowest values', 'smalltobig', False), BooleanOption('Flat view', 'flatview', False)]
presetdict["Oldest file on tape last modification"] = Preset(lr, True, 'Dirs', '/castor', 18, optionsset)

#Preset for average file on tape last modification
lr = LevelRules()
for i in range(getDefaultNumberOfLevels()):
    lr.addRules(classname = 'Dirs', methodname = 'getDirs', parentmethodname = 'getDirParent', columnname = 'avgfileontapelastmod', level = i, postprocessorname = 'SubstractMinPostProcessor')
    lr.addRules('CnsFileMetadata', 'getChildren', 'getDirParent', 'filesize', i) #just to avoid errors if user applies on a file
    lr.addRules('Annex', 'getItems', 'getAnnexParent', 'evaluation', i)
optionsset = [BooleanOption('Lowest values', 'smalltobig', False), BooleanOption('Flat view', 'flatview', False)]
presetdict["Average file on tape last modification"] = Preset(lr, True, 'Dirs', '/castor', 19, optionsset)

#Preset for sigma file on tape last modification
lr = LevelRules()
for i in range(getDefaultNumberOfLevels()):
    lr.addRules(classname = 'Dirs', methodname = 'getDirs', parentmethodname = 'getDirParent', columnname = 'sigfileontapelastmod', level = i, postprocessorname = 'SubstractMinPostProcessor')
    lr.addRules('CnsFileMetadata', 'getChildren', 'getDirParent', 'filesize', i) #just to avoid errors if user applies on a file
    lr.addRules('Annex', 'getItems', 'getAnnexParent', 'evaluation', i)
optionsset = [BooleanOption('Lowest values', 'smalltobig', False), BooleanOption('Flat view', 'flatview', False)]
presetdict["Sigma file on tape last modification"] = Preset(lr, True, 'Dirs', '/castor', 20, optionsset)

#Preset for newest file on tape last modification
lr = LevelRules()
for i in range(getDefaultNumberOfLevels()):
    lr.addRules(classname = 'Dirs', methodname = 'getDirs', parentmethodname = 'getDirParent', columnname = 'newestfileontapelastmod', level = i, postprocessorname = 'SubstractMinPostProcessor')
    lr.addRules('CnsFileMetadata', 'getChildren', 'getDirParent', 'filesize', i) #just to avoid errors if user applies on a file
    lr.addRules('Annex', 'getItems', 'getAnnexParent', 'evaluation', i)
optionsset = [BooleanOption('Lowest values', 'smalltobig', False), BooleanOption('Flat view', 'flatview', False)]
presetdict["Newest file on tape last modification"] = Preset(lr, True, 'Dirs', '/castor', 21, optionsset)

#Preset for Requests Atlas
lr = LevelRules()
for i in range(getDefaultNumberOfLevels()):
    lr.addRules(classname = 'Requestsatlas', methodname = 'getChildren', parentmethodname = 'getParent', columnname = 'requestscount', level = i)
    lr.addRules('Annex', 'getItems', 'getAnnexParent', 'evaluation', i)
optionsset = [BooleanOption('Lowest values', 'smalltobig', False), BooleanOption('Flat view', 'flatview', False), DateOption('start', 'start', 120), DateOption('stop', 'stop', 0)]
presetdict["Requests Atlas"] = Preset(lr, False, 'Requestsatlas', '/castor', 22, optionsset)

#Preset for Requests cms
lr = LevelRules()
for i in range(getDefaultNumberOfLevels()):
    lr.addRules(classname = 'Requestscms', methodname = 'getChildren', parentmethodname = 'getParent', columnname = 'requestscount', level = i)
    lr.addRules('Annex', 'getItems', 'getAnnexParent', 'evaluation', i)
optionsset = [BooleanOption('Lowest values', 'smalltobig', False), BooleanOption('Flat view', 'flatview', False), DateOption('start', 'start', 120), DateOption('stop', 'stop', 0)]
presetdict["Requests CMS"] = Preset(lr, False, 'Requestscms', '/castor', 23, optionsset)






