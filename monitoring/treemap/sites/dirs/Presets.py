'''
Created on Sep 10, 2010

@author: kblaszcz
'''
from django.conf import settings
from sites.tools.Inspections import getDefaultNumberOfLevels
from sites.treemap.objecttree.TreeRules import LevelRules
import copy
    
class Preset(object):
    def __init__(self, lr, cachingenabled, rootmodel, rootsuffix, staticid):
        self.lr = lr
        self.cachingenabled = cachingenabled
        self.rootmodel = rootmodel
        self.rootsuffix = rootsuffix
        self.staticid = staticid
    
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
        return Preset(lr, True, 'Dirs', '/castor', 0)#0 must be default
    
def getPresetByStaticId(staticid):
    if staticid == 0:
        return getPreset("blahblah")
    for preset in presetdict.values():
        if preset.staticid == staticid:
            return preset
    return None
    
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
            
    retpreset = copy.copy(preset)
    retpreset.lr = copy.copy(lr)
    return retpreset
        
        
    
presetdict = {}

#Preset for Directory structure
lr = LevelRules()
for i in range(getDefaultNumberOfLevels()):
    lr.addRules('Dirs', 'getFilesAndDirectories', 'getDirParent', 'totalsize', i)
    lr.addRules('CnsFileMetadata', 'getChildren', 'getDirParent', 'filesize', i)
    lr.addRules('Annex', 'getItems', 'getAnnexParent', 'evaluation', i)
presetdict["Directory structure"] = Preset(lr, True, 'Dirs', '/castor', 1)

#Preset for default
lr = LevelRules()
for i in range(getDefaultNumberOfLevels()):
    lr.addRules('Dirs', 'getFilesAndDirectories', 'getDirParent', 'totalsize', i)
    lr.addRules('CnsFileMetadata', 'getChildren', 'getDirParent', 'filesize', i)
    lr.addRules('Annex', 'getItems', 'getAnnexParent', 'evaluation', i)
presetdict["Default (Directory structure)"] = Preset(lr, True, 'Dirs', '/castor', 1)

#Preset for Number of Files
lr = LevelRules()
for i in range(getDefaultNumberOfLevels()):
    lr.addRules('Dirs', 'getDirs', 'getDirParent', 'nbfiles', i)
    lr.addRules('CnsFileMetadata', 'getChildren', 'getDirParent', 'filesize', i) ##just to avoid errors if user applies on a file
    lr.addRules('Annex', 'getItems', 'getAnnexParent', 'evaluation', i)
presetdict["Number of files"] = Preset(lr, True, 'Dirs', '/castor', 2)

#Preset for Size on Tape
lr = LevelRules()
for i in range(getDefaultNumberOfLevels()):
    lr.addRules('Dirs', 'getFilesAndDirectories', 'getDirParent', 'sizeontape', i)
    lr.addRules('CnsFileMetadata', 'getChildren', 'getDirParent', 'filesize', i)
    lr.addRules('Annex', 'getItems', 'getAnnexParent', 'evaluation', i)
presetdict["Size on tapes"] = Preset(lr, True, 'Dirs', '/castor', 3)

#Preset for Data on Tape
lr = LevelRules()
for i in range(getDefaultNumberOfLevels()):
    lr.addRules('Dirs', 'getFilesAndDirectories', 'getDirParent', 'dataontape', i)
    lr.addRules('CnsFileMetadata', 'getChildren', 'getDirParent', 'filesize', i)
    lr.addRules('Annex', 'getItems', 'getAnnexParent', 'evaluation', i)
presetdict["Data on tapes"] = Preset(lr, True, 'Dirs', '/castor', 4)

#Preset for Number of tapes
lr = LevelRules()
for i in range(getDefaultNumberOfLevels()):
    lr.addRules('Dirs', 'getDirs', 'getDirParent', 'nbtapes', i)
    lr.addRules('CnsFileMetadata', 'getChildren', 'getDirParent', 'filesize', i) #just to avoid errors if user applies on a file
    lr.addRules('Annex', 'getItems', 'getAnnexParent', 'evaluation', i)
presetdict["Number of tapes"] = Preset(lr, True, 'Dirs', '/castor', 5)

#Preset for Number of files on tape
lr = LevelRules()
for i in range(getDefaultNumberOfLevels()):
    lr.addRules('Dirs', 'getFilesAndDirectories', 'getDirParent', 'nbfilesontape', i)
    lr.addRules('CnsFileMetadata', 'getChildren', 'getDirParent', 'filesize', i)
    lr.addRules('Annex', 'getItems', 'getAnnexParent', 'evaluation', i)
presetdict["Number of files on tapes"] = Preset(lr, True, 'Dirs', '/castor', 6)

#Preset for Number of File copies on Tape
lr = LevelRules()
for i in range(getDefaultNumberOfLevels()):
    lr.addRules('Dirs', 'getFilesAndDirectories', 'getDirParent', 'nbfilecopiesontape', i)
    lr.addRules('CnsFileMetadata', 'getChildren', 'getDirParent', 'filesize', i)
    lr.addRules('Annex', 'getItems', 'getAnnexParent', 'evaluation', i)
presetdict["Number of file copies on tapes"] = Preset(lr, True, 'Dirs', '/castor', 7)

#Preset for Number of directories
lr = LevelRules()
for i in range(getDefaultNumberOfLevels()):
    lr.addRules('Dirs', 'getDirs', 'getDirParent', 'nbsubdirs', i)
    lr.addRules('CnsFileMetadata', 'getChildren', 'getDirParent', 'filesize', i) #just to avoid errors if user applies on a file
    lr.addRules('Annex', 'getItems', 'getAnnexParent', 'evaluation', i)
presetdict["Number of directories"] = Preset(lr, True, 'Dirs', '/castor', 8)

#Preset for Time to migrate
lr = LevelRules()
for i in range(getDefaultNumberOfLevels()):
    lr.addRules('Dirs', 'getDirs', 'getDirParent', 'timetomigrate', i)
    lr.addRules('CnsFileMetadata', 'getChildren', 'getDirParent', 'filesize', i) #just to avoid errors if user applies on a file
    lr.addRules('Annex', 'getItems', 'getAnnexParent', 'evaluation', i)
presetdict["Time to migrate"] = Preset(lr, True, 'Dirs', '/castor', 9)

#Preset for Time lost in tape marks
lr = LevelRules()
for i in range(getDefaultNumberOfLevels()):
    lr.addRules('Dirs', 'getDirs', 'getDirParent', 'timelostintapemarks', i)
    lr.addRules('CnsFileMetadata', 'getChildren', 'getDirParent', 'filesize', i) #just to avoid errors if user applies on a file
    lr.addRules('Annex', 'getItems', 'getAnnexParent', 'evaluation', i)
presetdict["Time lost in tape marks"] = Preset(lr, True, 'Dirs', '/castor', 10)

#Preset for Optimal time to recall
lr = LevelRules()
for i in range(getDefaultNumberOfLevels()):
    lr.addRules('Dirs', 'getDirs', 'getDirParent', 'opttimetorecall', i)
    lr.addRules('CnsFileMetadata', 'getChildren', 'getDirParent', 'filesize', i) #just to avoid errors if user applies on a file
    lr.addRules('Annex', 'getItems', 'getAnnexParent', 'evaluation', i)
presetdict["Optimal time to recall"] = Preset(lr, True, 'Dirs', '/castor', 11)

#Preset for oldest file last modification
lr = LevelRules()
for i in range(getDefaultNumberOfLevels()):
    lr.addRules(classname = 'Dirs', methodname = 'getDirs', parentmethodname = 'getDirParent', columnname = 'oldestfilelastmod', level = i, postprocessorname = 'SubstractMinPostProcessor')
    lr.addRules('CnsFileMetadata', 'getChildren', 'getDirParent', 'filesize', i) #just to avoid errors if user applies on a file
    lr.addRules('Annex', 'getItems', 'getAnnexParent', 'evaluation', i)
presetdict["Oldest file last modification"] = Preset(lr, True, 'Dirs', '/castor', 12)

#Preset for average file last modification
lr = LevelRules()
for i in range(getDefaultNumberOfLevels()):
    lr.addRules(classname = 'Dirs', methodname = 'getDirs', parentmethodname = 'getDirParent', columnname = 'avgfilelastmod', level = i, postprocessorname = 'SubstractMinPostProcessor')
    lr.addRules('CnsFileMetadata', 'getChildren', 'getDirParent', 'filesize', i) #just to avoid errors if user applies on a file
    lr.addRules('Annex', 'getItems', 'getAnnexParent', 'evaluation', i)
presetdict["Average file last modification"] = Preset(lr, True, 'Dirs', '/castor', 13)

#Preset for oldest file last modification
lr = LevelRules()
for i in range(getDefaultNumberOfLevels()):
    lr.addRules(classname = 'Dirs', methodname = 'getDirs', parentmethodname = 'getDirParent', columnname = 'oldestfilelastmod', level = i, postprocessorname = 'SubstractMinPostProcessor')
    lr.addRules('CnsFileMetadata', 'getChildren', 'getDirParent', 'filesize', i) #just to avoid errors if user applies on a file
    lr.addRules('Annex', 'getItems', 'getAnnexParent', 'evaluation', i)
presetdict["Oldest file last modification"] = Preset(lr, True, 'Dirs', '/castor', 14)

#Preset for oldest file last modification
lr = LevelRules()
for i in range(getDefaultNumberOfLevels()):
    lr.addRules(classname = 'Dirs', methodname = 'getDirs', parentmethodname = 'getDirParent', columnname = 'oldestfilelastmod', level = i, postprocessorname = 'SubstractMinPostProcessor')
    lr.addRules('CnsFileMetadata', 'getChildren', 'getDirParent', 'filesize', i) #just to avoid errors if user applies on a file
    lr.addRules('Annex', 'getItems', 'getAnnexParent', 'evaluation', i)
presetdict["Oldest file last modification"] = Preset(lr, True, 'Dirs', '/castor', 15)

#Preset for sigma file last modification
lr = LevelRules()
for i in range(getDefaultNumberOfLevels()):
    lr.addRules(classname = 'Dirs', methodname = 'getDirs', parentmethodname = 'getDirParent', columnname = 'sigfilelastmod', level = i, postprocessorname = 'SubstractMinPostProcessor')
    lr.addRules('CnsFileMetadata', 'getChildren', 'getDirParent', 'filesize', i) #just to avoid errors if user applies on a file
    lr.addRules('Annex', 'getItems', 'getAnnexParent', 'evaluation', i)
presetdict["Sigma file last modification"] = Preset(lr, True, 'Dirs', '/castor', 16)

#Preset for newest file last modification
lr = LevelRules()
for i in range(getDefaultNumberOfLevels()):
    lr.addRules(classname = 'Dirs', methodname = 'getDirs', parentmethodname = 'getDirParent', columnname = 'newestfilelastmod', level = i, postprocessorname = 'SubstractMinPostProcessor')
    lr.addRules('CnsFileMetadata', 'getChildren', 'getDirParent', 'filesize', i) #just to avoid errors if user applies on a file
    lr.addRules('Annex', 'getItems', 'getAnnexParent', 'evaluation', i)
presetdict["Newest file last modification"] = Preset(lr, True, 'Dirs', '/castor', 17)

#Preset for oldest file on tape last modification
lr = LevelRules()
for i in range(getDefaultNumberOfLevels()):
    lr.addRules(classname = 'Dirs', methodname = 'getDirs', parentmethodname = 'getDirParent', columnname = 'oldestfileontapelastmod', level = i, postprocessorname = 'SubstractMinPostProcessor')
    lr.addRules('CnsFileMetadata', 'getChildren', 'getDirParent', 'filesize', i) #just to avoid errors if user applies on a file
    lr.addRules('Annex', 'getItems', 'getAnnexParent', 'evaluation', i)
presetdict["Oldest file on tape last modification"] = Preset(lr, True, 'Dirs', '/castor', 18)

#Preset for average file on tape last modification
lr = LevelRules()
for i in range(getDefaultNumberOfLevels()):
    lr.addRules(classname = 'Dirs', methodname = 'getDirs', parentmethodname = 'getDirParent', columnname = 'avgfileontapelastmod', level = i, postprocessorname = 'SubstractMinPostProcessor')
    lr.addRules('CnsFileMetadata', 'getChildren', 'getDirParent', 'filesize', i) #just to avoid errors if user applies on a file
    lr.addRules('Annex', 'getItems', 'getAnnexParent', 'evaluation', i)
presetdict["Average file on tape last modification"] = Preset(lr, True, 'Dirs', '/castor', 19)

#Preset for sigma file on tape last modification
lr = LevelRules()
for i in range(getDefaultNumberOfLevels()):
    lr.addRules(classname = 'Dirs', methodname = 'getDirs', parentmethodname = 'getDirParent', columnname = 'sigfileontapelastmod', level = i, postprocessorname = 'SubstractMinPostProcessor')
    lr.addRules('CnsFileMetadata', 'getChildren', 'getDirParent', 'filesize', i) #just to avoid errors if user applies on a file
    lr.addRules('Annex', 'getItems', 'getAnnexParent', 'evaluation', i)
presetdict["Sigma file on tape last modification"] = Preset(lr, True, 'Dirs', '/castor', 20)

#Preset for newest file on tape last modification
lr = LevelRules()
for i in range(getDefaultNumberOfLevels()):
    lr.addRules(classname = 'Dirs', methodname = 'getDirs', parentmethodname = 'getDirParent', columnname = 'newestfileontapelastmod', level = i, postprocessorname = 'SubstractMinPostProcessor')
    lr.addRules('CnsFileMetadata', 'getChildren', 'getDirParent', 'filesize', i) #just to avoid errors if user applies on a file
    lr.addRules('Annex', 'getItems', 'getAnnexParent', 'evaluation', i)
presetdict["Newest file on tape last modification"] = Preset(lr, True, 'Dirs', '/castor', 21)

#Preset for Requests Atlas
lr = LevelRules()
for i in range(getDefaultNumberOfLevels()):
    lr.addRules(classname = 'Requestsatlas', methodname = 'getChildren', parentmethodname = 'getParent', columnname = 'requestscount', level = i)
    lr.addRules('Annex', 'getItems', 'getAnnexParent', 'evaluation', i)
presetdict["Requests Atlas from last 120 minutes"] = Preset(lr, False, 'Requestsatlas', '/castor', 22)

#Preset for Requests cms
lr = LevelRules()
for i in range(getDefaultNumberOfLevels()):
    lr.addRules(classname = 'Requestscms', methodname = 'getChildren', parentmethodname = 'getParent', columnname = 'requestscount', level = i)
    lr.addRules('Annex', 'getItems', 'getAnnexParent', 'evaluation', i)
presetdict["Requests CMS from last 120 minutes"] = Preset(lr, False, 'Requestscms', '/castor', 23)





