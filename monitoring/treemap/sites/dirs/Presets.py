'''
Created on Sep 10, 2010

@author: kblaszcz
'''
from sites.treemap.objecttree.TreeRules import LevelRules
from django.conf import settings
from sites.dirs.views import getDefaultNumberOfLevels
    
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
        return lr
    
def getPresetNames():
    return presetdict.keys()
    
    
presetdict = {}

#Preset for Directory structure
lr = LevelRules()
for i in range(getDefaultNumberOfLevels()):
    lr.addRules('Dirs', 'getFilesAndDirectories', 'getDirParent', 'totalsize', i)
    lr.addRules('CnsFileMetadata', 'getChildren', 'getDirParent', 'filesize', i)
    lr.addRules('Annex', 'getItems', 'getAnnexParent', 'evaluation', i)
presetdict["Directory structure"] = lr