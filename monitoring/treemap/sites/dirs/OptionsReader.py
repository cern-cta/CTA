import re
import datetime
import sites.dirs

class OptionsReader(object):
    '''
    classdocs
    '''
    
    def __init__(self, options, presetid):
        self.optdict = {}
        self.fromlink = {}
        self.options = options 
        
        optset = sites.dirs.Presets.getPresetByStaticId(presetid).optionsset
        for urlopt in optset:
            try:
                self.optdict[urlopt.getName()] = urlopt.optionsToValue(options)
                self.fromlink[urlopt.getName()] = True
            except:
                self.optdict[urlopt.getName()] = self.getStandardValue(urlopt.getName(), presetid)
                self.fromlink[urlopt.getName()] = False
        try:    
            if self.optdict['start'] > self.optdict['stop']: 
                temp =  self.optdict['start']
                self.optdict['start'] = self.optdict['stop']
                self.optdict['stop'] = temp
                
                temp = self.fromlink['start']
                self.fromlink['start'] = self.fromlink['stop']
                self.fromlink['stop'] = temp
        except:
            pass
        
    def getOption(self, optionname):
        try:
            return self.optdict[optionname]
        except:
            return self.getStandardValue(optionname)
    
    def getCorrectedOptions(self, presetid):
        correctedoptions = []
        optset = sites.dirs.Presets.getPresetByStaticId(presetid).optionsset
        
        for urlopt in optset:
            if self.fromlink[urlopt.getName()]:
                if len(correctedoptions) > 0:
                    correctedoptions.append(', ')
                correctedoptions.append(urlopt.getCorrectedString(self.optdict[urlopt.getName()], self.options))
        
        return ''.join([bla for bla in correctedoptions])
    
    def getStandardValue(self, optionname, presetid = 0):
        optset = sites.dirs.Presets.getPresetByStaticId(presetid).optionsset
        for option in optset:
            if option.getName() == optionname:
                return option.getStdVal()

        for preset in sites.dirs.Presets.presetdict.values():
            optset = preset.optionsset
            for option in optset:
                if option.getName() == optionname:
                    return option.getStdVal()

        raise Exception("no standard value found")        
                    
    
        