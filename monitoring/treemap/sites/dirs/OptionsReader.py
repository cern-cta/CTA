import re
import datetime
import sites.dirs

class OptionsReader(object):
    '''
    classdocs
    '''
    
    def __init__(self, options, presetid):
        self.optdict = {}
        self.fromoptions = {}
        self.options = options 
        
        optset = sites.dirs.Presets.getPresetByStaticId(presetid).optionsset
        for urlopt in optset:
            try:
                self.optdict[urlopt.getName()] = urlopt.optionsToValue(options)
                self.fromoptions[urlopt.getName()] = True
            except:
                self.optdict[urlopt.getName()] = self.getStandardValue(urlopt.getName(), presetid)
                self.fromoptions[urlopt.getName()] = False
        
    def getOption(self, optionname):
        try:
            return self.optdict[optionname]
        except:
            return self.getStandardValue(optionname)
        
    def extendOptions(self, extension, presetid):
        assert(isinstance(extension, str))
        self.options = self.options + extension;
        
        optset = sites.dirs.Presets.getPresetByStaticId(presetid).optionsset
        for urlopt in optset:
            try:
                self.optdict[urlopt.getName()] = urlopt.optionsToValue(self.options)
                self.fromoptions[urlopt.getName()] = True
            except:
                pass
            
    
    def getCorrectedOptions(self, presetid):
        correctedoptions = []
        optset = sites.dirs.Presets.getPresetByStaticId(presetid).optionsset
        
        for urlopt in optset:
            if self.fromoptions[urlopt.getName()]:
                if len(correctedoptions) > 0:
                    correctedoptions.append(', ')
                correctedoptions.append(urlopt.valueToOptionString(self.optdict[urlopt.getName()]))
        
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
                    
    
        