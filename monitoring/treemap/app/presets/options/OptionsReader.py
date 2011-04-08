'''
expects the options string from the URL (the one between the {} brackets)
It reads available options by using the presetid and looks for that options inside of the string.
The parsed data is stored in a dictionary.
An important function is to generate a new corrected string from the dictionary.
An option can be excluded from the corrected string by setting includeasstring[optionname] to false
@author: kblaszcz
'''
import app.presets.Presets

class OptionsReader(object):
    '''
    classdocs
    '''
    
    def __init__(self, options, presetid):
        self.optdict = {}
        self.includeasstring = {}
        self.options = options 
        
        optset = app.presets.Presets.getPresetByStaticId(presetid).optionsset
        for urlopt in optset:
            try:
                self.optdict[urlopt.getName()] = urlopt.optionsToValue(options)
                self.includeasstring[urlopt.getName()] = True
            except:
                self.optdict[urlopt.getName()] = self.getStandardValue(urlopt.getName(), presetid)
                self.includeasstring[urlopt.getName()] = False
        
    def getOption(self, optionname):
        try:
            return self.optdict[optionname]
        except:
            return self.getStandardValue(optionname)
        
    def setOption(self, optionname, presetid, value):
        assert(isinstance(optionname, str))
        
        optset = app.presets.Presets.getPresetByStaticId(presetid).optionsset
        for urlopt in optset:
            try:
                if urlopt.getName() == optionname:
                    thevalue = urlopt.stringToValue(urlopt.valueToString(value))
                    if (thevalue != value): raise Warning("setOption couldn't set " + optionname + " to " + value + ": using " + thevalue + " instead")
                    self.optdict[optionname] = thevalue
                    self.includeasstring[urlopt.getName()] = True
                    return
            except:
                pass
        raise Exception("No option "+ optionname+ " for presetid " + presetid+ " found!")
        
    def extendOptions(self, extension, presetid):
        assert(isinstance(extension, str))
        self.options = self.options + extension;
        
        optset = app.presets.Presets.getPresetByStaticId(presetid).optionsset
        for urlopt in optset:
            try:
                self.optdict[urlopt.getName()] = urlopt.optionsToValue(self.options)
                self.includeasstring[urlopt.getName()] = True
            except:
                pass
            
    
    def getCorrectedOptions(self, presetid):
        correctedoptions = []
        optset = app.presets.Presets.getPresetByStaticId(presetid).optionsset
        
        for urlopt in optset:
            if self.includeasstring[urlopt.getName()]:
                if len(correctedoptions) > 0:
                    correctedoptions.append(', ')
                correctedoptions.append(urlopt.valueToOptionString(self.optdict[urlopt.getName()]))
        
        return ''.join([bla for bla in correctedoptions])
    
    def getStandardValue(self, optionname, presetid = 0):
        optset = app.presets.Presets.getPresetByStaticId(presetid).optionsset
        for option in optset:
            if option.getName() == optionname:
                return option.getStdVal()

        for preset in app.presets.Presets.presetdict.values():
            optset = preset.optionsset
            for option in optset:
                if option.getName() == optionname:
                    return option.getStdVal()

        raise Exception("no standard value found")        
                    
    
        