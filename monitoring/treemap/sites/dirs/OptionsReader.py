import re
import datetime
import sites.dirs.Presets

class OptionsReader(object):
    '''
    classdocs
    '''
    
    def __init__(self, options, presetid):
        self.optdict = {}
        try:
            start = re.match(sites.dirs.Presets.getPresetByStaticId(presetid).options['start'], options).groupdict()
            start = datetime.datetime(int(start['year']), int(start['month']), int(start['day']), int(start['hour']), int(start['minute']), int(start['second']))
            start = (datetime.datetime.now()-start).seconds
        except:
            start = 120*60
            
        try:
            stop = re.match(sites.dirs.Presets.getPresetByStaticId(presetid).options['stop'], options).groupdict()
            stop = datetime.datetime(int(stop['year']), int(stop['month']), int(stop['day']), int(stop['hour']), int(stop['minute']), int(stop['second']))
            stop = (datetime.datetime.now()-stop).seconds
        except:
            stop = 0*60
            
        if start < stop: 
            temp =  start
            start = stop
            stop = temp
            
        self.optdict['start'] = start/60.0
        self.optdict['stop'] = stop/60.0
        
    def getOption(self, optionname):
        return self.optdict[optionname]
    
        