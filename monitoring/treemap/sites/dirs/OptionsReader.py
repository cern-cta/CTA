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
        try:
            start = re.match(sites.dirs.Presets.getPresetByStaticId(presetid).options['start'], options).groupdict()
            start = datetime.datetime(int(start['year']), int(start['month']), int(start['day']), int(start['hour']), int(start['minute']), int(start['second']))
            start = (datetime.datetime.now()-start).seconds
            self.fromlink['start'] = True
        except:
            start = 120*60
            self.fromlink['start'] = False
            
        try:
            stop = re.match(sites.dirs.Presets.getPresetByStaticId(presetid).options['stop'], options).groupdict()
            stop = datetime.datetime(int(stop['year']), int(stop['month']), int(stop['day']), int(stop['hour']), int(stop['minute']), int(stop['second']))
            stop = (datetime.datetime.now()-stop).seconds
            self.fromlink['stop'] = True
        except:
            stop = 0*60
            self.fromlink['stop'] = False
            
        if start < stop: 
            temp =  start
            start = stop
            stop = temp
            
            temp = self.fromlink['start']
            self.fromlink['start'] = self.fromlink['stop']
            self.fromlink['stop'] = temp
            
        self.optdict['start'] = start/60.0
        self.optdict['stop'] = stop/60.0
        
        try:
            smalltobig = re.match(sites.dirs.Presets.getPresetByStaticId(presetid).options['smalltobig'], options).groupdict()
            if smalltobig['boolean'] == 'true':
                smalltobig = True
            else:
                smalltobig = False
            self.fromlink['smalltobig'] = True
        except:
            smalltobig = False
            self.fromlink['smalltobig'] = False
            
        self.optdict['smalltobig'] = smalltobig
        
        try:
            flatview = re.match(sites.dirs.Presets.getPresetByStaticId(presetid).options['flatview'], options).groupdict()
            if flatview['boolean'] == 'true':
                flatview = True
            else:
                flatview = False
            self.fromlink['flatview'] = True
        except:
            flatview = False
            self.fromlink['flatview'] = False
            
        self.optdict['flatview'] = flatview
        
    def getOption(self, optionname):
        return self.optdict[optionname]
    
    def getCorrectedOptions(self, presetid):
        correctedoptions = []
        
        for optionname, value in self.optdict.items():
            if optionname in sites.dirs.Presets.getPresetByStaticId(presetid).options and self.fromlink[optionname]:
                if(optionname == 'start'):
                    values = re.match(sites.dirs.Presets.getPresetByStaticId(presetid).options['start'], self.options).groupdict()
                        
                    if len(correctedoptions) > 0:
                        correctedoptions.append(', ')
                    correctedoptions.append('start=')
                    correctedoptions.append('%02d' % (int(values['day'])) )
                    correctedoptions.append('.')
                    correctedoptions.append('%02d' % (int(values['month'])) )
                    correctedoptions.append('.')
                    correctedoptions.append('%04d' % (int(values['year'])) )
                    correctedoptions.append('_')
                    correctedoptions.append('%02d' % (int(values['hour'])) )
                    correctedoptions.append(':')
                    correctedoptions.append('%02d' % (int(values['minute'])) )
                    correctedoptions.append(':')
                    correctedoptions.append('%02d' % (int(values['second'])) )
                    
                elif (optionname == 'stop'):
                    values = re.match(sites.dirs.Presets.getPresetByStaticId(presetid).options['stop'], self.options).groupdict()
                        
                    if len(correctedoptions) > 0:
                        correctedoptions.append(', ')
                    correctedoptions.append('start=')
                    correctedoptions.append('%02d' % (int(values['day'])) )
                    correctedoptions.append('.')
                    correctedoptions.append('%02d' % (int(values['month'])) )
                    correctedoptions.append('.')
                    correctedoptions.append('%04d' % (int(values['year'])) )
                    correctedoptions.append('_')
                    correctedoptions.append('%02d' % (int(values['hour'])) )
                    correctedoptions.append(':')
                    correctedoptions.append('%02d' % (int(values['minute'])) )
                    correctedoptions.append(':')
                    correctedoptions.append('%02d' % (int(values['second'])) )
                elif (optionname == 'smalltobig'):
                    values = re.match(sites.dirs.Presets.getPresetByStaticId(presetid).options['smalltobig'], self.options).groupdict()
                        
                    if len(correctedoptions) > 0:
                        correctedoptions.append(', ')
                    if self.optdict['smalltobig']:
                        correctedoptions.append('smalltobig=true')
                    else:
                        correctedoptions.append('smalltobig=false')
                        
                elif (optionname == 'flatview'):
                    values = re.match(sites.dirs.Presets.getPresetByStaticId(presetid).options['flatview'], self.options).groupdict()
                        
                    if len(correctedoptions) > 0:
                        correctedoptions.append(', ')
                    if self.optdict['flatview']:
                        correctedoptions.append('flatview=true')
                    else:
                        correctedoptions.append('flatview=false')
        
        return ''.join([bla for bla in correctedoptions])
                    
    
        