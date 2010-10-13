'''
Created on Oct 13, 2010

@author: kblaszcz
'''
import re
import datetime

class DateOption(object):

    def __init__(self, userfriendlyname, name, stdval):
        self.name = name
        self.userfriendlyname = userfriendlyname
        self.valueexpr =  r'(?P<day>[0-9]{2}).(?P<month>[0-9]{2}).(?P<year>[0-9]{4})_(?P<hour>[0-9]{2}):(?P<minute>[0-9]{2}):(?P<second>[0-9]{2})'
        
        self.fullexpression = r'.*?' + self.name+"="+self.valueexpr + r'.*?'
        self.stdval = stdval
        
    def getFullExpression(self):
        return self.fullexpression 
        
    def getName(self):
        return self.name
    
    def findvalue(self, options):
        valuedict = re.match(self.fullexpression, options).groupdict() 
        thetime = datetime.datetime(int(valuedict['year']), int(valuedict['month']), int(valuedict['day']), int(valuedict['hour']), int(valuedict['minute']), int(valuedict['second']))
        return (datetime.datetime.now()-thetime).seconds
    
    def getCorrectedString(self, value, options):
        valuedict = re.match(self.fullexpression, options).groupdict() 
        correctedoptions = []
        
        correctedoptions.append(self.name)
        correctedoptions.append("=")
        correctedoptions.append('%02d' % (int(valuedict['day'])) )
        correctedoptions.append('.')
        correctedoptions.append('%02d' % (int(valuedict['month'])) )
        correctedoptions.append('.')
        correctedoptions.append('%04d' % (int(valuedict['year'])) )
        correctedoptions.append('_')
        correctedoptions.append('%02d' % (int(valuedict['hour'])) )
        correctedoptions.append(':')
        correctedoptions.append('%02d' % (int(valuedict['minute'])) )
        correctedoptions.append(':')
        correctedoptions.append('%02d' % (int(valuedict['second'])) )
        
        return ''.join([bla for bla in correctedoptions])
        
    def getStdVal(self):
        return self.stdval