'''
Created on Oct 13, 2010

@author: kblaszcz
'''
from django.template.loader import render_to_string
import datetime
import re

class SpinnerOption(object):

    def __init__(self, userfriendlyname, name, template, stdval = 0, min = 0, max = float('inf'), step = 1, unit = ''):
        self.name = name
        self.userfriendlyname = userfriendlyname
        self.valueexpr =  r'(?P<value>\d+)'
        
        self.fullexpression = r'.*?' + self.name+"="+self.valueexpr + r'.*?'
        self.stdval = stdval;
        self.template = template
        self.min = min
        self.max = max
        assert(max>min)
        assert(stdval < max)
        assert(stdval >= min)
        self.step = step
        self.unit = unit
        
    def getFullExpression(self):
        return self.fullexpression 
    
    def getValueExpression(self):
        return r'.*?' + self.valueexpr + r'.*?'
        
    def getName(self):
        return self.name
    
    def getCorrectedString(self, value, options):
        valuedict = re.match(self.fullexpression, options).groupdict() 
        correctedoptions = []
        thevalue = int(valuedict['value'])
        if(thevalue > self.max): thevalue = self.max
        
        correctedoptions.append(self.name)
        correctedoptions.append("=")
        correctedoptions.append('%d' % thevalue) 
        
        return ''.join([bla for bla in correctedoptions])
    
    def stringToValue(self, thestring):
        expr = self.getValueExpression()
        valuedict = re.match(expr, thestring).groupdict() 
        value = int(valuedict['value'])
        if(value > self.max): value = self.max
        return value 
    
    def optionsToValue(self, options):
        expr = self.fullexpression
        valuedict = re.match(expr, options).groupdict() 
        value = int(valuedict['value'])
        if(value > self.max): value = self.max
        return value
    
    def valueToString(self, value):
        if(value > self.max): value = self.max
        return ('%d' % (value))
    
    def getStdVal(self):
        return self.stdval
   
    def toHtml(self, options):
        try:
            defaultvalue = self.optionsToValue(options)
        except:
            defaultvalue = self.getStdVal()
        
        return render_to_string(self.template, {'name':self.name, 'defaultvalue':self.valueToString(defaultvalue), 'userfriendlyname':self.userfriendlyname, 'min': self.min, 'max': self.max, 'step':self.step, 'unit':self.unit})
