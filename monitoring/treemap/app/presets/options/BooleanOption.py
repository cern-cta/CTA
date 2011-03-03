'''
Created on Oct 13, 2010
This class represents a named option in the web interface.
It includes regular expressions which define how to include that option into the URL
and it renders html code to represent a boolean value as a check box
@author: kblaszcz
'''
from django.template.loader import render_to_string
import re
from app.presets.options.OptionInterface import OptionInterface


class BooleanOption(OptionInterface):

    def __init__(self, userfriendlyname, name, template, stdval):
        self.name = name
        self.userfriendlyname = userfriendlyname
        self.valueexpr =  r'(?P<boolean>true|false)'
        
        self.fullexpression = r'.*?' + self.name+"="+self.valueexpr + r'.*?'
        self.stdval = stdval
        self.template = template
        
    def getFullExpression(self):
        return self.fullexpression 
    
    def getValueExpression(self):
        return r'.*?' + self.valueexpr + r'.*?'
    
    def getName(self):
        return self.name
    
    def stringToValue(self, thestring):
        expr = self.getValueExpression()
        valuedict = re.match(expr, thestring).groupdict() 
        if valuedict['boolean'] == 'true':
            return True
        else:
            return False
        
    def optionsToValue(self, options):
        expr = self.fullexpression
        valuedict = re.match(expr, options).groupdict() 
        if valuedict['boolean'] == 'true':
            return True
        else:
            return False

    def valueToString(self, value):
        if value:
            return 'true'
        else:
            return 'false'
        
    def valueToOptionString(self, value):
        correctedoptions = []
        
        correctedoptions.append(self.name)
        correctedoptions.append('=')
        
        correctedoptions.append(self.valueToString(value))
        
        return ''.join([bla for bla in correctedoptions])
        
    def getStdVal(self):
        return self.stdval
    
    def toHtml(self, options):
        try:
            checked = self.optionsToValue(options)
        except:
            checked =  self.getStdVal()

        return render_to_string(self.template, {'name':self.name, 'checked':checked, 'userfriendlyname':self.userfriendlyname})
        

        