'''
Created on Oct 13, 2010

@author: kblaszcz
'''
from django.template.loader import render_to_string
import re


class BooleanOption(object):

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
        
    def getCorrectedString(self, value, options = None):
        if (value):
            return self.name + "=" + "true"
        else:
            return self.name + "=" + "false"
        
    def getStdVal(self):
        return self.stdval
    
    def toHtml(self, options):
        try:
            checked = self.findvalue(options)
        except:
            checked =  self.getStdVal()

        return render_to_string(self.template, {'name':self.name, 'checked':checked, 'userfriendlyname':self.userfriendlyname})
        

        