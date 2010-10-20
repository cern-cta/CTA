'''
Created on Oct 13, 2010

@author: kblaszcz
'''
import re


class BooleanOption(object):

    def __init__(self, userfriendlyname, name, stdval):
        self.name = name
        self.userfriendlyname = userfriendlyname
        self.valueexpr =  r'(?P<boolean>true|false)'
        
        self.fullexpression = r'.*?' + self.name+"="+self.valueexpr + r'.*?'
        self.stdval = stdval
        
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
        htmlstrings = []
        try:
            checked = self.findvalue(options)
        except:
            checked =  self.getStdVal()
        
        htmlstrings.append("<input id=\"id_")
        htmlstrings.append(self.name)
        htmlstrings.append( "\" type=\"checkbox\" name=\"")
        htmlstrings.append(self.name)
        htmlstrings.append("\" value=\"true\"")
        if checked: htmlstrings.append(" checked")
        htmlstrings.append("> ")
        htmlstrings.append(self.userfriendlyname)
        
        return ''.join([bla for bla in htmlstrings])
        

        