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
    
    def getName(self):
        return self.name
    
    def findvalue(self, options):
        valuedict = re.match(self.fullexpression, options).groupdict()
        if valuedict['boolean'] == 'true':
            return True
        else:
            return False
        
    def getCorrectedString(self, value, options = None):
        if (value):
            return self.name + "=" + "true"
        else:
            return self.name + "=" + "false"
        
    def getStdVal(self):
        return self.stdval
        

        