'''
Created on Feb 23, 2011

@author: kblaszcz
'''

#take example from existing classes like BooleanOption to implement!
class OptionInterface(object):

    def __init__(self, userfriendlyname, name, template, stdval = 0, visible = True):
        pass
    
    #returns how to parse the entire option, ie variablename = date format
    def getFullExpression(self):
        raise Exception('getFullExpression not implemented!')
    
    #returns how to parse value only (without variable name), i.e a date format
    def getValueExpression(self):
        raise Exception('getValueExpression not implemented!')
    
    #returns internal name of the option, i.e used by OptionsReader   
    def getName(self):
        raise Exception('getName not implemented!')
    
    #parse a string to legal value that option can have
    def stringToValue(self, thestring):
        raise Exception('stringToValue not implemented!')
    
    #find option in option string and return a legal value (parse value from string, too)
    def optionsToValue(self, options):
        raise Exception('optionsToValue not implemented!')
    
    #convert value to string, i.e Date object to string like "12.01.2012"
    def valueToString(self, thetime):
        raise Exception('valueToString not implemented!')
    
    #convert value to a string which will be a part of options string, i.e option flatview = True => string "flatview = true"
    def valueToOptionString(self, thetime):
        raise Exception('valueToOptionString not implemented!')
    
    #get standard legal value if no value given
    def getStdVal(self):
        raise Exception('getStdVal not implemented!')
   
    #returns html representation of that option, i.e a boolean will be displayed as a checkbox, if boolean is true, checkbox should be checked
    def toHtml(self, options):
        raise Exception('getStdVal not implemented!')
        