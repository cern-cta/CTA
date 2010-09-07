'''
Created on May 11, 2010

@author: kblaszcz
'''
import warnings
from django.db.models.fields import *

def evalDecimalField(attr, fparam = None):
    "Defines how to evaluate a decimal field"
    def isStandard():
        return True
    if(attr is None): return 0.0
    return attr.__float__()

def evalIntegerField(attr, fparam = None):
    "Defines how to evaluate a integer field"
    def isStandard():
        return True
    if(attr is None): return 0
    return attr

def evalStringBySimilarity(attr, searchtext = None):
    "Defines how to evaluate a char field"
    "returns a value that defines the similarity of searchtext to attr"
    "The value is given in percent, but in case the searchtext appears more than one time in the string the value is over 100%"
    if searchtext == None: 
        warnings.warn('searchtext to evaluate CharField is missing. Attr value:  ' + attr , Warning)
        return 1.0
    
    def isStandard(attr):
        return True
    
    sattr = str(attr)
    ssearchtext = str(searchtext)
    map = {}
    matches = 0 #sum(total number of matches per substring)

    for leng in range(1, len(ssearchtext) + 1): #for all lengths
        for start in range(0, len(ssearchtext)-leng + 1): #for all substrings
            substring = ssearchtext[start:start+leng]
            found = sattr.find(substring)
            while found != -1:
                if (substring in map):
                    map[substring] = map[substring] + 1
                else:
                    map[substring] = 1
                found = sattr.find(substring, found + 1)
                matches = matches + 1
    similarity = matches/((len(ssearchtext)*((len(ssearchtext) + 1.0)))/2.0)
    return similarity

def evalDateField(attr, fparam = None):
    "Defines how to evaluate a date or datetime field"
    def isStandard():
        return True
    if(attr is None): return 0
    return datetime.datetime.now() - attr
