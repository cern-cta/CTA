'''
Created on Sep 16, 2010

@author: kblaszcz
'''
import exceptions
import math

class PostProcessorBase(object):

    def __init__(self):
        '''
        Constructor
        '''
    def process(self, value):
        raise Exception("process method not implemented")
    
    def getUserFriendlyName(self):
        raise Exception("getUserFriendlyName method not implemented")
        
class SubstractMinPostProcessor(PostProcessorBase):
    
    def __init__(self, subtrahend = 0):
        self.subtrahend = subtrahend
        
    def process(self, value):
        return value - self.subtrahend
    
    def getUserFriendlyName(self):
        return "Substract minimum value"
    
class DafaultPostProcessor(PostProcessorBase):
    
    def __init__(self):
        pass
    
    def process(self, value):
        return value
    
    def getUserFriendlyName(self):
        return "--"
    
class LogPostProcessor(PostProcessorBase):
    
    def __init__(self):
        pass
    
    def process(self, value): 
        if value == 0: return 0
        return 1.0/math.log(value - 1.0)

    
    def getUserFriendlyName(self):
        return "Logarithmic inverse"
     