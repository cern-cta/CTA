'''
Created on Sep 16, 2010

PostProcessors apply additional processing on metric values.

For example the DefaultInversePostProcessor makes big values small and small values big.
This enables to generate treemaps where the smallest values are displayed as the biggest.

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
    
class DefaultInversePostProcessor(PostProcessorBase):
    
    def __init__(self):
        pass
    
    def process(self, value): 
        if value == 0: return 0
        return 1.0/(math.log(value + 1.0)/math.log(100))

    
    def getUserFriendlyName(self):
        return "Logarithmic inverse"
    
class LogAndSubstractMinPostProcessor(PostProcessorBase):
    
    def __init__(self, min = 0.0): #default values must always be set!
        self.min = min
    
    def process(self, value): 
        if (value-self.min) == 0: return 0
        return 1.0/(math.log((value-self.min) + 1.0)/math.log(100))

    
    def getUserFriendlyName(self):
        return "Substract minimum and Logarithmic inverse"
    
#class InversePercentagePostProcessor(PostProcessorBase):
#    
#    def __init__(self, max = 0.0):
#        self.max = max
#        
#    def process(self, value):
#        if self.max > 0:
#            return 1.0-(value/self.max)
#        else:
#            return 0
#    
#    def getUserFriendlyName(self):
#        return "Inverse Percentage of the biggest"
    