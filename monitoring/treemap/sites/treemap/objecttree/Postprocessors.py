'''
Created on Sep 16, 2010

@author: kblaszcz
'''
import exceptions

class PostProcessorBase(object):

    def __init__(self):
        '''
        Constructor
        '''
    def process(self, value):
        raise Exception("process method not implemented")
        
class SubstractPostProcessor(PostProcessorBase):
    
    def __init__(self, subtrahend):
        self.subtrahend = subtrahend
        
    def process(self, value):
        return value - self.subtrahend