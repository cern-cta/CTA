'''
Created on Jul 2, 2010
self explanatory...
@author: kblaszcz
'''

class NoDataAvailableError( Exception ):
    def __init__( self, msg ):
        Exception.__init__( self, msg )