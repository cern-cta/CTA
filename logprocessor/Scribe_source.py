#!/usr/bin/env python
#-------------------------------------------------------------------------------
# Author: Lukasz Janyst <ljanyst@cern.ch>
# Date:   10.08.2009
# File:   DLF.py
# Desc:   Implements the DLF syslog message processing and storing in the
#         database
#-------------------------------------------------------------------------------


#-------------------------------------------------------------------------------
# Imports
#-------------------------------------------------------------------------------
import sys
import re
import time
import LoggingCommon





#-------------------------------------------------------------------------------
class ScribeMsgparser:
    """
    Message parser

    Parse the input messages according to given regular expressions
    """

    #---------------------------------------------------------------------------
    def __init__( self ):

        self.mexp = re.compile( r"""
                               ^                                         # beginning of the string
                               \s*                                       # white spaces
                               (\w+)                                     # key
                               \s*                                       # white spaces
                               =                                         # equals
                               \s*                                       # white spaces
                               (?:"(.*?)" | (\S+))                       # value
                                  """, re.VERBOSE )
        pass


    #---------------------------------------------------------------------------
    def __call__( self, msg ):
        """
        Parse the input string
        """




        #-------------------------------------------------------------------
        # Parse the key-value pairs
        #-------------------------------------------------------------------
        vals = []
        start = 0
        while True:
            res = self.mexp.match( msg[start:] )
            if not res:
                break
            start += res.span()[1]
            if res.group(2):
                val = res.group(2)
            else:
                val = res.group(3)
            vals.append((res.group(1), val))

        # key-value subdictionary or just one
        # result['key-value'] = dict(vals)


        #result_Tuples=regex.findall(msg)

        #print ""
        #print "---------------- MSG: ----------------"
        #print msg
        #print "--------------- RESULT: --------------"
        
        # Trick for old naming...
        result_Tuples=vals


        # Check i fthe message has been recognized
        if not result_Tuples:
            
            ########################################
            # Handling of messages not recognized: #
            ########################################

            # If the message is not recognized, we can both ignore it
            # (and reurn an empty message of kind 'log') or raise a Value Error

            #raise ValueError( 'Message: "' + msg.strip() + '" is malformed' )
            
            # HARD debugging:
            #print "Error parsing msg: "+msg  
           
            return {'MSG_NOT_VALID':'1','type':"log"}  


        # Create the result dictionary
        result={}

        # Time stuff
        time='' # and then add USECS
        date=''
        timezone=''
        usecs=''

        # Key-value pairs...
        for Tuple in result_Tuples:

            # Conversions..
            try:      
                # Time format for semplyfing life.. :)
                if (Tuple[0]=="TIMESTAMP"):
                    # time, date, timezone
                    splitted=Tuple[1].split('T')
                    #print splitted
                    time=splitted[1][0:8]
                    date=splitted[0]
                    timezone=splitted[1][8:14]

                # Just create key-value pairs
                result[Tuple[0]]=Tuple[1]
                
            except:
                # If something went wrong
                print "Error parsing msg that should be valid: "+msg   
                return {'MSG_NOT_VALID':'1','type':"log"}  


        # Add time stuff
        result['date']=date
        result['time']=time+'.'+usecs
        result['timezone']=timezone

        # Parameter for backward compatibility
        result['type']="log"


        return dict(result)


#-------------------------------------------------------------------------------
class ScribeLogFile(LoggingCommon.MsgSource):
    #---------------------------------------------------------------------------
    def __init__( self ):
        self.source = LoggingCommon.PipeSource()

    #---------------------------------------------------------------------------
    def getMessage( self ):
        return self.source.getMessage()

    #---------------------------------------------------------------------------
    def initialize( self, config ):
        if not config.has_key( 'type' ):
            raise LoggingCommon.ConfigError( 'DLFLogFile: type of input ' +
                                             'was not specified' )

        if config['type'] != 'pipe' and config['type'] != 'file':
            raise LoggingCommon.ConfigError( 'DLFLogFile: unknown input ' +
                                             'type: ' + config['type'] )

        if config['type'] == 'file':
            self.source = LoggingCommon.FileSource()

        self.source.transform = ScribeMsgparser()
        self.source.initialize( config )

    #---------------------------------------------------------------------------
    def finalize( self ):
        self.source.finalize()
