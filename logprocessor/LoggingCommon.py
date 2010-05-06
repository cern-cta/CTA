#!/usr/bin/env python
#-------------------------------------------------------------------------------
# Author: Lukasz Janyst <ljanyst@cern.ch>
# Date:   05.08.2009
# File:   LoggingCommon.py
# Desc:   Implements common functionality for the log processor
#-------------------------------------------------------------------------------

import inspect
import sys
import os
import time
import datetime
import stat
from datetime import date

# Bug Fix: python 2.3 has no os.SEEK_END constant
SEEK_END = 2

#-------------------------------------------------------------------------------
class ConfigError( Exception ):
    def __init__( self, msg ):
        Exception.__init__( self, msg )

#-------------------------------------------------------------------------------
def processConfig( config ):
    """
    Convert the information supplied by the configuration parser
    to a dictionary containing lists of sources destinations and processes
    """

    #---------------------------------------------------------------------------
    # Declare the dictionaries
    #---------------------------------------------------------------------------
    processes    = {}
    sources      = {}
    destinations = {}

    cfg = {}
    cfg['processes']    = processes
    cfg['sources']      = sources
    cfg['destinations'] = destinations

    #---------------------------------------------------------------------------
    # Process the sections of the configuration file
    #---------------------------------------------------------------------------
    for i in config.sections():
        #-----------------------------------------------------------------------
        # Create a dictionary from the items of this section
        #-----------------------------------------------------------------------
        secDict = {}
        for item in config.items(i):
            secDict[item[0]] = item[1]

        #-----------------------------------------------------------------------
        # Parse the processes
        #-----------------------------------------------------------------------
        if i[0:8] == 'process-':
            if len( i ) < 9:
                raise ConfigError( 'All processes must be named' )

            processes[i[8:]] = secDict

        #-----------------------------------------------------------------------
        # Parse the sources
        #-----------------------------------------------------------------------
        elif i[0:7] == 'source-':
            if len( i ) < 8:
                raise ConfigError( 'All sources must be named' )

            sources[i[7:]] = secDict

        #-----------------------------------------------------------------------
        # Parse the destinations
        #-----------------------------------------------------------------------
        elif i[0:5] == 'dest-':
            if len( i ) < 6:
                raise ConfigError( 'All destinations must be named' )

            destinations[i[5:]] = secDict

    return cfg

#-------------------------------------------------------------------------------
class Process( object ):
    """
    Class representing processes
    """
    def __init__( self, name, source, destination,
                  sourceConfig = {}, destConfig = {} ):
        self.name           = name
        self.source         = source
        self.destination    = destination
        self.__sourceConfig = sourceConfig
        self.__destConfig   = destConfig

    def initialize( self ):
        self.source.initialize( self.__sourceConfig )
        self.destination.initialize( self.__destConfig )

    def finalize( self ):
        self.source.finalize()
        self.destination.finalize()

    def run( self ):
        while True:
            try:
                message = self.source.getMessage()
                if not message:
                    break
                self.destination.sendMessage( message )
            except ValueError, e:
                print datetime.datetime.now(),
                print 'Errors occurred while processing message:', e

#-------------------------------------------------------------------------------
class MsgDestination(object):
    """
    Abstract interface for message destination
    """
    #---------------------------------------------------------------------------
    def sendMessage( self, msg ):
        raise NotImplementedError( 'Use one of the base classes' )

    #---------------------------------------------------------------------------
    def finalize( self ):
        pass

    #---------------------------------------------------------------------------
    def initizlize( self, config ):
        pass


#-------------------------------------------------------------------------------
class MsgSource(object):
    """
    Abstract interface for message source
    """

    #---------------------------------------------------------------------------
    def getMessage( self ):
        raise NotImplementedError( 'Use one of the base classes' )

    #---------------------------------------------------------------------------
    def initialize( self, config ):
        pass

    #---------------------------------------------------------------------------
    def finalize( self ):
        pass


#-------------------------------------------------------------------------------
# File source
#-------------------------------------------------------------------------------
class FileSource(MsgSource):
    """
    This message source opens a file, reads all of its contents and treats
    each line as a separate message
    """

    #---------------------------------------------------------------------------
    def __init__( self  ):
        MsgSource.__init__( self )
        self.transform = lambda x: {}

    #---------------------------------------------------------------------------
    def getMessage( self ):
        line = self.__file.readline()
        if not line:
            return None
        return self.transform( line )

    #---------------------------------------------------------------------------
    def initialize( self, config ):
        self.__file = file( config['path'], 'r' )

    #---------------------------------------------------------------------------
    def finalize( self ):
        self.__file.close()


#-------------------------------------------------------------------------------
# Pipe source
#-------------------------------------------------------------------------------
class PipeSource(MsgSource):
    """
    This message source opens a file, finds the end of it and waits for new
    content. It treats each new line as a separate message
    """
    #---------------------------------------------------------------------------
    def __init__( self ):
        self.transform = lambda x: {}
        self.__prevline = ''
        self.__lastline = time.time()
        self.__inputRotated = False

    #---------------------------------------------------------------------------
    def getMessageNoDynfiles( self ):
        while True:
            line = self.__file.readline()

            #-------------------------------------------------------------------
            # Nothing else left in the file
            #-------------------------------------------------------------------
            if not line:
                #---------------------------------------------------------------
                # We have been notified about the input logfile rotation and
                # the file exist
                #---------------------------------------------------------------
                if self.__inputRotated and os.path.exists( self.__path ):
                    self.__prevline = ''
                    self.__file.close()
                    self.__file = open( self.__path, 'r' )
                    self.__inputRotated = False

                #---------------------------------------------------------------
                # We haven't seen any log messages for over 1 minute, this might
                # be an indication that the file is no longer being updated by
                # syslog. Here we check the change time of the original path,
                # if its new, then we should trigger an internal SIGHUP like
                # operation which will reopen the file.
                #---------------------------------------------------------------
                if ( time.time() - self.__lastline ) > 60:
                    try:
                        ctime = os.stat( self.__path )[stat.ST_CTIME]
                        if ( time.time() - ctime ) > 60:
                            print datetime.datetime.now(), 'Logrotation or no activity detected, reopening logfile: ' + self.__path
                            self.__lastline = time.time()
                            self.__inputRotated = True
                    except OSError:
                        pass

                #---------------------------------------------------------------
                # Just no new data or the old file has been logrotated and
                # the new one has not been created yet, so take a nap
                # and decide later
                #---------------------------------------------------------------
                else:
                    time.sleep( 0.25 )

                continue

            #-------------------------------------------------------------------
            # We have encountered a line that is not really a full line and
            # have to compensate
            #-------------------------------------------------------------------
            if self.__prevline:
                line = self.__prevline + line
                self.__prevline = ''
            if line.endswith( '\n' ):
                self.__lastline = time.time()
                return self.transform (line)
            else:
                self.__prevline = line

    #---------------------------------------------------------------------------
    def getMessageDynfiles( self ):
        if not self.__file:
            self.waitForFile()

        line = self.__file.readline()
        if not line:
            while True:
                time.sleep( 0.25 )
                line = self.__file.readline()
                if line:
                    return self.transform( line )
                else:
                    if self.__openDate < date.today():
                        self.__file.close()
                        self.waitForFile()
        return self.transform( line )

    #---------------------------------------------------------------------------
    def waitForFile( self ):
        while True:
            p = date.today()
            path =  self.__path + '/'
            path += "%d-%02d-%02d.log" % (p.year, p.month, p.day)
            try:
                self.__file = file( path, 'r' )
            except IOError:
                time.sleep( 1 )
                continue
            self.__openDate = p
            if self.__seek:
                self.__file.seek( 0, SEEK_END )
                self.__seek = False
            break

    #---------------------------------------------------------------------------
    def initialize( self, config ):
        #-----------------------------------------------------------------------
        # Determine if we need to use dynfiles
        #-----------------------------------------------------------------------
        if config.has_key( 'dynfiles' ):
            if config['dynfiles'] == 'true':
                self.__dynfiles = True
            elif config['dynfiles'] == 'false':
                self.__dynfiles = False
            else:
                raise ConfigError( 'PipeSource: only true and false are ' +
                                   'are supported as values for dynfiles' )
        else:
            self.__dynfiles = False

        #-----------------------------------------------------------------------
        # Should we seek for the end of the file?
        #-----------------------------------------------------------------------
        if config.has_key( 'seek' ):
            if config['seek'] == 'true':
                self.__seek = True
            elif config['seek'] == 'false':
                self.__seek = False
            else:
                raise ConfigError( 'PipeSource: only true and false are ' +
                                   'are supported as values for dynfiles' )
        else:
            self.__seek = True

        #-----------------------------------------------------------------------
        # Check if we have the path specified
        #-----------------------------------------------------------------------
        if not config.has_key( 'path' ):
            raise ConfigError( 'Input file path is not specified' )

        self.__path = config['path']

        #-----------------------------------------------------------------------
        # We are using dynfiles
        #-----------------------------------------------------------------------
        if self.__dynfiles:
            self.__file = None
            self.getMessage = self.getMessageDynfiles
        #-----------------------------------------------------------------------
        # We are not using dynfiles
        #-----------------------------------------------------------------------
        else:
            self.__file = open( config['path'], 'r' )
            if self.__seek:
                self.__file.seek( 0, SEEK_END )
            self.getMessage = self.getMessageNoDynfiles   

    #---------------------------------------------------------------------------
    def finalize( self ):
        if self.__file:
            self.__file.close()


#-------------------------------------------------------------------------------
def createObject( moduleName, className ):
    """
    Create object of given class located in given module
    """
    if not moduleName in sys.modules.keys():
        try:
            module = __import__( moduleName )
        except ImportError, e:
            raise ConfigError( 'Unable to load module: ' + module )
    else:
        module = sys.modules[moduleName]

    classes = dict(inspect.getmembers( module, inspect.isclass ))
    if not className in classes:
        raise ConfigError( 'Unable to find ' + className + ' class in ' +
                           moduleName )

    cls = classes[className]
    return cls()


#-------------------------------------------------------------------------------
def createProcess( name, config ):
    """
    Create process of given name using given configuration data
    """
    #---------------------------------------------------------------------------
    # Check if the requested process is defined
    #---------------------------------------------------------------------------
    if not config['processes'].has_key( name ):
        raise ConfigError( 'Requested process is not defined in the config ' +
                           'file: ' + name )

    procConfig = config['processes'][name]

    #---------------------------------------------------------------------------
    # Check if the requested source and destination definitions are present
    #---------------------------------------------------------------------------
    if not procConfig.has_key('source') or not procConfig.has_key(
           'destination'):
        raise ConfigError( 'Both source and destination have to be defined ' +
                           'for the process ' + name )

    srcName  = procConfig['source']
    destName = procConfig['destination']

    #---------------------------------------------------------------------------
    # Create the source object
    #---------------------------------------------------------------------------
    if not config['sources'].has_key( srcName ):
        raise ConfigError( 'Requested source is not defined in the config ' +
                           'file: ' + srcName )

    srcConfig = config['sources'][srcName]

    if not srcConfig.has_key('module') or not srcConfig.has_key(
           'class'):
        raise ConfigError( 'Both module and class have to be defined ' +
                           'for the source ' + srcName )

    srcObj = createObject( srcConfig['module'], srcConfig['class'] )

    #---------------------------------------------------------------------------
    # Create the destination object
    #---------------------------------------------------------------------------
    if not config['destinations'].has_key( destName ):
        raise ConfigError( 'Requested destination is not defined in the ' +
                           'config file: ' + destName )

    destConfig = config['destinations'][destName]

    if not destConfig.has_key('module') or not destConfig.has_key(
           'class'):
        raise ConfigError( 'Both module and class have to be defined ' +
                           'for the destination ' + destName )

    destObj = createObject( destConfig['module'], destConfig['class'] )

    #---------------------------------------------------------------------------
    # Create the process object
    #---------------------------------------------------------------------------
    return Process( name, srcObj, destObj, srcConfig, destConfig )

