#/******************************************************************************
# *                   reportmanager.py
# *
# * This file is part of the Castor project.
# * See http://castor.web.cern.ch/castor
# *
# * Copyright (C) 2003  CERN
# * This program is free software; you can redistribute it and/or
# * modify it under the terms of the GNU General Public License
# * as published by the Free Software Foundation; either version 2
# * of the License, or (at your option) any later version.
# * This program is distributed in the hope that it will be useful,
# * but WITHOUT ANY WARRANTY; without even the implied warranty of
# * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# * GNU General Public License for more details.
# * You should have received a copy of the GNU General Public License
# * along with this program; if not, write to the Free Software
# * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
# *
# * ReportManager part of the transfer manager of the CASTOR project
# * This module is responsible for managing reports of the diskservers concerning their
# * status and update the stager database regularly (once per second by default)
# *
# * @author Castor Dev team, castor-dev@cern.ch
# *****************************************************************************/

'''This module is responsible for managing reports of the diskservers concerning their
status and update the stager database regularly (once per second by default)
It handles a cache of reports and has a thread class that allows to update the stager
database regularly (every second by default)
'''

import time
import castor_tools
import dlf
import threading
from transfermanagerdlf import msgs

class ReportCache(object):
    '''cache of reports.
       Reports are stored in a dictionnary per diskserver'''

    def __init__(self):
        '''constructor'''
        # storage for reports not yet sent to the DB
        self.reports = {}
        # some lock to insure thread safe access to the reports
        self.lock = threading.Lock()
    
    def push(self, report):
        '''pushes a new report from the given diskserver in the queue
           Reports should have the following form :
             ((diskServerName, mountPoint, maxFreeSpace, minAllowedFreeSpace, totalSpace,
               freeSpace, nbReadStreams, nbWriteStreams, nbRecalls, nbMigrations), ...) '''
        try:
            self.lock.acquire()
            for FSReport in report:
                # note that we may overwrite previous reports for the same mountPoint
                diskServer, mountPoint = FSReport[0:2]
                self.reports[(diskServer, mountPoint)] = FSReport
        finally:
            self.lock.release()

    def popall(self):
        '''retrieves all reports in one go and empties the queue'''
        try:
            self.lock.acquire()
            allReports = self.reports.values()
            self.reports = {}
        finally:
            self.lock.release()
        return allReports

# the main report cache
_reportCache = ReportCache()

def handleStateReport(report):
    '''handles a report received from a diskserver. Reports should have the following form :
       ((diskServerName, mountPoint, maxFreeSpace, minAllowedFreeSpace, totalSpace,
         freeSpace, nbReadStreams, nbWriteStreams, nbRecalls, nbMigrations), ...) '''
    # add receive report to the cache of reports to be sent to the stager DB
    print 'HERE;'
    _reportCache.push(report)

class ReportManagerThread(threading.Thread):
    '''This thread is regularly updating the stager database with received status'''

    def __init__(self):
        '''constructor of this thread'''
        super(ReportManagerThread, self).__init__(name='ReportManager')
        # whether we should continue running
        self.running = True
        # the global configuration object
        self.config = castor_tools.castorConf()
        # connection to the stager DB
        self.stagerConnection = None
        # start the thread
        self.setDaemon(True)
        self.start()

    def dbConnection(self):
        '''returns a connection to the stager DB.
        The connection is cached and reconnections are handled'''
        if self.stagerConnection == None:
            self.stagerConnection = castor_tools.connectToStager()
            self.stagerConnection.autocommit = True
        return self.stagerConnection

    def run(self):
        '''main method, containing the infinite loop'''
        while self.running:
            try:
                # get list of waiting reports
                reports = _reportCache.popall()
                # update the stager DB
                strParams = sum([list(report[0:2]) for report in reports], [])
                strNums = sum([list(report[2:]) for report in reports], [])
                stcur = self.dbConnection().cursor()
                if not strNums:
                    # nothing to report here. We will still connect as the report
                    # is also triggering the check for too old heartbeats
                    # Sadly ORACLE does not like empty lists ("PLS-00418: array bind
                    # type must match PL/SQL table row type error") so we send a
                    # distinguisable single value
                    strParams = ['Empty']
                    strNums = [0]
                stcur.callproc('storeReports', [strParams, strNums])
            except Exception, e:
                # "Caught exception in ReportManager thread" message
                dlf.writeerr(msgs.REPORTMANAGEREXCEPTION, error=str(e))
            # Wait until the next DB update
            time.sleep(self.config.getValue('TransferManager', 'HeartBeatDBUpdateInterval', 1.0, float))

    def stop(self):
        '''Stops processing of this thread'''
        self.running = False
