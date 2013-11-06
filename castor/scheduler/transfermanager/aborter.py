#/******************************************************************************
# *                   aborter.py
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
# *
# * aborter class of the transfer manager of the CASTOR project
# * this class is responsible for polling the DB for transfers to abort and
# * effectively abort them on the relevant diskservers.
# *
# * @author Castor Dev team, castor-dev@cern.ch
# *****************************************************************************/

'''aborter thread of the CASTOR transfer manager.
Deals with abort requests by polling the stager DB for new ones'''

import time
import threading
import castor_tools, connectionpool
import dlf
from transfermanagerdlf import msgs

class AborterThread(threading.Thread):
  '''Aborter thread, responsible for connecting to the stager database and getting the list of transfers to be aborted'''

  def __init__(self):
    '''constructor of this thread'''
    super(AborterThread, self).__init__(name='Aborter')
    # whether we should continue running
    self.running = True
    # whether we are connected to the stager DB
    self.stagerConnection = None
    # the global configuration object
    self.config = castor_tools.castorConf()
    # start the thread
    self.setDaemon(True)
    self.start()

  def dbConnection(self):
    '''returns a connection to the stager DB.
    The connection is cached and reconnections are handled'''
    if self.stagerConnection == None:
      self.stagerConnection = castor_tools.connectToStager()
      self.stagerConnection.autocommit = False
    return self.stagerConnection

  def run(self):
    '''main method, containing the infinite loop'''
    try:
      while self.running:
        try:
          # setup an oracle connection and register our interest for 'transfersToAbort' alerts
          stcur = self.dbConnection().cursor()
          try:
            stcur.execute("BEGIN DBMS_ALERT.REGISTER('transfersToAbort'); END;")
            # prepare a cursor for database polling
            stcur = self.dbConnection().cursor()
            subReqIdsCur = self.dbConnection().cursor()
            subReqIdsCur.arraysize = 200
            # infinite loop over the polling of the DB
            while self.running:
              # see whether there is something to do
              # note that this will hang until something comes or the internal timeout is reached
              stcur.callproc('transfersToAbortProc', [subReqIdsCur])
              # kill the corresponding transfers
              subReqIds = tuple(id for item in subReqIdsCur.fetchall() for id in item)
              if subReqIds:
                # call the internal method on all schedulers (including ourselves)
                # note that this is a replication of the exposed_killtransfersinternal function
                # of TransferManagerService
                # Unfortunately, we cannot call it directly (lack of reference to the service object
                # as it's created by the rpyc framework) and we do not want to call it via rpyc
                # as it would creates too many intricated calls
                for scheduler in self.config.getValue('DiskManager', 'ServerHosts').split():
                  timeout = self.config.getValue('TransferManager', 'AdminTimeout', 5, float)
                  connectionpool.connections.killtransfersinternal(scheduler, subReqIds, timeout=timeout)
                # and commit the changes in the DB so that we do not try to drop these transfers again
                self.dbConnection().commit()
          finally:
            stcur.close()
        except Exception, e:
          # "Caught exception in Aborter thread" message
          dlf.writenotice(msgs.ABORTEREXCEPTION, Type=str(e.__class__), Message=str(e))
          # check whether we should reconnect to DB, and do so if needed
          self.dbConnection().checkForReconnection(e)
          # then sleep a bit to not loop to fast on the error
          time.sleep(1)
    finally:
      # try to clean up what we can
      try:
        stcur = self.dbConnection().cursor()
        try:
          stcur.execute("BEGIN DBMS_ALERT.REMOVE('transfersToAbort'); END;")
        finally:
          stcur.close()
      except Exception, e:
        # check whether we should reconnect to DB, and do so if needed
        self.dbConnection().checkForReconnection(e)
      try:
        castor_tools.disconnectDB(self.dbConnection())
      except Exception:
        pass

  def stop(self):
    '''Stops processing of this thread'''
    self.running = False
