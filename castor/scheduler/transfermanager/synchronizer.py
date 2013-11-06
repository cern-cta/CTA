#/******************************************************************************
# *                   synchronizer.py
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
# * synchronizer class of the transfer manager of the CASTOR project
# * this class is responsible for regularly synchronizing the stager DB with the running
# * transfers, meaning that it will check that transfers running for already long according to the DB
# * are effectively still running. If they are not, is will update the DB accordingly
# *
# * @author Castor Dev team, castor-dev@cern.ch
# *****************************************************************************/

'''synchronizer module of the CASTOR transfer manager.
Handle background synchronization between the stager DB and the transfer manager'''

import time
import threading
import castor_tools
import random
import socket
import dlf
from transfermanagerdlf import msgs
import connectionpool, diskserverlistcache

class SynchronizerThread(threading.Thread):
  '''synchronizer class of the transfer manager of the CASTOR project
  this class is responsible for regularly synchronizing the stager DB with the running
  transfers, meaning that it will check that transfers running for already long according to the DB
  are effectively still running. If they are not, is will update the DB accordingly'''

  def __init__(self):
    '''constructor of this thread'''
    super(SynchronizerThread, self).__init__(name='Synchronizer')
    # whether we should continue running
    self.running = True
    # whether we are connected to the stager DB
    self.stagerConnection = None
    # our own name
    self.hostname = socket.getfqdn()
    # configuration
    self.config = castor_tools.castorConf()
    # start the thread
    self.setDaemon(True)
    self.start()

  def dbConnection(self, autocommit=True):
    '''returns a connection to the stager DB.
    The connection is cached and reconnections are handled'''
    if self.stagerConnection == None:
      self.stagerConnection = castor_tools.connectToStager()
    self.stagerConnection.autocommit = autocommit
    return self.stagerConnection

  def checkDisappearedTransfers(self):
    '''check of running transfers that have disappeared from the scheduling system but are still in DB.
    Raises exception in case of DB error that are handled in the calling method.'''
    # 'Synchronizing stager DB with Transfer Manager' message
    dlf.writedebug(msgs.SYNCDBWITHTM)
    # get the timeout to be used
    timeout = self.config.getValue('TransferManager', 'AdminTimeout', 5, float)
    # prepare a cursor for database polling
    stcur = self.dbConnection().cursor()
    try:
      # prepare a cursor for the result
      subReqIdsCur = self.dbConnection().cursor()
      subReqIdsCur.arraysize = 200
      # list pending and running transfers in the stager database
      stcur.callproc('getSchedulerTransfers', [subReqIdsCur])
      subReqIds = set(subReqIdsCur.fetchall())
      try:
        # list pending and running transfers in the scheduling system
        allTMTransfers = set()
        diskservers = diskserverlistcache.diskServerList.getset()
        for ds in diskservers:
          # call the function on the appropriate diskserver
          allTMTransfers = allTMTransfers | set(connectionpool.connections.transferset(ds, timeout=timeout))
        # find out the set of transfers in the DB and no more in the scheduling system
        transfersToFail = list(subReqIds - allTMTransfers)
        # and inform the stager
        if transfersToFail:
          # get back the fileids for the logs. We need to go back to the DB just for this
          conn = self.dbConnection(False)
          fileIdsCur = conn.cursor()
          fileIdsCur.arraysize = 200
          stcur = conn.cursor()
          stcur.callproc('getFileIdsForSrs', [[transferid for transferid, reqid in transfersToFail], fileIdsCur])
          fileids = [tuple(item) for item in fileIdsCur.fetchall()]
          conn.commit()
          # 'Transfer killed by synchronization as it disappeared from the scheduling system'
          for (transferid, reqid), fileid in zip(transfersToFail, fileids):
            dlf.write(msgs.SYNCHROKILLEDTRANSFER, subreqid=transferid, reqid=reqid, fileid=fileid)
          # prepare the transfers so that we have only tuples going onver the wire
          transfers = tuple([tuple(item) for item in
                             zip([transferid for transferid, reqid in transfersToFail],
                                 fileids,
                                 [1015] * len(transfersToFail), # SEINTERNAL error code
                                 ['Transfer has disappeared from the scheduling system'] * len(transfersToFail),
                                 [reqid for transferid, reqid in transfersToFail])])
          # finally inform the stager
          connectionpool.connections.transfersKilled(self.hostname, transfers, timeout=timeout)
        else:
          # 'No discrepancy during synchronization' message
          dlf.writedebug(msgs.SYNCNODISCREPANCY)
      except Exception, e:
        # we could not list all pending running transfers in the system
        # Thus we have to give up with synchronization for this round
        # 'Error caught while trying to synchronize DB transfers with scheduler transfers. Giving up for this round.'
        dlf.writenotice(msgs.SYNCHROFAILED, Type=str(e.__class__), Message=str(e))
    finally:
      stcur.close()

  def checkD2dSrcLeftBehind(self):
    '''check of d2d source transfers left behind, that is over in the DB. This happens in case of timeout on
    the message signaling the end of the transfer to the source diskserver'''
    # 'Synchronizing stager DB with running d2d sources' message
    dlf.writedebug(msgs.SYNCDBWITHD2DSRC)
    try:
      # list d2d source running and handled by this transfer manager
      allTMD2dSrc = connectionpool.connections.getAllRunningD2dSourceTransfers(self.hostname, timeout=None)
      allTMD2dSrcSet = set([transferid for transferid, reqid, fileid in allTMD2dSrc])
    except connectionpool.Timeout:
      # we could not list all pending running transfers in the system because of timeouts
      # Thus we have to give up with synchronization for this round
      # 'Error caught while trying to get rid of disk to disk sources left behind. Giving up for this round.'
      dlf.writenotice(msgs.D2DSYNCFAILED)
      return
    except Exception:
      # we could not list all pending running transfers in the system for a unexpected error
      # 'Error caught while trying to get rid of disk to disk sources left behind. Giving up for this round.'
      dlf.writeerr(msgs.D2DSYNCFAILED)
      return
    # prepare a cursor for database polling
    stcur = self.dbConnection().cursor()
    try:
      # prepare a cursor for the results
      subReqIdsCur = self.dbConnection().cursor()
      subReqIdsCur.arraysize = 200
      # list pending and running transfers in the stager database
      stcur.callproc('getSchedulerD2dTransfers', [subReqIdsCur])
      subReqIds = set([t[0] for t in subReqIdsCur.fetchall()])
      # find out the set of these d2d source transfers that are no more in the DB
      transfersToEnd = list(allTMD2dSrcSet - subReqIds)
      # and end them
      if transfersToEnd:
        tid2fileid = dict([(transferid, fileid) for transferid, reqid, fileid in allTMD2dSrc])
        for transferid in transfersToEnd:
          # 'Transfer ended by synchronization as the transfer disappeared from the DB' message
          dlf.write(msgs.SYNCHROENDEDTRANSFER, subreqid=transferid, fileid=tid2fileid[transferid])
          try:
            connectionpool.connections.d2dendsrc(self.hostname, transferid)
          except Exception:
            # not a big deal, it may have ended in the mean time. Otherwise, we will retry later
            pass
      else:
        # 'No disk to disk source source left behind'
        dlf.writedebug(msgs.NOD2DLEFTBEHIND)
    finally:
      stcur.close()

  def run(self):
    '''main method, containing the infinite loop'''
    try:
      # first wait a bit before starting so that we are not syncronized between
      # daemons at a restart, or after errors
      slept = 0
      checkinterval = random.randint(1, self.config.getValue("TransferManager", "SynchronizationInterval", 300, int))
      while slept < checkinterval:
        # note that we sleep one second at a time so that we can exit
        # if the thread stops running
        if not self.running:
          break
        time.sleep(1)
        slept = slept + 1
      # loop over reconnections to the DB.
      # This loop should never actually loop unless we have DB issues
      while self.running:
        try:
          # infinite loop over the polling of the DB
          while self.running:
            # Go for a check of running transfers that have disappeared from the scheduling system
            self.checkDisappearedTransfers()
            # Go for a check of d2d source transfers that have been left behind (so no more in DB)
            self.checkD2dSrcLeftBehind()
            # sleep until next check
            slept = 0
            while slept < self.config.getValue("TransferManager", "SynchronizationInterval", 300, int):
              # note that we sleep one second at a time so that we can exit
              # if the thread stops running
              if not self.running:
                break
              time.sleep(1)
              slept = slept + 1
        except Exception, e:
          # "Caught exception in Synchronizer thread" message
          dlf.writeerr(msgs.SYNCHROEXCEPTION, Type=str(e.__class__), Message=str(e))
          # check whether we should reconnect to DB, and do so if needed
          self.dbConnection().checkForReconnection(e)
          # do not loop too fast
          time.sleep(1)
    finally:
      # try to clean up what we can
      try:
        castor_tools.disconnectDB(self.dbConnection())
      except Exception:
        pass

  def stop(self):
    '''Stops processing of this thread'''
    self.running = False
