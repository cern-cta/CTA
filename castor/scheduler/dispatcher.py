#/******************************************************************************
# *                   dispatcher.py
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
# * @(#)$RCSfile: castor_tools.py,v $ $Revision: 1.9 $ $Release$ $Date: 2009/03/23 15:47:41 $ $Author: sponcec3 $
# *
# * dispatcher class of the transfer manager of the CASTOR project
# * this class is responsible for polling the DB for transfers to dispatch and
# * effectively dispatch them on the relevant diskservers.
# *
# * @author Castor Dev team, castor-dev@cern.ch
# *****************************************************************************/

import time
import threading
import cx_Oracle
import socket
import castor_tools
import Queue
import dlf
from transfermanagerdlf import msgs

class Worker(threading.Thread):
  '''Worker thread, responsible for scheduling effectively the transfers on the diskservers'''

  def __init__(self, workqueue):
    '''constructor'''
    threading.Thread.__init__(self)
    # the queue to work with
    self.workqueue = workqueue
    # whether to continue running
    self.running = True
    # start the thread
    self.daemon = True
    self.start()

  def stop(self):
    '''Stops the thread processing'''
    self.running = False
  
  def run(self):
    '''main method to the threads. Only get work from the queue and do it'''
    while self.running:
      try:
        func, args = self.workqueue.get(True, 1)
        func(*args)
      except Queue.Empty:
        # we've timed out, let's just retry. We only use the timeout so that this
        # thread can stop even if there is nothing in the queue
        pass
      except Exception, e:
        # "Caught exception in Worker thread" message
        dlf.writeerr(msgs.WORKEREXCEPTION, type=str(e.__class__), msg=str(e))

class DBUpdater(threading.Thread):
  '''Worker thread, responsible for updating DB asynchronously and in bulk after the transfer scheduling'''

  def __init__(self, workqueue):
    '''constructor'''
    threading.Thread.__init__(self)
    # whether we are connected to the stager DB
    self.stagerConnection = None
    # the queue to work with
    self.workqueue = workqueue
    # whether to continue running
    self.running = True
    # start the thread
    self.daemon = True
    self.start()

  def stop(self):
    '''Stops the thread processing'''
    self.running = False

  def dbConnection(self):
    '''returns a connection to the stager DB.
    The connection is cached and reconnections are handled'''
    if self.stagerConnection == None:
      self.stagerConnection = castor_tools.connectToStager()
      self.stagerConnection.autocommit = True
    return self.stagerConnection

  def run(self):
    '''main method to the threads. Only get work from the queue and do it'''
    try:
      while self.running:
        # get something from the queue and then empty the queue and list all the updates to be done in one bulk
        successes = []
        failures = []
        # check whether there is something to do
        try:
          # we go out every second in case the thread has ended in the meantime
          transferid, fileid, errcode, errmsg, reqid = self.workqueue.get(True, 1)
          if errcode != None:
            failures.append((transferid, fileid, errcode, errmsg, reqid))
          else:
            successes.append((transferid, fileid, reqid))
        except Queue.Empty:
          continue
        # empty the queue so that we go only once to the DB
        try:
          while True:
            transferid, fileid, errcode, errmsg, reqid = self.workqueue.get(False)
            if errcode != None:
              failures.append((transferid, fileid, errcode, errmsg, reqid))
            else:
              successes.append((transferid, fileid, reqid))
        except Queue.Empty:
          # we are over, the queue is empty
          pass
        # Now call the DB for failures
        if failures:
          transferids, fileids, errcodes, errmsgs, reqids = zip(*failures)
          try:
            stcur = self.dbConnection().cursor()
            try:
              stcur.execute("BEGIN transferFailedLockedFile(:1, :2, :3); END;", [list(transferids), list(errcodes), list(errmsgs)])
              for transferid, fileid, errcode, errmsg, reqid in failures:
                # 'Failed transfer' message
                dlf.writeerr(msgs.FAILEDTRANSFER, subreqid=transferid, reqid=reqid, fileid=fileid, errorcode=errcode, errmsg=errmsg)
            finally:
              stcur.close()
          except Exception, e:
            for transferid, fileid, errcode, errmsg, reqid in failures:
              # 'Exception caught while failing transfer' message
              dlf.writeerr(msgs.FAILINGTRANSFEREXCEPTION, subreqid=transferid, reqid=reqid, fileid=fileid, type=str(e.__class__), msg=str(e))
            # check whether we should reconnect to DB, and do so if needed
            self.dbConnection().checkForReconnection(e)
        # And call the DB for successes
        if successes:
          for transferid, fileid, reqid in successes:
            # 'Marking transfers scheduled' message
            dlf.write(msgs.TRANSFERSCHEDULED, subreqid=transferid, reqid=reqid, fileid=fileid)
          try:
            stcur = self.dbConnection().cursor()
            try:
              stcur.execute("BEGIN transferScheduled(:transferids); END;", [[transferid for transferid, fileid, reqid in successes]])
            finally:
              stcur.close()
          except Exception, e:
            for transferid, fileid, reqid in successes:
              # 'Exception caught while marking transfer scheduled' message
              dlf.writeerr(msgs.TRANSFERSCHEDULEDEXCEPTION, subreqid=transferid, reqid=reqid, fileid=fileid,
                           type=str(e.__class__), msg=str(e))
            # check whether we should reconnect to DB, and do so if needed
            self.dbConnection().checkForReconnection(e)
    finally:
      if self.stagerConnection != None:
        try:
          castor_tools.disconnectDB(self.stagerConnection)
        except Exception:
          pass


class Dispatcher(threading.Thread):
  '''scheduling thread, responsible for connecting to the stager database and scheduling transfers'''

  def __init__(self, connections, queueingTransfers, nbWorkers=5):
    '''constructor of this thread. Arguments are the connection pool and the transfer queue to use'''
    threading.Thread.__init__(self)
    # whether we should continue running
    self.running = True
    # a pool of connections to the diskservers
    self.connections = connections
    # the list of queueing transfers
    self.queueingTransfers = queueingTransfers
    # our own name
    self.hostname = socket.getfqdn()
    # a counter of number of scheduled transfers in the current second
    self.nbTransfersScheduled = 0
    # the current second, so that we can reset the previous counter when it changes
    self.currentSecond = 0
    # whether we are connected to the stager DB
    self.stagerConnection = None
    # a queue of work to be done by the workers
    self.workToDispatch = Queue.Queue()
    # a queue of updates to be done in the DB
    self.updateDBQueue = Queue.Queue()
    # a thread pool of Schedulers
    self.workers = []
    for i in range(nbWorkers):
      t = Worker(self.workToDispatch)
      t.setName('Worker')
      self.workers.append(t)
    # a DBUpdater thread
    self.dbthread = DBUpdater(self.updateDBQueue)
    self.dbthread.setName('DBUpdater')

  def join(self, timeout=None):
    # join first the other threads
    for w in self.workers:
      w.join(timeout)
    self.dbthread.join(timeout)
    # then call join on master one
    threading.Thread.join(self, timeout)

  def dbConnection(self):
    '''returns a connection to the stager DB.
    The connection is cached and reconnections are handled'''
    if self.stagerConnection == None:
      self.stagerConnection = castor_tools.connectToStager()
      self.stagerConnection.autocommit = True
    return self.stagerConnection

  def scheduleD2d(self, reqId, srSubReqId, cfFileId, cfNsHost, reqDestDiskCopy,
                  reqSourceDiskCopy, reqSvcClass, reqCreationTime, sourceRfs, srRfs):
    '''Schedules a disk to disk copy on the source and destinations and handle issues '''
    # prepare command line
    basecmd = ["/usr/bin/d2dtransfer",
               "-r", str(reqId),
               "-s", str(srSubReqId),
               "-F", str(int(cfFileId)),
               "-H", str(cfNsHost),
               "-D", str(int(reqDestDiskCopy)),
               "-X", str(int(reqSourceDiskCopy)),
               "-S", str(reqSvcClass),
               "-t", str(int(reqCreationTime))]
    # first schedule a transfer on the source node
    schedSourceCandidates = [candidate.split(':') for candidate in sourceRfs.split('|')]
    transferid = srSubReqId
    fileid = (str(cfNsHost), int(cfFileId))
    # 'Scheduling d2d source' message
    dlf.writedebug(msgs.SCHEDD2DSRC, subreqid=transferid, reqid=reqId, fileid=fileid, machine=str(schedSourceCandidates[0]))
    diskserver, mountpoint = schedSourceCandidates[0]
    cmd = tuple(basecmd + ["-R", diskserver+':'+mountpoint])
    arrivaltime =  time.time()
    try:
      # register the transfer in the local list of pending transfers
      self.queueingTransfers.put(transferid, arrivaltime, [(diskserver, cmd)], 'd2dsrc')
      # send the transfer to the appropriate diskserver
      self.connections.scheduleTransfer(diskserver, self.hostname, transferid, cmd, arrivaltime, 'd2dsrc')
    except Exception, e:
      # 'Scheduling d2d source failed' message
      dlf.writeerr(msgs.SCHEDD2DSRCFAILED, subreqid=transferid, reqid=reqId, fileid=fileid, diskserver=diskserver, error=str(e))
      # we coudl not schedule the source so fail the transfer in the DB
      self.updateDBQueue.put((transferid, fileid, 1721, "Unable to schedule on source host", reqId))  # 1721 = ESTSCHEDERR
      # and remove it from the server queue
      self.queueingTransfers.remove(transferid)
      return
    # now schedule on all potential destinations
    schedDestCandidates = [candidate.split(':') for candidate in srRfs.split('|')]
    # 'Scheduling d2d destination' message
    dlf.writedebug(msgs.SCHEDD2DDEST, subreqid=transferid, reqid=reqId, fileid=fileid, machines=str(schedDestCandidates))
    # build the list of hosts and transfers to launch
    transferList = []
    for diskserver, mountpoint in schedDestCandidates:
      cmd = tuple(basecmd + ["-R", diskserver+':'+mountpoint])
      transferList.append((diskserver, cmd))
    # put transfers in the queue of pending transfers
    self.queueingTransfers.put(transferid, arrivaltime, transferList, 'd2ddest')
    # send the transfers to the appropriate diskservers
    scheduleSucceeded = False
    for diskserver, cmd in transferList:
      try:
        self.connections.scheduleTransfer(diskserver, self.hostname, transferid, cmd, arrivaltime, 'd2ddest')
        scheduleSucceeded = True
      except Exception, e:
        # 'Scheduling d2d destination failed' message
        dlf.writeerr(msgs.SCHEDD2DDESTFAILED, subreqid=transferid, reqid=reqId, fileid=fileid, diskserver=diskserver, error=str(e))
    # we are over, check whether we could schedule the destination at all
    if not scheduleSucceeded:
      # we could not schedule anywhere for destination.... so fail the transfer in the DB
      self.updateDBQueue.put((transferid, fileid, 1721, "Unable to schedule on any destination host", reqId))  # 1721 = ESTSCHEDERR
      # and remove it from the server queue
      self.queueingTransfers.remove(transferid)
    else:
      self.updateDBQueue.put((transferid, fileid, None, None, reqId))


  def scheduleTransfer(self, srRfs, clientIp, reqId, srSubReqId, cfFileId, cfNsHost,
                       srProtocol, srId, reqType, srOpenFlags, clientType, clientPort,
                       reqEuid, reqEgid, clientSecure, reqSvcClass, reqCreationTime):
    '''Schedules a disk to disk copy and handle issues '''
    # extract list of candidates where to schedule and log
    schedCandidates = [candidate.split(':') for candidate in srRfs.split('|')]
    transferid = srSubReqId
    fileid = (str(cfNsHost), int(cfFileId))
    # 'Scheduling standard transfer' message
    dlf.writedebug(msgs.SCHEDTRANSFER, subreqid=transferid, reqid=reqId, fileid=fileid, machines=str(schedCandidates))
    # build a list of transfers to schedule for each machine
    arrivaltime =  time.time()
    transferList = []
    for diskserver, mountpoint in schedCandidates:
      ipAddress = int(clientIp)
      cmd = ("/usr/bin/stagerjob",
             "-r", str(reqId),
             "-s", str(srSubReqId),
             "-F", str(int(cfFileId)),
             "-H", str(cfNsHost),
             "-p", str(srProtocol),
             "-i", str(srId),
             "-T", str(int(reqType)),
             "-m", str(srOpenFlags),
             # The client's identification as a string. This consists of the
             # client's object type, IP address and port
             "-C", str(int(clientType)) + ":" + \
               str((ipAddress & 0xFF000000) >> 24) + "." + \
               str((ipAddress & 0x00FF0000) >> 16) + "." + \
               str((ipAddress & 0x0000FF00) >> 8)  + "." + \
               str((ipAddress & 0x000000FF)) + ":" + \
               str(int(clientPort)),
             "-u", str(int(reqEuid)),
             "-g", str(int(reqEgid)),
             "-X", str(int(clientSecure)),
             "-S", str(reqSvcClass),
             "-t", str(int(reqCreationTime)),
             "-R", diskserver+':'+mountpoint)
      transferList.append((diskserver, cmd))
    # put transfers in the queue of pending transfers
    self.queueingTransfers.put(transferid, arrivaltime, transferList)
    # send the transfers to the appropriate diskservers
    scheduleSucceeded = False
    for diskserver, cmd in transferList:
      try:
        self.connections.scheduleTransfer(diskserver, self.hostname, transferid, cmd, arrivaltime)
        scheduleSucceeded = True
      except Exception, e:
        # 'Scheduling standard transfer failed' message
        dlf.writeerr(msgs.SCHEDTRANSFERFAILED, subreqid=transferid, reqid=reqId, fileid=fileid, diskserver=diskserver, error=str(e))
    # we are over, check whether we could schedule at all
    if not scheduleSucceeded:
      # we could not schedule anywhere.... so fail the transfer in the DB
      self.updateDBQueue.put((transferid, fileid, 1721, "Unable to schedule transfer", reqId)) # 1721 = ESTSCHEDERR
      # and remove it from the server queue
      self.queueingTransfers.remove(transferid)
    else:
      self.updateDBQueue.put((transferid, fileid, None, None, reqId))

  def run(self):
    '''main method, containing the infinite loop'''
    configuration = castor_tools.castorConf()
    try:
      while self.running:
        try:
          # setup an oracle connection and register our interest for 'transferReadyToSchedule' alerts
          stcur = self.dbConnection().cursor()
          try:
            stcur.execute("BEGIN DBMS_ALERT.REGISTER('transferReadyToSchedule'); END;")
            # prepare a cursor for database polling
            stcur = self.dbConnection().cursor()
            stcur.arraysize = 50
            srId = stcur.var(cx_Oracle.NUMBER)
            srSubReqId = stcur.var(cx_Oracle.STRING)
            srProtocol = stcur.var(cx_Oracle.STRING)
            srXsize = stcur.var(cx_Oracle.NUMBER)
            srRfs = stcur.var(cx_Oracle.STRING)
            reqId = stcur.var(cx_Oracle.STRING)
            cfFileId = stcur.var(cx_Oracle.NUMBER)
            cfNsHost = stcur.var(cx_Oracle.STRING)
            reqSvcClass = stcur.var(cx_Oracle.STRING)
            reqType = stcur.var(cx_Oracle.NUMBER)
            reqEuid = stcur.var(cx_Oracle.NUMBER)
            reqEgid = stcur.var(cx_Oracle.NUMBER)
            reqUsername = stcur.var(cx_Oracle.STRING)
            srOpenFlags = stcur.var(cx_Oracle.STRING)
            clientIp = stcur.var(cx_Oracle.NUMBER)
            clientPort = stcur.var(cx_Oracle.NUMBER)
            clientVersion = stcur.var(cx_Oracle.NUMBER)
            clientType = stcur.var(cx_Oracle.NUMBER)
            reqSourceDiskCopy = stcur.var(cx_Oracle.NUMBER)
            reqDestDiskCopy = stcur.var(cx_Oracle.NUMBER)
            clientSecure = stcur.var(cx_Oracle.NUMBER)
            reqSourceSvcClass = stcur.var(cx_Oracle.STRING)
            reqCreationTime = stcur.var(cx_Oracle.NUMBER)
            reqDefaultFileSize = stcur.var(cx_Oracle.NUMBER)
            sourceRfs = stcur.var(cx_Oracle.STRING)
            stTransferToSchedule = 'BEGIN transferToSchedule(:srId, :srSubReqId , :srProtocol, :srXsize, :srRfs, :reqId, :cfFileId, :cfNsHost, :reqSvcClass, :reqType, :reqEuid, :reqEgid, :reqUsername, :srOpenFlags, :clientIp, :clientPort, :clientVersion, :clientType, :reqSourceDiskCopy, :reqDestDiskCopy, :clientSecure, :reqSourceSvcClass, :reqCreationTime, :reqDefaultFileSize, :sourceRfs); END;'
            # infinite loop over the polling of the DB
            while self.running:
              # see whether there is something to do
              # not that this will hang until something comes or the internal timeout is reached
              stcur.execute(stTransferToSchedule, (srId, srSubReqId, srProtocol, srXsize, srRfs, reqId, cfFileId,
                                                   cfNsHost, reqSvcClass, reqType, reqEuid, reqEgid, reqUsername,
                                                   srOpenFlags, clientIp, clientPort, clientVersion, clientType,
                                                   reqSourceDiskCopy, reqDestDiskCopy, clientSecure, reqSourceSvcClass,
                                                   reqCreationTime, reqDefaultFileSize, sourceRfs))
              # in case of timeout, we may have nothing to do
              if srId.getvalue() != None:
                # standard transfers and disk to disk copies are not handled the same way
                # but in both cases, errors are handled internally and no exception
                # others than the ones implying the end of the processing
                if int(reqType.getvalue() == 133): # OBJ_StageDiskCopyReplicaRequest
                  self.workToDispatch.put((self.scheduleD2d,
                                           (reqId.getvalue(), srSubReqId.getvalue(), cfFileId.getvalue(),
                                            cfNsHost.getvalue(), reqDestDiskCopy.getvalue(),
                                            reqSourceDiskCopy.getvalue(), reqSvcClass.getvalue(),
                                            reqCreationTime.getvalue(), sourceRfs.getvalue(), srRfs.getvalue())))
                else:
                  self.workToDispatch.put((self.scheduleTransfer,
                                           (srRfs.getvalue(), clientIp.getvalue(), reqId.getvalue(),
                                            srSubReqId.getvalue(), cfFileId.getvalue(), cfNsHost.getvalue(),
                                            srProtocol.getvalue(), srId.getvalue(), reqType.getvalue(),
                                            srOpenFlags.getvalue(), clientType.getvalue(), clientPort.getvalue(),
                                            reqEuid.getvalue(), reqEgid.getvalue(), clientSecure.getvalue(),
                                            reqSvcClass.getvalue(), reqCreationTime.getvalue())))
                # if maxNbTransfersScheduledPerSecond is given, request throttling is active
                # What it does is keep a count of the number of scheduled request in the current second
                # and wait the rest of the second if it reached the limit
                maxNbTransfersScheduledPerSecond = configuration.getValue('TransferManager', 'MaxNbTransfersScheduledPerSecond', -1, int)
                if maxNbTransfersScheduledPerSecond >= 0:
                  currentTime = time.time()
                  currentSecond = int(currentTime)
                  # reset the counters if we've changed second
                  if currentSecond != self.currentSecond:
                    self.currentSecond = currentSecond
                    self.nbTransfersScheduled = 0
                  # increase counter of number of transfers scheduled within the current second
                  self.nbTransfersScheduled = self.nbTransfersScheduled + 1
                  # did we reach our quota of requests for this second ?
                  if self.nbTransfersScheduled >= maxNbTransfersScheduledPerSecond:
                    # check that the second is not just over, so that we do not sleep a negative time
                    if currentTime < self.currentSecond + 1:
                      # wait until the second is over
                      time.sleep(self.currentSecond + 1 - currentTime)
          finally:
            stcur.close()
        except Exception, e:
          # "Caught exception in Dispatcher thread" message
          dlf.writeerr(msgs.DISPATCHEXCEPTION, type=str(e.__class__), msg=str(e))
          # check whether we should reconnect to DB, and do so if needed
          self.dbConnection().checkForReconnection(e)
          # then sleep a bit to not loop to fast on the error
          time.sleep(1)
    finally:
      # try to clean up what we can
      for t in self.workers:
        try:
          t.stop()
          t.join()
        except Exception:
          pass
      try:
        self.dbthread.stop()
        self.dbthread.join()
      except Exception:
        pass
      try:
        stcur = self.dbConnection().cursor()
        try:
          stcur.execute("BEGIN DBMS_ALERT.REMOVE('transferReadyToSchedule'); END;")
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
    for w in self.workers:
      w.running = False
    self.dbthread.running = False
