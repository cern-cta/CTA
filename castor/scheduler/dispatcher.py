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

'''dispatcher module of the transfer manager daemon.
Handles the polling from the stager DB for new request
and their dispatching to the proper diskservers'''

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
        func, args = self.workqueue.get(True)
        # func may be None in case we wanted to exit the blocking get in order to close the service
        if func:
          func(*args)
      except Queue.Empty:
        # we've timed out, let's just retry. We only use the timeout so that this
        # thread can stop even if there is nothing in the queue
        pass
      except Exception, e:
        # "Caught exception in Worker thread" message
        dlf.writeerr(msgs.WORKEREXCEPTION, Type=str(e.__class__), Message=str(e))

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
        failures = []
        # check whether there is something to do
        try:
          # we go out every second in case the thread has ended in the meantime
          transferid, fileid, errcode, errmsg, reqid = self.workqueue.get(True)
          # transferid may be None in case we wanted to exit the blocking get in order to close the service
          if transferid:
            failures.append((transferid, fileid, errcode, errmsg, reqid))
          else:
            continue
        except Queue.Empty:
          continue
        # empty the queue so that we go only once to the DB
        try:
          while True:
            transferid, fileid, errcode, errmsg, reqid = self.workqueue.get(False)
            failures.append((transferid, fileid, errcode, errmsg, reqid))
        except Queue.Empty:
          # we are over, the queue is empty
          pass
        # Now call the DB for failures
        data = zip(*failures)
        transferids = data[0]
        errcodes = data[2]
        errmsgs = data[3]
        try:
          stcur = self.dbConnection().cursor()
          try:
            stcur.execute("BEGIN transferFailedLockedFile(:1, :2, :3); END;", [list(transferids), list(errcodes), list(errmsgs)])
            for transferid, fileid, errcode, errmsg, reqid in failures:
              # 'Failed transfer' message
              dlf.writenotice(msgs.FAILEDTRANSFER, subreqid=transferid, reqid=reqid, fileid=fileid, ErrorCode=errcode, ErrorMessage=errmsg)
          finally:
            stcur.close()
        except Exception, e:
          for transferid, fileid, errcode, errmsg, reqid in failures:
            # 'Exception caught while failing transfer' message
            dlf.writeerr(msgs.FAILINGTRANSFEREXCEPTION, subreqid=transferid, reqid=reqid, fileid=fileid, Type=str(e.__class__), Message=str(e))
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
    self.workToDispatch = Queue.Queue(2*nbWorkers)
    # a queue of updates to be done in the DB
    self.updateDBQueue = Queue.Queue()
    # a thread pool of Schedulers
    self.workers = []
    for i in range(nbWorkers):          # pylint: disable=W0612
      t = Worker(self.workToDispatch)
      t.setName('Worker')
      self.workers.append(t)
    # a DBUpdater thread
    self.dbthread = DBUpdater(self.updateDBQueue)
    self.dbthread.setName('DBUpdater')

  def join(self, timeout=None):
    # put None values to the worker queue so that workers go out of their blocking call to get
    for i_unused in range(len(self.workers)):
      self.workToDispatch.put((None, None))
    # join the worker threads
    for w in self.workers:
      w.join(timeout)
    # put None values to the updateDBQueue so that dbthread goes out of its blocking call to get
    self.updateDBQueue.put((None, None, None, None, None))
    # join the db thread
    self.dbthread.join(timeout)
    # join the master thread
    threading.Thread.join(self, timeout)

  def dbConnection(self):
    '''returns a connection to the stager DB.
    The connection is cached and reconnections are handled'''
    if self.stagerConnection == None:
      self.stagerConnection = castor_tools.connectToStager()
      self.stagerConnection.autocommit = True
    return self.stagerConnection

  def _schedule(self, transferid, reqId, fileid, arrivaltime, transferlist, transfertype, errorCode, errorMessage):
    '''schedules a given transfer on the given set of machine and handles errors.
    Returns whether the scheduling was successful'''
    # put transfers in the queue of pending transfers
    # Note that we have to do this before even attempting to schedule the
    # transfers for real, as a job may start very fast after the scheduling
    # on the first machine, and it expects the queue to be up to date.
    # the consequence is that we may have to amend the list in case we
    # could not schedule everywhere.
    self.queueingTransfers.put(transferid, arrivaltime, transferlist, transfertype)
    # send the transfers to the appropriate diskservers
    # not that we will retry up to 3 times if we do not manage to schedule anywhere
    # we then give up
    nbRetries = 0
    scheduleHosts = []
    while not scheduleHosts and nbRetries < 3:
      nbRetries = nbRetries + 1
      for diskserver, cmd in transferlist:
        try:
          self.connections.scheduleTransfer(diskserver, self.hostname, transferid, cmd, arrivaltime, transfertype)
          scheduleHosts.append(diskserver)
        except Exception, e:
          # 'Failed to schedule xxx' message
          dlf.writenotice(errorCode, subreqid=transferid, reqid=reqId, fileid=fileid,
                          DiskServer=diskserver, Type=str(e.__class__), Message=str(e))
    # we are over, check whether we could schedule at all
    if not scheduleHosts:
      # we could not schedule anywhere.... so fail the transfer in the DB
      self.updateDBQueue.put((transferid, fileid, 1721, errorMessage, reqId)) # 1721 = ESTSCHEDERR
      # and remove it from the server queue
      self.queueingTransfers.remove([transferid])
      return False
    else:
      # see where we could not schedule
      failedHosts = set([diskserver for diskserver, cmd in transferlist]) - set(scheduleHosts)
      # We could scheduler at least on one host
      if failedHosts:
        # but we have failed on others : inform server queue of the failures
        if self.queueingTransfers.transfersStartingFailed(transferid, reqId, failedHosts):
          # It seems that finally we have not been able to schedule anywhere....
          # This may seem in contradiction with the last but one comment but it actually
          # only means that the machines to which we've managed to schedule have already
          # tried to start the job in the mean time and have all failed, e.g because
          # they have no space.
          # So in practice, we will not start the job and we have to inform the DB
          self.updateDBQueue.put((transferid, fileid, 1721, errorMessage, reqId)) # 1721 = ESTSCHEDERR
          return False
      # 'Marking transfer as scheduled' message
      dlf.write(msgs.TRANSFERSCHEDULED, subreqid=transferid, reqid=reqId, fileid=fileid, type=transfertype, hosts=str(scheduleHosts))
      return True

  def _scheduleD2d(self, reqId, srSubReqId, cfFileId, cfNsHost, reqDestDiskCopy,
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
    dlf.writedebug(msgs.SCHEDD2DSRC, subreqid=transferid, reqid=reqId, fileid=fileid, DiskServer=str(schedSourceCandidates[0]))
    diskserver, mountpoint = schedSourceCandidates[0]
    cmd = tuple(basecmd + ["-R", diskserver+':'+mountpoint])
    arrivaltime =  time.time()
    # effectively schedule the transfer onto its source
    if not self._schedule(transferid, reqId, fileid, arrivaltime, [(diskserver, cmd)], 'd2dsrc',
                      msgs.SCHEDD2DSRCFAILED, 'Unable to schedule on source host'):
      return
    # now schedule on all potential destinations
    schedDestCandidates = [candidate.split(':') for candidate in srRfs.split('|')]
    # 'Scheduling d2d destination' message
    dlf.writedebug(msgs.SCHEDD2DDEST, subreqid=transferid, reqid=reqId, fileid=fileid, DiskServers=str(schedDestCandidates))
    # build the list of hosts and transfers to launch
    transferList = []
    for diskserver, mountpoint in schedDestCandidates:
      cmd = tuple(basecmd + ["-R", diskserver+':'+mountpoint])
      transferList.append((diskserver, cmd))
    # effectively schedule the transfer onto its destination
    self._schedule(transferid, reqId, fileid, arrivaltime, transferList, 'd2ddest',
                   msgs.SCHEDD2DDESTFAILED, 'Failed to schedule d2d destination')

  def _scheduleStandard(self, srRfs, clientIp, reqId, srSubReqId, cfFileId, cfNsHost,
                        srProtocol, srId, reqType, srOpenFlags, clientType, clientPort,
                        reqEuid, reqEgid, clientSecure, reqSvcClass, reqCreationTime):
    '''Schedules a disk to disk copy and handle issues '''
    # extract list of candidates where to schedule and log
    schedCandidates = [candidate.split(':') for candidate in srRfs.split('|')]
    transferid = srSubReqId
    fileid = (str(cfNsHost), int(cfFileId))
    # we explicitely forbid transfers done as root user
    if int(reqEuid) == 0:
      self.updateDBQueue.put((transferid, fileid, 1721, "Transfer refused as requester is root", reqId)) # 1721 = ESTSCHEDERR
      return
    # 'Scheduling standard transfer' message
    dlf.writedebug(msgs.SCHEDTRANSFER, subreqid=transferid, reqid=reqId, fileid=fileid, DiskServers=str(schedCandidates))
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
    # effectively schedule the transfer
    self._schedule(transferid, reqId, fileid, arrivaltime, transferList, 'standard',
                   msgs.SCHEDTRANSFERFAILED, 'Unable to schedule transfer')

  def run(self):
    '''main method, containing the infinite loop'''
    configuration = castor_tools.castorConf()
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
          stTransferToSchedule = 'BEGIN transferToSchedule(:srId, :srSubReqId , :srProtocol, :srXsize, :srRfs, :reqId, :cfFileId, :cfNsHost, :reqSvcClass, :reqType, :reqEuid, :reqEgid, :reqUsername, :srOpenFlags, :clientIp, :clientPort, :clientVersion, :clientType, :reqSourceDiskCopy, :reqDestDiskCopy, :clientSecure, :reqSourceSvcClass, :reqCreationTime, :reqDefaultFileSize, :sourceRfs); END;' # pylint: disable=C0301
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
                self.workToDispatch.put((self._scheduleD2d,
                                         (reqId.getvalue(), srSubReqId.getvalue(), cfFileId.getvalue(),
                                          cfNsHost.getvalue(), reqDestDiskCopy.getvalue(),
                                          reqSourceDiskCopy.getvalue(), reqSvcClass.getvalue(),
                                          reqCreationTime.getvalue(), sourceRfs.getvalue(), srRfs.getvalue())))
              else:
                self.workToDispatch.put((self._scheduleStandard,
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
        dlf.writeerr(msgs.DISPATCHEXCEPTION, Type=str(e.__class__), Message=str(e))
        # check whether we should reconnect to DB, and do so if needed
        self.dbConnection().checkForReconnection(e)
        # then sleep a bit to not loop to fast on the error
        time.sleep(1)

  def stop(self):
    '''Stops processing of this thread'''
    # first stop the acitivity of taking new jobs from the DB
    self.running = False
    # now wait that the internal queue is empty
    # note that this should be implemented with Queue.join and Queue.task_done if we would have python 2.5
    while not self.workToDispatch.empty():
      time.sleep(0.1)
    # then stop the workers
    for w in self.workers:
      w.running = False
    # and finally stop the DB thread
    self.dbthread.running = False
