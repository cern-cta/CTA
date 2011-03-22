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
# * dispatcher class of the low latency scheduling facility of the CASTOR project
# * this class is responsible for polling the DB for jobs to dispatch and
# * effectively dispatch them on the relevant diskservers.
# *
# * @author Castor Dev team, castor-dev@cern.ch
# *****************************************************************************/

import sys
import time
import threading
import cx_Oracle
import syslog
import socket
import castor_tools
import Queue

log = syslog.syslog

class Worker(threading.Thread):
  '''Worker thread, responsible for scheduling effectively the jobs on the diskservers'''

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
        log(syslog.LOG_ERR, 'Exception caught in Worker thread (' + str(e.__class__) + ') : ' + str(e))

class DBUpdater(threading.Thread):
  '''Worker thread, responsible for updating DB asynchronously and in bulk after the job scheduling'''

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
    while self.running:
      # get something from the queue and then empty the queue and list all the updates to be done in one bulk
      successes = []
      failures = []
      # check whether there is something to do
      try:
        # we go out every second in case the thread has ended in the meantime
        jobid, errcode, errmsg = self.workqueue.get(True, 1)
        if errcode != None:
          failures.append((jobid, errcode, errmsg))
        else:
          successes.append(jobid)
      except Queue.Empty:
        continue
      # empty the queue so that we go only once to the DB
      try:
        while True:
          jobid, errcode, errmsg = self.workqueue.get(False)
          if errcode != None:
            failures.append((jobid, errcode, errmsg))
          else:
            successes.append(jobid)
      except Queue.Empty:
        # we are over, the queue is empty
        pass
      # Now call the DB for failures
      if failures:
        failures = zip(*failures)
        log('Failing jobs ' + ', '.join(failures[0]))
        try:
          stcur = self.dbConnection().cursor()
          stcur.execute("BEGIN jobFailedLockedFile(:1, :2, :3); END;", failures)
        except Exception, e:
          log(syslog.LOG_ERR, 'Exception caught while failing jobs ' + ', '.join(failures[0]) + ' (' + str(e.__class__) + ') : ' + str(e))
      # And call the DB for successes
      if successes:
        log('Marking jobs scheduled : ' + ', '.join(successes))
        try:
          stcur = self.dbConnection().cursor()
          stcur.execute("BEGIN jobScheduled(:jobids); END;", [successes])
        except Exception, e:
          log(syslog.LOG_ERR, 'Exception caught while marking jobs ' + ', '.join(successes) + ' as scheduled (' + str(e.__class__) + ') : ' + str(e))


class Dispatcher(threading.Thread):
  '''scheduling thread, responsible for connecting to the stager database and scheduling jobs'''

  def __init__(self, connections, queuingJobs, configuration, nbWorkers=5):
    '''constructor of this thread. Arguments are the connection pool and the job queue to use'''
    threading.Thread.__init__(self)
    # whether we should continue running
    self.running = True
    # a pool of connections to the diskservers
    self.connections = connections
    # the list of queueing jobs
    self.queuingJobs = queuingJobs
    # the configuration to use
    self.config = configuration
    # our own name
    self.hostname = socket.gethostname()
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

  def join(self):
    # join first the other threads
    for w in self.workers: w.join()
    self.dbthread.join()
    # then call join on master one
    threading.Thread.join(self)

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
    # first schedule a job on the source node
    schedSourceCandidates = [candidate.split(':') for candidate in sourceRfs.split('|')]
    jobid = srSubReqId
    log(jobid + '(d2d source) : ' + str(schedSourceCandidates[0]))
    diskserver, mountpoint = schedSourceCandidates[0]
    cmd = tuple(basecmd + ["-R", diskserver+':'+mountpoint])
    arrivaltime =  time.time()
    try:
      # register the job in the local list of pending jobs
      self.queuingJobs.put(jobid, arrivaltime, [(diskserver, cmd)], 'd2dsource')
      # send the job to the appropriate diskserver
      self.connections.scheduleJob(diskserver, self.hostname, jobid, cmd, arrivaltime, 'd2dsource')
    except Exception, e:
      # log that we've failed
      log(syslog.LOG_ERR, "Scheduling d2d on " + diskserver + " (source) failed with error " + str(e))
      # we coudl not schedule the source so fail the job in the DB
      self.updateDBQueue.put((jobId, 1721, "Unable to schedule on source host"))  # 1721 = ESTSCHEDERR
      # and remove it from the server queue
      self.queuingJobs.remove(jobid)
      return
    # now schedule on all potential destinations
    schedDestCandidates = [candidate.split(':') for candidate in srRfs.split('|')]
    log(jobid + '(d2d destinations) : ' + str(schedDestCandidates))
    # build the list of hosts and jobs to launch
    jobList = []
    for diskserver, mountpoint in schedDestCandidates:
      cmd = tuple(basecmd + ["-R", diskserver+':'+mountpoint])
      jobList.append((diskserver, cmd))
    # put jobs in the queue of pending jobs
    self.queuingJobs.put(jobid, arrivaltime, jobList, 'd2ddest')
    # send the jobs to the appropriate diskservers
    scheduleSucceeded = False
    for diskserver, cmd in jobList:
      try:
        self.connections.scheduleJob(diskserver, self.hostname, jobid, cmd, arrivaltime, 'd2ddest')
        scheduleSucceeded = True
      except Exception, e:
        # log that we've failed
        log(syslog.LOG_ERR, "Scheduling d2d on " + diskserver + " (destination) failed with error " + str(e))
    # we are over, check whether we could schedule the destination at all
    if not scheduleSucceeded:
      # we could not schedule anywhere for destination.... so fail the job in the DB
      self.updateDBQueue.put((jobid, 1721, "Unable to schedule on any destination host"))  # 1721 = ESTSCHEDERR
      # and remove it from the server queue
      self.queuingJobs.remove(jobid)
    else:
      self.updateDBQueue.put((jobid, None, None))


  def scheduleJob(self, srRfs, clientIp, reqId, srSubReqId, cfFileId, cfNsHost,
                  srProtocol, srId, reqType, srOpenFlags, clientType, clientPort,
                  reqEuid, reqEgid, clientSecure, reqSvcClass, reqCreationTime):
    '''Schedules a disk to disk copy and handle issues '''
    # extract list of candidates where to schedule and log
    schedCandidates = [candidate.split(':') for candidate in srRfs.split('|')]
    jobid = srSubReqId
    log(jobid + ' : ' + str(schedCandidates))
    # build a list of jobs to schedule for each machine
    arrivaltime =  time.time()
    jobList = []
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
      jobList.append((diskserver, cmd))
    # put jobs in the queue of pending jobs
    self.queuingJobs.put(jobid, arrivaltime, jobList)
    # send the jobs to the appropriate diskservers
    scheduleSucceeded = False
    for diskserver, cmd in jobList:
      try:
        self.connections.scheduleJob(diskserver, self.hostname, jobid, cmd, arrivaltime)
        scheduleSucceeded = True
      except Exception, e:
        # log that we've failed
        log(syslog.LOG_ERR, "Scheduling on " + diskserver + " failed with error " + str(e))
    # we are over, check whether we could schedule at all
    if not scheduleSucceeded:
      # we could not schedule anywhere.... so fail the job in the DB
      self.updateDBQueue.put((jobid, 1721, "Unable to schedule job")) # 1721 = ESTSCHEDERR
      # and remove it from the server queue
      self.queuingJobs.remove(jobid)
    else:
      self.updateDBQueue.put((jobid, None, None))

  def run(self):
    '''main method, containing the infinite loop'''
    try:
      while self.running:
        try:
          # setup an oracle connection and register our interest for 'jobsReadyToSchedule' alerts
          stcur = self.dbConnection().cursor()
          stcur.execute("BEGIN DBMS_ALERT.REGISTER('jobsReadyToSchedule'); END;");
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
          stJobToSchedule = 'BEGIN jobToSchedule2(:srId, :srSubReqId , :srProtocol, :srXsize, :srRfs, :reqId, :cfFileId, :cfNsHost, :reqSvcClass, :reqType, :reqEuid, :reqEgid, :reqUsername, :srOpenFlags, :clientIp, :clientPort, :clientVersion, :clientType, :reqSourceDiskCopy, :reqDestDiskCopy, :clientSecure, :reqSourceSvcClass, :reqCreationTime, :reqDefaultFileSize, :sourceRfs); END;'
          # infinite loop over the polling of the DB
          while self.running:
              # see whether there is something to do
              # not that this will hang until something comes or the internal timeout is reached
              stcur.execute(stJobToSchedule, (srId, srSubReqId, srProtocol, srXsize, srRfs, reqId, cfFileId,
                                              cfNsHost, reqSvcClass, reqType, reqEuid, reqEgid, reqUsername,
                                              srOpenFlags, clientIp, clientPort, clientVersion, clientType,
                                              reqSourceDiskCopy, reqDestDiskCopy, clientSecure, reqSourceSvcClass,
                                              reqCreationTime, reqDefaultFileSize, sourceRfs));
              # in case of timeout, we may have nothing to do
              if srId.getvalue() != None:
                log(syslog.LOG_DEBUG, 'jobToSchedule returned srId ' + str(int(srId.getvalue())))
                # standard jobs and disk to disk copies are not handled the same way
                # but in both cases, errors are handled internally and no exception
                # others than the ones implying the end of the processing
                if int(reqType.getvalue() == 133): # OBJ_StageDiskCopyReplicaRequest
                  self.workToDispatch.put((self.scheduleD2d,
                                           (reqId.getvalue(), srSubReqId.getvalue(), cfFileId.getvalue(),
                                            cfNsHost.getvalue(), reqDestDiskCopy.getvalue(),
                                            reqSourceDiskCopy.getvalue(), reqSvcClass.getvalue(),
                                            reqCreationTime.getvalue(), sourceRfs.getvalue(), srRfs.getvalue())))
                else:
                  self.workToDispatch.put((self.scheduleJob,
                                           (srRfs.getvalue(), clientIp.getvalue(), reqId.getvalue(),
                                            srSubReqId.getvalue(), cfFileId.getvalue(), cfNsHost.getvalue(),
                                            srProtocol.getvalue(), srId.getvalue(), reqType.getvalue(),
                                            srOpenFlags.getvalue(), clientType.getvalue(), clientPort.getvalue(),
                                            reqEuid.getvalue(), reqEgid.getvalue(), clientSecure.getvalue(),
                                            reqSvcClass.getvalue(), reqCreationTime.getvalue())))
        except Exception, e:
          # log error
          log(syslog.LOG_ERR, 'Caught exception in Dispatcher thread (' + str(e.__class__) + ') : ' + str(e))
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
        stcur.execute("BEGIN DBMS_ALERT.REMOVE('jobsReadyToSchedule'); END;");
      except Exception:
        pass
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
