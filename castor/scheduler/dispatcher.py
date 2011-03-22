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

log = syslog.syslog

class Dispatcher(threading.Thread):
  '''scheduling thread, responsible for connecting to the stager databse and scheduling jobs'''

  def __init__(self, connections, queuingJobs):
    '''constructor of this thread. Arguments are the connection pool and the job queue to use'''
    threading.Thread.__init__(self)
    self.running = True
    self.connections = connections
    self.queuingJobs = queuingJobs
    self.hostname = socket.gethostname()
    self.stagerConnection = None

  def dbConnection(self):
    '''returns a connection to the stager DB.
    The connection is cached and reconnections are handled'''
    if self.stagerConnection == None:
        self.stagerConnection = castor_tools.connectToStager()
    return self.stagerConnection

  def failJob(self, jobid, errno, errmsg):
    '''fails a given job in the database'''
    log('Failing job ' + jobid)
    try:
        stcur = self.dbConnection().cursor()
        stcur.execute("BEGIN jobFailed2(:1, :2, :3); END;", (jobid, errno, errmsg))
    except Exception, e:
        log(syslog.LOG_ERR, 'Exception caught while failing job ' + jobid + ' : ' + str(e))

  def scheduleD2d(self, reqId, srSubReqId, cfFileId, cfNsHost, reqDestDiskCopy,
                  reqSourceDiskCopy, reqSvcClass, reqCreationTime, sourceRfs, srRfs):
    '''Schedules a disk to disk copy on the source and destinations and handle issues '''
    # prepare command line
    basecmd = ["/usr/bin/d2dtransfer",
               "-r", str(reqId.getvalue()),
               "-s", str(srSubReqId.getvalue()),
               "-F", str(int(cfFileId.getvalue())),
               "-H", str(cfNsHost.getvalue()),
               "-D", str(int(reqDestDiskCopy.getvalue())),
               "-X", str(int(reqSourceDiskCopy.getvalue())),
               "-S", str(reqSvcClass.getvalue()),
               "-t", str(int(reqCreationTime.getvalue()))]
    # first schedule a job on the source node
    schedSourceCandidates = [candidate.split(':') for candidate in sourceRfs.getvalue().split('|')]
    jobid = srSubReqId.getvalue()
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
      log(LOG_ERR, "Scheduling d2d on " + diskserver + " (source) failed with error " + str(e))
      # we coudl not schedule the source so fail the job in the DB
      self.failJob(jobid, 1721, "Unable to schedule on source host")  # 1721 = ESTSCHEDERR
      # and remove it from the server queue
      self.queuingJobs.remove(jobid)
      return
    # now schedule on all potential destinations
    schedDestCandidates = [candidate.split(':') for candidate in srRfs.getvalue().split('|')]
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
        log(LOG_ERR, "Scheduling d2d on " + diskserver + " (destination) failed with error " + str(e))
    # we are over, check whether we could schedule the destination at all
    if not scheduleSucceeded:
      # we could not schedule anywhere for destination.... so fail the job in the DB
      self.failJob(jobid, 1721, "Unable to schedule on any destination host")  # 1721 = ESTSCHEDERR
      # and remove it from the server queue
      self.queuingJobs.remove(jobid)
   

  def scheduleJob(self, srRfs, clientIp, reqId, srSubReqId, cfFileId, cfNsHost,
                  srProtocol, srId, reqType, srOpenFlags, clientType, clientPort,
                  reqEuid, reqEgid, clientSecure, reqSvcClass, reqCreationTime):
    '''Schedules a disk to disk copy and handle issues '''
    # extract list of candidates where to schedule and log
    schedCandidates = [candidate.split(':') for candidate in srRfs.getvalue().split('|')]
    jobid = srSubReqId.getvalue()
    log(jobid + ' : ' + str(schedCandidates))
    # build a list of jobs to schedule for each machine
    arrivaltime =  time.time()
    jobList = []
    for diskserver, mountpoint in schedCandidates:
      ipAddress = int(clientIp.getvalue())
      cmd = ("/usr/bin/stagerjob",
             "-r", str(reqId.getvalue()),
             "-s", str(srSubReqId.getvalue()),
             "-F", str(int(cfFileId.getvalue())),
             "-H", str(cfNsHost.getvalue()),
             "-p", str(srProtocol.getvalue()),
             "-i", str(srId.getvalue()),
             "-T", str(int(reqType.getvalue())),
             "-m", str(srOpenFlags.getvalue()),
             # The client's identification as a string. This consists of the
             # client's object type, IP address and port
             "-C", str(int(clientType.getvalue())) + ":" + \
               str((ipAddress & 0xFF000000) >> 24) + "." + \
               str((ipAddress & 0x00FF0000) >> 16) + "." + \
               str((ipAddress & 0x0000FF00) >> 8)  + "." + \
               str((ipAddress & 0x000000FF)) + ":" + \
               str(int(clientPort.getvalue())),
             "-u", str(int(reqEuid.getvalue())),
             "-g", str(int(reqEgid.getvalue())),
             "-X", str(int(clientSecure.getvalue())),
             "-S", str(reqSvcClass.getvalue()),
             "-t", str(int(reqCreationTime.getvalue())),
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
        log(LOG_ERR, "Scheduling on " + diskserver + " failed with error " + str(e))
    # we are over, check whether we could schedule at all
    if not scheduleSucceeded:
      # we could not schedule anywhere.... so fail the job in the DB
      self.failJob(jobid, 1721, "Unable to schedule job") # 1721 = ESTSCHEDERR
      # and remove it from the server queue
      self.queuingJobs.remove(jobid)


  def run(self):
    '''main method, containing the infinite loop'''
    try:
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
        sourceRfs = stcur.var(cx_Oracle.STRING) # castor.DiskServerList_Cur
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
              self.scheduleD2d(reqId, srSubReqId, cfFileId, cfNsHost, reqDestDiskCopy,
                               reqSourceDiskCopy, reqSvcClass, reqCreationTime, sourceRfs, srRfs)
            else:
              self.scheduleJob(srRfs, clientIp, reqId, srSubReqId, cfFileId, cfNsHost, srProtocol,
                               srId, reqType, srOpenFlags, clientType, clientPort, reqEuid, reqEgid,
                               clientSecure, reqSvcClass, reqCreationTime)
            # commit the change of status to the stage DB
            self.dbConnection().commit()
      except Exception, e:
        if self.running:
          # if we are running, log and exit
          log(syslog.LOG_ALERT, 'Dispatcher thread exiting due to following exception : ' + str(e))
          sys.exit(1)
    finally:
      # try to clean up what we can
      try :
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
