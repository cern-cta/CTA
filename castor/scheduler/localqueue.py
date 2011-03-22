#/******************************************************************************
# *                   localqueue.py
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
# * localqueue class of the low latency scheduling facility of the CASTOR project
# * this class is responsible for managing a local queue of pending jobs for a given diskserver.
# *
# * @author Castor Dev team, castor-dev@cern.ch
# *****************************************************************************/

import pwd
import time
import Queue
import socket
import threading
import syslog

class LocalQueue(Queue.Queue):
  '''Class managing a queue of pending jobs.
  There are actually 2 queues, a dictionnary and a set involved :
    - the main queue is the object itself and holds all jobs that were not considered so far.
    - _pendingD2dDest is a list of disk2disk destinations jobs that have been considered but could not
      be started because the source was not ready. They will be retried regularly until the source
      becomes ready. They are stored together with the next time when to retry. The interval between
      retries is equal to half the time gone since the arrival of the job, with a maximum
      configured in castor.conf (DiskManager/MaxRetryInterval).
    - _priorityQueue is a queue of jobs to be started as soon as possible. These jobs are the disk2disk
      destination jobs that have been pending on the resdiness of the source and can now be started
    - finally, all jobs handled are indexed in a dictionnary called _queueingJobs handling detailed
      information
  '''

  def __init__(self, config):
    '''constructor'''
    Queue.Queue.__init__(self)
    # configuration
    self.config = config
    # dictionnary of jobs
    self._queuingJobs = {}
    # a global lock for the dictionnary
    self._lock = threading.Lock()
    # set of pending disk2disk destinations
    self._pendingD2dDest = []
    # set of jobs to start with highest priority
    self._priorityQueue = Queue.Queue()

  def put(self, scheduler, jobid, job, jobtype, arrivaltime):
    '''Put new job in the queue'''
    self._lock.acquire()
    try:
      # first keep note of the new job (index by subreqId)
      self._queuingJobs[jobid] = (scheduler, job, jobtype, arrivaltime)
      # then add it to the underlying queue
      Queue.Queue.put(self,jobid)
    finally:
      self._lock.release()

  def priorityPut(self, scheduler, jobid, job, jobtype, arrivaltime):
    '''Put new job in the priority queue'''
    self._lock.acquire()
    try:
      # first keep note of the new job (index by subreqId)
      self._queuingJobs[jobid] = (scheduler, job, jobtype, arrivaltime)
      # then add it to the underlying queue
      self._priorityQueue.put(jobid)
    finally:
      self._lock.release()

  def get(self):
    '''get a job from the queue'''
    found = False
    while not found:
      # try to get a priority job first
      try :
        jobid = self._priorityQueue.get(False)
      except Queue.Empty:
        # else get next job from the regular queue
        # timeout after 1s so that we can go back to the priority queue
        try:
          jobid = Queue.Queue.get(self, timeout=1)
        except Queue.Empty:
          continue
      self._lock.acquire()
      try:
        try:
          # remove it from the list of pending jobs
          scheduler, job, jobtype, arrivalTime = self._queuingJobs[jobid]
          del self._queuingJobs[jobid]
          found = True
        except KeyError:
          # job not found, meaning it was canceled
          # ignore and go to next one
          pass
      finally:
        self._lock.release()
    # return
    return scheduler, jobid, job, jobtype, arrivalTime

  def remove(self, jobids):
    '''remove jobs from the queue'''
    # Loop through the jobs to remove
    self._lock.acquire()
    try:
      for jobid in jobids:
        try:
          # we only remove from the _queuingJobs dictionnary
          # the jobs stay in the queue but will be ignored when poped
          del self._queuingJobs[jobid]
        except KeyError:
          # job not found, either not running here or already canceled
          pass
    finally:
      self._lock.release()

  def d2dDestReady(self, scheduler, jobid, job, arrivaltime, jobtype):
    '''called when a d2ddest job could not start as the source is not ready'''
    self._lock.acquire()
    try:
      # compute next time when we should try to start thsi job
      currentTime = time.time()
      timeToNextTry = (currentTime-arrivaltime)/2
      if timeToNextTry > self.config['DiskManager']['MaxRetryInterval']:
        timeToNextTry = self.config['DiskManager']['MaxRetryInterval']
      # and put the job into the list of pending ones
      self._queuingJobs[jobid] = (scheduler, job, jobtype, arrivaltime)
      self._pendingD2dDest.append((jobid, currentTime + timeToNextTry))
    finally:
      self._lock.release()

  def pollD2dDest(self):
    '''Checks which d2d destinations jobs waiting for sources should be retried'''
    self._lock.acquire()
    currentTime = time.time()
    toBeDeleted = []
    try:
      # loop over all pending jobids
      for i in range(len(self._pendingD2dDest)):
        jobid, timeOfNextTry = self._pendingD2dDest[i]
        if timeOfNextTry < currentTime:
          # put the jobid back into the priority queue in it's time to retry the job
          syslog.syslog("Retrying job " + jobid)
          self._priorityQueue.put(jobid)
          toBeDeleted.append(jobid)
      # cleanup list of pending jobids
      self._pendingD2dDest = [(jobid, nextTry) for jobid, nextTry in self._pendingD2dDest if jobid not in toBeDeleted]
    finally:
      self._lock.release()

  def checkForTimeoutJobsCancelation(self, canceledJobs):
    '''Checks which jobs need to be canceled because they are queuing for too long'''
    # get timeouts from configuration
    timeouts = dict([entry.split(':') for entry in configuration['JobManager']['PendingTimeouts'].split()])
    for svcclass, timeout in timeouts:
      try:
        timeouts[svcclass] = int(timeout)
      except ValueError:
        del timeouts[svcclass]
        syslog.syslog(syslog.LOG_ERR, "Invalid JobManager/PendingTimeouts option, ignoring entry " + svcclass + ':' + timeout)
    # get the disk to disk copy timeout
    d2dtimeout =  configuration.getValue('JobManager', 'DiskCopyPendingTimeout', None, int)
    # get current time and diskserver status
    currenttime = time.time()
    # loop over the jobs
    for jobid, (scheduler, job, jobtype, arrivaltime) in self._pendingD2dDest:
      # find the apropriate timeout
      if jobtype == 'standard':
        try:
          svcclass = job[-5]
          timeout = timeouts[svcclass]
        except KeyError:
          try :
            timeout = timeouts['all']
          except KeyError:
            # no timeout could be found, so we take it as infinite, meaning we do not cancel anything
            timeout = None
      else:
        timeout = d2dtimeout
      # if we found a timeout, check and cancel if needed
      if timeout != None and currenttime - arrivaltime > timeout:
          self.remove(jobid)
          if scheduler not in canceledJobs: canceledJobs[scheduler] = []
          canceledJobs[scheduler].append((jobid, -1, 'Timed out while queueing'))

  def checkForDisabledHardwareJobsCancelation(self, canceledJobs):
    '''Checks which jobs need to be canceled because hardware has been disabled'''
    # get the hardware status
    try :
      state = dict([entry.rstrip().split('=') for entry in open('/etc/castor/status').readlines()])
    except:
      # could not get the status, let's have it empty, meaning disabled
      state = {}
    # loop over the jobs
    for jobid, (scheduler, job, jobtype, arrivaltime) in self._pendingD2dDest:
      # get the concerned fileSystem
      filesystem = job[-1].split(':')[1]
      try:
        # find out whether the job should be canceled
        cancelJob = not ((state['DiskServerStatus'] == 'DISKSERVER_PRODUCTION' and
                        state[filesystem] == 'FILESYSTEM_PRODUCTION') or
                       (jobtype = 'd2dsource' and
                        state['DiskServerStatus'] in ('DISKSERVER_PRODUCTION', 'DISKSERVER_DRAINING') and
                        state[filesystem] in ('FILESYSTEM_PRODUCTION', 'FILESYSTEM_DRAINING')))
      except ValueError:
        # no state means disable
        cancelJob = True
      if cancelJob:
        self.remove(jobid)
        if scheduler not in canceledJobs: canceledJobs[scheduler] = []
        if jobtype == 'd2dsource':
          msg = "Job terminated, source filesystem for disk2disk copy is DISABLED"
        else: # standard, d2ddest
          msg = "Job terminated, all filesystems are DRAINING or DISABLED"
        canceledJobs[scheduler].append((jobid, -1, msg))
        continue

  def checkForJobsCancelation(self):
    '''Checks which jobs need to be canceled'''
    canceledJobs = {}
    # check whether jobs need to be canceled for timeouts
    jobQueue.checkForTimeoutJobsCancelation(canceledJobs)
    # All further processing requires resource killing to be active.
    if configuration['JobManager']['ResReqKill'].lower() != 'no':
      # check whether jobs need to be canceled when hardware gets disabled
      jobQueue.checkForDisabledHardwareJobsCancelation(canceledJobs)
    # Inform the schedulers of canceled jobs
    for scheduler, jobs in canceledJobs:
      connections.jobsCanceled(scheduler, socket.gethostname(), jobs)

  def nbJobs(self):
    '''returns number of queuing jobs'''
    return len(self._queuingJobs)

  def bjobs(self):
    '''lists pending jobs in suitable format for bjobs'''
    res = []
    for job in self._queuingJobs.items():
      scheduler, rawjob, jobtype, arrivalTime = job[1]
      if jobtype == 'standard':
        try:
          user = pwd.getpwuid(int(rawjob[20]))[0]
        except KeyError:
          user = rawjob[20]
      else:
        user = 'stage'
      res.append((job[0], scheduler, user, 'PEND', jobtype, arrivalTime, None))
    return res

  def jobset(self):
    '''Lists all pending and running jobs'''
    return set(self._queuingJobs.keys())

  def listQueuingJobs(self):
    '''lists pending jobs'''
    return [tuple([job[0]])+job[1][1:4] for job in self._queuingJobs.items()]
