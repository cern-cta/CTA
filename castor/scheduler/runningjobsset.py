#/******************************************************************************
# *                   runningjobset.py
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
# * runningjobset class of the low latency scheduling facility of the CASTOR project
# * this class is responsible for managing a local set of running jobs and check for their
# * termination when called on the poll method
# *
# * @author Castor Dev team, castor-dev@cern.ch
# *****************************************************************************/

import os, pwd
import threading
import subprocess
import time
import syslog

log = syslog.syslog

class PopenWrapper(subprocess.Popen):
  '''little wrapper around Popen allowing to create a Popen object from a process pid.
     note that this code is linux specific and that the Popen object will not be fully
     functionnal. But at least it poll method can be used'''
  def __init__(self, pid):
      '''constructor'''
      self.pid = pid

class RunningJobsSet:
  '''handles a list of running jobs and is able to poll them regularly and list the ones that ended'''

  def __init__(self, connections, fake=False):
    '''constructor'''
    # connection pool to be used for sending messages to schedulers
    self.connections = connections
    # do we run in fake mode ?
    self.fake = fake
    # standard jobs and disk2disk copy destinations
    self._jobs = set()
    # global lock for the 2 sets
    self._lock = threading.Lock()
    # list jobs already running on the node, left over from the last time we ran
    self.populate()

  def populate(self):
    '''populates the list of ongoing jobs from the system.
    note that this is linux specific code'''
    log(syslog.LOG_DEBUG, 'populating running scripts from system')
    # loop over all processes
    pids= [pid for pid in os.listdir('/proc') if pid.isdigit()]
    for pid in pids:
      cmdline = open(os.path.join('/proc', pid, 'cmdline'), 'rb').read()
      # find out the ones concerning us
      if cmdline.startswith('/usr/bin/stagerjob') or cmdline.startswith('/usr/bin/d2dtransfer'):
          # get all details we need
          args = cmdline.split()
          jobid = args[4]
          notifFileName = args[-1][7:] # drop the leading 'file://'
          process = PopenWrapper(pid)
          jobtype = 'standard'
          if args[0] == '/usr/bin/d2dtransfer': jobtype = 'd2ddest'
          arrivalTime = os.stat(os.path.join('/proc', pid, 'cmdline'))[-2]
          startTime = arrivalTime
          # create the entry in the list of running jobs
          runningJobs.add(jobid, 'unknown', cmdline, notifFileName, process, jobtype, arrivalTime, startTime)
          # log
          log('Found job ' + jobid + ' already running')

  def add(self, jobid, scheduler, job, notifyFileName, process, jobtype, arrivalTime, startTime=None):
    '''add a new running job to the list'''
    self._lock.acquire()
    try:
      if startTime == None:
        startTime = time.time()
      self._jobs.add((jobid, scheduler, job, notifyFileName, process, jobtype, arrivalTime, startTime))
    finally:
      self._lock.release()

  def nbJobs(self):
    '''returns number of running jobs'''
    return len(self._jobs)

  def poll(self):
    '''checks for finished jobs and clean them up'''
    sourcesToBeInformed = []
    self._lock.acquire()
    try:
      ended = []
      for jobid, scheduler, job, notifyFileName, process, jobtype, arrivalTime, runTime in self._jobs:
        # take care that fake d2dsource jobs have no process
        if process != None:
          if not self.fake:
            rc = process.poll()
          else:
            rc = True
          if rc != None:
            # remove the notification file
            os.remove(notifyFileName)
            # append to list of ended jobs
            ended.append(process)
            # in case of disk to disk copy, remember to inform source
            if jobtype == 'd2ddest':
              sourcesToBeInformed.append((scheduler, jobid))
      # cleanup ended jobs
      self._jobs = set(job for job in self._jobs if job[4] not in ended)
    finally:
      self._lock.release()
    for scheduler, jobid in sourcesToBeInformed:
      try:
        self.connections.d2dend(scheduler, jobid)
      except Exception, e:
        # log that we've failed
        log(syslog.LOG_ERR, "Informing " + scheduler + " that d2d transfer of " + jobid + " is over failed with error " + str(e))

  def d2dend(self, jobid):
    '''called when a disk to disk copy ends and we are the source of it'''
    self._lock.acquire()
    try:
      self._jobs = set(job for job in self._jobs if job[0] != jobid)
    finally:
      self._lock.release()

  def bjobs(self):
    '''lists running jobs in suitable format for bjobs'''
    res = []
    for jobid, scheduler, job, notifyFileName, process, jobtype, arrivalTime, runTime in self._jobs:
      if jobtype == 'standard':
        try:
          user = pwd.getpwuid(int(job[20]))[0]
        except KeyError:
          user = job[20]
      else:
        user = 'stage'
      res.append((jobid, scheduler, user, 'RUN', jobtype, arrivalTime, runTime))
    return res

  def listRunningD2dSources(self):
    '''lists running d2dsource jobs'''
    return [(jobid, job, arrivalTime) for jobid, scheduler, job, notifyFileName, process, jobtype, arrivalTime, runTime in self._jobs if jobtype == 'd2dsource']
