#/******************************************************************************
# *                   serverqueue.py
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
# * serverqueue class of the low latency scheduling facility of the CASTOR project
# * this class is responsible for managing a queue of jobs in the server memory.
# * This queue lists all jobs pending on the different diskservers, and started
# * by a given server
# *
# * @author Castor Dev team, castor-dev@cern.ch
# *****************************************************************************/

import threading
import syslog

log = syslog.syslog

class ServerQueue(dict):
  '''a dictionnary of queuing jobs, with the list of machines to which they were sent
  and a reverse lookup facility by machine'''

  def __init__(self, connections):
    '''constructor'''
    dict.__init__(self)
    self.connections = connections
    # a global lock for this queue
    self._lock = threading.Lock()
    # dictionnary containing the set of jobs running on each machine
    self._jobsLocations = {}
    # list of source d2d jobs running
    self._d2dsourcerunning = {}

  def put(self, jobid, arrivaltime, jobList, jobtype='standard'):
    '''Adds a new job. jobtype can be one of 'standard', 'd2dsource' and 'd2ddest' '''
    self._lock.acquire()
    try:
      for diskserver, job in jobList:
        # add job to the list of queuing jobs on the diskserver
        # note the extra argument, used only for d2ddest jobs and telling whether the source is ready
        # As it could be that the source has already started when the destinations are entered, we have
        # to infer this argument from the jobid, looking into the list of running sources
        if diskserver not in self:
          self[diskserver] = {}
        self[diskserver][jobid] = [job, arrivaltime, jobtype, jobid in self._d2dsourcerunning]
        # add the diskserver to the jobLocations list for this job (except for sources)
        if jobtype != 'd2dsource':
          if jobid not in self._jobsLocations:
            self._jobsLocations[jobid] = set()
          self._jobsLocations[jobid].add(diskserver)
    finally:
      self._lock.release()

  def remove(self, jobids):
    '''drops jobs from the queues'''
    jobsPerMachine = {}
    self._lock.acquire()
    try:
      # for each job
      for jobid in jobids:
        try:
          # get the list of machines potentially running the job
          machines = self._jobsLocations[jobid]
          # clean up _jobsLocations
          del self._jobsLocations[jobid]
          # for each machine, cleanup the server queue and note down where the job was sent
          for machine in machines:
            if machine not in jobsPerMachine: jobsPerMachine[machine] = []
            jobsPerMachine[machine].append(jobid)
            del self[machine][jobid]
          # if we have a source job already running, stop it
          if jobid in self._d2dsourcerunning: d2dend(jobid,lock=False)
        except KeyError:
          # we are not handling this job, fine, ignore it then
          pass
    finally:
      self._lock.release()
    # finally tell the machines to remove these jobs from their queues
    for machine in jobsPerMachine:
      connections.bkill(machine, tuple(jobsPerMachine[machine]))

  def jobStarting(self, diskserver, jobid, jobtype):
    '''Removes a job and gives back the list of other nodes where it was pending.
    Raises ValueError when not found'''
    if jobtype == 'd2dsource':
      self._lock.acquire()
      try:
        # the source job is starting, mark all destinations (and the source) ready
        try :
          for ds in self._jobsLocations[jobid]:
            self[ds][jobid] = self[ds][jobid][0:3]+[True]
        except KeyError:
          # no destination yet, no problem, the source was only too fast to start
          pass
        # remember where the source is running
        self._d2dsourcerunning[jobid] = diskserver
      finally:
        self._lock.release()
    else:
      self._lock.acquire()
      try:
        # check whether the job has already been started somewhere else
        if jobid not in self._jobsLocations:
          # in such a case, let the diskserver know by raising an exception
          log(syslog.LOG_DEBUG, diskserver + " : job " + jobid + " had already started. Cancel start.")
          raise ValueError
        # if a destination job wants to start, check whether the source is ready
        if jobtype == 'd2ddest' and not self[diskserver][jobid][3]:
          log(syslog.LOG_DEBUG, "Source is not ready yet for jobid " + jobid + ' and diskserver ' + diskserver)
          raise EnvironmentError
        # drop the job from all queues. Note that this desynchronizes the queues as seen
        # by the diskservers from the queues seen by the central manager. In case of a
        # restart of a diskserver manager, its queue will be magically "cleaned up"
        # However, the diskservers concerned will be notified.
        machines = self._jobsLocations[jobid]
        del self._jobsLocations[jobid]
        for machine in machines: del self[machine][jobid]
      finally:
        self._lock.release()

  def d2dend(self, jobid, lock=True):
    '''called when a d2d copy ends in order to inform the source'''
    if lock:
      self._lock.acquire()
    try:
      # get the source location
      diskserver = self._d2dsourcerunning[jobid]
      # remove d2dsource job from the queue
      del self[diskserver][jobid]
      # remove from list of d2dsources
      del self._d2dsourcerunning[jobid]
    finally:
      if lock: 
        self._lock.release()
    # inform the source diskserver
    try:
      self.connections.d2dend(diskserver, jobid)
    except Exception, e:
      # log that we've failed
      log(syslog.LOG_ERR, "Informing " + diskserver + " that d2d copy of " + jobid + " is over failed with error " + str(e))

  def putRunningD2dSource(self, diskserver, jobid, job, arrivaltime):
    '''Adds a new d2dsource job to the list of runnign ones'''
    self._lock.acquire()
    try:
      # add job to the list of running d2dsource jobs on the diskserver
      if diskserver not in self: self[diskserver] = {}
      self[diskserver][jobid] = [job, arrivaltime, 'd2dsource', 'True']
      self._d2dsourcerunning[jobid] = diskserver
    finally:
      self._lock.release()

  def listQueuingJobs(self, diskserver):
    '''This is called by the scheduler when rebuilding the list of queuing jobs for a given diskserver'''
    if diskserver in self:
      res = [tuple([jobid]+job[0:3]) for jobid, job in self[diskserver].items() if jobid not in self._d2dsourcerunning]
      return res
    else:
      return []

  def listRunningD2dSources(self, diskserver):
    '''This is called by the scheduler when rebuilding the list of running d2dsource jobs for a given diskserver'''
    res = []
    for jobid in [jobid for jobid in self._d2dsourcerunning if self._d2dsourcerunning[jobid] == diskserver]:
      job, arrivaltime, jobtype, ready = self[diskserver][jobid]
      res.append((jobid, job, arrivaltime))
    return res

  def exposed_jobsCanceled(self, machine, jobs):
    '''cancels jobs in the queues and informs the stager in case there is no
    remaining machines where it could run'''
    jobsKilled = []
    self._lock.acquire()
    try:
      # for each job
      for jobid, rc, msg in jobs:
        try:
          # cleanup the queue for the given job on the given machine
          del self._jobsLocations[jobid][machine]
          del self[machine][jobid]
          if not self._jobsLocations[jobid]:
            # no other candidate machine for this job. It has to be failed
            jobsKilled.append((jobid, rc, msg))
            # clean up _jobsLocations
            del self._jobsLocations[jobid]
            # if we have a source job already running, stop it
            if jobid in self._d2dsourcerunning: d2dend(jobid,lock=False)
        except KeyError, e:
          # we are not handling this job or it is not queued for the given machine
          # we can only log this oddity and ignore
          log(syslog.LOG_ERR, 'Unexpected KeyError exception caught in jobsCanceled : ' + str(e))
    finally:
      self._lock.release()
    # inform the stager of the jobs that were killed
    llsfd.jobsKilled(jobsKilled)

