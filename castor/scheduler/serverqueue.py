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

  def put(self, diskserver, jobid, job, arrivaltime, jobtype='standard'):
    '''Adds a new job. jobtype can be one of 'standard', 'd2dsource' and 'd2ddest' '''
    self._lock.acquire()
    # add job to the list of queuing jobs on the diskserver
    # note the extra argument, used only for d2ddest jobs and telling whether the source is ready
    if diskserver not in self:
      self[diskserver] = {}
    self[diskserver][jobid] = [job, arrivaltime, jobtype, False]
    # add the diskserver to the jobLocations list for this job (except for sources)
    if jobtype != 'd2dsource':
      if jobid not in self._jobsLocations:
        self._jobsLocations[jobid] = set()
      self._jobsLocations[jobid].add(diskserver)
    self._lock.release()

  def remove(self, diskserver, jobid, jobtype):
    '''Removes a job and gives back the list of other nodes where it was pending.
    Raises ValueError when not found'''
    self._lock.acquire()
    if jobtype == 'd2dsource':
      # the source job is starting, mark all destinations ready
      for ds in self._jobsLocations[jobid]:
        self[ds][jobid] = self[ds][jobid][0:3]+[True]
      # remember where the source is running
      self._d2dsourcerunning[jobid] = diskserver
      self._lock.release()
      # inform all destination machines that the source is ready
      for machine in self._jobsLocations[jobid]:
        if machine != diskserver:
          log(syslog.LOG_DEBUG, 'Calling d2dSourceReady on ' + machine + ' for jobid ' + jobid)
          self.connections.d2dSourceReady(machine, jobid)
    else:
      # check whether we are in a race condition where the job has already been
      # started somewhere else but the calling diskserver does not yet know about it.
      if jobid not in self._jobsLocations:
        # in such a case, let the diskserver know by raising an exception
        log(syslog.LOG_DEBUG, "Race condition detected : this job has already started : " + jobid)
        self._lock.release()
        raise ValueError
      # if a destination job wants to start, check whether the source is ready
      if jobtype == 'd2ddest' and not self[diskserver][jobid][3]:
        log(syslog.LOG_DEBUG, "Source is not ready yet for jobid " + jobid + ' and diskserver ' + diskserver)
        self._lock.release()
        raise EnvironmentError
      # drop the job from all queues. Note that this desynchronizes the queues as seen
      # by the diskservers from the queues seen by the central manager. In case of a
      # restart of a diskserver manager, its queue will be magically "cleaned up"
      # However, the diskservers concerned will be notified.
      machines = self._jobsLocations[jobid]
      del self._jobsLocations[jobid]
      for machine in machines: del self[machine][jobid]
      self._lock.release()
      # inform all other machines that this job has been handled
      for machine in machines:
        if machine != diskserver:
          self.connections.jobAlreadyStarted(machine, jobid)

  def d2dend(self, jobid):
    '''called when a d2d copy ends in order to inform the source'''
    self._lock.acquire()
    # get the source location
    diskserver = self._d2dsourcerunning[jobid]
    # cleanup
    del self._d2dsourcerunning[jobid]
    # inform the source diskserver
    self.connections.d2dend(diskserver, jobid)
    self._lock.release()

  def list(self, diskserver):
    if diskserver in self:
      return [tuple([job[0]]+job[1][0:3]) for job in self[diskserver].items()]
    else:
      return []
