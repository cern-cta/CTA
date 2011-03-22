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

import time
import Queue
import threading

class LocalQueue(Queue.Queue):
  '''Class managing a queue of pending jobs.
  There are actually 2 queues, a dictionnary and a set involved :
    - the main queue is the object itself and holds all jobs that were not considered so far.
    - _pendingD2dDest is a set of disk2disk destinations jobs that have been considered but could not
      be started because the source was not ready. They will stay in this set until the source
      becomes ready
    - _priorityQueue is a queue of jobs to be started as soon as possible. These jobs are the disk2disk
      destination jobs that have been pending on the resdiness of the source and can now be started
    - finally, all jobs handled are indexed in a dictionnary called _queueingJobs handling detailed
      information
  '''

  def __init__(self):
    '''constructor'''
    Queue.Queue.__init__(self)
    # dictionnary of jobs
    self._queuingJobs = {}
    # a global lock for the dictionnary
    self._lock = threading.Lock()
    # set of pending disk2disk destinations
    self._pendingD2dDest = set()
    # set of jobs to start with highest priority
    self._priorityQueue = Queue.Queue()

  def put(self, scheduler, jobid, job, jobtype, arrivaltime = None):
    '''Put new job in the queue'''
    self._lock.acquire()
    # first keep note of the new job (index by subreqId)
    if arrivaltime == None: arrivaltime = time.time()
    self._queuingJobs[jobid] = (scheduler, job, jobtype, arrivaltime)
    # then add it to the underlying queue
    Queue.Queue.put(self,jobid)
    self._lock.release()
    return arrivaltime

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
        # remove it from the list of pending jobs
        scheduler, job, jobtype, arrivalTime = self._queuingJobs[jobid]
        del self._queuingJobs[jobid]
        found = True
      except KeyError:
        # this job has already been canceled. Go to next one
        pass
      self._lock.release()
    # return
    return scheduler, jobid, job, jobtype, arrivalTime

  def cancel(self, jobid):
    '''cancels a given queuing job so that it will never be started'''
    self._lock.acquire()
    # only remove it from the _queuingJobs dictionnary as we cannot
    # remove it from the queue. When it will get picked from the
    # queue, it will be ignored
    try:
      del self._queuingJobs[jobid]
    except KeyError:
      # already gone ? Fine with us
      pass
    self._lock.release()

  def d2dSourceReady(self, jobid):
    '''Called when a disk 2 disk copy source informs us that it's ready
    we can thus unblock the corresponding job'''
    # ignore if nothing is pending for this source
    self._lock.acquire()
    if jobid in self._pendingD2dDest:
      self._pendingD2dDest.remove(jobid)
      # push job to priority queue
      self._priorityQueue.put(jobid)
    self._lock.release()

  def d2dDestReady(self, scheduler, jobid, job, arrivaltime, jobtype):
    '''called when a d2ddest job could not start as the source is not ready'''
    self._lock.acquire()
    # we put the job on a pending list
    self._queuingJobs[jobid] = (scheduler, job, jobtype, arrivaltime)
    self._pendingD2dDest.add(jobid)
    self._lock.release()

  def bjobs(self):
    '''lists pending jobs in suitable format for bjobs'''
    return [(job[0], job[1][0], job[1][1][20], 'PEND', job[1][2], job[1][3], None) for job in self._queuingJobs.items()]

  def list(self):
    '''lists pending jobs'''
    return [tuple([job[0]])+job[1][1:4] for job in self._queuingJobs.items()]
