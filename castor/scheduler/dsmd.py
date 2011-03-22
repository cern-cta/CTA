#!/usr/bin/python
#/******************************************************************************
# *                   dsmd.py
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
# * the disk server manager daemon of CASTOR
# *
# * @author Castor Dev team, castor-dev@cern.ch
# *****************************************************************************/

import os, sys
import getopt
import time
import rpyc
import subprocess
import socket
import tempfile
import threading
import Queue
import connectionpool
import castor_tools
import syslog
import daemon

# usage function
def usage(exitCode):
  print 'Usage : ' + sys.argv[0] + ' [-h|--help] [-v|--verbose]'
  sys.exit(exitCode)

# first parse the options
verbose = False
fake = False
try:
    options, args = getopt.getopt(sys.argv[1:], 'hv', ['help', 'verbose', 'fakemode'])
except Exception, e:
    print e
    usage(1)
for f, v in options:
    if f == '-h' or f == '--help':
        usage(0)
    elif f == '-v' or f == '--verbose':
        verbose = True
    elif f == '--fakemode':
        fake = True
    else:
        print "unknown option : " + f
        usage(1)

# If any arg, complain and stop
if len(args) != 0:
    print "Unknown arguments : " + ' '.join(args) + "\n"
    usage(1)


class DiskServerManagerService(rpyc.Service):
    '''implementation of the DiskServer Manager service.
    This service handles all the calls from the central scheduling, from
    job starting to monitoring'''

    def exposed_scheduleJob(self, scheduler, jobid, job, jobtype='standard'):
        '''Called when a new job needs to be scheduled.
        Queues the new job.'''
        log(syslog.LOG_DEBUG, 'scheduleJob called from ' + scheduler + ' for jobid ' + jobid + ' (' + jobtype + ')')
        return jobQueue.put(scheduler, jobid, job, jobtype)

    def exposed_nbJobs(self):
        '''Called when bhosts or bqueues monitoring is needed.
        Returns the number of slots. running jobs and pending jobs on the diskserver'''
        log(syslog.LOG_DEBUG, 'nbJobs called')
        return (int(configuration['JobManager']['MaxNbStreams']), runningJobs.nbJobs(), jobQueue.qsize())

    def exposed_badmin(self):
        '''Called when the diskserver should be reconfigured, e.g. on a badmin call'''
        log(syslog.LOG_DEBUG, 'badmin called')
        configuration.refresh()

    def exposed_bjobs(self):
        '''Called when bjobs monitoring is needed.
        Lists all jobs running or pending on this host'''
        log(syslog.LOG_DEBUG, 'bjobs called')
        return tuple(runningJobs.bjobs() + jobQueue.bjobs())

    def exposed_jobAlreadyStarted(self, jobid):
        '''Called when a job has started on another diskserver and can be canceled on this one'''
        log('Canceled start of ' + jobid + '(already started on another host)')
        jobQueue.cancel(jobid)

    def exposed_getQueuingJobs(self, scheduler):
        '''returns the list of jobs in the queue'''
        log(syslog.LOG_DEBUG, 'getQueuingJobs called from ' + scheduler)
        return tuple(jobQueue.list())

    def exposed_d2dSourceReady(self, jobid):
        '''called when a d2d source is ready to deliver data'''
        log(syslog.LOG_DEBUG, 'd2dSourceReady called for jobid ' + jobid)
        jobQueue.d2dSourceReady(jobid)

    def exposed_d2dend(self, jobid):
        '''called when a d2d copy is over and we are the source'''
        log(syslog.LOG_DEBUG, 'd2dend called for jobid ' + jobid)
        runningJobs.d2dend(jobid)

class ActivityControl(threading.Thread):
    '''activity control thread.
    This thread is responsible for starting new jobs when free slots are available'''

    def __init__(self):
        '''constructor'''
        super(ActivityControl,self).__init__()
        self._alive = True

    def run(self):
        '''main method, containing the infinite loop'''
        while True:
            # check number of running jobs against limit
            if runningJobs.nbJobs() < int(configuration['JobManager']['MaxNbStreams']):
                # get a new job from the jobQueue
                scheduler, jobid, job, jobtype, arrivalTime = jobQueue.get()
                try:
                    # notifies the central scheduler that we want to start this job
                    # this may raise a ValueError exception if it already started
                    # somewhere else
                    connections.jobStarting(scheduler, socket.gethostname(), jobid, jobtype)
                    # log job start
                    log('Starting ' + jobid + ' (' + jobtype + ')')
                    # in case of d2dsource, do not actually start for real, only take note
                    if jobtype == 'd2dsource':
                        runningJobs.d2dstart(jobid, scheduler, job, jobtype, arrivalTime)
                    else:
                        # create a local file to hold the former LSF scheduler info
                        notifFileHandle, notifFileName = tempfile.mkstemp()
                        notifFile = os.fdopen(notifFileHandle, 'w')
                        notifFile.write(job[-1])
                        notifFile.close()
                        os.chmod(notifFileName, 0666)
                        # build the stagerJob command line and execute it
                        cmd = list(job[:-1])
                        cmd.append('file://'+notifFileName)
                        # start the job
                        if not fake:
                          process = subprocess.Popen(cmd)
                        else:
                          process = 0
                        runningJobs.add(jobid, scheduler, job, notifFileName, process, jobtype, arrivalTime)
                except ValueError:
                    log('Canceled start of ' + jobid + '(already started on another host)')
                    # the job has already started somewhere else, so give up
                    pass
                except EnvironmentError:
                    # we have tried to start a disk to disk copy and the source is not yet ready
                    # we will put it in a priority queue
                    jobQueue.d2dDestReady(scheduler, jobid, job, arrivalTime, jobtype)
            else:
                time.sleep(.05)

    def stop(self):
        '''stops processing in this thread'''
        self._alive = False


class RunningJobs:
    '''handles a list of running jobs and is able to poll them regularly and list the ones that ended'''
    _jobs = []
    _d2dsourcejobs = set()
    _lock = threading.Lock()

    def add(self, jobid, scheduler, job, notifyFileName, process, jobtype, arrivalTime):
        '''add a new running job to the list'''
        self._lock.acquire()
        self._jobs.append((jobid, scheduler, job, notifyFileName, process, jobtype, arrivalTime, time.time()))
        self._lock.release()

    def nbJobs(self):
        '''returns number of running jobs'''
        return len(self._jobs)+len(self._d2dsourcejobs)

    def poll(self):
        '''checks for finished jobs and clean them up'''
        self._lock.acquire()
        ended = []
        for jobid, scheduler, job, notifyFileName, process, jobtype, arrivalTime, runTime in self._jobs:
            # take care that fake d2dsource jobs have no process
            if process != None:
                if not fake:
                  rc = process.poll()
                else:
                  rc = True
                if rc != None:
                    # remove the notification file
                    os.remove(notifyFileName)
                    # append to list of ended jobs
                    ended.append(process)
        # cleanup ended jobs
        self._jobs = [job for job in self._jobs if job[4] not in ended]
        self._lock.release()

    def d2dstart(self, jobid, scheduler, job, jobtype, arrivalTime):
        '''called when a disk to disk copy starts and we are the source of it'''
        self._lock.acquire()
        self._jobs.append((jobid, scheduler, job, None, None, jobtype, arrivalTime, time.time()))
        self._d2dsourcejobs.add((jobid))
        self._lock.release()

    def d2dend(self, jobid):
        '''called when a disk to disk copy ends and we are the source of it'''
        try:
            self._lock.acquire()
            self._d2dsourcejobs.remove(jobid)
            self._jobs = [job for job in self._jobs if job[0] != jobid]
        except KeyError:
            # we've probably restarted and forgot about it, let's give up
            pass
        self._lock.release()

    def bjobs(self):
        '''lists running jobs in suitable format for bjobs'''
        res = []
        for jobid, scheduler, job, notifyFileName, process, jobtype, arrivalTime, runTime in self._jobs:
            res.append((jobid, scheduler, job[20], 'RUN', jobtype, arrivalTime, runTime))
        return res

class JobCompletionControl(threading.Thread):
    '''Core of the job completion control thread. This thread is responsible for detecting
    terminated jobs and cleaning up the list of running jobs accordingly'''

    def __init__(self):
        '''constructor'''
        super(JobCompletionControl,self).__init__()
        self._alive = True

    def run(self):
        '''main method, containing the infinite loop'''
        while self._alive:
            runningJobs.poll()
            time.sleep(1)

    def stop(self):
        '''stops processing in this thread'''
        self._alive = False

class JobQueue(Queue.Queue):
    '''Class managing a queue of pending jobs'''
    _queuingJobs = {}
    _lock = threading.Lock()
    _pendingD2dDest = set()
    _priorityQueue = Queue.Queue()
    
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

def initQueues():
    for scheduler in configuration['JobManager']['Hosts'].split():
        try:
            for jobid, job, arrivaltime, jobtype in connections.getQueuingJobs(scheduler,socket.gethostname()):
                jobQueue.put(scheduler, jobid, job, jobtype, arrivaltime)
        except Exception, e:
            # we could not connect. No problem, the scheduler is probably not running, so no queue to retrieve
            log('No queue could be retrieved from ' + scheduler + ' (' + str(e) + ')')
            pass

# Note that from python 2.5 on, the daemonization should use the "with" statement :
# with daemon.DaemonContext():
#   rest of the code
context = daemon.DaemonContext()
context.__enter__()
try:
    # setup logging
    syslog.openlog('dsmd', 0, syslog.LOG_LOCAL3)
    log = syslog.syslog
    # get configuration
    configuration = castor_tools.castorConf()
    # create a queue of jobs to be run
    jobQueue = JobQueue()
    # create a list of running Jobs
    runningJobs = RunningJobs()
    # create a connection pool for connections to the stagers
    connections = connectionpool.ConnectionPool(int(configuration['JobManager']['SchedulerPort']))
    # initialize the queues of pending and running jobs using the knowledge of the central schedulers
    initQueues()
    # launch a processing thread that will regularly check if we need to start new jobs from the queue
    activityControl = ActivityControl()
    activityControl.setDaemon(True)
    activityControl.start()
    # launch a processing thread that will regularly check job completions
    jobCompletionControl = JobCompletionControl()
    jobCompletionControl.setDaemon(True)
    jobCompletionControl.start()
    # launch a service listening for new jobs from the central scheduler and filling the queue
    from rpyc.utils.server import ThreadedServer
    t = ThreadedServer(DiskServerManagerService, port = int(configuration['JobManager']['DiskManagerPort']), auto_register=False)
    t.daemon = True
    t.start()
    # we reach this point when the service has stopped
    # so let's stop the other threads
    activityControl.stop()
    jobCompletionControl.stop()
finally:
    context.__exit__(None, None, None)
