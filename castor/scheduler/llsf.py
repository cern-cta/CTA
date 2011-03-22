#!/usr/bin/python
#/******************************************************************************
# *                   llsf.py
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
# * low latency scheduling facility of the CASTOR project
# *
# * @author Castor Dev team, castor-dev@cern.ch
# *****************************************************************************/

import sys
import getopt
import castor_tools
import cx_Oracle
import rpyc
import time
import threading
import connectionpool
import socket
import syslog
import daemon

# usage function
def usage(exitCode):
  print 'Usage : ' + sys.argv[0] + ' [-h|--help] [-v|--verbose]'
  sys.exit(exitCode)

# first parse the options
verbose = False
try:
    options, args = getopt.getopt(sys.argv[1:], 'hv', ['help', 'verbose'])
except Exception, e:
    print e
    usage(1)
for f, v in options:
    if f == '-h' or f == '--help':
        usage(0)
    elif f == '-v' or f == '--verbose':
        verbose = True
    else:
        print "unknown option : " + f
        usage(1)

# If any arg, complain and stop
if len(args) != 0:
    print "Unknown arguments : " + ' '.join(args) + "\n"
    usage(1)

class QueuingJobs(dict):
    '''a dictionnary of queuing jobs, with the list of machines to which they were sent
    plus a reverse lookup facility by machine'''
    _lock = threading.Lock()
    # dictionnary containing the set of jobs running on each machine
    _jobsLocations = {}
    # list of source d2d jobs running
    _d2dsourcerunning = {}

    def put(self, diskserver, jobid, job, arrivaltime, jobtype='standard'):
        '''Adds a new job. jobtype can be one of 'standard', 'd2dsource' and 'd2ddest' '''
        self._lock.acquire()
        # add job to the list of queuing jobs on the diskserver
        # note the extra argument, used only for d2ddest jobs and telling whether the source is ready
        if diskserver not in self:
            self[diskserver] = {}
        self[diskserver][jobid] = [job, arrivaltime, jobtype, False]
        # add the diskserver to the jobLocations list for this job
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
                    connections.d2dSourceReady(machine, jobid)
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
                    connections.jobAlreadyStarted(machine, jobid)

    def d2dend(self, jobid):
        '''called when a d2d copy ends in order to inform the source'''
        self._lock.acquire()
        # get the source location
        diskserver = self._d2dsourcerunning[jobid]
        # cleanup
        del self._d2dsourcerunning[jobid]
        # inform the source diskserver
        connections.d2dend(diskserver, jobid)
        self._lock.release()

    def list(self, diskserver):
        if diskserver in self:
            return [tuple([job[0]]+job[1][0:3]) for job in self[diskserver].items()]
        else:
            return []


class Scheduler(threading.Thread):
    '''scheduling thread, responsible for connecting to the stager databse and scheduling jobs'''

    def run(self):
        '''main method, containing the infinite loop'''
        try:
            # setup an oracle connection and register our interest for 'jobsReadyToSchedule' alerts
            stconn = castor_tools.connectToStager()
            stcur = stconn.cursor()
            stcur.execute("BEGIN DBMS_ALERT.REGISTER('jobsReadyToSchedule'); END;");
            # prepare a cursor for database polling
            stcur = stconn.cursor()
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
            while True:
                # see whether there is something to do
                # not that this will hang until something comes or the internal timeout is reached
                stcur.execute(stJobToSchedule, (srId, srSubReqId, srProtocol, srXsize, srRfs, reqId, cfFileId, cfNsHost, reqSvcClass, reqType, reqEuid, reqEgid, reqUsername, srOpenFlags, clientIp, clientPort, clientVersion, clientType, reqSourceDiskCopy, reqDestDiskCopy, clientSecure, reqSourceSvcClass, reqCreationTime, reqDefaultFileSize, sourceRfs));
                log(syslog.LOG_DEBUG, 'back from jobToSchedule2 with srId : ' + str(srId.getvalue()))
                # in case of timeout, we may have nothing to do
                if srId.getvalue() != None:
                    # retrieve jobid
                    jobid = srSubReqId.getvalue()
                    # standard jobs and disk to disk copies are not handled the same way
                    if int(reqType.getvalue() == 133): # OBJ_StageDiskCopyReplicaRequest
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
                        log(jobid + '(d2d source) : ' + schedSourceCandidates[0])
                        diskserver, mountpoint = schedSourceCandidates[0]
                        cmd = tuple(basecmd + ["-R", diskserver+':'+mountpoint])
                        # send the job to the appropriate diskserver
                        arrivaltime = connections.scheduleJob(diskserver, hostname, jobid, cmd, 'd2dsource')
                        # register the job in the local list of pending jobs
                        queuingJobs.put(diskserver,jobid, cmd, arrivaltime, 'd2dsource')
                        # now schedule on all potential destinations
                        schedDestCandidates = [candidate.split(':') for candidate in srRfs.getvalue().split('|')]
                        log(jobid + '(d2d destinations) : ' + str(schedDestCandidates))
                        for diskserver, mountpoint in schedDestCandidates:
                            cmd = tuple(basecmd + ["-R", diskserver+':'+mountpoint])
                            # send the job to the appropriate diskserver
                            arrivaltime = connections.scheduleJob(diskserver, hostname, jobid, cmd, 'd2ddest')
                            # register the job in the local list of pending jobs
                            queuingJobs.put(diskserver,jobid, cmd, arrivaltime, 'd2ddest')
                    else:
                        schedCandidates = [candidate.split(':') for candidate in srRfs.getvalue().split('|')]
                        log(jobid + ' : ' + str(schedCandidates))
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
                            # send the job to the appropriate diskserver
                            arrivaltime = connections.scheduleJob(diskserver, hostname, jobid, cmd)
                            # register the job in the local list of pending jobs
                            queuingJobs.put(diskserver,jobid, cmd, arrivaltime)
                    # commit the change of status to the stage DB
                    stconn.commit()
            stcur = stconn.cursor()
            stcur.execute("BEGIN DBMS_ALERT.REMOVE('jobsReadyToSchedule'); END;");
            castor_tools.disconnectDB(stconn)
        except Exception, e:
            log(syslog.LOG_ALERT, str(e))
            sys.exit(1)

class DiskServerListCache:
    '''cache for the list of diskservers, organized by service class.'''
    _diskServerList = None

    def getlist(self):
        '''returns the list from the cache, or builds it if needed'''
        if self._diskServerList == None:
            # query the stager database
            stconn = castor_tools.connectToStager()
            stcur = stconn.cursor()
            stcur.arraysize = 50
            stDiskServers = '''SELECT UNIQUE DiskServer.name, SvcClass.name
                                 FROM FileSystem, DiskServer, DiskPool2SvcClass, SvcClass
                                WHERE FileSystem.diskServer = DiskServer.id
                                  AND FileSystem.diskPool = DiskPool2SvcClass.parent
                                  AND DiskPool2SvcClass.child = SvcClass.id'''
            stcur.execute(stDiskServers)
            rows = stcur.fetchall()
            # build up the list
            self._diskServerList = {}
            for diskserver, svcClass in rows:
                if svcClass not in self._diskServerList : self._diskServerList[svcClass] = []
                self._diskServerList[svcClass].append(diskserver)
        return self._diskServerList

    def clear(self):
        '''clears the cache'''
        _diskServerList = None

class SchedulerService(rpyc.Service):
    '''This service is responsible for answering all requests made to the scheduler.
    There are mainly 2 kinds : monitoring requests and callbacks from the diskservers'''

    def dispatch(self, func, svcClass):
        '''gather the results of the call to func for all diskServers'''
        res = {}
        dslist = diskServerList.getlist()
        if svcClass != None: dslist = {svcClass : dslist[svcClass]}
        for svcClass in dslist:
            res[svcClass] = {}
            for ds in dslist[svcClass]:                
                try:
                    # call the function on the appropriate diskserver
                    res[svcClass][ds] = getattr(connections,func)(ds)
                except Exception, e:
                    # we've tried...
                    log(syslog.LOG_NOTICE, 'No answer to ' + func + ' from ' + ds + ' (' + str(e) + ')')
                    pass
        return res

    def exposed_bqueues(self, svcClass=None):
        '''bqueues lists the number of running and pending jobs per queue'''
        log(syslog.LOG_DEBUG, 'bqueues called for svcClass ' + str(svcClass))
        nbs = self.dispatch('nbJobs', svcClass)
        res = []
        for svcClass in nbs:
            if len(nbs[svcClass]) > 0:
                res.append(tuple([svcClass] + map(sum,zip(*nbs[svcClass].values()))))
            else:
                res.append((svcClass,0,0,0))
        return tuple(res)

    def exposed_bhosts(self, svcClass=None):
        '''bhosts lists the hosts in the scheduling facility together with the number of
        running and pending jobs for each of them and their number of slots'''
        log(syslog.LOG_DEBUG, 'bhosts called for svcClass ' + str(svcClass))
        # get the raw data as a dictionnary of dictionnaries (svclass then diskserver level)
        hosts = self.dispatch('nbJobs', svcClass)
        # flatten it to a list of (diskserver, (values))
        hostslist = reduce(lambda x,y:x+y, [diskserver.items() for diskserver in hosts.values()])
        # return a tuple of (diskserver, value1, value2, ...)
        return tuple(tuple([host[0]]+list(host[1])) for host in hostslist)

    def exposed_badmin(self, svcClass=None):
        '''badmin reconfigures the scheduling facility'''
        log(syslog.LOG_DEBUG, 'badmin called for svcClass ' + str(svcClass))
        # reset all known diskservers
        self.dispatch('badmin', svcClass)
        # and reset list of known diskServers
        diskServerList.clear()

    def exposed_bjobs(self, svcClass=None):
        '''bqueues lists the number of running and pending jobs per queue'''
        log(syslog.LOG_DEBUG, 'bjobs called for svcClass ' + str(svcClass))
        jobs = self.dispatch('bjobs', svcClass)
        res = []
        for svcClass in jobs:
            for ds in jobs[svcClass]:
                for jobid, scheduler, user, status, jobtype, arrivalTime, startTime in jobs[svcClass][ds]:
                    res.append((jobid, scheduler, user, status, svcClass, ds, jobtype, arrivalTime, startTime))
        return tuple(res)

    def exposed_jobStarting(self, diskserver, jobid, jobtype):
        '''called when a job started on a given diskserver'''
        log(syslog.LOG_DEBUG, 'jobStarting called for job ' + jobid + ' (' + jobtype + ') on ' + diskserver)
        machinesToInform = queuingJobs.remove(diskserver, jobid, jobtype)

    def exposed_getQueuingJobs(self, diskserver):
        '''called on the (re)start of the diskManager to rebuild the queue of jobs'''
        log(syslog.LOG_DEBUG, 'getQueuingJobs called for ' + diskserver)
        return tuple(queuingJobs.list(diskserver))

    def exposed_d2dend(self, jobid):
        '''called when a d2d is over. Responsible for informing the source of the d2d'''
        log(syslog.LOG_DEBUG, 'd2dend called for jobid ' + jobid)
        queuingJobs.d2dend(jobid)

def initQueues():
    dslist = diskServerList.getlist()
    for svcClass in dslist:
        for ds in dslist[svcClass]:
            try:
                for jobid, job, jobtype, arrivaltime in connections.getConnection(ds).getQueuingJobs(hostname):
                    queuingJobs.put(ds, jobid, job, arrivaltime, jobtype)
            except Exception, e:
                # we could not connect. No problem, the scheduler is probably not running, so no queue to retrieve
                log(syslog.LOG_NOTICE, 'No queue could be retrieved from ' + ds + ' (' + str(e) + ')')
                pass

# Note that from python 2.5 on, the daemonization should use the "with" statement :
# with daemon.DaemonContext():
#   rest of the code
context = daemon.DaemonContext()
context.__enter__()
try:
    hostname = socket.gethostname()
    # setup logging
    syslog.openlog('llsf', 0, syslog.LOG_LOCAL3)
    log = syslog.syslog
    # get configuration
    configuration = castor_tools.castorConf()
    # global cache on the list of diskservers
    diskServerList = DiskServerListCache()
    # create a connection pool for connections to the DiskServers
    connections = connectionpool.ConnectionPool(int(configuration['JobManager']['DiskManagerPort']))
    # a dictionnary holding the list of jobs submitted by us and still queuing for each diskserver
    queuingJobs = QueuingJobs()
    # initialize the queues of pending and running jobs using the knowledge of the diskservers
    initQueues()
    # launch a processing thread that will regularly check if we need to schedule new jobs
    scheduler = Scheduler()
    scheduler.setDaemon(True)
    scheduler.start()
    # launch a service listening for monitoring queries and answering them
    from rpyc.utils.server import ThreadedServer
    t = ThreadedServer(SchedulerService, port = int(configuration['JobManager']['SchedulerPort']), auto_register=False)
    t.daemon = True
    t.start()
finally:
    context.__exit__(None, None, None)
