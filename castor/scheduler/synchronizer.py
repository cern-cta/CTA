#/******************************************************************************
# *                   synchronizer.py
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
# * synchronizer class of the low latency scheduling facility of the CASTOR project
# * this class is responsible for regularly synchronizing the stager DB with the running
# * jobs, meaning that it will check that jobs running for already long according to the DB
# * are effectively still running. If they are not, is will update the DB accordingly
# *
# * @author Castor Dev team, castor-dev@cern.ch
# *****************************************************************************/

import time
import threading
import castor_tools
import random
import socket
import castor_tools
import dlf
from llsfddlf import msgs

class Synchronizer(threading.Thread):
  '''synchronizer class of the low latency scheduling facility of the CASTOR project
  this class is responsible for regularly synchronizing the stager DB with the running
  jobs, meaning that it will check that jobs running for already long according to the DB
  are effectively still running. If they are not, is will update the DB accordingly'''

  def __init__(self, connections, diskServerList, llsfd):
    '''constructor of this thread. Arguments are the connection pool and the parent llsfd object'''
    threading.Thread.__init__(self)
    # whether we should continue running
    self.running = True
    # a pool of connections to the diskservers
    self.connections = connections
    # list of diskservers
    self.diskServerList = diskServerList
    # link to the llsfd object
    self.llsfd = llsfd
    # whether we are connected to the stager DB
    self.stagerConnection = None

  def dbConnection(self, autocommit=True):
    '''returns a connection to the stager DB.
    The connection is cached and reconnections are handled'''
    if self.stagerConnection == None:
      self.stagerConnection = castor_tools.connectToStager()
    self.stagerConnection.autocommit = autocommit
    return self.stagerConnection

  def run(self):
    '''main method, containing the infinite loop'''
    # get the interval between 2 checks
    checkInterval = configuration.getValue("JobManager","ManagementInterval",None,int)
    if checkInterval == None:
      raise ValueError, 'no JobManager/ManagementInterval entry in configuration file'
      sys.exit(1)
    try:
      while self.running:
        # first wait a bit before starting so that we are not syncronized between
        # daemons at a restart, or after errors
        slept = 0
        while slept < random.randint(1, checkInterval):
          # note that we sleep one second at a time so that we can exit
          # if the thread stops running
          if not self.running: break
          time.sleep(1)
          slept = slept + 1
        try:
          # prepare a cursor for database polling
          stcur = self.dbConnection().cursor()
          subReqIdsCur = self.dbConnection().cursor()
          subReqIdsCur.arraysize = 200
          # infinite loop over the polling of the DB
          while self.running:
              # list pending and running jobs in the scheduling system
              stcur.callproc('getSchedulerJobs2', subReqIdsCur)
              subReqIds = set(subReqIdsCur.fetchall())
              try:
                # list pending and running jobs in the scheduling system
                allLlsfJobs = set()
                diskservers = diskServerList.getset()
                for ds in diskServerList:
                  # call the function on the appropriate diskserver
                  allLlsfJobs = allLlsfJobs | set(connections.jobset(ds))
                # find out the set of jobs in the DB and no more in the scheduling system
                jobsToFail = list(subReqIds - allLlsfJobs)
                # and inform the stager
                if jobsToFail:
                  # get back the fileids for the logs. We need to go back to the DB just for this
                  conn = self.dbConnection(False)
                  fileIdsCur = conn.cursor()
                  fileIdsCur.arraysize = 200
                  stcur = conn.cursor()
                  stcur.callproc('getFileIdsForSrs', [jobsToFail, fileIdsCur])
                  fileids = map(tuple, fileIdsCur.fetchall())
                  conn.commit()
                  # 'Job killed by synchronization as it disappeared from the scheduling system'
                  for jobid, fileid in zip(jobsToFail, fileids):
                    dlf.write(msgs.SYNCHROKILLEDJOB, subreqid=jobid, fileid=fileid)
                  # prepare the jobs so that we have only tuples going onver the wire
                  jobs = tuple(map(tuple,
                                   zip(jobsToFail, fileids,
                                       [1015] * len(jobsToFail), # SEINTERNAL error code
                                       ['Job has disappeared from the scheduling system'] * len(jobsToFail))))
                  # finally inform the stager
                  self.connections.jobsKilled(self.hostname, jobs)
              except Exception, e:
                # we could not list all pending running jobs in the system
                # Thus we have to give up with synchronization for this round
                # 'Error caught while trying to synchronize DB jobs with scheduler jobs. Giving up for this round.'
                dlf.writeerr(msgs.SYNCHROFAILED, type=str(e.__class__), msg=str(e))
              # sleep until next check
              slept = 0
              while slept < checkInterval:
                # note that we sleep one second at a time so that we can exit
                # if the thread stops running
                if not self.running: break
                time.sleep(1)
                slept = slept + 1
          finally:
            stcur.close()
        except Exception, e:
          # "Caught exception in Synchronizer thread" message
          dlf.writeerr(msgs.SYNCHROEXCEPTION, type=str(e.__class__), msg=str(e))
    finally:
      # try to clean up what we can
      try:
        castor_tools.disconnectDB(self.dbConnection())
      except Exception:
        pass

  def stop(self):
    '''Stops processing of this thread'''
    self.running = False
