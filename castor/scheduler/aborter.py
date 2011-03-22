#/******************************************************************************
# *                   aborter.py
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
# * aborter class of the low latency scheduling facility of the CASTOR project
# * this class is responsible for polling the DB for jobs to abort and
# * effectively abort them on the relevant diskservers.
# *
# * @author Castor Dev team, castor-dev@cern.ch
# *****************************************************************************/

import time
import threading
import castor_tools
import socket
import dlf
from llsfddlf import msgs

class Aborter(threading.Thread):
  '''Aborter thread, responsible for connecting to the stager database and getting the list of jobs to be aborted'''

  def __init__(self, connections, llsfd):
    '''constructor of this thread. Arguments are the connection pool and the parent llsfd object'''
    threading.Thread.__init__(self)
    # whether we should continue running
    self.running = True
    # a pool of connections to the diskservers
    self.connections = connections
    # link to the llsfd object
    self.llsfd = llsfd
    # whether we are connected to the stager DB
    self.stagerConnection = None

  def dbConnection(self):
    '''returns a connection to the stager DB.
    The connection is cached and reconnections are handled'''
    if self.stagerConnection == None:
      self.stagerConnection = castor_tools.connectToStager()
      self.stagerConnection.autocommit = False
    return self.stagerConnection

  def run(self):
    '''main method, containing the infinite loop'''
    try:
      while self.running:
        try:
          # setup an oracle connection and register our interest for 'jobsReadyToSchedule' alerts
          stcur = self.dbConnection().cursor()
          stcur.execute("BEGIN DBMS_ALERT.REGISTER('jobsToAbort'); END;");
          # prepare a cursor for database polling
          stcur = self.dbConnection().cursor()
          subReqIdsCur = self.dbConnection().cursor()
          subReqIdsCur.arraysize = 200
          # infinite loop over the polling of the DB
          while self.running:
              # see whether there is something to do
              # not that this will hang until something comes or the internal timeout is reached
              stcur.callproc('jobsToAbortProc', (1, subReqIdsCur))
              # kill the corresponding jobs
              subReqIds = subReqIdsCur.fetchall()
              if subReqIds:
                self.llsfd.bkill(subReqIds)
              # and commit the changes in the DB so that we do not try to drop these jobs again
              stcur.execute("COMMIT")
        except Exception, e:
          # "Caught exception in Aborter thread" message
          dlf.writeerr(msgs.ABORTEREXCEPTION, type=str(e.__class__), msg=str(e))
          # roll back in case
          try:
            stcur.execute("ROLLBACK")
          except:
            pass
          # then sleep a bit to not loop to fast on the error
          time.sleep(1)
    finally:
      # try to clean up what we can
      try:
        stcur = self.dbConnection().cursor()
        stcur.execute("BEGIN DBMS_ALERT.REMOVE('jobsToAbort'); END;");
      except Exception:
        pass
      try:
        castor_tools.disconnectDB(self.dbConnection())
      except Exception:
        pass

  def stop(self):
    '''Stops processing of this thread'''
    self.running = False
