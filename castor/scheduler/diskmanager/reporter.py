#!/usr/bin/python
#/******************************************************************************
# *                   reporter.py
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
# * the reporter thread of the disk server manager daemon of CASTOR
# *
# * @author Castor Dev team, castor-dev@cern.ch
# *****************************************************************************/

"""reporter thread of the disk server manager daemon of CASTOR."""

import threading, socket, os, time
import connectionpool
import dlf
from diskmanagerdlf import msgs

class StreamCount(object):
  '''a small object handling count of streams'''
  def __init__(self):
    '''constructor'''
    self.nbReads = 0
    self.nbWrites = 0
    self.nbRecalls = 0
    self.nbMigrations = 0

class ReporterThread(threading.Thread):
  '''activity control thread.
  This thread is responsible for regularly sending the resource monitoring and status
   information concerning the diskserver where it runs to the transfermanager'''

  def __init__(self, runningTransfers, configuration):
    '''constructor'''
    super(ReporterThread, self).__init__(name='Reporter')
    self.alive = True
    self.runningTransfers = runningTransfers
    self.configuration = configuration
    # start the thread
    self.setDaemon(True)
    self.start()

  def buildReport(self):
    '''builds a report concerning all filesystems from this diskserver'''
    # the report will have the following form :
    # ((diskServerName, mountPoint, maxFreeSpace, minAllowedFreeSpace, totalSpace,
    #   freeSpace, nbReadStreams, nbWriteStreams, nbRecalls, nbMigrations), ...)
    # with one tuple per filesystem
    diskServerName = socket.getfqdn()
    maxFreeSpace = self.configuration.getValue('DiskManager', 'FSMaxFreeSpace', 0.10, float)
    minAllowedFreeSpace = self.configuration.getValue('DiskManager', 'FSMinAllowedFreeSpace', 0.05, float)
    mountPoints = set(self.configuration.getValue('DiskManager', 'MountPoints', '').split())
    streamCount = self.runningTransfers.getStreamCount()
    reports = []
    for mountPoint in mountPoints:
      # check we really have a mount point, abort if not
      if not os.path.ismount(mountPoint):
        raise Exception("mountPoint %s is not a physical mount point" % mountPoint)
      # get total and free space
      stat = os.statvfs(mountPoint)
      totalSpace = stat.f_blocks*stat.f_bsize
      freeSpace = stat.f_bavail*stat.f_bsize
      # get number of streams of each kind
      try:
        sc = streamCount[mountPoint]
      except KeyError:
        # no stream at all on this filesystem
        sc = StreamCount()
      # fill report
      reports.append((diskServerName, mountPoint, maxFreeSpace, minAllowedFreeSpace,
                      totalSpace, freeSpace, sc.nbReads, sc.nbWrites, sc.nbRecalls, sc.nbMigrations))
      # check whether we should create subdirectories in the mountPoints, that is
      # whether this is a brand new mountPoints that we should populate
      try:
        os.stat(os.sep.join([mountPoint, '01']))
      except OSError:
        # no, then let's create the missing directories
        for i in range(100):
          os.mkdir(os.sep.join([mountPoint, '%02d' % i]))
    return tuple(reports)

  def run(self):
    '''main method, containing the infinite loop'''
    # remember time of the last error log
    lastErrorLogTime = 0.0
    while self.alive:
      try:
        # build and send report
        report = self.buildReport()
        schedulers = self.configuration.getValue('DiskManager', 'ServerHosts').split()
        if len(schedulers) == 0:
          # "No scheduler given in configfile, please fix" message
          dlf.writeerr(msgs.NOSCHEDULERINCONFIG)
        else:
          notSent = True
          i = 0
          while notSent and i < len(schedulers):
             try:
               connectionpool.connections.stateReport(schedulers[i], report)
               notSent = False
             except Exception, e:
               # "Caught exception when sending report to transfermanager,
               # will try other transfermanagers" message
               dlf.writedebug(msgs.SENDREPORTEXCEPTION, transferManager=schedulers[i], error=str(e))
             i += 1
          if notSent:
             # no more than one error log every HeartbeatNotSentLogInterval to not flood the logs
             if time.time() > lastErrorLogTime + \
                self.configuration.getValue('DiskManager', 'HeartbeatNotSentLogInterval', 300.0, float):
                # "Could not send report to any transfermanager, giving up" message
                dlf.writeerr(msgs.SENDREPORTFAILURE, schedulers=str(schedulers), error=str(e))
                # remember last error time
                lastErrorLogTime = time.time()
             else:
                # "Could not send report to any transfermanager, giving up" message
                dlf.writedebug(msgs.SENDREPORTFAILURE, schedulers=str(schedulers), error=str(e))
      except Exception, e:
        # "Caught exception in Reporter thread" message
        dlf.writeerr(msgs.REPORTEREXCEPTION, error=str(e))
      # do not loop too fast, send only one report every second
      time.sleep(self.configuration.getValue('DiskManager', 'HeartbeatInterval', 1.0, float))

  def stop(self):
    '''stops processing in this thread'''
    self.alive = False
