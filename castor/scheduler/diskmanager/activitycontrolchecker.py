#!/usr/bin/python
#/******************************************************************************
# *                   activitycontrolchecker.py
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
# * the activity control checker thread of the disk server manager daemon of CASTOR
# *
# * @author Castor Dev team, castor-dev@cern.ch
# *****************************************************************************/

"""activity control checker thread of the disk server manager daemon of CASTOR."""

import threading, time, dlf
import connectionpool
from diskmanagerdlf import msgs

class ActivityControlCheckerThread(threading.Thread):
  '''ActivityControlChecker thread of the disk server manager, checking the status
  of the ActivityControl thread.
  This thread regularly checks whether there are jobs waiting in the queue with slots
  available for too long. In such a case, it drops the rpyc connections.
  This is actually a work around some rpyc instabilities where we can get stuck in
  receiving an rpyc message forever. This happens if only half of the message arrives,
  as the reading of the rest of it has no timeout'''

  def __init__(self, runningTranfers, transferQueue, configuration, activityControlThread):
    '''constructor'''
    super(ActivityControlCheckerThread, self).__init__(name='ActivityControlChecker')
    self.alive = True
    self.runningTransfers = runningTranfers
    self.transferQueue = transferQueue
    self.configuration = configuration
    self.activityControlThread = activityControlThread
    # start the thread
    self.setDaemon(True)
    self.start()

  def run(self):
    '''main method, containing the infinite loop'''
    while self.alive:
      # do not loop too fast, but be able to exit in < 1s
      for i in range(5):
        if not self.alive:
          return
        time.sleep(1)
      # go for the checks
      try:
        # if empty queue, nothing looks bad
        if self.transferQueue.empty():
          continue
        # if too few slots left, nothing looks bad
        nbslots = self.configuration.getValue('DiskManager', 'NbSlots', 0, int)
        maxnbemptyslots = self.configuration.getValue('DiskManager', 'maxNbEmptySlotsUnderLoad', 10, int)
        if nbslots - self.runningTransfers.nbUsedSlots() < maxnbemptyslots:
          continue
        # did we scheduler recently ? Then it's ok, we are emptying the queue
        if time.time() - self.activityControlThread.lastschedattempt < 1:
          continue
        # We did not schedule recently.
        # Let's wait for a bit so that the system has a chance to schedule something
        time.sleep(1)
        # check whether something was scheduled in the meantime
        if time.time() - self.activityControlThread.lastschedattempt > 1.5:
          # nothing scheduled, this looks bad -> drop the connections
          connectionpool.connections.closeall()
          # 'Detected stuck ActivityControl thread. Killed connections to transfermanagerd' message
          dlf.writenotice(msgs.ACTIVITYCHECKDROPCONN)
      except Exception, e:
        # 'Caught exception in ActivityControlChecker thread. Giving up for this round' message
        dlf.writeerr(msgs.ACTIVITYCHECKEXCEPTION, Type=str(e.__class__), Message=str(e))

  def stop(self):
    '''stops processing in this thread'''
    self.alive = False

