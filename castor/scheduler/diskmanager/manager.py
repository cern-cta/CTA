#!/usr/bin/python
#/******************************************************************************
# *                   manager.py
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
# * the manager thread of the disk server manager daemon of CASTOR
# *
# * @author Castor Dev team, castor-dev@cern.ch
# *****************************************************************************/

"""manager thread of the disk server manager daemon of CASTOR."""

import threading, dlf, time
import connectionpool
from diskmanagerdlf import msgs

class ManagerThread(threading.Thread):
  '''Management thread of the disk server manager, dealing with transfer completion control and associated cleanup,
  with transfer cancelation in case of no space, timeouts, etc... and with restart of disk to disk transfer
  that are waiting on the sources when these are ready'''

  def __init__(self, runningTranfers, transferQueue):
    '''constructor'''
    super(ManagerThread, self).__init__(name='ManagementThread')
    self.alive = True
    self.runningTransfers = runningTranfers
    self.transferQueue = transferQueue
    # start the thread
    self.setDaemon(True)
    self.start()

  def run(self):
    '''main method, containing the infinite loop'''
    while self.alive:
      try:
        # check for running transfers that have completed
        self.runningTransfers.poll()
        # check for d2d destination transfers that could now start
        self.transferQueue.pollD2dDest()
        # check for transfer to be canceled for various reasons (timeout, missing resources, ...)
        self.transferQueue.checkForTransfersCancelation()
        # recompute list of ongoing tape transfers
        self.runningTransfers.listTapeTransfers()
      except connectionpool.Timeout, e:
        # 'Caught exception in Management thread' message
        dlf.writenotice(msgs.MGMTEXCEPTION, Type=str(e.__class__), Message=str(e))
      except Exception, e:
        # 'Caught exception in Management thread' message
        dlf.writeerr(msgs.MGMTEXCEPTION, Type=str(e.__class__), Message=str(e))
      # do not loop too fast
      time.sleep(1)

  def stop(self):
    '''stops processing in this thread'''
    self.alive = False

