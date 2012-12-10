#/******************************************************************************
# *                   suicider.py
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
# * suicider class of the transfer manager of the CASTOR project
# * This class is responsible for commiting suicide at the level of the transfermanagerd process :-)
# * It will nevertheless wait that diskmanagers are over with the transfers they
# * have in queue.
# *
# * @author Castor Dev team, castor-dev@cern.ch
# *****************************************************************************/

'''this class is responsible for commiting suicide at the level of the transfermanagerd process :-)
It will nevertheless wait that diskmanagers are over with the transfers they have in queue.'''

import time
import threading
import socket
import dlf
import diskserverlistcache, connectionpool
from transfermanagerdlf import msgs

class SuiciderThread(threading.Thread):
  '''Regularly checks the number of ongoing transfers and commits suicide when it goes down to 0'''

  def __init__(self, transfermanager):
    '''constructor'''
    super(SuiciderThread, self).__init__(name='Suicider')
    self.transfermanager = transfermanager
    # start the thread
    self.setDaemon(True)
    self.start()

  def run(self):
    '''main method, containing the infinite loop checking for the transfers to be over'''
    # loop until we commit suicide
    while True:
      oktodie = True
      # checks whether it is ok to die
      dslist = diskserverlistcache.diskServerList.getlist()
      for diskPool in dslist:
        for ds in dslist[diskPool]:
          try:
            oktodie = not connectionpool.connections.anyTransfersFromScheduler(ds, socket.getfqdn())
          except Exception, e:
            # 'Could not contact diskserver' message
            dlf.writenotice(msgs.COULDNOTCONTACTDS, Function='anyTransfersFromScheduler', DiskServer=ds,
                            Type=str(e.__class__), Message=str(e))
          # do not continue contacting machines if some are already not ok
          if not oktodie:
            break
        if not oktodie:
          break
      # if all is over, commit suicide
      if oktodie:
        self.transfermanager.close()
        break
      else:
        # else sleep one second between two checks
        time.sleep(1)
