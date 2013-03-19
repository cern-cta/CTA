#!/usr/bin/python
#/******************************************************************************
# *                   activitycontrol.py
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
# * the activity control thread of the disk server manager daemon of CASTOR
# *
# * @author Castor Dev team, castor-dev@cern.ch
# *****************************************************************************/

"""activity control thread of the disk server manager daemon of CASTOR."""

import os, sys, threading, time, socket, tempfile, subprocess
import connectionpool, dlf
from diskmanagerdlf import msgs

class ActivityControlThread(threading.Thread):
  '''activity control thread.
  This thread is responsible for starting new transfers when free slots are available'''

  def __init__(self, runningTranfers, transferQueue, configuration, fake):
    '''constructor'''
    super(ActivityControlThread, self).__init__(name='ActivityControl')
    self.alive = True
    self.runningTransfers = runningTranfers
    self.transferQueue = transferQueue
    self.configuration = configuration
    self.fake = fake
    # last time we've tried to schedule a transfer. This is used by the ActivityControlChecker
    # to find out whether the ActivityControl thread is dead or not
    self.lastschedattempt = 0
    # start the thread
    self.setDaemon(True)
    self.start()

  def checkSpaceLeftOnFS(self, transfertype, transfer):
    ''' check whether there is space available for a given transfer'''
    # check whether this transfer will write data
    if transfertype == 'd2dsrc':
      return True
    if transfertype == 'standard' and transfer[16] == 'r' :
      return True
    # get the transfer filesystem
    fs = transfer[-1].split(':')[1]
    # find out the limit in terms of free space, from the config file
    minFreeSpacePerc = self.configuration.getValue('DiskManager', 'FSMinAllowedFreeSpace', 0.05, float)
    # get status of the filesystem
    stat = os.statvfs(fs)
    availableSpace = stat.f_bavail * stat.f_frsize
    totalSpace = stat.f_blocks * stat.f_bsize
    # do the check
    return availableSpace > minFreeSpacePerc * totalSpace

  def run(self):
    '''main method, containing the infinite loop'''
    while self.alive:
      try:
        # check number of running transfers against limit
        if self.runningTransfers.nbUsedSlots() < self.configuration.getValue('DiskManager', 'NbSlots', 0, int):
          # get a new transfer from the transferQueue
          scheduler, transferid, transfer, transfertype, arrivalTime = self.transferQueue.get()
          self.lastschedattempt = time.time()
          # check whether we got something or timed out
          if scheduler == None:
            # We timed out, retry straight
            continue
          fileid = (transfer[8], int(transfer[6]))
          try:
            # in case of transfers wanting to write data, we check that space is available
            if not self.checkSpaceLeftOnFS(transfertype, transfer):
              # not enough space, we cancel the transfer
              connectionpool.connections.transfersCanceled(scheduler, socket.getfqdn(), tuple([(transferid, fileid, 28, 'No space left on device', transfer[2])])) # ENOSPC
            else:
              # notifies the central scheduler that we want to start this transfer
              # this may raise a ValueError exception if it already started
              # somewhere else
              connectionpool.connections.transferStarting(scheduler, socket.getfqdn(), transferid, transfer[2], transfertype)
              # "Transfer starting" message
              dlf.write(msgs.TRANSFERSTARTING, subreqid=transferid, reqid=transfer[2], fileid=fileid, TransferType=transfertype)
              # in case of d2dsrc, do not actually start for real, only take note
              if transfertype == 'd2dsrc':
                self.runningTransfers.add(transferid, scheduler, transfer, None, None, transfertype, arrivalTime)
              else:
                # create a local file to hold the former scheduler info
                # Note that this is inherited from the former scheduler limitations andd
                # should be dropped in favour of passing directly the value to
                # the stagerJob on the command line
                try:
                  notifFileHandle, notifFileName = tempfile.mkstemp()
                  notifFile = os.fdopen(notifFileHandle, 'w')
                  notifFile.write(transfer[-1])
                  notifFile.close()
                  os.chmod(notifFileName, 0444)
                except Exception, e:
                  # avoid raising standard exception, as some are used for dedicated purposes
                  raise Exception(str(e))
                # build the stagerTransfer command line and execute it
                cmd = list(transfer[:-1])
                cmd.append('file://'+notifFileName)
                # start the transfer
                if not self.fake:
                  try:
                    process = subprocess.Popen(cmd, close_fds=True, stderr=subprocess.PIPE)
                  except Exception, e:
                    # avoid raising standard exception, as some are used for dedicated purposes
                    raise Exception(str(e))
                else:
                  process = 0
                self.runningTransfers.add(transferid, scheduler, transfer, notifFileName, process, transfertype, arrivalTime)
          except connectionpool.Timeout:
            # we timed out in the call to transfersCanceled or transferStarting. We need to try again
            # thus we put the transfer into the priority queue and inform the scheduler
            # note that we need to pass a tuple of tuples
            transferList = ((socket.getfqdn(), transfer),)
            try:
              connectionpool.connections.transferBackToQueue(scheduler, transferid, arrivalTime, transferList, transfertype)
              self.transferQueue.putPriority(scheduler, transferid, transfer, transfertype, arrivalTime)
              # 'Timeout when trying to start/cancel transfer. Putting it back to the queue' message
              dlf.writenotice(msgs.TRANSFERSTARTTIMEOUT, subreqid=transferid, reqid=transfer[2], fileid=fileid)
            except connectionpool.Timeout:
              # 'Failed to start transfer and got timeout when putting back to queue'
              dlf.writenotice(msgs.TRANSFERBACKTOQUEUEFAILED, subreqid=transferid, reqid=transfer[2], fileid=fileid)
          except ValueError:
            if transfertype == 'd2dsrc':
              # 'source transfer has been canceled while queuing. Not starting it'
              dlf.writedebug(msgs.SRCTRANSFERCANCELED, subreqid=transferid, reqid=transfer[2], fileid=fileid)
            else:
              # 'Transfer start canceled (already started on another host)' message
              dlf.writedebug(msgs.TRANSFERALREADYSTARTED, subreqid=transferid, reqid=transfer[2], fileid=fileid)
              # the transfer has already started somewhere else, so give up
          except EnvironmentError:
            # we have tried to start a disk to disk copy and the source is not yet ready
            # we will put it in a pending queue
            # "Start postponned until source is ready" message
            dlf.write(msgs.POSTPONEDFORSRCNOTREADY, subreqid=transferid, reqid=transfer[2], fileid=fileid)
            # put the transfer into the pending queue
            self.transferQueue.d2dDestReady(scheduler, transferid, transfer, arrivalTime, transfertype)
          except Exception:
            # startup of the transfer failed with unexpected error
            # We need to try again thus we put the transfer into the priority queue and inform the scheduler
            # note that we need to pass a tuple of tuples
            transferList = ((socket.getfqdn(), transfer),)
            try:
              connectionpool.connections.transferBackToQueue(scheduler, transferid, arrivalTime, transferList, transfertype)
              self.transferQueue.putPriority(scheduler, transferid, transfer, transfertype, arrivalTime)
              # 'Failed to start transfer. Putting it back to the queue' message
              dlf.writeerr(msgs.TRANSFERSTARTINGFAILED, subreqid=transferid, reqid=transfer[2], fileid=fileid)
              time.sleep(1)
            except connectionpool.Timeout:
              # clear this exception context, i.e. the Timeout, so that we log the original error
              sys.exc_clear()
              # 'Failed to start transfer and got timeout when putting back to queue'
              dlf.writeerr(msgs.TRANSFERBACKTOQUEUEFAILED, subreqid=transferid, reqid=transfer[2], fileid=fileid)
        else:
          time.sleep(.05)
      except Exception:
        # "Caught exception in ActivityControl thread" message
        dlf.writeerr(msgs.ACTIVITYCONTROLEXCEPTION)
        time.sleep(1)

  def stop(self):
    '''stops processing in this thread'''
    self.alive = False
