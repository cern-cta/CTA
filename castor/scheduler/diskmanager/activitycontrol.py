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

import os, sys, threading, time, subprocess
import connectionpool, dlf
from diskmanagerdlf import msgs
from transfer import TransferType, RunningTransfer, transferToCmdLine

class ActivityControlThread(threading.Thread):
  '''activity control thread.
  This thread is responsible for starting new transfers when free slots are available'''

  def __init__(self, runningTransfers, transferQueue, configuration, fake):
    '''constructor'''
    super(ActivityControlThread, self).__init__(name='ActivityControl')
    self.alive = True
    self.runningTransfers = runningTransfers
    self.transferQueue = transferQueue
    self.configuration = configuration
    self.fake = fake
    # last time we've tried to schedule a transfer. This is used by the ActivityControlChecker
    # to find out whether the ActivityControl thread is dead or not
    self.lastschedattempt = 0
    # start the thread
    self.setDaemon(True)
    self.start()

  def checkSpaceLeftOnFS(self, transfer):
    ''' check whether there is space available for a given transfer'''
    # check whether this transfer will write data
    if transfer.transferType == TransferType.D2DSRC:
      return True
    if transfer.transferType == TransferType.STD and transfer.flags == 'r' :
      return True
    # find out the limit in terms of free space, from the config file
    minFreeSpacePerc = self.configuration.getValue('DiskManager', 'FSMinAllowedFreeSpace', 0.02, float)
    # get status of the filesystem
    stat = os.statvfs(transfer.mountPoint)
    availableSpace = stat.f_bavail * stat.f_frsize
    totalSpace = stat.f_blocks * stat.f_bsize
    # do the check
    return availableSpace > minFreeSpacePerc * totalSpace

  def startD2DTransfer(self, scheduler, transfer, srcDcPath, destDcPath):
    '''effectively starts a disk to disk transfer'''
    # build command line
    cmdLine = ['rfcp', srcDcPath, destDcPath]
    # start transfer process
    process = 0
    try:
      process = subprocess.Popen(cmdLine, close_fds=True, stderr=subprocess.PIPE)
    except Exception, e:
      # avoid raising standard exception, as some are used for dedicated purposes
      raise Exception(str(e))
    self.runningTransfers.add(RunningTransfer(scheduler, process, time.time(), transfer, destDcPath))

  def run(self):
    '''main method, containing the infinite loop'''
    while self.alive:
      try:
        # check number of running transfers against limit
        if self.runningTransfers.nbUsedSlots() < self.configuration.getValue('DiskManager', 'NbSlots', 0, int):
          # get a new transfer from the transferQueue
          qTransfer = self.transferQueue.get()
          self.lastschedattempt = time.time()
          # check whether we got something or timed out
          if qTransfer == None:
            # We timed out, retry straight
            continue
          scheduler, transfer = qTransfer.scheduler, qTransfer.transfer
          try:
            # in case of transfers wanting to write data, we check that space is available
            if not self.checkSpaceLeftOnFS(transfer):
              # not enough space, we cancel the transfer
              connectionpool.connections.transfersCanceled(scheduler, (transfer.asTuple(), 28, 'No space left on device')) # ENOSPC
            else:
              # notifies the central scheduler that we want to start this transfer
              # this may raise a ValueError exception if we should give up (e.g. the
              # job has already started somewhere else)
              result = connectionpool.connections.transferStarting(scheduler, transfer.asTuple())
              # "Transfer starting" message
              dlf.write(msgs.TRANSFERSTARTING, subreqid=transfer.transferId,
                        reqid=transfer.reqId, fileId=transfer.fileId,
                        TransferType=TransferType.toStr(transfer.transferType))
              # in case of d2dsrc, do not actually start for real, only take note
              if transfer.transferType == TransferType.D2DSRC:
                self.runningTransfers.add(RunningTransfer(scheduler, None, time.time(), transfer, None))
              elif transfer.transferType == TransferType.D2DDST:
                # we have to launch the actual transfer
                srcPath, dstPath = result
                self.startD2DTransfer(scheduler, transfer, srcPath, dstPath)
              else:
                # start the transfer
                if not self.fake:
                  try:
                    process = subprocess.Popen(transferToCmdLine(transfer), close_fds=True, stderr=subprocess.PIPE)
                  except Exception, e:
                    # avoid raising standard exception, as some are used for dedicated purposes
                    raise Exception(str(e))
                else:
                  process = 0
                self.runningTransfers.add(RunningTransfer(scheduler, process, time.time(), transfer, None))
          except connectionpool.Timeout:
            # we timed out in the call to transfersCanceled or transferStarting. We need to try again
            # thus we put the transfer into the priority queue and inform the scheduler
            try:
              self.transferQueue.putPriority(scheduler, transfer)
              connectionpool.connections.transferBackToQueue(scheduler, transfer.asTuple())
              # 'Timeout when trying to start/cancel transfer. Putting it back to the queue' message
              dlf.writenotice(msgs.TRANSFERSTARTTIMEOUT, subreqid=transfer.transferId,
                              reqid=transfer.reqId, fileId=transfer.fileId)
            except connectionpool.Timeout:
              # 'Failed to start transfer and got timeout when putting back to queue'
              dlf.writenotice(msgs.TRANSFERBACKTOQUEUEFAILED, subreqid=transfer.transferId,
                              reqid=transfer.reqId, fileId=transfer.fileId, originalError='Timeout')
          except ValueError, e:
            # 'Transfer start canceled' message
            dlf.writedebug(msgs.TRANSFERSTARTCANCELED, reason=e.args, subreqid=transfer.transferId,
                           fileId=transfer.fileId)
            # the transfer has already started somewhere else, or has been canceled, so give up
          except EnvironmentError:
            # we have tried to start a disk to disk copy and the source is not yet ready
            # we will put it in a pending queue
            # "Start postponned until source is ready" message
            dlf.write(msgs.POSTPONEDFORSRCNOTREADY, subreqid=transfer.transferId,
                      reqid=transfer.reqId, fileId=transfer.fileId)
            # put the transfer into the pending queue
            self.transferQueue.d2dDestReady(scheduler, transfer)
          except Exception, e:
            # startup of the transfer failed with unexpected error
            # We need to try again thus we put the transfer into the priority queue and inform the scheduler
            try:
              self.transferQueue.putPriority(scheduler, transfer)
              connectionpool.connections.transferBackToQueue(scheduler, transfer.asTuple())
              # 'Failed to start transfer. Putting it back to the queue' message
              dlf.writeerr(msgs.TRANSFERSTARTINGFAILED, subreqid=transfer.transferId,
                           reqid=transfer.reqId, fileId=transfer.fileId, error=e)
              time.sleep(1)
            except connectionpool.Timeout:
              # clear this exception context, i.e. the Timeout, so that we log the original error
              sys.exc_clear()
              # 'Failed to start transfer and got timeout when putting back to queue'
              dlf.writeerr(msgs.TRANSFERBACKTOQUEUEFAILED, subreqid=transfer.transferId,
                           reqid=transfer.reqId, fileId=transfer.fileId, originalError=e)
        else:
          time.sleep(.05)
      except Exception:
        # "Caught exception in ActivityControl thread" message
        dlf.writeerr(msgs.ACTIVITYCONTROLEXCEPTION)
        time.sleep(1)

  def stop(self):
    '''stops processing in this thread'''
    self.alive = False
