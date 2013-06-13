#/******************************************************************************
# *                   localqueue.py
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
# * localqueue class of the transfer manager of the CASTOR project
# * this class is responsible for managing a local queue of pending transfers for a given diskserver.
# *
# * @author Castor Dev team, castor-dev@cern.ch
# *****************************************************************************/

'''localqueue module of the CASTOR disk server manager.
Handles a local queue of transfers pending on a given diskserver'''

import pwd
import time
import Queue
import threading
import dlf
import castor_tools
import connectionpool
from diskmanagerdlf import msgs
from transfer import TransferType

class QueueingTransfer(object):
  '''little container describing a queueing transfer'''
  def __init__(self, scheduler, transfer):
    '''constructor'''
    self.scheduler = scheduler
    self.transfer = transfer

class LocalQueue(Queue.Queue):
  '''Class managing a queue of pending transfers.
  There are actually 2 queues, a dictionnary and a set involved :
    - the main queue is the object itself and holds all transfers that were not considered so far.
    - _pendingD2dDest is a dictionary of disk2disk destinations transfers that have been considered but could not
      be started because the source was not ready. They will be retried regularly until the source
      becomes ready. They are stored together with the next time when to retry. The interval between
      retries is equal to half the time gone since the arrival of the transfer, with a maximum
      configured in castor.conf (DiskManager/MaxRetryInterval).
    - _priorityQueue is a queue of transfers to be started as soon as possible. These transfers are the disk2disk
      destination transfers that have been pending on the resdiness of the source and can now be started
    - finally, all transfers handled are indexed in a dictionnary called queueingTransfers handling detailed
      information
  '''

  def __init__(self):
    '''constructor'''
    Queue.Queue.__init__(self)
    # get configuration
    self.config = castor_tools.castorConf()
    # dictionnary of transfers
    self.queueingTransfers = {}
    # a global lock for the dictionnary
    self.lock = threading.Lock()
    # set of pending disk2disk destinations
    self.pendingD2dDest = {}
    # set of transfers to start with highest priority
    self.priorityQueue = Queue.Queue()

  def put(self, scheduler, transfer):
    '''Put new transfer in the queue'''
    self.lock.acquire()
    try:
      # first keep note of the new transfer (index by subreqId)
      self.queueingTransfers[transfer.transferId] = QueueingTransfer(scheduler, transfer)
      # then add it to the underlying queue
      Queue.Queue.put(self, transfer.transferId)
    finally:
      self.lock.release()

  def putPriority(self, scheduler, transfer):
    '''Put new transfer in the queue'''
    self.lock.acquire()
    try:
      # first keep note of the new transfer (index by subreqId)
      self.queueingTransfers[transfer.transferId] = QueueingTransfer(scheduler, transfer)
      # then add it to the underlying priority queue
      self.priorityQueue.put(transfer.transferId)
    finally:
      self.lock.release()

  def get(self):
    '''get a transfer from the queue. Times out after 1s'''
    found = False
    while not found:
      # try to get a priority transfer first
      try :
        transferId = self.priorityQueue.get(False)
      except Queue.Empty:
        # else get next transfer from the regular queue
        # timeout after 1s so that we can go back to the priority queue
        try:
          transferId = Queue.Queue.get(self, timeout=1)
        except Queue.Empty:
          return None
      self.lock.acquire()
      try:
        try:
          # remove it from the list of pending transfers
          qTransfer = self.queueingTransfers[transferId]
          del self.queueingTransfers[transferId]
          found = True
        except KeyError:
          # transfer not found, meaning it was canceled
          # ignore and go to next one
          pass
      finally:
        self.lock.release()
    # return
    return qTransfer

  def _remove(self, transferIds):
    '''internal method removing transfers from the queue.
    Takes no lock so it is supposed to be called in a safe context'''
    for transferId in transferIds:
      try:
        # we only remove from the queueingTransfers dictionnary
        # the transfers stay in the queue but will be ignored when poped
        del self.queueingTransfers[transferId]
      except KeyError:
        # transfer not found, either not running here or already canceled
        pass

  def remove(self, transferIds):
    '''remove transfers from the queue'''
    # Loop through the transfers to remove
    self.lock.acquire()
    try:
      self._remove(transferIds)
    finally:
      self.lock.release()

  def d2dDestReady(self, scheduler, transfer):
    '''called when a d2ddest transfer could not start as the source is not ready'''
    self.lock.acquire()
    try:
      # compute next time when we should try to start this transfer. We will actually wait
      # as much as has already passed between the arrival of this request and now,
      # caped to the MaxRetryInterval
      currentTime = time.time()
      timeToNextTry = currentTime - transfer.creationTime
      maxTime = self.config.getValue('DiskManager', 'MaxRetryInterval', 300, int)
      if timeToNextTry > maxTime:
        timeToNextTry = maxTime
      # and put the transfer into the list of pending ones
      self.queueingTransfers[transfer.transferId] = QueueingTransfer(scheduler, transfer)
      self.pendingD2dDest[transfer.transferId] = (currentTime + timeToNextTry)
    finally:
      self.lock.release()

  def retryD2dDest(self, transferId, reqid):
    '''retries a given d2ddest job that was on hold because the source is not available '''
    self.lock.acquire()
    try:
      # put the transferId back into the priority queue in it's time to retry the transfer
      dlf.writedebug(msgs.RETRYTRANSFER, subreqid=transferId, reqid=reqid) # "Retrying transfer" message
      self.priorityQueue.put(transferId)
      del self.pendingD2dDest[transferId]
    finally:
      self.lock.release()

  def pollD2dDest(self):
    '''Checks which d2d destinations transfers waiting for sources should be retried'''
    self.lock.acquire()
    currentTime = time.time()
    toBeDeleted = []
    try:
      # loop over all pending transferIds
      for transferId in self.pendingD2dDest:
        timeOfNextTry = self.pendingD2dDest[transferId]
        if timeOfNextTry < currentTime:
          # put the transferId back into the priority queue in it's time to retry the transfer
          dlf.writedebug(msgs.RETRYTRANSFER, subreqid=transferId) # "Retrying transfer" message
          self.priorityQueue.put(transferId)
          toBeDeleted.append(transferId)
      # cleanup list of pending transferIds
      for tid in toBeDeleted:
        del self.pendingD2dDest[tid]
    finally:
      self.lock.release()

  def checkForTimeoutTransfersCancelation(self, canceledTransfers):
    '''Checks which transfers need to be canceled because they are queueing for too long'''
    # get timeouts from configuration
    timeouts = dict([entry.split(':') for entry in
                     self.config.getValue('TransferManager', 'PendingTimeouts', '').split()])
    for svcclass, timeout in timeouts.items():
      try:
        timeouts[svcclass] = int(timeout)
      except ValueError:
        del timeouts[svcclass]
        # "Invalid TransferManager/PendingTimeouts option, ignoring entry" message
        dlf.writeerr(msgs.INVALIDTIMEOUTOPTION, SvcClass=svcclass, Timeout=timeout)
    # get the disk to disk copy timeout
    d2dtimeout =  self.config.getValue('TransferManager', 'DiskCopyPendingTimeout', 7200, int)
    # get current time and diskserver status
    currenttime = time.time()
    # loop over the transfers
    self.lock.acquire()
    try:
      toberemoved = []
      for qTransfer in self.queueingTransfers.itervalues():
        transfer = qTransfer.transfer
        scheduler = qTransfer.scheduler
        # find the apropriate timeout
        if transfer.transferType == TransferType.STD:
          try:
            timeout = timeouts[transfer.svcClassName]
          except KeyError:
            try :
              timeout = timeouts['all']
            except KeyError:
              # no timeout could be found, so we take it as infinite, meaning we do not cancel anything
              timeout = -1
        else:
          timeout = d2dtimeout
        # if we found a timeout, check and cancel if needed
        if timeout >= 0 and currenttime - transfer.creationTime > timeout:
          toberemoved.append(transfer.transferId)
          if scheduler not in canceledTransfers:
            canceledTransfers[scheduler] = []
          canceledTransfers[scheduler].append((transfer.asTuple(), 1004,  # SETIMEDOUT
                                               'Timed out while queueing (timeout was %ds)' % timeout))
      if toberemoved:
        self._remove(toberemoved)
    finally:
      self.lock.release()

  def checkForTransfersCancelation(self):
    '''Checks which transfers need to be canceled'''
    canceledTransfers = {}
    # check whether transfers need to be canceled for timeouts
    self.checkForTimeoutTransfersCancelation(canceledTransfers)
    # Inform the schedulers of canceled transfers
    timeout = self.config.getValue('TransferManager', 'AdminTimeout', 5, float)
    for scheduler, transfers in canceledTransfers.iteritems():
      connectionpool.connections.transfersCanceled(scheduler, transfers, timeout=timeout)

  def FSDisabled(self, mountPoints):
    '''cancels queuing jobs when some filesystems are disabled.
       mountPoints lists the filesystems. If mountPoints is None, then the whole
       machine is considered to be disabled'''
    canceledTransfers = {}
    timeout = self.config.getValue('TransferManager', 'AdminTimeout', 5, float)
    self.lock.acquire()
    try:
      toberemoved = []
      # loop over all transfers
      for transferId, qTransfer in self.queueingTransfers.iteritems():
        # do not cancel if not requested
        if mountPoints and qTransfer.transfer.mountPoint not in mountPoints:
          continue
        # add transfer to list of transfers to be removed from this queue
        toberemoved.append(transferId)
        # add transfer to the list of transfers to be canceled in transfermanager
        if qTransfer.transfer.transferType == TransferType.D2DSRC:
          msg = "Transfer terminated, source filesystem for disk2disk copy is DISABLED"
        else: # standard, d2ddest
          msg = "Transfer terminated, all filesystems are DRAINING or DISABLED"
        if qTransfer.scheduler not in canceledTransfers:
          canceledTransfers[qTransfer.scheduler] = []
        canceledTransfers[qTransfer.scheduler].append((transferId, qTransfer.transfer.fileid, 1023, msg, qTransfer.transfer.reqId)) # SEWOULDBLOCK, Resource temporarily unavailable
        continue
      # Remove transfers locally
      self._remove(toberemoved)
    finally:
      self.lock.release()
    # Cancel transfers in transfermanagerd
    for scheduler, transfers in canceledTransfers.iteritems():
      connectionpool.connections.transfersCanceled(scheduler, tuple(transfers), timeout=timeout)

  def nbTransfers(self, reqUser=None, detailed=False):
    '''returns number of queueing transfers and number of queueing slots, plus details
    per protocol if the detailed paremeter is true.
    The exact format of the returned tuple if detailed is False is :
     (nbQueueingTransfers, nbQueueingSlots)
    If detailed is True, then it is :
     (nbQueueingTransfers, (('proto1', nbQueueingTransfersForProto1), ...),
      nbQueueingSlots, (('proto1', nbQueueingSlotsForProto1), ...)) '''
    n = 0
    nproto = {}
    ns = 0
    nsproto = {}
    self.lock.acquire()
    try:
      for qTransfer in self.queueingTransfers.values():
        protocol = qTransfer.transfer.protocol
        if qTransfer.transfer.transferType in (TransferType.D2DSRC, TransferType.D2DDST):
          user = 'stage'
        else:
          try:
            user = pwd.getpwuid(qTransfer.transfer.euid)[0]
          except KeyError:
            user = str(qTransfer.transfer.euid)
        if not reqUser or reqUser == user:
          n = n + 1
          nbslots = self.config.getValue('DiskManager', protocol+'Weight', 1, int)
          ns = ns + nbslots
          if protocol not in nproto:
            nproto[protocol] = 0
          nproto[protocol] = nproto[protocol] + 1
          if protocol not in nsproto:
            nsproto[protocol] = 0
          nsproto[protocol] = nsproto[protocol] + nbslots
    finally:
      self.lock.release()
    if detailed:
      return (n, tuple(nproto.items()), ns, tuple(nsproto.items()))
    else:
      return (n, ns)

  def listTransfers(self, reqUser=None):
    '''lists pending transfers'''
    res = []
    n = 0
    self.lock.acquire()
    try:
      for transferId, qTransfer in self.queueingTransfers.items():
        if qTransfer.transfer.transferType == TransferType.STD:
          try:
            user = pwd.getpwuid(qTransfer.transfer.euid)[0]
          except KeyError:
            user = qTransfer.transfer.euid
        else:
          user = 'stage'
        if not reqUser or user == reqUser:
          res.append((transferId, qTransfer.transfer.fileId, qTransfer.scheduler, user, 'PEND', qTransfer.transfer.protocol, qTransfer.transfer.creationTime, None))
          n = n + 1
          if n >= 100: # give up with full listing if too many transfers
            break
    finally:
      self.lock.release()
    return res

  def transferset(self):
    '''Lists all pending and running transfers'''
    self.lock.acquire()
    try:
      return set([(transferId, qTransfer.transfer.reqId) for transferId, qTransfer in self.queueingTransfers.items()])
    finally:
      self.lock.release()

  def listQueueingTransfers(self, scheduler):
    '''lists pending transfers'''
    self.lock.acquire()
    try:
      # return transferId, transfer, transfertype, arrivaltime
      return [tuple(qTransfer.transfer.items()) for qTransfer in self.queueingTransfers.itervalues()
              if qTransfer.scheduler == scheduler]
    finally:
      self.lock.release()

  def anyTransfersFromScheduler(self, reqscheduler):
    '''Tells whether any transfer is queueing that is handled by the given scheduler'''
    self.lock.acquire()
    try:
      # go through the transfer
      for qTransfer in self.queueingTransfers.values():
        # Stop whenever we find one
        if reqscheduler == qTransfer.scheduler:
          return True
      # No transfer found
      return False
    finally:
      self.lock.release()
