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
# * @(#)$RCSfile: castor_tools.py,v $ $Revision: 1.9 $ $Release$ $Date: 2009/03/23 15:47:41 $ $Author: sponcec3 $
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
import socket
import threading
import dlf
import castor_tools
from diskmanagerdlf import msgs

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

  def __init__(self, connections):
    '''constructor'''
    Queue.Queue.__init__(self)
    # connection pool to be used for sending messages to schedulers
    self.connections = connections
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

  def put(self, scheduler, transferid, transfer, transfertype, arrivaltime):
    '''Put new transfer in the queue'''
    self.lock.acquire()
    try:
      # first keep note of the new transfer (index by subreqId)
      self.queueingTransfers[transferid] = (scheduler, transfer, transfertype, arrivaltime)
      # then add it to the underlying queue
      Queue.Queue.put(self, transferid)
    finally:
      self.lock.release()

  def putPriority(self, scheduler, transferid, transfer, transfertype, arrivaltime):
    '''Put new transfer in the queue'''
    self.lock.acquire()
    try:
      # first keep note of the new transfer (index by subreqId)
      self.queueingTransfers[transferid] = (scheduler, transfer, transfertype, arrivaltime)
      # then add it to the underlying priority queue
      self.priorityQueue.put(transferid)
    finally:
      self.lock.release()

  def get(self):
    '''get a transfer from the queue. Times out after 1s'''
    found = False
    while not found:
      # try to get a priority transfer first
      try :
        transferid = self.priorityQueue.get(False)
      except Queue.Empty:
        # else get next transfer from the regular queue
        # timeout after 1s so that we can go back to the priority queue
        try:
          transferid = Queue.Queue.get(self, timeout=1)
        except Queue.Empty:
          return None, None, None, None, None
      self.lock.acquire()
      try:
        try:
          # remove it from the list of pending transfers
          scheduler, transfer, transfertype, arrivalTime = self.queueingTransfers[transferid]
          del self.queueingTransfers[transferid]
          found = True
        except KeyError:
          # transfer not found, meaning it was canceled
          # ignore and go to next one
          pass
      finally:
        self.lock.release()
    # return
    return scheduler, transferid, transfer, transfertype, arrivalTime

  def _remove(self, transferids):
    '''internal method removing transfers from the queue.
    Takes no lock so it is supposed to be called in a safe context'''
    for transferid in transferids:
      try:
        # we only remove from the queueingTransfers dictionnary
        # the transfers stay in the queue but will be ignored when poped
        del self.queueingTransfers[transferid]
      except KeyError:
        # transfer not found, either not running here or already canceled
        pass

  def remove(self, transferids):
    '''remove transfers from the queue'''
    # Loop through the transfers to remove
    self.lock.acquire()
    try:
      self._remove(transferids)
    finally:
      self.lock.release()

  def d2dDestReady(self, scheduler, transferid, transfer, arrivaltime, transfertype):
    '''called when a d2ddest transfer could not start as the source is not ready'''
    self.lock.acquire()
    try:
      # compute next time when we should try to start this transfer. We will actually wait
      # as much as has already passed between the arrival of this request and now,
      # caped to the MaxRetryInterval
      currentTime = time.time()
      timeToNextTry = currentTime - arrivaltime
      maxTime = self.config.getValue('DiskManager', 'MaxRetryInterval', 300, int)
      if timeToNextTry > maxTime:
        timeToNextTry = maxTime
      # and put the transfer into the list of pending ones
      self.queueingTransfers[transferid] = (scheduler, transfer, transfertype, arrivaltime)
      self.pendingD2dDest[transferid] = (transfer[2], currentTime + timeToNextTry)
    finally:
      self.lock.release()

  def retryD2dDest(self, transferid, reqid):
    '''retries a given d2ddest job that was on hold because the source is not available '''
    self.lock.acquire()
    try:
      # put the transferid back into the priority queue in it's time to retry the transfer
      dlf.writedebug(msgs.RETRYTRANSFER, subreqid=transferid, reqid=reqid) # "Retrying transfer" message
      self.priorityQueue.put(transferid)
      del self.pendingD2dDest[transferid]
    finally:
      self.lock.release()

  def pollD2dDest(self):
    '''Checks which d2d destinations transfers waiting for sources should be retried'''
    self.lock.acquire()
    currentTime = time.time()
    toBeDeleted = []
    try:
      # loop over all pending transferids
      for transferid in self.pendingD2dDest:
        reqid, timeOfNextTry = self.pendingD2dDest[transferid]
        if timeOfNextTry < currentTime:
          # put the transferid back into the priority queue in it's time to retry the transfer
          dlf.writedebug(msgs.RETRYTRANSFER, subreqid=transferid, reqid=reqid) # "Retrying transfer" message
          self.priorityQueue.put(transferid)
          toBeDeleted.append(transferid)
      # cleanup list of pending transferids
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
      for transferid, (scheduler, transfer, transfertype, arrivaltime) in self.queueingTransfers.iteritems():
        # find the apropriate timeout
        if transfertype == 'standard':
          try:
            svcclass = transfer[-5]
            timeout = timeouts[svcclass]
          except KeyError:
            try :
              timeout = timeouts['all']
            except KeyError:
              # no timeout could be found, so we take it as infinite, meaning we do not cancel anything
              timeout = None
        else:
          timeout = d2dtimeout
        # if we found a timeout, check and cancel if needed
        if timeout >= 0 and currenttime - arrivaltime > timeout:
          toberemoved.append(transferid)
          if scheduler not in canceledTransfers:
            canceledTransfers[scheduler] = []
          fileid = (transfer[8], int(transfer[6]))
          canceledTransfers[scheduler].append((transferid, fileid, 1004, 'Timed out while queueing (timeout was ' + str(timeout) + 's)', transfer[2])) # SETIMEDOUT
      if toberemoved:
        self._remove(toberemoved)
    finally:
      self.lock.release()

  def checkForDisabledHardwareTransfersCancelation(self, canceledTransfers):
    '''Checks which transfers need to be canceled because hardware has been disabled'''
    # get the hardware status
    try :
      state = dict([entry.rstrip().split('=') for entry in open('/etc/castor/status').readlines()])
    except Exception:
      # could not get the status, let's have it empty, meaning disabled
      state = {}
    # loop over the transfers
    self.lock.acquire()
    try:
      toberemoved = []
      for transferid, transfertuple in self.queueingTransfers.iteritems():
        scheduler, transfer, transfertype = transfertuple[:3]
        # get the concerned fileSystem
        filesystem = transfer[-1].split(':')[1]
        try:
          # find out whether the transfer should be canceled
          cancelTransfer = not ((state['DiskServerStatus'] == 'DISKSERVER_PRODUCTION' and
                          state[filesystem] == 'FILESYSTEM_PRODUCTION') or
                         (transfertype == 'd2dsrc' and
                          state['DiskServerStatus'] in ('DISKSERVER_PRODUCTION', 'DISKSERVER_DRAINING') and
                          state[filesystem] in ('FILESYSTEM_PRODUCTION', 'FILESYSTEM_DRAINING')))
        except ValueError:
          # no state means disable
          cancelTransfer = True
        if cancelTransfer:
          toberemoved.append(transferid)
          if scheduler not in canceledTransfers:
            canceledTransfers[scheduler] = []
          if transfertype == 'd2dsrc':
            msg = "Transfer terminated, source filesystem for disk2disk copy is DISABLED"
          else: # standard, d2ddest
            msg = "Transfer terminated, all filesystems are DRAINING or DISABLED"
          fileid = (transfer[8], int(transfer[6]))
          canceledTransfers[scheduler].append((transferid, fileid, 1023, msg, transfer[2])) # SEWOULDBLOCK, Resource temporarily unavailable
          continue
      self._remove(toberemoved)
    finally:
      self.lock.release()

  def checkForTransfersCancelation(self):
    '''Checks which transfers need to be canceled'''
    canceledTransfers = {}
    # check whether transfers need to be canceled for timeouts
    self.checkForTimeoutTransfersCancelation(canceledTransfers)
    # All further processing requires resource killing to be active.
    if self.config.getValue('TransferManager', 'KillRequests', 'yes').lower() != 'no':
      # check whether transfers need to be canceled when hardware gets disabled
      self.checkForDisabledHardwareTransfersCancelation(canceledTransfers)
    # Inform the schedulers of canceled transfers
    timeout = self.config.getValue('TransferManager', 'AdminTimeout', 5, float)
    for scheduler, transfers in canceledTransfers.iteritems():
      self.connections.transfersCanceled(scheduler, socket.getfqdn(), tuple(transfers), timeout=timeout)

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
      for transfertuple in self.queueingTransfers.values():
        rawtransfer = transfertuple[1]
        transfertype = transfertuple[2]
        if transfertype in ('d2dsrc', 'd2ddest'):
          protocol = transfertype
          user = 'stage'
        else:
          protocol = rawtransfer[10]
          try:
            user = pwd.getpwuid(int(rawtransfer[20]))[0]
          except KeyError:
            user = rawtransfer[20]
        if not reqUser or reqUser == user:
          n = n + 1
          nbslots = self.config.getValue('DiskManager', protocol+'Weight', None, int)
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
      for transfer in self.queueingTransfers.items():
        scheduler, rawtransfer, transfertype, arrivalTime = transfer[1]
        if transfertype in ('d2dsrc', 'd2ddest'):
          protocol = transfertype
        else:
          protocol = rawtransfer[10]
        if transfertype == 'standard':
          try:
            user = pwd.getpwuid(int(rawtransfer[20]))[0]
          except KeyError:
            user = rawtransfer[20]
        else:
          user = 'stage'
        if not reqUser or user == reqUser:
          res.append((transfer[0], rawtransfer[6], scheduler, user, 'PEND', protocol, rawtransfer, arrivalTime, None))
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
      return set([(transferid, transfer[1][2]) for transferid, transfer in self.queueingTransfers.items()])
    finally:
      self.lock.release()

  def listQueueingTransfers(self):
    '''lists pending transfers'''
    self.lock.acquire()
    try:
      # return transferid, transfer, transfertype, arrivaltime
      return [(transferid, transfertuple[1], transfertuple[2], transfertuple[3]) for transferid, transfertuple in self.queueingTransfers.iteritems()]
    finally:
      self.lock.release()

  def anyTransfersFromScheduler(self, reqscheduler):
    '''Tells whether any transfer is queueing that is handled by the given scheduler'''
    self.lock.acquire()
    try:
      # go through the transfer
      for transfertuple in self.queueingTransfers.values():
        # Stop whenever we find one
        if reqscheduler == transfertuple[0]:
          return True
      # No transfer found
      return False
    finally:
      self.lock.release()
