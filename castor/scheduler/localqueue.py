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

import pwd
import time
import Queue
import socket
import threading
import dlf
from diskmanagerdlf import msgs

class LocalQueue(Queue.Queue):
  '''Class managing a queue of pending transfers.
  There are actually 2 queues, a dictionnary and a set involved :
    - the main queue is the object itself and holds all transfers that were not considered so far.
    - _pendingD2dDest is a list of disk2disk destinations transfers that have been considered but could not
      be started because the source was not ready. They will be retried regularly until the source
      becomes ready. They are stored together with the next time when to retry. The interval between
      retries is equal to half the time gone since the arrival of the transfer, with a maximum
      configured in castor.conf (DiskManager/MaxRetryInterval).
    - _priorityQueue is a queue of transfers to be started as soon as possible. These transfers are the disk2disk
      destination transfers that have been pending on the resdiness of the source and can now be started
    - finally, all transfers handled are indexed in a dictionnary called _queueingTransfers handling detailed
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
    self._queueingTransfers = {}
    # a global lock for the dictionnary
    self._lock = threading.Lock()
    # set of pending disk2disk destinations
    self._pendingD2dDest = []
    # set of transfers to start with highest priority
    self._priorityQueue = Queue.Queue()

  def put(self, scheduler, transferid, transfer, transfertype, arrivaltime):
    '''Put new transfer in the queue'''
    self._lock.acquire()
    try:
      # first keep note of the new transfer (index by subreqId)
      self._queueingTransfers[transferid] = (scheduler, transfer, transfertype, arrivaltime)
      # then add it to the underlying queue
      Queue.Queue.put(self,transferid)
    finally:
      self._lock.release()

  def priorityPut(self, scheduler, transferid, transfer, transfertype, arrivaltime):
    '''Put new transfer in the priority queue'''
    self._lock.acquire()
    try:
      # first keep note of the new transfer (index by subreqId)
      self._queueingTransfers[transferid] = (scheduler, transfer, transfertype, arrivaltime)
      # then add it to the underlying queue
      self._priorityQueue.put(transferid)
    finally:
      self._lock.release()

  def get(self):
    '''get a transfer from the queue'''
    found = False
    while not found:
      # try to get a priority transfer first
      try :
        transferid = self._priorityQueue.get(False)
      except Queue.Empty:
        # else get next transfer from the regular queue
        # timeout after 1s so that we can go back to the priority queue
        try:
          transferid = Queue.Queue.get(self, timeout=1)
        except Queue.Empty:
          continue
      self._lock.acquire()
      try:
        try:
          # remove it from the list of pending transfers
          scheduler, transfer, transfertype, arrivalTime = self._queueingTransfers[transferid]
          del self._queueingTransfers[transferid]
          found = True
        except KeyError:
          # transfer not found, meaning it was canceled
          # ignore and go to next one
          pass
      finally:
        self._lock.release()
    # return
    return scheduler, transferid, transfer, transfertype, arrivalTime

  def remove(self, transferids):
    '''remove transfers from the queue'''
    # Loop through the transfers to remove
    self._lock.acquire()
    try:
      for transferid in transferids:
        try:
          # we only remove from the _queueingTransfers dictionnary
          # the transfers stay in the queue but will be ignored when poped
          del self._queueingTransfers[transferid]
        except KeyError:
          # transfer not found, either not running here or already canceled
          pass
    finally:
      self._lock.release()

  def d2dDestReady(self, scheduler, transferid, transfer, arrivaltime, transfertype):
    '''called when a d2ddest transfer could not start as the source is not ready'''
    self._lock.acquire()
    try:
      # compute next time when we should try to start this transfer
      currentTime = time.time()
      timeToNextTry = (currentTime-arrivaltime)/2
      maxTime = self.config.getValue('DiskManager', 'MaxRetryInterval', 300, int)
      if timeToNextTry > maxTime: timeToNextTry = maxTime
      # and put the transfer into the list of pending ones
      self._queueingTransfers[transferid] = (scheduler, transfer, transfertype, arrivaltime)
      self._pendingD2dDest.append((transferid, currentTime + timeToNextTry))
    finally:
      self._lock.release()

  def pollD2dDest(self):
    '''Checks which d2d destinations transfers waiting for sources should be retried'''
    self._lock.acquire()
    currentTime = time.time()
    toBeDeleted = []
    try:
      # loop over all pending transferids
      for i in range(len(self._pendingD2dDest)):
        transferid, timeOfNextTry = self._pendingD2dDest[i]
        if timeOfNextTry < currentTime:
          # put the transferid back into the priority queue in it's time to retry the transfer
          dlf.writedebug(msgs.RETRYTRANSFER, subreqid=transferid) # "Retrying transfer" message
          self._priorityQueue.put(transferid)
          toBeDeleted.append(transferid)
      # cleanup list of pending transferids
      self._pendingD2dDest = [(transferid, nextTry) for transferid, nextTry in self._pendingD2dDest if transferid not in toBeDeleted]
    finally:
      self._lock.release()

  def checkForTimeoutTransfersCancelation(self, canceledTransfers):
    '''Checks which transfers need to be canceled because they are queueing for too long'''
    # get timeouts from configuration
    timeouts = dict([entry.split(':') for entry in
                     self.config.getValue('JobManager', 'PendingTimeouts', '').split()])
    for svcclass, timeout in timeouts:
      try:
        timeouts[svcclass] = int(timeout)
      except ValueError:
        del timeouts[svcclass]
        # "Invalid JobManager/PendingTimeouts option, ignoring entry" message
        dlf.writeerr(msgs.INVALIDTIMEOUTOPTION, svcclass=svcclass, timeout=timeout)
    # get the disk to disk copy timeout
    d2dtimeout =  self.config.getValue('JobManager', 'DiskCopyPendingTimeout', None, int)
    # get current time and diskserver status
    currenttime = time.time()
    # loop over the transfers
    toberemoved = []
    for transferid, (scheduler, transfer, transfertype, arrivaltime) in self._queueingTransfers.iteritems():
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
        if scheduler not in canceledTransfers: canceledTransfers[scheduler] = []
        fileid = (transfer[8], int(transfer[6]))
        canceledTransfers[scheduler].append((transferid, fileid, 1004, 'Timed out while queueing (timeout was ' + str(timeout) + 's')) # SETIMEDOUT
    if toberemoved:
      self.remove(toberemoved)

  def checkForDisabledHardwareTransfersCancelation(self, canceledTransfers):
    '''Checks which transfers need to be canceled because hardware has been disabled'''
    # get the hardware status
    try :
      state = dict([entry.rstrip().split('=') for entry in open('/etc/castor/status').readlines()])
    except:
      # could not get the status, let's have it empty, meaning disabled
      state = {}
    # loop over the transfers
    toberemoved = []
    for transferid, (scheduler, transfer, transfertype, arrivaltime) in self._queueingTransfers.iteritems():
      # get the concerned fileSystem
      filesystem = transfer[-1].split(':')[1]
      try:
        # find out whether the transfer should be canceled
        cancelTransfer = not ((state['DiskServerStatus'] == 'DISKSERVER_PRODUCTION' and
                        state[filesystem] == 'FILESYSTEM_PRODUCTION') or
                       (transfertype == 'd2dsource' and
                        state['DiskServerStatus'] in ('DISKSERVER_PRODUCTION', 'DISKSERVER_DRAINING') and
                        state[filesystem] in ('FILESYSTEM_PRODUCTION', 'FILESYSTEM_DRAINING')))
      except ValueError:
        # no state means disable
        cancelTransfer = True
      if cancelTransfer:
        toberemoved.append(jobid)
        if scheduler not in canceledTransfers: canceledTransfers[scheduler] = []
        if transfertype == 'd2dsource':
          msg = "Transfer terminated, source filesystem for disk2disk copy is DISABLED"
        else: # standard, d2ddest
          msg = "Transfer terminated, all filesystems are DRAINING or DISABLED"
        fileid = (transfer[8], int(transfer[6]))
        CANCELEDTRANSFERS[scheduler].append((transferid, fileid, 1023, msg)) # SEWOULDBLOCK, Resource temporarily unavailable
        continue
    self.remove(toberemoved)

  def checkForTransfersCancelation(self):
    '''Checks which transfers need to be canceled'''
    canceledTransfers = {}
    # check whether transfers need to be canceled for timeouts
    self.checkForTimeoutTransfersCancelation(canceledTransfers)
    # All further processing requires resource killing to be active.
    if self.config.getValue('JobManager','ResReqKill','yes').lower() != 'no':
      # check whether transfers need to be canceled when hardware gets disabled
      self.checkForDisabledHardwareTransfersCancelation(canceledTransfers)
    # Inform the schedulers of canceled transfers
    for scheduler, transfers in canceledTransfers.iteritems():
      self.connections.transfersCanceled(scheduler, socket.gethostname(), transfers)

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
    for scheduler, rawtransfer, transfertype, arrivalTime in self._queueingTransfers.items():
      if transfertype == 'd2dsource':
        protocol = 'd2dsrc'
        user = 'stage'
      elif transfertype == 'd2ddest':
        protocol = 'd2ddest'
        user = 'stage'
      else:
        protocol = rawtransfer[10]
        try:
          user = pwd.getpwuid(int(rawtransfer[20]))[0]
        except KeyError:
          user = rawtransfer[20]
      if not reqUser or reqUser == user:
        n = n + 1
        ns = ns + self.config.getValue('DiskManager', protocol+'Weight', None, int)
        if protocol not in nproto: nproto[protocol] = 0
        nproto[protocol] = nproto[protocol] + 1
        if protocol not in nsproto: nsproto[protocol] = 0
        nsproto[protocol] = nsproto[protocol] + 1
    if detailed:
      return n, tuple(nproto.items()), ns, tuple(nsproto.items())
    else:
      return n, ns

  def listTransfers(self, reqUser=None):
    '''lists pending transfers'''
    res = []
    n = 0
    for transfer in self._queueingTransfers.items():
      scheduler, rawtransfer, transfertype, arrivalTime = transfer[1]
      if transfertype == 'standard':
        try:
          user = pwd.getpwuid(int(rawtransfer[20]))[0]
        except KeyError:
          user = rawtransfer[20]
      else:
        user = 'stage'
      if not reqUser or user == reqUser:
        res.append((transfer[0], scheduler, user, 'PEND', transfertype, arrivalTime, None))
        n = n + 1
        if n >= 1000: # give up with full listing if too many transfers
          break
    return res

  def transferset(self):
    '''Lists all pending and running transfers'''
    return set(self._queueingTransfers.keys())

  def listQueueingTransfers(self):
    '''lists pending transfers'''
    return [tuple([transfer[0]])+transfer[1][1:4] for transfer in self._queueingTransfers.items()]
