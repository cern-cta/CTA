#/******************************************************************************
# *                   serverqueue.py
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
# * serverqueue class of the transfer manager of the CASTOR project
# * this class is responsible for managing a queue of transfers in the server memory.
# * This queue lists all transfers pending on the different diskServers, and started
# * by a given server
# *
# * @author Castor Dev team, castor-dev@cern.ch
# *****************************************************************************/

'''serverqueue module of the CASTOR transfer manager.
Manages the transfers pending on the different diskServers'''

import threading, socket
import dlf, time
import castor_tools, diskserverlistcache, connectionpool
from transfermanagerdlf import msgs
from transfer import TransferType, tupleToTransfer

class RecentSchedules(object):
  '''This class remembers the latest schedules and is thus able to detect double scheduling.
     It actually keeps in memory the transfers of the current and lst time slots, where a slot
     is 10s long.'''

  def __init__(self):
    '''constructor'''
    # dictionnary of recently scheduled transfers and where they were scheduled
    self.schedulesInCurrentSlot = {}
    # dictionnary of transfers in the previous time slot and where they were scheduled
    self.schedulesInPreviousSlot = {}
    # start of current slot
    self.currentSlot = 0

  def add(self, transferid, diskServer):
    '''adds a new job to the recently scheduled ones. Also drops old jobs when needed'''
    # get slot of this transfer
    slot = int(time.time())/10
    # in case we start a new slot, cleanup
    if slot != self.currentSlot:
      # rotate slots
      self.schedulesInPreviousSlot = self.schedulesInCurrentSlot
      self.schedulesInCurrentSlot = {}
      # remember new slot
      self.currentSlot = slot
    # add transfer to the new slot
    self.schedulesInCurrentSlot[transferid] = diskServer

  def isDoubleScheduling(self, transferid, diskServer):
    '''Checks whether the given job has been already scheduled on the given machine recently'''
    return (transferid in self.schedulesInCurrentSlot and self.schedulesInCurrentSlot[transferid] == diskServer) or \
           (transferid in self.schedulesInPreviousSlot and self.schedulesInPreviousSlot[transferid] == diskServer)
 

class QueueingTransfer(object):
  '''little container describing a queueing transfer'''
  def __init__(self, transfer, isSrcRunning):
    '''constructor'''
    self.transfer = transfer
    self.isSrcRunning = isSrcRunning

class ServerQueue(dict):
  '''a dictionnary of queueing transfers, with the list of machines to which they were sent
  and a reverse lookup facility by machine'''

  def __init__(self):
    '''constructor'''
    dict.__init__(self)
    # a global lock for this queue
    self.lock = threading.Lock()
    # dictionnary containing the set of machines on which each transfer is queueing
    self.transfersLocations = {}
    # list of source d2d transfers pending
    self.d2dsrcpending = set()
    # list of source d2d transfers running
    self.d2dsrcrunning = {}
    # memory of recently scheduled jobs
    self.recentlyScheduled = RecentSchedules()
    # get configuration
    self.config = castor_tools.castorConf()

  def put(self, transfer, putBack=False):
    '''Adds a new transfer'''
    self.lock.acquire()
    try:
      # in case we put back a d2ddest transfer, check that source is still around
      if putBack and transfer.transferType == TransferType.D2DDST and \
         transfer.transferId not in self.d2dsrcpending and transfer.transferId not in self.d2dsrcrunning:
        # a d2ddest wants to (re)start but no source exists in the queuing nor in the running lists:
        # we probably have a race condition with a d2ddest that was put back in the queue too late
        dlf.writenotice(msgs.D2DDESTRESTARTERROR, subreqId=transfer.transferId)
        return
      # add transfer to the list of queueing transfers on the diskServer
      # note the d2dsrcrunning aregument, used only for d2ddest transfers and telling whether the source is ready
      # As it could be that the source has already started when the destinations are entered, we have
      # to infer this argument from the transferId, looking into the list of running sources
      if transfer.diskServer not in self:
        self[transfer.diskServer] = {}
      self[transfer.diskServer][transfer.transferId] = QueueingTransfer(transfer,
                                                                        transfer.transferId in self.d2dsrcrunning)
      # add the diskServer to the transfersLocations list for this transfer (except for sources)
      if transfer.transferType != TransferType.D2DSRC:
        if transfer.transferId not in self.transfersLocations:
          self.transfersLocations[transfer.transferId] = set()
        self.transfersLocations[transfer.transferId].add(transfer.diskServer)
      else:
        # in case, drop source from running job. It may be there id we agreed we its start and
        # our answer never made it to the diskServer, which then called transferBackToQueue
        if transfer.transferId in self.d2dsrcrunning:
          del self.d2dsrcrunning[transfer.transferId]
        # put back source to pending
        self.d2dsrcpending.add(transfer.transferId)
        # reset flag of the destinations that believe source is running
        if transfer.transferId in self.transfersLocations:
          for ds in self.transfersLocations[transfer.transferId]:
            self[ds][transfer.transferId].isSrcRunning = False
    finally:
      self.lock.release()

  def _removetransfer(self, transferId, transfersPerMachine):
    '''internal method removing a given transfer from the queue, adding its locations
    to the given dictionnary and calling d2dend if needed. Returns the deleted transfer.
    Note that the locking is not handled here, it is left to the responsability of the caller'''
    try:
      # get the list of machines potentially running the transfer
      machines = self.transfersLocations[transferId]
      # clean up transfersLocations
      del self.transfersLocations[transferId]
      # for each machine, cleanup the server queue and note down where the transfer was sent
      transfer = None
      for machine in machines:
        # if first one, found out fileId
        if not transfer:
          transfer = self[machine][transferId].transfer
        # note where transfer was sent
        if machine not in transfersPerMachine:
          transfersPerMachine[machine] = []
        transfersPerMachine[machine].append(transfer.transferId)
      # if we have a source transfer already running, stop it
      if transfer.transferId in self.d2dsrcrunning:
        self.d2dend(transfer, transferCancelation=True)
      # cleanup. Has to be done after the d2dend call
      for machine in machines:
        del self[machine][transfer.transferId]
      return transfer
    except KeyError:
      # we are not handling this transfer, fine, ignore it then
      return None

  def remove(self, transferIds):
    '''drops transfers from the queues and return the dropped transfers'''
    transfersPerMachine = {}
    results = []
    # first cleanup our own queue
    self.lock.acquire()
    try:
      for transferId in transferIds:
        transfer = self._removetransfer(transferId, transfersPerMachine)
        if transfer:
          results.append(transfer)
    finally:
      self.lock.release()
    # then tell the machines to remove these transfers from their queues
    for machine in transfersPerMachine:
      try:
        connectionpool.connections.killtransfers(machine, tuple(transfersPerMachine[machine]))
      except Exception:
        # ignore, we've tried, there is not much more we can do
        None
    # return
    return results

  def removeall(self, requser):
    '''drops transfers from the queues return a dictionnary "transferId => fileId" listing
    the transfers actually removed and for each of them, the concerned file'''
    transfersPerMachine = {}
    results = []
    # first cleanup our own queue
    self.lock.acquire()
    try:
      transferstodrop = []
      # Go through all transfers
      for transferId in self.transfersLocations:
        # ignore if not the right user
        if requser:
          # get user of for first location. For + break is used as set has no access to a random item
          for ds in self.transfersLocations[transferId]:
            transfer = self[ds][transferId].transfer
            break
          if transfer.user != requser:
            continue
        # else put in the list of transfers to really drop
        # note that we can not drop here as we are looping on transfersLocations that would be modified
        transferstodrop.append(transfer.transferId)
      # now remove selected transfers
      for transferId in transferstodrop:
        transfer = self._removetransfer(transferId, transfersPerMachine)
        if transfer:
          results.append(transfer)
    finally:
      self.lock.release()
    # then tell the machines to remove these transfers from their queues
    timeout = self.config.getValue('TransferManager', 'AdminTimeout', 5, float)
    for machine in transfersPerMachine:
      connectionpool.connections.killtransfers(machine, tuple(transfersPerMachine[machine]), timeout=timeout)
    # return
    return results

  def transferStarting(self, transfer):
    '''Removes a transfer and gives back the list of other nodes where it was pending.
    Raises ValueError when not found'''
    if transfer.transferType == TransferType.D2DSRC:
      self.lock.acquire()
      try:
        # the source transfer is starting, mark all destinations (and the source) ready
        try :
          # check whether the source transfer was canceled in the meantime
          if transfer.transferId not in self.d2dsrcpending:
            # source was canceled. This only happens when all desctinations were canceled too
            # log 'denying start of source transfer as it has been canceled'
            dlf.writedebug(msgs.TRANSFERSRCCANCELED, DiskServer=transfer.diskServer,
                           subreqId=transfer.transferId, reqId=transfer.reqId)
            raise ValueError
          # remember where the source is running
          self.d2dsrcrunning[transfer.transferId] = transfer.diskServer
          self.d2dsrcpending.remove(transfer.transferId)
          # and make the destinations (if any) aware that it can now start this transfer
          for ds in self.transfersLocations[transfer.transferId]:
            self[ds][transfer.transferId].isSrcRunning = True
            try:
              connectionpool.connections.retryD2dDest(ds, transfer.transferId, transfer.reqId)
            except Exception:
              # not a big deal, the destination will retry it soon by itself
              pass
        except KeyError:
          # no destination yet, no problem, the source was only too fast to start
          pass
      finally:
        self.lock.release()
    else:
      self.lock.acquire()
      try:
        # check whether the transfer has already been started somewhere else
        if transfer.transferId not in self.transfersLocations or \
           transfer.transferId not in self[transfer.diskServer]:
          # this transfer is supposed to be running. Let's check that it is not suppose to be
          # running on the very machine that wants to start it. That would mean that our answer
          # to this machine never arrived and the machine is retrying
          if self.recentlyScheduled.isDoubleScheduling(transfer.transferId, transfer.diskServer):
            # we are precisely in the mentionned case. We can safely return as we already think
            # that the job is running on that machine
            # "Transfer starting reconfirmed" message
            dlf.writedebug(msgs.TRANSFERSTARTCONFIRMED, DiskServer=transfer.diskServer,
                           subreqId=transfer.transferId, reqId=transfer.reqId)
            return
          # The transfer has really started somewhere else. Let the diskServer know by raising an exception
          # "Transfer had already started. Cancel start" message
          dlf.writedebug(msgs.TRANSFERALREADYSTARTED, DiskServer=transfer.diskServer,
                         subreqId=transfer.transferId, reqId=transfer.reqId)
          raise ValueError
        # We are now sure that the transfer has not yet started
        # if a destination transfer wants to start, check whether the source is ready
        if transfer.transferType == TransferType.D2DDST and \
           not self[transfer.diskServer][transfer.transferId].isSrcRunning:
          # "Source is not ready yet" message
          dlf.writedebug(msgs.SOURCENOTREADY, DiskServer=transfer.diskServer,
                         subreqId=transfer.transferId, reqId=transfer.reqId)
          raise EnvironmentError
        # this time, the transfer can start.
        # drop the transfer from all queues. Note that this desynchronizes the queues as seen
        # by the diskServers from the queues seen by the central manager. In case of a
        # restart of a diskServer manager, its queue will be magically "cleaned up"
        # However, the diskServers concerned will be notified.
        machines = self.transfersLocations[transfer.transferId]
        del self.transfersLocations[transfer.transferId]
        for machine in machines:
          del self[machine][transfer.transferId]
        # Add this transfer to the recently scheduled ones
        self.recentlyScheduled.add(transfer.transferId, transfer.diskServer)
      finally:
        self.lock.release()
      # inform all other machines that this job has been handled
      for machine in machines:
        if machine != transfer.diskServer:
          try:
            # "Informing diskServer that job started somewhere else" message
            dlf.writedebug(msgs.INFODSJOBSTARTED, DiskServer=machine,
                           subreqId=transfer.transferId, reqId=transfer.reqId)
            connectionpool.connections.transferAlreadyStarted(machine, transfer.transferId, transfer.reqId)
          except Exception, e:
            # "Failed to inform diskServer that job started elsewhere" message
            dlf.writenotice(msgs.INFODSJOBSTARTEDFAILED, DiskServer=machine,
                            subreqId=transfer.transferId, reqId=transfer.reqId,
                            Type=str(e.__class__), Message=str(e))

  def d2dend(self, transfer, transferCancelation=False):
    '''called when a d2d copy ends in order to inform the source.
    When called for a transfer cancelation, second parameter should be
    true so that we do not take the lock again and do not cleanup twice'''
    if not transferCancelation:
      self.lock.acquire()
    try:
      if transfer.transferId not in self.d2dsrcrunning:
        # the transfer has already disappeared (or was never started). This
        # can happen due to raise conditions, e.g when we get a timeout on the
        # start of the transfer when it has started already but the acknoledgement
        # did not yet come.
        # We can ignore these cases, but we still log
        # "Unable to end d2d as it's not in the server list. Probable race condition" message
        dlf.writewarning(msgs.D2DENDEXCEPTION, subreqId=transfer.transferId, reqId=transfer.reqId)
        # in case, discard the src transfer from the pending list
        self.d2dsrcpending.discard(transfer.transferId)
        return
      # get the source location
      diskServer = self.d2dsrcrunning[transfer.transferId]
      # remember the transfer
      qTransfer = self[diskServer][transfer.transferId]
      # remove d2dsrc transfer from the queue
      if not transferCancelation:
        del self[diskServer][transfer.transferId]
      # remove from list of d2dsrcs
      del self.d2dsrcrunning[transfer.transferId]
    finally:
      if not transferCancelation:
        self.lock.release()
    # inform the source diskServer
    try:
      connectionpool.connections.d2dend(diskServer, transfer.asTuple())
    except Exception, e:
      # "Failed to inform diskServer that a d2d copy is over"
      dlf.writenotice(msgs.D2DOVERINFORMFAILED, DiskServer=diskServer, subreqId=transfer.transferId,
                      reqId=transfer.reqId, fileId=transfer.fileId, Type=str(e.__class__), Message=str(e))
      # add back the transfer to the local lists
      if not transferCancelation:
        self.lock.acquire()
      try:
        self[diskServer][qTransfer.transferId] = qTransfer
        self.d2dsrcrunning[qTransfer.transferId] = diskServer
      finally:
        if not transferCancelation:
          self.lock.release()


  def putRunningD2dSource(self, diskServer, transfer):
    '''Adds a new d2dsrc transfer to the list of runnign ones'''
    self.lock.acquire()
    try:
      # add transfer to the list of running d2dsrc transfers on the diskServer
      if diskServer not in self:
        self[diskServer] = {}
      self[diskServer][transfer.transferId] = QueueingTransfer(transfer, 'True')
      self.d2dsrcrunning[transfer.transferId] = diskServer
    finally:
      self.lock.release()

  def _isTransferMatchingAndGetPool(self, transferId, diskServerList, reqdiskpool=None, reqUser=None):
    '''checks whether a given transfer matches the given filter on diskpool and user'''
    # pick a random machine (we loop and break straight, due to lack of "getrandomitem" on a set
    for diskServer in self.transfersLocations[transferId]:
      # get diskpool
      try:
        diskpool = diskServerList.getDiskServerPool(diskServer)
        # are we interested in this diskpool ?
        if reqdiskpool and reqdiskpool != diskpool:
          return False
      except KeyError:
        # no diskServer found with this name. It must have disappeared while the associated
        # transfer was queuein. Let's use the diskpool '???'
        if reqdiskpool:
          return False
        diskpool = '???'
      # are we interested in this user ?
      if reqUser:
        if reqUser != self[diskServer][transferId].transfer.user:
          return False
      break
    # transfer matches the filter
    return diskpool

  def listTransfers(self, reqdiskpool=None, reqUser=None):
    '''lists pending transfers'''
    res = []
    self.lock.acquire()
    try:
      # for each transfer
      for transferId in self.transfersLocations:
        # check that we are interested in it
        diskpool = self._isTransferMatchingAndGetPool(transferId, diskserverlistcache.diskServerList,
                                                      reqdiskpool, reqUser)
        if diskpool:
          # go through the different instances of this transfer
          for diskServer in self.transfersLocations[transferId]:
            # get information about the transfer
            transfer = self[diskServer][transferId].transfer
            try:
              protocol = transfer.protocol
            except AttributeError:
              protocol = '-'
            # add the transfer to list of results
            res.append((transferId, transfer.fileId, socket.getfqdn(),
                        transfer.user, 'PEND', diskpool, diskServer,
                        protocol, transfer.arrivaltime, None))
    finally:
      self.lock.release()
    return res


  def nbTransfersPerPool(self, reqdiskpool=None, requser=None):
    '''returns the number of unique transfers pending on the pool given (or all pools)
    for the given user (or all users)'''
    res = {}
    self.lock.acquire()
    try:
      # for each transfer
      for transferId in self.transfersLocations:
        # check that we are interested in it
        diskpool = self._isTransferMatchingAndGetPool(transferId, diskserverlistcache.diskServerList,
                                                      reqdiskpool, requser)
        if diskpool:
          # increase counter for corresponding diskpool
          if diskpool not in res:
            res[diskpool] = 0
          res[diskpool] = res[diskpool] + 1
    finally:
      self.lock.release()
    return res

  def listQueueingTransfers(self, diskServer):
    '''This is called by the scheduler when rebuilding the list of queueing transfers for a given diskServer'''
    if diskServer in self:
      self.lock.acquire()
      try:
        res = [qTransfer.transfer.asTuple() for qTransfer in self[diskServer].values()
               if (qTransfer.transfer.transferId not in self.d2dsrcrunning) or
               self.d2dsrcrunning[qTransfer.transfer.transferId] != diskServer]
        return res
      finally:
        self.lock.release()
    else:
      return []

  def listAllRunningD2dSources(self):
    '''lists all running d2dsrc transfers as a tuple of (transferId,reqId,fileId) tuples'''
    res = []
    self.lock.acquire()
    try:
      for transferId in self.d2dsrcrunning:
        diskServer = self.d2dsrcrunning[transferId]
        transfer = self[diskServer][transferId].transfer
        res.append((transferId, transfer.reqId, transfer.fileId))
      return tuple(res)
    finally:
      self.lock.release()

  def listRunningD2dSources(self, diskServer=None):
    '''lists running d2dsrc transfers for a given diskServer'''
    res = []
    self.lock.acquire()
    try:
      for transferId in [transferId for transferId in self.d2dsrcrunning
                         if self.d2dsrcrunning[transferId] == diskServer]:
        qTransfer = self[diskServer][transferId]
        res.append((qTransfer.transfer.asTuple(), qTransfer.startTime))
    finally:
      self.lock.release()
    return res

  def transfersCanceled(self, transfers):
    '''cancels transfers in the queues and informs the stager in case there is no
    remaining machines where it could run'''
    transfersKilled = []
    self.lock.acquire()
    try:
      # for each transfer
      for transferTuple, errorCode, errorMsg in transfers:
        transfer = tupleToTransfer(transferTuple)
        dlf.writedebug(msgs.INVOKINGTRANSFERSCANCELED, DiskServer=transfer.diskServer,
                       subreqId=transfer.transferId, reqId=transfer.reqId, fileid=transfer.fileId,
                       ErrorCode=errorCode, ErrorMessage=errorMsg)
        if transfer.transferId not in self.transfersLocations:
          # the transfer has already disappeared (or was never started). This
          # can happen due to raise conditions, e.g when we get a timeout on the
          # start of the transfer when it has started already but the acknowledgement
          # did not yet come.
          # We can ignore these cases, but we still log
          # "Unable to cancel transfer as it's not in the transfer list. Probable race condition" message
          dlf.writewarning(msgs.TRANSFERCANCELEXCEPTION, subreqId=transfer.transferId,
                           reqId=transfer.reqId, fileId=transfer.fileId)
          continue
        # cleanup the queue for the given transfer on the given machine
        try:
          self.transfersLocations[transfer.transferId].remove(transfer.diskServer)
        except KeyError:
          # "Unable to cancel transfer as it's not in the transfer list. Probable race condition" message
          dlf.writewarning(msgs.TRANSFERCANCELEXCEPTION, subreqId=transfer.transferId,
                           reqId=transfer.reqId, fileId=transfer.fileId, machine=transfer.diskServer)
          continue
        if not self.transfersLocations[transfer.transferId]:
          # no other candidate machine for this transfer. It has to be failed
          transfersKilled.append((transfer.transferId, transfer.fileId[1], errorCode, errorMsg, transfer.reqId))
          # clean up _transfersLocations
          del self.transfersLocations[transfer.transferId]
          # if we have a source transfer already running, stop it
          if transfer.transferId in self.d2dsrcrunning:
            self.d2dend(transfer, transferCancelation=True)
          else:
            # if we have a source transfer pending, drop it
            self.d2dsrcpending.discard(transfer.transferId)
        # drop the transfer id from the machine queue
        del self[transfer.diskServer][transfer.transferId]
    finally:
      self.lock.release()
    # inform the stager of the transfers that were killed
    return transfersKilled

  def transfersStartingFailed(self, transfers):
    '''Amend the list of locations for transfers that could finally not be started on some machines.
    Returns a boolean saying whether the transfer is definitely failed or not'''
    ret = False
    self.lock.acquire()
    try:
      for transfer in transfers:
        # it could happen that the transfer has already been started on another machine
        # and is already gone. We can safely ignore
        if transfer.transferId in self.transfersLocations:
          # Remove the machine where the transfer could not start from
          # the transfer's locations and machine's queues
          self.transfersLocations[transfer.transferId].remove(transfer.diskServer)
          del self[transfer.diskServer][transfer.transferId]
          if not self.transfersLocations[transfer.transferId]:
            # no other candidate machine for this transfer. It has to be failed
            ret = True
            # clean up _transfersLocations
            del self.transfersLocations[transfer.transferId]
            # if we have a source transfer already running, stop it
            if transfer.transferId in self.d2dsrcrunning:
              self.d2dend(transfer, transferCancelation=True)
    finally:
      self.lock.release()
    return ret
