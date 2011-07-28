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
# * This queue lists all transfers pending on the different diskservers, and started
# * by a given server
# *
# * @author Castor Dev team, castor-dev@cern.ch
# *****************************************************************************/

'''serverqueue module of the CASTOR transfer manager.
Manages the transfers pending on the different diskservers'''

import threading
import dlf, pwd, time
import castor_tools
from transfermanagerdlf import msgs

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

  def add(self, transferid, diskserver):
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
    self.schedulesInCurrentSlot[transferid] = diskserver

  def isDoubleScheduling(self, transferid, diskserver):
    '''Checks whether the given job has been already scheduled on the given machine recently'''
    return (transferid in self.schedulesInCurrentSlot and self.schedulesInCurrentSlot[transferid] == diskserver) or \
           (transferid in self.schedulesInPreviousSlot and self.schedulesInPreviousSlot[transferid] == diskserver)
 

class ServerQueue(dict):
  '''a dictionnary of queueing transfers, with the list of machines to which they were sent
  and a reverse lookup facility by machine'''

  def __init__(self, connections):
    '''constructor'''
    dict.__init__(self)
    # a pool of connections to the diskservers
    self.connections = connections
    # a global lock for this queue
    self.lock = threading.Lock()
    # dictionnary containing the set of machines on which each transfer is queueing
    self.transfersLocations = {}
    # list of source d2d transfers running
    self.d2dsrcrunning = {}
    # memory of recently scheduled jobs
    self.recentlyScheduled = RecentSchedules()
    # get configuration
    self.config = castor_tools.castorConf()

  def put(self, transferid, arrivaltime, transferList, transfertype='standard'):
    '''Adds a new transfer. transfertype can be one of 'standard', 'd2dsrc' and 'd2ddest' '''
    self.lock.acquire()
    try:
      for diskserver, transfer in transferList:
        # add transfer to the list of queueing transfers on the diskserver
        # note the extra argument, used only for d2ddest transfers and telling whether the source is ready
        # As it could be that the source has already started when the destinations are entered, we have
        # to infer this argument from the transferid, looking into the list of running sources
        if diskserver not in self:
          self[diskserver] = {}
        self[diskserver][transferid] = [transfer, arrivaltime, transfertype, transferid in self.d2dsrcrunning]
        # add the diskserver to the transferLocations list for this transfer (except for sources)
        if transfertype != 'd2dsrc':
          if transferid not in self.transfersLocations:
            self.transfersLocations[transferid] = set()
          self.transfersLocations[transferid].add(diskserver)
    finally:
      self.lock.release()

  def _removetransfer(self, transferid, transfersPerMachine):
    '''internal method removing one given transfer from the queue, adding its locations
    to the given dictionnary and calling d2dend if needed. Also returns the fileid of the
    file concerned by the transfer for logging reasons.
    Not that the locking is not handled here, it is left to the responsability of the caller'''
    try:
      # get the list of machines potentially running the transfer
      machines = self.transfersLocations[transferid]
      # clean up _transfersLocations
      del self.transfersLocations[transferid]
      # for each machine, cleanup the server queue and note down where the transfer was sent
      fileid = None
      for machine in machines:
        # if first one, found out fileid
        if not fileid:
          transfer = self[machine][transferid][0]
          fileid = (transfer[8], int(transfer[6]))
          reqid = transfer[2]
        # note where transfer was sent
        if machine not in transfersPerMachine:
          transfersPerMachine[machine] = []
        transfersPerMachine[machine].append(transferid)
      # if we have a source transfer already running, stop it
      if transferid in self.d2dsrcrunning:
        self.d2dend(transferid, reqid, transferCancelation=True)
      # cleanup. Has to be done after the d2dend call
      for machine in machines:
        del self[machine][transferid]
      return fileid, reqid
    except KeyError:
      # we are not handling this transfer, fine, ignore it then
      return None, None

  def remove(self, transferids):
    '''drops transfers from the queues and return a dictionnary "transferid => fileid" listing
    the transfers actually removed and for each of them, the concerned file'''
    transfersPerMachine = {}
    fileids = {}
    # first cleanup our own queue
    self.lock.acquire()
    try:
      for transferid in transferids:
        fileid, reqid = self._removetransfer(transferid, transfersPerMachine)
        if fileid:
          fileids[transferid] = (fileid, reqid)
    finally:
      self.lock.release()
    # then tell the machines to remove these transfers from their queues
    for machine in transfersPerMachine:
      self.connections.killtransfers(machine, tuple(transfersPerMachine[machine]))
    # return
    return fileids

  def _transfer2user(self, transfertype, rawtransfer):
    '''finds out the user for the given transfer, depending on the type'''
    if transfertype == 'standard':
      try:
        return pwd.getpwuid(int(rawtransfer[20]))[0]
      except KeyError:
        return rawtransfer[20]
    else:
      return 'stage'

  def removeall(self, requser):
    '''drops transfers from the queues return a dictionnary "transferid => fileid" listing
    the transfers actually removed and for each of them, the concerned file'''
    transfersPerMachine = {}
    fileids = {}
    # first cleanup our own queue
    self.lock.acquire()
    try:
      transferstodrop = []
      # Go through all transfers
      for transferid in self.transfersLocations:
        # ignore if not the right user
        if requser:
          # get user of for first location. For + break is used as set has no access to a random item
          for ds in self.transfersLocations[transferid]:
            rawtransfer = self[ds][transferid][0]
            transfertype = self[ds][transferid][2]
            break
          if self._transfer2user(transfertype, rawtransfer) != requser:
            continue
        # else put in the list of transfers to really drop
        # note that we can not drop here as we are looping on transfersLocations that would be modified
        transferstodrop.append(transferid)
      # now remove selected transfers
      for transferid in transferstodrop:
        fileid, reqid = self._removetransfer(transferid, transfersPerMachine)
        if fileid:
          fileids[transferid] = (fileid, reqid)
    finally:
      self.lock.release()
    # then tell the machines to remove these transfers from their queues
    timeout = self.config.getValue('TransferManager', 'AdminTimeout', 5, float)
    for machine in transfersPerMachine:
      self.connections.killtransfers(machine, tuple(transfersPerMachine[machine]), timeout=timeout)
    # return
    return fileids

  def transferStarting(self, diskserver, transferid, reqid, transfertype):
    '''Removes a transfer and gives back the list of other nodes where it was pending.
    Raises ValueError when not found'''
    if transfertype == 'd2dsrc':
      self.lock.acquire()
      try:
        # the source transfer is starting, mark all destinations (and the source) ready
        try :
          # remember where the source is running
          self.d2dsrcrunning[transferid] = diskserver
          # and make the destinations (if any) aware that it can now start this transfer
          for ds in self.transfersLocations[transferid]:
            self[ds][transferid] = self[ds][transferid][0:3]+[True]
            try:
              self.connections.retryD2dDest(ds, transferid, reqid)
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
        if transferid not in self.transfersLocations:
          # this transfer is supposed to be running. Let's check that it is not suppose to be
          # running on the very machine that wants to start it. That would mean that our answer
          # to this machine never arrived and the machine is retrying
          if self.recentlyScheduled.isDoubleScheduling(transferid, diskserver):
            # we are precisely in the mentionned case. We can safely return as we already think
            # that the job is running on that machine
            # "Transfer starting reconfirmed" message
            dlf.writedebug(msgs.TRANSFERSTARTCONFIRMED, DiskServer=diskserver, subreqid=transferid, reqid=reqid)
            return
          # The tranfer has really started somewhere else. Let the diskserver know by raising an exception
          # "Transfer had already started. Cancel start" message
          dlf.writedebug(msgs.TRANSFERALREADYSTARTED, DiskServer=diskserver, subreqid=transferid, reqid=reqid)
          raise ValueError
        # We are now sure that the transfer has not yet started
        # if a destination transfer wants to start, check whether the source is ready
        if transfertype == 'd2ddest' and not self[diskserver][transferid][3]:
          # "Source is not ready yet" message
          dlf.writedebug(msgs.SOURCENOTREADY, DiskServer=diskserver, subreqid=transferid, reqid=reqid)
          raise EnvironmentError
        # this time, the transfer can start.
        # drop the transfer from all queues. Note that this desynchronizes the queues as seen
        # by the diskservers from the queues seen by the central manager. In case of a
        # restart of a diskserver manager, its queue will be magically "cleaned up"
        # However, the diskservers concerned will be notified.
        machines = self.transfersLocations[transferid]
        del self.transfersLocations[transferid]
        for machine in machines:
          del self[machine][transferid]
        # Add this transfer to the recently scheduled ones
        self.recentlyScheduled.add(transferid, diskserver)
      finally:
        self.lock.release()
      # inform all other machines that this job has been handled
      for machine in machines:
        if machine != diskserver:
          try:
            # "Informing diskserver that job started somewhere else" message
            dlf.writedebug(msgs.INFODSJOBSTARTED, DiskServer=machine, subreqid=transferid, reqid=reqid)
            self.connections.transferAlreadyStarted(machine, transferid, reqid)
          except Exception, e:
            # "Failed to inform diskserver that job started elsewhere" message
            dlf.writenotice(msgs.INFODSJOBSTARTEDFAILED, DiskServer=machine, subreqid=transferid, reqid=reqid, Type=str(e.__class__), Message=str(e))

  def d2dend(self, transferid, reqid, transferCancelation=False):
    '''called when a d2d copy ends in order to inform the source.
    When called for a transfer cancelation, second parameter should be
    true so that we do not take the lock again and do not cleanup twice'''
    if not transferCancelation:
      self.lock.acquire()
    try:
      if transferid not in self.d2dsrcrunning:
        # the transfer has already disappeared (or was never started). This
        # can happen due to raise conditions, e.g when we get a timeout on the
        # start of the transfer when it has started already but the acknoledgement
        # did not yet come.
        # We can ignore these cases, but we still log
        # "Unable to end d2d as it's not in the server list. Probable race condition" message
        dlf.writewarning(msgs.D2DENDEXCEPTION, subreqid=transferid, reqid=reqid)
        return
      # get the source location
      diskserver = self.d2dsrcrunning[transferid]
      # remember the fileid in case of error
      transfertuple = self[diskserver][transferid]
      transfer = transfertuple[0]
      fileid = (transfer[8], int(transfer[6]))
      # remove d2dsrc transfer from the queue
      if not transferCancelation:
        del self[diskserver][transferid]
      # remove from list of d2dsrcs
      del self.d2dsrcrunning[transferid]
    finally:
      if not transferCancelation:
        self.lock.release()
    # inform the source diskserver
    try:
      self.connections.d2dend(diskserver, transferid, reqid)
    except Exception, e:
      # "Failed to inform diskserver that a d2d copy is over"
      dlf.writeerr(msgs.D2DOVERINFORMFAILED, DiskServer=diskserver, subreqid=transferid, reqid=reqid, fileid=fileid, Type=str(e.__class__), Message=str(e))
      # add back the transfer to the local lists
      if not transferCancelation:
        self.lock.acquire()
      try:
        self[diskserver][transferid] = transfertuple
        self.d2dsrcrunning[transferid] = diskserver
      finally:
        if not transferCancelation:
          self.lock.release()


  def putRunningD2dSource(self, diskserver, transferid, transfer, arrivaltime):
    '''Adds a new d2dsrc transfer to the list of runnign ones'''
    self.lock.acquire()
    try:
      # add transfer to the list of running d2dsrc transfers on the diskserver
      if diskserver not in self:
        self[diskserver] = {}
      self[diskserver][transferid] = [transfer, arrivaltime, 'd2dsrc', 'True']
      self.d2dsrcrunning[transferid] = diskserver
    finally:
      self.lock.release()

  def nbTransfersPerPool(self, diskServerList, reqdiskpool=None, requser=None):
    '''returns the number of unique transfers pending on the pool given (or all pools)
    for the given user (or all users)'''
    res = {}
    self.lock.acquire()
    try:
      # for each transfer
      for transferid in self.transfersLocations:
        # pick a random machine (we loop and break straight, due to lack of "getrandomitem" on a set
        for diskserver in self.transfersLocations[transferid]:
          # get diskpool
          diskpool = diskServerList.getDiskServerPool(diskserver)
          # are we interested in this diskpool ?
          if reqdiskpool and reqdiskpool != diskpool:
            break
          # are we interested in this user ?
          if requser:
            rawtransfer = self[diskserver][transferid][0]
            transfertype = self[diskserver][transferid][2]
            if requser != self._transfer2user(transfertype, rawtransfer):
              break
          # increase counter for corresponding diskpool
          if diskpool not in res:
            res[diskpool] = 0
          res[diskpool] = res[diskpool] + 1
          # no need to really loop, we already counted this guy
          break
    finally:
      self.lock.release()
    return res

  def listQueueingTransfers(self, diskserver):
    '''This is called by the scheduler when rebuilding the list of queueing transfers for a given diskserver'''
    if diskserver in self:
      self.lock.acquire()
      try:
        res = [tuple([transferid]+transfer[0:3]) for transferid, transfer in self[diskserver].items() if (transferid not in self.d2dsrcrunning) or self.d2dsrcrunning[transferid] != diskserver]
        return res
      finally:
        self.lock.release()
    else:
      return []

  def listAllRunningD2dSources(self):
    '''lists all running d2dsrc transfers as a tuple of (transferid,reqid,fileid) tuples'''
    res = []
    self.lock.acquire()
    try:
      for transferid in self.d2dsrcrunning:
        diskserver = self.d2dsrcrunning[transferid]
        transfer = self[diskserver][transferid][0]
        fileid = (transfer[8], int(transfer[6]))
        reqid = transfer[2]
        res.append((transferid, reqid, fileid))
      return tuple(res)
    finally:
      self.lock.release()

  def listRunningD2dSources(self, diskserver=None):
    '''lists running d2dsrc transfers for a given diskserver'''
    res = []
    self.lock.acquire()
    try:
      for transferid in [transferid for transferid in self.d2dsrcrunning
                         if self.d2dsrcrunning[transferid] == diskserver]:
        transfer, arrivaltime = self[diskserver][transferid][0:2]
        res.append((transferid, transfer, arrivaltime))
    finally:
      self.lock.release()
    return res

  def transfersCanceled(self, machine, transfers):
    '''cancels transfers in the queues and informs the stager in case there is no
    remaining machines where it could run'''
    transfersKilled = []
    self.lock.acquire()
    try:
      # for each transfer
      for transferid, fileid, rc, msg, reqid in transfers:
        if transferid not in self.transfersLocations:
          # the transfer has already disappeared (or was never started). This
          # can happen due to raise conditions, e.g when we get a timeout on the
          # start of the transfer when it has started already but the acknoledgement
          # did not yet come.
          # We can ignore these cases, but we still log
          # "Unable to cancel transfer as it's not in the transfer list. Probable race condition" message
          dlf.writewarning(msgs.TRANSFERCANCELEXCEPTION, subreqid=transferid, reqid=reqid, fileid=fileid)
          continue
        # cleanup the queue for the given transfer on the given machine
        self.transfersLocations[transferid].remove(machine)
        if not self.transfersLocations[transferid]:
          # no other candidate machine for this transfer. It has to be failed
          transfersKilled.append((transferid, fileid, rc, msg, reqid))
          # clean up _transfersLocations
          del self.transfersLocations[transferid]
          # if we have a source transfer already running, stop it
          if transferid in self.d2dsrcrunning:
            self.d2dend(transferid, reqid, transferCancelation=True)
        # drop the transfer id from the machine queue
        del self[machine][transferid]
    finally:
      self.lock.release()
    # inform the stager of the transfers that were killed
    return transfersKilled

  def transfersStartingFailed(self, transferid, machines):
    '''Amend the list of locations for transfers that could finally not be started on some machines.
    Returns a boolean saying whether the transfer is definitely failed or not'''
    ret = False
    self.lock.acquire()
    try:
      # it could happen that the transfer has already been started on another machine
      # and is already gone. We can safely ignore
      if transferid in self.transfersLocations:
        # Remove the machines where the transfer could not start from the transfer's locations
        for machine in machines:
          self.transfersLocations[transferid].remove(machine)
          if not self.transfersLocations[transferid]:
            # no other candidate machine for this transfer. It has to be failed
            ret = True
            # clean up _transfersLocations
            del self.transfersLocations[transferid]
            # if we have a source transfer already running, stop it
            if transferid in self.d2dsrcrunning:
              self.d2dend(transferid, reqid, transferCancelation=True)
            # drop the transfer id from the machine queue
            del self[machine][transferid]
    finally:
      self.lock.release()
    return ret
