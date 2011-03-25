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

import threading
import dlf, pwd
from transfermanagerdlf import msgs

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

  def put(self, transferid, arrivaltime, transferList, transfertype='standard'):
    '''Adds a new transfer. transfertype can be one of 'standard', 'd2dsrc' and 'd2ddest' '''
    self.lock.acquire()
    try:
      for diskserver, transfer in transferList:
        # add transfer to the list of queueing transfers on the diskserver
        # note the extra argument, used only for d2ddest transfers and telling whether the source is ready
        # As it could be that the source has already started when the destinations are entered, we have
        # to infer this argument from the transferid, looking into the list of running sources
        if diskserver not in self: self[diskserver] = {}
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
    Not that the locking is not handled here, it's left to the responsability of the caller'''
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
        # note where transfer was sent
        if machine not in transfersPerMachine: transfersPerMachine[machine] = []
        transfersPerMachine[machine].append(transferid)
        # cleanup
        del self[machine][transferid]
      # if we have a source transfer already running, stop it
      if transferid in self.d2dsrcrunning: d2dend(transferid,lock=False)
      return fileid, transfer[2]
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
            rawtransfer, arrivaltime, transfertype, d2drun = self[ds][transferid]
            break
          if self._transfer2user(transfertype, rawtransfer) != requser: continue
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
    for machine in transfersPerMachine:
      self.connections.killtransfers(machine, tuple(transfersPerMachine[machine]))
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
          for ds in self.transfersLocations[transferid]:
            self[ds][transferid] = self[ds][transferid][0:3]+[True]
        except KeyError:
          # no destination yet, no problem, the source was only too fast to start
          pass
        # remember where the source is running
        self.d2dsrcrunning[transferid] = diskserver
      finally:
        self.lock.release()
    else:
      self.lock.acquire()
      try:
        # check whether the transfer has already been started somewhere else
        if transferid not in self.transfersLocations:
          # in such a case, let the diskserver know by raising an exception
          # "Transfer had already started. Cancel start" message
          dlf.writedebug(msgs.TRANSFERALREADYSTARTED, diskserver=diskserver, subreqid=transferid, reqid=reqid)
          raise ValueError
        # if a destination transfer wants to start, check whether the source is ready
        if transfertype == 'd2ddest' and not self[diskserver][transferid][3]:
          # "Source is not ready yet" message
          dlf.writedebug(msgs.SOURCENOTREADY, diskserver=diskserver, subreqid=transferid, reqid=reqid)
          raise EnvironmentError
        # drop the transfer from all queues. Note that this desynchronizes the queues as seen
        # by the diskservers from the queues seen by the central manager. In case of a
        # restart of a diskserver manager, its queue will be magically "cleaned up"
        # However, the diskservers concerned will be notified.
        machines = self.transfersLocations[transferid]
        del self.transfersLocations[transferid]
        for machine in machines: del self[machine][transferid]
      finally:
        self.lock.release()
      # inform all other machines that this job has been handled
      for machine in machines:
        if machine != diskserver:
          try:
            # "Informing diskserver that job started somewhere else" message
            dlf.writedebug(msgs.INFODSJOBSTARTED, diskserver=machine, subreqid=transferid, reqid=reqid)
            self.connections.transferAlreadyStarted(machine, transferid, reqid)
          except Exception, e:
            # "Informing diskserver that job started somewhere else failed" message
            dlf.writeerr(msgs.INFODSJOBSTARTEDFAILED, diskserver=machine, subreqid=transferid, reqid=reqid, type=str(e.__class__), msg=str(e))

  def d2dend(self, transferid, reqid, lock=True):
    '''called when a d2d copy ends in order to inform the source'''
    if lock:
      self.lock.acquire()
    try:
      # get the source location
      diskserver = self.d2dsrcrunning[transferid]
      # remember the fileid in case of error
      transfer = self[diskserver][transferid][0]
      fileid = (transfer[8], int(transfer[6]))
      # remove d2dsrc transfer from the queue
      del self[diskserver][transferid]
      # remove from list of d2dsrcs
      del self.d2dsrcrunning[transferid]
    finally:
      if lock: 
        self.lock.release()
    # inform the source diskserver
    try:
      self.connections.d2dend(diskserver, transferid, reqid)
    except Exception, e:
      # "Informing diskserver that d2d copy is over failed" message
      dlf.writeerr(msgs.D2DOVERINFORMFAILED, diskserver=diskserver, subreqid=transferid, reqid=reqid, fileid=fileid, error=str(e))

  def putRunningD2dSource(self, diskserver, transferid, transfer, arrivaltime):
    '''Adds a new d2dsrc transfer to the list of runnign ones'''
    self.lock.acquire()
    try:
      # add transfer to the list of running d2dsrc transfers on the diskserver
      if diskserver not in self: self[diskserver] = {}
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
          if reqdiskpool and reqdiskPool != diskpool: break
          # are we interested in this user ?
          if requser:
            rawtransfer, arrivaltime, transfertype, d2drun = self[diskserver][transferid]
            if requser != self._transfer2user(transfertype, rawtransfer): break
          # increase counter for corresponding diskpool
          if diskpool not in res: res[diskpool] = 0
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
        res = [tuple([transferid]+transfer[0:3]) for transferid, transfer in self[diskserver].items() if transferid not in self.d2dsrcrunning]
        return res
      finally:
        self.lock.release()
    else:
      return []

  def listRunningD2dSources(self, diskserver):
    '''This is called by the scheduler when rebuilding the list of running d2dsrc transfers for a given diskserver'''
    res = []
    self.lock.acquire()
    try:
      for transferid in [transferid for transferid in self.d2dsrcrunning if self.d2dsrcrunning[transferid] == diskserver]:
        transfer, arrivaltime, transfertype, ready = self[diskserver][transferid]
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
        try:
          # cleanup the queue for the given transfer on the given machine
          self.transfersLocations[transferid].remove(machine)
          del self[machine][transferid]
          if not self.transfersLocations[transferid]:
            # no other candidate machine for this transfer. It has to be failed
            transfersKilled.append((transferid, fileid, rc, msg, reqid))
            # clean up _transfersLocations
            del self.transfersLocations[transferid]
            # if we have a source transfer already running, stop it
            if transferid in self.d2dsrcrunning: d2dend(transferid,reqid,lock=False)
        except KeyError, e:
          # we are not handling this transfer or it is not queued for the given machine
          # we can only log this oddity and ignore
          # "Unexpected KeyError exception caught in transfersCanceled" message
          dlf.writeerr(msgs.TRANSFERCANCELEXCEPTION, error=str(e))
    finally:
      self.lock.release()
    # inform the stager of the transfers that were killed
    return transfersKilled

