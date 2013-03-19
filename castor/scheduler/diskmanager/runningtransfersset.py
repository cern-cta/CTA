#/******************************************************************************
# *                   runningtransferset.py
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
# * runningtransferset class of the transfer manager of the CASTOR project
# * this class is responsible for managing a local set of running transfers and check for their
# * termination when called on the poll method
# *
# * @author Castor Dev team, castor-dev@cern.ch
# *****************************************************************************/

'''runningtransferset module of the CASTOR disk server manager.
Handle a set of running transfers on a given diskserver'''

import os, socket
import threading
import time
import dlf
from diskmanagerdlf import msgs
import castor_tools
import connectionpool
from transfer import cmdLineToTransfer, cmdLineToTransferId, TransferType, TapeTransfer, TapeTransferType
from reporter import StreamCount

class RunningTransfersSet(object):
  '''handles a list of running transfers and is able to poll them regularly and list the ones that ended'''
 
  def __init__(self, fake=False):
    '''constructor'''
    # do we run in fake mode ?
    self.fake = fake
    # standard transfers and disk2disk copy destinations
    self.transfers = set()
    # lock for the transfers variable
    self.lock = threading.Lock()
    # get configuration
    self.config = castor_tools.castorConf()
    # list transfers already running on the node, left over from the last time we ran
    self.leftOverTransfers = self.populate()
    # list of ongoing tape transfers. Only transfer type and start time are listed here
    self.tapeTransfers = []
    # dictionary of previously known tape transfers
    self.prevTapeTransfers = {}
    # lock for the tapeTransfers variable
    self.tapelock = threading.Lock()

  def listTapeTransfers(self):
    '''updates the list of ongoing tape transfers'''
    # the disk manager daemon still considers a migration stream as active even
    # if no physical transfer is taking place for up to 10 seconds after the
    # stream has ended.
    #
    # This logic prevents the disk manager daemon from starting a transfer just
    # at the point when one tape migration stops and another one starts.
    # Here we simply perform some maintenance operations on the list of
    # previously known tape transfers.
    for key in self.prevTapeTransfers.keys():
      if self.prevTapeTransfers[key].transferType == TapeTransferType.RECALL:
        del self.prevTapeTransfers[key]  # always get rid of recall cases
      elif self.prevTapeTransfers[key].lastTimeViewed < time.time() - 10:
        del self.prevTapeTransfers[key]
      else:
        # reset the hostname to '-', this is not strictly necessary but
        # provides a means to see which tape migrations are ongoing and those
        # which have just ended
        self.prevTapeTransfers[key].clientHost = '-'
        
    # loop over all processes listed in proc
    procpath = os.path.sep + 'proc'
    for pid in os.listdir(procpath):
      if not pid.isdigit():
        continue  # not a pid
      try:
        pidpath = os.path.join(procpath, pid)
        cmdpath = os.path.join(pidpath, 'cmdline')
        cmdline = open(cmdpath, 'rb').read()
        # ignore non rfiod processes
        if cmdline[0:18] != '/usr/bin/rfiod\0-sl':
          continue
        # container to hold the paramaters listed in the rfiod information
        # file
        params = {}
        # open the rfio information file and extract the key value pairs
        infopath = os.path.join('/var/lib/rfiod', pid + '.info')
        f = open(infopath, 'r')
        for line in f.readlines():
          key, value = line.strip().split('=')
          params[key] = value
        f.close()
        # process the parameters in the file
        if params['CASTOR_USER'] != 'stage':
          continue  # ignore processes not running as stage
        if params['CASTOR_D2D'] == 'true':
          continue  # ignore processes which are d2d transfers
        # use the access mode to determine the type of transfer. If open
        # exclusively for read then it's a migration stream
        transfertype = None
        if params['CASTOR_ACCESSMODE'] == 'r':
          transfertype = TapeTransferType.MIGRATION
        elif params['CASTOR_ACCESSMODE'] == 'w':
          transfertype = TapeTransferType.RECALL
        if transfertype == None:
          continue
        # extract the fileid and mountPoint from the filename
        filepath = params['CASTOR_FILENAME']
        fileid = int(os.path.basename(filepath).split('@')[0])
        mountPoint = filepath.rsplit(os.sep, 2)[0]+os.sep
        # we found a tape transfer
        key = str(pid) + ":" + params['CASTOR_OPENTIME']
        self.prevTapeTransfers[key] = TapeTransfer(transfertype,
                                                   int(params['CASTOR_OPENTIME']),
                                                   params['CASTOR_CLIENTHOSTNAME'],
                                                   fileid,
                                                   mountPoint,
                                                   time.time())
      except Exception:
        # ignore any exceptions, these are probably related to attempts to
        # access process information which doesn't exist because the process
        # disappeared when we were analysing it.
        continue

    # renew the tapeTransfers list
    self.tapelock.acquire()
    try:
      # reset the list
      self.tapeTransfers = []
      # update the list
      for transfer in self.prevTapeTransfers.itervalues():
        self.tapeTransfers.append(transfer)
    finally:
      self.tapelock.release()

  def populate(self):
    '''populates the list of ongoing transfers from the system.
    Then synchronize with the stager DB (through the transfer manager)
    so that transfers which are no more running but were not ended
    properly in the databases are ended.
    Note that this is linux specific code.'''
    # 'populating running scripts from system' message
    dlf.write(msgs.POPULATING)
    # get a random scheduler host
    scheduler = self.config.getValue('DiskManager', 'ServerHosts').split()[0]
    # loop over all processes
    pids = [pid for pid in os.listdir('/proc') if pid.isdigit()]
    leftOvers = {}
    for pid in pids:
      try:
        cmdline = open(os.path.sep+os.path.join('proc', pid, 'cmdline'), 'rb').read()
        rTransfer = cmdLineToTransfer(cmdline, scheduler)
        if rTransfer != None:
          # create the entry in the list of running transfers
          self.transfers.add(rTransfer)
          # keep in memory that this was a rebuilt entry
          leftOvers[rTransfer.transfer.transferId] = int(pid)
          # 'Found transfer already running' message
          dlf.write(msgs.FOUNDTRANSFERALREADYRUNNING, subreqId=rTransfer.transfer.transferId,
                    reqId=rTransfer.transfer.reqId, fileid=rTransfer.transfer.fileId,
                    startTime=rTransfer.startTime)
      except Exception:
        # exceptions caught here mean we could not get the info we wanted on the
        # process we were looking at. We only ignore this process as it has probably
        # finished in the mean time
        pass
    # send the list of running transfers to the stager DB for synchronization
    try:
      while True:
        try:
          timeout = self.config.getValue('TransferManager', 'AdminTimeout', 5, float)
          connectionpool.connections.syncRunningTransfers(scheduler, socket.getfqdn(),
                                                          tuple(leftOvers.keys()), timeout=timeout)
          break
        except connectionpool.Timeout:
          # as long as we get a timeout, we retry. This will not last. We still wait a little bit
          time.sleep(0.1)
    except Exception, e:
      # 'Exception caught when trying to synchronize running transfers with the database. Giving up' message
      dlf.writeerr(msgs.SYNCRUNTRANSFERFAILED, Type=str(e.__class__), Message=str(e))
    # finally return
    return leftOvers

  def add(self, rTransfer):
    '''add a new running transfer to the list'''
    self.lock.acquire()
    try:
      self.transfers.add(rTransfer)
    finally:
      self.lock.release()

  def nbTransfers(self, reqUser=None, detailed=False):
    '''returns number of running transfers and number of running slots, plus details
    per protocol if the detailed parameter is true.
    The exact format of the returned tuple if detailed is False is :
     (nbRunningTransfers, nbRunningSlots)
    If detailed is True, then it is :
     (nbRunningTransfers, (('proto1', nbRunningTransfersForProto1), ...),
      nbRunningSlots, (('proto1', nbRunningSlotsForProto1), ...)) '''
    n = 0
    nproto = {}
    ns = 0
    nsproto = {}
    # first we deal with the regular transfers
    self.lock.acquire()
    try:
      for rTransfer in self.transfers:
        if not reqUser or reqUser == rTransfer.transfer.user:
          n = n + 1
          nbslots = self.config.getValue('DiskManager', rTransfer.transfer.protocol+'Weight', None, int)
          ns = ns + nbslots
          if not detailed:
            continue
          protocol = rTransfer.transfer.protocol
          if protocol not in nproto:
            nproto[protocol] = 0
          nproto[protocol] = nproto[protocol] + 1
          if protocol not in nsproto:
            nsproto[protocol] = 0
          nsproto[protocol] = nsproto[protocol] + nbslots
    finally:
      self.lock.release()
    # then we go for the tape transfers
    if not reqUser or reqUser == 'stage':
      self.tapelock.acquire()
      try:
        for tTransfer in self.tapeTransfers:
          n = n + 1
          nbslots = self.config.getValue('DiskManager', tTransfer.transfer.transferType+'Weight', None, int)
          ns = ns + nbslots
          if not detailed:
            continue
          transferType = tTransfer.transferType
          if transferType not in nproto:
            nproto[transferType] = 0
          nproto[transferType] = nproto[transferType] + 1
          if transferType not in nsproto:
            nsproto[transferType] = 0
          nsproto[transferType] = nsproto[transferType] + nbslots
      finally:
        self.tapelock.release()
    if detailed:
      return n, tuple(nproto.items()), ns, tuple(nsproto.items())
    else:
      return n, ns

  def getStreamCount(self):
    '''returns number of streams running per filesystem for each type of stream :
       (read, write, recalls, migrations) as a dictionnary with the
       mountPoint of the key'''
    res = {}
    # first we deal with the regular transfers
    self.lock.acquire()
    try:
      for rTransfer in self.transfers:
        mountPoint = rTransfer.transfer.mountPoint
        if mountPoint not in res:
          res[mountPoint] = StreamCount()
        if rTransfer.transfer.transferType == TransferType.D2DSRC:
          res[mountPoint].nbReads += 1
        elif rTransfer.transfer.transferType == TransferType.D2DDST:
          res[mountPoint].nbWrites += 1
        else:
          # we consider read/writes as read
          if rTransfer.transfer.flags in ('o', 'r'):
            res[mountPoint].nbReads += 1
          if rTransfer.transfer.flags in ('w'):
            res[mountPoint].nbWrites += 1
    finally:
      self.lock.release()
    # then we go for the tape transfers
    self.tapelock.acquire()
    try:
      for tTransfer in self.tapeTransfers:
        mountPoint = tTransfer.transfer.mountPoint
        if mountPoint not in res:
          res[mountPoint] = StreamCount()
        if tTransfer.transfer.transferType == TapeTransferType.RECALL:
          res[mountPoint].nbRecalls += 1
        else:
          res[mountPoint].nbMigrations += 1
    finally:
      self.tapelock.release()
    return res

  def nbUsedSlots(self):
    '''returns number of slots occupied by running transfers'''
    n = 0
    # regular transfers first
    self.lock.acquire()
    try:
      for rTransfer in self.transfers:
        n = n + self.config.getValue('DiskManager', rTransfer.transfer.protocol+'Weight', None, int)
    finally:
      self.lock.release()
    # and now tape transfers
    self.tapelock.acquire()
    try:
      for tTransfer in self.tapeTransfers:
        n = n + self.config.getValue('DiskManager', tTransfer.transfer.transferType+'Weight', None, int)
    finally:
      self.tapelock.release()
    return n

  def poll(self):
    '''checks for finished transfers and clean them up'''
    sourcesToBeInformed = []
    killedTransfers = {}
    self.lock.acquire()
    try:
      ended = []
      for rTransfer in self.transfers:
        transferId = rTransfer.transfer.transferId
        # check whether the transfer is over
        isEnded = False
        if transferId in self.leftOverTransfers:
          # special care for left over transfers, we use a signal 0 for them as they are not our children
          try :
            pid = self.leftOverTransfers[transferId]
            os.kill(pid, 0)
            # a process with this pid exists, now is it really our guy or something new ?
            cmdline = open(os.path.sep+os.path.join('proc', str(pid), 'cmdline'), 'rb').read().split('\0')
            if transferId != cmdLineToTransferId(cmdline):
              # it's a new one, not our guy
              isEnded = True
          except OSError:
            # process is dead
            isEnded = True
          if isEnded:
            rc = -1
            del self.leftOverTransfers[transferId]
        elif rTransfer.process != None:
          # check regular transfers (non left over), except for d2dsrc transfers as they have no process
          # for this case, the transfer is our child, so we can poll
          if self.fake:
            isEnded = True
          else:
            rc = rTransfer.process.poll()
            isEnded = (rc!=None)
        if isEnded:
          # "transfer ended message"
          dlf.writedebug(msgs.TRANSFERENDED, subreqId=transferId, reqId=rTransfer.transfer.reqId,
                         fileid=rTransfer.transfer.fileId, ReturnCode=rc)
          # append to list of ended transfers
          ended.append(transferId)
          # in case of disk to disk copy, remember to inform source
          if rTransfer.transfer.transferType == 'd2ddest':
            sourcesToBeInformed.append(rTransfer)
          # in case of transfers killed by a signal, remember to inform the DB
          if rc < 0:
            if rTransfer.scheduler not in killedTransfers:
              killedTransfers[rTransfer.scheduler] = []
            errMsg = 'Transfer has been killed'
            if rTransfer.process:
              errMsg += ' : ' + rTransfer.process.stderr.read()
            killedTransfers[rTransfer.scheduler].append((rTransfer.transfer.transferId,
                                                         rTransfer.transfer.fileId, rc, errMsg,
                                                         rTransfer.transfer.reqId))
      # cleanup ended transfers
      self.transfers = set(rTransfer for rTransfer in self.transfers if rTransfer.transfer.transferId not in ended)
    finally:
      self.lock.release()
    # get the admin timeout
    timeout = self.config.getValue('TransferManager', 'AdminTimeout', 5, float)
    # inform schedulers of disk to disk transfers that are over
    for rTransfer in sourcesToBeInformed:
      try:
        connectionpool.connections.d2dend(rTransfer.scheduler, rTransfer.transfer, timeout=timeout)
      except connectionpool.Timeout, e:
        # "Failed to inform scheduler that a d2d transfer is over" message
        dlf.writenotice(msgs.INFORMTRANSFERISOVERFAILED, Scheduler=rTransfer.scheduler,
                        subreqId=rTransfer.transfer.transferId, reqId=rTransfer.transfer.reqId,
                        fileid=rTransfer.transfer.fileId, Type=str(e.__class__), Message=str(e))
      except Exception, e:
        # "Failed to inform scheduler that a d2d transfer is over" message
        dlf.writeerr(msgs.INFORMTRANSFERISOVERFAILED, Scheduler=rTransfer.scheduler,
                     subreqId=rTransfer.transfer.transferId, reqId=rTransfer.transfer.reqId,
                     fileid=rTransfer.transfer.fileId, Type=str(e.__class__), Message=str(e))
    # inform schedulers of transfers killed
    for scheduler in killedTransfers:
      try:
        connectionpool.connections.transfersKilled(scheduler, tuple(killedTransfers[scheduler]), timeout=timeout)
        for transferId, fileId, rc, msg, reqId in killedTransfers[scheduler]:
          # "Informed scheduler that transfer was killed by a signal" message
          dlf.write(msgs.INFORMTRANSFERKILLED, Scheduler=scheduler, subreqId=transferId,
                    reqId=reqId, fileid=fileId, signal=-rc, Message=msg)
      except connectionpool.Timeout, e:
        for transferId, fileId, rc, msg, reqId in killedTransfers[scheduler]:
          # "Failed to inform scheduler that transfer was killed by a signal" message
          dlf.writenotice(msgs.INFORMTRANSFERKILLEDFAILED, Scheduler=scheduler,
                          subreqId=transferId, reqId=reqId, fileid=fileId, Message=msg)
      except Exception, e:
        for transferId, fileId, rc, msg, reqId in killedTransfers[scheduler]:
          # "Failed to inform scheduler that transfer was killed by a signal" message
          dlf.writeerr(msgs.INFORMTRANSFERKILLEDFAILED, Scheduler=scheduler,
                       subreqId=transferId, reqId=reqId, fileid=fileId, Message=msg)

  def d2dend(self, transferId):
    '''called when a disk to disk copy ends and we are the source of it'''
    self.lock.acquire()
    try:
      self.transfers = set(transfer for transfer in self.transfers if transfer[0] != transferId)
    finally:
      self.lock.release()

  def listTransfers(self, reqUser=None):
    '''lists running transfers'''
    res = []
    n = 0
    # first list the standard transfers
    self.lock.acquire()
    try:
      for rTransfer in self.transfers:
        transfer = rTransfer.transfer
        if not reqUser or transfer.user == transfer.user:
          res.append((transfer.transferId, transfer.fileId, rTransfer.scheduler,
                      transfer.user, 'RUN', transfer.protocol, transfer.creationTime,
                      rTransfer.startTime))
          n = n + 1
          if n >= 100: # give up with full listing if too many transfers
            break
    finally:
      self.lock.release()
    # then add the tape ones
    self.tapelock.acquire()
    try:
      for tTransfer in self.tapeTransfers:
        res.append(('-', tTransfer.fileId, tTransfer.clienthost, 'stage', 'TAPE',
                    TransferType.toStr(tTransfer.transferType), tTransfer.startTime,
                    tTransfer.startTime))
    finally:
      self.tapelock.release()
    return res

  def transferset(self):
    '''Lists all pending and running transfers'''
    self.lock.acquire()
    try:
      # retrieve transferId and reqId
      return set([(rTransfer.transfer.transferId, rTransfer.transfer.reqId) for rTransfer in self.transfers])
    finally:
      self.lock.release()

  def listRunningD2dSources(self, scheduler):
    '''lists running d2dsrc transfers'''
    self.lock.acquire()
    try:
      # retrieve transferId, transfer and arrivalTime for transfertype 'd2dsrc'
      return [rTransfer.transfer.asTuple()
              for rTransfer in self.transfers
              if rTransfer.transfer.transferType == TransferType.D2DSRC and rTransfer.scheduler == scheduler]
    finally:
      self.lock.release()

  def anyTransfersFromScheduler(self, reqscheduler):
    '''Tells whether any transfer is running that is handled by the given scheduler'''
    self.lock.acquire()
    try:
      # go through the transfer
      for rTransfer in self.transfers:
        # Stop whenever we find one
        if reqscheduler == rTransfer.scheduler:
          return True
      # No transfer found
      return False
    finally:
      self.lock.release()
