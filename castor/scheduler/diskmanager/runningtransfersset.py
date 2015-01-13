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
# *
# * runningtransferset class of the transfer manager of the CASTOR project
# * this class is responsible for managing a local set of running transfers and check for their
# * termination when called on the poll method
# *
# * @author Castor Dev team, castor-dev@cern.ch
# *****************************************************************************/

'''runningtransferset module of the CASTOR disk server manager.
Handle a set of running transfers on a given diskserver'''

import os, ast, socket, subprocess
import threading
import time
from XRootD import client as XrdClient
from XRootD.client.flags import QueryCode
import dlf
from diskmanagerdlf import msgs
import castor_tools
import connectionpool
from transfer import cmdLineToTransfer, cmdLineToTransferId, TransferType, TapeTransferType, TapeTransfer
from reporter import StreamCount
import xrootiface

class RunningTransfersSet(object):
  '''handles a list of running transfers and is able to poll them regularly and list the ones that ended.
  Moreover, in case of a timeout transferEnded is called to fail the transfer.'''

  def __init__(self, fake=False):
    '''constructor'''
    # do we run in fake mode ?
    self.fake = fake
    # standard transfers and disk2disk copy destinations
    self.transfers = set()
    # lock for the transfers variable
    self.lock = threading.Lock()
    # lock for the tapeTransfers variable
    self.tapelock = threading.Lock()
    # get configuration
    self.config = castor_tools.castorConf()
    # list of ongoing tape transfers. Only transfer type and start time are listed here
    self.tapeTransfers = []
    # list transfers already running on the node, left over from the last time we ran
    self.leftOverTransfers = self.populate()

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
    # first loop over all processes
    pids = [pid for pid in os.listdir('/proc') if pid.isdigit()]
    leftOvers = {}
    for pid in pids:
      try:
        cmdline = open(os.path.sep+os.path.join('proc', pid, 'cmdline'), 'rb').read()
        rTransfer = cmdLineToTransfer(cmdline, scheduler, pid)
        if rTransfer != None:
          # create the entry in the list of running transfers
          self.transfers.add(rTransfer)
          # keep in memory that this was a rebuilt entry
          leftOvers[rTransfer.transfer.transferId] = int(pid)
          # 'Found transfer already running' message
          dlf.write(msgs.FOUNDTRANSFERALREADYRUNNING, transferType='user', subreqId=rTransfer.transfer.transferId,
                    fileid=rTransfer.transfer.fileId, startTime=rTransfer.startTime)
      except Exception:
        # exceptions caught here mean we could not get the info we wanted on the
        # process we were looking at. We only ignore this process as it has probably
        # finished in the mean time
        pass
    # then query xroot for its transfers
    try:
      # XXX TODO replace socket.getfqdn() with 'localhost' after deploying the new xrootd-python package
      fs = XrdClient.FileSystem(xrootiface.buildXrootURL(socket.getfqdn(), '', None, ''))
      st_stat, resp = fs.query(QueryCode.OPAQUE, "transfers")
      if not st_stat.ok:
        # 'Failed to query xrootd server' message
        dlf.writenotice(msgs.FAILTOQUERYXROOT, Message=st_stat.message)
      else:
        # this is the list of currently running transfers according to xroot
        for t in ast.literal_eval(resp):
          rTransfer = xrootiface.xrootTupleToTransfer(scheduler, t)
          if type(rTransfer) == TapeTransfer:
            self.tapeTransfers.append(rTransfer)
            # 'Found transfer already running' message
            dlf.write(msgs.FOUNDTRANSFERALREADYRUNNING, transferType=TapeTransferType.toStr(rTransfer.transferType),
                      subreqId=rTransfer.transferId, fileid=rTransfer.fileId, startTime=rTransfer.startTime)
          else:
            self.transfers.add(rTransfer)
            # 'Found transfer already running' message
            dlf.write(msgs.FOUNDTRANSFERALREADYRUNNING, transferType=TransferType.toPreciseStr(rTransfer.transfer),
                      subreqId=rTransfer.transfer.transferId, fileid=rTransfer.transfer.fileId, startTime=rTransfer.startTime)
    except Exception, e:
      dlf.writeerr(msgs.FAILTOQUERYXROOT, Type=str(e.__class__), Message=str(e))

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
    '''add a new running transfer to the set'''
    self.lock.acquire()
    try:
      self.transfers.add(rTransfer)
    finally:
      self.lock.release()

  def addTapeTransfer(self, tTransfer):
    '''appends a tape transfer to the dedicated list'''
    self.tapelock.acquire()
    try:
      self.tapeTransfers.append(tTransfer)
    finally:
      self.tapelock.release()

  def get(self, transferid):
    '''get a transfer by transferid. Raise KeyError if not found'''
    self.lock.acquire()
    try:
      for t in self.transfers:
        if t.transfer.transferId == transferid:
          return t
    finally:
      self.lock.release()
    # try a tape transfer
    self.tapelock.acquire()
    try:
      for t in self.tapeTransfers:
        if t.transferId == transferid:
          return t
    finally:
      self.tapelock.acquire()
    raise KeyError

  def setProcess(self, transferid, process):
    '''set the process object to the existing running transfer'''
    self.lock.acquire()
    try:
      t = self.get(transferid)
      t.process = process
    finally:
      self.lock.release()

  def kill(self, transferIds):
    '''removes a set of transfers from the list, and kills corresponding processes when possible'''
    self.lock.acquire()
    try:
      # kill what can be killed
      toBeKilled = set(rTransfer for rTransfer in self.transfers if rTransfer.transfer.transferId in transferIds)
      for rTransfer in toBeKilled:
        if type(rTransfer.process) == subprocess.Popen:
          rTransfer.process.terminate()
      # cleanup list of running transfers
      self.transfers = set(rTransfer for rTransfer in self.transfers if rTransfer.transfer.transferId not in transferIds)
    finally:
      self.lock.release()

  def remove(self, transfer):
    '''removes a transfer from the appropriate list, assuming the corresponding mover process is gone'''
    self.lock.acquire()
    try:
      self.transfers.remove(transfer)
      return
    except KeyError:
      pass
    finally:
      self.lock.release()
    # not found, try a tape transfer
    self.tapelock.acquire()
    try:
      self.tapeTransfers.remove(transfer)
    except ValueError:
      pass
    finally:
      self.tapelock.release()

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
          nbslots = self.config.getValue('DiskManager', rTransfer.transfer.protocol+'Weight', 1, int)
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
          transferType = TapeTransferType.toStr(tTransfer.transferType)
          n = n + 1
          nbslots = self.config.getValue('DiskManager', transferType+'Weight', 1, int)
          ns = ns + nbslots
          if not detailed:
            continue
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
          if rTransfer.transfer.flags in ('w',):
            res[mountPoint].nbWrites += 1
    finally:
      self.lock.release()
    # then we go for the tape transfers
    self.tapelock.acquire()
    try:
      for tTransfer in self.tapeTransfers:
        mountPoint = tTransfer.mountPoint
        if mountPoint not in res:
          res[mountPoint] = StreamCount()
        if tTransfer.transferType == TapeTransferType.RECALL:
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
        n = n + self.config.getValue('DiskManager', rTransfer.transfer.protocol+'Weight', 1, int)
    finally:
      self.lock.release()
    # and now tape transfers
    self.tapelock.acquire()
    try:
      for tTransfer in self.tapeTransfers:
        n = n + self.config.getValue('DiskManager', TransferType.toStr(tTransfer.transferType)+'Weight', 1, int)
    finally:
      self.tapelock.release()
    return n

  def _isTransferOver(self, rTransfer):
    '''checks if the given running transfer is still alive, and returns a (boolean, int) tuple
    where the first element says whether the transfer is over and the second is its return code,
    None for transfers that never run.'''
    transferId = rTransfer.transfer.transferId
    # check whether the transfer is over
    isEnded = False
    rc = None
    if transferId in self.leftOverTransfers:
      # special care for left over transfers, we use a signal 0 for them as they are not our children
      try:
        pid = self.leftOverTransfers[transferId]
        os.kill(pid, 0)
        # a process with this pid exists, now is it really our guy or something new ?
        cmdline = open(os.path.sep+os.path.join('proc', str(pid), 'cmdline'), 'rb').read()
        if transferId != cmdLineToTransferId(cmdline, pid):
          # it's a new one, not our guy
          isEnded = True
      except OSError:
        # process is dead
        isEnded = True
      if isEnded:
        rc = -1
        del self.leftOverTransfers[transferId]
    elif rTransfer.process != None:
      # check regular transfers (non left over), except for d2dsrc transfers or transfers
      # that have just been scheduled and not yet started as they have no process
      try:
        rc = rTransfer.process.poll()
        isEnded = (rc != None)
        if isEnded and rTransfer.transfer.transferType == TransferType.D2DDST:
          # only for disk-to-disk copies, check the xrdcp subprocess output for logging
          if rc != 0:
            # log "Failed to end the transfer"
            dlf.writenotice(msgs.TRANSFERENDEDFAILED, subreqId=transferId,
                            reqId=rTransfer.transfer.reqId, errCode=rc, output=rTransfer.process.stdout.read())
          else:
            # log "Transfer ended"
            dlf.writedebug(msgs.TRANSFERENDED, subreqId=transferId, output=rTransfer.process.stdout.read())
      except AttributeError:
        # this is a running (xrootd) transfer for which we have no process associated, so we can't poll():
        # in this case we must assume the transfer is still running
        isEnded = False
    else:
      if self.fake:
        isEnded = True
      else:
        # running transfers without a process are the ones for which a slot has been allocated
        # but the client has not come yet. They must be timed out to avoid a slot leak - this would
        # be the case of xrootd clients in particular, due to the stalling mechanism, but also of
        # any client that does not reconnect after the call back.
        if rTransfer.transfer.transferType == TransferType.STD and \
           rTransfer.startTime + rTransfer.transfer.getTimeout() < time.time():
          isEnded = True
    return isEnded, rc

  def poll(self):
    '''checks for finished transfers and clean them up'''
    failedTransfers = {}
    self.lock.acquire()
    try:
      ended = []
      for rTransfer in self.transfers:
        transferId = rTransfer.transfer.transferId
        isEnded, rc = self._isTransferOver(rTransfer)
        if isEnded:
          # "Transfer ended" message
          dlf.writedebug(msgs.TRANSFERENDED, subreqId=transferId, reqId=rTransfer.transfer.reqId,
                         fileid=rTransfer.transfer.fileId, returnCode=rc)
          # append to list of ended transfers
          ended.append(transferId)
          # for transfers that failed without being able to inform a moverhandler thread, clean them up
          if not rTransfer.ended:
            if rc == None:
              # the transfer didn't take place
              errCode = 1004  # SETIMEDOUT
              errMsg = 'Timed out waiting for client connection'
            elif rc < 0:         # in case of transfers killed by a signal, remember to inform the DB
              errMsg = 'Transfer has been killed by signal %d' % (-rc)
              errCode = 1015  # SEINTERNAL
            elif rc > 0:         # these are transfers that got interrupted or somehow failed
              errMsg = 'Mover exited with failure, rc=%d' % rc
              errCode = 1015  # SEINTERNAL
            if rTransfer.scheduler not in failedTransfers:
              failedTransfers[rTransfer.scheduler] = []
            failedTransfers[rTransfer.scheduler].append((rTransfer.transfer.transferId,
                                                         rTransfer.transfer.fileId, errCode, errMsg,
                                                         rTransfer.transfer.reqId))
      # cleanup ended transfers
      self.transfers = set(rTransfer for rTransfer in self.transfers if rTransfer.transfer.transferId not in ended)
    finally:
      self.lock.release()
    # get the admin timeout
    timeout = self.config.getValue('TransferManager', 'AdminTimeout', 5, float)
    # inform schedulers of killed transfers
    for scheduler in failedTransfers:
      try:
        connectionpool.connections.transfersKilled(scheduler, tuple(failedTransfers[scheduler]), timeout=timeout)
        for transferId, fileId, rc, msg, reqId in failedTransfers[scheduler]:
          # "Informed scheduler that transfer failed" message
          dlf.write(msgs.INFORMTRANSFERFAILED, Scheduler=scheduler, subreqId=transferId,
                    reqId=reqId, fileid=fileId, errCode=rc, Message=msg)
      except connectionpool.Timeout:
        for transferId, fileId, rc, msg, reqId in failedTransfers[scheduler]:
          # "Failed to inform scheduler that transfer failed" message
          dlf.writenotice(msgs.INFORMTRANSFERFAILEDFAILED, Scheduler=scheduler,
                          subreqId=transferId, reqId=reqId, fileid=fileId, Message=msg)
      except Exception, e:
        for transferId, fileId, rc, msg, reqId in failedTransfers[scheduler]:
          # "Failed to inform scheduler that transfer failed" message
          dlf.writeerr(msgs.INFORMTRANSFERFAILEDFAILED, Scheduler=scheduler,
                       subreqId=transferId, reqId=reqId, fileid=fileId, Message=msg, error=e)


  def listTransfers(self, reqUser=None):
    '''lists running transfers'''
    res = []
    # first list the standard transfers
    self.lock.acquire()
    try:
      for rTransfer in self.transfers:
        transfer = rTransfer.transfer
        if not reqUser or transfer.user == transfer.user:
          res.append((transfer.transferId, transfer.fileId, rTransfer.scheduler,
                      transfer.user, 'RUN', TransferType.toPreciseStr(transfer), transfer.creationTime,
                      rTransfer.startTime))
    finally:
      self.lock.release()
    # then add the tape ones
    self.tapelock.acquire()
    try:
      for tTransfer in self.tapeTransfers:
        res.append(('-', tTransfer.fileId, tTransfer.clientHost, 'stage', 'TAPE',
                    TapeTransferType.toStr(tTransfer.transferType), tTransfer.startTime,
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
      # go through the transfers
      for rTransfer in self.transfers:
        # Stop whenever we find one
        if reqscheduler == rTransfer.scheduler:
          return True
      # No transfer found
      return False
    finally:
      self.lock.release()
