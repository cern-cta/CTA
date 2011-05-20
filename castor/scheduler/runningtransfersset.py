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

import os, pwd, stat, re, string, socket
import threading
import time
import dlf
from diskmanagerdlf import msgs
import castor_tools
import connectionpool

class RunningTransfersSet(object):
  '''handles a list of running transfers and is able to poll them regularly and list the ones that ended'''
 
  _SOCKET_INODE_REGEXP = re.compile(r"socket:\[(\d+)\]$")
  _SOCKET_TABLE_LINE_DELIMITER = re.compile("[%s:]+" % string.whitespace)

  def __init__(self, connections, fake=False):
    '''constructor'''
    # connection pool to be used for sending messages to schedulers
    self.connections = connections
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
    # lock for the tapeTransfers variable
    self.tapelock = threading.Lock()

  def _parsesockettable(self, inodes):
    '''Parses the proc filesystem to map socket inodes to actual host names.
    note that only IPv4 is supported here, although supporting IPv6 is trivial.
    Only a different file should be open, namefile /proc/net/tcp6.
    This code is linux specific'''
    try:
      connfile = open('/proc/net/tcp')
      inode2host = {}
      for l in connfile.readlines()[1:]:
        # parse the line
        cols = self._SOCKET_TABLE_LINE_DELIMITER.split(l.strip())
        # get inode and see whether we are interested
        inode = int(cols[13])
        if inode not in inodes:
          continue
        # get remote IP address
        rawip = cols[3] # needs to be reversed
        packedip = "".join([chr(int(rawip[i:i+2], 16)) for i in range(len(rawip)-2, -1, -2)])
        try:
          ip = socket.inet_ntop(socket.AF_INET, packedip)
          # get remote host
          addrstr = socket.gethostbyaddr(ip)
          # build dictionnary of connections
          inode2host[inode] = addrstr[0]
        except Exception:
          pass
      connfile.close()
      return inode2host
    except IOError:
      return {}

  def listTapeTransfers(self):
    '''updates the list of ongoing tape transfers'''
    # build a first list of new tape transfers
    newTapeTransfers = []
    # loop over all processes
    procpath = os.path.sep + 'proc'
    pids = [pid for pid in os.listdir('/proc') if pid.isdigit()]
    for pid in pids :
      try:
        pidpath = os.path.join(procpath, pid)
        cmdpath = os.path.join(pidpath, 'cmdline')
        cmdline = open(cmdpath, 'rb').read()
        # do we deal with a tape transfer ?
        if cmdline[0:18] != '/usr/bin/rfiod\0-sl':
          continue
        # recall or migration ? To find out, we find the open file on the filesystem and look at its mode
        fdpath = os.path.join(pidpath, 'fd')
        # loop though the filedescriptors
        foundlocalfile = False
        foundsocket = False
        for fd in os.listdir(fdpath):
          linkpath = os.path.join(fdpath, fd)
          fdstat = os.lstat(linkpath)
          # only consider links
          if not stat.S_ISLNK(fdstat.st_mode):
            continue
          # and only consider links to local filesystem or sockets
          targetpath = os.readlink(linkpath)
          if not foundlocalfile:
            for fs in self.config.getValue('RmNode', 'MountPoints').split():
              if targetpath.startswith(fs):
                # we got one tape transfer, let's compute it's direction
                if (fdstat.st_mode & stat.S_IRUSR) == 0:
                  transfertype = 'recall'
                else:
                  transfertype = 'migr'
                # find out the fileid
                fileid = int(os.path.basename(targetpath).split('@')[0])
                # and add it to the list of ongoing tape transfers
                newTapeTransfer = (transfertype, os.stat(cmdpath).st_mtime, fileid)
                foundlocalfile = True
                break
          if not foundsocket:
            matchobj = self._SOCKET_INODE_REGEXP.match(targetpath)
            if matchobj:
              foundsocket = True
              inode = int(matchobj.group(1))
          if foundlocalfile and foundsocket:
            break
        if foundlocalfile:
          if foundsocket:
            newTapeTransfers.append(newTapeTransfer+(inode,))
          else:
            newTapeTransfers.append(newTapeTransfer+(None,))
      except Exception:
        # exceptions caught here mean we could not get the info we wanted on the
        # process we were looking at. We only ignore this process for this round
        # as it's probably not a tape process (or it would not have raised an exception)
        pass
    # convert the inodes in the first list into host names
    inode2host = self._parsesockettable([item[2] for item in newTapeTransfers])
    # renew the tapeTransfers list
    self.tapelock.acquire()
    try:
      # first reset the list
      self.tapeTransfers = []
      # then refill it
      for ttype, ttime, fileid, inode in newTapeTransfers:
        try:
          thost = inode2host[inode]
        except KeyError:
          thost = '?'
        self.tapeTransfers.append((ttype, ttime, thost, fileid))
    finally:
      self.tapelock.release()

  def populate(self):
    '''populates the list of ongoing transfers from the system.
    Then synchronize with the stager DB (through the transfer manager)
    so that transfers which are no more running but were not ended
    properly in the databases are ended.
    Note that this is linux specific code.'''
    # 'populating running scripts from system' message
    dlf.writedebug(msgs.POPULATING)
    # get a random scheduler host
    scheduler = self.config.getValue('DiskManager', 'ServerHosts').split()[0]
    # loop over all processes
    pids = [pid for pid in os.listdir('/proc') if pid.isdigit()]
    leftOvers = {}
    for pid in pids:
      try:
        cmdline = open(os.path.sep+os.path.join('proc', pid, 'cmdline'), 'rb').read()
        # find out the ones concerning us
        if cmdline.startswith('/usr/bin/stagerjob') or cmdline.startswith('/usr/bin/d2dtransfer'):
          # get all details we need
          args = cmdline.split('\0')
          transferid = args[4]
          reqid = args[2]
          notifFileName = args[-1][7:] # drop the leading 'file://'
          transfertype = 'standard'
          if args[0] == '/usr/bin/d2dtransfer':
            transfertype = 'd2ddest'
          arrivalTime = os.stat(os.path.sep+os.path.join('proc', pid, 'cmdline'))[-2]
          startTime = arrivalTime
          # create the entry in the list of running transfers, associated to the first scheduler we find
          self.transfers.add((transferid, scheduler, tuple(args), notifFileName, None, transfertype, arrivalTime, startTime))
          # keep in memory that this was a rebuilt entry
          leftOvers[transferid] = int(pid)
          # 'Found transfer already running' message
          fileid = (args[8], int(args[6]))
          dlf.write(msgs.FOUNDTRANSFERALREADYRUNNING, subreqid=transferid, reqid=reqid, fileid=fileid)
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
          self.connections.syncRunningTransfers(scheduler, socket.getfqdn(), tuple(leftOvers.keys()), timeout=timeout)
          break
        except connectionpool.Timeout:
          # as long as we get a timeout, we retry. This will not last. We still wait a little bit
          time.sleep(0.1)
    except Exception, e:
      # 'Exception caught when trying to synchronize running transfers with the database. Giving up' message
      dlf.writeerr(msgs.SYNCRUNTRANSFERFAILED, Type=str(e.__class__), Message=str(e))
    # finally return
    return leftOvers

  def add(self, transferid, scheduler, transfer, notifyFileName, process, transfertype, arrivalTime, startTime=None):
    '''add a new running transfer to the list'''
    self.lock.acquire()
    try:
      if startTime == None:
        startTime = time.time()
      self.transfers.add((transferid, scheduler, transfer, notifyFileName, process, transfertype, arrivalTime, startTime))
    finally:
      self.lock.release()

  def nbTransfers(self, reqUser=None, detailed=False):
    '''returns number of running transfers and number of running slots, plus details
    per protocol if the detailed paremeter is true.
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
      for transfertuple in self.transfers:
        transfer = transfertuple[2]
        transfertype = transfertuple[5]
        if transfertype in ('d2dsrc', 'd2ddest'):
          protocol = transfertype
          user = 'stage'
        else:
          protocol = transfer[10]
          try:
            user = pwd.getpwuid(int(transfer[20]))[0]
          except KeyError:
            user = transfer[20]
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
    # then we go for the tape transfers
    if not reqUser or reqUser == 'stage':
      self.tapelock.acquire()
      try:
        for transfertuple in self.tapeTransfers:
          transfertype = transfertuple[0]
          n = n + 1
          nbslots = self.config.getValue('DiskManager', transfertype+'Weight', None, int)
          ns = ns + nbslots
          if transfertype not in nproto:
            nproto[transfertype] = 0
          nproto[transfertype] = nproto[transfertype] + 1
          if transfertype not in nsproto:
            nsproto[transfertype] = 0
          nsproto[transfertype] = nsproto[transfertype] + nbslots
      finally:
        self.tapelock.release()
    if detailed:
      return n, tuple(nproto.items()), ns, tuple(nsproto.items())
    else:
      return n, ns

  def nbUsedSlots(self):
    '''returns number of slots occupied by running transfers'''
    n = 0
    # regular transfers first
    self.lock.acquire()
    try:
      for transfertuple in self.transfers:
        transfer = transfertuple[2]
        transfertype = transfertuple[5]
        if transfertype in ('d2dsrc', 'd2ddest'):
          protocol = transfertype
        else:
          protocol = transfer[10]
        n = n + self.config.getValue('DiskManager', protocol+'Weight', None, int)
    finally:
      self.lock.release()
    # and now tape transfers
    self.tapelock.acquire()
    try:
      for transfertuple in self.tapeTransfers:
        transfertype = transfertuple[0]
        n = n + self.config.getValue('DiskManager', transfertype+'Weight', None, int)        
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
      for transfertuple in self.transfers:
        transferid, scheduler, transfer, notifyFileName, process, transfertype = transfertuple[:6]
        # check whether the transfer is over
        isEnded = False
        if transferid in self.leftOverTransfers:
          # special care for left over transfers, we use a signal 0 for them as they are not our children
          try :
            pid = self.leftOverTransfers[transferid]
            os.kill(pid, 0)
            # a process with this pid exists, now is it really our guy or something new ?
            cmdline = open(os.path.sep+os.path.join('proc', str(pid), 'cmdline'), 'rb').read().split('\0')
            if len(cmdline) < 5 or transferid != cmdline[4]:
              # it's a new one, not our guy
              isEnded = True
          except OSError:
            # process is dead
            isEnded = True
          if isEnded:
            rc = -1
            del self.leftOverTransfers[transferid]
        elif process != None:
          # check regular transfers (non left over), except for d2dsrc transfers as they have no process
          # for this case, the transfer is our child, so we can poll
          if self.fake:
            isEnded = True
          else:
            rc = process.poll(-1)
            isEnded = (rc!=None)
        if isEnded:
          # get fileid
          fileid = (transfer[8], int(transfer[6]))
          # "transfer ended message"
          dlf.writedebug(msgs.TRANSFERENDED, subreqid=transferid, reqid=transfer[2], fileid=fileid, ReturnCode=rc)
          # remove the notification file
          try:
            os.remove(notifyFileName)
          except OSError:
            # already removed ? that's fine
            pass
          # append to list of ended transfers
          ended.append(transferid)
          # in case of disk to disk copy, remember to inform source
          if transfertype == 'd2ddest':
            sourcesToBeInformed.append((scheduler, transferid, fileid, transfer[2]))
          # in case of transfers killed by a signal, remember to inform the DB
          if rc < 0:
            if scheduler not in killedTransfers:
              killedTransfers[scheduler] = []
            killedTransfers[scheduler].append((transferid, fileid, rc, 'Transfer has been killed', transfer[2]))
      # cleanup ended transfers
      self.transfers = set(transfer for transfer in self.transfers if transfer[0] not in ended)
    finally:
      self.lock.release()
    # get the admin timeout
    timeout = self.config.getValue('TransferManager', 'AdminTimeout', 5, float)
    # inform schedulers of disk to disk transfers that are over
    for scheduler, transferid, fileid, reqid in sourcesToBeInformed:
      try:
        self.connections.d2dend(scheduler, transferid, reqid, timeout=timeout)
      except Exception, e:
        # "Failed to inform scheduler that a d2d transfer is over" message
        dlf.writeerr(msgs.INFORMTRANSFERISOVERFAILED, Scheduler=scheduler, subreqid=transferid, reqid=reqid, fileid=fileid, Type=str(e.__class__), Message=str(e))
    # inform schedulers of transfers killed
    for scheduler in killedTransfers:
      try:
        self.connections.transfersKilled(scheduler, tuple(killedTransfers[scheduler]), timeout=timeout)
      except Exception, e:
        for transferid, fileid, rc, msg, reqid in killedTransfers[scheduler]:
          # "Failed to inform scheduler that transfers were killed by signals" message
          dlf.writeerr(msgs.INFORMTRANSFERKILLEDFAILED, Scheduler=scheduler, subreqid=transferid, reqid=reqid, fileid=fileid, Message=msg)

  def d2dend(self, transferid):
    '''called when a disk to disk copy ends and we are the source of it'''
    self.lock.acquire()
    try:
      self.transfers = set(transfer for transfer in self.transfers if transfer[0] != transferid)
    finally:
      self.lock.release()

  def listTransfers(self, reqUser=None):
    '''lists running transfers'''
    res = []
    n = 0
    # first list the standard transfers
    self.lock.acquire()
    try:
      for transfertuple in self.transfers:
        transferid, scheduler, transfer = transfertuple[:3]
        transfertype, arrivalTime, runTime = transfertuple[5:]
        if transfertype == 'standard':
          try:
            user = pwd.getpwuid(int(transfer[20]))[0]
          except KeyError:
            user = transfer[20]
        else:
          user = 'stage'
        if not reqUser or user == reqUser:
          res.append((transferid, transfer[6], scheduler, user, 'RUN', transfertype, arrivalTime, runTime))
          n = n + 1
          if n >= 100: # give up with full listing if too many transfers
            break
    finally:
      self.lock.release()
    # then add the tape ones
    self.tapelock.acquire()
    try:
      for transfertype, startTime, remotehost, fileid in self.tapeTransfers:
        res.append(('-', fileid, remotehost, 'stage', 'TAPE', transfertype, startTime, startTime))
    finally:
      self.tapelock.release()
    return res

  def transferset(self):
    '''Lists all pending and running transfers'''
    self.lock.acquire()
    try:
      # retrieve transferid and reqid
      return set([(transfertuple[0], transfertuple[2][2]) for transfertuple in self.transfers])
    finally:
      self.lock.release()

  def listRunningD2dSources(self):
    '''lists running d2dsrc transfers'''
    self.lock.acquire()
    try:
      # retrieve transferid, transfer and arrivalTime for transfertype 'd2dsrc'
      return [(transfertuple[0], transfertuple[2], transfertuple[6]) for transfertuple in self.transfers if transfertuple[5] == 'd2dsrc']
    finally:
      self.lock.release()

  def anyTransfersFromScheduler(self, reqscheduler):
    '''Tells whether any transfer is running that is handled by the given scheduler'''
    self.lock.acquire()
    try:
      # go through the transfer
      for transfertuple in self.transfers:
        # Stop whenever we find one
        if reqscheduler == transfertuple[1]:
          return True
      # No transfer found
      return False
    finally:
      self.lock.release()
