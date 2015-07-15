#!/usr/bin/python
#/******************************************************************************
# *                   transfer.py
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
# * small class defining a CASTOR transfer
# *
# * @author Castor Dev team, castor-dev@cern.ch
# *****************************************************************************/

'''small class defining a CASTOR transfer.
   BaseTransfer is the common abstract class while Transfer and D2dTransfer are the two real types'''

import pwd, time, os, stat
import castor_tools

class TransferType(object):
  '''Constants defining the existing types of transfer'''
  STD = 1
  D2DSRC = 2
  D2DDST = 3
  def __init__(self):
    '''empty constructor, raises an exception'''
    raise NotImplementedError
  @staticmethod
  def toStr(tType):
    '''prints a readable version of the transfer types'''
    if tType == TransferType.STD:
      return 'standard'
    elif tType == TransferType.D2DSRC:
      return 'd2dsrc'
    elif tType == TransferType.D2DDST:
      return 'd2ddest'
    else:
      return 'UNKNOWN'
  @staticmethod
  def toPreciseStr(transfer):
    '''prints a readable version of the transfer type of a transfer, with details'''
    if transfer.transferType == TransferType.STD:
      return transfer.protocol
    elif transfer.transferType in (TransferType.D2DSRC, TransferType.D2DDST):
      return TransferType.toStr(transfer.transferType) + '-' + D2DTransferType.toStr(transfer.replicationType)
    else:
      return 'UNKNOWN'

class TapeTransferType(object):
  '''Constants defining the existing types of tape transfers'''
  RECALL = 1
  MIGRATION = 2
  def __init__(self):
    '''empty constructor, raises an exception'''
    raise NotImplementedError
  @staticmethod
  def toStr(tType):
    '''prints a readable version of the tape transfer types'''
    if tType == TapeTransferType.RECALL:
      return 'recall'
    elif tType == TapeTransferType.MIGRATION:
      return 'migr'
    else:
      return 'UNKNOWN'


class D2DTransferType(object):
  '''Constants defining the existing types of d2d transfers'''
  USER = 0
  INTERNAL = 1
  DRAINING = 2
  REBALANCE = 3
  def __init__(self):
    '''empty constructor, raises an exception'''
    raise NotImplementedError
  @staticmethod
  def toStr(tType):
    '''prints a readable version of the d2d transfer types'''
    if tType == D2DTransferType.USER:
      return 'user'
    elif tType == D2DTransferType.INTERNAL:
      return 'internal'
    elif tType == D2DTransferType.DRAINING:
      return 'draining'
    elif tType == D2DTransferType.REBALANCE:
      return 'rebalance'
    else:
      return 'UNKNOWN'
  @staticmethod
  def toType(strType):
    '''returns a D2DTransferType corresponding to the given representation'''
    if strType == 'user':
      return D2DTransferType.USER
    elif strType == 'internal':
      return D2DTransferType.INTERNAL
    elif strType == 'draining':
      return D2DTransferType.DRAINING
    elif strType == 'rebalance':
      return D2DTransferType.REBALANCE
    else:
      raise ValueError('Invalid replication type %s' % strType)


def cmdLineToTransfer(cmdLine, scheduler, pid):
  '''creates a RunningTransfer object from the command line that launched the transfer.
  Depending on the command, the appropriate type of transfer will be created inside the
  RunningTransfer set. In case the command is not recognized, None is returned'''
  # find out if we have a regular transfer or disk 2 disk copies
  if cmdLine.startswith('/usr/bin/rfiod'):
    # parse command line
    cmdLine = cmdLine.split('\0')[:-1]
    args = dict([cmdLine[i:i+2] for i in range(1, len(cmdLine), 2)])
    # extract the metadata
    transferId = args['-i']
    destDcPath = args['-F']
    flags = args['-a']
    protocol = 'rfio'
    startTime = os.stat('/proc/' + str(pid)).st_ctime
  elif cmdLine.startswith('/usr/sbin/globus-gridftp-server'):
    protocol = 'gsiftp'
    startTime = os.stat('/proc/' + str(pid)).st_ctime
    # parse environment of process pid
    env = open('/proc/' + str(pid) + '/environ').read().split('\0')
    for e in env:
      kv = e.split('=')
      if kv[0] == 'UUID':
        transferId = kv[1]
      elif kv[0] == 'FULLDESTPATH':
        destDcPath = kv[1]
      elif kv[0] == 'ACCESS_MODE':
        flags = kv[1]
  else:
    # other command lines are not recognized
    return None
  fid, nshost = os.path.basename(destDcPath).split('@')
  nshost = nshost.split('.')[0]
  fileid = (nshost, int(fid))
  # create the transfer object; note that many data are missing
  transfer = Transfer(transferId, '-', fileid, -1, -1, 'unknownSvcClass', startTime, protocol, 0, 0, flags)
  # and wrap it into a RunningTransfer object
  return RunningTransfer(scheduler, None, startTime, transfer, destDcPath)


def cmdLineToTransferId(cmdLine, pid):
  '''extracts the transferId from a command line that launched a transfer'''
  if cmdLine.startswith('/usr/bin/rfiod'):
    cmdLine = cmdLine.split('\0')
    args = dict([cmdLine[i:i+2] for i in range(1, len(cmdLine), 2)])
    return args['-i']
  elif cmdLine.startswith('/usr/sbin/globus-gridftp-server'):
    env = open('/proc/' + str(pid) + '/environ').read().split('\0')
    for e in env:
      kv = e.split('=')
      if kv[0] == 'UUID':
        return kv[1]
  return None


def tupleToTransfer(t):
  '''creates a transfer object from a tuple of tuples which actually contains the
  dictionary of members and their values. Depending on the transferType member,
  the appropriate object will be created.
  See BaseTransfer.asTuple for the opposite direction'''
  try:
    d = dict(t)
  except TypeError:
    raise ValueError('parameter of tupleToTransfer was not a valid tuple of tuples : %s' % str(t))
  transferType = d['transferType']
  try:
    if transferType == TransferType.STD:
      del d['transferType']
      return Transfer(**d)
    elif transferType == TransferType.D2DSRC or transferType == TransferType.D2DDST:
      return D2DTransfer(**d)
    else:
      raise ValueError('unknown transferType found in tupleToTransfer : %d' % d['transferType'])
  except KeyError:
    raise ValueError('no transferType in tuple given to tupleToTransfer : %s' % str(d))


class BaseTransfer(object):
  '''little container describing a basic transfer'''
  def __init__(self, transferId, reqId, fileId, euid, egid,
               svcClassName, creationTime, transferType, diskServer, mountPoint, submissionTime=0):
    '''constructor'''
    self.transferId = transferId
    self.reqId = reqId
    self.fileId = fileId
    self.euid = euid
    self.egid = egid
    self.svcClassName = svcClassName
    self.creationTime = creationTime
    self.transferType = transferType
    self.diskServer = diskServer
    self.mountPoint = mountPoint
    if submissionTime > 0:
      self.submissionTime = submissionTime
    else:
      self.submissionTime = time.time()

  @property
  def user(self):
    '''get user name, built from euid and local users'''
    try:
      return pwd.getpwuid(self.euid)[0]
    except KeyError:
      return str(self.euid)

  def asTuple(self):
    '''returns this class streamed into tuples of tuples.
       See method tupleToTransfer for the opposite direction'''
    return tuple(self.__dict__.items())

  def __str__(self):
    '''a string representation of this transfer object'''
    return str(self.__dict__.items())


class Transfer(BaseTransfer):
  '''A container describing a regular transfer, with some utility methods'''
  # class level variable pointing to the CASTOR configuration singleton
  configuration = castor_tools.castorConf()

  def __init__(self, transferId, reqId, fileId, euid, egid, svcClassName, creationTime,
               protocol, srId, reqType, flags, clientIpAddress='', clientPort=0,
               diskServer='', mountPoint='', submissionTime=0, destDcPath='', moverFd=-1):
    '''constructor'''
    super(Transfer, self).__init__(transferId, reqId, fileId, euid, egid, svcClassName,
                                   creationTime, TransferType.STD, diskServer, mountPoint, submissionTime)
    self.protocol = protocol
    self.srId = srId
    self.reqType = reqType
    self.flags = flags
    self.clientIpAddress = clientIpAddress
    self.clientPort = clientPort
    self.destDcPath = destDcPath
    self.moverFd = moverFd

  def toCmdLine(self):
    '''returns the command line for launching the mover for this transfer'''
    # find out if we have a regular transfer or disk 2 disk copies
    if self.transferType not in (TransferType.D2DDST, TransferType.STD):
      raise ValueError('no command line for transfer of type %s' % TransferType.toStr(self.transferType))
    if self.destDcPath == '':
      # this transfer was not yet fully initialized by the clientsListener thread, give up
      raise Exception('Not a valid destination path (%s) for the mover' % self.destDcPath)
    # compose the command line depending on the type of transfer
    if self.protocol == 'rfio' or self.protocol == 'rfio3':
      cmdLine = ['/usr/bin/rfiod']
      cmdLine.append('-1Ulnf')
      cmdLine.append(Transfer.configuration.getValue('RFIO', 'LOGFILE', '/var/log/castor/rfiod.log'))
      cmdLine.append('-M')
      cmdLine.append(str(stat.S_IWGRP|stat.S_IWOTH))
      cmdLine.append('-i')
      cmdLine.append(self.transferId)
      cmdLine.append('-F')
      cmdLine.append(self.destDcPath)
      cmdLine.append('-a')
      cmdLine.append(self.flags)
    elif self.protocol == 'gsiftp':
      cmdLine = ['/usr/sbin/globus-gridftp-server']
      cmdLine.append('-i')    # inetd mode
      cmdLine.append('-d')
      cmdLine.append(Transfer.configuration.getValue('GSIFTP', 'LOGLEVEL', 'ALL'))
      cmdLine.append('-auth-level')
      cmdLine.append('0')     # don't run as the client user
      cmdLine.append('-control-idle-timeout')
      cmdLine.append('3600')
      cmdLine.append('-Z')
      cmdLine.append(Transfer.configuration.getValue('GSIFTP', 'NETLOGFILE', '/var/log/globus-gridftp.log'))
      cmdLine.append('-l')
      cmdLine.append(Transfer.configuration.getValue('GSIFTP', 'LOGFILE', '/var/log/gridftp.log'))
      cmdLine.append('-dsi')
      cmdLine.append('CASTOR2')   # our plugin
      cmdLine.append('-allowed-modules')
      cmdLine.append('CASTOR2')
    else:
      raise ValueError('No valid mover for protocol %s' % self.protocol)
    return cmdLine

  def getEnvironment(self):
    '''returns the OS environment for the execution of the mover for this transfer'''
    if self.destDcPath == '':
      raise EnvironmentError('Not a valid destination path (%s) for the mover' % self.destDcPath)
    moverEnv = os.environ.copy()
    if self.protocol == 'gsiftp':
      if self.flags == 'r' or self.flags == 'w':
        moverEnv['ACCESS_MODE'] = self.flags
      else:
        raise ValueError('Invalid flags value %c for transfer %s' % (self.flags, self.transferId))
      moverEnv['UUID'] = self.transferId
      moverEnv['FULLDESTPATH'] = self.destDcPath
      moverEnv['GLOBUS_TCP_PORT_RANGE'] = Transfer.configuration.getValue('GSIFTP', 'DATA_TCP_PORT_RANGE', '20000,21000')
      moverEnv['GLOBUS_TCP_SOURCE_RANGE'] = Transfer.configuration.getValue('GSIFTP', 'DATA_TCP_SOURCE_RANGE', '20000,21000')
      moverEnv['X509_USER_CERT'] = Transfer.configuration.getValue('GSIFTP', 'X509_USER_CERT', '/etc/grid-security/castor-gridftp-dsi/castor-gridftp-dsi-cert.pem')
      moverEnv['X509_USER_KEY'] = Transfer.configuration.getValue('GSIFTP', 'X509_USER_KEY', '/etc/grid-security/castor-gridftp-dsi/castor-gridftp-dsi-key.pem')
    # else no special environment is required for the other movers
    return moverEnv

  def getPortRange(self):
    '''returns the port range for the mover for this transfer'''
    if self.protocol == 'rfio' or self.protocol == 'rfio3':
      low, high = Transfer.configuration.getValue('RFIOD', 'PORT_RANGE', '50000,55000').split(',')
    elif self.protocol == 'gsiftp':
      low, high = Transfer.configuration.getValue('GSIFTP', 'CONTROL_TCP_PORT_RANGE', '20000,21000').split(',')
    else:
      raise ValueError('No valid port range for protocol %s' % self.protocol)
    return int(low), int(high)

  def getTimeout(self):
    '''returns the accept timeout for the mover of this transfer'''
    if self.protocol == 'rfio' or self.protocol == 'rfio3':
      t = Transfer.configuration.getValue('RFIOD', 'CONNTIMEOUT', 10)
    elif self.protocol == 'gsiftp':
      t = Transfer.configuration.getValue('GSIFTP', 'TIMEOUT', 180)
    elif self.protocol == 'xroot':
      t = Transfer.configuration.getValue('XROOT', 'TIMEOUT', 300)
    else:
      raise ValueError('No valid timeout value for protocol %s' % self.protocol)
    return t


class D2DTransfer(BaseTransfer):
  '''little container describing a disk to disk transfer'''
  def __init__(self, transferId, reqId, fileId, euid, egid, svcClassName, creationTime, transferType,
               replicationType, diskServer='', mountPoint='', isSrcRunning=False, submissionTime=0):
    '''constructor'''
    super(D2DTransfer, self).__init__(transferId, reqId, fileId, euid, egid, svcClassName,
                                      creationTime, transferType, diskServer, mountPoint, submissionTime)
    self.replicationType = replicationType
    self.isSrcRunning = isSrcRunning

  @property
  def protocol(self):
    '''get protocol name, either taking the actual protocol member or building one from the transferType'''
    return TransferType.toStr(self.transferType)


class TapeTransfer(object):
  '''little container describing a tape transfer'''
  def __init__(self, transferId, transferType, startTime, clientHost, fileId, mountPoint):
    '''constructor'''
    self.transferId = transferId
    self.transferType = transferType
    self.startTime = startTime
    self.clientHost = clientHost
    self.fileId = fileId
    self.mountPoint = mountPoint


class RunningTransfer(object):
  '''little container describing a running transfer'''
  def __init__(self, scheduler, process, startTime, transfer, localPath=None):
    '''constructor'''
    self.scheduler = scheduler
    self.process = process
    self.startTime = startTime
    self.transfer = transfer
    self.localPath = localPath
    # this flag is set to True by a moverhandler thread when a CLOSE call takes place
    self.ended = False
