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

import pwd, subprocess, re, time, os, stat
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


def getProcessStartTime(pid):
  '''Little utility able to get the start time of a process
  Note that this is linux specific code'''
  timeasstr = subprocess.Popen(['ps', '-o', 'etime=', '-p', str(pid)],
                               stdout=subprocess.PIPE).stdout.read().strip()
  m = re.match('(?:(?:(\\d+)?-)?(\\d+):)?(\\d+):(\\d+)', timeasstr)
  if m:
    elapsed = int(m.group(3))*60+int(m.group(4))
    if m.group(1):
      elapsed += int(m.group(1))*86400
    if m.group(2):
      elapsed += int(m.group(2))*3600
    return time.time() - elapsed
  else:
    raise ValueError('No such process')


def cmdLineToTransfer(cmdLine, scheduler):
  '''creates a RunningTransfer object from the command line that launched the transfer.
  Depending on the command, the appropriate type of transfer will be created inside the
  RunningTransfer. In case the command is not recognized, None is returned'''
  # find out if we have a regular transfer or disk 2 disk copies
  if cmdLine.startswith('/usr/bin/stagerjob'):
    # parse command line
    cmdLine = cmdLine.split('\0')
    executable = cmdLine[0]
    args = dict([cmdLine[i:i+2] for i in range(1, len(cmdLine), 2)])
    # create the transfer object
    if executable == '/usr/bin/stagerjob':
      _unused_clientType, clientIpAddress, clientPort = args['-C'].split(':')
      diskServer, mountPoint = args['-R'].split(':')
      creationTime = int(args['-t'])
      transfer = Transfer(args['-s'], args['-r'], (args['-H'], int(args['-F'])),
                          int(args['-u']), int(args['-g']), args['-S'], creationTime,
                          args['-p'], args['-i'], TransferType.STD, args['-m'], clientIpAddress,
                          int(clientPort), int(args['-X']), diskServer, mountPoint)
    # wrap into a RunningTransfer object
    return RunningTransfer(scheduler, None, creationTime, transfer, '')
  else:
    return None


def cmdLineToTransferId(cmdLine):
  '''extracts transferId from a command line that launched a given transfer'''
  args = dict([cmdLine[i:i+2] for i in range(1, len(cmdLine), 2)])
  return args['-s']


def tupleToTransfer(t):
  '''creates a transfer object from a tuple of tuples which actually contains the
  dictionnary of members and their values. Depending on the reqType member,
  the appropriate object will be created.
  See BaseTransfer.asTuple for the opposite direction'''
  try:
    d = dict(t)
  except TypeError:
    raise ValueError('parameter of tupleToTransfer was no a valid tuple of tuples : %s' % str(t))
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
    '''get user name, build form euid and local users'''
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
               protocol, srId, reqType, flags, clientIpAddress, clientPort,
               clientSecure, diskServer='', mountPoint='', submissionTime=0,
               destDcPath='', moverFd=-1):
    '''constructor'''
    super(Transfer, self).__init__(transferId, reqId, fileId, euid, egid, svcClassName,
                                   creationTime, TransferType.STD, diskServer, mountPoint, submissionTime)
    self.protocol = protocol
    self.srId = srId
    self.reqType = reqType
    self.flags = flags
    self.clientIpAddress = clientIpAddress
    self.clientPort = clientPort
    self.clientSecure = clientSecure
    self.destDcPath = destDcPath
    self.moverFd = moverFd

  def toCmdLine(self):
    '''returns the command line for launching the mover for this transfer'''
    # find out if we have a regular transfer or disk 2 disk copies
    if self.transferType not in (TransferType.D2DDST, TransferType.STD):
      raise TypeError('no command line for transfer of type %s' % TransferType.toStr(self.transferType))
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
    elif self.protocol == 'gsiftp':
      cmdLine = ['/usr/sbin/globus-gridftp-server']
      cmdLine.append('-i')    # inetd mode
      cmdLine.append('-d')
      cmdLine.append(Transfer.configuration.getValue('GSIFTP', 'LOGLEVEL', 'ALL'))
      #cmdLine.append('-auth-level 0')
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
      raise TypeError('No valid mover for protocol %s' % self.protocol)
    return cmdLine

  def getEnvironment(self):
    '''returns the OS environment for the execution of the mover for this transfer'''
    if self.destDcPath == '':
      raise EnvironmentError('Not a valid destination path (%s) for the mover' % self.destDcPath)
    moverEnv = os.environ.copy()
    if self.protocol == 'gsiftp':
      if self.flags == 'r':
        moverEnv['ACCESS_MODE'] = 'r'
      elif self.flags == 'w':
        moverEnv['ACCESS_MODE'] = 'w'
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
      raise TypeError('No valid port range for protocol %s' % self.protocol)
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
      raise TypeError('No valid timeout value for protocol %s' % self.protocol)
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
  def __init__(self, transferType, startTime, clientHost, fileId, mountPoint, lastTimeViewed):
    '''constructor'''
    self.transferType = transferType
    self.startTime = startTime
    self.clientHost = clientHost
    self.fileId = fileId
    self.mountPoint = mountPoint
    self.lastTimeViewed = lastTimeViewed


class RunningTransfer(object):
  '''little container describing a running transfer'''
  def __init__(self, scheduler, process, startTime, transfer, localPath=None):
    '''constructor'''
    self.scheduler = scheduler
    self.process = process
    self.startTime = startTime
    self.transfer = transfer
    self.localPath = localPath
