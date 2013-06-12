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

import pwd, subprocess, re, time

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


def getProcessStartTime(pid):
  '''Little utility able to get the start time of a process
  Note that this is linux specific code'''
  timeasstr = subprocess.Popen(['ps', '-o', 'etime=', '-p', str(pid)],
                               stdout=subprocess.PIPE).stdout.read().strip()
  m = re.match('(?:(?:(\d+)?-)?(\d+):)?(\d+):(\d+)', timeasstr)
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
  if cmdLine.startswith('/usr/bin/stagerjob') or cmdLine.startswith('/usr/bin/d2dtransfer'):
    # parse command line
    cmdLine = cmdLine.split('\0')
    executable = cmdLine[0]
    args = dict([cmdLine[i:i+2] for i in range(1, len(cmdLine), 2)])
    # create the transfer object
    if executable == '/usr/bin/stagerjob':
      _unused_clientType, clientIPAddress, clientPort = args['-C'].split(':')
      diskServer, mountPoint = args['-R'].split(':')
      creationTime = int(args['-t'])
      transfer = Transfer(args['-s'], args['-r'], (args['-H'], int(args['-F'])),
                          int(args['-u']), int(args['-g']), args['-S'], creationTime,
                          args['-p'], args['-i'], TransferType.STD, args['-m'], clientIPAddress,
                          int(clientPort), int(args['-X']), diskServer, mountPoint)
    elif executable == '/usr/bin/d2dtransfer':
      diskServer, mountPoint = args['-R'].split(':')
      creationTime = int(args['-t'])
      transfer = D2DTransfer(args['-s'], args['-r'], (args['-H'], int(args['-F'])),
                             int(args['-u']), int(args['-g']), args['-S'], creationTime,
                             TransferType.D2DDST, 'd2ddest', int(args['-D']), int(args['-X']),
                             diskServer, mountPoint)
    # wrap into a RunningTransfer object
    return RunningTransfer(scheduler, None, creationTime, transfer)
  else:
    return None


def transferToCmdLine(transfer):
  '''creates a cmdLine for launching a given transfer'''
  # find out if we have a regular transfer or disk 2 disk copies
  if transfer.transferType not in (TransferType.D2DDST, TransferType.STD):
    raise TypeError, 'no command line for transfer of type %s' % TransferType.toStr(transfer.transferType)
  else:
    # first the executable
    if transfer.transferType == TransferType.D2DDST:
      cmdLine = ['/usr/bin/d2dtransfer']
    else:
      cmdLine = ['/usr/bin/stagerjob']
    # then common options
    cmdLine.append('-r')
    cmdLine.append(transfer.reqId)
    cmdLine.append('-s')
    cmdLine.append(transfer.transferId)
    cmdLine.append('-F')
    cmdLine.append(str(transfer.fileId[1]))
    cmdLine.append('-H')
    cmdLine.append(transfer.fileId[0])
    # then type dependent options
    if transfer.transferType == TransferType.D2DDST:
      cmdLine.append('-D')
      cmdLine.append(str(transfer.destDiskCopyId))
      cmdLine.append('-X')
      cmdLine.append(str(transfer.srcDiskCopyId))
    else:
      cmdLine.append('-p')
      cmdLine.append(transfer.protocol)
      cmdLine.append('-i')
      cmdLine.append(str(transfer.srId))
      cmdLine.append('-T')
      cmdLine.append(str(transfer.reqType))
      cmdLine.append('-m')
      cmdLine.append(transfer.flags)
      cmdLine.append('-C')
      cmdLine.append('129:'+transfer.clientIpAddress+':'+str(transfer.clientPort))
      cmdLine.append('-X')
      cmdLine.append(str(transfer.clientSecure))
    # back to common options
    cmdLine.append('-u')
    cmdLine.append(str(transfer.euid))
    cmdLine.append('-g')
    cmdLine.append(str(transfer.egid))
    cmdLine.append('-S')
    cmdLine.append(transfer.svcClassName)
    cmdLine.append('-t')
    cmdLine.append(str(transfer.creationTime))
    cmdLine.append('-R')
    cmdLine.append(transfer.diskServer+':'+transfer.mountPoint)
    return cmdLine


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
    raise ValueError, 'parameter of tupleToTransfer was no a valid tuple of tuples : %s' % str(t)
  transferType = d['transferType']
  try:
    if transferType == TransferType.STD:
      del d['transferType']
      return Transfer(**d)
    elif transferType == TransferType.D2DSRC or transferType == TransferType.D2DDST:
      return D2DTransfer(**d)
    else:
      raise ValueError, 'unknown transferType found in tupleToTransfer : %d' % d['transferType']
  except KeyError:
    raise ValueError, 'no transferType in tuple given to tupleToTransfer : %s' % str(d)


class BaseTransfer(object):
  '''little container describing a basic transfer'''
  def __init__(self, transferId, reqId, fileId, euid, egid,
               svcClassName, creationTime, transferType, diskServer, mountPoint):
    '''constructor'''
    self.transferId = transferId
    self.reqId = reqId
    self.fileId = fileId
    self.euid = euid
    self.egid = egid
    self.svcClassName = svcClassName
    self.creationTime = creationTime
    self.diskServer = diskServer
    self.mountPoint = mountPoint
    self.transferType = transferType

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

  
class Transfer(BaseTransfer):
  '''little container describing a regular transfer'''
  def __init__(self, transferId, reqId, fileId, euid, egid, svcClassName, creationTime,
               protocol, srId, reqType, flags, clientIpAddress, clientPort,
               clientSecure, diskServer='', mountPoint=''):
    '''constructor'''
    super(Transfer, self).__init__(transferId, reqId, fileId, euid, egid,
                                   svcClassName, creationTime, TransferType.STD, diskServer, mountPoint)
    self.protocol = protocol
    self.srId = srId
    self.reqType = reqType
    self.flags = flags
    self.clientIpAddress = clientIpAddress
    self.clientPort = clientPort
    self.clientSecure = clientSecure


class D2DTransfer(BaseTransfer):
  '''little container describing a disk to disk transfer'''
  def __init__(self, transferId, reqId, fileId, euid, egid, svcClassName, creationTime, transferType,
               diskServer='', mountPoint='', isSrcRunning=False):
    '''constructor'''
    super(D2DTransfer, self).__init__(transferId, reqId, fileId, euid, egid,
                                      svcClassName, creationTime, transferType, diskServer, mountPoint)
    self.isSrcRunning = isSrcRunning

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
  def __init__(self, scheduler, process, startTime, transfer, localPath):
    '''constructor'''
    self.scheduler = scheduler
    self.process = process
    self.startTime = startTime
    self.transfer = transfer
    self.localPath = localPath
