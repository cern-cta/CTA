#!/usr/bin/python
#/******************************************************************************
# *                   activitycontrol.py
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
# * the activity control thread of the disk server manager daemon of CASTOR
# *
# * @author Castor Dev team, castor-dev@cern.ch
# *****************************************************************************/

"""activity control thread of the disk server manager daemon of CASTOR."""

import os, sys, threading, time, subprocess
import Crypto.Hash.SHA as SHA1
import Crypto.PublicKey.RSA as RSA
import Crypto.Signature.PKCS1_v1_5 as PKCS1
import base64
import connectionpool, dlf, clientsreplier
from commonexceptions import TransferCanceled, TransferFailed, SourceNotStarted
from diskmanagerdlf import msgs
from transfer import TransferType, RunningTransfer

def signBase64(content, RSAKey):
    '''signs content with the given key and encode the result in base64'''
    contentHash = SHA1.new(content)
    signer = PKCS1.new(RSAKey)
    signature = signer.sign(contentHash)
    return base64.b64encode(signature)

def buildXrootURL(self, diskserver, path):
    '''Builds a xroot valid url for the given path on the given diskserver'''
    # base url and key parameter
    url = 'root://'+diskserver+':1095//' + path + '?'
    opaque_dict = {'castor2fs.pfn1' : path,
                   'castor2fs.exptime' : str(int(time.time()) + 3600)}

    # signature part
    try:
        # get Xroot RSA key
        keyFile = self.configuration.getValue('XROOT', 'PrivateKey', \
                                              '/opt/xrootd/keys/key.pem')
        key = RSA.importKey(open(keyFile, 'r').read())
        # sign opaque part obtained by concatenating the values
        opaque_token = ''.join([opaque_dict['castor2fs.pfn1'],
                                "0", # accessop
                                opaque_dict['castor2fs.exptime']])
        signature = signBase64(opaque_token, key)
        opaque = ""

        # build the opaque info
        for key, val in opaque_dict.iteritems():
          opaque += key + '=' + val + '&'

        url += opaque + 'castor2fs.signature=' + signature
        return url
    except Exception, e:
        # avoid raising standard exception, as some are used for dedicated purposes
        raise Exception(str(e))

class ActivityControlThread(threading.Thread):
  '''activity control thread.
  This thread is responsible for starting new transfers when free slots are available'''

  def __init__(self, runningTransfers, transferQueue, configuration, clientsReplier, clientsListener, fake):
    '''constructor'''
    super(ActivityControlThread, self).__init__(name='ActivityControl')
    self.alive = True
    self.runningTransfers = runningTransfers
    self.transferQueue = transferQueue
    self.configuration = configuration
    self.clientsReplier = clientsReplier
    self.clientsListener = clientsListener
    self.fake = fake
    # last time we've tried to schedule a transfer. This is used by the ActivityControlChecker
    # to find out whether the ActivityControl thread is dead or not
    self.lastschedattempt = 0
    # start the thread
    self.setDaemon(True)
    self.start()

  def checkSpaceLeftOnFS(self, transfer):
    ''' check whether there is space available for a given transfer'''
    # check whether this transfer will write data
    if transfer.transferType == TransferType.D2DSRC:
      return True
    if transfer.transferType == TransferType.STD and transfer.flags == 'r':
      return True
    # find out the limit in terms of free space, from the config file
    minFreeSpacePerc = self.configuration.getValue('DiskManager', 'FSMinAllowedFreeSpace', 0.02, float)
    if transfer.mountPoint:
      # get status of the filesystem
      stat = os.statvfs(transfer.mountPoint)
      availableSpace = stat.f_bavail * stat.f_frsize
      totalSpace = stat.f_blocks * stat.f_bsize
      # do the check
      return availableSpace > minFreeSpacePerc * totalSpace
    else:
      return True

  def startD2DTransfer(self, scheduler, transfer, srcDcPath, destDcPath):
    '''effectively starts a disk to disk transfer'''
    # build command line
    srcDS, srcPath = srcDcPath.split(':', 1)
    cmdLine = ['xrdcp', buildXrootURL(self, srcDS, srcPath), \
                        buildXrootURL(self, 'localhost', destDcPath)]
    # "Transfer starting" message
    dlf.write(msgs.TRANSFERSTARTING, subreqid=transfer.transferId,
              reqid=transfer.reqId, fileId=transfer.fileId,
              TtransferType=TransferType.toStr(transfer.transferType),
              cmdLine=' '.join(cmdLine))
    # start transfer process
    process = 0
    try:
      process = subprocess.Popen(cmdLine, close_fds=True, stderr=subprocess.PIPE)
    except Exception, e:
      # avoid raising standard exception, as some are used for dedicated purposes
      raise Exception(str(e))
    self.runningTransfers.add(RunningTransfer(scheduler, process, time.time(), transfer, destDcPath))

  def startMover(self, transfer):
    '''effectively starts the required mover after the client had initiated its connection'''
    try:
      # log "Executing mover"
      dlf.write(msgs.MOVERSTARTING, cmdLine=' '.join(transfer.toCmdLine()), env=transfer.getEnvironment(), moverFd=transfer.moverFd)
      # execute mover, with the environment specific to this transfer. Note the 'inetd mode': we pass the fd
      # of the connection that has just been opened with the client as stdin/out/err.
      process = subprocess.Popen(transfer.toCmdLine(), close_fds=True, \
                                 stdin=transfer.moverFd, stdout=transfer.moverFd, stderr=transfer.moverFd, \
                                 env=transfer.getEnvironment())
      # and store the process in our set of running transfers
      self.runningTransfers.setProcess(transfer.transferId, process)
    except Exception, e:
      # log "Failed to execute mover" with all details about the transfer
      dlf.writeerr(msgs.MOVERSTARTFAILED, transfer=transfer, error=str(e))
      # clean up this transfer from the list of running transfers
      self.runningTransfers.remove([transfer.transferId])

  def run(self):
    '''main method, containing the infinite loop'''
    while self.alive:
      try:
        # check number of running transfers against limit
        if self.runningTransfers.nbUsedSlots() < self.configuration.getValue('DiskManager', 'NbSlots', 0, int):
          # get a new transfer from the transferQueue
          qTransfer = self.transferQueue.get()
          self.lastschedattempt = time.time()
          # check whether we got something or timed out
          if qTransfer == None:
            # We timed out, retry straight
            continue
          scheduler, transfer = qTransfer.scheduler, qTransfer.transfer
          try:
            # in case of transfers wanting to write data, we check that space is available
            if not self.checkSpaceLeftOnFS(transfer):
              # not enough space, we cancel the transfer
              connectionpool.connections.transfersCanceled(scheduler, ((transfer.asTuple(), 28, 'No space left on device'),)) # ENOSPC
            else:
              # notifies the central scheduler that we want to start this transfer
              # this may raise a ValueError exception if we should give up (e.g. the
              # job has already started somewhere else)
              result = connectionpool.connections.transferStarting(scheduler, transfer.asTuple())
              if transfer.transferType == TransferType.D2DSRC:
                # in this case do not actually start for real, only take note
                # "Transfer starting" message
                dlf.write(msgs.TRANSFERSTARTING, subreqid=transfer.transferId,
                          reqid=transfer.reqId, fileId=transfer.fileId,
                          transferType=TransferType.toStr(transfer.transferType))
                self.runningTransfers.add(RunningTransfer(scheduler, None, time.time(), transfer))
              elif transfer.transferType == TransferType.D2DDST:
                # here we have to launch the actual transfer
                srcPath, dstPath = result
                self.startD2DTransfer(scheduler, transfer, srcPath, dstPath)
              else:
                # this is a regular transfer, we have to answer the client
                # store the destination path in the transfer object
                transfer.destDcPath = result
                # "Transfer starting" message
                dlf.write(msgs.TRANSFERSTARTING, subreqid=transfer.transferId,
                          reqid=transfer.reqId, fileId=transfer.fileId,
                          transferType=TransferType.toStr(transfer.transferType))
                dlf.writedebug(msgs.TRANSFERSTARTING, transfer=transfer)
                if not self.fake:
                  if transfer.protocol != 'xroot':
                    # prepare the listening port for the client to connect. The mover is executed only after the client connects back
                    moverPort = self.clientsListener.createSocketForMover(qTransfer, self.startMover)
                    destPath = transfer.transferId
                  else:
                    # xroot is special here: we don't create a socket for the mover, but instead we tell
                    # the xroot server (through the redirector) to use our mover handler port for telling
                    # us when the file is effectively opened and closed
                    moverPort = self.configuration.getValue('DiskManager', 'MoverHandlerPort', 15511)
                    # and on top, we have to tell the physical destination as the path in the response
                    destPath = transfer.destDcPath
                  # prepare the response to the client with the created port
                  ioresp = clientsreplier.IOResponse(clientsreplier.IOResponse.READY, '', 0, transfer.fileId[1], transfer.transferId, \
                                                     0, '', transfer.reqId, destPath, moverPort, \
                                                     'rfio' if transfer.protocol == 'rfio3' else transfer.protocol)   # rfio and rfio3 are the same thing
                  # and asynchronously answer client
                  self.clientsReplier.sendResponse(qTransfer, transfer.clientIpAddress, transfer.clientPort, ioresp)
                # from now on we consider the transfer running, even if the client didn't come yet:
                # this ensures that the slot is reserved for it. See how it is taken out of the running
                # transfers in clientslistener in case of failures or time outs.
                self.runningTransfers.add(RunningTransfer(qTransfer.scheduler, None, time.time(), qTransfer.transfer))
          except connectionpool.Timeout:
            # we timed out in the call to transfersCanceled or transferStarting. We need to try again
            # thus we put the transfer into the priority queue and inform the scheduler
            try:
              self.transferQueue.putPriority(scheduler, transfer)
              connectionpool.connections.transferBackToQueue(scheduler, transfer.asTuple())
              # 'Timeout when trying to start/cancel transfer. Putting it back to the queue' message
              dlf.writenotice(msgs.TRANSFERSTARTTIMEOUT, subreqid=transfer.transferId,
                              reqid=transfer.reqId, fileId=transfer.fileId)
            except Exception, e:
              # 'Failed to start transfer and got timeout when putting back to queue'
              dlf.writenotice(msgs.TRANSFERBACKTOQUEUEFAILED, subreqid=transfer.transferId,
                              reqid=transfer.reqId, fileId=transfer.fileId, originalError='Timeout', error=e)
          except TransferCanceled, e:
            # 'Transfer start canceled' message
            dlf.writedebug(msgs.TRANSFERSTARTCANCELED, reason=e.args, subreqId=transfer.transferId, fileId=transfer.fileId)
            # the transfer has already started somewhere else, or has been canceled, so give up
          except TransferFailed, e:
            # 'Transfer start canceled' message
            dlf.write(msgs.TRANSFERSTARTCANCELED, reason=e.args, subreqId=transfer.transferId, fileId=transfer.fileId)
            # this is a permanent failure because of the stager, so try and inform the client that the transfer cannot take place
            ioresp = clientsreplier.IOResponse(clientsreplier.IOResponse.FAILED, '', 0, transfer.fileId, transfer.transferId, \
                                               1725, e.message, transfer.reqId, transfer.reqId, 0, transfer.protocol)  # 1725 = ESTREQCANCELED
            # use asynch response: exceptions are handled in the clientsReplier thread
            self.clientsReplier.sendResponse((qTransfer, transfer.clientIpAddress, transfer.clientPort, ioresp))
          except SourceNotStarted, e:
            # we have tried to start a disk to disk copy and the source is not yet ready
            # we will put it in a pending queue
            # "Start postponed until source is ready" message
            dlf.write(msgs.POSTPONEDFORSRCNOTREADY, subreqid=transfer.transferId,
                      reqid=transfer.reqId, fileId=transfer.fileId, msg=str(e))
            # put the transfer into the pending queue
            self.transferQueue.d2dDestReady(scheduler, transfer)
          except Exception, e:
            # startup of the transfer failed with unexpected error
            # We need to try again thus we put the transfer into the queue and inform the scheduler
            # note that we put it now at the end of the queue (opposite approach compared to
            # the Timeout case). This allows other transfers to go through in cas of persistent
            # errors (e.g. Connection refused because of a dead transfermanagerd)
            try:
              self.transferQueue.put(scheduler, transfer)
              connectionpool.connections.transferBackToQueue(scheduler, transfer.asTuple())
              # 'Failed to start transfer. Putting it back to the queue' message
              dlf.writeerr(msgs.TRANSFERSTARTINGFAILED, subreqid=transfer.transferId,
                           reqid=transfer.reqId, fileId=transfer.fileId, error=e)
            except Exception, e2:
              # clear this exception context, i.e. the Timeout, so that we log the original error
              sys.exc_clear()
              # 'Failed to start transfer and got timeout when putting back to queue'
              dlf.writeerr(msgs.TRANSFERBACKTOQUEUEFAILED, subreqid=transfer.transferId,
                           reqid=transfer.reqId, fileId=transfer.fileId, originalError=e, error=e2)
            # wait a bit to avoid a tight loop in case of persistent errors
            time.sleep(.1)
        else:
          # all slots are full, wait a bit before checking again
          time.sleep(.05)
      except Exception:
        # "Caught exception in ActivityControl thread" message
        dlf.writeerr(msgs.ACTIVITYCONTROLEXCEPTION)
        time.sleep(1)

  def stop(self):
    '''stops processing in this thread'''
    self.alive = False
