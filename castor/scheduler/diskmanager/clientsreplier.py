#!/usr/bin/python
#/******************************************************************************
# *                   clientsreplier.py
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
# * disk manager of the CASTOR project
# *
# * @author Castor Dev team, castor-dev@cern.ch
# *****************************************************************************/

"""disk manager daemon of CASTOR: interface to CASTOR2 clients."""

import struct, ctypes, socket, Queue, threading
import dlf
from diskmanagerdlf import msgs

class IOResponse(object):
  '''A class implementing the CASTOR2 protocol for IOResponse'''
  MAGIC = 0x1e10131       # SEND_REQUEST_MAGIC in castor/Constants.hpp
  OBJ_IOResponse = 108    # from castor/Constants.hpp
  READY = 6               # SUBREQUEST_READY
  FAILED = 7              # SUBREQUEST_FAILED

  def __init__(self, status, castorFileName, fileSize, fileId, transferId, errorCode, errorMessage,
               reqAssociated, fileName, port, protocol):
    '''constructor, generating the binary buffer'''
    self.buf = ctypes.create_string_buffer(1024)
    self.pos = 0    # current position
    # stream all data. Payload format: MAGIC(32bit) length(32bit) buffer
    self.packInt(self.MAGIC)
    self.pos += 4   # length, will put it afterwards
    self.packInt(self.OBJ_IOResponse)
    self.packInt(status)
    self.packString(castorFileName)
    self.packInt64(fileSize)
    self.packInt64(fileId)
    self.packString(transferId)
    self.packInt64(0)   # DB id, irrelevant
    self.packInt(errorCode)
    self.packString(errorMessage)
    self.packString(reqAssociated)
    self.packString(fileName)
    self.packString(socket.gethostname())  # server is always ourselves
    self.packInt(port)
    self.packString(protocol)
    # pack now the final payload length, that is current position - 2 x 32bit
    struct.pack_into('<I', self.buf, 4, self.pos-8)

  def packString(self, s):
    '''stream a string with its length in front'''
    struct.pack_into('<I%ds' % len(s), self.buf, self.pos, len(s), s)
    self.pos += 4 + len(s)

  def packInt(self, i):
    '''stream a 32bit integer'''
    struct.pack_into('<I', self.buf, self.pos, int(i))
    self.pos += 4

  def packInt64(self, i):
    '''stream a 64bit integer'''
    struct.pack_into('<Q', self.buf, self.pos, long(i))
    self.pos += 8

  def getBuffer(self):
    '''get the raw buffer to be sent over the wire'''
    return self.buf.raw.rstrip('\0')


class ClientsReplierThread(threading.Thread):
  '''The clients replier thread.
  This thread connects to CASTOR clients and sends an IOResponse. The architecture is similar
  to the stager daemon's request replier thread: a queue of responses is managed asynchronously,
  so that the sendResponse() method returns immediately.
  '''

  def __init__(self, config):
    '''constructor'''
    super(ClientsReplierThread, self).__init__(name='ClientsReplier')
    self.alive = True
    self.config = config
    self.queue = Queue.Queue()
    # start the thread
    self.setDaemon(True)
    self.start()

  def sendResponse(self, qTransfer, clientHost, clientPort, ioResponse):
    '''push a response to the queue for a given queued transfer'''
    self.queue.put((qTransfer, clientHost, clientPort, ioResponse))

  def run(self):
    '''main method, consuming the queue'''
    while self.alive:
      try:
        # get next response to send out
        qTransfer, clientHost, clientPort, ioResponse = self.queue.get(timeout=1)
      except Queue.Empty:
        continue
      try:
        # got one, create socket to connect to client
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((clientHost, clientPort))
        # send response and close the connection
        sock.send(ioResponse.getBuffer())
        sock.shutdown(socket.SHUT_RDWR)
        sock.close()
      except Exception, e:
        # "Caught exception in ClientsReplier thread" message
        dlf.writenotice(msgs.CLIENTSREPLIEREXCEPTION, error=str(e), subreqId=qTransfer.transfer.transferId,
                        clientHost=clientHost, clientPort=clientPort)
        # now we should fail the outstanding transfer, as the client will never connect,
        # but we rely on the manager thread, which fails those transfers on timeout.

  def stop(self):
    '''stops processing in this thread'''
    self.alive = False
