#!/usr/bin/python
#/******************************************************************************
# *                   clientslistener.py
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
# * the clients listener thread of the disk server manager daemon of CASTOR
# *
# * @author Castor Dev team, castor-dev@cern.ch
# *****************************************************************************/

"""clients listener thread of the disk server manager daemon of CASTOR."""

import threading, socket, select, time, random
import connectionpool, dlf
from diskmanagerdlf import msgs


class MoverSocket(object):
  '''A little container describing an open socket for a mover'''
  def __init__(self, qTransfer, callback, socket):
    self.qTransfer = qTransfer
    self.callback = callback
    self.socket = socket
    self.expirationTime = time.time() + qTransfer.transfer.getTimeout()


class ClientsListenerThread(threading.Thread):
  '''the clients listener thread.
  This thread is responsible for listening to clients callbacks, so that movers
  can be executed in inetd mode only when the client has already connected. In case of
  timeout, a call to transferEnded is executed to fail the transfer.
  During the listening time, the transfer slot is already reserved (cf. activitycontrol),
  so that when the client arrives, the transfer starts immediately.
  '''

  def __init__(self, runningTransfers, config):
    '''constructor'''
    super(ClientsListenerThread, self).__init__(name='ClientsListener')
    self.alive = True
    self.runningTransfers = runningTransfers
    self.config = config
    # an fd -> MoverSocket dictionary for keeping track of the outstanding movers
    self.outstandingMovers = {}
    self.clientsPoll = select.poll()
    # start the thread
    self.setDaemon(True)
    self.start()

  def createSocketForMover(self, qTransfer, callback):
    '''
    add a client to the set of polled connections; callback is a callable of type
    lambda(transfer) that is called after a successful connection from the client.
    '''
    # get the port range for the mover of this transfer
    lowPort, highPort = qTransfer.transfer.getPortRange()
    port = random.randrange(lowPort, highPort)
    while True:
      try:
        # attempt to bind and listen
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.bind((socket.gethostname(), port))
        sock.listen(0)
        break
      except Exception:
        # port is already taken, go to next one
        port += 1
        if port > highPort:
          port = lowPort
          time.sleep(1)
        continue
    # add the socket to our map of outstanding movers
    self.outstandingMovers[sock.fileno()] = MoverSocket(qTransfer, callback, sock)
    # and add the fd to the multipoll structure
    self.clientsPoll.register(sock, select.POLLIN|select.POLLPRI)
    # return the mover port for the client
    return port

  def run(self):
    '''main method, containing the infinite poll listening loop'''
    while self.alive:
      try:
        fds = self.clientsPoll.poll(1000)
        for (fd, event) in fds:
          if event == select.POLLIN or event == select.POLLPRI:
            # a client connected: drop this fd from our poll record
            self.clientsPoll.unregister(fd)
            # get the corresponding container
            # in case we don't find it, i.e. we got a client for a mover
            # that got dropped, raise error and go to next one
            moverSock = self.outstandingMovers[fd]
            del self.outstandingMovers[fd]
            # accept this incoming connection
            clientSock, addr_unused = moverSock.socket.accept()
            # keep the fd for the mover (inetd-like mode)
            moverSock.qTransfer.transfer.moverFd = clientSock.fileno()
            # now we are ready to start the mover
            moverSock.callback(moverSock.qTransfer.transfer)
          # any other event is ignored, eventually the transfer is failed by the time out
        # check timeout for all outstanding movers
        for moverSock in self.outstandingMovers.values():
          if moverSock.expirationTime < time.time():
            # the client for this mover did not come on time, fail the transfer
            self.runningTransfers.failTransfer(moverSock.qTransfer.scheduler, moverSock.qTransfer.transfer, 1004,  # SETIMEDOUT
                                               'Timed out waiting for client connection', removeFromRunningSet=True)
            # and drop it from our structures
            self.clientsPoll.unregister(moverSock.socket.fileno())
            del self.outstandingMovers[moverSock.socket.fileno()]
      except Exception, e:
        # "Caught exception in ClientsListener thread" message
        dlf.writeerr(msgs.CLIENTSLISTENEREXCEPTION, Type=str(e.__class__), Message=str(e))

  def stop(self):
    '''stops processing in this thread'''
    self.alive = False
