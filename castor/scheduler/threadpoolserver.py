#!/usr/bin/python
#/******************************************************************************
# *                   transfermanagerd.py
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
# * extension of the rpyc framework with a thread pool based server where threads are reused
# * instead of being created for each new request
# *
# * @author Castor Dev team, castor-dev@cern.ch
# *****************************************************************************/

"""extension of the rpyc framework with a thread pool based server where threads are reused
instead of being created for each new request."""

import rpyc.utils.server, rpyc.core.async
import threading
import Queue
import time
import logging, sys

class NullHandler(logging.Handler):
    """This handler does nothing. It's a backport of NullHandler present in newer
    versions of pythons so that we can run on SLC5"""
    def handle(self, record):
      '''empty handle method'''
      pass

    def emit(self, record):
      '''empty emit method'''
      pass

    def createLock(self):
      '''empty createLock method'''
      self.lock = None

class ThreadPoolServer(rpyc.utils.server.Server):
  '''This server is threaded like the ThreadedServer but reuses threads so that
  recreation is not necessary for each request. The pool of threads has a fixed
  size that can be set with the 'nbThreads' argument. Otherwise, the default is 20'''

  def __init__(self, *args, **kwargs):
    '''Initializes a ThreadPoolServer. In particular, instantiate the thread pool.'''
    # last time we were overloaded
    self.last_overload = 0
    # get the number of threads in the pool
    nbthreads = 20
    if 'nbThreads' in kwargs:
      nbthreads = kwargs['nbThreads']
      del kwargs['nbThreads']
    # init the parent
    rpyc.utils.server.Server.__init__(self, *args, **kwargs)
    # create a queue where requests will be pending until a thread is ready
    self._client_queue = Queue.Queue(nbthreads)
    # declare the pool as already active
    self.active = True
    # setup the thread pool
    for i in range(nbthreads):
      t = threading.Thread(target = self._authenticate_and_serve_clients, args=(self._client_queue,))
      t.daemon = True
      t.start()
    self.logger.addHandler(NullHandler())

  def _authenticate_and_serve_clients(self, queue):
    '''Main method run by the threads of the thread pool. It gets work from the
    internal queue and calls the _authenticate_and_serve_client method'''
    while self.active:
      try:
        sock = queue.get(True, 1)
        self._authenticate_and_serve_client(sock)
      except Queue.Empty:
        # we've timed out, let's just retry. We only use the timeout so that this
        # thread can stop even if there is nothing in the queue
        pass
      except Exception, e:
        # "Caught exception in Worker thread" message
        self.logger.warning("failed to serve client, caught exception : %s", str(e))
        # wait a bit so that we do not loop too fast in case of error
        time.sleep(.2)

  def _accept_method(self, sock):
    '''Implementation of the accept method : only pushes the work to the internal queue.
    In case the queue is full, raises an AsynResultTimeout error'''
    try:
      # try to put the request in the queue
      self._client_queue.put_nowait(sock)
    except Queue.Full:
      # queue was full, we ignore the request
      try:
        sock.shutdown(socket.SHUT_RDWR)
      except Exception, e:
        pass
      try:
        sock.close()
      except Exception, e:
        pass
      # but we log something regularly (max every 5s) when we are overloaded
      current_time = time.time()
      if current_time - self.last_overload > 5:
        self.logger.warning("server is overloaded")
        self.last_overload = current_time
