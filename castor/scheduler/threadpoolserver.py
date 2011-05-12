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
import logging
import select
import castor_tools
from rpyc.utils.authenticators import AuthenticationError

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
        # castor configuration
        self.config = castor_tools.castorConf()
        # get the number of threads in the pool
        nbthreads = 20
        if 'nbThreads' in kwargs:
            nbthreads = kwargs['nbThreads']
            del kwargs['nbThreads']
        # init the parent
        rpyc.utils.server.Server.__init__(self, *args, **kwargs)
        # a queue of connections having somethign to process
        self._active_connection_queue = Queue.Queue()
        # declare the pool as already active
        self.active = True
        # setup the thread pool for handling requests
        self.workers = []
        for i_unused in range(nbthreads):
            t = threading.Thread(target = self._serve_clients)
            t.setName('ThreadPoolWroker')
            t.daemon = True
            t.start()
            self.workers.append(t)
        # a polling object to be used be the polling thread
        self.poll_object = select.poll()
        # a dictionnary fd -> connection
        self.fd_to_conn = {}
        # setup a thread for polling inactive connections
        self.pollingThread = threading.Thread(target = self._poll_inactive_clients)
        self.pollingThread.setName('PollingThread')
        self.pollingThread.daemon = True
        self.pollingThread.start()
        # disable logging
        self.logger.addHandler(NullHandler())

    def close(self):
        '''closes a ThreadPoolServer. In particular, joins the thread pool.'''
        # close parent server
        rpyc.utils.server.Server.close(self)
        # stop producer thread
        self.pollingThread.join()
        # cleanup thread pool : first fill the pool with None fds so that all threads exit
        # the blocking get on the queue of active connections. Then join the threads
        for i_unused in range(len(self.workers)):
            self._active_connection_queue.put(None)
        for w in self.workers:
            w.join()

    def _remove_from_inactive_connection(self, fd):
        '''removes a connection from the set of inactive ones'''
        # unregister the connection in the polling object
        try:
            self.poll_object.unregister(fd)
        except KeyError:
            # the connection has already been unregistered
            pass

    def _drop_connection(self, fd):
        '''removes a connection by closing it and removing it from internal structs'''
        # cleanup fd_to_conn dictionnary
        try:
            conn = self.fd_to_conn[fd]
            del self.fd_to_conn[fd]
        except KeyError:
            # the active connection has already been removed
            pass
        # close connection
        conn.close()

    def _add_inactive_connection(self, fd):
        '''adds a connection to the set of inactive ones'''
        self.poll_object.register(fd, select.POLLIN|select.POLLPRI|select.POLLNVAL|select.POLLHUP|select.POLLERR)
        
    def _handle_poll_result(self, connlist):
        '''adds a connection to the set of inactive ones'''
        for fd, event_unused in connlist:
            try:
                # remove connection from the inactive ones
                self._remove_from_inactive_connection(fd)
                # Is it an error ?
                if (event_unused & (select.POLLNVAL|select.POLLHUP|select.POLLERR)) != 0:
                    # it was an error, connection was closed. Do the same on our side
                    self._drop_connection(fd)
                else:
                    # connection has data, let's add it to the active queue
                    self._active_connection_queue.put(fd)
            except KeyError:
                # the connection has already been dropped. Give up
                pass

    def _poll_inactive_clients(self):
        '''Main method run by the polling thread of the thread pool.
        Check whether inactive clients have become active'''
        while self.active:
            try:
                # the actual poll, with a timeout of 1s so that we can exit in case
                # we re not active anymore
                active_clients = self.poll_object.poll(1)
                # for each client that became active, put them in the active queue
                self._handle_poll_result(active_clients)
            except Exception, e:
                # "Caught exception in Worker thread" message
                self.logger.warning("failed to poll clients, caught exception : %s", str(e))
                # wait a bit so that we do not loop too fast in case of error
                time.sleep(.2)

    def _serve_requests(self, fd):
        '''Serves requests from the given connection and puts it back to the appropriate queue'''
        for i_unused in range(self.config.getValue('TransferManager', 'RequestBatchSize', 10, int)):
            try:
                # serve a maximum of RequestBatchSize requests for this connection
                if not self.fd_to_conn[fd].poll(): # note that poll serves the request. Name could have been better...
                    # we could not find a request, so we put this connection back to the inactive set
                    self._add_inactive_connection(fd)
                    return
            except EOFError:   
                # the connection has been closed by the remote end. Close it on our side and return
                self._drop_connection(fd)
                return
            except Exception:
                # put back the connection to active queue in doubt and raise the exception to the upper level
                self._active_connection_queue.put(fd)
                raise
        # we've processed the maximum number of requests. Put back the connection in the active queue
        self._active_connection_queue.put(fd)

    def _serve_clients(self):
        '''Main method run by the processing threads of the thread pool.
        Loops forever, handling requests read from the connections present in the active_queue'''
        while self.active:
            try:
                # note that we do not use a timeout here. This is because the implementation of
                # the timeout version performs badly. So we block forever, and exit by filling
                # the queue with None fds
                fd = self._active_connection_queue.get(True)
                # fd may be None (case where we want to exit the blocking get to close the service)
                if fd:
                    # serve the requests of this connection
                    self._serve_requests(fd)
            except Queue.Empty:
                # we've timed out, let's just retry. We only use the timeout so that this
                # thread can stop even if there is nothing in the queue
                pass
            except Exception, e:
                # "Caught exception in Worker thread" message
                self.logger.warning("failed to serve client, caught exception : %s", str(e))
                # wait a bit so that we do not loop too fast in case of error
                time.sleep(.2)

    def _authenticate_and_build_connection(self, sock):
        '''Authenticate a client and if it succees, wraps the socket in a connection object.
        Note that this code is cut and paste from the rpyc internals and may have to be
        changed if rpyc evolves'''
        # authenticate
        if self.authenticator:
            h, p = sock.getpeername()
            try:
                sock, credentials = self.authenticator(sock)
            except AuthenticationError:
                self.logger.info("%s:%s failed to authenticate, rejecting connection", h, p)
                return None
        else:
            credentials = None
        # build a connection
        h, p = sock.getpeername()
        config = dict(self.protocol_config, credentials=credentials, connid="%s:%d"%(h, p))
        return rpyc.core.Connection(self.service, rpyc.core.Channel(rpyc.core.SocketStream(sock)), config=config)

    def _accept_method(self, sock):
        '''Implementation of the accept method : only pushes the work to the internal queue.
        In case the queue is full, raises an AsynResultTimeout error'''
        try:
            # authenticate and build connection object
            conn = self._authenticate_and_build_connection(sock)
            # put the connection in the active queue
            if conn:
                fd = conn.fileno()
                self.fd_to_conn[fd] = conn
                self._add_inactive_connection(fd)
                self.clients.clear()
        except Exception, e:
            self.logger.warning("failed to serve client, caught exception : %s", str(e))
