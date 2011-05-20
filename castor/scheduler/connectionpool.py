#/******************************************************************************
# *                   connectionpool.py
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
# * Code handling pools of connections to identical remote services
# *
# * @author Castor Dev team, castor-dev@cern.ch
# *****************************************************************************/

'''connection pool module of the CASTOR transfer manager.
Handles a pool of cached rpyc connections to different nodes'''

import rpyc, rpyc.core.netref
import castor_tools
import exceptions

class Timeout(rpyc.core.async.AsyncResultTimeout):
  '''Our name for the timeout exception on asynchronous calls'''
  pass

def asyncreq(proxy, handler, *args):
    """Overwrite of the rpyc.core.netref.asyncreq helper function to fix
    the case where the connection has been dropped in the mean time"""
    conn = object.__getattribute__(proxy, "____conn__")
    connection = conn()
    if not connection:
      raise exceptions.ReferenceError('weakly-referenced object no longer exists')
    oid = object.__getattribute__(proxy, "____oid__")
    return connection.async_request(handler, oid, *args)

class ConnectionPool(object):
  '''Object handling a pool of connections to identical remote services'''

  def __init__(self):
    '''constructor'''
    # the configuration of CASTOR
    self.config = castor_tools.castorConf()
    # cached connections
    self.connections = {}

  def getConnection(self, machine):
    '''returns an open connection to the specified machine'''
    try:
      # get the cached connection
      conn = self.connections[machine]
      # return it, as we found one
      return conn.root
    except KeyError:
      # no cached connection, create a new one
      # find out the port to be used
      if machine not in self.config.getValue('DiskManager', 'ServerHosts').split():
        # we have a diskserver
        port = self.config.getValue('DiskManager', 'Port', 15011, int)
      else:
        # we have a master node
        port = self.config.getValue('Scheduler', 'Port', 15011, int)
      # create the connection
      rpcconn = rpyc.connect(machine, port)
      # Make the setting up of the connection able to timeout
      # This is in principle done by the rpyc framework on first use of the root attribute
      # but as rpyc does it in a blocking manner, we had to do it "by hand" with
      # proper timeout functionality
      remote_root_ref = rpcconn.async_request(rpyc.core.consts.HANDLE_GETROOT)
      remote_root_ref.set_expiry(self.config.getValue('TransferManager', 'ConnectionTimeout', 0.1, float))
      rpcconn._remote_root = remote_root_ref.value
      # cache it
      self.connections[machine] = rpcconn
      # and return
      return rpcconn.root

  def close(self, machine):
    '''Close the connection to a given machine'''
    if machine in self.connections:
      conn = self.connections[machine]
      del self.connections[machine]
      try:
        conn.close()
      except Exception:
        pass

  def closeall(self):
    '''Close all connections'''
    for machine in self.connections:
      try:
        self.connections[machine].close()
      except Exception:
        pass
    self.connections = {}

  def __getattr__(self, name):
    '''we implement a proxying facility here, where any method will be forwarded
    to the connection of the machine given as first argument.
    So calling connectionPool.foo('machine1', 1,2,3) will be forwarded to a call
    to foo(1,2,3) on the connection associated to machine 'machine1'
    A keyword argument timeout can also be passed to set a dedicated timeout on
    the remote call and overwrite the default value given by the
    TransferManager/ConnectionTimeout entry of the config file. Note that None
    can be passed, meaning there is no timeout.
    As an example, one can call connectionPool.foo('machine1', 1,2,3, timeout=3.5)'''
    def f(machine, *args, **kwargs):
      '''wrapped method doing the actual call'''
      try:
        # first see whether a timeout has been explicitely given. Note that this is the
        # timeout for the actual call to the function. The timeout for internal calls
        # is not touched and still given by the TransferManager/ConnectionTimeout entry
        # in the config file
        try:
          timeout = kwargs['timeout']
          del kwargs['timeout']
        except KeyError:
          timeout = self.config.getValue('TransferManager', 'ConnectionTimeout', 0.1, float)
        if len(kwargs) > 0:
          raise TypeError("got unexpected keyword argument %r" % (kwargs.keys()[0],))
        # we may want to retry in case we get an exception
        gotException = False
        while True:
          try:
            # try existing connection
            conn = self.getConnection(machine)
            remote_attr = asyncreq(conn, rpyc.core.consts.HANDLE_GETATTR, name)
            remote_attr.set_expiry(self.config.getValue('TransferManager', 'ConnectionTimeout', 0.1, float))
            result = asyncreq(remote_attr.value, rpyc.core.consts.HANDLE_CALL, args)
            result.set_expiry(timeout)
            return result.value
          except (exceptions.ReferenceError, EOFError), e:
            # if connection was lost, drop it
            self.close(machine)
            # and try again with a brand new connection, if it was our first attempt
            if gotException:
              if isinstance(e, exceptions.ReferenceError):
                raise Timeout()
            else:
              gotException = True
      except Exception, e:
        # amend errors with the name of the machine that we were connecting too
        e.args = e.args + ('connected to ' + machine,)
        # replace AsyncResultTimeout with our own Timeout
        if isinstance(e, rpyc.core.async.AsyncResultTimeout):
          raise Timeout(e.args)
        raise e
    return f
