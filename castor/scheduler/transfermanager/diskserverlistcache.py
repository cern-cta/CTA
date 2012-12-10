#/******************************************************************************
# *                   diskserverlistcache.py
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
# * DiskServerListCache class of the transfer manager of the CASTOR project
# * This class is responsible provides a cache on the current list of diskservers
# * in the CASTOR instance. This caches aims at reducing the number of accesses
# * to the underlying stager database
# *
# * @author Castor Dev team, castor-dev@cern.ch
# *****************************************************************************/

'''this class is responsible provides a cache on the current list of diskservers
in the CASTOR instance. This caches aims at reducing the number of accesses
to the underlying stager database'''

import time
import castor_tools
import dlf
from transfermanagerdlf import msgs

class DiskServerListCache:
  '''cache for the list of diskservers.
  Automatically refreshes the list regularly, by default every 5mn.
  Set the refresh delay to 0 to always refresh (not recommended) and
  to a negative number to never refresh'''

  def __init__(self, refreshDelay=300):
    '''constructor'''
    # a dictionnary of diskpools with the list of diskservers in each of them
    self.diskPool2DiskServer = {}
    # a dictionnary of diskservers with the associated pool for each of them
    self.diskServer2DiskPool = {}
    self.refreshDelay = refreshDelay
    self.lastRefresh = 0
    # build the cache right now
    self.refresh()

  def refresh(self):
    '''refreshes the cache of diskservers/diskpools'''
    try:
      # query the stager database
      stconn = castor_tools.connectToStager()
      try:
        stcur = stconn.cursor()
        stcur.arraysize = 50
        stDiskServers = '''SELECT UNIQUE DiskServer.name, DiskPool.name
                             FROM FileSystem, DiskServer, DiskPool
                            WHERE FileSystem.diskServer = DiskServer.id
                              AND FileSystem.diskPool = DiskPool.id
                              AND DiskServer.status IN (0,1)
                              AND FileSystem.status IN (0,1)'''
        stcur.execute(stDiskServers)
        rows = stcur.fetchall()
        # build up the list of new ones
        self.diskPool2DiskServer = {}
        self.diskServer2DiskPool = {}
        for diskserver, diskPool in rows:
          if diskPool not in self.diskPool2DiskServer:
            self.diskPool2DiskServer[diskPool] = []
          self.diskPool2DiskServer[diskPool].append(diskserver)
          self.diskServer2DiskPool[diskserver] = diskPool
        self.lastRefresh = time.time()
      finally:
        try:
          castor_tools.disconnectDB(stconn)
        except Exception:
          pass
    except Exception, e:
      # 'failed to refresh list of diskservers, kept old list' message
      dlf.writewarning(msgs.DSREFRESHFAILED, Type=str(e.__class__), Message=str(e))

  def getlist(self):
    '''returns the list of diskservers from the cache, clustered by diskpool.
    Builds this list form the DB if needed'''
    # check whether we need to refresh our data first
    if self.refreshDelay >= 0 and time.time() > self.lastRefresh + self.refreshDelay:
      self.refresh()
    # return the list
    return self.diskPool2DiskServer

  def getset(self):
    '''returns the set of diskservers from the cache, or builds it from the DB if needed'''
    # check whether we need to refresh our data first
    if self.refreshDelay >= 0 and time.time() > self.lastRefresh + self.refreshDelay:
      self.refresh()
    # return the set
    return self.diskServer2DiskPool.keys()

  def getDiskServerPool(self, diskserver):
    '''returns the pool in which a given diskserver lives'''
    # check whether we need to refresh our data first
    if self.refreshDelay >= 0 and time.time() > self.lastRefresh + self.refreshDelay:
      self.refresh()
    return self.diskServer2DiskPool[diskserver]

# Global cache on the list of diskservers
diskServerList = DiskServerListCache()
