#/******************************************************************************
# *                   castor_tools.py
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
# * utility functions for castor tools written in python
# *
# * @author Castor Dev team, castor-dev@cern.ch
# *****************************************************************************/

import os, sys, time, dlf

def checkValueFound(name, value, instance, configFile):
    if len(value) == 0:
        raise ValueError, "No " + name + " found for " + instance + " in " + configFile

#-------------------------------------------------------------------------------
# importOracle
#-------------------------------------------------------------------------------
def importOracle():
    global cx_Oracle
    try:
        import cx_Oracle
    except Exception:
        raise Exception, '''Fatal: could not load module cx_Oracle.
Make sure it is installed and $PYTHONPATH includes the directory where cx_Oracle.so resides and that
your ORACLE environment is setup properly.'''

# DLF initialization
DLF_BASE_CASTORTOOLSLIB = 1000

msgs = dlf.enum('CREATEDORACONN', 'DROPPEDORACONN', 'INVALIDOPT', 
                base=DLF_BASE_CASTORTOOLSLIB)
dlf.addmessages({msgs.CREATEDORACONN : 'Created new Oracle connection',
                 msgs.DROPPEDORACONN : 'Oracle connection dropped',
                 msgs.INVALIDOPT : 'Invalid option in castor.conf'})

#-------------------------------------------------------------------------------
# DBConnection
#-------------------------------------------------------------------------------
class DBConnection:
    '''This class wraps an Oracle database connection with the ability to automatically reconnect when 
    the underlying db connection drops. See also castor/db/ora/OraCnvSvc.cpp'''
    
    # class-level constant defining the database schemaVersion expected by the code
    SCHEMAVERSION = "2_1_10_0"
    
    def __init__(self, connString):
        '''Constructor'''
        importOracle()
        self.connString = connString
        self._autocommit = False
        self.connection = None
        # the following ORA error codes are known (see OraCnvSvc.cpp) to be raised when the Oracle connection drops
        self.errorCodesForReconnect = [28, 3113, 3114, 32102, 3135, 12170, 12541, 1012, 1003, 12571, 1033, 1089, 12537]
    
    def initConnection(self):
        '''Instantiates an Oracle connection and checks the schema version defined in the db against the code one.
        Raises ValueError in case of mismatch, cx_Oracle.OperationalError for any other Oracle issue.'''
        self.connection = cx_Oracle.Connection(self.connString)
        self.connection.autocommit = self._autocommit
        cur = self.connection.cursor()
        cur.execute("SELECT schemaVersion FROM CastorVersion")
        dbVer = cur.fetchone()
        cur.close()
        if dbVer == None:
            raise ValueError, 'No CastorVersion table found in the database'
        if dbVer[0] <> DBConnection.SCHEMAVERSION:
            raise ValueError, 'Version mismatch between the database and the software : "'
            + dbVer + '" versus "' + DBConnection.SCHEMAVERSION + '"'
        # 'Created new Oracle connection' message
        dlf.write(msgs.CREATEDORACONN)
    
    # autocommit property
    def set_autocommit(self, value):
        self._autocommit = value
        if self.connection:
            self.connection.autocommit = value
    
    def get_autocommit(self):
        return self._autocommit
        
    autocommit = property(get_autocommit, set_autocommit)
    
    def __getattr__(self, name):
        '''Implements a facade pattern: any method of the underlying connection is exposed by this class,
        but disconnections are handled automatically'''
        if not self.connection:
            self.initConnection()
        def facade(*args):
            try:
                if hasattr(self.connection, name):
                    return getattr(self.connection, name)(*args)
                else:
                    return lambda: NotImplemented()
            except cx_Oracle.OperationalError, e:
                # we got an Oracle error, let's see if we have to reconnect
                if (e.args.code in self.errorCodesForReconnect) or (e.args.code >= 25401 and e.args.code <= 25409):
                    # yes, try again
                    self.connection.close()
                    self.connection = None
                raise
        return facade

#-------------------------------------------------------------------------------
# connectToDB
#-------------------------------------------------------------------------------
def connectToDB(user, passwd, dbname):
    return DBConnection(user + '/' + passwd + '@' + dbname)

#-------------------------------------------------------------------------------
# disconnectDB
#-------------------------------------------------------------------------------
def disconnectDB(connection):
    connection.close()
    # 'Oracle connection dropped'
    dlf.write(msgs.DROPPEDORACONN)


#-------------------------------------------------------------------------------
# getStagerDBConnectParams
#-------------------------------------------------------------------------------
def getStagerDBConnectParams():
    # find out the instance to use
    full_name = "DbCnvSvc"
    inst = "default"
    if os.environ.has_key('CASTOR_INSTANCE'):
        inst = os.environ['CASTOR_INSTANCE']
        full_name = full_name + '_' + inst
        inst = "'" + inst + "' stager"
    # go through the lines of ORASTAGERCONFIG
    user = ""
    passwd = ""
    dbname = ""
    for l in open ('/etc/castor/ORASTAGERCONFIG').readlines():
        if len(l.strip()) == 0 or l.strip()[0] == '#':
            continue
        try:
            instance, entry, value = l.split()
            if instance == full_name:
                if entry == 'user':
                    user = value
                elif entry == 'passwd':
                    passwd = value
                elif entry == 'dbName':
                    dbname = value
        except ValueError:
            # ignore line
            pass
    checkValueFound("user name", user, inst, 'ORASTAGERCONFIG')
    checkValueFound("password", passwd, inst, 'ORASTAGERCONFIG')
    checkValueFound("DB name", dbname, inst, 'ORASTAGERCONFIG')
    return user, passwd, dbname

#-------------------------------------------------------------------------------
# getNSDBConnectParam
#-------------------------------------------------------------------------------
def getNSDBConnectParam(file):
    line = open('/etc/castor/' + file).readline()
    line = line[0:len(line)-1] #drop trailing \n
    sl = line.find('/')
    if sl == -1:
        raise ValueError, 'Invalid connection string in /etc/castor/NSCONFIG'
    ar = line.find('@',sl)
    if ar == -1:
        raise ValueError, 'Invalid connection string in /etc/castor/NSCONFIG'
    user = line[0:sl]
    passwd = line[sl+1:ar]
    dbname = line[ar+1:]
    checkValueFound("user name", user, 'nameserver', file)
    checkValueFound("password", passwd, 'nameserver', file)
    checkValueFound("DB name", dbname, 'nameserver', file)
    return user, passwd, dbname

#-------------------------------------------------------------------------------
# getVdqmDBConnectParams
#-------------------------------------------------------------------------------
def getVdqmDBConnectParams():
    # find out the instance to use
    full_name = "DbCnvSvc"
    inst      = "default"
    if os.environ.has_key('CASTOR_INSTANCE'):
        inst = os.environ['CASTOR_INSTANCE']
        full_name = full_name + '_' + inst
        inst = "'" + inst + "' vdqm"
    # go through the lines of ORAVDQMCONFIG
    user   = ""
    passwd = ""
    dbname = ""
    for l in open ('/etc/castor/ORAVDQMCONFIG').readlines():
        if len(l.strip()) == 0 or l.strip()[0] == '#':
            continue
        try:
            instance, entry, value = l.split()
            if instance == full_name:
                if entry == 'user':
                    user = value
                elif entry == 'passwd':
                    passwd = value
                elif entry == 'dbName':
                    dbname = value
        except ValueError:
            # ignore linei
            pass
    checkValueFound("user name", user,   inst, 'ORAVDQMCONFIG')
    checkValueFound("password",  passwd, inst, 'ORAVDQMCONFIG')
    checkValueFound("DB name",   dbname, inst, 'ORAVDQMCONFIG')
    return user, passwd, dbname


#-------------------------------------------------------------------------------
# connectTo_ methods
#-------------------------------------------------------------------------------
def connectToVmgr():
    # find out the instance to use
    full_name = "DbCnvSvc"
    inst      = "default"
    if os.environ.has_key('CASTOR_INSTANCE'):
        inst = os.environ['CASTOR_INSTANCE']
        full_name = full_name + '_' + inst
        inst = "'" + inst + "' vmgr"
    # go through the lines of VMGRCONFIG
    for l in open ('/etc/castor/VMGRCONFIG').readlines():
        if len(l.strip()) == 0 or l.strip()[0] == '#':
            continue
        try:
            conn = DBConnection( l.strip() )
            break
        except cx_Oracle.DatabaseError, exc:
            print 'connectToVmgr Error:  Unexpected error while connecting to the VMGR db'
            raise
    return conn

def connectToStager():
    user, passwd, dbname = getStagerDBConnectParams()
    return connectToDB(user, passwd, dbname)

def connectToNS():
    user, passwd, dbname = getNSDBConnectParam('NSCONFIG')
    return connectToDB(user, passwd, dbname)

def connectToDLF():
    user, passwd, dbname = getNSDBConnectParam('DLFCONFIG')
    return connectToDB(user, passwd, dbname)


#-------------------------------------------------------------------------------
# parseNsListClass
#-------------------------------------------------------------------------------
def parseNsListClass():
  '''creates the nsFileCl dictionary from nslistclass'''
  global nsFileCl 
  nsFileCl = {'0' : 'null'}
  print "Parsing nslistclass output..."
  #nsout = open('nslistclass_output.txt', 'r')
  nsout = os.popen("nslistclass | grep -C1 CLASS_ID | awk '{ print $2 }'", 'r')
  while(1):
    currentLine = nsout.readline()
    if currentLine == '':
      nsout.close()
      return
    nsFileCl[currentLine.strip('\n')] = nsout.readline().strip('\n')
    nsout.readline()
    nsout.readline()


DiskCopyStatus = ["DISKCOPY_STAGED",
                  "DISKCOPY_WAITDISK2DISKCOPY",
                  "DISKCOPY_WAITTAPERECALL",
                  "DISKCOPY_DELETED",
                  "DISKCOPY_FAILED",
                  "DISKCOPY_WAITFS",
                  "DISKCOPY_STAGEOUT",
                  "DISKCOPY_INVALID",
                  "DISKCOPY_GCCANDIDATE",
                  "DISKCOPY_BEINGDELETED",
                  "DISKCOPY_CANBEMIGR",
                  "DISKCOPY_WAITFS_SCHEDULING"]

areBooleans = ["aborted",
               "emptyfile",
               "disk1behavior",
               "replicateonclose",
               "failJobsWhenNoSpace",
               "recursive",
               "created",
               "isgranted",
               "granted",
               "concat",
               "deferedallocation"]

def intToBoolean(entry, value):
    if entry.lower() in areBooleans:
        if value == 0:
            return 'No'
        else:
            return 'Yes'
    else:
        return value

#-------------------------------------------------------------------------------
# castorObject
#-------------------------------------------------------------------------------
class castorObject(dict):
  '''a base object for CASTOR items.
  This includes clever printing and case insensitive member access'''
  def __init__(self, name):
    self.name = name
  def __str__(self):
    res = '[# ' + self.name + ' #]\n'
    for s in self.keys():
      if isinstance(self[s], type([])):
        res = res + s.lower() + " :\n"
        i = 0
        for t in self[s]:
          res = res + "  " + str(i) + " :\n"
          # this is only reindenting
          for l in str(t).split('\n'):
            if len(l) > 0:
              res = res + '    ' + l + '\n'
          i = i + 1
      else:
        res = res + s.lower() + " : " + str(intToBoolean(s, self[s])) + "\n"
    return res
  def __getattr__(self, name):
    return self[name.upper()]

def getObject(stcur, table, key, value):
  '''Gets an object from the DB'''
  stmt = 'SELECT * FROM ' + table + ' WHERE ' + key + '='
  if type(value) == type(''): stmt = stmt + "'"
  stmt = stmt + value
  if type(value) == type(''): stmt = stmt + "'"  
  stcur.execute(stmt)
  rawobj = stcur.fetchone()
  if None == rawobj:
    raise ValueError, "Found no " + table + " with " + key + " : '" + value + "'"
  obj = castorObject(table)
  i = 0
  for d in stcur.description:
    obj[d[0]] = rawobj[i]
    i = i + 1
  return obj

def fillObjectgeneric(stcur, obj, entry, table, stmt):
  '''Fill an object with the children given by a DB stmt'''
  stcur.execute(stmt)
  rawobjs = stcur.fetchall()
  obj[entry] = []
  for r in rawobjs:
    child = castorObject(table)
    i = 0
    for d in stcur.description:
      child[d[0]] = r[i]
      i = i + 1
    obj[entry].append(child)

def fillObject12n(stcur, obj, entry, table, key):
  '''Fill an object following a 1->n relation'''
  stmt = 'SELECT * FROM ' + table + ' WHERE ' + key + '=' + str(obj.id)
  fillObjectgeneric(stcur, obj, entry, table, stmt)

def fillObjectn21(stcur, obj, entry, table):
  '''Fill an object following a n->1 relation'''
  value = obj[entry.upper()]
  if value == 0:
      obj[entry.upper()] = None
  else:
      stmt = 'SELECT * FROM ' + table + ' WHERE id =' + str(value)
      fillObjectgeneric(stcur, obj, entry.upper(), table, stmt)

def fillObjectn2n(stcur, obj, entry, table):
  '''Fill an object following a n->n relation'''
  if obj.name < table:
    jointable = obj.name + '2' + table
    key1 = 'child'
    key2 = 'parent'
  else:
    jointable = table + '2' + obj.name
    key1 = 'parent'
    key2 = 'child'
  stmt = 'SELECT ' + table + '.* FROM ' + table + ',' + jointable + ' WHERE ' + jointable + '.' + key1 + '=' + table + '.id AND ' + jointable + '.' + key2 + '=' + str(obj.id)
  fillObjectgeneric(stcur, obj, entry, table, stmt)

#-------------------------------------------------------------------------------
# getSvcClass
#-------------------------------------------------------------------------------
def getSvcClass(svcClassName):
    '''Gets the content of a given service class'''
    try:
        stconn = connectToStager()
        stcur = stconn.cursor()
        stcur.arraysize = 50
        svcClass = getObject(stcur, 'SvcClass', 'name', svcClassName)
        fillObjectn2n(stcur, svcClass, 'DiskPools', 'DiskPool')
        fillObjectn2n(stcur, svcClass, 'TapePools', 'TapePool')
        fillObjectn21(stcur, svcClass, 'forcedFileClass', 'FileClass')
        disconnectDB(stconn)
        return svcClass
    except Exception, e:
        print e
        sys.exit(-1)

#-------------------------------------------------------------------------------
# getFileClass
#-------------------------------------------------------------------------------
def getFileClass(fileClassName):
    '''Gets the content of a given file class'''
    try:
        stconn = connectToStager()
        stcur = stconn.cursor()
        fileClass = getObject(stcur, 'FileClass', 'name', fileClassName)
        disconnectDB(stconn)
        return fileClass
    except Exception, e:
        print e
        sys.exit(-1)


#-------------------------------------------------------------------------------
# CastorConf
#-------------------------------------------------------------------------------
class CastorConf(dict):
    '''This class allows easy manipulation of the castor config file from python.
    It caches the content of the file for fast access and refreshes it regularly.
    Refreshing can also be forced via the refresh method.
    By default, the refresh delay is 30s, 0 means it will always (not recommended)
    and to a negative number prevents any refresh'''

    def __init__(self, refreshDelay=30, fileName='/etc/castor/castor.conf'):
        '''constructor'''
        self.fileName = fileName
        self.refreshDelay = refreshDelay
        self.lastRefresh = 0
        # read the config file right now
        self.refresh()

    def refresh(self):
        '''refresh the cache of the config file by rereading and reparsing it'''
        # reset the current configuration
        self.clear()
        # parse the file
        self.lastRefresh = time.time()
        f = open(self.fileName)
        for line in f.readlines():
            line = line.strip()
            if len(line) == 0 or line[0] == '#': continue # ignore comments
            category, name, value = line.split(None,2)
            if category not in self:
                self[category] = {}
            if name not in self[category]:
                self[category][name] = value
            else:
                print "Ignoring entry %s %s %s as it's a redefinition" % (category, name, value)
                print "Value used : " + self[category][name]
        f.close()

    def getValue(self,category,key,default=None,typ=str):
        '''returns the value of a configuration item casted into the given type.
        also handles casting errors and a default value if nothing is found'''
        # check whether we need to refresh our data first
        if self.refreshDelay >= 0 and time.time() > self.lastRefresh + self.refreshDelay:
            self.refresh()
        # now deal with the config item
        try:
            strvalue = self[category][key]
            try:
                value = typ(strvalue)
            except ValueError:
                # 'Invalid option in castor.conf' message
                dlf.writeerr(msgs.INVALIDOPT, msg='Invalid ' + category + '/' + key + ' option, ignoring it : ' + strvalue)
                value = default
        except KeyError:
            value = default
        return value


globalCastorConf = None
def castorConf():
    '''method to access a singleton CastorConf object representing the default configuration'''
    global globalCastorConf
    if globalCastorConf == None:
        globalCastorConf = CastorConf()
    return globalCastorConf
