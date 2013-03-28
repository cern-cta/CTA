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
# *
# * utility functions for castor tools written in python
# *
# * @author Castor Dev team, castor-dev@cern.ch
# *****************************************************************************/

"""utility functions for castor tools written in python"""

import os, sys, time, thread, subprocess, re
import dlf

def _checkValueFound(name, value, instance, configFile):
    '''Checks whether value is empty and raise an exception if it is'''
    if len(value) == 0:
        raise ValueError, "No " + name + " found for " + instance + " in " + configFile

# DLF initialization
DLF_BASE_CASTORTOOLSLIB = 1000

msgs = dlf.enum('CREATEDORACONN', 'ORAVERSIONMISMATCH', 'DROPPEDORACONN', 'INVALIDOPT',
                base=DLF_BASE_CASTORTOOLSLIB)
dlf.addmessages({msgs.CREATEDORACONN : 'Created new Oracle connection',
                 msgs.ORAVERSIONMISMATCH : 'Version mismatch between the database and the software, ignoring',
                 msgs.DROPPEDORACONN : 'Oracle connection dropped',
                 msgs.INVALIDOPT : 'Invalid option in castor.conf'})

#-------------------------------------------------------------------------------
# DBConnection
#-------------------------------------------------------------------------------
class DBConnection(object):
    '''This class wraps an Oracle database connection with the ability to automatically reconnect when 
    the underlying db connection drops. See also castor/db/ora/OraCnvSvc.cpp'''
    
    def __init__(self, connString, schemaVersion, enforceCheck=True):
        '''Constructor'''
        try:
            import cx_Oracle
        except Exception:
            raise Exception, '''Fatal: could not load module cx_Oracle.
            Make sure it is installed, that $PYTHONPATH includes the directory where cx_Oracle.so resides and that
            your ORACLE environment is setup properly.'''
        self.cx_Oracle = cx_Oracle
        self.connString = connString
        self.schemaVersion = schemaVersion
        self.enforceCheck = enforceCheck
        self._autocommit = False
        self.connection = None
        # the following ORA error codes are known (see OraCnvSvc.cpp) to be raised when the Oracle connection drops
        self.errorCodesForReconnect = [28, 3113, 3114, 32102, 3135, 12170, 12541, 1012, 1003, 12571, 1033, 1089, 12537]
    
    def initConnection(self):
        '''Instantiates an Oracle connection and checks the schema version defined in the db against the code one.
        Raises ValueError in case of mismatch, cx_Oracle.OperationalError for any other Oracle issue.'''
        self.connection = self.cx_Oracle.Connection(self.connString)
        self.connection.autocommit = self._autocommit
        cur = self.connection.cursor()
        cur.execute("SELECT schemaVersion FROM CastorVersion")
        dbVer = cur.fetchone()
        cur.close()
        if dbVer == None:
            raise ValueError, 'No CastorVersion table found in the database'
        if dbVer[0] != self.schemaVersion:
            if self.enforceCheck:
                raise ValueError, 'Version mismatch between the database and the software : ' + dbVer[0] + ' versus ' + self.schemaVersion
            else:
                # log 'Version mismatch error' message, keep going
                dlf.writenotice(msgs.ORAVERSIONMISMATCH, dbVersion=dbVer[0], softwareVersion=self.schemaVersion)
        # 'Created new Oracle connection' message
        dlf.write(msgs.CREATEDORACONN)
        # pass client info for debugging purposes. The thread ID is cut to 5 digits, cf. dlf.py
        cur = self.connection.cursor()
        cur.execute("BEGIN DBMS_APPLICATION_INFO.SET_CLIENT_INFO('CASTOR pid=%d tid=%d'); END;" % (os.getpid(), thread.get_ident()%10000))
    
    def dropConnection(self):
        '''Drop existing internal connection'''
        try:
            # first close the connection
            self.connection.close()
        except Exception:
            # it may fail in some cases, but we anyway wanted to close it, so we ignore
            pass
        self.connection = None

    def checkForReconnection(self, e):
        '''Given an exception, check whether we want to reconnect on that one.
        If yes, drop the existing connection'''
        # first check whether we have an Oracle exception. If not, exit
        if not isinstance(e, self.cx_Oracle.Error):
            return
        # extract ORACLE error code
        error, = e.args
        if isinstance(error, self.cx_Oracle._Error):
            errorcode = error.code
            # check whether to reconnect
            if (errorcode in self.errorCodesForReconnect) or (errorcode >= 25401 and errorcode <= 25409):
                # We should reconnect
                self.dropConnection()

    # autocommit property
    def set_autocommit(self, value):
        '''Sets the autocommit property of this class'''
        self._autocommit = value
        if self.connection:
            self.connection.autocommit = value
    
    def get_autocommit(self):
        '''Gets the autocommit property of this class'''
        return self._autocommit
        
    autocommit = property(get_autocommit, set_autocommit)
    
    def __getattr__(self, name):
        '''Implements a facade pattern: any method of the underlying connection is exposed by this class,
        but disconnections are handled automatically'''
        if not self.connection:
            self.initConnection()
        def facade(*args):
            '''internal method returned by getattr and wrapping the original one'''
            try:
                if hasattr(self.connection, name):
                    return getattr(self.connection, name)(*args)
                else:
                    return lambda: NotImplemented()
            except self.cx_Oracle.Error, e:
                # we got an Oracle error, let's see if we have to reconnect
                self.checkForReconnection(e)
                raise
        return facade

#-------------------------------------------------------------------------------
# connectToDB
#-------------------------------------------------------------------------------
def connectToDB(user, passwd, dbname, schemaVersion, enforceCheck=True):
    '''returns a connection to the required database, and checks its version'''
    return DBConnection(user + '/' + passwd + '@' + dbname, schemaVersion, enforceCheck)

#-------------------------------------------------------------------------------
# disconnectDB
#-------------------------------------------------------------------------------
def disconnectDB(connection):
    '''disconnects from a database and logs the disconnection'''
    connection.close()
    # 'Oracle connection dropped'
    dlf.write(msgs.DROPPEDORACONN)

def _openFileAsStage(fileName):
    '''Tries to open the given file. If it fails, tried again as stage/st user (using sudo)'''
    try:
        return open(fileName)
    except IOError:
        # we failed reading the file as ourselves, try using sudo and be stage/st
        return subprocess.Popen("sudo -n -u stage cat " + fileName, shell=True, stdout=subprocess.PIPE).stdout

#-------------------------------------------------------------------------------
# getDBConnectParams
#-------------------------------------------------------------------------------
def getDBConnectParams(configFileName, dbType):
    '''Gets the connection parameters for the given DB from environment and/or config file'''
    # find out the instance to use
    full_name = "DbCnvSvc"
    inst = "default"
    if os.environ.has_key('CASTOR_INSTANCE'):
        inst = os.environ['CASTOR_INSTANCE']
        full_name = full_name + '_' + inst
        inst = "'" + inst + "' " + dbType
    # go through the lines of ORASTAGERCONFIG
    user = ""
    passwd = ""
    dbname = ""
    for l in _openFileAsStage(os.sep.join(['', 'etc', 'castor', configFileName])).readlines():
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
    _checkValueFound("user name", user, inst, configFileName)
    _checkValueFound("password", passwd, inst, configFileName)
    _checkValueFound("DB name", dbname, inst, configFileName)
    return user, passwd, dbname

#-------------------------------------------------------------------------------
# getNSDBConnectParam
#-------------------------------------------------------------------------------
def getStagerDBConnectParams():
    '''Gets the connection parameters for the stager DB'''
    return getDBConnectParams('ORASTAGERCONFIG', 'stager')

#-------------------------------------------------------------------------------
# getVdqmDBConnectParams
#-------------------------------------------------------------------------------
def getVdqmDBConnectParams():
    '''Gets the connection parameters for the vdqm DB'''
    return getDBConnectParams('ORAVDQMCONFIG', 'vdqm')

#-------------------------------------------------------------------------------
# getNSDBConnectParam
#-------------------------------------------------------------------------------
def getNSDBConnectParam(filename):
    '''Gets the connection parameters for nameserver like DBs from the given config file'''
    for rawline in open('/etc/castor/' + filename).readlines():
        # ignore empty and commented lines
        line = rawline.strip()
        if len(line) == 0 or line[0] == '#':
            continue
        sl = line.find('/')
        if sl == -1:
            raise ValueError, 'Invalid connection string in /etc/castor/NSCONFIG : "%s"' % line
        ar = line.find('@', sl)
        if ar == -1:
            raise ValueError, 'Invalid connection string in /etc/castor/NSCONFIG : "%s"' % line
        user = line[0:sl]
        passwd = line[sl+1:ar]
        dbname = line[ar+1:]
        _checkValueFound("user name", user, 'nameserver', file)
        _checkValueFound("password", passwd, 'nameserver', file)
        _checkValueFound("DB name", dbname, 'nameserver', file)
        return user, passwd, dbname
    raise ValueError, 'empty config file /etc/castor/NSCONFIG'

#-------------------------------------------------------------------------------
# connectTo_ methods
#-------------------------------------------------------------------------------
def connectToVdqm():
    VDQMSCHEMAVERSION = "2_1_12_0"
    user, passwd, dbname = getVdqmDBConnectParams()
    return connectToDB(user, passwd, dbname, VDQMSCHEMAVERSION)

def connectToVmgr():
    '''Gets the connection parameters for the VMGR DB from environment and/or config file'''
    # find out the instance to use
    VMGRSCHEMAVERSION = "2_1_10_1"
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
        conn = DBConnection(l.strip(), VMGRSCHEMAVERSION, enforceCheck=False)
        break
    return conn

def connectToStager():
    '''Connects to the stager database'''
    STAGERSCHEMAVERSION = "2_1_14_0"
    user, passwd, dbname = getStagerDBConnectParams()
    return connectToDB(user, passwd, dbname, STAGERSCHEMAVERSION)

def connectToNS():
    '''Connects to the nameserver database'''
    NSSCHEMAVERSION = "2_1_13_0"
    user, passwd, dbname = getNSDBConnectParam('NSCONFIG')
    return connectToDB(user, passwd, dbname, NSSCHEMAVERSION, enforceCheck=False)

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
    '''Converts a value to boolean if the entry is listed as being a boolean'''
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
    '''A base object for CASTOR items.
    This includes clever printing and case insensitive member access'''
    def __init__(self, name):
        dict.__init__(self)
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
    if type(value) == type(''):
        stmt = stmt + "'"
    stmt = stmt + value
    if type(value) == type(''):
        stmt = stmt + "'"  
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
class CastorConf(object):
    '''This class allows easy manipulation of the castor config file from python.
    It caches the content of the file for fast access and refreshes it regularly.
    Refreshing can also be forced via the refresh method.
    By default, the refresh delay is 30s, 0 means it will always (not recommended)
    and to a negative number prevents any refresh'''

    def __init__(self, refreshDelay=30, fileName='/etc/castor/castor.conf'):
        '''constructor'''
        # the config file to be used
        self.fileName = fileName
        # the refreshing cycle delay
        self.refreshDelay = refreshDelay
        self.lastRefresh = 0
        # the cached dictionnary of configuration items
        self.cache = {}
        # read the config file right now
        self.refresh()

    def refresh(self):
        '''refresh the cache of the config file by rereading and reparsing it'''
        # build up a new configuration dictionnary
        newcache = {}
        # parse the file
        f = open(self.fileName)
        for line in f.readlines():
            line = line.strip()
            if len(line) == 0 or line[0] == '#':
                continue # ignore comments
            splitLine = line.split(None, 2)
            if len(splitLine) == 3:
                category, name, value = splitLine
            elif len(splitLine) == 2:
                category, name = splitLine
                value = ''
            else:
                raise ValueError('Invalid Entry found in configuration file :\n' + line)
            if category not in newcache:
                newcache[category] = {}
            if name not in newcache[category]:
                newcache[category][name] = value
            else:
                raise ValueError("Duplicated entry found in config file : %s %s\nOriginal value was %s\nNew value is %s\nPlease fix" % (category, name, newcache[category][name], value))
        f.close()
        # replace the current cache with the new one (note : the update operation is atomic)
        self.cache.update(newcache)
        self.lastRefresh = time.time()

    def getValue(self, category, key, default=None, typ=str):
        '''returns the value of a configuration item casted into the given type.
        also handles casting errors and a default value if nothing is found. In case
        default is not given or is None, a KeyError is raised if the key is not found'''
        # check whether we need to refresh our data first
        if self.refreshDelay >= 0 and time.time() > self.lastRefresh + self.refreshDelay:
            self.refresh()
        # now deal with the config item
        try:
            strvalue = self.cache[category][key]
            try:
                value = typ(strvalue)
            except ValueError:
                # 'Invalid option in castor.conf' message
                dlf.writeerr(msgs.INVALIDOPT, msg='Invalid ' + category + '/' + key + ' option, ignoring it : ' + strvalue)
                value = default
        except KeyError:
            if default != None:
                value = default
            else:
                raise KeyError('No Entry found for ' + category + '/' + key + ' in config file')
        return value


globalCastorConf = None
def castorConf():
    '''method to access a singleton CastorConf object representing the default configuration'''
    global globalCastorConf
    if globalCastorConf == None:
        globalCastorConf = CastorConf()
    return globalCastorConf


#-------------------------------------------------------------------------------
# prettyPrintTable
#-------------------------------------------------------------------------------
def _interleaveIter(it1, it2):
    ''' interleaves 2 iterables, e.g. (1,3), (2,4) -> (1,2,3,4)'''
    return tuple([item for pair in map(None, it1, it2) for item in pair])

def prettyPrintTable(titles, data, hasSummary=False):
    '''Prints the given data in a table form.
    data must be an iterable of lines, themselves iterable of values matching the title
    data will be printed using their str function'''
    nbCols = len(titles)
    nbRows = len(data)
    for i in range(nbRows):
        if len(data[i]) != nbCols:
            raise ValueError, "Wrong number of data compared to title in row %d : %d/%d" % (i, len(data[i]), nbCols)
    widths = [max([len(titles[i])] + [len(str(row[i])) for row in data]) for i in range(nbCols)]
    lineFormat = ('%*s ' * nbCols)[:-1]
    headerContent = _interleaveIter(widths, titles)
    headerWidth = sum(widths)+nbCols-1  # do not forget spaces
    print lineFormat % headerContent
    print '-'*headerWidth
    for row in data[:-1]:
        print lineFormat % _interleaveIter(widths, [str(item) for item in row])
    if hasSummary:
        print '-'*headerWidth
    if data:
        print lineFormat % _interleaveIter(widths, [str(item) for item in data[-1]])

#-------------------------------------------------------------------------------
# useful printing functions
#-------------------------------------------------------------------------------
def secsToDate(s):
    '''converts number of seconds since the epoch into readable date'''
    return time.strftime('%d-%b-%Y %H:%M:%S', time.localtime(s))

def intToBool(b):
    '''converts a boolean given as an int into a string, taking into account null values'''
    if b == None:
        return '-'
    else:
        return str(bool(b))

def nbToDataAmount(n):
    '''converts a number into a readable amount of data'''
    ext = ['B', 'KiB', 'MiB', 'GiB', 'TiB', 'PiB', 'EiB']
    magn = 0
    while n / 1024 > 5:
        magn += 1
        n = n / 1024
    return str(n) + ext[magn]

def nbToAge(n):
    '''converts a number of seconds into a readable age'''
    s = ''
    if n >= 86400:
        s = s + str(n/86400) + 'd'
        n = n % 86400
    if n >= 3600:
        s = s + str(n/3600) + 'h'
        n = n % 3600
    if n >= 60:
        s = s + str(n/60) + 'mn'
        n = n % 60
    if n > 0:
        s = s + str(n) + 's'
    if len(s) == 0:
        s = '0s'
    return s

def printUser((uid, gid)):
    '''converts uid/gid pair into printable string'''
    if uid == None:
        userName = ''
        uid = ''
    else:
        try:
            userName = pwd.getpwuid(uid)[0]
        except KeyError:
            userName = '<%d>' % uid
        uid = str(uid)
    try:
        groupName = grp.getgrgid(gid)[0]
    except KeyError:
        groupName = '<%d>' % gid
    return '%s:%s (%s:%d)' % (userName, groupName, uid, gid)

#-------------------------------------------------------------------------------
# useful parsing functions
#-------------------------------------------------------------------------------
def parseUser(rawUserGroup):
    '''parses input of a given user.
       format must be one of :
          (<userName>|<uid>)
          (<userName>|<uid>)?:(groupName|<gid>)
    '''
    # split user and group from input
    colonindex = rawUserGroup.find(':')
    if colonindex < 0:
        rawUser = rawUserGroup
        rawGroup = None
    else:
        rawUser = rawUserGroup[0:colonindex]
        rawGroup = rawUserGroup[colonindex+1:]
    # parse user
    if rawUser == '':
        uid = None
    else:
        try:
            # suppose a uid is given
            uid = int(rawUser)
            if uid < 0:
                print 'Invalid uid %d' % uid
                usage(1)
        except ValueError:
            # was not a uid
            try:
                pwdEntry = pwd.getpwnam(rawUser)
                uid = pwdEntry.pw_uid
                gid = pwdEntry.pw_gid
            except KeyError:
                print 'Unknown user %s' % rawUser
                usage(1)
    # find out the group, if given
    if rawGroup == None:
        gid = None
    else:
        try:
            # suppose a gid is given
            gid = int(rawGroup)
            if gid < 0:
                print 'Invalid gid %d' % uid
                usage(1)
        except ValueError:
            # was not a gid
            try:
                gid = grp.getgrnam(rawGroup).gr_gid
            except KeyError:
                print 'Unknown group %s' % rawGroup
                usage(1)
    return uid,gid

def parsePositiveInt(name, svalue):
    '''parses a positive int value and exits with proper error message in case the value does not fit'''
    try:
        value = int(svalue)
        if value < 0:
            raise ValueError
        return value
    except ValueError:
        print 'Invalid %s %s' % (name, svalue)
        usage(1)

def parsePositiveNonNullInt(name, svalue):
    '''parses a positive, non null int value and exits with proper error message in case the value does not fit'''
    value = parsePositiveInt(name, svalue)
    if value == 0:
        print '%s cannot be set to 0' % name
        usage(1)
    return value

def parseBool(name, svalue):
    '''parses a boolean value and exits with proper error message in case the value does not fit'''
    validTrues = ['true', '1', 't', 'y', 'yes']
    validFalses = ['false', '0', 'f', 'n', 'no']
    if svalue.lower() in validTrues:
        return True
    elif svalue.lower() in validFalses:
        return False
    else:
        print 'Invalid %s %s' % (name, svalue)
        print 'Note that accepted booleans are %s and %s' % (','.join(validTrues), ','.join(validFalses))
        usage(1)

def parseDataAmount(name, svalue):
    '''parses a value describing an amount of data. Exits with proper error message in case the value does not fit'''
    try:
        exts = {'K' : 1000, 'M' : 1000*1000, 'G' : 1000*1000*1000, 'T' : 1000*1000*1000*1000,
                'P' : 1000*1000*1000*1000*1000, 'E' : 1000*1000*1000*1000*1000*1000,
                'Ki' : 1024, 'Mi' : 1024*1024, 'Gi' : 1024*1024*1024, 'Ti' : 1024*1024*1024*1024,
                'Pi' : 1024*1024*1024*1024*1024, 'Ei' : 1024*1024*1024*1024*1024*1024}
        regexp = re.compile('^(?P<nb>\d+)(?P<ext>(?:[KMGT]i?)?)B?$')
        m = regexp.match(svalue)
        if not m:
            raise ValueError
        value = int(m.group('nb'))
        if m.group('ext'):
            value *= exts[m.group('ext')]
        return value
    except ValueError:
        print 'Invalid %s %s' % (name, svalue)
        usage(1)

def parseTimeDuration(name, svalue):
    '''parses a value describing a time duration. Exits with proper error message in case the value does not fit'''
    try:
        # check validity
        if not re.compile('^(\d+(s|mn?|h|d)?)+$').match(svalue):
            raise ValueError
        # parse
        exts = {'s' : 1, 'm' : 60, 'mn' : 60, 'h' : 3600, 'd' : 86400}
        regexp = re.compile('(\d+)(s|mn?|h|d)?')
        value = 0
        for nb, ext in regexp.findall(svalue):
            partvalue = int(nb)
            if ext:
                partvalue *= exts[ext]
            value += partvalue
        return value
    except ValueError:
        print 'Invalid %s %s' % (name, svalue)
        usage(1)

