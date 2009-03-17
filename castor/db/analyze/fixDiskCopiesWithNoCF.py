#!/usr/bin/python
#/******************************************************************************
# *                      fixDiskCopiesWithNoCF.py
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
# * @(#)$RCSfile: fixDiskCopiesWithNoCF.py,v $ $Revision: 1.7 $ $Release$ $Date: 2009/03/17 10:36:30 $ $Author: waldron $
# *
# * Fixes the DiskCopy.castorFile FK values by reinserting all needed entries in
# * CastorFile and make the orphaned DiskCopies point to them. The affected
# * DiskCopies will also be invalidated.
# * It is recommended to run this script prior to an upgrade to 2.1.8-3 as from
# * this release on, a FK constraint will be enforced and previous orphaned
# * entries will make the upgrade script fail.
# *
# * @author Castor Dev team, castor-dev@cern.ch
# *****************************************************************************/

import os
import sys
import getopt
import castor_tools

nsHost = "castorns"          # write your nameserver host alias here

# usage function
def usage(exitCode):
  print 'Usage : ' + sys.argv[0] + ' [-h] [-n NSHOST]'
  sys.exit(exitCode)

## main()
# first parse the options
try:
    options, args = getopt.getopt(sys.argv[1:], 'hn:', ['help', 'nshost='])
except Exception, e:
    print e
    usage(1)
for f, v in options:
    if f == '-h' or f == '--help':
        usage(0)
    elif f == '-n' or f == '--nshost':
        nsHost = v
    else:
        print "unknown option : " + f
        usage(1)

# connect to DB
try:
    stconn = castor_tools.connectToStager()
    castor_tools.parseNsListClass()
    dbcursor = stconn.cursor()
    # create temp procedure
    sql = '''
CREATE OR REPLACE PROCEDURE reinsertCastorFile(
        dcId IN NUMBER,
        fid IN NUMBER,
        nh IN VARCHAR2,
        fcName IN VARCHAR2,
        fileSize IN NUMBER,
        fname IN VARCHAR2) AS
 scId NUMBER;
 cfId NUMBER;
 unused NUMBER;
 fcId NUMBER;
BEGIN
  SELECT id INTO fcId
    FROM FileClass
   WHERE name = fcname;
  SELECT d2s.child INTO scId
    FROM diskcopy, filesystem, diskpool2svcclass d2s 
   WHERE diskcopy.id = dcId 
     and diskcopy.filesystem = filesystem.id
     and filesystem.diskpool = d2s.parent
     and rownum < 2;  -- take the first one randomly if more than one
  selectCastorFile(fid, nh, scId, fcId, fileSize, fname, cfId, unused);
  UPDATE DiskCopy SET castorFile = cfId, status = 7 WHERE id = dcId AND status != 10;
  COMMIT;
EXCEPTION WHEN NO_DATA_FOUND THEN
  DELETE FROM DiskCopy WHERE id = dcId;
  DELETE FROM Id2Type WHERE id = dcId;
  COMMIT;
END;'''
    dbcursor.execute(sql)
    # Get list of files to fix and do it
    sqlList = '''
SELECT diskcopy.id, substr(path, instr(path, '/',1,1)+1, instr(path,'@',1,1)-instr(path, '/',1,1)-1)
  FROM DiskCopy
 WHERE not exists (select 1 from CastorFile where id = DiskCopy.castorFile)'''
    dbcursor.execute(sqlList)
    files = dbcursor.fetchall()
    if len(files) == 0:
        print 'Found no diskcopy to fix, exiting'
    else:
        print 'Found %d diskcopies to be updated, starting...' % len(files)
    for f in files:
        namefd = os.popen('nsgetpath castorns ' + f[1])
        name = namefd.read().strip('\n')
        rc = namefd.close()
        if rc == None:
            print 'fileid %s' % f[1]
            lsfd = os.popen('nsls -l --class ' + name)
            ls = lsfd.read().strip('\n').split()
            #print [f[0], int(f[1]), nsHost, castor_tools.nsFileCl[ls[0]], int(ls[5]), ls[9]]
            dbcursor.execute('BEGIN reinsertCastorFile(:dcId, :fid, :nh, :fcName, :fs, :fname); END;',
                    dcId=long(f[0]), fid=int(f[1]), nh=nsHost, fcName=castor_tools.nsFileCl[ls[0]], fs=int(ls[5]), fname=ls[9])
        else:
            # the file does not exist anywhere (the ones on disk would have been taken by the synchronization)
            dbcursor.execute('BEGIN DELETE FROM DiskCopy WHERE id = :dcId; DELETE FROM Id2Type WHERE id = :dcId; END;',
                    dcId=long(f[0]))

    # cleanup
    sql = 'DROP PROCEDURE reinsertCastorFile'
    dbcursor.execute(sql)
    print 'Update completed'
except Exception, e:
    print e
    sys.exit(-1)
