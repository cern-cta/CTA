#!/usr/bin/python
#/******************************************************************************
# *                      C1-C2FileMover.py
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
# * @(#)$RCSfile: migrateStageOutFiles.py,v $ $Revision: 1.1 $ $Release$ $Date: 2006/11/22 15:20:26 $ $Author: sponcec3 $
# *
# * Tries to resurrect files that are stuck in STAGEOUT after a crash e.g.
# * in prepareForMigration. These files usually have a 0 size in the nameserver
# * and are unreachable for the user
# * 
# * THIS SCRIPT HAS TO BE MODIFIED TO COPE WITH THE EXACT SITUATION.
# * YOU SHOULD NOT USE IT WITHOUT UNDERSTANDING IT !!!!!
# *
# * What the script does for each problematic file is the following :
# *  - create a fake prepareToPut request/subrequest
# *  - if the file exists in the nameserver :
# *    +  call the prepareForMigration SQL procedure on all stuck puts so that
# *       they are removed from the DB
# *    + call stager_putDone from the command line so that the file is properly
# *      closed and the nameserver is updated with the right size
# *  - if the file was removed form the nameserver, calls the puFailedProc for
# *    each put so that the file is deleted from the stager
# *
# * You have AT LEAST to modify the selection statement that lists the DiskCopies
# * to be repaired, especially adjusting the times.
# *
# * @author Castor Dev team, castor-dev@cern.ch
# *****************************************************************************/
import os
import sys
import time
try:
  import cx_Oracle
except:
  print '''Fatal: could not load module cx_Oracle.
  Make sure it is installed and $PYTHONPATH includes the directory where cx_Oracle.so resides and that
  your ORACLE environment is setup properly.\n'''
  sys.exit(2)

stagerDb = "user/xxxxxxx@stagerdb"     # write your stager db login here

## main()

# connect to DB
print "Connecting to CASTOR 2 stager db..."
conn = cx_Oracle.connect(stagerDb)
dbcursor = conn.cursor()

# create PL/SQL import procedure
sql = '''
CREATE OR REPLACE PROCEDURE castor_stager.createPPut(cfId number) IS
  ppid number;
  cid NUMBER;
  srid NUMBER;
  fId NUMBER;
  nh VARCHAR2(2048);
  userId INTEGER;
  groupId INTEGER;
BEGIN
  SELECT ids_seq.nextval INTO ppid FROM DUAL;
  SELECT ids_seq.nextval INTO cid FROM DUAL;
  SELECT ids_seq.nextval INTO srid FROM DUAL;
  INSERT INTO ID2Type (id, type) VALUES (ppid, 37);
  INSERT INTO StagePrepareToPutRequest (flags, userName, euid, egid, mask, pid, machine, svcClassName, userTag, reqId, creationTime, lastModificationTime, id, svcClass, client) VALUES (0, 'sponce', 14493, 1028, 0, 0, '', 'Cleanup', 'Cleanup', '', gettime(), gettime(), ppid, 2, cid);
  INSERT INTO ID2Type (id, type) VALUES (cid, 3);
  INSERT INTO Client (ipAddress, port, id) VALUES (0, 0, cid);
  INSERT INTO ID2Type (id, type) VALUES (srid, 27);
  INSERT INTO SubRequest (retryCounter, fileName, protocol, xsize, priority, subreqId, flags, modeBits, creationTime, lastModificationTime, answered, id, diskcopy, castorFile, parent, status, request, getNextStatus) VALUES (0, '', 'none', 0, 0, '', 0, 0, gettime(), gettime(), 1, srid, 0, cfid, 0, 6, ppid, 0);
  -- end old subrequests
  FOR sr IN (select s.id FROM Subrequest s, stagePutRequest r WHERE s.castorfile = cfid AND s.status = 6 AND s.id != srid AND s.request = r.id) LOOP
   prepareForMigration(sr.id, 0, fid, nh, userid, groupid);
  END LOOP;  
END;
'''
dbcursor.execute(sql)

# Verify input parameters against stager db
sql = '''
select c.fileid, c.id
 from DiskCopy d, Castorfile c, SubRequest r, StagePutRequest p, Diskserver s, FileSystem f
where d.status = 6 and d.creationtime < 1164026000 and d.creationtime > 1163562000
  and r.castorfile = c.id and r.request = p.id and r.status = 6
  and c.id = d.castorfile and d.filesystem = f.id and f.diskserver = s.id
  and f.status = 0 and s.status = 0 and rownum <= 20
      ''';
createpput = "begin createpput(:1); end;";
failedcase = '''BEGIN
  FOR sr IN (select id FROM Subrequest WHERE castorfile = :1 AND status = 6) LOOP
   putFailedProc(sr.id);
  END LOOP;  
END;''';
dbcursor.execute(sql)
files = dbcursor.fetchall()
while len(files) > 0 :
  for f in files:
    print 'fileid =', f[0], ', castorFileId =', f[1]
    namefd = os.popen('nsGetPath castorns ' + str(f[0]))
    rc = namefd.close()
    if rc == None:
      print 'createpput', f[1]
      dbcursor.execute(createpput, [f[1]]);
    else:
      print 'failedcase', f[1]
      dbcursor.execute(failedcase, [f[1]]);
  dbcursor.execute("commit");
  for f in files:
    namefd = os.popen('nsGetPath castorns ' + str(f[0]))
    name = namefd.read().strip('\n')
    rc = namefd.close()
    if rc == None:
      com = 'stager_putdone -o -M ' + name + '&';
      print com
      #print os.popen(com).read()
      os.system(com)
  time.sleep(5)
  dbcursor.execute(sql)
  files = dbcursor.fetchall()
  
