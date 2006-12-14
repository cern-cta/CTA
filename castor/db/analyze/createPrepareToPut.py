#!/usr/bin/python
import os
import sys
try:
  import cx_Oracle
except:
  print '''Fatal: could not load module cx_Oracle.
  Make sure it is installed and $PYTHONPATH includes the directory where cx_Oracle.so resides.\n'''
  sys.exit(2)

stagerDb = "username/password@dbname"     # write your stager db login here

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
  FOR sr IN (select id FROM Subrequest WHERE castorfile = cfid AND status = 6 AND id != srid) LOOP
   prepareForMigration(sr.id, 0, fid, nh, userid, groupid);
  END LOOP;  
END;
'''
dbcursor.execute(sql)

# Verify input parameters against stager db
sql = '''select c.fileid, c.id
 from DiskCopy d, Castorfile c, SubRequest r, StagePutRequest p, Diskserver s, FileSystem f
where d.status = 6 and d.creationtime < 1164022000 and d.creationtime > 1163562000
  and r.castorfile = c.id and r.request = p.id and r.status = 6
  and c.id = d.castorfile and d.filesystem = f.id and f.diskserver = s.id
  and f.status = 0 and s.status = 0 and rownum <= 100''';
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
      dbcursor.execute(createpput, [f[1]]);
    else:
      dbcursor.execute(failedcase, [f[1]]);
  dbcursor.execute("commit");
  for f in files:
    namefd = os.popen('nsGetPath castorns ' + str(f[0]))
    name = namefd.read().strip('\n')
    rc = namefd.close()
    if rc == None:
      com = 'stager_putdone -o -M ' + name + '&';
      print com
      print os.popen(com).read()
  dbcursor.execute(sql)
  files = dbcursor.fetchall()  
