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

# Go through files and fix them
sql = '''SELECT unique s.fileName, r.svcClassName, c.filesize, c.id
  FROM DiskCopy d, SubRequest s, StagePrepareToPutRequest r, Castorfile c
 WHERE d.castorfile = s.castorfile AND s.request = r.id
   AND c.id = d.castorfile
   AND d.creationtime < getTime() - 172800 AND d.status = 6'''; # 2days, STAGEOUT
dropFile = '''
DECLARE
  dcIds "numList";
BEGIN
  UPDATE DiskCopy SET status = 7
   WHERE status = 6 AND castorfile = 333
  RETURNING id BULK COLLECT INTO dcIds;
  UPDATE SubRequest SET status = 7
   WHERE diskCopy MEMBER OF dcIds AND status = 6;
END;
''';
dbcursor.execute(sql)
files = dbcursor.fetchall()
for f in files:
  if f[2] == 0:
    print 'file ' + f[0] + ' dropped (size 0)'
    dbcursor.execute(dropFile, [f[3]]);
    conn.commit()
  else:
    print 'file ' + f[0] + ', calling stager_putDone'
    os.putenv('STAGE_SVCCLASS', f[1])
    os.popen('stager_putdone -M ' + f[0])
