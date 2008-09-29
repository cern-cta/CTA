#!/usr/bin/python
import os
import sys
try:
  import cx_Oracle
except:
  print '''Fatal: could not load module cx_Oracle.
  Make sure it is installed and $PYTHONPATH includes the directory where cx_Oracle.so resides.\n'''
  sys.exit(2)

nsHost = "castorns"          # write your nameserver host alias here

nsFileCl = {'0' : 'null'}


# create the nsFileCl dictionary from nslistclass
def parseNsListClass():
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


## main()

# connect to DB
stgDb = raw_input("Please insert the full connection string to a CASTOR2 stager db (user/passwd@stgdb): ")
print "Connecting to the stager db..."
conn = cx_Oracle.connect(stgDb)
dbcursor = conn.cursor()

parseNsListClass()

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
  SELECT d2s.child INTO scId
    FROM diskcopy, filesystem, diskpool2svcclass d2s 
   WHERE diskcopy.id = dcId 
     and diskcopy.filesystem = filesystem.id
     and filesystem.diskpool = d2s.parent;
  SELECT id INTO fcId
    FROM FileClass
   WHERE name = fcname;
  selectCastorFile(fid, nh, scId, fcId, fileSize, fname, cfId, unused);
  UPDATE DiskCopy set castorFile = cfId WHERE id = dcId;
  COMMIT;
END;
'''

dbcursor.execute(sql)

# Get list of files to fix and do it
sqlList = '''
SELECT diskcopy.id, substr(path, instr(path, '/',1,1)+1, instr(path,'@',1,1)-instr(path, '/',1,1)-1)
  FROM DiskCopy WHERE castorFile = 0 or castorFile is null
'''
dbcursor.execute(sqlList)
files = dbcursor.fetchall()
print 'Found %d diskcopies to be updated, starting...' % len(files)
for f in files:
    namefd = os.popen('nsgetpath castorns ' + str(f[1]))
    name = namefd.read().strip('\n')
    rc = namefd.close()
    if rc == None:
      lsfd = os.popen('nsls -l --class ' + name)
      ls = lsfd.read().strip('\n').split()
      dbcursor.callproc('reinsertCastorFile', ([f[0], f[1], nsHost, nsFileCl[ls[0]], ls[5], ls[9]]));
#   else the file does not exist: we can probably drop everything then

# cleanup
sql = 'DROP PROCEDURE reinsertCastorFile'
dbcursor.execute(sql)

print 'Update completed'
