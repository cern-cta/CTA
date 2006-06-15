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
# * @(#)$RCSfile: C1-C2FileMover.py,v $ $Revision: 1.2 $ $Release$ $Date: 2006/06/15 14:18:40 $ $Author: itglp $
# *
# * Imports a list of files from a Castor1 stager to a Castor2 one.
# * The file is moved from the original staging area to a Castor2 one on this
# * diskserver, without passing through the scheduler. The filesystem is
# * chosen (pseudo)randomly.
# * Info about the original files has to be obtained by issuing stageqry --dump -a
# * and files that don't belong to this diskserver are ignored (no scp performed).
# * Moreover, currently only STAGED files are taken into account.
# *
# * @author Castor Dev team, castor-dev@cern.ch
# *****************************************************************************/

import os
import sys
import cx_Oracle

nsHost = "castor-8.cr.cnaf.infn.it"                # write your nameserver here
stagerDb = "user/pwd@CASTORSTAGER"     # write your stager db login here

nsFileCl = {'0' : 'null'}
svcClass = ''
currentLine = ''
localhost = os.popen('hostname --short').read().strip(' \n')


# usage function
def usage():
  print "Usage:", sys.argv[0], "<stgdump> <svcclass>"
  print "  <stgdump> is the dump of the stgdaemon catalogue (see stageqry --dump)"
  print "  <svcclass> is the name of the target service class"


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


# Get a single text line from stageqry dump
def getline():
  global currentLine
  currentLine = source.readline()
  if currentLine == '':
    raise ValueError, 'EOF'


# Get details about each stageqry dump entry
def getItem():
  global currentLine

  # get status and fileserver
  while(currentLine.find(localhost) < 0):
    getline()
    tobemigr = (currentLine.find('CANBEMIGR') >= 0 or currentLine.find('PUT_FAILED') >= 0)
    for i in range(14): getline()
    if(currentLine.find(localhost) < 0):
      for i in range(19): getline()

  # grep other info
  oldpath = currentLine[currentLine.find('/'):].strip(' \n')
  for i in range(8): getline()
  fsize = currentLine[currentLine.find(':')+1:].strip(' \n')
  for i in range(4): getline()
  cfname = currentLine[currentLine.find(':')+1:].strip(' \n')
  for i in range(2): getline()
  fid = currentLine[currentLine.find(':')+1:].strip(' \n')
  getline()
  fcname = nsFileCl[currentLine[currentLine.find(':')+1:].strip(' \n')]
  for i in range(4): getline()

  return (oldpath, fsize, cfname, fid, fcname, tobemigr)



#
## main()

# first parse options
if (len(sys.argv) != 3):
  usage()
  sys.exit(0)

parseNsListClass()

# connect to DB
print "Connecting to Castor2 stager db..."
conn = cx_Oracle.connect(stagerDb)
dbcursor = conn.cursor()

# create PL/SQL import procedure
sql = '''
CREATE OR REPLACE PROCEDURE importCastor1File(cfname IN VARCHAR2,
                                              cfsize IN NUMBER,
                                              fileid IN NUMBER,
                                              nsHost IN VARCHAR2,
                                              svcClass IN VARCHAR2,
                                              fcname IN VARCHAR2,
                                              disksrv IN VARCHAR2,
                                              toBeMigr IN INTEGER) AS
  scId NUMBER;
  fcId NUMBER;
  cfId NUMBER;
  fsIds "numList";
  fsId NUMBER;
  dcId NUMBER;
  newpath VARCHAR2(2048);

BEGIN
  -- retrieve svcClass and fileClass. **Warning: no protection against errors here!
  SELECT id INTO scId FROM SvcClass WHERE name = svcClass;
  SELECT id INTO fcId FROM FileClass WHERE name = fcname;

  -- select FileSystem
  SELECT FileSystem.id BULK COLLECT INTO fsIds
    FROM FileSystem, DiskServer
   WHERE FileSystem.diskServer = DiskServer.id
     AND DiskServer.name = disksrv;
  fsId := fsIds(MOD(fileid, fsids.COUNT)+1);  -- "randomly" select a FileSystem on this DiskServer

  -- create CastorFile
  selectCastorFile(fileid, nsHost, scId, fcId, cfsize, cfname, cfId, dcId);  -- dcId is unused here

  -- create DiskCopy
  SELECT ids_seq.nextval INTO dcId FROM DUAL;
  buildPathFromFileId(fileId, nsHost, dcId, newpath);
  INSERT INTO DiskCopy (path, id, fileSystem, castorFile, status, creationTime)
         VALUES (newpath, dcId, fsId, cfId, 0, getTime());    -- STAGED
  INSERT INTO Id2Type (id, type) VALUES (dcId, 5);   -- OBJ_DiskCopy
	
  -- if needed make it CANBEMIGR and create TapeCopies
  IF toBeMigr = 1 THEN
    UPDATE DiskCopy set status = 6 WHERE id = dcId;   -- STAGEOUT
    putDoneFunc(cfId, fsId, 0);
  END IF;

  -- commit everything
  COMMIT;
END;
'''
dbcursor.execute(sql)

# Loop over the stg entries and perform import
print "Starting import..."
source = open(sys.argv[1], 'r')
getline()
sql = '''SELECT FS.mountPoint || DC.path FROM FileSystem FS, CastorFile CF, DiskCopy DC 
          WHERE DC.filesystem = FS.id AND DC.castorfile = CF.id AND CF.fileid = :1'''

try:
  while(1):
    (oldpath, fsize, cfname, fid, fcname, tobemigr) = getItem()
    if(tobemigr == 1): continue         # don't import tobemigr files
    print "\n  importing %s" % cfname

    dbcursor.callproc('importCastor1File',
      (cfname, long(fsize), long(fid), nsHost, sys.argv[2], fcname, localhost, int(tobemigr)))
    dbcursor.execute(sql, [long(fid)])
    targetpath = dbcursor.fetchone()[0]
    
    # really move it
    print "  mv %s %s" % (oldpath, targetpath)
    os.system("mv %s %s" % (oldpath, targetpath))
    os.system("chown stage.st %s" % targetpath)

except ValueError:
  source.close()
  conn.close()
  print "\nImport complete."
