/******************************************************************************
 *              stager_2.1.9-7_to_2.1.9-7-1.sql
 *
 * This file is part of the Castor project.
 * See http://castor.web.cern.ch/castor
 *
 * Copyright (C) 2003  CERN
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
 *
 * This script upgrades a CASTOR v2.1.9-6 STAGER database to v2.1.9-7
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

/* Stop on errors */
WHENEVER SQLERROR EXIT FAILURE
DECLARE
  unused VARCHAR2(30);
BEGIN
  UPDATE UpgradeLog
     SET failureCount = failureCount + 1
   WHERE schemaVersion = '2_1_9_4'
     AND release = '2_1_9_7_1'
     AND state != 'COMPLETE';
  COMMIT;
EXCEPTION WHEN NO_DATA_FOUND THEN
  NULL;
END;
/

/* Version cross check and update */
DECLARE
  unused VARCHAR(100);
BEGIN
  SELECT release INTO unused FROM CastorVersion
   WHERE release = '2_1_9_7';
EXCEPTION WHEN NO_DATA_FOUND THEN
  -- Error, we can't apply this script
  raise_application_error(-20000, 'PL/SQL release mismatch. Please run previous upgrade scripts before this one.');
END;
/

INSERT INTO UpgradeLog (schemaVersion, release, type)
VALUES ('2_1_9_4', '2_1_9_7_1', 'TRANSPARENT');
COMMIT;



/* Update and revalidation of PL-SQL code */
/******************************************/


create or replace PROCEDURE tg_getFileToMigrate(transactionId IN NUMBER,
                                                ret OUT INTEGER,
                                                outVid OUT NOCOPY VARCHAR2,
                                                outputFile OUT castorTape.FileToMigrateCore_cur) AS
  CONSTRAINT_VIOLATED EXCEPTION;
  PRAGMA EXCEPTION_INIT(CONSTRAINT_VIOLATED, -1);
  strId NUMBER;
  policy VARCHAR2(100);
  ds VARCHAR2(2048);
  mp VARCHAR2(2048);
  path  VARCHAR2(2048);
  dcid NUMBER:=0;
  cfId NUMBER;
  fileId NUMBER;
  nsHost VARCHAR2(2048);
  fileSize  INTEGER;
  tcId  INTEGER:=0;
  lastUpdateTime NUMBER;
  knownName VARCHAR2(2048);
  trId NUMBER;
  srId NUMBER;
  unused INTEGER;
BEGIN
  ret:=0;
  BEGIN
    SELECT streammigration, id INTO strId, trId
      FROM TapeGatewayRequest
     WHERE vdqmvolreqid = transactionId;

    SELECT vid INTO outVid
      FROM Tape
     WHERE id IN
         (SELECT tape
            FROM Stream
           WHERE id = strId);
  EXCEPTION WHEN NO_DATA_FOUND THEN
    ret:=-2;   -- stream is over
    RETURN;
  END;

  BEGIN  
    SELECT 1 INTO unused FROM dual 
      WHERE EXISTS (SELECT 'x' FROM Stream2TapeCopy 
                      WHERE parent=strId);
  EXCEPTION WHEN NO_DATA_FOUND THEN
    ret:=-1;   -- no more files
    RETURN;
  END;
 -- lock to avoid deadlock with mighunter 
 SELECT id INTO strId FROM stream WHERE id=strId FOR UPDATE;
  -- get the policy name and execute the policy
  BEGIN
    SELECT migrSelectPolicy INTO policy
      FROM Stream, TapePool
     WHERE Stream.id = strId
       AND Stream.tapePool = TapePool.id;
    -- check for NULL value
    IF policy IS NULL THEN
      policy := 'defaultMigrSelPolicy';
    END IF;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    policy := 'defaultMigrSelPolicy';
  END;

  IF policy like 'defaultMigrSelPolicy' THEN
    -- default policy
    tg_defaultMigrSelPolicy(strId,ds,mp,path,dcid ,cfId, fileId,nsHost, fileSize,tcId, lastUpdateTime);
  ELSIF  policy LIKE 'repackMigrSelPolicy' THEN
    -- repack policy
    tg_repackMigrSelPolicy(strId,ds,mp,path,dcid ,cfId, fileId,nsHost, fileSize,tcId, lastUpdateTime);
  ELSIF  policy LIKE 'drainDiskMigrSelPolicy' THEN
    -- drain disk policy
    tg_drainDiskMigrSelPolicy(strId,ds,mp,path,dcid ,cfId, fileId,nsHost, fileSize,tcId, lastUpdateTime);
  ELSE
    -- default if we don't know at this point
    tg_defaultMigrSelPolicy(strId,ds,mp,path,dcid ,cfId, fileId,nsHost, fileSize,tcId, lastUpdateTime);
  END IF;

  IF tcId = 0 OR dcid=0 THEN
    ret := -1; -- the migration selection policy didn't find any candidate
    COMMIT;
    RETURN;
  END IF;

  -- Here we found a tapeCopy and we process it
  -- update status of selected tapecopy and stream
  UPDATE TapeCopy SET status = 3 -- SELECTED
   WHERE id = tcId;
  -- detach the tapecopy from the stream now that it is SELECTED;
  DELETE FROM Stream2TapeCopy
   WHERE child = tcId;


  SELECT lastknownfilename INTO knownName
    FROM CastorFile
   WHERE id = cfId; -- we rely on the check done before

  DECLARE
    newFseq NUMBER;
  BEGIN
   -- Atomically increment and read the next FSEQ to be written to
   UPDATE TapeGatewayRequest
     SET lastfseq=lastfseq+1
     WHERE id=trId
     RETURNING lastfseq-1 into newFseq; -- The previous calue is where we'll write

    INSERT INTO TapeGatewaySubRequest
      (fseq, id, tapecopy, request,diskcopy)
      VALUES (newFseq, ids_seq.nextval, tcId,trId, dcid)
      RETURNING id INTO srId;
      
    INSERT INTO id2type
      (id,type)
      VALUES (srId,180);
  

   OPEN outputFile FOR
     SELECT fileId,nshost,lastUpdateTime,ds,mp,path,knownName,fseq,filesize,id
       FROM tapegatewaysubrequest
      WHERE id = srId;
      
  EXCEPTION WHEN CONSTRAINT_VIOLATED THEN
    -- detach from this stream
    -- resurrect the candidate if there is no other stream containing the tapecopy
    DELETE FROM stream2tapecopy WHERE child= tcid AND parent= strid;
    UPDATE tapecopy SET status=1 WHERE id = tcId AND NOT EXISTS (SELECT 'x' FROM stream2tapecopy WHERE child=tcId);
    -- recursion to avoid extra mount
    tg_getFileToMigrate(transactionId,ret,outVid,outputFile);
 END;
 COMMIT;
END;
/

CREATE OR REPLACE PROCEDURE inputForMigrationPolicy(
  svcclassName IN  VARCHAR2,
  policyName   OUT NOCOPY VARCHAR2,
  svcId        OUT NUMBER,
  dbInfo       OUT castorTape.DbMigrationInfo_Cur) AS
  tcIds "numList";
BEGIN
  -- do the same operation of getMigrCandidate and return the dbInfoMigrationPolicy
  -- we look first for repack condidates for this svcclass
  -- we update atomically WAITPOLICY
  SELECT SvcClass.migratorpolicy, SvcClass.id INTO policyName, svcId
    FROM SvcClass
   WHERE SvcClass.name = svcClassName;

  UPDATE
     /*+ LEADING(TC CF)
         INDEX_RS_ASC(CF PK_CASTORFILE_ID)
         INDEX_RS_ASC(TC I_TAPECOPY_STATUS) */
         TapeCopy TC
     SET status = 7
   WHERE status IN (0, 1)
     AND (EXISTS
       (SELECT 'x' FROM SubRequest, StageRepackRequest
         WHERE StageRepackRequest.svcclass = svcId
           AND SubRequest.request = StageRepackRequest.id
           AND SubRequest.status = 12  -- SUBREQUEST_REPACK
           AND TC.castorfile = SubRequest.castorfile
      ) OR EXISTS (
        SELECT 'x'
          FROM CastorFile CF
         WHERE TC.castorFile = CF.id
           AND CF.svcClass = svcId)) AND rownum < 10000
    RETURNING TC.id -- CREATED / TOBEMIGRATED
    BULK COLLECT INTO tcIds;
  COMMIT;
  -- return the full resultset
  OPEN dbInfo FOR
    SELECT TapeCopy.id, TapeCopy.copyNb, CastorFile.lastknownfilename,
           CastorFile.nsHost, CastorFile.fileid, CastorFile.filesize
      FROM Tapecopy,CastorFile
     WHERE CastorFile.id = TapeCopy.castorfile
       AND TapeCopy.id IN
         (SELECT /*+ CARDINALITY(tcidTable 5) */ *
            FROM table(tcIds) tcidTable);
END;
/

DROP PROCEDURE INPUTMIGRPOLICYGATEWAY;

DROP PROCEDURE INPUTMIGRPOLICYRTCP;

/* attach tapecopies to streams for tapegateway */
CREATE OR REPLACE PROCEDURE attachTCGateway
(tapeCopyIds IN castor."cnumList",
 tapePoolId IN NUMBER)
AS
  unused NUMBER;
  streamId NUMBER; -- stream attached to the tapepool
  nbTapeCopies NUMBER;
BEGIN
  -- WARNING: tapegateway ONLY version
  FOR str IN (SELECT id FROM Stream WHERE tapepool = tapePoolId) LOOP
    BEGIN
      -- add choosen tapecopies to all Streams associated to the tapepool used by the policy
      SELECT id INTO streamId FROM stream WHERE id = str.id FOR UPDATE;
      -- add choosen tapecopies to all Streams associated to the tapepool used by the policy
      FOR i IN tapeCopyIds.FIRST .. tapeCopyIds.LAST LOOP
         BEGIN
           SELECT /*+ index(tapecopy, PK_TAPECOPY_ID)*/ id INTO unused
             FROM TapeCopy
            WHERE Status in (2,7) AND id = tapeCopyIds(i) FOR UPDATE;
           DECLARE CONSTRAINT_VIOLATED EXCEPTION;
           PRAGMA EXCEPTION_INIT (CONSTRAINT_VIOLATED, -1);
           BEGIN
             INSERT INTO stream2tapecopy (parent ,child)
             VALUES (streamId, tapeCopyIds(i));
             UPDATE /*+ index(tapecopy, PK_TAPECOPY_ID)*/ TapeCopy
                SET Status = 2 WHERE status = 7 AND id = tapeCopyIds(i);
           EXCEPTION WHEN CONSTRAINT_VIOLATED THEN
             -- if the stream does not exist anymore
             -- it might also be that the tapecopy does not exist anymore
             -- already exist the tuple parent-child
             NULL;
           END;
         EXCEPTION WHEN NO_DATA_FOUND THEN
           -- Go on the tapecopy has been resurrected or migrated
           NULL;
         END;
      END LOOP; -- loop tapecopies
      COMMIT;
    EXCEPTION WHEN NO_DATA_FOUND THEN
      -- no stream anymore
      NULL;
    END;
  END LOOP; -- loop streams

  -- resurrect the ones never attached
  FORALL i IN tapeCopyIds.FIRST .. tapeCopyIds.LAST
    UPDATE TapeCopy SET status = 1 WHERE id = tapeCopyIds(i) AND status = 7;
  COMMIT;
END;
/

/* Recompile all invalid procedures, triggers and functions */
/************************************************************/
BEGIN
  FOR a IN (SELECT object_name, object_type
              FROM user_objects
             WHERE object_type IN ('PROCEDURE', 'TRIGGER', 'FUNCTION')
               AND status = 'INVALID')
  LOOP
    IF a.object_type = 'PROCEDURE' THEN
      EXECUTE IMMEDIATE 'ALTER PROCEDURE '||a.object_name||' COMPILE';
    ELSIF a.object_type = 'TRIGGER' THEN
      EXECUTE IMMEDIATE 'ALTER TRIGGER '||a.object_name||' COMPILE';
    ELSE
      EXECUTE IMMEDIATE 'ALTER FUNCTION '||a.object_name||' COMPILE';
    END IF;
  END LOOP;
END;
/

/* Flag the schema upgrade as COMPLETE */
/***************************************/
UPDATE UpgradeLog SET endDate = sysdate, state = 'COMPLETE'
 WHERE release = '2_1_9_7_1';
COMMIT;
