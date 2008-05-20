/******************************************************************************
 *              2.1.7-6_to_2.1.7-7.sql
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
 * @(#)$RCSfile: 2.1.7-6_to_2.1.7-7.sql,v $ $Release: 1.2 $ $Release$ $Date: 2008/05/20 09:37:23 $ $Author: waldron $
 *
 * This script upgrades a CASTOR v2.1.7-6 database into v2.1.7-7
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

/* Stop on errors - this only works from sqlplus */
WHENEVER SQLERROR EXIT FAILURE;

/* Version cross check and update */
DECLARE
  unused VARCHAR(100);
BEGIN
  SELECT release INTO unused FROM CastorVersion WHERE release LIKE '2_1_7_6%';
EXCEPTION WHEN NO_DATA_FOUND THEN
  -- Error, we can't apply this script
  raise_application_error(-20000, 'PL/SQL release mismatch. Please run previous upgrade scripts before this one.');
END;

UPDATE CastorVersion SET release = '2_1_7_7';
COMMIT;

/* schema changes */

/* Global temporary table to store castor file ids temporarily in the filesDeletedProc procedure */
CREATE GLOBAL TEMPORARY TABLE FilesDeletedProcHelper
  (cfId NUMBER)
  ON COMMIT DELETE ROWS;

/* PL/SQL method implementing checkForD2DCopyOrRecall */
/* dcId is the DiskCopy id of the best candidate for replica, 0 if none is found (tape recall), -1 in case of user error */
/* Internally used by getDiskCopiesForJob and processPrepareRequest */
CREATE OR REPLACE
PROCEDURE checkForD2DCopyOrRecall(cfId IN NUMBER, srId IN NUMBER, reuid IN NUMBER, regid IN NUMBER,
                                  svcClassId IN NUMBER, dcId OUT NUMBER, srcSvcClassId OUT NUMBER) AS
  destSvcClass VARCHAR2(2048);
  authDest NUMBER;
BEGIN
  -- First check whether we are a disk only pool that is already full.
  -- In such a case, we should fail the request with an ENOSPACE error
  IF (checkFailJobsWhenNoSpace(svcClassId) = 1) THEN
    dcId := -1;
    UPDATE SubRequest
       SET status = 7, -- FAILED
           errorCode = 28, -- ENOSPC
           errorMessage = 'File creation canceled since diskPool is full'
     WHERE id = srId;
    COMMIT;
    RETURN;
  END IF;
  -- Resolve the service class id to a name
  SELECT name INTO destSvcClass FROM SvcClass WHERE id = svcClassId;
  -- Check that the user has the necessary access rights to create a file in the
  -- destination service class. I.e Check for StagePutRequest access rights.
  checkPermission(destSvcClass, reuid, regid, 40, authDest);
  -- Check whether there are any diskcopies available for a disk2disk copy
  SELECT id, sourceSvcClassId INTO dcId, srcSvcClassId
    FROM (
      SELECT DiskCopy.id, SvcClass.name sourceSvcClass, SvcClass.id sourceSvcClassId
        FROM DiskCopy, FileSystem, DiskServer, DiskPool2SvcClass, SvcClass
       WHERE DiskCopy.castorfile = cfId
         AND DiskCopy.status IN (0, 10) -- STAGED, CANBEMIGR
         AND FileSystem.id = DiskCopy.fileSystem
         AND FileSystem.diskpool = DiskPool2SvcClass.parent
         AND DiskPool2SvcClass.child = SvcClass.id
         AND FileSystem.status IN (0, 1) -- PRODUCTION, DRAINING
         AND DiskServer.id = FileSystem.diskserver
         AND DiskServer.status IN (0, 1) -- PRODUCTION, DRAINING
         -- Check that the user has the necessary access rights to replicate a
         -- file from the source service class. Note: instead of using a
         -- StageGetRequest type here we use a StagDiskCopyReplicaRequest type
         -- to be able to distinguish between and read and replication requst.
         AND checkPermissionOnSvcClass(SvcClass.name, reuid, regid, 133) = 0
         AND NOT EXISTS (
           -- Don't select source diskcopies which already failed more than 10 times
           SELECT 'x'
             FROM StageDiskCopyReplicaRequest R, SubRequest
            WHERE SubRequest.request = R.id
              AND R.sourceDiskCopy = DiskCopy.id
              AND SubRequest.status = 9 -- FAILED
           HAVING COUNT(*) >= 10)
       ORDER BY FileSystemRate(FileSystem.readRate, FileSystem.writeRate, FileSystem.nbReadStreams,
                               FileSystem.nbWriteStreams, FileSystem.nbReadWriteStreams,
                               FileSystem.nbMigratorStreams, FileSystem.nbRecallerStreams) DESC
    )
  WHERE authDest = 0
    AND ROWNUM < 2;
  -- We found at least one, therefore we schedule a disk2disk
  -- copy from the existing diskcopy not available to this svcclass
EXCEPTION WHEN NO_DATA_FOUND THEN
  -- We found no diskcopies at all. We should not schedule
  -- and make a tape recall... except ... in 2 cases :
  --   - if there is some temporarily unavailable diskcopy
  --     that is in CANBEMIGR or STAGEOUT
  -- in such a case, what we have is an existing file, that
  -- was migrated, then overwritten but never migrated again.
  -- So the unavailable diskCopy is the only copy that is valid.
  -- We will tell the client that the file is unavailable
  -- and he/she will retry later
  --   - if we have an available STAGEOUT copy. This can happen
  -- when the copy is in a given svcclass and we were looking
  -- in another one. Since disk to disk copy is impossible in this
  -- case, the file is declared BUSY.
  --   - if we have an available WAITFS, WAITFSSCHEDULING copy in such
  -- a case, we tell the client that the file is BUSY
  DECLARE
    dcStatus NUMBER;
    fsStatus NUMBER;
    dsStatus NUMBER;
  BEGIN
    SELECT DiskCopy.status, nvl(FileSystem.status, 0), nvl(DiskServer.status, 0)
      INTO dcStatus, fsStatus, dsStatus
      FROM DiskCopy, FileSystem, DiskServer
     WHERE DiskCopy.castorfile = cfId
       AND DiskCopy.status IN (5, 6, 10, 11) -- WAITFS, STAGEOUT, CANBEMIGR, WAITFSSCHEDULING
       AND FileSystem.id(+) = DiskCopy.fileSystem
       AND DiskServer.id(+) = FileSystem.diskserver
       AND ROWNUM < 2;
    -- We are in one of the special cases. Don't schedule, don't recall
    dcId := -1;
    UPDATE SubRequest
       SET status = 7, -- FAILED
           errorCode = CASE
             WHEN dcStatus IN (5,11) THEN 16 -- WAITFS, WAITFSSCHEDULING, EBUSY
             WHEN dcStatus = 6 AND fsStatus = 0 and dsStatus = 0 THEN 16 -- STAGEOUT, PRODUCTION, PRODUCTION, EBUSY
             ELSE 1718 -- ESTNOTAVAIL
           END,
           errorMessage = CASE
             WHEN dcStatus IN (5, 11) THEN -- WAITFS, WAITFSSCHEDULING
               'File is being (re)created right now by another user'
             WHEN dcStatus = 6 AND fsStatus = 0 and dsStatus = 0 THEN -- STAGEOUT, PRODUCTION, PRODUCTION
               'File is being written to in another SvcClass'
             ELSE
               'All copies of this file are unavailable for now. Please retry later'
           END
     WHERE id = srId;
    COMMIT;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    -- We did not find the very special case, so if the user has the necessary
    -- access rights to create file in the destination service class we 
    -- trigger a tape recall.
    IF authDest = 0 THEN
      dcId := 0;
    ELSE
      dcId := -1;
      UPDATE SubRequest
         SET status = 7, -- FAILED
             errorCode = 13, -- EACCES
             errorMessage = 'Insufficient user privileges to trigger a recall or file replication request to the '''||destSvcClass||''' service class '
       WHERE id = srId;
      COMMIT;
    END IF;
  END;
END;


/*
 * PL/SQL method implementing filesDeleted
 * Note that we don't increase the freespace of the fileSystem.
 * This is done by the monitoring daemon, that knows the
 * exact amount of free space.
 * dcIds gives the list of diskcopies to delete.
 * fileIds returns the list of castor files to be removed
 * from the name server
 */
CREATE OR REPLACE PROCEDURE filesDeletedProc
(dcIds IN castor."cnumList",
 fileIds OUT castor.FileList_Cur) AS
BEGIN
  IF dcIds.COUNT > 0 THEN
    -- list the castorfiles to be cleaned up afterwards
    FORALL i IN dcIds.FIRST .. dcIds.LAST
      INSERT INTO filesDeletedProcHelper VALUES
           ((SELECT castorFile FROM DiskCopy
              WHERE id = dcIds(i)));
    -- Loop over the deleted files; first use FORALL for bulk operation
    FORALL i IN dcIds.FIRST .. dcIds.LAST
      DELETE FROM Id2Type WHERE id = dcIds(i);
    FORALL i IN dcIds.FIRST .. dcIds.LAST
      DELETE FROM DiskCopy WHERE id = dcIds(i);
    -- then use a normal loop to clean castorFiles
    FOR cf IN (SELECT * FROM filesDeletedProcHelper) LOOP
      deleteCastorFile(cf.cfId);
    END LOOP;
  END IF;
  OPEN fileIds FOR SELECT * FROM FilesDeletedProcOutput;
END;


/* createOrUpdateStream */
CREATE OR REPLACE PROCEDURE createOrUpdateStream
(svcClassName IN VARCHAR2,
 initialSizeToTransfer IN NUMBER, -- total initialSizeToTransfer for the svcClass
 volumeThreashold IN NUMBER, -- parameter given by -V
 initialSizeCeiling IN NUMBER, -- to calculate the initialSizeToTransfer per stream
 doClone IN INTEGER,
 nbMigrationCandidate IN INTEGER,
 retCode OUT INTEGER) -- all candidate before applying the policy
AS
  nbOldStream NUMBER; -- stream for the specific svcclass
  nbDrives NUMBER; -- drives associated to the svcclass
  initSize NUMBER; --  the initialSizeToTransfer per stream
  tpId NUMBER; -- tape pool id
  strId NUMBER; -- stream id
  streamToClone NUMBER; -- stream id to clone
  svcId NUMBER; --svcclass id
  tcId NUMBER; -- tape copy id
  oldSize NUMBER; -- value for a cloned stream
BEGIN
  retCode := 0;
  -- get streamFromSvcClass
  BEGIN
    SELECT id INTO svcId FROM SvcClass 
     WHERE name = svcClassName AND ROWNUM <2;
    SELECT count(Stream.id) INTO nbOldStream 
      FROM Stream, SvcClass2TapePool
     WHERE SvcClass2TapePool.child = Stream.tapepool 
       AND SvcClass2TapePool.parent = svcId;
  EXCEPTION
    WHEN NO_DATA_FOUND THEN
    -- RTCPCLD_MSG_NOTPPOOLS
    -- restore candidate 
    retCode := -1;
    RETURN;
  END; 
    
  IF nbOldStream <= 0 AND initialSizeToTransfer < volumeThreashold THEN
    -- restore WAITINSTREAM to TOBEMIGRATED, not enough data
    retCode :=-2 ; -- RTCPCLD_MSG_DATALIMIT
    RETURN;
  END IF;
    
  IF nbOldStream >=0 AND (doClone = 1 OR nbMigrationCandidate > 0) THEN
    -- stream creator
    SELECT SvcClass.nbDrives INTO nbDrives FROM SvcClass WHERE id = svcId;
    IF nbDrives = 0 THEN
    	retCode :=-3 ; -- RESTORE NEEDED
    	RETURN;
    END IF;
    -- get the initialSizeToTransfer to associate to the stream
    IF initialSizeToTransfer/nbDrives > initialSizeCeiling THEN
      initSize := initialSizeCeiling;
    ELSE 
      initSize := initialSizeToTransfer/nbDrives;
    END IF;

    -- loop until the max number of stream
    IF nbOldStream < nbDrives THEN
      LOOP   
        -- get the tape pool with less stream
        BEGIN    
         -- tapepool without stream randomly chosen    
          SELECT a INTO tpId
            FROM ( 
              SELECT TapePool.id AS a FROM TapePool,SvcClass2TapePool  
               WHERE TapePool.id NOT IN (SELECT TapePool FROM Stream)
                 AND TapePool.id = SvcClass2TapePool.child
	         AND SvcClass2TapePool.parent = svcId 
            ORDER BY dbms_random.value
	    ) WHERE ROWNUM < 2;
        EXCEPTION WHEN NO_DATA_FOUND THEN
          -- at least one stream foreach tapepool
           SELECT tapepool INTO tpId 
             FROM (
               SELECT tapepool, count(*) AS c 
                 FROM Stream 
                WHERE tapepool IN (
                  SELECT SvcClass2TapePool.child 
                    FROM SvcClass2TapePool 
                   WHERE SvcClass2TapePool.parent = svcId)
             GROUP BY tapepool 
             ORDER BY c ASC, dbms_random.value)
           WHERE ROWNUM < 2;
	END;
	           
        -- STREAM_CREATED
        INSERT INTO Stream 
          (id, initialsizetotransfer, lastFileSystemChange, tape, lastFileSystemUsed, 
           lastButOneFileSystemUsed, tapepool, status)  
        VALUES (ids_seq.nextval, initSize, 0, 0, 0, 0, tpId, 5) RETURN id INTO strId;
        INSERT INTO Id2Type (id, type) values (strId,26); -- Stream type
    	IF doClone = 1 THEN
	  BEGIN
	  -- clone the new stream with one from the same tapepool
	  	SELECT id, initialsizetotransfer INTO streamToClone, oldSize 
            	FROM Stream WHERE tapepool = tpId AND id != strId AND ROWNUM < 2; 
          	FOR tcId IN (SELECT child FROM Stream2TapeCopy 
                        WHERE Stream2TapeCopy.parent = streamToClone) 
          	LOOP
            -- a take the first one, they are supposed to be all the same
          	  INSERT INTO stream2tapecopy (parent, child) VALUES (strId, tcId.child); 
          	END LOOP;
          	UPDATE Stream set initialSizeToTransfer=oldSize WHERE id=strId;        
           EXCEPTION WHEN NO_DATA_FOUND THEN
  		-- no stream to clone for this tapepool
  		NULL;
	   END;     	
	END IF;
        nbOldStream := nbOldStream + 1;
        EXIT WHEN nbOldStream >= nbDrives;
      END LOOP;
    END IF;
  END IF;
END;


/* attach tapecopies to stream */
CREATE OR REPLACE PROCEDURE attachTapeCopiesToStreams
(tapeCopyIds IN castor."cnumList",
 tapePoolIds IN castor."cnumList")
AS
  streamId NUMBER; -- stream attached to the tapepool
  counter NUMBER := 0;
  unused NUMBER;
  nbStream NUMBER;
BEGIN
  -- add choosen tapecopies to all Streams associated to the tapepool used by the policy 
  FOR i IN tapeCopyIds.FIRST .. tapeCopyIds.LAST LOOP
    BEGIN
      SELECT count(id) into nbStream FROM Stream 
       WHERE Stream.tapepool = tapePoolIds(i);
      IF nbStream <> 0 THEN 
        -- we have at least a stream for that tapepool
        SELECT id INTO unused 
          FROM TapeCopy 
         WHERE Status = 7 AND id = tapeCopyIds(i) FOR UPDATE;
        -- let's attach it to the different streams
        FOR streamId IN (SELECT id FROM Stream WHERE Stream.tapepool = tapePoolIds(i)) LOOP
          UPDATE TapeCopy SET Status = 2 WHERE Status = 7 AND id = tapeCopyIds(i);
          DECLARE CONSTRAINT_VIOLATED EXCEPTION;
          PRAGMA EXCEPTION_INIT (CONSTRAINT_VIOLATED, -1);
          BEGIN 
            INSERT INTO stream2tapecopy (parent ,child) VALUES (streamId.id, tapeCopyIds(i));
          EXCEPTION WHEN CONSTRAINT_VIOLATED THEN
            -- if the stream does not exist anymore
            UPDATE tapecopy SET status = 7 where id = tapeCopyIds(i);
            -- it might also be that the tapecopy does not exist anymore
          END;
        END LOOP; -- stream loop	
      END IF;
    EXCEPTION WHEN NO_DATA_FOUND THEN
      -- Go on the tapecopy has been resurrected or migrated
      NULL;
    END;   
    counter := counter + 1;
    IF counter = 100 THEN
      counter := 0;
      COMMIT;
    END IF;
  END LOOP; -- loop tapecopies
      	  
  -- resurrect the one never attached
  FOR i IN tapeCopyIds.FIRST .. tapeCopyIds.LAST LOOP
    UPDATE TapeCopy SET Status = 1 WHERE id = tapeCopyIds(i) AND Status = 7;
  END LOOP;  
  COMMIT;
END;

/* start choosen stream */
CREATE OR REPLACE PROCEDURE startChosenStreams
        (streamIds IN castor."cnumList", initSize IN NUMBER) AS
BEGIN	
  FORALL i IN streamIds.FIRST .. streamIds.LAST
    UPDATE Stream 
       SET status = 0, -- PENDING
           -- initialSize overwritten to initSize only if it is 0
           initialSizeToTransfer = decode(initialSizeToTransfer, 0, initSize, initialSizeToTransfer)
     WHERE Stream.status = 7 -- WAITPOLICY
       AND id = streamIds(i);
  COMMIT;
END;

/* stop chosen stream */
CREATE OR REPLACE PROCEDURE startChosenStreams
        (streamIds IN castor."cnumList", initSize IN NUMBER) AS
  nbTc NUMBER;
BEGIN	
  FOR i IN streamIds.FIRST .. streamIds.LAST LOOP
    BEGIN  
      SELECT count(*) INTO nbTc FROM stream2tapecopy WHERE parent = streamIds(i); 
      IF nbTc = 0 THEN
        DELETE FROM Stream where id = streamIds(i);
      ELSE
        UPDATE Stream 
           SET status = 0, -- PENDING
                -- initialSize overwritten to initSize only if it is 0
                initialSizeToTransfer = decode(initialSizeToTransfer, 0, initSize, initialSizeToTransfer)
         WHERE Stream.status = 7 -- WAITPOLICY
           AND id = streamIds(i);
      END IF;
      COMMIT;
   END;  
  END LOOP;
END;
