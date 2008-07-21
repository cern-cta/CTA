/******************************************************************************
 *              2.1.7-10_to_2.1.7-11.sql
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
 * @(#)$RCSfile: 2.1.7-10_to_2.1.7-12.sql,v $ $Release: 1.2 $ $Release$ $Date: 2008/07/21 12:24:05 $ $Author: waldron $
 *
 * This script upgrades a CASTOR v2.1.7-10 database into v2.1.7-11
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

/* Stop on errors - this only works from sqlplus */
WHENEVER SQLERROR EXIT FAILURE;

/* Version cross check and update */
DECLARE
  unused VARCHAR(100);
BEGIN
  SELECT release INTO unused FROM CastorVersion WHERE release LIKE '2_1_7_10%';
EXCEPTION WHEN NO_DATA_FOUND THEN
  -- Error, we can't apply this script
  raise_application_error(-20000, 'PL/SQL release mismatch. Please run previous upgrade scripts before this one.');
END;

UPDATE CastorVersion SET schemaVersion = '2_1_7_11', release = '2_1_7_11';
COMMIT;

/* Schema changes go here */
/**************************/

/* Name remaining unnamed indexes */
BEGIN
  FOR a IN (SELECT index_name
              FROM user_indexes
             WHERE table_name = 'TYPE2OBJ'
               AND index_name LIKE 'SYS_%')
  LOOP
    EXECUTE IMMEDIATE 'ALTER INDEX '|| a.index_name ||' RENAME TO I_Type2Obj_Obj';
  END LOOP;
END;

/* Precision on floats */
ALTER TABLE DiskCopy MODIFY (gcWeight NUMBER);
ALTER TABLE FileSystem MODIFY (minFreeSpace NUMBER, minAllowedFreeSpace NUMBER, maxFreeSpace NUMBER);
ALTER TABLE SetFileGcWeight MODIFY (weight NUMBER);

INSERT INTO Type2Obj (type, object) VALUES (159, 'VectorAddress');
INSERT INTO Type2Obj (type, object) VALUES (160, 'Tape2DriveDedication');

/* Function-based index on the DiskCopy table for BEINGDELETED status */
CREATE INDEX I_DiskCopy_Status_9 ON DiskCopy (decode(status,9,status,NULL));

/* Global temporary table to store ids temporarily in the bulkCreateObj procedures */
CREATE GLOBAL TEMPORARY TABLE bulkSelectHelper(objId NUMBER) ON COMMIT DELETE ROWS;

/* Update and revalidation of all PL-SQL code */
/**********************************************/

/* Get the tapecopys, segments and streams associated with the reference number */
CREATE OR REPLACE FUNCTION getTCs(ref number) RETURN castor_debug.TapeCopyDebug PIPELINED AS
BEGIN
  FOR t IN (SELECT TapeCopy.id AS TCId, TapeCopy.status AS TCStatus,
                   Segment.Id, Segment.status AS SegStatus, Segment.errorCode AS SegErrCode,
                   Tape.vid AS VID, Tape.tpMode AS tpMode, Tape.Status AS TapeStatus,
                   count(*) AS nbStreams, Segment.errMsgTxt AS SegErr
              FROM TapeCopy, Segment, Tape, Stream2TapeCopy
             WHERE TapeCopy.id = Segment.copy(+)
               AND Segment.tape = Tape.id(+)
               AND TapeCopy.castorfile = getCF(ref)
               AND Stream2TapeCopy.child = TapeCopy.id
              GROUP BY TapeCopy.id, TapeCopy.status, Segment.id, Segment.status, Segment.errorCode,
                       Tape.vid, Tape.tpMode, Tape.Status, Segment.errMsgTxt) LOOP
     PIPE ROW(t);
  END LOOP;
END;

/* PL/SQL method implementing bestTapeCopyForStream */
CREATE OR REPLACE PROCEDURE bestTapeCopyForStream(streamId IN INTEGER,
                                                  diskServerName OUT VARCHAR2, mountPoint OUT VARCHAR2,
                                                  path OUT VARCHAR2, dci OUT INTEGER,
                                                  castorFileId OUT INTEGER, fileId OUT INTEGER,
                                                  nsHost OUT VARCHAR2, fileSize OUT INTEGER,
                                                  tapeCopyId OUT INTEGER, optimized IN INTEGER) AS
  policy VARCHAR(2048);
BEGIN
  -- get the policy name
  BEGIN
    SELECT migrSelectPolicy INTO policy
      FROM Stream, TapePool
     WHERE Stream.id = streamId
       AND Stream.tapePool = TapePool.id;
    -- check for NULL value
    IF policy IS NULL THEN
      policy := 'defaultMigrSelPolicy';
    END IF;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    policy := 'defaultMigrSelPolicy';
  END;
  EXECUTE IMMEDIATE 'BEGIN ' || policy || '(:streamId, :diskServerName, :mountPoint, :path, :dci, :castorFileId, :fileId, :nsHost, :fileSize, :tapeCopyId, :optimized); END;'
    USING IN streamId, OUT diskServerName, OUT mountPoint, OUT path, OUT dci, OUT castorFileId, OUT fileId, OUT nsHost, OUT fileSize, OUT tapeCopyId, IN optimized;
END;

/* PL/SQL method implementing selectFiles2Delete
   This is the standard garbage collector: it sorts STAGED
   diskcopies by gcWeight and selects them for deletion up to
   the desired free space watermark */
CREATE OR REPLACE PROCEDURE selectFiles2Delete(diskServerName IN VARCHAR2,
                                               files OUT castorGC.SelectFiles2DeleteLine_Cur) AS
  dcIds "numList";
  freed INTEGER;
  deltaFree INTEGER;
  toBeFreed INTEGER;
  dontGC INTEGER;
BEGIN
  -- First of all, check if we have GC enabled
  dontGC := 0;
  FOR sc IN (SELECT gcEnabled 
               FROM SvcClass, DiskPool2SvcClass D2S, DiskServer, FileSystem
              WHERE SvcClass.id = D2S.child
                AND D2S.parent = FileSystem.diskPool
                AND FileSystem.diskServer = DiskServer.id
                AND DiskServer.name = diskServerName) LOOP
    -- If any of the service classes to which we belong (normally a single one)
    -- says don't GC, we don't GC STAGED files.
    IF sc.gcEnabled = 0 THEN
      dontGC := 1;
      EXIT;
    END IF;
  END LOOP;
  -- Loop on all concerned fileSystems
  FOR fs IN (SELECT FileSystem.id
               FROM FileSystem, DiskServer
              WHERE FileSystem.diskServer = DiskServer.id
                AND DiskServer.name = diskServerName) LOOP
    -- First take the INVALID diskcopies, they have to go in any case
    UPDATE DiskCopy
       SET status = 9, -- BEING_DELETED
           lastAccessTime = getTime() -- See comment below on the status = 9 condition
     WHERE fileSystem = fs.id 
       AND (
             (status = 7 AND NOT EXISTS -- INVALID
               (SELECT 'x' FROM SubRequest
                 WHERE SubRequest.diskcopy = DiskCopy.id
                   AND SubRequest.status IN (0, 1, 2, 3, 4, 5, 6, 12, 13, 14))) -- All but FINISHED, FAILED*, ARCHIVED
        OR (status = 9 AND lastAccessTime < getTime() - 1800))
        -- For failures recovery we also take all DiskCopies which were already
        -- selected but got stuck somehow and didn't get removed after 30 mins. 
    RETURNING id BULK COLLECT INTO dcIds;
    COMMIT;

    -- Continue processing but with STAGED files.
    IF dontGC = 0 THEN
      -- Determine the space that would be freed if the INVALID files selected above
      -- were to be removed
      IF dcIds.COUNT > 0 THEN
        SELECT SUM(diskCopySize) INTO freed
          FROM DiskCopy
         WHERE DiskCopy.id IN 
             (SELECT /*+ CARDINALITY(fsidTable 5) */ * 
                FROM TABLE(dcIds) dcidTable);
      ELSE
        freed := 0;
      END IF;
      -- Get the amount of space to be liberated
      SELECT decode(sign(maxFreeSpace * totalSize - free), -1, 0, maxFreeSpace * totalSize - free)
        INTO toBeFreed
        FROM FileSystem
       WHERE id = fs.id;
      -- If space is still required even after removal of INVALID files, consider
      -- removing STAGED files until we are below the free space watermark
      IF freed < toBeFreed THEN
        -- Loop on file deletions
        FOR dc IN (SELECT id, castorFile FROM DiskCopy
                    WHERE fileSystem = fs.id
                      AND status = 0 -- STAGED
                      AND NOT EXISTS (
                          SELECT 'x' FROM SubRequest 
                           WHERE DiskCopy.status = 0 AND diskcopy = DiskCopy.id 
                             AND SubRequest.status IN (0, 1, 2, 3, 4, 5, 6, 12, 13, 14)) -- All but FINISHED, FAILED*, ARCHIVED
                      ORDER BY gcWeight ASC) LOOP
          -- Mark the DiskCopy
          UPDATE DiskCopy SET status = 9 -- BEINGDELETED
           WHERE id = dc.id RETURNING diskCopySize INTO deltaFree;
          -- Update toBeFreed
          freed := freed + deltaFree;
          -- Shall we continue ?
          IF toBeFreed <= freed THEN
            EXIT;
          END IF;
        END LOOP;
      END IF;
      COMMIT;
    END IF;
  END LOOP;
      
  -- Now select all the BEINGDELETED diskcopies in this diskserver for the gcDaemon
  OPEN files FOR
    SELECT /*+ INDEX(CastorFile I_CastorFile_ID) */ FileSystem.mountPoint || DiskCopy.path, DiskCopy.id,
	   Castorfile.fileid, Castorfile.nshost
      FROM CastorFile, DiskCopy, FileSystem, DiskServer
     WHERE decode(DiskCopy.status,9,DiskCopy.status,NULL) = 9 -- BEINGDELETED
       AND DiskCopy.castorfile = CastorFile.id
       AND DiskCopy.fileSystem = FileSystem.id
       AND FileSystem.diskServer = DiskServer.id
       AND DiskServer.name = diskServerName;
END;
