/******************************************************************************
 *                 stager_2.1.12-4_to_2.1.12-4-1.sql
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
 * This script upgrades a CASTOR v2.1.12-4 STAGER database to v2.1.12-4-1
 *
 * It fixes the following bugs :
 *  - bug #92194: Tape gateway marks all files with an error when the migration session fails
 *  - bug #92211: Too many migrationmounts created
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

/* Stop on errors */
WHENEVER SQLERROR EXIT FAILURE
BEGIN
  -- If we have encountered an error rollback any previously non committed
  -- operations. This prevents the UPDATE of the UpgradeLog from committing
  -- inconsistent data to the database.
  ROLLBACK;
  UPDATE UpgradeLog
     SET failureCount = failureCount + 1
   WHERE schemaVersion = '2_1_12_0'
     AND release = '2_1_12_4_1'
     AND state != 'COMPLETE';
  COMMIT;
END;
/

/* Verify that the script is running against the correct schema and version */
DECLARE
  unused VARCHAR(100);
BEGIN
  SELECT release INTO unused FROM CastorVersion
   WHERE schemaName = 'STAGER'
     AND release LIKE '2_1_12_4%';
EXCEPTION WHEN NO_DATA_FOUND THEN
  -- Error, we cannot apply this script
  raise_application_error(-20000, 'PL/SQL release mismatch. Please run previous upgrade scripts for the STAGER before this one.');
END;
/

INSERT INTO UpgradeLog (schemaVersion, release, type)
VALUES ('2_1_12_0', '2_1_12_4_1', 'TRANSPARENT');
COMMIT;

/* Update and revalidation of PL-SQL code */
/******************************************/

CREATE OR REPLACE
PROCEDURE tg_endTapeSession(inTransId IN NUMBER, inErrorCode IN INTEGER) AS
  CONSTRAINT_VIOLATED EXCEPTION;
  PRAGMA EXCEPTION_INIT(CONSTRAINT_VIOLATED, -02292);
  varUnused NUMBER;
  varTpId NUMBER;        -- TapeGateway Taperecall
  varTgrId NUMBER;       -- TapeGatewayRequest ID
  varMountId NUMBER;     -- Migration mount ID
  varSegNum INTEGER;     -- Segment count
  varTcIds "numList";    -- recall/migration job Ids

BEGIN
  -- Prepare to revert changes
  SAVEPOINT MainEndTapeSession;
  -- Find the Tape read or migration mount for this VDQM request
  tg_findFromVDQMReqId (inTransId, varTpId, varMountId);
  -- Pre-process the read and write: find corresponding TapeGatewayRequest Id.
  -- Lock corresponding Tape or MigrationMount. This will bomb if we
  -- don't find exactly ones (which is good).
  varTgrId := NULL;
  IF (varTpId IS NOT NULL) THEN
    -- Find and lock tape
    SELECT TapeGatewayRequestId INTO varTgrId
      FROM Tape
     WHERE id = varTpId
       FOR UPDATE;
  ELSIF (varMountId IS NOT NULL) THEN
    -- Find and lock migration mount
    SELECT TapeGatewayRequestId INTO varTgrId
      FROM MigrationMount
     WHERE id = varMountId
       FOR UPDATE;
  ELSE
    -- Nothing found for the VDQMRequestId: whine and leave.
    ROLLBACK TO SAVEPOINT MainEndTapeSession;
    RAISE_APPLICATION_ERROR (-20119,
     'No tape or migration mount found for VDQM ID='|| inTransId);
  END IF;
  -- If we failed to get the TG req Id, no point in going further.
  IF (varTgrId IS NULL) THEN
    ROLLBACK TO SAVEPOINT MainEndTapeSession;
    RAISE_APPLICATION_ERROR (-20119,
     'Got NULL TapeGatewayRequestId for tape ID='|| varTpId||
     ' or MigrationMount Id='|| varMountId||' processing VDQM Id='||inTransId||
     ' in tg_endTapeSession.');
  END IF;

  -- Process the read case
  IF (varTpId IS NOT NULL) THEN
    -- find and lock the RecallJobs
    SELECT id BULK COLLECT INTO varTcIds
      FROM RecallJob
     WHERE TapeGatewayRequestId = varTgrId
       FOR UPDATE;
    IF (inErrorCode != 0) THEN
        -- if a failure is reported
        -- fail all the segments
        UPDATE Segment SEG
           SET SEG.status=tconst.SEGMENT_FAILED
         WHERE SEG.copy IN (SELECT * FROM TABLE(varTcIds));
        -- mark RecallJob as RECALLJOB_RETRY
        UPDATE RecallJob
           SET status    = tconst.RECALLJOB_RETRY,
               errorcode = inErrorCode
         WHERE id IN (SELECT * FROM TABLE(varTcIds));
    END IF;
    -- resurrect lost segments
    UPDATE Segment SEG
       SET SEG.status = tconst.SEGMENT_UNPROCESSED
     WHERE SEG.status = tconst.SEGMENT_SELECTED
       AND SEG.tape = varTpId;
    -- check if there is work for this tape
    SELECT count(*) INTO varSegNum
      FROM segment SEG
     WHERE SEG.Tape = varTpId
       AND status = tconst.SEGMENT_UNPROCESSED;
    -- Restart the unprocessed segments' tape if there are any.
    IF varSegNum > 0 THEN
      UPDATE Tape T
         SET T.status = tconst.TAPE_WAITPOLICY -- for rechandler
       WHERE T.id=varTpId;
    ELSE
      UPDATE Tape
         SET status = tconst.TAPE_UNUSED
       WHERE id=varTpId;
     END IF;
  ELSIF (varMountId IS NOT NULL) THEN
    -- find and lock the MigrationJobs.
    SELECT id BULK COLLECT INTO varTcIds
      FROM MigrationJob
     WHERE TapeGatewayRequestId = varTgrId
       FOR UPDATE;
    -- Process the write case
    -- just resurrect the remaining migrations
    UPDATE MigrationJob
       SET status = tconst.MIGRATIONJOB_PENDING,
           VID = NULL,
           TapeGatewayRequestId = NULL
     WHERE id IN (SELECT * FROM TABLE(varTcIds))
       AND status = tconst.MIGRATIONJOB_SELECTED;
    -- delete the migration mount
    DELETE FROM MigrationMount  WHERE tapegatewayrequestid = varMountId;
  ELSE

    -- Small infusion of paranoia ;-) We should never reach that point...
    ROLLBACK TO SAVEPOINT MainEndTapeSession;
    RAISE_APPLICATION_ERROR (-20119,
     'No tape or migration mount found on second pass for VDQM ID='|| inTransId ||
     ' in tg_endTapeSession');
  END IF;
  COMMIT;
END;
/

CREATE OR REPLACE PROCEDURE startMigrationMounts AS
  varNbMounts INTEGER;
  varDataAmount INTEGER;
  varNbFiles INTEGER;
  varOldestCreationTime NUMBER;
BEGIN
  -- loop through tapepools
  FOR t IN (SELECT id, nbDrives, minAmountDataForMount,
                   minNbFilesForMount, maxFileAgeBeforeMount
              FROM TapePool) LOOP
    -- get number of mounts already running for this tapepool
    SELECT count(*) INTO varNbMounts
      FROM MigrationMount
     WHERE tapePool = t.id;
    -- get the amount of data and number of files to migrate, plus the age of the oldest file
    BEGIN
      SELECT SUM(fileSize), COUNT(*), MIN(creationTime) INTO varDataAmount, varNbFiles, varOldestCreationTime
        FROM MigrationJob
       WHERE tapePool = t.id
         AND status = tconst.MIGRATIONJOB_PENDING
       GROUP BY tapePool;
      -- Create as many mounts as needed according to amount of data and number of files
      WHILE (varNbMounts < t.nbDrives) AND
            ((varDataAmount/(varNbMounts+1) >= t.minAmountDataForMount) OR
             (varNbFiles/(varNbMounts+1) >= t.minNbFilesForMount)) AND
            (varNbMounts < varNbFiles) LOOP   -- in case minAmountDataForMount << avgFileSize, stop creating more than one mount per file
        insertMigrationMount(t.id);
        varNbMounts := varNbMounts + 1;
      END LOOP;
      -- force creation of a unique mount in case no mount was created at all and some files are too old
      IF varNbFiles > 0 AND varNbMounts = 0 AND t.nbDrives > 0 AND
         gettime() - varOldestCreationTime > t.maxFileAgeBeforeMount THEN
        insertMigrationMount(t.id);
      END IF;
    EXCEPTION WHEN NO_DATA_FOUND THEN
      -- there is no file to migrate, so nothing to do
      NULL;
    END;
    COMMIT;
  END LOOP;
END;
/


/* Recompile all invalid procedures, triggers and functions */
/************************************************************/
BEGIN
  FOR a IN (SELECT object_name, object_type
              FROM user_objects
             WHERE object_type IN ('PROCEDURE', 'TRIGGER', 'FUNCTION', 'VIEW', 'PACKAGE BODY')
               AND status = 'INVALID')
  LOOP
    IF a.object_type = 'PACKAGE BODY' THEN a.object_type := 'PACKAGE'; END IF;
    BEGIN
      EXECUTE IMMEDIATE 'ALTER ' ||a.object_type||' '||a.object_name||' COMPILE';
    EXCEPTION WHEN OTHERS THEN
      -- ignore, so that we continue compiling the other invalid items
      NULL;
    END;
  END LOOP;
END;
/

/* Flag the schema upgrade as COMPLETE */
/***************************************/
UPDATE UpgradeLog SET endDate = sysdate, state = 'COMPLETE'
 WHERE release = '2_1_12_4_1';
COMMIT;
