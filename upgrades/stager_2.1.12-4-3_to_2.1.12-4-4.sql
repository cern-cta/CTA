/******************************************************************************
 *                 stager_2.1.12-4-2_to_2.1.12-4-3.sql
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
 * This script upgrades a CASTOR v2.1.12-4-2 STAGER database to v2.1.12-4-3
 *
 * It fixes the following bug :
 *    bug #92384: Incorrect restarting of replication requests when a diskserver comes back online
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
     AND release = '2_1_12_4_4'
     AND state != 'COMPLETE';
  COMMIT;
END;
/

/* Verify that the script is running against the correct schema and version */
DECLARE
  unused VARCHAR(100);
BEGIN
  -- here we accept ANY 2.1.12-4 hotfix as starting point, on the
  -- assumption that 2.1.12-5 will reset and align all relevant PL/SQL code.
  SELECT release INTO unused FROM CastorVersion
   WHERE schemaName = 'STAGER'
     AND release LIKE '2_1_12_4_%';
EXCEPTION WHEN NO_DATA_FOUND THEN
  -- Error, we cannot apply this script
  raise_application_error(-20000, 'PL/SQL release mismatch. Please run previous upgrade scripts for the STAGER before this one.');
END;
/

INSERT INTO UpgradeLog (schemaVersion, release, type)
VALUES ('2_1_12_0', '2_1_12_4_4', 'TRANSPARENT');
COMMIT;

/* Update and revalidation of PL-SQL code */
/******************************************/
/* delete taperequest  for not existing tape */
CREATE OR REPLACE
PROCEDURE tg_deleteTapeRequest( inTGReqId IN NUMBER ) AS
  /* The tape gateway request does not exist per se, but 
   * references to its ID should be removed (with needed consequences
   * from the structures pointing to it) */
  CONSTRAINT_VIOLATED EXCEPTION;
  PRAGMA EXCEPTION_INIT(CONSTRAINT_VIOLATED, -02292);
  varTpReqId NUMBER;     -- Tape Recall (TapeGatewayReequest.TapeRecall)
  varMountId NUMBER;       -- migration mount Id.
  varSegNum INTEGER;
  varCfId NUMBER;        -- CastorFile Id
  varRjIds "numList";    -- recall job IDs
BEGIN
  -- Find the relevant migration mount or reading tape id.
  tg_findFromTGRequestId (inTGReqId, varTpReqId, varMountId);
  -- Find out whether this is a read or a write
  IF (varTpReqId IS NOT NULL) THEN
    -- Lock and reset the tape in case of a read
    UPDATE Tape T
      SET T.status = tconst.TAPE_UNUSED
      WHERE T.id = varTpReqId;
    SELECT SEG.copy BULK COLLECT INTO varRjIds 
      FROM Segment SEG 
     WHERE SEG.tape = varTpReqId;
    FOR i IN varRjIds.FIRST .. varRjIds.LAST  LOOP
      -- lock castorFile, skip if it's missing
      BEGIN
        SELECT RJ.castorFile INTO varCfId 
          FROM RecallJob RJ, CastorFile CF
          WHERE RJ.id = varRjIds(i) 
          AND CF.id = RJ.castorfile 
          FOR UPDATE OF CF.id;
      EXCEPTION
        WHEN NO_DATA_FOUND THEN
          /* We have a recall orphaned of it castofile (usually with castorfile = 0)
           * We just drop the segment and recalljob as all the rest is castorfile
           * driven and hence will fail. Setting varCfId to NULL makes the remainder
           * of the loop neutral. */
          varCfId := NULL;
          DELETE FROM Segment WHERE copy = varRjIds(i);
          DELETE FROM RecallJob WHERE id = varRjIds(i);
          COMMIT;
      END;
      -- fail diskcopy, drop recall and migration jobs
      UPDATE DiskCopy DC SET DC.status = dconst.DISKCOPY_FAILED
       WHERE DC.castorFile = varCfId 
         AND DC.status = dconst.DISKCOPY_WAITTAPERECALL;
      deleteRecallJobs(varCfId);
      -- Fail the subrequest
      UPDATE /*+ INDEX(SR I_Subrequest_Castorfile)*/ SubRequest SR
         SET SR.status = dconst.SUBREQUEST_FAILED,
             SR.getNextStatus = dconst.GETNEXTSTATUS_FILESTAGED, --  (not strictly correct but the request is over anyway)
             SR.lastModificationTime = getTime(),
             SR.errorCode = 1015,  -- SEINTERNAL
             SR.errorMessage = 'File recall from tape has failed, please try again later',
             SR.parent = 0
       WHERE SR.castorFile = varCfId 
         AND SR.status IN (dconst.SUBREQUEST_WAITTAPERECALL, dconst.SUBREQUEST_WAITSUBREQ);
       -- Release lock on castorFile
       COMMIT;
    END LOOP;
  ELSIF (varMountId IS NOT NULL) THEN
    -- In case of a write, delete the migration mount
    DELETE FROM MigrationMount  WHERE vdqmVolReqId = varMountId;
  ELSE
    -- Wrong Access Mode encountered. Notify.
    RAISE_APPLICATION_ERROR(-20292, 'tg_deleteTapeRequest: no read tape or '||
      'migration mount found for TapeGatewayRequestId: '|| inTGReqId);
  END IF;
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
 WHERE release = '2_1_12_4_4';
COMMIT;
