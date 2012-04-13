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
     AND release = '2_1_12_4_3'
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
VALUES ('2_1_12_0', '2_1_12_4_3', 'TRANSPARENT');
COMMIT;

/* Update and revalidation of PL-SQL code */
/******************************************/
/* PL/SQL method to get the next failed SubRequest to do according to the given service */
/* the service parameter is not used now, it will with the new stager */
CREATE OR REPLACE PROCEDURE subRequestFailedToDo(srId OUT NUMBER, srFileName OUT VARCHAR2, srSubReqId OUT VARCHAR2,
                                                 srErrorCode OUT INTEGER, srErrorMessage OUT VARCHAR2, rReqId OUT VARCHAR2,
                                                 clIpAddress OUT INTEGER, clPort OUT INTEGER, clVersion OUT INTEGER,
                                                 srFileId OUT NUMBER) AS
  SrLocked EXCEPTION;
  PRAGMA EXCEPTION_INIT (SrLocked, -54);
  CURSOR c IS
     SELECT /*+ FIRST_ROWS(10) INDEX(SR I_SubRequest_RT_CT_ID) */ SR.id
       FROM SubRequest PARTITION (P_STATUS_7) SR; -- FAILED
  varSRId NUMBER;
  varCFId NUMBER;
  varRId NUMBER;
  varSrAnswered INTEGER;
  varRName VARCHAR2(100);
  varClientId NUMBER;
BEGIN
  OPEN c;
  LOOP
    FETCH c INTO varSRId;
    EXIT WHEN c%NOTFOUND;
    BEGIN
      SELECT /*+ INDEX(Subrequest PK_Subrequest_Id)*/ answered INTO varSrAnswered
        FROM SubRequest PARTITION (P_STATUS_7) 
       WHERE id = varSRId FOR UPDATE NOWAIT;
      IF varSrAnswered = 1 THEN
        -- already answered, archive and move on
        archiveSubReq(varSRId, dconst.SUBREQUEST_FAILED_FINISHED);
        -- release the lock on this request as it's completed
        COMMIT;
      ELSE
        -- we got our subrequest, select all relevant data and hold the lock
        SELECT /*+ INDEX(Subrequest PK_Subrequest_Id)*/ fileName, subReqId, errorCode, errorMessage,
          (SELECT object FROM Type2Obj WHERE type = reqType), request, castorFile
          INTO srFileName, srSubReqId, srErrorCode, srErrorMessage, varRName, varRId, varCFId
          FROM SubRequest
         WHERE id = varSRId;
        srId := varSRId;
        srFileId := 0;
        BEGIN
          CASE
            WHEN varRName = 'StagePrepareToPutRequest' THEN
              SELECT reqId, client
                INTO rReqId, varClientId
                FROM StagePrepareToPutRequest WHERE id = varRId;
            WHEN varRName = 'StagePrepareToGetRequest' THEN
              SELECT reqId, client
                INTO rReqId, varClientId
                FROM StagePrepareToGetRequest WHERE id = varRId;
            WHEN varRName = 'StagePrepareToUpdateRequest' THEN
              SELECT reqId, client
                INTO rReqId, varClientId
                FROM StagePrepareToUpdateRequest WHERE id = varRId;
            WHEN varRName = 'StageRepackRequest' THEN
              SELECT reqId, client
                INTO rReqId, varClientId
                FROM StageRepackRequest WHERE id = varRId;
            WHEN varRName = 'StagePutRequest' THEN
              SELECT reqId, client
                INTO rReqId, varClientId
                FROM StagePutRequest WHERE id = varRId;
            WHEN varRName = 'StageGetRequest' THEN
              SELECT reqId, client
                INTO rReqId, varClientId
                FROM StageGetRequest WHERE id = varRId;
            WHEN varRName = 'StageUpdateRequest' THEN
              SELECT reqId, client
                INTO rReqId, varClientId
                FROM StageUpdateRequest WHERE id = varRId;
            WHEN varRName = 'StagePutDoneRequest' THEN
              SELECT reqId, client
                INTO rReqId, varClientId
                FROM StagePutDoneRequest WHERE id = varRId;
            WHEN varRName = 'StageRmRequest' THEN
              SELECT reqId, client
                INTO rReqId, varClientId
                FROM StageRmRequest WHERE id = varRId;
            WHEN varRName = 'SetFileGCWeight' THEN
              SELECT reqId, client
                INTO rReqId, varClientId
                FROM SetFileGCWeight WHERE id = varRId;
            ELSE
              -- Unsupported request type, should never happen
              RAISE NO_DATA_FOUND;
          END CASE;
          SELECT ipAddress, port, version
            INTO clIpAddress, clPort, clVersion
            FROM Client WHERE id = varClientId;
          IF varCFId > 0 THEN
            SELECT fileId INTO srFileId FROM CastorFile WHERE id = varCFId;
          END IF;
          EXIT;
        EXCEPTION WHEN NO_DATA_FOUND THEN
          -- This should never happen, we have either an orphaned subrequest
          -- or a request with an unsupported type.
          -- As we couldn't get the client, we just archive and move on.
          -- XXX For next version, call logToDLF() instead of silently archive. 
          srId := 0;
          archiveSubReq(varSRId, dconst.SUBREQUEST_FAILED_FINISHED);
          COMMIT;
        END;
      END IF;
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        -- Go to next candidate, this subrequest was processed already and its status changed
        NULL;
      WHEN SrLocked THEN
        -- Go to next candidate, this subrequest is being processed by another thread
        NULL;
    END;
  END LOOP;
  CLOSE c;
END;
/

CREATE OR REPLACE PROCEDURE cancelRecall(cfId NUMBER, dcId NUMBER) AS
  srIds "numList";
  unused NUMBER;
BEGIN
  -- Lock the CastorFile
  SELECT id INTO unused FROM CastorFile
   WHERE id = cfId FOR UPDATE;
  -- Cancel the recall
  deleteRecallJobs(cfId);
  -- Invalidate the DiskCopy
  UPDATE DiskCopy SET status = dconst.DISKCOPY_INVALID WHERE id = dcId;
  -- Look for and update requests associated to the recall plus dependent ones
  UPDATE /*+ INDEX(Subrequest I_Subrequest_Diskcopy) */ SubRequest
     SET status = dconst.SUBREQUEST_RESTART,
         answered = 0
   WHERE diskCopy = dcId
     AND status = dconst.SUBREQUEST_WAITTAPERECALL
  RETURNING id BULK COLLECT INTO srIds;
  UPDATE /*+ INDEX(Subrequest I_Subrequest_Parent)*/ SubRequest
     SET status = dconst.SUBREQUEST_RESTART,
         parent = 0
   WHERE status = dconst.SUBREQUEST_WAITSUBREQ
     AND parent IN
       (SELECT /*+ CARDINALITY(sridTable 5) */ *
          FROM TABLE(srIds) sridTable)
     AND castorfile = cfId;
END;
/

/* PL/SQL method implementing bulkCheckFSBackInProd for processing
 * filesystems in one bulk operation to optimise database performance
 */
CREATE OR REPLACE PROCEDURE bulkCheckFSBackInProd AS
  fsIds "numList";
BEGIN
  -- Extract a list of filesystems which have been scheduled to be
  -- checked in a bulk operation on the database.
  UPDATE FileSystemsToCheck SET toBeChecked = 0
   WHERE toBeChecked = 1
  RETURNING fileSystem BULK COLLECT INTO fsIds;
  -- Nothing found, return
  IF fsIds.COUNT = 0 THEN
    RETURN;
  END IF;
  -- Look for recalls concerning files that are STAGED or CANBEMIGR
  -- on all filesystems scheduled to be checked, and restart their
  -- subrequests (reconsidering the recall source).
  FOR dc IN (SELECT /*+ USE_NL(D E)
                     INDEX_RS_ASC(D I_DiskCopy_FileSystem)
                     INDEX_RS_ASC(E I_DiskCopy_CastorFile) */
                    UNIQUE D.castorfile, E.id
               FROM DiskCopy D, DiskCopy E
              WHERE D.castorfile = E.castorfile
                AND D.fileSystem IN
                  (SELECT /*+ CARDINALITY(fsidTable 5) */ *
                     FROM TABLE(fsIds) fsidTable)
                AND D.status IN (dconst.DISKCOPY_STAGED, dconst.DISKCOPY_CANBEMIGR)
                AND E.status = dconst.DISKCOPY_WAITTAPERECALL) LOOP
    -- Cancel recall
    cancelRecall(dc.castorfile, dc.id);
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
 WHERE release = '2_1_12_4_3';
COMMIT;
