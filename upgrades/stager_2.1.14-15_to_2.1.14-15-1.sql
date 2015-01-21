/******************************************************************************
 *                 stager_2.1.14-15_to_2.1.14-15-1.sql
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
 * This script upgrades a CASTOR v2.1.14-15 STAGER database to v2.1.14-15-1
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
   WHERE schemaVersion = '2_1_14_2'
     AND release = '2_1_14_15_1'
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
     AND release LIKE '2_1_14_15%';
EXCEPTION WHEN NO_DATA_FOUND THEN
  -- Error, we cannot apply this script
  raise_application_error(-20000, 'PL/SQL release mismatch. Please run previous upgrade scripts for the STAGER before this one.');
END;
/

INSERT INTO UpgradeLog (schemaVersion, release, type)
VALUES ('2_1_14_2', '2_1_14_15_1', 'TRANSPARENT');
COMMIT;

/* Update and revalidation of PL-SQL code */
/******************************************/

/* PL/SQL method creating MigrationJobs for missing segments of a file if needed */
/* Can throw a -20100 exception when no route to tape is found for the missing segments */
CREATE OR REPLACE PROCEDURE createMJForMissingSegments(inCfId IN INTEGER,
                                                       inFileSize IN INTEGER,
                                                       inFileClassId IN INTEGER,
                                                       inAllValidCopyNbs IN "numList",
                                                       inAllValidVIDs IN strListTable,
                                                       inNbExistingSegments IN INTEGER,
                                                       inFileId IN INTEGER,
                                                       inNsHost IN VARCHAR2,
                                                       inLogParams IN VARCHAR2) AS
  varExpectedNbCopies INTEGER;
  varCreatedMJs INTEGER := 0;
  varNextCopyNb INTEGER := 1;
  varNb INTEGER;
BEGIN
  -- check whether there are missing segments and whether we should create new ones
  SELECT nbCopies INTO varExpectedNbCopies FROM FileClass WHERE id = inFileClassId;
  IF varExpectedNbCopies > inNbExistingSegments THEN
    -- some copies are missing
    DECLARE
      unused INTEGER;
    BEGIN
      -- check presence of migration jobs for this file
      SELECT id INTO unused FROM MigrationJob WHERE castorFile=inCfId AND ROWNUM < 2;
      -- there are MigrationJobs already, so remigrations were already handled. Nothing to be done
      -- we typically are in a situation where the file was already waiting for recall for
      -- another recall group.
      -- log "detected missing copies on tape, but migrations ongoing"
      logToDLF(NULL, dlf.LVL_DEBUG, dlf.RECALL_MISSING_COPIES_NOOP, inFileId, inNsHost, 'stagerd',
               inLogParams || ' nbMissingCopies=' || TO_CHAR(varExpectedNbCopies-inNbExistingSegments));
      RETURN;
    EXCEPTION WHEN NO_DATA_FOUND THEN
      -- we need to remigrate this file
      NULL;
    END;
    -- log "detected missing copies on tape"
    logToDLF(NULL, dlf.LVL_SYSTEM, dlf.RECALL_MISSING_COPIES, inFileId, inNsHost, 'stagerd',
             inLogParams || ' nbMissingCopies=' || TO_CHAR(varExpectedNbCopies-inNbExistingSegments));
    -- copies are missing, try to recreate them
    WHILE varExpectedNbCopies > inNbExistingSegments + varCreatedMJs AND varNextCopyNb <= varExpectedNbCopies LOOP
      BEGIN
        -- check whether varNextCopyNb is already in use by a valid copy
        SELECT * INTO varNb FROM TABLE(inAllValidCopyNbs) WHERE COLUMN_VALUE=varNextCopyNb;
        -- this copy number is in use, go to next one
      EXCEPTION WHEN NO_DATA_FOUND THEN
        -- copy number is not in use, create a migrationJob using it (may throw exceptions)
        initMigration(inCfId, inFileSize, NULL, NULL, varNextCopyNb, tconst.MIGRATIONJOB_WAITINGONRECALL);
        varCreatedMJs := varCreatedMJs + 1;
        -- log "create new MigrationJob to migrate missing copy"
        logToDLF(NULL, dlf.LVL_SYSTEM, dlf.RECALL_MJ_FOR_MISSING_COPY, inFileId, inNsHost, 'stagerd',
                 inLogParams || ' copyNb=' || TO_CHAR(varNextCopyNb));
      END;
      varNextCopyNb := varNextCopyNb + 1;
    END LOOP;
    -- Did we create new copies ?
    IF varExpectedNbCopies > inNbExistingSegments + varCreatedMJs THEN
      -- We did not create enough new copies, this means that we did not find enough
      -- valid copy numbers. Odd... Log something !
      logToDLF(NULL, dlf.LVL_ERROR, dlf.RECALL_COPY_STILL_MISSING, inFileId, inNsHost, 'stagerd',
               inLogParams || ' nbCopiesStillMissing=' ||
               TO_CHAR(varExpectedNbCopies - inAllValidCopyNbs.COUNT - varCreatedMJs));
    ELSE
      -- Yes, then create migrated segments for the existing segments if there are none
      SELECT count(*) INTO varNb FROM MigratedSegment WHERE castorFile = inCfId;
      IF varNb = 0 THEN
        FOR i IN inAllValidCopyNbs.FIRST .. inAllValidCopyNbs.LAST LOOP
          INSERT INTO MigratedSegment (castorFile, copyNb, VID)
          VALUES (inCfId, inAllValidCopyNbs(i), inAllValidVIDs(i));
        END LOOP;
      END IF;
    END IF;
  END IF;
END;
/

/* PL/SQL method creating RecallJobs
 * It also creates MigrationJobs for eventually missing segments
 * It returns 0 if successful, else an error code
 */
CREATE OR REPLACE FUNCTION createRecallJobs(inCfId IN INTEGER,
                                            inFileId IN INTEGER,
                                            inNsHost IN VARCHAR2,
                                            inFileSize IN INTEGER,
                                            inFileClassId IN INTEGER,
                                            inRecallGroupId IN INTEGER,
                                            inSvcClassId IN INTEGER,
                                            inEuid IN INTEGER,
                                            inEgid IN INTEGER,
                                            inRequestTime IN NUMBER,
                                            inLogParams IN VARCHAR2) RETURN INTEGER AS
  -- list of all valid segments, whatever the tape status. Used to trigger remigrations
  varAllValidCopyNbs "numList" := "numList"();
  varAllValidVIDs strListTable := strListTable();
  varNbExistingSegments INTEGER := 0;
  -- whether we found a segment at all (valid or not). Used to detect potentially lost files
  varFoundSeg boolean := FALSE;
  varI INTEGER := 1;
  NO_TAPE_ROUTE EXCEPTION;
  PRAGMA EXCEPTION_INIT(NO_TAPE_ROUTE, -20100);
  varErrorMsg VARCHAR2(2048);
BEGIN
  BEGIN
    -- loop over the existing segments
    FOR varSeg IN (SELECT s_fileId as fileId, 0 as lastModTime, copyNo, segSize, 0 as comprSize,
                          Cns_seg_metadata.vid, fseq, blockId, checksum_name, nvl(checksum, 0) as checksum,
                          Cns_seg_metadata.s_status as segStatus, Vmgr_tape_status_view.status as tapeStatus
                     FROM Cns_seg_metadata@remotens, Vmgr_tape_status_view@remotens
                    WHERE Cns_seg_metadata.s_fileid = inFileId
                      AND Vmgr_tape_status_view.VID = Cns_seg_metadata.VID
                    ORDER BY copyno, fsec) LOOP
      varFoundSeg := TRUE;
      -- Is the segment valid
      IF varSeg.segStatus = '-' THEN
        -- Is the segment on a valid tape from recall point of view ?
        IF BITAND(varSeg.tapeStatus, tconst.TAPE_DISABLED) = 0 AND
           BITAND(varSeg.tapeStatus, tconst.TAPE_EXPORTED) = 0 AND
           BITAND(varSeg.tapeStatus, tconst.TAPE_ARCHIVED) = 0 THEN
          -- remember the copy number and tape
          varAllValidCopyNbs.EXTEND;
          varAllValidCopyNbs(varI) := varSeg.copyno;
          varAllValidVIDs.EXTEND;
          varAllValidVIDs(varI) := varSeg.vid;
          varI := varI + 1;
          -- create recallJob
          INSERT INTO RecallJob (id, castorFile, copyNb, recallGroup, svcClass, euid, egid,
                                 vid, fseq, status, fileSize, creationTime, blockId, fileTransactionId)
          VALUES (ids_seq.nextval, inCfId, varSeg.copyno, inRecallGroupId, inSvcClassId,
                  inEuid, inEgid, varSeg.vid, varSeg.fseq, tconst.RECALLJOB_PENDING, inFileSize, inRequestTime,
                  varSeg.blockId, NULL);
          varNbExistingSegments := varNbExistingSegments + 1;
          -- log "created new RecallJob"
          logToDLF(NULL, dlf.LVL_SYSTEM, dlf.RECALL_CREATING_RECALLJOB, inFileId, inNsHost, 'stagerd',
                   inLogParams || ' copyNb=' || TO_CHAR(varSeg.copyno) || ' TPVID=' || varSeg.vid ||
                   ' fseq=' || TO_CHAR(varSeg.fseq || ' FileSize=' || TO_CHAR(inFileSize)));
        ELSE
          -- Should the segment be counted in the count of existing segments ?
          -- In other terms, should we recreate a segment for replacing this one ?
          -- Yes if the segment in on an EXPORTED tape.
          IF BITAND(varSeg.tapeStatus, tconst.TAPE_EXPORTED) = 0 THEN
            -- invalid tape found with segments that are counting for the total count.
            -- "createRecallCandidate: found segment on unusable tape"
            logToDLF(NULL, dlf.LVL_SYSTEM, dlf.RECALL_UNUSABLE_TAPE, inFileId, inNsHost, 'stagerd',
                     inLogParams || ' segStatus=OK tapeStatus=' || tapeStatusToString(varSeg.tapeStatus) ||
                     ' recreatingSegment=No');
            varNbExistingSegments := varNbExistingSegments + 1;
          ELSE
            -- invalid tape found with segments that will be completely ignored.
            -- "createRecallCandidate: found segment on unusable tape"
            logToDLF(NULL, dlf.LVL_DEBUG, dlf.RECALL_UNUSABLE_TAPE, inFileId, inNsHost, 'stagerd',
                     inLogParams || ' segStatus=OK tapeStatus=' || tapeStatusToString(varSeg.tapeStatus) ||
                     ' recreatingSegment=Yes');
          END IF;
        END IF;
      ELSE
        -- invalid segment tape found. Log it.
        -- "createRecallCandidate: found unusable segment"
        logToDLF(NULL, dlf.LVL_NOTICE, dlf.RECALL_INVALID_SEGMENT, inFileId, inNsHost, 'stagerd',
                 inLogParams || ' segStatus=' ||
                 CASE varSeg.segStatus WHEN '-' THEN 'OK'
                                       WHEN 'D' THEN 'DISABLED'
                                       ELSE 'UNKNOWN:' || varSeg.segStatus END);
      END IF;
    END LOOP;
  EXCEPTION WHEN OTHERS THEN
    -- log "error when retrieving segments from namespace"
    logToDLF(NULL, dlf.LVL_ERROR, dlf.RECALL_UNKNOWN_NS_ERROR, inFileId, inNsHost, 'stagerd',
             inLogParams || ' ErrorMessage=' || SQLERRM);
    RETURN serrno.SEINTERNAL;
  END;
  -- If we did not find any valid segment to recall, log a critical error as the file is probably lost
  IF NOT varFoundSeg THEN
    -- log "createRecallCandidate: no segment found for this file. File is probably lost"
    logToDLF(NULL, dlf.LVL_CRIT, dlf.RECALL_NO_SEG_FOUND_AT_ALL, inFileId, inNsHost, 'stagerd', inLogParams);
    RETURN serrno.ESTNOSEGFOUND;
  END IF;
  -- If we found no valid segment (but some disabled ones), log a warning
  IF varAllValidCopyNbs.COUNT = 0 THEN
    -- log "createRecallCandidate: no valid segment to recall found"
    logToDLF(NULL, dlf.LVL_WARNING, dlf.RECALL_NO_SEG_FOUND, inFileId, inNsHost, 'stagerd', inLogParams);
    RETURN serrno.ESTNOSEGFOUND;
  END IF;
  BEGIN
    -- create missing segments if needed
    createMJForMissingSegments(inCfId, inFileSize, inFileClassId, varAllValidCopyNbs,
                               varAllValidVIDs, varNbExistingSegments, inFileId, inNsHost, inLogParams);
  EXCEPTION WHEN NO_TAPE_ROUTE THEN
    -- there's at least a missing segment and we cannot recreate it!
    -- log a "no route to tape defined for missing copy" error, but don't fail the recall
    logToDLF(NULL, dlf.LVL_ALERT, dlf.RECALL_MISSING_COPY_NO_ROUTE, inFileId, inNsHost, 'stagerd', inLogParams);
  WHEN OTHERS THEN
    -- some other error happened, log "unexpected error when creating missing copy", but don't fail the recall
    varErrorMsg := 'Oracle error caught : ' || SQLERRM;
    logToDLF(NULL, dlf.LVL_ERROR, dlf.RECALL_MISSING_COPY_ERROR, inFileId, inNsHost, 'stagerd',
      'errorCode=' || to_char(SQLCODE) ||' errorMessage="' || varErrorMsg
      ||'" stackTrace="' || dbms_utility.format_error_backtrace ||'"');
  END;
  RETURN 0;
END;
/

/* PL/SQL procedure implementing triggerRepackRecall
 * this triggers a recall in the repack context
 */
CREATE OR REPLACE PROCEDURE triggerRepackRecall
(inCfId IN INTEGER, inFileId IN INTEGER, inNsHost IN VARCHAR2, inBlock IN RAW,
 inFseq IN INTEGER, inCopynb IN INTEGER, inEuid IN INTEGER, inEgid IN INTEGER,
 inRecallGroupId IN INTEGER, inSvcClassId IN INTEGER, inVid IN VARCHAR2, inFileSize IN INTEGER,
 inFileClass IN INTEGER, inAllValidSegments IN VARCHAR2, inReqUUID IN VARCHAR2,
 inSubReqUUID IN VARCHAR2, inRecallGroupName IN VARCHAR2) AS
  varLogParam VARCHAR2(2048);
  varAllValidCopyNbs "numList" := "numList"();
  varAllValidVIDs strListTable := strListTable();
  varAllValidSegments strListTable;
  varFileClassId INTEGER;
BEGIN
  -- create recallJob for the given VID, copyNb, etc.
  INSERT INTO RecallJob (id, castorFile, copyNb, recallGroup, svcClass, euid, egid,
                         vid, fseq, status, fileSize, creationTime, blockId, fileTransactionId)
  VALUES (ids_seq.nextval, inCfId, inCopynb, inRecallGroupId, inSvcClassId,
          inEuid, inEgid, inVid, inFseq, tconst.RECALLJOB_PENDING, inFileSize, getTime(),
          inBlock, NULL);
  -- log "created new RecallJob"
  varLogParam := 'SUBREQID=' || inSubReqUUID || ' RecallGroup=' || inRecallGroupName;
  logToDLF(inReqUUID, dlf.LVL_SYSTEM, dlf.RECALL_CREATING_RECALLJOB, inFileId, inNsHost, 'stagerd',
           varLogParam || ' fileClass=' || TO_CHAR(inFileClass) || ' copyNb=' || TO_CHAR(inCopynb)
           || ' TPVID=' || inVid || ' fseq=' || TO_CHAR(inFseq) || ' FileSize=' || TO_CHAR(inFileSize));
  -- create missing segments if needed
  SELECT * BULK COLLECT INTO varAllValidSegments
    FROM TABLE(strTokenizer(inAllValidSegments));
  FOR i IN 1 .. varAllValidSegments.COUNT/2 LOOP
    varAllValidCopyNbs.EXTEND;
    varAllValidCopyNbs(i) := TO_NUMBER(varAllValidSegments(2*i-1));
    varAllValidVIDs.EXTEND;
    varAllValidVIDs(i) := varAllValidSegments(2*i);
  END LOOP;
  SELECT id INTO varFileClassId
    FROM FileClass WHERE classId = inFileClass;
  -- Note that the number given here for the number of existing segments is the total number of
  -- segment, without removing the ones on  EXPORTED tapes. This means that repack will not
  -- recreate new segments for files that have some copies on EXPORTED tapes.
  -- This is different from standard recalls that would recreate segments on EXPORTED tapes.
  createMJForMissingSegments(inCfId, inFileSize, varFileClassId, varAllValidCopyNbs,
                             varAllValidVIDs, varAllValidCopyNbs.COUNT, inFileId, inNsHost, varLogParam);
END;
/

CREATE OR REPLACE PROCEDURE processBulkAbortFileReqs
(origReqId IN INTEGER, fileIds IN "numList", nsHosts IN strListTable, reqType IN NUMBER) AS
  nbItems NUMBER;
  nbItemsDone NUMBER := 0;
  SrLocked EXCEPTION;
  PRAGMA EXCEPTION_INIT (SrLocked, -54);
  unused NUMBER;
  firstOne BOOLEAN := TRUE;
  commitWork BOOLEAN := FALSE;
BEGIN
  -- Gather the list of subrequests to abort
  IF fileIds.count() = 0 THEN
    -- handle the case of an empty request, meaning that all files should be aborted
    INSERT INTO ProcessBulkAbortFileReqsHelper (srId, cfId, fileId, nsHost, uuid) (
      SELECT /*+ INDEX_RS_ASC(Subrequest I_Subrequest_Request)*/
             SubRequest.id, CastorFile.id, CastorFile.fileId, CastorFile.nsHost, SubRequest.subreqId
        FROM SubRequest, CastorFile
       WHERE SubRequest.castorFile = CastorFile.id
         AND request = origReqId);
  ELSE
    -- handle the case of selective abort
    FOR i IN fileIds.FIRST .. fileIds.LAST LOOP
      DECLARE
        CONSTRAINT_VIOLATED EXCEPTION;
        PRAGMA EXCEPTION_INIT(CONSTRAINT_VIOLATED, -1);
      BEGIN
        -- note that we may insert several rows in one go in case the abort request contains
        -- several times the same file
        INSERT INTO processBulkAbortFileReqsHelper
          (SELECT /*+ INDEX_RS_ASC(Subrequest I_Subrequest_CastorFile)*/
                  DISTINCT SubRequest.id, CastorFile.id, fileIds(i), nsHosts(i), SubRequest.subreqId
             FROM SubRequest, CastorFile
            WHERE request = origReqId
              AND SubRequest.castorFile = CastorFile.id
              AND CastorFile.fileid = fileIds(i)
              AND CastorFile.nsHost = nsHosts(i));
        -- check that we found something
        IF SQL%ROWCOUNT = 0 THEN
          -- this fileid/nshost did not exist in the request, send an error back
          INSERT INTO ProcessBulkRequestHelper (fileId, nsHost, errorCode, errorMessage)
          VALUES (fileIds(i), nsHosts(i), serrno.ENOENT, 'No subRequest found for this fileId/nsHost');
        END IF;
      EXCEPTION WHEN CONSTRAINT_VIOLATED THEN
        -- the insertion in ProcessBulkRequestHelper triggered a violation of the
        -- primary key. This primary key being the subrequest id, this means that
        -- this subrequest is already in the list of the ones to be aborted. So
        -- nothing left to be done
        NULL;
      END;
    END LOOP;
  END IF;
  SELECT COUNT(*) INTO nbItems FROM processBulkAbortFileReqsHelper;
  -- handle aborts in bulk while avoiding deadlocks
  WHILE nbItems > 0 LOOP
    FOR sr IN (SELECT srId, cfId, fileId, nsHost, uuid FROM processBulkAbortFileReqsHelper) LOOP
      BEGIN
        IF firstOne THEN
          -- on the first item, we take a blocking lock as we are sure that we will not
          -- deadlock and we would like to process at least one item to not loop endlessly
          SELECT id INTO unused FROM CastorFile WHERE id = sr.cfId FOR UPDATE;
          firstOne := FALSE;
        ELSE
          -- on the other items, we go for a non blocking lock. If we get it, that's
          -- good and we process this extra subrequest within the same session. If
          -- we do not get the lock, then we close the session here and go for a new
          -- one. This will prevent dead locks while ensuring that a minimal number of
          -- commits is performed.
          SELECT id INTO unused FROM CastorFile WHERE id = sr.cfId FOR UPDATE NOWAIT;
        END IF;
        -- we got the lock on the Castorfile, we can handle the abort for this subrequest
        CASE reqType
          WHEN 1 THEN processAbortForGet(sr);
          WHEN 2 THEN processAbortForPut(sr);
        END CASE;
        DELETE FROM processBulkAbortFileReqsHelper WHERE srId = sr.srId;
        -- make the scheduler aware so that it can remove the transfer from the queues if needed
        INSERT INTO TransfersToAbort (uuid) VALUES (sr.uuid);
        nbItemsDone := nbItemsDone + 1;
      EXCEPTION WHEN SrLocked THEN
        commitWork := TRUE;
      END;
      -- commit anyway from time to time, to avoid too long redo logs
      IF commitWork OR nbItemsDone >= 1000 THEN
        -- exit the current loop and restart a new one, in order to commit without getting invalid ROWID errors
        EXIT;
      END IF;
    END LOOP;
    -- commit
    COMMIT;
    -- wake up the scheduler so that it can remove the transfer from the queues
    alertSignalNoLock('transfersToAbort');
    -- reset all counters
    nbItems := nbItems - nbItemsDone;
    nbItemsDone := 0;
    firstOne := TRUE;
    commitWork := FALSE;
  END LOOP;
END;
/

/* inserts file Requests in the stager DB.
 * This handles StageGetRequest, StagePrepareToGetRequest, StagePutRequest,
 * StagePrepareToPutRequest, StageUpdateRequest, StagePrepareToUpdateRequest,
 * StagePutDoneRequest, StagePrepareToUpdateRequest, and StageRmRequest requests.
 */ 	 
CREATE OR REPLACE PROCEDURE insertFileRequest
  (userTag IN VARCHAR2,
   machine IN VARCHAR2,
   euid IN INTEGER,
   egid IN INTEGER,
   pid IN INTEGER,
   mask IN INTEGER,
   userName IN VARCHAR2,
   flags IN INTEGER,
   svcClassName IN VARCHAR2,
   reqUUID IN VARCHAR2,
   inReqType IN INTEGER,
   clientIP IN INTEGER,
   clientPort IN INTEGER,
   clientVersion IN INTEGER,
   clientSecure IN INTEGER,
   freeStrParam IN VARCHAR2,
   freeNumParam IN NUMBER,
   srFileNames IN castor."strList",
   srProtocols IN castor."strList",
   srXsizes IN castor."cnumList",
   srFlags IN castor."cnumList",
   srModeBits IN castor."cnumList") AS
  svcClassId NUMBER;
  reqId NUMBER;
  subreqId NUMBER;
  clientId NUMBER;
  creationTime NUMBER;
  svcHandler VARCHAR2(100);
  protocols VARCHAR2(100);
BEGIN
  -- do prechecks and get the service class
  svcClassId := insertPreChecks(euid, egid, svcClassName, inReqType);
  -- get the list of valid protocols
  protocols := ' ' || getConfigOption('Stager', 'Protocols', 'rfio rfio3 gsiftp xroot') || ' ';
  -- get unique ids for the request and the client and get current time
  SELECT ids_seq.nextval INTO reqId FROM DUAL;
  SELECT ids_seq.nextval INTO clientId FROM DUAL;
  creationTime := getTime();
  -- insert the request itself
  CASE
    WHEN inReqType = 35 THEN -- StageGetRequest
      INSERT INTO StageGetRequest (flags, userName, euid, egid, mask, pid, machine, svcClassName, userTag, reqId, creationTime, lastModificationTime, id, svcClass, client)
      VALUES (flags,userName,euid,egid,mask,pid,machine,svcClassName,userTag,reqUUID,creationTime,creationTime,reqId,svcClassId,clientId);
    WHEN inReqType = 36 THEN -- StagePrepareToGetRequest
      INSERT INTO StagePrepareToGetRequest (flags, userName, euid, egid, mask, pid, machine, svcClassName, userTag, reqId, creationTime, lastModificationTime, id, svcClass, client)
      VALUES (flags,userName,euid,egid,mask,pid,machine,svcClassName,userTag,reqUUID,creationTime,creationTime,reqId,svcClassId,clientId);
    WHEN inReqType = 40 THEN -- StagePutRequest
      INSERT INTO StagePutRequest (flags, userName, euid, egid, mask, pid, machine, svcClassName, userTag, reqId, creationTime, lastModificationTime, id, svcClass, client)
      VALUES (flags,userName,euid,egid,mask,pid,machine,svcClassName,userTag,reqUUID,creationTime,creationTime,reqId,svcClassId,clientId);
    WHEN inReqType = 37 THEN -- StagePrepareToPutRequest
      INSERT INTO StagePrepareToPutRequest (flags, userName, euid, egid, mask, pid, machine, svcClassName, userTag, reqId, creationTime, lastModificationTime, id, svcClass, client)
      VALUES (flags,userName,euid,egid,mask,pid,machine,svcClassName,userTag,reqUUID,creationTime,creationTime,reqId,svcClassId,clientId);
    WHEN inReqType = 44 THEN -- StageUpdateRequest
      INSERT INTO StageUpdateRequest (flags, userName, euid, egid, mask, pid, machine, svcClassName, userTag, reqId, creationTime, lastModificationTime, id, svcClass, client)
      VALUES (flags,userName,euid,egid,mask,pid,machine,svcClassName,userTag,reqUUID,creationTime,creationTime,reqId,svcClassId,clientId);
    WHEN inReqType = 38 THEN -- StagePrepareToUpdateRequest
      INSERT INTO StagePrepareToUpdateRequest (flags, userName, euid, egid, mask, pid, machine, svcClassName, userTag, reqId, creationTime, lastModificationTime, id, svcClass, client)
      VALUES (flags,userName,euid,egid,mask,pid,machine,svcClassName,userTag,reqUUID,creationTime,creationTime,reqId,svcClassId,clientId);
    WHEN inReqType = 42 THEN -- StageRmRequest
      INSERT INTO StageRmRequest (flags, userName, euid, egid, mask, pid, machine, svcClassName, userTag, reqId, creationTime, lastModificationTime, id, svcClass, client)
      VALUES (flags,userName,euid,egid,mask,pid,machine,svcClassName,userTag,reqUUID,creationTime,creationTime,reqId,svcClassId,clientId);
    WHEN inReqType = 39 THEN -- StagePutDoneRequest
      INSERT INTO StagePutDoneRequest (flags, userName, euid, egid, mask, pid, machine, svcClassName, userTag, reqId, creationTime, lastModificationTime, parentUuid, id, svcClass, client)
      VALUES (flags,userName,euid,egid,mask,pid,machine,svcClassName,userTag,reqUUID,creationTime,creationTime,freeStrParam,reqId,svcClassId,clientId);
    WHEN inReqType = 95 THEN -- SetFileGCWeight
      INSERT INTO SetFileGCWeight (flags, userName, euid, egid, mask, pid, machine, svcClassName, userTag, reqId, creationTime, lastModificationTime, weight, id, svcClass, client)
      VALUES (flags,userName,euid,egid,mask,pid,machine,svcClassName,userTag,reqUUID,creationTime,creationTime,freeNumParam,reqId,svcClassId,clientId);
    ELSE
      raise_application_error(-20122, 'Unsupported request type in insertFileRequest : ' || TO_CHAR(inReqType));
  END CASE;
  -- insert the client information
  INSERT INTO Client (ipAddress, port, version, secure, id)
  VALUES (clientIP,clientPort,clientVersion,clientSecure,clientId);
  -- get the request's service handler
  SELECT svcHandler INTO svcHandler FROM Type2Obj WHERE type=inReqType;
  -- Loop on subrequests
  FOR i IN srFileNames.FIRST .. srFileNames.LAST LOOP
    -- check protocol validity for Get/Update/Put requests only, for other requests the protocol is irrelevant
    IF inReqType IN (35, 40, 44) AND INSTR(protocols, ' ' || srProtocols(i) || ' ') = 0 THEN
      raise_application_error(-20122, 'Unsupported protocol in insertFileRequest : ' || srProtocols(i));
    END IF;
    -- get unique ids for the subrequest
    SELECT ids_seq.nextval INTO subreqId FROM DUAL;
    -- insert the subrequest
    INSERT INTO SubRequest (retryCounter, fileName, protocol, xsize, priority, subreqId, flags, modeBits, creationTime, lastModificationTime, answered, errorCode, errorMessage, requestedFileSystems, svcHandler, id, diskcopy, castorFile, status, request, getNextStatus, reqType)
    VALUES (0, srFileNames(i), srProtocols(i), srXsizes(i), 0, NULL, srFlags(i), srModeBits(i), creationTime, creationTime, 0, 0, '', NULL, svcHandler, subreqId, NULL, NULL, dconst.SUBREQUEST_START, reqId, 0, inReqType);
  END LOOP;
  -- send one single alert to accelerate the processing of the request
  CASE
  WHEN inReqType = 35 OR   -- StageGetRequest
       inReqType = 40 OR   -- StagePutRequest
       inReqType = 44 THEN -- StageUpdateRequest
    alertSignalNoLock('wakeUpJobReqSvc');
  WHEN inReqType = 36 OR   -- StagePrepareToGetRequest
       inReqType = 37 OR   -- StagePrepareToPutRequest
       inReqType = 38 THEN -- StagePrepareToUpdateRequest
    alertSignalNoLock('wakeUpPrepReqSvc');
  WHEN inReqType = 42 OR   -- StageRmRequest
       inReqType = 39 OR   -- StagePutDoneRequest
       inReqType = 95 THEN -- SetFileGCWeight
    alertSignalNoLock('wakeUpStageReqSvc');
  END CASE;
END;
/

/* This procedure is used to check if the maxReplicaNb has been exceeded
 * for some CastorFiles. It checks all the files listed in TooManyReplicasHelper
 * This is called from a DB job and is fed by the tr_DiskCopy_Created trigger
 * on creation of new diskcopies
 */
CREATE OR REPLACE PROCEDURE checkNbReplicas AS
  varSvcClassId INTEGER;
  varCfId INTEGER;
  varMaxReplicaNb NUMBER;
  varNbFiles NUMBER;
  varDidSth BOOLEAN;
BEGIN
  -- Loop over the CastorFiles to be processed
  LOOP
    varCfId := NULL;
    DELETE FROM TooManyReplicasHelper
     WHERE ROWNUM < 2
    RETURNING svcClass, castorFile INTO varSvcClassId, varCfId;
    IF varCfId IS NULL THEN
      -- we can exit, we went though all files to be processed
      EXIT;
    END IF;
    BEGIN
      -- Lock the castorfile
      SELECT id INTO varCfId FROM CastorFile
       WHERE id = varCfId FOR UPDATE;
    EXCEPTION WHEN NO_DATA_FOUND THEN
      -- the file was dropped meanwhile, ignore and continue
      CONTINUE;
    END;
    -- Get the max replica number of the service class
    SELECT maxReplicaNb INTO varMaxReplicaNb
      FROM SvcClass WHERE id = varSvcClassId;
    -- Produce a list of diskcopies to invalidate should too many replicas be online.
    varDidSth := False;
    FOR b IN (SELECT id FROM (
                SELECT rownum ind, id FROM (
                  SELECT /*+ INDEX_RS_ASC (DiskCopy I_DiskCopy_Castorfile) */ DiskCopy.id
                    FROM DiskCopy, FileSystem, DiskPool2SvcClass, SvcClass,
                         DiskServer
                   WHERE DiskCopy.filesystem = FileSystem.id
                     AND FileSystem.diskpool = DiskPool2SvcClass.parent
                     AND FileSystem.diskserver = DiskServer.id
                     AND DiskPool2SvcClass.child = SvcClass.id
                     AND DiskCopy.castorfile = varCfId
                     AND DiskCopy.status = dconst.DISKCOPY_VALID
                     AND SvcClass.id = varSvcClassId
                   -- Select non-PRODUCTION hardware first
                   ORDER BY decode(FileSystem.status, 0,
                            decode(DiskServer.status, 0, 0, 1), 1) ASC,
                            DiskCopy.gcWeight DESC))
               WHERE ind > varMaxReplicaNb)
    LOOP
      -- Sanity check, make sure that the last copy is never dropped!
      SELECT /*+ INDEX_RS_ASC(DiskCopy I_DiskCopy_CastorFile) */ count(*) INTO varNbFiles
        FROM DiskCopy, FileSystem, DiskPool2SvcClass, SvcClass, DiskServer
       WHERE DiskCopy.filesystem = FileSystem.id
         AND FileSystem.diskpool = DiskPool2SvcClass.parent
         AND FileSystem.diskserver = DiskServer.id
         AND DiskPool2SvcClass.child = SvcClass.id
         AND DiskCopy.castorfile = varCfId
         AND DiskCopy.status = dconst.DISKCOPY_VALID
         AND SvcClass.id = varSvcClassId;
      IF varNbFiles = 1 THEN
        EXIT;  -- Last file, so exit the loop
      END IF;
      -- Invalidate the diskcopy
      UPDATE DiskCopy
         SET status = dconst.DISKCOPY_INVALID,
             gcType = dconst.GCTYPE_TOOMANYREPLICAS
       WHERE id = b.id;
      varDidSth := True;
      -- update importance of remaining diskcopies
      UPDATE DiskCopy SET importance = importance + 1
       WHERE castorFile = varCfId
         AND status = dconst.DISKCOPY_VALID;
    END LOOP;
    IF varDidSth THEN COMMIT; END IF;
  END LOOP;
  -- commit the deletions in case no modification was done that commited them before
  COMMIT;
END;
/

/* revalidate all code */
BEGIN
  recompileAll();
END;
/

/* Flag the schema upgrade as COMPLETE */
/***************************************/
UPDATE UpgradeLog SET endDate = systimestamp, state = 'COMPLETE'
 WHERE release = '2_1_14_15_1';
COMMIT;
