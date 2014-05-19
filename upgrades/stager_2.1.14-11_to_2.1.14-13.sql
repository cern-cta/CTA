/******************************************************************************
 *                 stager_2.1.14-11_to_2.1.14-13.sql
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
 * This script upgrades a CASTOR v2.1.14-11 STAGER database to v2.1.14-13
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
     AND release = '2_1_14_13'
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
     AND release LIKE '2_1_14_11%';
EXCEPTION WHEN NO_DATA_FOUND THEN
  -- Error, we cannot apply this script
  raise_application_error(-20000, 'PL/SQL release mismatch. Please run previous upgrade scripts for the STAGER before this one.');
END;
/

INSERT INTO UpgradeLog (schemaVersion, release, type)
VALUES ('2_1_14_2', '2_1_14_13', 'TRANSPARENT');
COMMIT;

/* Job management */
BEGIN
  FOR a IN (SELECT * FROM user_scheduler_jobs)
  LOOP
    -- Stop any running jobs
    IF a.state = 'RUNNING' THEN
      dbms_scheduler.stop_job(a.job_name, force=>TRUE);
    END IF;
    -- Schedule the start date of the job to 15 minutes from now. This
    -- basically pauses the job for 15 minutes so that the upgrade can
    -- go through as quickly as possible.
    dbms_scheduler.set_attribute(a.job_name, 'START_DATE', SYSDATE + 15/1440);
  END LOOP;
END;
/

-- new config parameter
INSERT INTO CastorConfig
  VALUES ('Migration', 'NbMigCandConsidered', '10000', 'The number of migration jobs considered in time order by each selection of the best files to migrate');

CREATE INDEX I_MigrationJob_TPStatusCT ON MigrationJob(tapePool, status, creationTime);

DROP VIEW LateMigrationsView;
CREATE VIEW LateMigrationsView AS
  SELECT /*+ LEADING(CF DC MJ CnsFile) INDEX_RS_ASC(CF I_CastorFile_TapeStatus) INDEX_RS_ASC(DC I_DiskCopy_CastorFile) INDEX_RS_ASC(MJ I_MigrationJob_CastorFile) */
         CF.fileId, CF.lastKnownFileName as filePath, DC.creationTime as dcCreationTime,
         decode(MJ.creationTime, NULL, -1, getTime() - MJ.creationTime) as mjElapsedTime, nvl(MJ.status, -1) as mjStatus
    FROM CastorFile CF, DiskCopy DC, MigrationJob MJ, cns_file_metadata@remotens CnsFile
   WHERE CF.fileId = CnsFile.fileId
     AND DC.castorFile = CF.id
     AND MJ.castorFile(+) = CF.id
     AND CF.tapeStatus = 0  -- CASTORFILE_NOTONTAPE
     AND DC.status = 0  -- DISKCOPY_VALID
     AND CF.fileSize > 0
     AND DC.creationTime < getTime() - 86400
  ORDER BY DC.creationTime DESC;


/* Code revalidation */
/*********************/

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
  varAllCopyNbs "numList" := "numList"();
  varAllVIDs strListTable := strListTable();
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
          varAllCopyNbs.EXTEND;
          varAllCopyNbs(varI) := varSeg.copyno;
          varAllVIDs.EXTEND;
          varAllVIDs(varI) := varSeg.vid;
          varI := varI + 1;
          -- create recallJob
          INSERT INTO RecallJob (id, castorFile, copyNb, recallGroup, svcClass, euid, egid,
                                 vid, fseq, status, fileSize, creationTime, blockId, fileTransactionId)
          VALUES (ids_seq.nextval, inCfId, varSeg.copyno, inRecallGroupId, inSvcClassId,
                  inEuid, inEgid, varSeg.vid, varSeg.fseq, tconst.RECALLJOB_PENDING, inFileSize, inRequestTime,
                  varSeg.blockId, NULL);
          -- log "created new RecallJob"
          logToDLF(NULL, dlf.LVL_SYSTEM, dlf.RECALL_CREATING_RECALLJOB, inFileId, inNsHost, 'stagerd',
                   inLogParams || ' copyNb=' || TO_CHAR(varSeg.copyno) || ' TPVID=' || varSeg.vid ||
                   ' fseq=' || TO_CHAR(varSeg.fseq || ' FileSize=' || TO_CHAR(inFileSize)));
        ELSE
          -- invalid tape found. Log it.
          -- "createRecallCandidate: found segment on unusable tape"
          logToDLF(NULL, dlf.LVL_DEBUG, dlf.RECALL_UNUSABLE_TAPE, inFileId, inNsHost, 'stagerd',
                   inLogParams || ' segStatus=OK tapeStatus=' || tapeStatusToString(varSeg.tapeStatus));
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
  IF varAllCopyNbs.COUNT = 0 THEN
    -- log "createRecallCandidate: no valid segment to recall found"
    logToDLF(NULL, dlf.LVL_WARNING, dlf.RECALL_NO_SEG_FOUND, inFileId, inNsHost, 'stagerd', inLogParams);
    RETURN serrno.ESTNOSEGFOUND;
  END IF;
  BEGIN
    -- create missing segments if needed
    createMJForMissingSegments(inCfId, inFileSize, inFileClassId, varAllCopyNbs,
                               varAllVIDs, inFileId, inNsHost, inLogParams);
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

/* prechecks common to all insert*Request methods*/
CREATE OR REPLACE FUNCTION insertPreChecks
  (euid IN INTEGER,
   egid IN INTEGER,
   svcClassName IN VARCHAR2,
   reqType IN INTEGER) RETURN NUMBER AS
  reqName VARCHAR2(100);
BEGIN
  -- Check permissions
  IF 0 != checkPermission(svcClassName, euid, egid, reqType) THEN
    -- permission denied
    SELECT object INTO reqName FROM Type2Obj WHERE type = reqType;
    raise_application_error(-20121, 'Insufficient privileges for user ' || euid ||','|| egid
        ||' performing a '|| reqName ||' request on svcClass '''|| svcClassName ||'''');
  END IF;  
  -- check the validity of the given service class and return its internal id
  RETURN checkForValidSvcClass(svcClassName, 1, 1);
END;
/

/* Get the diskcopys associated with the reference number */
CREATE OR REPLACE FUNCTION getDCs(ref number) RETURN castorDebug.DiskCopyDebug PIPELINED AS
BEGIN
  FOR d IN (SELECT DiskCopy.id, getObjStatusName('DiskCopy', 'status', DiskCopy.status) AS status,
                   getTimeString(DiskCopy.creationtime) AS creationtime,
                   DiskPool.name AS diskpool,
                   DiskServer.name || ':' || FileSystem.mountPoint || DiskCopy.path AS location,
                   decode(DiskServer.hwOnline, 0, 'N',
                     decode(DiskServer.status, 2, 'N',
                       decode(FileSystem.status, 2, 'N', 'Y'))) AS available,
                   DiskCopy.diskCopySize AS diskcopysize,
                   CastorFile.fileSize AS castorfilesize,
                   trunc(DiskCopy.gcWeight, 2) AS gcweight
              FROM DiskCopy, FileSystem, DiskServer, DiskPool, CastorFile
             WHERE DiskCopy.fileSystem = FileSystem.id(+)
               AND FileSystem.diskServer = diskServer.id(+)
               AND DiskPool.id(+) = fileSystem.diskPool
               AND DiskCopy.castorFile = getCF(ref)
               AND DiskCopy.castorFile = CastorFile.id) LOOP
     PIPE ROW(d);
  END LOOP;
END;
/

/* PL/SQL method implementing bestFileSystemForRecall */
CREATE OR REPLACE PROCEDURE bestFileSystemForRecall(inCfId IN INTEGER, outFilePath OUT VARCHAR2) AS
  varCfId INTEGER;
  varFileSystemId NUMBER := 0;
  nb NUMBER;
BEGIN
  -- try and select a good FileSystem for this recall
  FOR f IN (SELECT /*+ INDEX_RS_ASC(RecallJob I_RecallJob_Castorfile_VID) */
                   DiskServer.name ||':'|| FileSystem.mountPoint AS remotePath, FileSystem.id,
                   FileSystem.diskserver, CastorFile.fileSize, CastorFile.fileId, CastorFile.nsHost
              FROM DiskServer, FileSystem, DiskPool2SvcClass, CastorFile, RecallJob
             WHERE CastorFile.id = inCfId
               AND RecallJob.castorFile = inCfId
               AND RecallJob.svcClass = DiskPool2SvcClass.child
               AND FileSystem.diskPool = DiskPool2SvcClass.parent
               -- a priori, we want to have enough free space. However, if we don't, we accept to start writing
               -- if we have a minimum of 30GB free and count on gerbage collection to liberate space while writing
               -- We still check that the file fit on the disk, and actually keep a 30% margin so that very recent
               -- files can be kept
               AND (FileSystem.free - FileSystem.minAllowedFreeSpace * FileSystem.totalSize > CastorFile.fileSize
                 OR (FileSystem.free - FileSystem.minAllowedFreeSpace * FileSystem.totalSize > 30000000000
                 AND FileSystem.totalSize * 0.7 > CastorFile.fileSize))
               AND FileSystem.status = dconst.FILESYSTEM_PRODUCTION
               AND DiskServer.id = FileSystem.diskServer
               AND DiskServer.status = dconst.DISKSERVER_PRODUCTION
               AND DiskServer.hwOnline = 1
          ORDER BY -- order by filesystem load first: this works if the feedback loop is fast enough, that is
                   -- the transfer of the selected files in bulk does not take more than a couple of minutes
                   FileSystem.nbMigratorStreams + FileSystem.nbRecallerStreams ASC,
                   -- then use randomness for tie break
                   DBMS_Random.value)
  LOOP
    varFileSystemId := f.id;
    buildPathFromFileId(f.fileId, f.nsHost, ids_seq.nextval, outFilePath);
    outFilePath := f.remotePath || outFilePath;
    -- Check that we don't already have a copy of this file on this filesystem.
    -- This will never happen in normal operations but may be the case if a filesystem
    -- was disabled and did come back while the tape recall was waiting.
    -- Even if we optimize by cancelling remaining unneeded tape recalls when a
    -- fileSystem comes back, the ones running at the time of the come back will have
    -- the problem.
    SELECT /*+ INDEX_RS_ASC(DiskCopy I_DiskCopy_CastorFile) */ count(*) INTO nb
      FROM DiskCopy
     WHERE fileSystem = f.id
       AND castorfile = inCfid
       AND status = dconst.DISKCOPY_VALID;
    IF nb != 0 THEN
      raise_application_error(-20115, 'Recaller could not find a FileSystem in production in the requested SvcClass and without copies of this file');
    END IF;
    RETURN;
  END LOOP;
  IF varFileSystemId = 0 THEN
    raise_application_error(-20115, 'No suitable filesystem found for this recalled file');
  END IF;
END;
/

/* PL/SQL method implementing createEmptyFile */
CREATE OR REPLACE PROCEDURE createEmptyFile
(cfId IN NUMBER, fileId IN NUMBER, nsHost IN VARCHAR2,
 srId IN INTEGER, schedule IN INTEGER) AS
  dcPath VARCHAR2(2048);
  gcw NUMBER;
  gcwProc VARCHAR(2048);
  fsId NUMBER;
  dcId NUMBER;
  svcClassId NUMBER;
  ouid INTEGER;
  ogid INTEGER;
  fsPath VARCHAR2(2048);
BEGIN
  -- update filesize overriding any previous value
  UPDATE CastorFile SET fileSize = 0 WHERE id = cfId;
  -- get an id for our new DiskCopy
  dcId := ids_seq.nextval();
  -- compute the DiskCopy Path
  buildPathFromFileId(fileId, nsHost, dcId, dcPath);
  -- find a fileSystem for this empty file
  SELECT id, svcClass, euid, egid, name || ':' || mountpoint
    INTO fsId, svcClassId, ouid, ogid, fsPath
    FROM (SELECT /*+ INDEX(Subrequest PK_Subrequest_Id)*/
                 FileSystem.id, Request.svcClass, Request.euid, Request.egid, DiskServer.name, FileSystem.mountpoint
            FROM DiskServer, FileSystem, DiskPool2SvcClass,
                 (SELECT /*+ INDEX(StageGetRequest PK_StageGetRequest_Id) */
                         id, svcClass, euid, egid from StageGetRequest UNION ALL
                  SELECT /*+ INDEX(StagePrepareToGetRequest PK_StagePrepareToGetRequest_Id) */
                         id, svcClass, euid, egid from StagePrepareToGetRequest UNION ALL
                  SELECT /*+ INDEX(StageUpdateRequest PK_StageUpdateRequest_Id) */
                         id, svcClass, euid, egid from StageUpdateRequest UNION ALL
                  SELECT /*+ INDEX(StagePrepareToUpdateRequest PK_StagePrepareToUpdateRequ_Id) */
                         id, svcClass, euid, egid from StagePrepareToUpdateRequest) Request,
                  SubRequest
           WHERE SubRequest.id = srId
             AND Request.id = SubRequest.request
             AND Request.svcclass = DiskPool2SvcClass.child
             AND FileSystem.diskpool = DiskPool2SvcClass.parent
             AND FileSystem.status = dconst.FILESYSTEM_PRODUCTION
             AND DiskServer.id = FileSystem.diskServer
             AND DiskServer.status = dconst.DISKSERVER_PRODUCTION
             AND DiskServer.hwOnline = 1
        ORDER BY -- use randomness to scatter filesystem usage
                 DBMS_Random.value)
   WHERE ROWNUM < 2;
  -- compute it's gcWeight
  gcwProc := castorGC.getRecallWeight(svcClassId);
  EXECUTE IMMEDIATE 'BEGIN :newGcw := ' || gcwProc || '(0); END;'
    USING OUT gcw;
  -- then create the DiskCopy
  INSERT INTO DiskCopy
    (path, id, filesystem, castorfile, status, importance,
     creationTime, lastAccessTime, gcWeight, diskCopySize, nbCopyAccesses, owneruid, ownergid)
  VALUES (dcPath, dcId, fsId, cfId, dconst.DISKCOPY_VALID, -1,
          getTime(), getTime(), GCw, 0, 0, ouid, ogid);
  -- link to the SubRequest and schedule an access if requested
  IF schedule = 0 THEN
    UPDATE /*+ INDEX(Subrequest PK_Subrequest_Id)*/ SubRequest SET diskCopy = dcId WHERE id = srId;
  ELSE
    UPDATE /*+ INDEX(Subrequest PK_Subrequest_Id)*/ SubRequest
       SET diskCopy = dcId,
           requestedFileSystems = fsPath,
           xsize = 0, status = dconst.SUBREQUEST_READYFORSCHED,
           getNextStatus = dconst.GETNEXTSTATUS_FILESTAGED
     WHERE id = srId;
  END IF;
EXCEPTION WHEN NO_DATA_FOUND THEN
  raise_application_error(-20115, 'No suitable filesystem found for this empty file');
END;
/

/* Search and delete old diskCopies in bad states */
CREATE OR REPLACE PROCEDURE deleteFailedDiskCopies(timeOut IN NUMBER) AS
  dcIds "numList";
  cfIds "numList";
BEGIN
  LOOP
    -- select INVALID diskcopies without filesystem (they can exist after a
    -- stageRm that came before the diskcopy had been created on disk) and ALL FAILED
    -- ones (coming from failed recalls or failed removals from the GC daemon).
    -- Note that we don't select INVALID diskcopies from recreation of files
    -- because they are taken by the standard GC as they physically exist on disk.
    -- go only for 1000 at a time and retry if the limit was reached
    SELECT id
      BULK COLLECT INTO dcIds
      FROM DiskCopy
     WHERE (status = 4 OR (status = 7 AND fileSystem = 0))
       AND creationTime < getTime() - timeOut
       AND ROWNUM <= 1000;
    SELECT /*+ INDEX(DC PK_DiskCopy_ID) */ UNIQUE castorFile
      BULK COLLECT INTO cfIds
      FROM DiskCopy DC
     WHERE id IN (SELECT /*+ CARDINALITY(ids 5) */ * FROM TABLE(dcIds) ids);
    -- drop the DiskCopies - not in bulk because of the constraint violation check
    FOR i IN 1 .. dcIds.COUNT LOOP
      DECLARE
        CONSTRAINT_VIOLATED EXCEPTION;
        PRAGMA EXCEPTION_INIT(CONSTRAINT_VIOLATED, -1);
      BEGIN
        DELETE FROM DiskCopy WHERE id = dcIds(i);
      EXCEPTION WHEN CONSTRAINT_VIOLATED THEN
        IF sqlerrm LIKE '%constraint (CASTOR_STAGER.FK_DRAININGERRORS_DC) violated%' OR
           sqlerrm LIKE '%constraint (CASTOR_STAGER.FK_DISK2DISKCOPYJOB_SRCDCID) violated%' THEN
          -- Ignore the deletion, this diskcopy was either implied in a draining action and
          -- the draining error is still around or it is the source of another d2d copy that
          -- is not over
          NULL;
        ELSE
          -- Any other constraint violation is an error
          RAISE;
        END IF;
      END;
    END LOOP;
    COMMIT;
    -- maybe delete the CastorFiles if nothing is left for them
    FOR i IN 1 .. cfIds.COUNT LOOP
      deleteCastorFile(cfIds(i));
    END LOOP;
    COMMIT;
    -- exit if we did less than 1000
    IF dcIds.COUNT < 1000 THEN EXIT; END IF;
  END LOOP;
END;
/

/* PL/SQL method implementing disk2DiskCopyEnded
 * Note that inDestDsName, inDestPath and inReplicaFileSize are not used when inErrorMessage is not NULL
 * inErrorCode is used in case of error to decide whether to retry and also to invalidate
 * the source diskCopy if the error is an ENOENT
 */
CREATE OR REPLACE PROCEDURE disk2DiskCopyEnded
(inTransferId IN VARCHAR2, inDestDsName IN VARCHAR2, inDestPath IN VARCHAR2,
 inReplicaFileSize IN INTEGER, inErrorCode IN INTEGER, inErrorMessage IN VARCHAR2) AS
  varCfId INTEGER;
  varUid INTEGER := -1;
  varGid INTEGER := -1;
  varDestDcId INTEGER;
  varSrcDcId INTEGER;
  varDropSource INTEGER;
  varDestSvcClass INTEGER;
  varRepType INTEGER;
  varRetryCounter INTEGER;
  varFileId INTEGER;
  varNsHost VARCHAR2(2048);
  varFileSize INTEGER;
  varDestPath VARCHAR2(2048);
  varDestFsId INTEGER;
  varDcGcWeight NUMBER := 0;
  varDcImportance NUMBER := 0;
  varNewDcStatus INTEGER := dconst.DISKCOPY_VALID;
  varLogMsg VARCHAR2(2048);
  varComment VARCHAR2(2048);
  varDrainingJob VARCHAR2(2048);
BEGIN
  BEGIN
    IF inDestPath IS NOT NULL THEN
      -- Parse destination path
      parsePath(inDestDsName ||':'|| inDestPath, varDestFsId, varDestPath, varDestDcId, varFileId, varNsHost);
    -- ELSE we are called because of an error at start: try to gather information
    -- from the Disk2DiskCopyJob entry and fail accordingly.
    END IF;
    -- Get data from the Disk2DiskCopyJob
    SELECT castorFile, ouid, ogid, destDcId, srcDcId, destSvcClass, replicationType,
           dropSource, retryCounter, drainingJob
      INTO varCfId, varUid, varGid, varDestDcId, varSrcDcId, varDestSvcClass, varRepType,
           varDropSource, varRetryCounter, varDrainingJob
      FROM Disk2DiskCopyJob
     WHERE transferId = inTransferId;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    -- The job was probably canceled: so our brand new copy
    -- has to be created as invalid to trigger GC, and linked
    -- to the (hopefully existing) correct CastorFile.
    varNewDcStatus := dconst.DISKCOPY_INVALID;
    varLogMsg := dlf.D2D_D2DDONE_CANCEL;
    BEGIN
      SELECT id INTO varCfId
        FROM CastorFile
       WHERE fileId = varFileId;
    EXCEPTION WHEN NO_DATA_FOUND THEN
      -- Here we also lost the CastorFile: this could happen
      -- if the GC ran meanwhile. Fail and leave dark data behind,
      -- the GC will eventually catch up. A full solution would be
      -- to gather here all missing information to correctly
      -- recreate the CastorFile entry, but this is too complex
      -- for what we would gain.
      logToDLF(NULL, dlf.LVL_NOTICE, dlf.D2D_D2DDONE_CANCEL, varFileId, varNsHost, 'transfermanagerd',
               'transferId=' || inTransferId || ' errorMessage="CastorFile disappeared, giving up"');
      RETURN;
    END;
  END;
  varLogMsg := CASE WHEN inErrorMessage IS NULL THEN dlf.D2D_D2DDONE_OK ELSE dlf.D2D_D2DFAILED END;
  -- lock the castor file (and get logging info)
  SELECT fileid, nsHost, fileSize INTO varFileId, varNsHost, varFileSize
    FROM CastorFile
   WHERE id = varCfId
     FOR UPDATE;
  -- check the filesize
  IF inReplicaFileSize != varFileSize THEN
    -- replication went wrong !
    IF varLogMsg = dlf.D2D_D2DDONE_OK THEN
      varLogMsg := dlf.D2D_D2DDONE_BADSIZE;
      varNewDcStatus := dconst.DISKCOPY_INVALID;
    END IF;
  END IF;
  -- Log success or failure of the replication
  varComment := 'transferId="' || inTransferId ||
         '" destSvcClass=' || getSvcClassName(varDestSvcClass) ||
         ' dstDcId=' || TO_CHAR(varDestDcId) || ' destPath="' || inDestPath ||
         '" euid=' || TO_CHAR(varUid) || ' egid=' || TO_CHAR(varGid) ||
         ' fileSize=' || TO_CHAR(varFileSize);
  IF inErrorMessage IS NOT NULL THEN
    varComment := varComment || ' replicaFileSize=' || TO_CHAR(inReplicaFileSize) ||
                  ' errorMessage=' || inErrorMessage;
  END IF;
  logToDLF(NULL, dlf.LVL_SYSTEM, varLogMsg, varFileId, varNsHost, 'transfermanagerd', varComment);
  -- if success, create new DiskCopy, restart waiting requests, cleanup and handle replicate on close
  IF inErrorMessage IS NULL THEN
    -- compute GcWeight and importance of the new copy
    IF varNewDcStatus = dconst.DISKCOPY_VALID THEN
      DECLARE
        varGcwProc VARCHAR2(2048);
      BEGIN
        varGcwProc := castorGC.getCopyWeight(varDestSvcClass);
        EXECUTE IMMEDIATE
          'BEGIN :newGcw := ' || varGcwProc || '(:size); END;'
          USING OUT varDcGcWeight, IN varFileSize;
        SELECT /*+ INDEX_RS_ASC (DiskCopy I_DiskCopy_CastorFile) */
               COUNT(*)+1 INTO varDCImportance FROM DiskCopy
         WHERE castorFile=varCfId AND status = dconst.DISKCOPY_VALID;
      END;
    END IF;
    -- create the new DiskCopy
    INSERT INTO DiskCopy (path, gcWeight, creationTime, lastAccessTime, diskCopySize, nbCopyAccesses,
                          owneruid, ownergid, id, gcType, fileSystem, castorFile, status, importance)
    VALUES (varDestPath, varDcGcWeight, getTime(), getTime(), varFileSize, 0, varUid, varGid, varDestDcId,
            CASE varNewDcStatus WHEN dconst.DISKCOPY_INVALID THEN dconst.GCTYPE_FAILEDD2D ELSE NULL END,
            varDestFsId, varCfId, varNewDcStatus, varDCImportance);
    -- Wake up waiting subrequests
    UPDATE SubRequest
       SET status = dconst.SUBREQUEST_RESTART,
           getNextStatus = CASE WHEN inErrorMessage IS NULL THEN dconst.GETNEXTSTATUS_FILESTAGED ELSE getNextStatus END,
           lastModificationTime = getTime()
           -- XXX due to bug #103715 requests for disk-to-disk copies actually stay in WAITTAPERECALL,
           -- XXX so we have to restart also those. This will go with the refactoring of the stager.
     WHERE status IN (dconst.SUBREQUEST_WAITSUBREQ, dconst.SUBREQUEST_WAITTAPERECALL)
       AND castorfile = varCfId;
    alertSignalNoLock('wakeUpJobReqSvc');
    -- delete the disk2diskCopyJob
    DELETE FROM Disk2DiskCopyjob WHERE transferId = inTransferId;
    -- In case of valid new copy
    IF varNewDcStatus = dconst.DISKCOPY_VALID THEN
      IF varDropSource = 1 THEN
        -- drop source if requested
        UPDATE DiskCopy SET status = dconst.DISKCOPY_INVALID WHERE id = varSrcDcId;
      ELSE
        -- update importance of other DiskCopies if it's an additional one
        UPDATE DiskCopy SET importance = varDCImportance WHERE castorFile = varCfId;
      END IF;
      -- Trigger the creation of additional copies of the file, if any
      replicateOnClose(varCfId, varUid, varGid);
    END IF;
    -- In case of draining, update DrainingJob
    IF varDrainingJob IS NOT NULL THEN
      updateDrainingJobOnD2dEnd(varDrainingJob, varFileSize, False);
    END IF;
  ELSE
    -- failure
    DECLARE
      varMaxNbD2dRetries INTEGER := TO_NUMBER(getConfigOption('D2dCopy', 'MaxNbRetries', 2));
    BEGIN
      -- shall we try again ?
      -- we should not when the job was deliberately killed, neither when we reach the maximum
      -- number of attempts
      IF varRetryCounter < varMaxNbD2dRetries AND inErrorCode != serrno.ESTKILLED THEN
        -- yes, so let's restart the Disk2DiskCopyJob
        UPDATE Disk2DiskCopyJob
           SET status = dconst.DISK2DISKCOPYJOB_PENDING,
               retryCounter = varRetryCounter + 1
         WHERE transferId = inTransferId;
        logToDLF(NULL, dlf.LVL_SYSTEM, dlf.D2D_D2DDONE_RETRIED, varFileId, varNsHost, 'transfermanagerd', varComment ||
                 ' RetryNb=' || TO_CHAR(varRetryCounter+1) || ' maxNbRetries=' || TO_CHAR(varMaxNbD2dRetries));
      ELSE
        -- no retry, let's delete the disk to disk job copy
        BEGIN
          DELETE FROM Disk2DiskCopyjob WHERE transferId = inTransferId;
          -- and remember the error in case of draining
          IF varDrainingJob IS NOT NULL THEN
            INSERT INTO DrainingErrors (drainingJob, errorMsg, fileId, nsHost, diskCopy, timeStamp)
            VALUES (varDrainingJob, inErrorMessage, varFileId, varNsHost, varSrcDcId, getTime());
          END IF;
        EXCEPTION WHEN NO_DATA_FOUND THEN
          -- the Disk2DiskCopyjob was already dropped (e.g. because of an interrupted draining)
          -- in such a case, forget about the error
          NULL;
        END;
        logToDLF(NULL, dlf.LVL_NOTICE, dlf.D2D_D2DDONE_NORETRY, varFileId, varNsHost, 'transfermanagerd', varComment ||
                 ' maxNbRetries=' || TO_CHAR(varMaxNbD2dRetries));
        -- Fail waiting subrequests
        UPDATE SubRequest
           SET status = dconst.SUBREQUEST_FAILED,
               lastModificationTime = getTime(),
               errorCode = serrno.SEINTERNAL,
               errorMessage = 'Disk to disk copy failed after ' || TO_CHAR(varMaxNbD2dRetries) ||
                              'retries. Last error was : ' || inErrorMessage
         WHERE status = dconst.SUBREQUEST_WAITSUBREQ
           AND castorfile = varCfId;
        -- In case of draining, update DrainingJob
        IF varDrainingJob IS NOT NULL THEN
          updateDrainingJobOnD2dEnd(varDrainingJob, varFileSize, True);
        END IF;
      END IF;
    END;
  END IF;
END;
/

/* handle the creation of the Disk2DiskCopyJobs for the running drainingJobs */
CREATE OR REPLACE PROCEDURE drainRunner AS
  varNbRunningJobs INTEGER;
  varMaxNbOfSchedD2dPerDrain INTEGER;
  varUnused INTEGER;
BEGIN
  -- get maxNbOfSchedD2dPerDrain
  varMaxNbOfSchedD2dPerDrain := TO_NUMBER(getConfigOption('Draining', 'MaxNbSchedD2dPerDrain', '1000'));
  -- loop over draining jobs
  FOR dj IN (SELECT id, fileSystem, svcClass, fileMask, euid, egid
               FROM DrainingJob WHERE status = dconst.DRAININGJOB_RUNNING) LOOP
    DECLARE
      CONSTRAINT_VIOLATED EXCEPTION;
      PRAGMA EXCEPTION_INIT(CONSTRAINT_VIOLATED, -1);
    BEGIN
      BEGIN
        -- lock the draining job first
        SELECT id INTO varUnused FROM DrainingJob WHERE id = dj.id FOR UPDATE;
      EXCEPTION WHEN NO_DATA_FOUND THEN
        -- it was (already!) canceled, go to the next one
        CONTINUE;
      END;
      -- check how many disk2DiskCopyJobs are already running for this draining job
      SELECT count(*) INTO varNbRunningJobs FROM Disk2DiskCopyJob WHERE drainingJob = dj.id;
      -- Loop over the creation of Disk2DiskCopyJobs. Select max 1000 files, taking running
      -- ones into account. Also take the most important jobs first
      logToDLF(NULL, dlf.LVL_SYSTEM, dlf.DRAINING_REFILL, 0, '', 'stagerd',
               'svcClass=' || getSvcClassName(dj.svcClass) || ' DrainReq=' ||
               TO_CHAR(dj.id) || ' MaxNewJobsCount=' || TO_CHAR(varMaxNbOfSchedD2dPerDrain-varNbRunningJobs));
      FOR F IN (SELECT * FROM
                 (SELECT CastorFile.id cfId, Castorfile.nsOpenTime, DiskCopy.id dcId, CastorFile.fileSize
                    FROM DiskCopy, CastorFile
                   WHERE DiskCopy.fileSystem = dj.fileSystem
                     AND CastorFile.id = DiskCopy.castorFile
                     AND ((dj.fileMask = dconst.DRAIN_FILEMASK_NOTONTAPE AND
                           CastorFile.tapeStatus IN (dconst.CASTORFILE_NOTONTAPE, dconst.CASTORFILE_DISKONLY)) OR
                          (dj.fileMask = dconst.DRAIN_FILEMASK_ALL))
                     AND DiskCopy.status = dconst.DISKCOPY_VALID
                     AND NOT EXISTS (SELECT 1 FROM Disk2DiskCopyJob WHERE castorFile = CastorFile.id)
                     AND NOT EXISTS (SELECT 1 FROM DrainingErrors WHERE diskCopy = DiskCopy.id)
                   ORDER BY DiskCopy.importance DESC)
                 WHERE ROWNUM <= varMaxNbOfSchedD2dPerDrain-varNbRunningJobs) LOOP
        createDisk2DiskCopyJob(F.cfId, F.nsOpenTime, dj.svcClass, dj.euid, dj.egid,
                               dconst.REPLICATIONTYPE_DRAINING, F.dcId, TRUE, dj.id, FALSE);
      END LOOP;
      UPDATE DrainingJob
         SET lastModificationTime = getTime()
       WHERE id = dj.id;
      COMMIT;
    EXCEPTION WHEN CONSTRAINT_VIOLATED THEN
      -- check that the constraint violated is due to deletion of the drainingJob
      IF sqlerrm LIKE '%constraint (CASTOR_STAGER.FK_DISK2DISKCOPYJOB_DRAINJOB) violated%' THEN
        -- give up with this DrainingJob as it was canceled
        ROLLBACK;
      ELSE
        raise;
      END IF;
    END;
  END LOOP;
END;
/

/* PL/SQL method implementing getBestDiskCopyToRead used to return the
 * best location of a file based on monitoring information. This is
 * useful for xrootd so that it can avoid scheduling reads
 */
CREATE OR REPLACE PROCEDURE getBestDiskCopyToRead(cfId IN NUMBER,
                                                  svcClassId IN NUMBER,
                                                  diskServer OUT VARCHAR2,
                                                  filePath OUT VARCHAR2) AS
BEGIN
  -- Select best diskcopy
  SELECT name, path INTO diskServer, filePath FROM (
    SELECT DiskServer.name, FileSystem.mountpoint || DiskCopy.path AS path
      FROM DiskCopy, FileSystem, DiskServer, DiskPool2SvcClass
     WHERE DiskCopy.castorfile = cfId
       AND DiskCopy.fileSystem = FileSystem.id
       AND FileSystem.diskpool = DiskPool2SvcClass.parent
       AND DiskPool2SvcClass.child = svcClassId
       AND FileSystem.status IN (dconst.FILESYSTEM_PRODUCTION, dconst.FILESYSTEM_READONLY)
       AND FileSystem.diskserver = DiskServer.id
       AND DiskServer.status IN (dconst.DISKSERVER_PRODUCTION, dconst.DISKSERVER_READONLY)
       AND DiskServer.hwOnline = 1
       AND DiskCopy.status IN (dconst.DISKCOPY_VALID, dconst.DISKCOPY_STAGEOUT)
     ORDER BY FileSystem.nbReadStreams + FileSystem.nbWriteStreams ASC,
              DBMS_Random.value)
   WHERE rownum < 2;
EXCEPTION WHEN NO_DATA_FOUND THEN
  RAISE; -- No file found to be read
END;
/

/* PL/SQL method implementing handleGet */
CREATE OR REPLACE PROCEDURE handleGet(inCfId IN INTEGER, inSrId IN INTEGER,
                                      inFileId IN INTEGER, inNsHost IN VARCHAR2,
                                      inFileSize IN INTEGER, inNsOpenTimeInUsec IN INTEGER) AS
  varNsOpenTime INTEGER;
  varEuid NUMBER;
  varEgid NUMBER;
  varSvcClassId NUMBER;
  varUpd INTEGER;
  varReqUUID VARCHAR(2048);
  varSrUUID VARCHAR(2048);
  varNbDCs INTEGER;
  varDcStatus INTEGER;
BEGIN
  -- lock the castorFile to be safe in case of concurrent subrequests
  SELECT nsOpenTime INTO varNsOpenTime FROM CastorFile WHERE id = inCfId FOR UPDATE;
  -- retrieve the svcClass, user and log data for this subrequest
  SELECT /*+ INDEX(Subrequest PK_Subrequest_Id)*/
         Request.euid, Request.egid, Request.svcClass, Request.upd,
         Request.reqId, SubRequest.subreqId
    INTO varEuid, varEgid, varSvcClassId, varUpd, varReqUUID, varSrUUID
    FROM (SELECT /*+ INDEX(StageGetRequest PK_StageGetRequest_Id) */
                 id, euid, egid, svcClass, 0 upd, reqId from StageGetRequest UNION ALL
          SELECT /*+ INDEX(StageUpdateRequest PK_StageUpdateRequest_Id) */
                 id, euid, egid, svcClass, 1 upd, reqId from StageUpdateRequest) Request,
         SubRequest
   WHERE Subrequest.request = Request.id
     AND Subrequest.id = inSrId;
  -- log
  logToDLF(varReqUUID, dlf.LVL_DEBUG, dlf.STAGER_GET, inFileId, inNsHost, 'stagerd',
           'SUBREQID=' || varSrUUID);

  -- First see whether we should wait on an ongoing request
  DECLARE
    varDcIds "numList";
  BEGIN
    SELECT /*+ INDEX_RS_ASC (DiskCopy I_DiskCopy_CastorFile) */ DiskCopy.id BULK COLLECT INTO varDcIds
      FROM DiskCopy, FileSystem, DiskServer
     WHERE inCfId = DiskCopy.castorFile
       AND FileSystem.id(+) = DiskCopy.fileSystem
       AND nvl(FileSystem.status, 0) IN (dconst.FILESYSTEM_PRODUCTION, dconst.FILESYSTEM_READONLY)
       AND DiskServer.id(+) = FileSystem.diskServer
       AND nvl(DiskServer.status, 0) IN (dconst.DISKSERVER_PRODUCTION, dconst.DISKSERVER_READONLY)
       AND nvl(DiskServer.hwOnline, 1) = 1
       AND DiskCopy.status = dconst.DISKCOPY_WAITFS_SCHEDULING;
    IF varDcIds.COUNT > 0 THEN
      -- DiskCopy is in WAIT*, make SubRequest wait on previous subrequest and do not schedule
      UPDATE /*+ INDEX(Subrequest PK_Subrequest_Id)*/ SubRequest
         SET status = dconst.SUBREQUEST_WAITSUBREQ,
             lastModificationTime = getTime()
      WHERE SubRequest.id = inSrId;
      logToDLF(varReqUUID, dlf.LVL_SYSTEM, dlf.STAGER_WAITSUBREQ, inFileId, inNsHost, 'stagerd',
               'SUBREQID=' || varSrUUID || ' svcClassId=' || getSvcClassName(varSvcClassId) ||
               ' reason="ongoing write request"' || ' existingDcId=' || TO_CHAR(varDcIds(1)));
      RETURN;
    END IF;
  END;

  -- We should actually check whether our disk cache is stale,
  -- that is IF CF.nsOpenTime < inNsOpenTime THEN invalidate our diskcopies.
  -- This is pending the full deployment of the 'new open mode' as implemented
  -- in the fix of bug #95189: Time discrepencies between
  -- disk servers and name servers can lead to silent data loss on input.
  -- The problem being that in 'Compatibility' mode inNsOpenTime is the
  -- namespace's mtime, which can be modified by nstouch,
  -- hence nstouch followed by a Get would destroy the data on disk!

  -- Look for available diskcopies. The status is needed for the
  -- internal replication processing, and only if count = 1, hence
  -- the min() function does not represent anything here.
  -- Note that we accept copies in READONLY hardware here as we're processing Get
  -- and Update requests, and we would deny Updates switching to write mode in that case.
  SELECT /*+ INDEX_RS_ASC (DiskCopy I_DiskCopy_CastorFile) */
         COUNT(DiskCopy.id), min(DiskCopy.status) INTO varNbDCs, varDcStatus
    FROM DiskCopy, FileSystem, DiskServer, DiskPool2SvcClass
   WHERE DiskCopy.castorfile = inCfId
     AND DiskCopy.fileSystem = FileSystem.id
     AND FileSystem.diskpool = DiskPool2SvcClass.parent
     AND DiskPool2SvcClass.child = varSvcClassId
     AND FileSystem.status IN (dconst.FILESYSTEM_PRODUCTION, dconst.FILESYSTEM_READONLY)
     AND FileSystem.diskserver = DiskServer.id
     AND DiskServer.status IN (dconst.DISKSERVER_PRODUCTION, dconst.DISKSERVER_READONLY)
     AND DiskServer.hwOnline = 1
     AND DiskCopy.status IN (dconst.DISKCOPY_VALID, dconst.DISKCOPY_STAGEOUT);

  -- we may be the first update inside a prepareToPut. Check for this case
  IF varNbDCs = 0 AND varUpd = 1 THEN
    SELECT COUNT(DiskCopy.id) INTO varNbDCs
      FROM DiskCopy
     WHERE DiskCopy.castorfile = inCfId
       AND DiskCopy.status = dconst.DISKCOPY_WAITFS;
    IF varNbDCs = 1 THEN
      -- we are the first update, so we actually need to behave as a Put
      handlePut(inCfId, inSrId, inFileId, inNsHost, inNsOpenTimeInUsec);
      RETURN;
    END IF;
  END IF;

  -- first handle the case where we found diskcopies
  IF varNbDCs > 0 THEN
    -- We may still need to replicate the file (replication on demand)
    IF varUpd = 0 AND (varNbDCs > 1 OR varDcStatus != dconst.DISKCOPY_STAGEOUT) THEN
      handleReplication(inSRId, inFileId, inNsHost, inCfId, varNsOpenTime, varSvcClassId,
                        varEuid, varEgid, varReqUUID, varSrUUID);
    END IF;
    DECLARE
      varDcList VARCHAR2(2048);
    BEGIN
      -- List available diskcopies for job scheduling
      SELECT /*+ INDEX_RS_ASC (DiskCopy I_DiskCopy_CastorFile) INDEX(Subrequest PK_Subrequest_Id)*/ 
             LISTAGG(DiskServer.name || ':' || FileSystem.mountPoint, '|')
             WITHIN GROUP (ORDER BY FileSystem.nbReadStreams + FileSystem.nbWriteStreams)
        INTO varDcList
        FROM DiskCopy, FileSystem, DiskServer, DiskPool2SvcClass
       WHERE DiskCopy.castorfile = inCfId
         AND FileSystem.diskpool = DiskPool2SvcClass.parent
         AND DiskPool2SvcClass.child = varSvcClassId
         AND DiskCopy.status IN (dconst.DISKCOPY_VALID, dconst.DISKCOPY_STAGEOUT)
         AND FileSystem.id = DiskCopy.fileSystem
         AND FileSystem.status IN (dconst.FILESYSTEM_PRODUCTION, dconst.FILESYSTEM_READONLY)
         AND DiskServer.id = FileSystem.diskServer
         AND DiskServer.status IN (dconst.DISKSERVER_PRODUCTION, dconst.DISKSERVER_READONLY)
         AND DiskServer.hwOnline = 1;
      -- mark subrequest for scheduling
      UPDATE SubRequest
         SET requestedFileSystems = varDcList,
             xsize = 0,
             status = dconst.SUBREQUEST_READYFORSCHED,
             getNextStatus = dconst.GETNEXTSTATUS_FILESTAGED
       WHERE id = inSrId;
    END;
  ELSE
    -- No diskcopies available for this service class. We may need to recall or trigger a disk to disk copy
    DECLARE
      varD2dId NUMBER;
    BEGIN
      -- Check whether there's already a disk to disk copy going on
      SELECT id INTO varD2dId
        FROM Disk2DiskCopyJob
       WHERE destSvcClass = varSvcClassId    -- this is the destination service class
         AND castorFile = inCfId;
      -- There is an ongoing disk to disk copy, so let's wait on it
      UPDATE /*+ INDEX(Subrequest PK_Subrequest_Id)*/ SubRequest
         SET status = dconst.SUBREQUEST_WAITSUBREQ
       WHERE id = inSrId;
      logToDLF(varReqUUID, dlf.LVL_SYSTEM, dlf.STAGER_WAITSUBREQ, inFileId, inNsHost, 'stagerd',
               'SUBREQID=' || varSrUUID || ' svcClassId=' || getSvcClassName(varSvcClassId) ||
               ' reason="ongoing replication"' || ' ongoingD2dSubReqId=' || TO_CHAR(varD2dId));
    EXCEPTION WHEN NO_DATA_FOUND THEN
      DECLARE 
        varIgnored INTEGER;
      BEGIN
        -- no ongoing disk to disk copy, trigger one or go for a recall
        varIgnored := triggerD2dOrRecall(inCfId, varNsOpenTime, inSrId, inFileId, inNsHost, varEuid, varEgid,
                                         varSvcClassId, inFileSize, varReqUUID, varSrUUID);
      END;
    END;
  END IF;
END;
/

/* PL/SQL method to handle a Repack request. This is performed as part of a DB job. */
CREATE OR REPLACE PROCEDURE handleRepackRequest(inReqUUID IN VARCHAR2,
                                                outNbFilesProcessed OUT INTEGER, outNbFailures OUT INTEGER) AS
  varEuid INTEGER;
  varEgid INTEGER;
  svcClassId INTEGER;
  varRepackVID VARCHAR2(10);
  varReqId INTEGER;
  cfId INTEGER;
  dcId INTEGER;
  repackProtocol VARCHAR2(2048);
  nsHostName VARCHAR2(2048);
  lastKnownFileName VARCHAR2(2048);
  varRecallGroupId INTEGER;
  varRecallGroupName VARCHAR2(2048);
  varCreationTime NUMBER;
  unused INTEGER;
  firstCF boolean := True;
  isOngoing boolean := False;
  varTotalSize INTEGER := 0;
BEGIN
  UPDATE StageRepackRequest SET status = tconst.REPACK_STARTING
   WHERE reqId = inReqUUID
  RETURNING id, euid, egid, svcClass, repackVID
    INTO varReqId, varEuid, varEgid, svcClassId, varRepackVID;
  -- commit so that the status change is visible, even if the request is empty for the moment
  COMMIT;
  outNbFilesProcessed := 0;
  outNbFailures := 0;
  -- Check which protocol should be used for writing files to disk
  repackProtocol := getConfigOption('Repack', 'Protocol', 'rfio');
  -- creation time for the subrequests
  varCreationTime := getTime();
  -- name server host name
  nsHostName := getConfigOption('stager', 'nsHost', '');
  -- find out the recallGroup to be used for this request
  getRecallGroup(varEuid, varEgid, varRecallGroupId, varRecallGroupName);
  -- update potentially missing metadata introduced with v2.1.14. This will be dropped in 2.1.15.
  update2114Data@RemoteNS(varRepackVID);
  COMMIT;
  -- Get the list of files to repack from the NS DB via DBLink and store them in memory
  -- in a temporary table. We do that so that we do not keep an open cursor for too long
  -- in the nameserver DB
  -- Note the truncation of stagerTime to 5 digits. This is needed for consistency with
  -- the stager code that uses the OCCI api and thus loses precision when recuperating
  -- 64 bits integers into doubles (lack of support for 64 bits numbers in OCCI)
  INSERT INTO RepackTapeSegments (fileId, lastOpenTime, blockId, fseq, segSize, copyNb, fileClass, allSegments)
    (SELECT s_fileid, TRUNC(stagertime,5), blockid, fseq, segSize,
            copyno, fileclass,
            (SELECT LISTAGG(TO_CHAR(oseg.copyno)||','||oseg.vid, ',')
             WITHIN GROUP (ORDER BY copyno)
               FROM Cns_Seg_Metadata@remotens oseg
              WHERE oseg.s_fileid = seg.s_fileid
                AND oseg.s_status = '-'
              GROUP BY oseg.s_fileid)
       FROM Cns_Seg_Metadata@remotens seg, Cns_File_Metadata@remotens fileEntry
      WHERE seg.vid = varRepackVID
        AND seg.s_fileid = fileEntry.fileid
        AND seg.s_status = '-'
        AND fileEntry.status != 'D');
  FOR segment IN (SELECT * FROM RepackTapeSegments) LOOP
    DECLARE
      varSubreqId INTEGER;
      varSubreqUUID VARCHAR2(2048);
      varSrStatus INTEGER := dconst.SUBREQUEST_REPACK;
      varMjStatus INTEGER := tconst.MIGRATIONJOB_PENDING;
      varNbCopies INTEGER;
      varSrErrorCode INTEGER := 0;
      varSrErrorMsg VARCHAR2(2048) := NULL;
      varWasRecalled NUMBER;
      varMigrationTriggered BOOLEAN := False;
    BEGIN
      IF MOD(outNbFilesProcessed, 1000) = 0 THEN
        -- Commit from time to time. Update total counter so that the display is correct.
        UPDATE StageRepackRequest
           SET fileCount = outNbFilesProcessed
         WHERE reqId = inReqUUID;
        COMMIT;
        firstCF := TRUE;
      END IF;
      outNbFilesProcessed := outNbFilesProcessed + 1;
      varTotalSize := varTotalSize + segment.segSize;
      -- lastKnownFileName we will have in the DB
      lastKnownFileName := CONCAT('Repack_', TO_CHAR(segment.fileid));
      -- find the Castorfile (and take a lock on it)
      DECLARE
        locked EXCEPTION;
        PRAGMA EXCEPTION_INIT (locked, -54);
      BEGIN
        -- This may raise a Locked exception as we do not want to wait for locks (except on first file).
        -- In such a case, we commit what we've done so far and retry this file, this time waiting for the lock.
        -- The reason for such a complex code is to avoid commiting each file separately, as it would be
        -- too heavy. On the other hand, we still need to avoid dead locks.
        -- Note that we pass 0 for the subrequest id, thus the subrequest will not be attached to the
        -- CastorFile. We actually attach it when we create it.
        selectCastorFileInternal(segment.fileid, nsHostName, segment.fileclass,
                                 segment.segSize, lastKnownFileName, 0, segment.lastOpenTime, firstCF, cfid, unused);
        firstCF := FALSE;
      EXCEPTION WHEN locked THEN
        -- commit what we've done so far
        COMMIT;
        -- And lock the castorfile (waiting this time)
        selectCastorFileInternal(segment.fileid, nsHostName, segment.fileclass,
                                 segment.segSize, lastKnownFileName, 0, segment.lastOpenTime, TRUE, cfid, unused);
      END;
      -- create  subrequest for this file.
      -- Note that the svcHandler is not set. It will actually never be used as repacks are handled purely in PL/SQL
      INSERT INTO SubRequest (retryCounter, fileName, protocol, xsize, priority, subreqId, flags, modeBits, creationTime, lastModificationTime, answered, errorCode, errorMessage, requestedFileSystems, svcHandler, id, diskcopy, castorFile, status, request, getNextStatus, reqType)
      VALUES (0, lastKnownFileName, repackProtocol, segment.segSize, 0, uuidGen(), 0, 0, varCreationTime, varCreationTime, 1, 0, '', NULL, 'NotNullNeeded', ids_seq.nextval, 0, cfId, dconst.SUBREQUEST_START, varReqId, 0, 119)
      RETURNING id, subReqId INTO varSubreqId, varSubreqUUID;
      -- if the file is being overwritten, fail
      SELECT /*+ INDEX_RS_ASC(DiskCopy I_DiskCopy_CastorFile) */
             count(DiskCopy.id) INTO varNbCopies
        FROM DiskCopy
       WHERE DiskCopy.castorfile = cfId
         AND DiskCopy.status = dconst.DISKCOPY_STAGEOUT;
      IF varNbCopies > 0 THEN
        varSrStatus := dconst.SUBREQUEST_FAILED;
        varSrErrorCode := serrno.EBUSY;
        varSrErrorMsg := 'File is currently being overwritten';
        outNbFailures := outNbFailures + 1;
      ELSE
        -- find out whether this file is already on disk
        SELECT /*+ INDEX_RS_ASC(DC I_DiskCopy_CastorFile) */
               count(DC.id) INTO varNbCopies
          FROM DiskCopy DC, FileSystem, DiskServer
         WHERE DC.castorfile = cfId
           AND DC.fileSystem = FileSystem.id
           AND FileSystem.status IN (dconst.FILESYSTEM_PRODUCTION, dconst.FILESYSTEM_READONLY)
           AND FileSystem.diskserver = DiskServer.id
           AND DiskServer.status IN (dconst.DISKSERVER_PRODUCTION, dconst.DISKSERVER_READONLY)
           AND DiskServer.hwOnline = 1
           AND DC.status = dconst.DISKCOPY_VALID;
        IF varNbCopies = 0 THEN
          -- find out whether this file is already being recalled
          SELECT count(*) INTO varWasRecalled FROM RecallJob WHERE castorfile = cfId AND ROWNUM < 2;
          IF varWasRecalled = 0 THEN
            -- trigger recall
            triggerRepackRecall(cfId, segment.fileid, nsHostName, segment.blockid,
                                segment.fseq, segment.copyNb, varEuid, varEgid,
                                varRecallGroupId, svcClassId, varRepackVID, segment.segSize,
                                segment.fileclass, segment.allSegments, inReqUUID, varSubreqUUID, varRecallGroupName);
          END IF;
          -- file is being recalled
          varSrStatus := dconst.SUBREQUEST_WAITTAPERECALL;
          isOngoing := TRUE;
        END IF;
        -- deal with migrations
        IF varSrStatus = dconst.SUBREQUEST_WAITTAPERECALL THEN
          varMJStatus := tconst.MIGRATIONJOB_WAITINGONRECALL;
        END IF;
        DECLARE
          noValidCopyNbFound EXCEPTION;
          PRAGMA EXCEPTION_INIT (noValidCopyNbFound, -20123);
          noMigrationRoute EXCEPTION;
          PRAGMA EXCEPTION_INIT (noMigrationRoute, -20100);
        BEGIN
          triggerRepackMigration(cfId, varRepackVID, segment.fileid, segment.copyNb, segment.fileclass,
                                 segment.segSize, segment.allSegments, varMJStatus, varMigrationTriggered);
          IF varMigrationTriggered THEN
            -- update CastorFile tapeStatus
            UPDATE CastorFile SET tapeStatus = dconst.CASTORFILE_NOTONTAPE WHERE id = cfId;
          END IF;
          isOngoing := True;
        EXCEPTION WHEN noValidCopyNbFound OR noMigrationRoute THEN
          -- cleanup recall part if needed
          IF varWasRecalled = 0 THEN
            DELETE FROM RecallJob WHERE castorFile = cfId;
          END IF;
          -- fail SubRequest
          varSrStatus := dconst.SUBREQUEST_FAILED;
          varSrErrorCode := serrno.EINVAL;
          varSrErrorMsg := SQLERRM;
          outNbFailures := outNbFailures + 1;
        END;
      END IF;
      -- update SubRequest
      UPDATE /*+ INDEX(Subrequest PK_Subrequest_Id) */ SubRequest
         SET status = varSrStatus,
             errorCode = varSrErrorCode,
             errorMessage = varSrErrorMsg
       WHERE id = varSubreqId;
    EXCEPTION WHEN OTHERS THEN
      -- something went wrong: log "handleRepackRequest: unexpected exception caught"
      outNbFailures := outNbFailures + 1;
      varSrErrorMsg := 'Oracle error caught : ' || SQLERRM;
      logToDLF(NULL, dlf.LVL_ERROR, dlf.REPACK_UNEXPECTED_EXCEPTION, segment.fileId, nsHostName, 'repackd',
        'errorCode=' || to_char(SQLCODE) ||' errorMessage="' || varSrErrorMsg
        ||'" stackTrace="' || dbms_utility.format_error_backtrace ||'"');
      -- cleanup and fail SubRequest
      IF varWasRecalled = 0 THEN
        DELETE FROM RecallJob WHERE castorFile = cfId;
      END IF;
      IF varMigrationTriggered THEN
        DELETE FROM MigrationJob WHERE castorFile = cfId;
        DELETE FROM MigratedSegment WHERE castorFile = cfId;
      END IF;
      UPDATE /*+ INDEX(Subrequest PK_Subrequest_Id) */ SubRequest
         SET status = dconst.SUBREQUEST_FAILED,
             errorCode = serrno.SEINTERNAL,
             errorMessage = varSrErrorMsg
       WHERE id = varSubreqId;
    END;
  END LOOP;
  -- cleanup RepackTapeSegments
  EXECUTE IMMEDIATE 'TRUNCATE TABLE RepackTapeSegments';
  -- update status of the RepackRequest
  IF isOngoing THEN
    UPDATE StageRepackRequest
       SET status = tconst.REPACK_ONGOING,
           fileCount = outNbFilesProcessed,
           totalSize = varTotalSize
     WHERE StageRepackRequest.id = varReqId;
  ELSE
    IF outNbFailures > 0 THEN
      UPDATE StageRepackRequest
         SET status = tconst.REPACK_FAILED,
             fileCount = outNbFilesProcessed,
             totalSize = varTotalSize
       WHERE StageRepackRequest.id = varReqId;
    ELSE
      -- CASE of an 'empty' repack : the tape had no files at all
      UPDATE StageRepackRequest
         SET status = tconst.REPACK_FINISHED,
             fileCount = 0,
             totalSize = 0
       WHERE StageRepackRequest.id = varReqId;
    END IF;
  END IF;
  COMMIT;
END;
/

/* insert new Migration Mount */
CREATE OR REPLACE PROCEDURE insertMigrationMount(inTapePoolId IN NUMBER,
                                                 minimumAge IN INTEGER,
                                                 outMountId OUT INTEGER) AS
  varMigJobId INTEGER;
BEGIN
  -- Check that the mount would be honoured by running a dry-run file selection:
  -- note that in case the mount was triggered because of age, we check that
  -- we have a valid candidate that is at least minimumAge seconds old.
  -- This is almost a duplicate of the query in tg_getFilesToMigrate.
  SELECT /*+ FIRST_ROWS_1
             LEADING(MigrationJob CastorFile DiskCopy FileSystem DiskServer)
             USE_NL(MMigrationJob CastorFile DiskCopy FileSystem DiskServer)
             INDEX(CastorFile PK_CastorFile_Id)
             INDEX_RS_ASC(DiskCopy I_DiskCopy_CastorFile)
             INDEX_RS_ASC(MigrationJob I_MigrationJob_TPStatusCT) */
         MigrationJob.id mjId INTO varMigJobId
    FROM MigrationJob, DiskCopy, FileSystem, DiskServer, CastorFile
   WHERE MigrationJob.tapePool = inTapePoolId
     AND MigrationJob.status = tconst.MIGRATIONJOB_PENDING
     AND (minimumAge = 0 OR MigrationJob.creationTime < getTime() - minimumAge)
     AND CastorFile.id = MigrationJob.castorFile
     AND CastorFile.id = DiskCopy.castorFile
     AND CastorFile.tapeStatus = dconst.CASTORFILE_NOTONTAPE
     AND DiskCopy.status = dconst.DISKCOPY_VALID
     AND FileSystem.id = DiskCopy.fileSystem
     AND FileSystem.status IN (dconst.FILESYSTEM_PRODUCTION, dconst.FILESYSTEM_DRAINING, dconst.FILESYSTEM_READONLY)
     AND DiskServer.id = FileSystem.diskServer
     AND DiskServer.status IN (dconst.DISKSERVER_PRODUCTION, dconst.DISKSERVER_DRAINING, dconst.DISKSERVER_READONLY)
     AND DiskServer.hwOnline = 1
     AND ROWNUM < 2;
  -- The select worked out, create a mount for this tape pool
  INSERT INTO MigrationMount
              (mountTransactionId, id, startTime, VID, label, density,
               lastFseq, lastVDQMPingTime, tapePool, status)
    VALUES (NULL, ids_seq.nextval, gettime(), NULL, NULL, NULL,
            NULL, 0, inTapePoolId, tconst.MIGRATIONMOUNT_WAITTAPE)
    RETURNING id INTO outMountId;
EXCEPTION WHEN NO_DATA_FOUND THEN
  -- No valid candidate found: this could happen e.g. when candidates exist
  -- but reside on non-available hardware. In this case we drop the mount and log
  outMountId := 0;
END;
/

/* PL/SQL method internalPutDoneFunc, used by putDoneFunc.
   checks for diskcopies in STAGEOUT and creates the migration jobs
 */
CREATE OR REPLACE PROCEDURE internalPutDoneFunc (cfId IN INTEGER,
                                                 fs IN INTEGER,
                                                 context IN INTEGER,
                                                 nbTC IN INTEGER,
                                                 svcClassId IN INTEGER) AS
  tcId INTEGER;
  gcwProc VARCHAR2(2048);
  gcw NUMBER;
  ouid INTEGER;
  ogid INTEGER;
BEGIN
  -- compute the gc weight of the brand new diskCopy
  gcwProc := castorGC.getUserWeight(svcClassId);
  EXECUTE IMMEDIATE 'BEGIN :newGcw := ' || gcwProc || '(:fs); END;'
    USING OUT gcw, IN fs;
  -- update the DiskCopy
  UPDATE DiskCopy
     SET status = dconst.DISKCOPY_VALID,
         lastAccessTime = getTime(),  -- for the GC, effective lifetime of this diskcopy starts now
         gcWeight = gcw,
         diskCopySize = fs,
         importance = -1              -- we have a single diskcopy for now
   WHERE castorFile = cfId AND status = dconst.DISKCOPY_STAGEOUT
   RETURNING owneruid, ownergid INTO ouid, ogid;
  -- update the CastorFile
  UPDATE Castorfile SET tapeStatus = (CASE WHEN nbTC = 0 OR fs = 0
                                           THEN dconst.CASTORFILE_DISKONLY
                                           ELSE dconst.CASTORFILE_NOTONTAPE
                                       END)
   WHERE id = cfId;
  -- trigger migration when needed
  IF nbTC > 0 AND fs > 0 THEN
    FOR i IN 1..nbTC LOOP
      initMigration(cfId, fs, NULL, NULL, i, tconst.MIGRATIONJOB_PENDING);
    END LOOP;
  END IF;
  -- If we are a real PutDone (and not a put outside of a prepareToPut/Update)
  -- then we have to archive the original prepareToPut/Update subRequest
  IF context = 2 THEN
    -- There can be only a single PrepareTo request: any subsequent PPut would be
    -- rejected and any subsequent PUpdate would be directly archived
    DECLARE
      srId NUMBER;
    BEGIN
      SELECT /*+ INDEX_RS_ASC(Subrequest I_Subrequest_Castorfile)*/ SubRequest.id INTO srId
        FROM SubRequest,
         (SELECT /*+ INDEX(StagePrepareToPutRequest PK_StagePrepareToPutRequest_Id) */ id
            FROM StagePrepareToPutRequest UNION ALL
          SELECT /*+ INDEX(StagePrepareToUpdateRequest PK_StagePrepareToUpdateRequ_Id) */ id
            FROM StagePrepareToUpdateRequest) Request
       WHERE SubRequest.castorFile = cfId
         AND SubRequest.request = Request.id
         AND SubRequest.status = 6;  -- READY
      archiveSubReq(srId, 8);  -- FINISHED
    EXCEPTION WHEN NO_DATA_FOUND THEN
      NULL;   -- ignore the missing subrequest
    END;
  END IF;
  -- Trigger the creation of additional copies of the file, if necessary.
  replicateOnClose(cfId, ouid, ogid);
END;
/

/* PL/SQL method internalPutDoneFunc, used by putDoneFunc.
   checks for diskcopies in STAGEOUT and creates the migration jobs
 */
CREATE OR REPLACE PROCEDURE internalPutDoneFunc (cfId IN INTEGER,
                                                 fs IN INTEGER,
                                                 context IN INTEGER,
                                                 nbTC IN INTEGER,
                                                 svcClassId IN INTEGER) AS
  tcId INTEGER;
  gcwProc VARCHAR2(2048);
  gcw NUMBER;
  ouid INTEGER;
  ogid INTEGER;
BEGIN
  -- compute the gc weight of the brand new diskCopy
  gcwProc := castorGC.getUserWeight(svcClassId);
  EXECUTE IMMEDIATE 'BEGIN :newGcw := ' || gcwProc || '(:fs); END;'
    USING OUT gcw, IN fs;
  -- update the DiskCopy
  UPDATE DiskCopy
     SET status = dconst.DISKCOPY_VALID,
         lastAccessTime = getTime(),  -- for the GC, effective lifetime of this diskcopy starts now
         gcWeight = gcw,
         diskCopySize = fs,
         importance = -1              -- we have a single diskcopy for now
   WHERE castorFile = cfId AND status = dconst.DISKCOPY_STAGEOUT
   RETURNING owneruid, ownergid INTO ouid, ogid;
  -- update the CastorFile
  UPDATE Castorfile SET tapeStatus = (CASE WHEN nbTC = 0 OR fs = 0
                                           THEN dconst.CASTORFILE_DISKONLY
                                           ELSE dconst.CASTORFILE_NOTONTAPE
                                       END)
   WHERE id = cfId;
  -- trigger migration when needed
  IF nbTC > 0 AND fs > 0 THEN
    FOR i IN 1..nbTC LOOP
      initMigration(cfId, fs, NULL, NULL, i, tconst.MIGRATIONJOB_PENDING);
    END LOOP;
  END IF;
  -- If we are a real PutDone (and not a put outside of a prepareToPut/Update)
  -- then we have to archive the original prepareToPut/Update subRequest
  IF context = 2 THEN
    -- There can be only a single PrepareTo request: any subsequent PPut would be
    -- rejected and any subsequent PUpdate would be directly archived
    DECLARE
      srId NUMBER;
    BEGIN
      SELECT /*+ INDEX_RS_ASC(Subrequest I_Subrequest_Castorfile)*/ SubRequest.id INTO srId
        FROM SubRequest,
         (SELECT /*+ INDEX(StagePrepareToPutRequest PK_StagePrepareToPutRequest_Id) */ id
            FROM StagePrepareToPutRequest UNION ALL
          SELECT /*+ INDEX(StagePrepareToUpdateRequest PK_StagePrepareToUpdateRequ_Id) */ id
            FROM StagePrepareToUpdateRequest) Request
       WHERE SubRequest.castorFile = cfId
         AND SubRequest.request = Request.id
         AND SubRequest.status = 6;  -- READY
      archiveSubReq(srId, 8);  -- FINISHED
    EXCEPTION WHEN NO_DATA_FOUND THEN
      NULL;   -- ignore the missing subrequest
    END;
  END IF;
  -- Trigger the creation of additional copies of the file, if necessary.
  replicateOnClose(cfId, ouid, ogid);
END;
/

/* Get next candidates for a given migration mount.
 * input:  VDQM transaction id, count and total size
 * output: outVid    the target VID,
           outFiles  a cursor for the set of migration candidates. 
 * Locks are taken on the selected migration jobs.
 *
 * We should only propose a migration job for a file that does not
 * already have another copy migrated to the same tape.
 * The already migrated copies are kept in MigratedSegment until the whole set
 * of siblings has been migrated.
 */
CREATE OR REPLACE PROCEDURE tg_getBulkFilesToMigrate(inLogContext IN VARCHAR2,
                                                     inMountTrId IN NUMBER,
                                                     inCount IN INTEGER,
                                                     inTotalSize IN INTEGER,
                                                     outFiles OUT castorTape.FileToMigrateCore_cur) AS
  varMountId NUMBER;
  varCount INTEGER;
  varTotalSize INTEGER;
  varOldestAcceptableAge NUMBER;
  varVid VARCHAR2(10);
  varNewFseq INTEGER;
  varFileTrId NUMBER;
  varTpId INTEGER;
  varUnused INTEGER;
  CONSTRAINT_VIOLATED EXCEPTION;
  PRAGMA EXCEPTION_INIT(CONSTRAINT_VIOLATED, -00001);
BEGIN
  BEGIN
    -- Get id, VID and last valid fseq for this migration mount, lock
    SELECT id, vid, tapePool, lastFSeq INTO varMountId, varVid, varTpId, varNewFseq
      FROM MigrationMount
     WHERE mountTransactionId = inMountTrId
       FOR UPDATE;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    -- migration mount is over or unknown request: return an empty cursor
    OPEN outFiles FOR
      SELECT fileId, nsHost, lastKnownFileName, filePath, fileTransactionId, fseq, fileSize
        FROM FilesToMigrateHelper;
    RETURN;
  END;
  varCount := 0;
  varTotalSize := 0;
  SELECT TapePool.maxFileAgeBeforeMount INTO varOldestAcceptableAge
    FROM TapePool, MigrationMount
   WHERE MigrationMount.id = varMountId
     AND MigrationMount.tapePool = TapePool.id;
  -- Get candidates up to inCount or inTotalSize
  FOR Cand IN (
    SELECT /*+ FIRST_ROWS(100)
               LEADING(Job CastorFile DiskCopy FileSystem DiskServer)
               USE_NL(Job CastorFile DiskCopy FileSystem DiskServer)
               INDEX(CastorFile PK_CastorFile_Id)
               INDEX_RS_ASC(DiskCopy I_DiskCopy_CastorFile) */
           Job.id mjId, DiskServer.name || ':' || FileSystem.mountPoint || DiskCopy.path filePath,
           CastorFile.fileId, CastorFile.nsHost, CastorFile.fileSize, CastorFile.lastKnownFileName,
           Castorfile.id as castorfile
      FROM (SELECT * FROM
             (SELECT /*+ FIRST_ROWS(100) INDEX_RS_ASC(MigrationJob I_MigrationJob_TPStatusCT) */
                     id, castorfile, destCopyNb, creationTime
                FROM MigrationJob
               WHERE tapePool = varTpId
                 AND status = tconst.MIGRATIONJOB_PENDING
               ORDER BY creationTime)
             WHERE ROWNUM < TO_NUMBER(getConfigOption('Migration', 'NbMigCandConsidered', 10000)))
           Job, CastorFile, DiskCopy, FileSystem, DiskServer
     WHERE CastorFile.id = Job.castorFile
       AND CastorFile.id = DiskCopy.castorFile
       AND CastorFile.tapeStatus = dconst.CASTORFILE_NOTONTAPE
       AND DiskCopy.status = dconst.DISKCOPY_VALID
       AND FileSystem.id = DiskCopy.fileSystem
       AND FileSystem.status IN (dconst.FILESYSTEM_PRODUCTION, dconst.FILESYSTEM_DRAINING, dconst.FILESYSTEM_READONLY)
       AND DiskServer.id = FileSystem.diskServer
       AND DiskServer.status IN (dconst.DISKSERVER_PRODUCTION, dconst.DISKSERVER_DRAINING, dconst.DISKSERVER_READONLY)
       AND DiskServer.hwOnline = 1
       AND NOT EXISTS (SELECT /*+ USE_NL(MigratedSegment)
                                  INDEX_RS_ASC(MigratedSegment I_MigratedSegment_CFCopyNBVID) */ 1
                         FROM MigratedSegment
                        WHERE MigratedSegment.castorFile = Job.castorfile
                          AND MigratedSegment.copyNb != Job.destCopyNb
                          AND MigratedSegment.vid = varVid)
       ORDER BY -- we first order by a multi-step function, which gives old guys incrasingly more priority:
                -- migrations younger than varOldestAccep2tableAge will be taken last
                TRUNC(Job.creationTime/varOldestAcceptableAge) ASC,
                -- and then, for all migrations between (N-1)*varOldestAge and N*varOldestAge, by filesystem load
                FileSystem.nbRecallerStreams + FileSystem.nbMigratorStreams ASC)
  LOOP
    -- last part of the above statement. Could not be part of it as ORACLE insisted on not
    -- optimizing properly the execution plan
    BEGIN
      SELECT /*+ INDEX_RS_ASC(MJ I_MigrationJob_CFVID) */ 1 INTO varUnused
        FROM MigrationJob MJ
       WHERE MJ.castorFile = Cand.castorFile
         AND MJ.vid = varVid
         AND MJ.vid IS NOT NULL;
      -- found one, so skip this candidate
      CONTINUE;
    EXCEPTION WHEN NO_DATA_FOUND THEN
      -- nothing, it's a valid candidate. Let's lock it and revalidate the status
      DECLARE
        MjLocked EXCEPTION;
        PRAGMA EXCEPTION_INIT (MjLocked, -54);
      BEGIN
        SELECT id INTO varUnused
          FROM MigrationJob
         WHERE id = Cand.mjId
           AND status = tconst.MIGRATIONJOB_PENDING
           FOR UPDATE NOWAIT;
      EXCEPTION WHEN MjLocked THEN
        -- this migration job is being handled else where, let's go to next one
        CONTINUE;
                WHEN NO_DATA_FOUND THEN
        -- this migration job has already been handled else where, let's go to next one
        CONTINUE;
      END;
    END;
    BEGIN
      -- Try to take this candidate on this mount
      INSERT INTO FilesToMigrateHelper (fileId, nsHost, lastKnownFileName, filePath, fileTransactionId, fileSize, fseq)
        VALUES (Cand.fileId, Cand.nsHost, Cand.lastKnownFileName, Cand.filePath, ids_seq.NEXTVAL, Cand.fileSize, varNewFseq)
        RETURNING fileTransactionId INTO varFileTrId;
    EXCEPTION WHEN CONSTRAINT_VIOLATED THEN
      -- If we fail here, it means that another copy of this file was already selected for this mount.
      -- Not a big deal, we skip this candidate and keep going.
      CONTINUE;
    END;
    varCount := varCount + 1;
    varTotalSize := varTotalSize + Cand.fileSize;
    UPDATE MigrationJob
       SET status = tconst.MIGRATIONJOB_SELECTED,
           vid = varVid,
           fSeq = varNewFseq,
           mountTransactionId = inMountTrId,
           fileTransactionId = varFileTrId
     WHERE id = Cand.mjId;
    varNewFseq := varNewFseq + 1;    -- we still decide the fseq for each migration candidate
    IF varCount >= inCount OR varTotalSize >= inTotalSize THEN
      -- we have enough candidates for this round, exit loop
      EXIT;
    END IF;
  END LOOP;
  -- Update last fseq
  UPDATE MigrationMount
     SET lastFseq = varNewFseq
   WHERE id = varMountId;
  -- Return all candidates (potentially an empty cursor). Don't commit now, this will be done
  -- in C++ after the results have been collected as the temporary table will be emptied. 
  OPEN outFiles FOR
    SELECT fileId, nsHost, lastKnownFileName, filePath, fileTransactionId, fseq, fileSize
      FROM FilesToMigrateHelper;
END;
/

/* Get next candidates for a given recall mount.
 * input:  VDQM transaction id, count and total size
 * output: outFiles, a cursor for the set of recall candidates.
 */
CREATE OR REPLACE PROCEDURE tg_getBulkFilesToRecall(inLogContext IN VARCHAR2,
                                                    inMountTrId IN NUMBER,
                                                    inCount IN INTEGER,
                                                    inTotalSize IN INTEGER,
                                                    outFiles OUT SYS_REFCURSOR) AS
  varVid VARCHAR2(10);
  varPreviousFseq INTEGER;
  varCount INTEGER;
  varTotalSize INTEGER;
  varPath VARCHAR2(2048);
  varFileTrId INTEGER;
  varNewFseq INTEGER;
  bestFSForRecall_error EXCEPTION;
  PRAGMA EXCEPTION_INIT(bestFSForRecall_error, -20115);
  varNbLockedFiles INTEGER := 0;
BEGIN
  BEGIN
    -- Get VID and last processed fseq for this recall mount, lock
    SELECT vid, lastProcessedFseq INTO varVid, varPreviousFseq
      FROM RecallMount
     WHERE mountTransactionId = inMountTrId
       FOR UPDATE;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    -- recall is over or unknown request: return an empty cursor
    OPEN outFiles FOR
      SELECT fileId, nsHost, fileTransactionId, filePath, blockId, fseq, copyNb,
             euid, egid, VID, fileSize, creationTime, nbRetriesInMount, nbMounts
        FROM FilesToRecallHelper;
    RETURN;
  END;
  varCount := 0;
  varTotalSize := 0;
  varNewFseq := varPreviousFseq;
  -- Get candidates up to inCount or inTotalSize
  -- Select only the ones further down the tape (fseq > current one) as long as possible
  LOOP
    DECLARE
      varRjId INTEGER;
      varFSeq INTEGER;
      varBlockId RAW(4);
      varFileSize INTEGER;
      varCfId INTEGER;
      varFileId INTEGER;
      varNsHost VARCHAR2(2048);
      varCopyNb NUMBER;
      varEuid NUMBER;
      varEgid NUMBER;
      varCreationTime NUMBER;
      varNbRetriesInMount NUMBER;
      varNbMounts NUMBER;
      CfLocked EXCEPTION;
      PRAGMA EXCEPTION_INIT (CfLocked, -54);
    BEGIN
      -- Find the unprocessed recallJobs of this tape with lowest fSeq
      -- that is above the previous one
      SELECT * INTO varRjId, varFSeq, varBlockId, varFileSize, varCfId, varCopyNb,
                    varEuid, varEgid, varCreationTime, varNbRetriesInMount,
                    varNbMounts
        FROM (SELECT id, fSeq, blockId, fileSize, castorFile, copyNb, eUid, eGid,
                     creationTime, nbRetriesWithinMount,  nbMounts
                FROM RecallJob
               WHERE vid = varVid
                 AND status = tconst.RECALLJOB_PENDING
                 AND fseq > varNewFseq
               ORDER BY fseq ASC)
       WHERE ROWNUM < 2;
      -- move up last fseq used. Note that it moves up even if bestFileSystemForRecall
      -- (or any other statement) fails and the file is actually not recalled.
      -- The goal is that a potential retry within the same mount only occurs after
      -- we went around the other files on this tape.
      varNewFseq := varFseq;
      -- lock the corresponding CastorFile, give up if we do not manage as it means that
      -- this file is already being handled by someone else
      -- Note that the giving up is handled by the handling of the CfLocked exception
      SELECT fileId, nsHost INTO varFileId, varNsHost
        FROM CastorFile
       WHERE id = varCfId
         FOR UPDATE NOWAIT;
      -- Now that we have the lock, double check that the RecallJob is still there and
      -- valid (due to race conditions, it may have been processed in between our first select
      -- and the taking of the lock)
      BEGIN
        SELECT id INTO varRjId FROM RecallJob WHERE id = varRJId AND status = tconst.RECALLJOB_PENDING;
      EXCEPTION WHEN NO_DATA_FOUND THEN
        -- we got the race condition ! So this has already been handled, let's move to next file
        CONTINUE;
      END;
      -- Find the best filesystem to recall the selected file
      bestFileSystemForRecall(varCfId, varPath);
      varCount := varCount + 1;
      varTotalSize := varTotalSize + varFileSize;
      INSERT INTO FilesToRecallHelper (fileId, nsHost, fileTransactionId, filePath, blockId, fSeq,
                 copyNb, euid, egid, VID, fileSize, creationTime, nbRetriesInMount, nbMounts)
        VALUES (varFileId, varNsHost, ids_seq.nextval, varPath, varBlockId, varFSeq,
                varCopyNb, varEuid, varEgid, varVID, varFileSize, varCreationTime, varNbRetriesInMount,
                varNbMounts)
        RETURNING fileTransactionId INTO varFileTrId;
      -- update RecallJobs of this file. Only the recalled one gets a fileTransactionId
      UPDATE RecallJob
         SET status = tconst.RECALLJOB_SELECTED,
             fileTransactionID = CASE WHEN id = varRjId THEN varFileTrId ELSE NULL END
       WHERE castorFile = varCfId;
      IF varCount >= inCount OR varTotalSize >= inTotalSize THEN
        -- we have enough candidates for this round, exit loop
        EXIT;
      END IF;
    EXCEPTION
      WHEN CfLocked THEN
        -- Go to next candidate, this CastorFile is being processed by another thread
        -- still check that this does not happen too often
        -- the reason is that a long standing lock (due to another bug) would make us spin
        -- like mad (learnt the hard way in production...)
        varNbLockedFiles := varNbLockedFiles + 1;
        IF varNbLockedFiles >= 100 THEN
          DECLARE
            lastSQL VARCHAR2(2048);
            prevSQL VARCHAR2(2048);
          BEGIN
            -- find the blocking SQL
            SELECT lastSql.sql_text, prevSql.sql_text INTO lastSQL, prevSQL
              FROM v$session currentSession, v$session blockerSession, v$sql lastSql, v$sql prevSql
             WHERE currentSession.sid = (SELECT sys_context('USERENV','SID') FROM DUAL)
               AND blockerSession.sid(+) = currentSession.blocking_session
               AND lastSql.sql_id(+) = blockerSession.sql_id
               AND prevSql.sql_id(+) = blockerSession.prev_sql_id;
            -- log the issue and exit, as if we were out of candidates
            logToDLF(NULL, dlf.LVL_ERROR, dlf.RECALL_LOOPING_ON_LOCK, varFileId, varNsHost, 'tapegatewayd',
                   'SQLOfLockingSession="' || lastSQL || '" PreviousSQLOfLockingSession="' || prevSQL ||
                   '" mountTransactionId=' || to_char(inMountTrId) ||' '|| inLogContext);
            EXIT;
          END;
        END IF;
      WHEN bestFSForRecall_error THEN
        -- log 'bestFileSystemForRecall could not find a suitable destination for this recall' and skip it
        logToDLF(NULL, dlf.LVL_ERROR, dlf.RECALL_FS_NOT_FOUND, varFileId, varNsHost, 'tapegatewayd',
                 'errorMessage="' || SQLERRM || '" mountTransactionId=' || to_char(inMountTrId) ||' '|| inLogContext);
        -- mark the recall job as failed, and maybe retry
        retryOrFailRecall(varCfId, varVID, NULL, inLogContext);
      WHEN NO_DATA_FOUND THEN
        -- nothing found. In case we did not try so far, try to restart with low fseqs
        IF varNewFseq > -1 THEN
          varNewFseq := -1;
        ELSE
          -- low fseqs were tried, we are really out of candidates, so exit the loop
          EXIT;
        END IF;
    END;
  END LOOP;
  -- Record last fseq at the mount level
  UPDATE RecallMount
     SET lastProcessedFseq = varNewFseq
   WHERE vid = varVid;
  -- Return all candidates. Don't commit now, this will be done in C++
  -- after the results have been collected as the temporary table will be emptied.
  OPEN outFiles FOR
    SELECT fileId, nsHost, fileTransactionId, filePath, blockId, fseq,
           copyNb, euid, egid, VID, fileSize, creationTime,
           nbRetriesInMount, nbMounts
      FROM FilesToRecallHelper;
END;
/

/* Procedure responsible for rebalancing one given filesystem by moving away
 * the given amount of data */
CREATE OR REPLACE PROCEDURE rebalance(inFsId IN INTEGER, inDataAmount IN INTEGER,
                                      inDestSvcClassId IN INTEGER,
                                      inDiskServerName IN VARCHAR2, inMountPoint IN VARCHAR2) AS
  CURSOR DCcur IS
    SELECT /*+ FIRST_ROWS_10 */
           DiskCopy.id, DiskCopy.diskCopySize, CastorFile.id, CastorFile.nsOpenTime
      FROM DiskCopy, CastorFile
     WHERE DiskCopy.fileSystem = inFsId
       AND DiskCopy.status = dconst.DISKCOPY_VALID
       AND CastorFile.id = DiskCopy.castorFile;
  varDcId INTEGER;
  varDcSize INTEGER;
  varCfId INTEGER;
  varNsOpenTime INTEGER;
  varTotalRebalanced INTEGER := 0;
  varNbFilesRebalanced INTEGER := 0;
BEGIN
  -- disk to disk copy files out of this node until we reach inDataAmount
  -- "rebalancing : starting" message
  logToDLF(NULL, dlf.LVL_SYSTEM, dlf.REBALANCING_START, 0, '', 'stagerd',
           'DiskServer=' || inDiskServerName || ' mountPoint=' || inMountPoint ||
           ' dataToMove=' || TO_CHAR(TRUNC(inDataAmount)));
  -- Loop on candidates until we can lock one
  OPEN DCcur;
  LOOP
    -- Fetch next candidate
    FETCH DCcur INTO varDcId, varDcSize, varCfId, varNsOpenTime;
    -- no next candidate : this is surprising, but nevertheless, we should go out of the loop
    IF DCcur%NOTFOUND THEN EXIT; END IF;
    -- stop if it would be too much
    IF varTotalRebalanced + varDcSize > inDataAmount THEN EXIT; END IF;
    -- compute new totals
    varTotalRebalanced := varTotalRebalanced + varDcSize;
    varNbFilesRebalanced := varNbFilesRebalanced + 1;
    -- create disk2DiskCopyJob for this diskCopy
    createDisk2DiskCopyJob(varCfId, varNsOpenTime, inDestSvcClassId,
                           0, 0, dconst.REPLICATIONTYPE_REBALANCE,
                           varDcId, TRUE, NULL, FALSE);
  END LOOP;
  CLOSE DCcur;
  -- "rebalancing : stopping" message
  logToDLF(NULL, dlf.LVL_SYSTEM, dlf.REBALANCING_STOP, 0, '', 'stagerd',
           'DiskServer=' || inDiskServerName || ' mountPoint=' || inMountPoint ||
           ' dataMoveTriggered=' || TO_CHAR(varTotalRebalanced) ||
           ' nbFileMovesTriggered=' || TO_CHAR(varNbFilesRebalanced));
END;
/

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
  fid NUMBER;
  fc NUMBER;
  nsh VARCHAR2(2048);
  nb INTEGER;
  CONSTRAINT_VIOLATED EXCEPTION;
  PRAGMA EXCEPTION_INIT(CONSTRAINT_VIOLATED, -1);
BEGIN
  IF dcIds.COUNT > 0 THEN
    -- List the castorfiles to be cleaned up afterwards
    FORALL i IN dcIds.FIRST .. dcIds.LAST
      INSERT INTO FilesDeletedProcHelper (cfId, dcId) (
        SELECT castorFile, id FROM DiskCopy
         WHERE id = dcIds(i));
    -- Use a normal loop to clean castorFiles. Note: We order the list to
    -- prevent a deadlock
    FOR cf IN (SELECT cfId, dcId
                 FROM filesDeletedProcHelper
                ORDER BY cfId ASC) LOOP
      BEGIN
        -- Get data and lock the castorFile
        SELECT fileId, nsHost, fileClass
          INTO fid, nsh, fc
          FROM CastorFile
         WHERE id = cf.cfId FOR UPDATE;
        BEGIN
          -- delete the original diskcopy to be dropped
          DELETE FROM DiskCopy WHERE id = cf.dcId;
        EXCEPTION WHEN CONSTRAINT_VIOLATED THEN
          -- there's some left-over d2d job linked to this diskCopy:
          -- drop it and try again, the physical disk copy does not exist any longer
          DELETE FROM Disk2DiskCopyJob WHERE srcDcId = cf.dcId;
          DELETE FROM DiskCopy WHERE id = cf.dcId;
        END;
        -- Cleanup: attempt to delete the CastorFile. Thanks to FKs,
        -- this will fail if in the meantime some other activity took
        -- ownership of the CastorFile entry.
        BEGIN
          DELETE FROM CastorFile WHERE id = cf.cfId;
          -- And check whether this file potentially had copies on tape
          SELECT nbCopies INTO nb FROM FileClass WHERE id = fc;
          IF nb = 0 THEN
            -- This castorfile was created with no copy on tape
            -- So removing it from the stager means erasing
            -- it completely. We should thus also remove it
            -- from the name server
            INSERT INTO FilesDeletedProcOutput (fileId, nsHost) VALUES (fid, nsh);
          END IF;
        EXCEPTION WHEN CONSTRAINT_VIOLATED THEN
          -- Ignore the deletion, some draining/rebalancing/recall activity
          -- started to reuse the CastorFile
          NULL;
        END;
      EXCEPTION WHEN NO_DATA_FOUND THEN
        -- This means that the castorFile did not exist.
        -- There is thus no way to find out whether to remove the
        -- file from the nameserver. For safety, we thus keep it
        NULL;
      END;
    END LOOP;
  END IF;
  OPEN fileIds FOR
    SELECT fileId, nsHost FROM FilesDeletedProcOutput;
END;
/

/* PL/SQL procedure called when a repack request is over: see archiveSubReq */
CREATE OR REPLACE PROCEDURE handleEndOfRepack(inReqId INTEGER) AS
  nbLeftSegs INTEGER;
BEGIN
  -- check if any segment is left in the original tape
  SELECT 1 INTO nbLeftSegs FROM Cns_seg_metadata@RemoteNS
   WHERE vid = (SELECT repackVID FROM StageRepackRequest WHERE id = inReqId)
     AND ROWNUM < 2;
  -- we found at least one segment, final status is FAILED
  UPDATE StageRepackRequest
     SET status = tconst.REPACK_FAILED,
         lastModificationTime = getTime()
   WHERE id = inReqId;
EXCEPTION WHEN NO_DATA_FOUND THEN
  -- no segments found, repack was successful
  UPDATE StageRepackRequest
     SET status = tconst.REPACK_FINISHED,
         lastModificationTime = getTime()
   WHERE id = inReqId;
END;
/

/* PL/SQL method to archive a SubRequest */
CREATE OR REPLACE PROCEDURE archiveSubReq(srId IN INTEGER, finalStatus IN INTEGER) AS
  unused INTEGER;
  rId INTEGER;
  rName VARCHAR2(100);
  rType NUMBER := 0;
  clientId INTEGER;
BEGIN
  UPDATE /*+ INDEX(Subrequest PK_Subrequest_Id) */ SubRequest
     SET diskCopy = NULL,  -- unlink this subrequest as it's dead now
         lastModificationTime = getTime(),
         status = finalStatus
   WHERE id = srId
   RETURNING request, reqType, (SELECT object FROM Type2Obj WHERE type = reqType) INTO rId, rType, rName;
  -- Try to see whether another subrequest in the same
  -- request is still being processed. For this, we
  -- need a master lock on the request.
  EXECUTE IMMEDIATE
    'BEGIN SELECT client INTO :clientId FROM '|| rName ||' WHERE id = :rId FOR UPDATE; END;'
    USING OUT clientId, IN rId;
  BEGIN
    -- note the decode trick to use the dedicated index I_SubRequest_Req_Stat_no89
    SELECT request INTO unused FROM SubRequest
     WHERE request = rId AND decode(status,8,NULL,9,NULL,status) IS NOT NULL
       AND ROWNUM < 2;  -- all but {FAILED_,}FINISHED
  EXCEPTION WHEN NO_DATA_FOUND THEN
    -- All subrequests have finished, we can archive:
    -- drop the associated Client entity
    DELETE FROM Client WHERE id = clientId;
    -- archive the successful subrequests
    UPDATE /*+ INDEX_RS_ASC(SubRequest I_SubRequest_Request) */ SubRequest
       SET status = dconst.SUBREQUEST_ARCHIVED
     WHERE request = rId
       AND status = dconst.SUBREQUEST_FINISHED;
    -- special handling in case of repack
    IF rType = 119 THEN  -- OBJ_StageRepackRequest
      handleEndOfRepack(rId);
    END IF;
  END;
END;
/

-- Drop obsoleted entities
DROP INDEX I_FileSystem_Rate;
DROP FUNCTION fileSystemRate;
DROP INDEX I_MigrationJob_TPStatusId;

/* Recompile all invalid procedures, triggers and functions */
/************************************************************/
BEGIN
  recompileAll();
END;
/

/* Flag the schema upgrade as COMPLETE */
/***************************************/
UPDATE UpgradeLog SET endDate = systimestamp, state = 'COMPLETE'
 WHERE release = '2_1_14_13';
COMMIT;

/* Post-upgrade cleanup phase: archive all requests older than 24h and stuck in status 4 */
BEGIN
  FOR s in (SELECT id FROM SubRequest WHERE status = 4 AND creationTime < getTime() - 86400) LOOP
    archiveSubReq(s.id, dconst.SUBREQUEST_FINISHED);
    COMMIT;
  END LOOP;
END;
/

