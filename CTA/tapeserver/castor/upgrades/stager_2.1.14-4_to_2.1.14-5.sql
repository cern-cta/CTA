/******************************************************************************
 *                 stager_2.1.14-4_to_2.1.14-5.sql
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
 * This script upgrades a CASTOR v2.1.14-4 STAGER database to v2.1.14-5
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
     AND release = '2_1_14_5'
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
     AND release LIKE '2_1_14_4%';
EXCEPTION WHEN NO_DATA_FOUND THEN
  -- Error, we cannot apply this script
  raise_application_error(-20000, 'PL/SQL release mismatch. Please run previous upgrade scripts for the STAGER before this one.');
END;
/

INSERT INTO UpgradeLog (schemaVersion, release, type)
VALUES ('2_1_14_2', '2_1_14_5', 'TRANSPARENT');
COMMIT;

/* Disable garbage collection during the cleanup of CastorFiles with NULL tapeStatus */
/* this is done by replacing selectFiles2Delete by a version selecting only INVALID files */

/* PL/SQL method implementing selectFiles2Delete
   This is the standard garbage collector: it sorts VALID diskcopies
   that do not need to go to tape by gcWeight and selects them for deletion up to
   the desired free space watermark */
CREATE OR REPLACE
PROCEDURE selectFiles2Delete(diskServerName IN VARCHAR2,
                             files OUT castorGC.SelectFiles2DeleteLine_Cur) AS
  dcIds "numList";
  freed INTEGER;
  deltaFree INTEGER;
  toBeFreed INTEGER;
  totalCount INTEGER;
  unused INTEGER;
  backoff INTEGER;
  CastorFileLocked EXCEPTION;
  PRAGMA EXCEPTION_INIT (CastorFileLocked, -54);
BEGIN
  -- Loop on all concerned fileSystems in a random order.
  totalCount := 0;
  FOR fs IN (SELECT FileSystem.id
               FROM FileSystem, DiskServer
              WHERE FileSystem.diskServer = DiskServer.id
                AND DiskServer.name = diskServerName
             ORDER BY dbms_random.value) LOOP

    -- Count the number of diskcopies on this filesystem that are in a
    -- BEINGDELETED state. These need to be reselected in any case.
    freed := 0;
    SELECT totalCount + count(*), nvl(sum(DiskCopy.diskCopySize), 0)
      INTO totalCount, freed
      FROM DiskCopy
     WHERE DiskCopy.fileSystem = fs.id
       AND decode(status, 9, status, NULL) = 9;  -- BEINGDELETED (decode used to use function-based index)

    -- estimate the number of GC running the "long" query, that is the one dealing with the GCing of
    -- VALID files.
    SELECT COUNT(*) INTO backoff
      FROM v$session s, v$sqltext t
     WHERE s.sql_id = t.sql_id AND t.sql_text LIKE '%I_DiskCopy_FS_GCW%';

    -- Process diskcopies that are in an INVALID state.
    UPDATE /*+ INDEX_RS_ASC(DiskCopy I_DiskCopy_Status_7_FS)) */ DiskCopy
       SET status = 9, -- BEINGDELETED
           gcType = decode(gcType, NULL, dconst.GCTYPE_USER, gcType)
     WHERE fileSystem = fs.id
       AND decode(status, 7, status, NULL) = 7  -- INVALID (decode used to use function-based index)
       AND rownum <= 10000 - totalCount
    RETURNING id BULK COLLECT INTO dcIds;
    COMMIT;

    -- If we have more than 10,000 files to GC, exit the loop. There is no point
    -- processing more as the maximum sent back to the client in one call is
    -- 10,000. This protects the garbage collector from being overwhelmed with
    -- requests and reduces the stager DB load. Furthermore, if too much data is
    -- sent back to the client, the transfer time between the stager and client
    -- becomes very long and the message may timeout or may not even fit in the
    -- clients receive buffer!
    totalCount := totalCount + dcIds.COUNT();
    EXIT WHEN totalCount >= 10000;
  END LOOP;

  -- Now select all the BEINGDELETED diskcopies in this diskserver for the GC daemon
  OPEN files FOR
    SELECT /*+ INDEX(CastorFile PK_CastorFile_ID) */ FileSystem.mountPoint || DiskCopy.path,
           DiskCopy.id,
           Castorfile.fileid, Castorfile.nshost,
           DiskCopy.lastAccessTime, DiskCopy.nbCopyAccesses, DiskCopy.gcWeight,
           getObjStatusName('DiskCopy', 'gcType', DiskCopy.gcType),
          getSvcClassList(FileSystem.id)
      FROM CastorFile, DiskCopy, FileSystem, DiskServer
     WHERE decode(DiskCopy.status, 9, DiskCopy.status, NULL) = 9 -- BEINGDELETED
       AND DiskCopy.castorfile = CastorFile.id
       AND DiskCopy.fileSystem = FileSystem.id
       AND FileSystem.diskServer = DiskServer.id
       AND DiskServer.name = diskServerName
       AND rownum <= 10000;
END;
/


/* Fix bug #103242 Incorrect evaluation of lastModificationTime leads to double recalls + dark data. */


/* Checks whether a recall that was reported successful is ok from the namespace
 * point of view. This includes :
 *   - checking that the file still exists
 *   - checking that the file was not overwritten
 *   - checking the checksum, and setting it if there was none
 * In case one of the check fails, appropriate cleaning actions are taken.
 * Returns whether the checks were all ok. If not, the caller should
 * return immediately as all corrective actions were already taken.
 */
CREATE OR REPLACE FUNCTION checkRecallInNS(inCfId IN INTEGER,
                                           inMountTransactionId IN INTEGER,
                                           inVID IN VARCHAR2,
                                           inCopyNb IN INTEGER,
                                           inFseq IN INTEGER,
                                           inFileId IN INTEGER,
                                           inNsHost IN VARCHAR2,
                                           inCksumName IN VARCHAR2,
                                           inCksumValue IN INTEGER,
                                           inLastOpenTime IN NUMBER,
                                           inReqId IN VARCHAR2,
                                           inLogContext IN VARCHAR2) RETURN BOOLEAN AS
  varNSOpenTime NUMBER;
  varNSSize INTEGER;
  varNSCsumtype VARCHAR2(2048);
  varNSCsumvalue VARCHAR2(2048);
  varOpenMode CHAR(1);
BEGIN
  -- retrieve data from the namespace: note that if stagerTime is (still) NULL,
  -- we're still in compatibility mode and we resolve to using mtime.
  -- To be dropped in 2.1.15 where stagerTime is NOT NULL by design.
  SELECT NVL(stagerTime, mtime), csumtype, csumvalue, filesize
    INTO varNSOpenTime, varNSCsumtype, varNSCsumvalue, varNSSize
    FROM Cns_File_Metadata@RemoteNS
   WHERE fileid = inFileId;
  -- check open mode: in compatibility mode we still have only seconds precision,
  -- hence the NS open time has to be truncated prior to comparing it with our time.
  varOpenMode := getConfigOption@RemoteNS('stager', 'openmode', NULL);
  IF varOpenMode = 'C' THEN
    varNSOpenTime := TRUNC(varNSOpenTime);
  END IF;
  -- was the file overwritten in the meantime ?
  IF varNSOpenTime > inLastOpenTime THEN
    -- yes ! reset it and thus restart the recall from scratch
    resetOverwrittenCastorFile(inCfId, varNSOpenTime, varNSSize);
    -- in case of repack, just stop and archive the corresponding request(s) as we're not interested
    -- any longer (the original segment disappeared). This potentially stops the entire recall process.
    archiveOrFailRepackSubreq(inCfId, serrno.ENSFILECHG);
    -- log "setFileRecalled : file was overwritten during recall, restarting from scratch or skipping repack"
    logToDLF(inReqId, dlf.LVL_NOTICE, dlf.RECALL_FILE_OVERWRITTEN, inFileId, inNsHost, 'tapegatewayd',
             'mountTransactionId=' || TO_CHAR(inMountTransactionId) || ' TPVID=' || inVID ||
             ' fseq=' || TO_CHAR(inFseq) || ' NSOpenTime=' || TRUNC(varNSOpenTime, 6) ||
             ' NsOpenTimeAtStager=' || TRUNC(inLastOpenTime, 6) ||' '|| inLogContext);
    RETURN FALSE;
  END IF;

  -- is the checksum set in the namespace ?
  IF varNSCsumtype IS NULL THEN
    -- no -> let's set it (note that the function called commits in the remote DB)
    setSegChecksumWhenNull@remoteNS(inFileId, inCopyNb, inCksumName, inCksumValue);
    -- log 'checkRecallInNS : created missing checksum in the namespace'
    logToDLF(inReqId, dlf.LVL_SYSTEM, dlf.RECALL_CREATED_CHECKSUM, inFileId, inNsHost, 'nsd',
             'mountTransactionId=' || TO_CHAR(inMountTransactionId) || ' copyNb=' || TO_CHAR(inCopyNb) ||
             ' TPVID=' || inVID || ' fseq=' || TO_CHAR(inFseq) || ' checksumType='  || inCksumName ||
             ' checksumValue=' || TO_CHAR(inCksumValue));
  ELSE
    -- is the checksum matching ?
    -- note that this is probably useless as it was already checked at transfer time
    IF inCksumName = varNSCsumtype AND TO_CHAR(inCksumValue, 'XXXXXXXX') != varNSCsumvalue THEN
      -- not matching ! log "checkRecallInNS : bad checksum detected, will retry if allowed"
      logToDLF(inReqId, dlf.LVL_ERROR, dlf.RECALL_BAD_CHECKSUM, inFileId, inNsHost, 'tapegatewayd',
               'mountTransactionId=' || TO_CHAR(inMountTransactionId) || ' TPVID=' || inVID ||
               ' fseq=' || TO_CHAR(inFseq) || ' copyNb=' || TO_CHAR(inCopyNb) || ' checksumType=' || inCksumName ||
               ' expectedChecksumValue=' || varNSCsumvalue ||
               ' checksumValue=' || TO_CHAR(inCksumValue, 'XXXXXXXX') ||' '|| inLogContext);
      retryOrFailRecall(inCfId, inVID, inReqId, inLogContext);
      RETURN FALSE;
    END IF;
  END IF;
  RETURN TRUE;
EXCEPTION WHEN NO_DATA_FOUND THEN
  -- file got dropped from the namespace, recall should be cancelled
  deleteRecallJobs(inCfId);
  -- potentially terminate repack requests
  archiveOrFailRepackSubreq(inCfId, serrno.ENOENT);
  -- and fail remaining requests
  UPDATE SubRequest
       SET status = dconst.SUBREQUEST_FAILED,
           errorCode = serrno.ENOENT,
           errorMessage = 'File was removed during recall'
     WHERE castorFile = inCfId
       AND status = dconst.SUBREQUEST_WAITTAPERECALL;
  -- log "checkRecallInNS : file was dropped from namespace during recall, giving up"
  logToDLF(inReqId, dlf.LVL_NOTICE, dlf.RECALL_FILE_DROPPED, inFileId, inNsHost, 'tapegatewayd',
           'mountTransactionId=' || TO_CHAR(inMountTransactionId) || ' TPVID=' || inVID ||
           ' fseq=' || TO_CHAR(inFseq) || ' CFLastOpenTime=' || TO_CHAR(inLastOpenTime) || ' ' || inLogContext);
  RETURN FALSE;
END;
/

/* Fix [bug #103299] Missing tapeStatus value after recalls skews GC */

/* update the db after a successful recall */
CREATE OR REPLACE PROCEDURE tg_setFileRecalled(inMountTransactionId IN INTEGER,
                                               inFseq IN INTEGER,
                                               inFilePath IN VARCHAR2,
                                               inCksumName IN VARCHAR2,
                                               inCksumValue IN INTEGER,
                                               inReqId IN VARCHAR2,
                                               inLogContext IN VARCHAR2) AS
  varFileId         INTEGER;
  varNsHost         VARCHAR2(2048);
  varVID            VARCHAR2(2048);
  varCopyNb         INTEGER;
  varSvcClassId     INTEGER;
  varEuid           INTEGER;
  varEgid           INTEGER;
  varLastOpenTime   NUMBER;
  varCfId           INTEGER;
  varFSId           INTEGER;
  varDCPath         VARCHAR2(2048);
  varDcId           INTEGER;
  varFileSize       INTEGER;
  varFileClassId    INTEGER;
  varNbMigrationsStarted INTEGER;
  varGcWeight       NUMBER;
  varGcWeightProc   VARCHAR2(2048);
  varRecallStartTime NUMBER;
BEGIN
  -- get diskserver, filesystem and path from full path in input
  BEGIN
    parsePath(inFilePath, varFSId, varDCPath, varDCId, varFileId, varNsHost);
  EXCEPTION WHEN NO_DATA_FOUND THEN
    -- log "setFileRecalled : unable to parse input path. giving up"
    logToDLF(inReqId, dlf.LVL_ERROR, dlf.RECALL_INVALID_PATH, 0, '', 'tapegatewayd',
             'mountTransactionId=' || TO_CHAR(inMountTransactionId) || ' TPVID=' || varVID ||
             ' fseq=' || TO_CHAR(inFseq) || ' filePath=' || inFilePath || ' ' || inLogContext);
    RETURN;
  END;

  -- first lock Castorfile, check NS and parse path
  -- Get RecallJob and lock Castorfile
  BEGIN
    SELECT CastorFile.id, CastorFile.fileId, CastorFile.nsHost, CastorFile.nsOpenTime,
           CastorFile.fileSize, CastorFile.fileClass, RecallMount.VID, RecallJob.copyNb,
           RecallJob.euid, RecallJob.egid
      INTO varCfId, varFileId, varNsHost, varLastOpenTime, varFileSize, varFileClassId, varVID,
           varCopyNb, varEuid, varEgid
      FROM RecallMount, RecallJob, CastorFile
     WHERE RecallMount.mountTransactionId = inMountTransactionId
       AND RecallJob.vid = RecallMount.vid
       AND RecallJob.fseq = inFseq
       AND RecallJob.status = tconst.RECALLJOB_SELECTED
       AND RecallJob.castorFile = CastorFile.id
       AND ROWNUM < 2
       FOR UPDATE OF CastorFile.id;
    -- the ROWNUM < 2 clause is worth a comment here :
    -- this select will select a single CastorFile and RecallMount, but may select
    -- several RecallJobs "linked" to them. All these recall jobs have the same copyNb
    -- but different uid/gid. They exist because these different uid/gid are attached
    -- to different recallGroups.
    -- In case of several recallJobs present, they are all equally responsible for the
    -- recall, thus we pick the first one as "the" responsible. The only consequence is
    -- that it's uid/gid will be used for the DiskCopy creation
  EXCEPTION WHEN NO_DATA_FOUND THEN
    -- log "setFileRecalled : unable to identify Recall. giving up"
    logToDLF(inReqId, dlf.LVL_ERROR, dlf.RECALL_NOT_FOUND, varFileId, varNsHost, 'tapegatewayd',
             'mountTransactionId=' || TO_CHAR(inMountTransactionId) ||
             ' fseq=' || TO_CHAR(inFseq) || ' filePath=' || inFilePath || ' ' || inLogContext);
    RETURN;
  END;

  -- Deal with the DiskCopy: it is created now as the recall is effectively over. The subsequent
  -- check in the NS may make it INVALID, which is fine as opposed to forget about it and generating dark data.

  -- compute GC weight of the recalled diskcopy
  -- first get the svcClass
  SELECT Diskpool2SvcClass.child INTO varSvcClassId
    FROM Diskpool2SvcClass, FileSystem
   WHERE FileSystem.id = varFSId
     AND Diskpool2SvcClass.parent = FileSystem.diskPool
     AND ROWNUM < 2;
  -- Again, the ROWNUM < 2 is worth a comment : the diskpool may be attached
  -- to several svcClasses. However, we do not support that these different
  -- SvcClasses have different GC policies (actually the GC policy should be
  -- moved to the DiskPool table in the future). Thus it is safe to take any
  -- SvcClass from the list
  varGcWeightProc := castorGC.getRecallWeight(varSvcClassId);
  EXECUTE IMMEDIATE 'BEGIN :newGcw := ' || varGcWeightProc || '(:size); END;'
    USING OUT varGcWeight, IN varFileSize;
  -- create the DiskCopy, after getting how many copies on tape we have, for the importance number
  DECLARE
    varNbCopiesOnTape INTEGER;
  BEGIN
    SELECT nbCopies INTO varNbCopiesOnTape FROM FileClass WHERE id = varFileClassId;
    INSERT INTO DiskCopy (path, gcWeight, creationTime, lastAccessTime, diskCopySize, nbCopyAccesses,
                          ownerUid, ownerGid, id, gcType, fileSystem, castorFile, status, importance)
    VALUES (varDCPath, varGcWeight, getTime(), getTime(), varFileSize, 0,
            varEuid, varEgid, varDCId, NULL, varFSId, varCfId, dconst.DISKCOPY_VALID,
            -1-varNbCopiesOnTape*100);
  END;

  -- Check that the file is still there in the namespace (and did not get overwritten)
  -- Note that error handling and logging is done inside the function
  IF NOT checkRecallInNS(varCfId, inMountTransactionId, varVID, varCopyNb, inFseq, varFileId, varNsHost,
                         inCksumName, inCksumValue, varLastOpenTime, inReqId, inLogContext) THEN
    RETURN;
  END IF;

  -- Then deal with recalljobs and potential migrationJobs
  -- Find out starting time of oldest recall for logging purposes
  SELECT MIN(creationTime) INTO varRecallStartTime FROM RecallJob WHERE castorFile = varCfId;
  -- Delete recall jobs
  DELETE FROM RecallJob WHERE castorFile = varCfId;
  -- trigger waiting migrations if any
  -- Note that we reset the creation time as if the MigrationJob was created right now
  -- this is because "creationTime" is actually the time of entering the "PENDING" state
  -- in the cases where the migrationJob went through a WAITINGONRECALL state
  UPDATE /*+ INDEX_RS_ASC (MigrationJob I_MigrationJob_CFVID) */ MigrationJob
     SET status = tconst.MIGRATIONJOB_PENDING,
         creationTime = getTime()
   WHERE status = tconst.MIGRATIONJOB_WAITINGONRECALL
     AND castorFile = varCfId;
  varNbMigrationsStarted := SQL%ROWCOUNT;
  -- in case there are migrations, update CastorFile's tapeStatus to NOTONTAPE, otherwise it is ONTAPE
  UPDATE CastorFile
     SET tapeStatus = CASE varNbMigrationsStarted
                        WHEN 0
                        THEN dconst.CASTORFILE_ONTAPE
                        ELSE dconst.CASTORFILE_NOTONTAPE
                      END
   WHERE id = varCfId;

  -- Finally deal with user requests
  UPDATE SubRequest
     SET status = decode(reqType,
                         119, dconst.SUBREQUEST_REPACK, -- repack case
                         dconst.SUBREQUEST_RESTART),    -- standard case
         getNextStatus = dconst.GETNEXTSTATUS_FILESTAGED,
         lastModificationTime = getTime()
   WHERE castorFile = varCfId
     AND status = dconst.SUBREQUEST_WAITTAPERECALL;

  -- trigger the creation of additional copies of the file, if necessary.
  replicateOnClose(varCfId, varEuid, varEgid);

  -- log success
  logToDLF(inReqId, dlf.LVL_SYSTEM, dlf.RECALL_COMPLETED_DB, varFileId, varNsHost, 'tapegatewayd',
           'mountTransactionId=' || TO_CHAR(inMountTransactionId) || ' TPVID=' || varVID ||
           ' fseq=' || TO_CHAR(inFseq) || ' filePath=' || inFilePath || ' recallTime=' ||
           to_char(trunc(getTime() - varRecallStartTime, 0)) || ' ' || inLogContext);
END;
/

/* clean up all CastorFile with NULL tape Status, generated by the previous bug */

/* first create a dedicated index to speed up the job */
CREATE INDEX I_CF_tapeStatus_null ON CastorFile (decode(nvl(tapeStatus, -1), -1, -1, NULL)) ONLINE;

/* the real cleanup */
DECLARE
  varUnused NUMBER;
BEGIN
  FOR f IN (SELECT /*+ INDEX(f I_CF_tapeStatus_null) */ fileid, id
              FROM CastorFile
             WHERE decode(nvl(tapeStatus, -1), -1, -1, NULL) = -1) LOOP
    BEGIN
      -- take the lock on the current CastorFile
      SELECT id INTO varUnused FROM CastorFile WHERE id = f.id AND tapeStatus IS NULL FOR UPDATE;
      -- check that the file has a segment
      SELECT s_fileid INTO varUnused FROM Cns_Seg_MetaData@remotens WHERE s_fileid = f.fileid AND ROWNUM < 2;
      -- fix the status to ONTAPE now that we are sure that we have a segment
      UPDATE CastorFile
         SET tapeStatus = dconst.CASTORFILE_ONTAPE
       WHERE id = f.id;
      COMMIT;
    EXCEPTION WHEN NO_DATA_FOUND THEN
      -- CastorFile has disappeared in the meantime or the file has no segment, meaning
      -- it is being created (cannot be DiskOnly as previous bug is RECALL related).
      -- Even in case of a second bug that would make the previous statement wrong,
      -- we do not take any risk and we do not touch these files.
      -- Conclusion : nothing to do here.
      NULL;
    END;
  END LOOP;
END;
/

/* cleanup temporary index */
DROP INDEX I_CF_tapeStatus_null;

/* reactivate garbage collection by revalidating the original selectFiles2Delete procedure */

/* PL/SQL method implementing selectFiles2Delete
   This is the standard garbage collector: it sorts VALID diskcopies
   that do not need to go to tape by gcWeight and selects them for deletion up to
   the desired free space watermark */
CREATE OR REPLACE
PROCEDURE selectFiles2Delete(diskServerName IN VARCHAR2,
                             files OUT castorGC.SelectFiles2DeleteLine_Cur) AS
  dcIds "numList";
  freed INTEGER;
  deltaFree INTEGER;
  toBeFreed INTEGER;
  dontGC INTEGER;
  totalCount INTEGER;
  unused INTEGER;
  backoff INTEGER;
  CastorFileLocked EXCEPTION;
  PRAGMA EXCEPTION_INIT (CastorFileLocked, -54);
BEGIN
  -- First of all, check if we are in a Disk1 pool
  dontGC := 0;
  FOR sc IN (SELECT disk1Behavior
               FROM SvcClass, DiskPool2SvcClass D2S, DiskServer, FileSystem
              WHERE SvcClass.id = D2S.child
                AND D2S.parent = FileSystem.diskPool
                AND FileSystem.diskServer = DiskServer.id
                AND DiskServer.name = diskServerName) LOOP
    -- If any of the service classes to which we belong (normally a single one)
    -- say this is Disk1, we don't GC files.
    IF sc.disk1Behavior = 1 THEN
      dontGC := 1;
      EXIT;
    END IF;
  END LOOP;

  -- Loop on all concerned fileSystems in a random order.
  totalCount := 0;
  FOR fs IN (SELECT FileSystem.id
               FROM FileSystem, DiskServer
              WHERE FileSystem.diskServer = DiskServer.id
                AND DiskServer.name = diskServerName
             ORDER BY dbms_random.value) LOOP

    -- Count the number of diskcopies on this filesystem that are in a
    -- BEINGDELETED state. These need to be reselected in any case.
    freed := 0;
    SELECT totalCount + count(*), nvl(sum(DiskCopy.diskCopySize), 0)
      INTO totalCount, freed
      FROM DiskCopy
     WHERE DiskCopy.fileSystem = fs.id
       AND decode(status, 9, status, NULL) = 9;  -- BEINGDELETED (decode used to use function-based index)

    -- estimate the number of GC running the "long" query, that is the one dealing with the GCing of
    -- VALID files.
    SELECT COUNT(*) INTO backoff
      FROM v$session s, v$sqltext t
     WHERE s.sql_id = t.sql_id AND t.sql_text LIKE '%I_DiskCopy_FS_GCW%';

    -- Process diskcopies that are in an INVALID state.
    UPDATE /*+ INDEX_RS_ASC(DiskCopy I_DiskCopy_Status_7_FS)) */ DiskCopy
       SET status = 9, -- BEINGDELETED
           gcType = decode(gcType, NULL, dconst.GCTYPE_USER, gcType)
     WHERE fileSystem = fs.id
       AND decode(status, 7, status, NULL) = 7  -- INVALID (decode used to use function-based index)
       AND rownum <= 10000 - totalCount
    RETURNING id BULK COLLECT INTO dcIds;
    COMMIT;

    -- If we have more than 10,000 files to GC, exit the loop. There is no point
    -- processing more as the maximum sent back to the client in one call is
    -- 10,000. This protects the garbage collector from being overwhelmed with
    -- requests and reduces the stager DB load. Furthermore, if too much data is
    -- sent back to the client, the transfer time between the stager and client
    -- becomes very long and the message may timeout or may not even fit in the
    -- clients receive buffer!
    totalCount := totalCount + dcIds.COUNT();
    EXIT WHEN totalCount >= 10000;

    -- Continue processing but with VALID files, only in case we are not already loaded
    IF dontGC = 0 AND backoff < 4 THEN
      -- Do not delete VALID files from non production hardware
      BEGIN
        SELECT FileSystem.id INTO unused
          FROM DiskServer, FileSystem
         WHERE FileSystem.id = fs.id
           AND FileSystem.status IN (dconst.FILESYSTEM_PRODUCTION, dconst.FILESYSTEM_READONLY)
           AND FileSystem.diskserver = DiskServer.id
           AND DiskServer.status IN (dconst.DISKSERVER_PRODUCTION, dconst.DISKSERVER_READONLY)
           AND DiskServer.hwOnline = 1;
      EXCEPTION WHEN NO_DATA_FOUND THEN
        EXIT;
      END;
      -- Calculate the amount of space that would be freed on the filesystem
      -- if the files selected above were to be deleted.
      IF dcIds.COUNT > 0 THEN
        SELECT /*+ INDEX(DiskCopy PK_DiskCopy_Id) */ freed + sum(diskCopySize) INTO freed
          FROM DiskCopy
         WHERE DiskCopy.id IN
             (SELECT /*+ CARDINALITY(fsidTable 5) */ *
                FROM TABLE(dcIds) dcidTable);
      END IF;
      -- Get the amount of space to be liberated
      SELECT decode(sign(maxFreeSpace * totalSize - free), -1, 0, maxFreeSpace * totalSize - free)
        INTO toBeFreed
        FROM FileSystem
       WHERE id = fs.id;
      -- If space is still required even after removal of INVALID files, consider
      -- removing VALID files until we are below the free space watermark
      IF freed < toBeFreed THEN
        -- Loop on file deletions
        FOR dc IN (SELECT /*+ INDEX_RS_ASC(DiskCopy I_DiskCopy_FS_GCW) */ DiskCopy.id, castorFile
                     FROM DiskCopy, CastorFile
                    WHERE fileSystem = fs.id
                      AND status = dconst.DISKCOPY_VALID
                      AND CastorFile.id = DiskCopy.castorFile
                      AND CastorFile.tapeStatus IN (dconst.CASTORFILE_DISKONLY, dconst.CASTORFILE_ONTAPE)
                      ORDER BY gcWeight ASC) LOOP
          BEGIN
            -- Lock the CastorFile
            SELECT id INTO unused FROM CastorFile
             WHERE id = dc.castorFile FOR UPDATE NOWAIT;
            -- Mark the DiskCopy as being deleted
            UPDATE DiskCopy
               SET status = dconst.DISKCOPY_BEINGDELETED,
                   gcType = dconst.GCTYPE_AUTO
             WHERE id = dc.id RETURNING diskCopySize INTO deltaFree;
            totalCount := totalCount + 1;
            -- Update freed space
            freed := freed + deltaFree;
            -- update importance of remianing copies of the file if any
            UPDATE DiskCopy
               SET importance = importance + 1
             WHERE castorFile = dc.castorFile
               AND status = dconst.DISKCOPY_VALID;
            -- Shall we continue ?
            IF toBeFreed <= freed THEN
              EXIT;
            END IF;
            IF totalCount >= 10000 THEN
              EXIT;
            END IF;           
          EXCEPTION
            WHEN NO_DATA_FOUND THEN
              -- The file no longer exists or has the wrong state
              NULL;
            WHEN CastorFileLocked THEN
              -- Go to the next candidate, processing is taking place on the
              -- file
              NULL;
          END;
          COMMIT;
        END LOOP;
      END IF;
    END IF;
    -- We have enough files to exit the loop ?
    EXIT WHEN totalCount >= 10000;
  END LOOP;

  -- Now select all the BEINGDELETED diskcopies in this diskserver for the GC daemon
  OPEN files FOR
    SELECT /*+ INDEX(CastorFile PK_CastorFile_ID) */ FileSystem.mountPoint || DiskCopy.path,
           DiskCopy.id,
           Castorfile.fileid, Castorfile.nshost,
           DiskCopy.lastAccessTime, DiskCopy.nbCopyAccesses, DiskCopy.gcWeight,
           getObjStatusName('DiskCopy', 'gcType', DiskCopy.gcType),
          getSvcClassList(FileSystem.id)
      FROM CastorFile, DiskCopy, FileSystem, DiskServer
     WHERE decode(DiskCopy.status, 9, DiskCopy.status, NULL) = 9 -- BEINGDELETED
       AND DiskCopy.castorfile = CastorFile.id
       AND DiskCopy.fileSystem = FileSystem.id
       AND FileSystem.diskServer = DiskServer.id
       AND DiskServer.name = diskServerName
       AND rownum <= 10000;
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
           ' dataTomove=' || TO_CHAR(inDataAmount));
  -- Loop on candidates until we can lock one
  OPEN DCcur;
  LOOP
    -- Fetch next candidate
    FETCH DCcur INTO varDcId, varDcSize, varCfId, varNsOpenTime;
    varTotalRebalanced := varTotalRebalanced + varDcSize;
    varNbFilesRebalanced := varNbFilesRebalanced + 1;
    -- stop if it would be too much
    IF varTotalRebalanced > inDataAmount THEN EXIT; END IF;
    -- create disk2DiskCopyJob for this diskCopy
    createDisk2DiskCopyJob(varCfId, varNsOpenTime, inDestSvcClassId,
                           0, 0, dconst.REPLICATIONTYPE_REBALANCE,
                           varDcId, NULL, FALSE);
  END LOOP;
  CLOSE DCcur;
  -- "rebalancing : stopping" message
  logToDLF(NULL, dlf.LVL_SYSTEM, dlf.REBALANCING_STOP, 0, '', 'stagerd',
           'DiskServer=' || inDiskServerName || ' mountPoint=' || inMountPoint ||
           ' dataMoveTriggered=' || TO_CHAR(varTotalRebalanced) ||
           ' nbFileMovesTriggered=' || TO_CHAR(varNbFilesRebalanced));
END;
/

/* handle the creation of the Disk2DiskCopyJobs for the running drainingJobs */
CREATE OR REPLACE PROCEDURE drainRunner AS
  varNbFiles INTEGER;
  varNbBytes INTEGER;
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
      -- lock the draining Job first
      SELECT id INTO varUnused FROM DrainingJob WHERE id = dj.id FOR UPDATE;
      -- check how many disk2DiskCopyJobs are already running for this draining job
      SELECT count(*) INTO varNbRunningJobs FROM Disk2DiskCopyJob WHERE drainingJob = dj.id;
      -- Loop over the creation of Disk2DiskCopyJobs. Select max 1000 files, taking running
      -- ones into account. Also take the most important jobs first
      logToDLF(NULL, dlf.LVL_SYSTEM, dlf.DRAINING_REFILL, 0, '', 'stagerd',
               'svcClass=' || getSvcClassName(dj.svcClass) || ' DrainReq=' ||
               TO_CHAR(dj.id) || ' MaxNewJobsCount=' || TO_CHAR(varMaxNbOfSchedD2dPerDrain-varNbRunningJobs));
      varNbFiles := 0;
      varNbBytes := 0;
      FOR F IN (SELECT * FROM
                 (SELECT CastorFile.id cfId, Castorfile.nsOpenTime, DiskCopy.id dcId, CastorFile.fileSize
                    FROM DiskCopy, CastorFile
                   WHERE DiskCopy.fileSystem = dj.fileSystem
                     AND CastorFile.id = DiskCopy.castorFile
                     AND ((dj.fileMask = dconst.DRAIN_FILEMASK_NOTONTAPE AND
                           CastorFile.tapeStatus IN (dconst.CASTORFILE_NOTONTAPE, dconst.CASTORFILE_DISKONLY)) OR
                          (dj.fileMask = dconst.DRAIN_FILEMASK_ALL))
                     AND DiskCopy.status = dconst.DISKCOPY_VALID
                     AND NOT EXISTS (SELECT 1 FROM Disk2DiskCopyJob WHERE castorFile=CastorFile.id)
                   ORDER BY DiskCopy.importance DESC)
                 WHERE ROWNUM <= varMaxNbOfSchedD2dPerDrain-varNbRunningJobs) LOOP
        createDisk2DiskCopyJob(F.cfId, F.nsOpenTime, dj.svcClass, dj.euid, dj.egid,
                               dconst.REPLICATIONTYPE_DRAINING, F.dcId, dj.id, FALSE);
        varNbFiles := varNbFiles + 1;
        varNbBytes := varNbBytes + F.fileSize;
      END LOOP;
      -- commit and update counters
      UPDATE DrainingJob
         SET totalFiles = totalFiles + varNbFiles,
             totalBytes = totalBytes + varNbBytes,
             lastModificationTime = getTime()
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


/* Recompile all procedures, triggers and functions */
BEGIN
  recompileAll();
END;
/

/* Flag the schema upgrade as COMPLETE */
/***************************************/
UPDATE UpgradeLog SET endDate = sysdate, state = 'COMPLETE'
 WHERE release = '2_1_14_5';
COMMIT;
