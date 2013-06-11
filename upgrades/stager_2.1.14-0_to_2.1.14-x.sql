/***** TO BE MERGED INTO stager_2.1.13-6_to_2.1.14-0.sql *****/

INSERT INTO UpgradeLog (schemaVersion, release, type)
VALUES ('2_1_14_0', '2_1_14_X', 'NON TRANSPARENT');
COMMIT;

/* For deleteDiskCopy */
CREATE GLOBAL TEMPORARY TABLE DeleteDiskCopyHelper
  (dcId INTEGER CONSTRAINT PK_DDCHelper_dcId PRIMARY KEY, rc INTEGER)
  ON COMMIT PRESERVE ROWS;


ALTER TABLE CastorFile ADD (tapeStatus INTEGER, nsOpenTime NUMBER);
DELETE FROM ObjStatus WHERE object='DiskCopy' AND field='status'
                        AND statusName IN ('DISKCOPY_STAGED', 'DISKCOPY_CANBEMIGR');
BEGIN setObjStatusName('DiskCopy', 'status', 0, 'DISKCOPY_VALID'); END;
/


-- temporary function-based index to speed up the following update
CREATE INDEX I_CF_OpenTimeNull
    ON CastorFile(decode(nvl(nsOpenTime, -1), -1, 1, NULL));

DECLARE
  remCF INTEGER;
  CURSOR cfs IS
    SELECT id FROM CastorFile WHERE decode(nvl(nsOpenTime, -1), -1, 1, NULL) = 1;
  ids "numList";
BEGIN
  FOR cf IN (SELECT unique castorFile, status, nbCopies
               FROM DiskCopy, CastorFile, FileClass
              WHERE DiskCopy.castorFile = CastorFile.id
                AND CastorFile.fileClass = FileClass.id
                AND DiskCopy.status IN (0, 10)) LOOP
    UPDATE CastorFile
       SET tapeStatus = DECODE(cf.status,
                               10, dconst.CASTORFILE_NOTONTAPE,
                               DECODE(cf.nbCopies,
                                      0, dconst.CASTORFILE_DISKONLY,
                                      dconst.CASTORFILE_ONTAPE))
     WHERE id = cf.castorFile;
  END LOOP;
  COMMIT;
  -- Update all nsOpenTime values
  LOOP
    OPEN cfs;
    FETCH cfs BULK COLLECT INTO ids LIMIT 1000;
    EXIT WHEN ids.count = 0;
    FORALL i IN 1 .. ids.COUNT
      UPDATE CastorFile SET nsOpenTime = lastUpdateTime WHERE id = ids(i);
    CLOSE cfs;
    COMMIT;
  END LOOP;

  SELECT COUNT(*) INTO remCF FROM CastorFile WHERE tapestatus IS NULL;

XXX cleanup files with no diskcopy ???

  IF remCF > 0 THEN
    raise_application_error(-20001, 'Found ' || TO_CHAR(remCF) || ' CastorFile with no DiskCopy !');
  END IF;
END;
/

ALTER TABLE CastorFile MODIFY (nsOpenTime CONSTRAINT NN_CastorFile_NsOpenTime NOT NULL);


ALTER TABLE StageRepackRequest ADD (fileCount INTEGER, totalSize INTEGER);
UPDATE StageRepackRequest SET fileCount = 0, totalSize = 0;  -- XXX do we need to recompute those figures? probably not...
ALTER TABLE StageRepackRequest MODIFY (fileCount CONSTRAINT NN_StageRepackReq_fileCount NOT NULL, totalSize CONSTRAINT NN_StageRepackReq_totalSize NOT NULL, status CONSTRAINT NN_StageRepackReq_status NOT NULL, repackVid CONSTRAINT NN_StageRepackReq_repackVid NOT NULL);

DROP TRIGGER tr_DiskCopy_Online;
DROP PROCEDURE getDiskCopiesForJob;
DROP PROCEDURE processPrepareRequest;
DROP PROCEDURE recreateCastorFile;

/* bug #101714: RFE: VMGR DB should provide tape statuses independently of table definitions */
/* For Vmgr_tape_status_view */
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
        -- remember the copy number and tape
        varAllCopyNbs.EXTEND;
        varAllCopyNbs(varI) := varSeg.copyno;
        varAllVIDs.EXTEND;
        varAllVIDs(varI) := varSeg.vid;
        varI := varI + 1;
        -- Is the segment on a valid tape from recall point of view ?
        IF BITAND(varSeg.tapeStatus, tconst.TAPE_DISABLED) = 0 AND
           BITAND(varSeg.tapeStatus, tconst.TAPE_EXPORTED) = 0 AND
           BITAND(varSeg.tapeStatus, tconst.TAPE_ARCHIVED) = 0 THEN
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

UPDATE UpgradeLog SET endDate = systimestamp, state = 'COMPLETE'
 WHERE release = '2_1_14_X';
COMMIT;
