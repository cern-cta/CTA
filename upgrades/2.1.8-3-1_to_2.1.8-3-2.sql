/******************************************************************************
 *              2.1.8-3-1_to_2.1.8-3-2.sql
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
 * @(#)$RCSfile: 2.1.8-3-1_to_2.1.8-3-2.sql,v $ $Release: 1.2 $ $Release$ $Date: 2008/11/26 12:48:41 $ $Author: sponcec3 $
 *
 * This script upgrades a CASTOR v2.1.8-3-1 STAGER database into v2.1.8-3-2
 *
 * Fixes:
 * - #44170: inputForMigrationPolicy procedure requires extra optimizer hint 
 *           to disable bad index access
 * - #44458: accounting deadlock
 * 
 * - Fixed a bug in the PL/SQL logic that deals with file queries by filename
 *   that logs the following error in the stager log file:
 *
 *   Error caught in diskCopies4File. ORA-06502: PL/SQL: numeric or value error: 
 *   NULL index table key value ORA-06512: at "CASTOR_STAGER.FILEIDSTAGEQUERY"
 *
 *   when the file does not exist on the disk cache. As a result of this error
 *   the client waits for a reply from the stager which will never be sent.
 *   Now the stager correctly replies with ENOENT (No such file or directory)
 *
 * - Added a missing commit in the rtcpclientd cleanup logic which is executed
 *   when rtcplientd starts up.
 *
 * - Fixed a rare race condition between the garbage collector and the stager,
 *   which could allow the creation of disk copies and subrequests which are 
 *   orphaned from their castorfile. Now proper locks are taken to make sure 
 *   that this can no longer happen.
 *
 * - Added support for overriding the nshost field of the castorfile table
 *   using the stager/nsHost configuration option defined in the CastorConfig 
 *   database table.
 *
 * - Fixed a bug in the deleteCastorFile procedure which could allow a castorfile
 *   entry to be removed from the Stager database despite the fact that it maybe
 *   still linked to an ongoing repack subrequest.
 *
 * - Fixed a bug in the cleanup logic which did not remove castorfile entries from
 *   the Stager database once all entries pointing to the file had been deleted.
 *
 * - Modified all dbms_scheduler jobs to be associated with a specific DB
 *   job_class. Prior to this modification it was possible for RAC based DB
 *   deployment models to have all daemons connected to one node of the RAC
 *   while jobs executed on another. As a consequence of this, block coping
 *   between the nodes of the RAC could cause performance related problems
 *
 *   Note: Prior to running this hotfix a DBA must first create a job_class for
 *         CASTOR. An example of how to do this can be found below:
 *
 *   BEGIN
 *     DBMS_SCHEDULER.CREATE_JOB_CLASS(job_class_name => 'CASTOR_JOB_CLASS',
 *                                     resource_consumer_group => null,
 *                                     service => '&castor_service_name',
 *                                     logging_level => DBMS_SCHEDULER.LOGGING_RUNS,
 *                                     log_history => null,
 *                                     comments => 'Castor job class');
 *  END;
 *  /
 *
 *  GRANT EXECUTE ON castor_job_class TO &user_to_run_jobs;
 *
 * *** WARNING ***
 *
 *   This hotfix requires stopping the instance. The anticipated downtime is
 *   less than 20 minutes.
 * 
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

/* Stop on errors - this only works from sqlplus */
WHENEVER SQLERROR EXIT FAILURE;

/* Version cross check and update */
DECLARE
  unused VARCHAR(100);
BEGIN
  SELECT release INTO unused FROM CastorVersion WHERE release = '2_1_8_3_1';
EXCEPTION WHEN NO_DATA_FOUND THEN
  -- Error, we can't apply this script
  raise_application_error(-20000, 'PL/SQL release mismatch. Please run previous upgrade scripts before this one.');
END;
/

UPDATE CastorVersion SET release = '2_1_8_3_2';
COMMIT;

/* Job management */
BEGIN
  FOR a IN (SELECT * FROM user_scheduler_jobs)
  LOOP
    -- Stop any running jobs
    IF a.state = 'RUNNING' THEN
      dbms_scheduler.stop_job(a.job_name);
    END IF;
    -- Schedule the start date of the job to 20 minutes from now. This
    -- basically pauses the job for 20 minutes so that the upgrade can
    -- go through as quickly as possible.
    dbms_scheduler.set_attribute(a.job_name, 'START_DATE', SYSDATE + 20/1440);
  END LOOP;
END;
/

/* Update all lastUpdateTime of 10s to avoid falling into the bug
 * of the nameserver taking the request arrival time instead of
 * the file closing time as lastUpdateTime
 */
UPDATE Castorfile SET lastUpdateTime = lastUpdateTime + 10 WHERE lastUpdatetime > 0;
COMMIT;

/* Recreate accounting info, based on diskpools */ 
DROP TABLE Accounting; 
CREATE TABLE Accounting (euid INTEGER, diskPool INTEGER, nbBytes INTEGER);

/* Fill the accounting table, before the triggers come in */ 
INSERT INTO Accounting (
  SELECT owneruid, diskpool, sum(diskCopySize)
    FROM DiskCopy, FileSystem
   WHERE DiskCopy.fileSystem = FileSystem.id
     AND DiskCopy.status IN (0, 10)
     AND DiskCopy.owneruid IS NOT NULL
     AND DiskCopy.ownergid IS NOT NULL
   GROUP BY owneruid, diskpool);
COMMIT;

/* Trigger used to update the accounting (Quota) information and
 * provide input to the statement level trigger defined above
 */
CREATE OR REPLACE TRIGGER tr_DiskCopy_Online
AFTER UPDATE OF status ON DiskCopy
FOR EACH ROW
WHEN ((new.status = 0 OR new.status = 10) AND -- STAGED, CANBEMIGR
       old.status != 10) -- CANBEMIGR (don't double count migrations)
BEGIN
  -- Update accouting figures
  MERGE INTO Accounting
  USING (SELECT :new.owneruid euid, :new.diskCopySize fileSize, diskPool
           FROM FileSystem
          WHERE FileSystem.id = :new.FileSystem) DC
     ON (Accounting.euid = DC.euid AND
         Accounting.diskPool = DC.diskPool)
   WHEN MATCHED THEN UPDATE SET nbBytes = nbBytes + DC.fileSize
   WHEN NOT MATCHED THEN INSERT (euid, nbBytes, diskPool)
                         VALUES (DC.euid, DC.fileSize, DC.diskPool);

  -- Insert the information about the diskcopy being processed into
  -- the TooManyReplicasHelper. This information will be used later
  -- on the DiskCopy AFTER UPDATE statement level trigger. We cannot
  -- do the work of that trigger here as it would result in
  -- `ORA-04091: table is mutating, trigger/function` errors
  INSERT INTO TooManyReplicasHelper
  VALUES (:new.id, :new.filesystem, :new.castorfile);
END;
/


/* Trigger used to update the accounting (Quota) information */
CREATE OR REPLACE TRIGGER tr_DiskCopy_Offline
AFTER UPDATE OF status ON DiskCopy
FOR EACH ROW
WHEN ((old.status = 0 OR old.status = 10) AND -- STAGED, CANBEMIGR
       new.status != 0) -- STAGED (don't double count migrations)
BEGIN
  -- Update accounting figures
  UPDATE Accounting
     SET nbBytes = nbBytes - :new.diskCopySize
   WHERE Accounting.euid = :new.owneruid
     AND Accounting.diskPool =
         (SELECT diskPool
            FROM FileSystem
           WHERE FileSystem.id = :new.FileSystem);
END;
/


/* Add new option to CastorConfig */
INSERT INTO CastorConfig
  VALUES ('stager', 'nsHost', 'undefined', 'The name of the name server host to set in the CastorFile table overriding the CNS/HOST option defined in castor.conf');


/* Function to extract a configuration option from the castor config
 * table.
 */
CREATE OR REPLACE FUNCTION getConfigOption
(className VARCHAR2, optionName VARCHAR2, defaultValue VARCHAR2) 
RETURN VARCHAR2 IS
  returnValue VARCHAR2(204) := defaultValue;
BEGIN
  SELECT value INTO returnValue
    FROM CastorConfig
   WHERE class = className
     AND key = optionName
     AND value != 'undefined';
  RETURN returnValue;
EXCEPTION WHEN NO_DATA_FOUND THEN
  RETURN returnValue;
END;
/


/* PL/SQL method to delete a CastorFile only when no Disk|TapeCopies are left for it */
/* Internally used in filesDeletedProc, putFailedProc and deleteOutOfDateDiskCopies */
CREATE OR REPLACE PROCEDURE deleteCastorFile(cfId IN NUMBER) AS
  nb NUMBER;
  LockError EXCEPTION;
  PRAGMA EXCEPTION_INIT (LockError, -54);
BEGIN
  -- First try to lock the castorFile
  SELECT id INTO nb FROM CastorFile
   WHERE id = cfId FOR UPDATE NOWAIT;
  -- See whether it has any DiskCopy
  SELECT count(*) INTO nb FROM DiskCopy
   WHERE castorFile = cfId;
  -- If any DiskCopy, give up
  IF nb = 0 THEN
    -- See whether it has any TapeCopy
    SELECT count(*) INTO nb FROM TapeCopy
     WHERE castorFile = cfId AND status != 6; -- FAILED
    -- If any TapeCopy, give up
    IF nb = 0 THEN
      -- See whether pending SubRequests exist
      SELECT count(*) INTO nb FROM SubRequest
       WHERE castorFile = cfId
         AND status IN (0, 1, 2, 3, 4, 5, 6, 7, 10, 12, 13, 14);   -- All but FINISHED, FAILED_FINISHED, ARCHIVED
      -- If any SubRequest, give up
      IF nb = 0 THEN
        DECLARE
          fid NUMBER;
          fc NUMBER;
          nsh VARCHAR2(2048);
        BEGIN
          -- Delete the failed TapeCopies
          deleteTapeCopies(cfId);
          -- Delete the CastorFile
          DELETE FROM id2Type WHERE id = cfId;
          DELETE FROM CastorFile WHERE id = cfId
            RETURNING fileId, nsHost, fileClass
            INTO fid, nsh, fc;
          -- check whether this file potentially had TapeCopies
          SELECT nbCopies INTO nb FROM FileClass WHERE id = fc;
          IF nb = 0 THEN
            -- This castorfile was created with no TapeCopy
            -- So removing it from the stager means erasing
            -- it completely. We should thus also remove it
            -- from the name server
            INSERT INTO FilesDeletedProcOutput VALUES (fid, nsh);
          END IF;
        END;
      END IF;
    END IF;
  END IF;
EXCEPTION WHEN NO_DATA_FOUND THEN
  -- ignore, this means that the castorFile did not exist.
  -- There is thus no way to find out whether to remove the
  -- file from the nameserver. For safety, we thus keep it
  NULL;
WHEN LockError THEN
  -- ignore, somebody else is dealing with this castorFile, 
  NULL;
END;
/


/* PL/SQL method implementing recreateCastorFile */
CREATE OR REPLACE PROCEDURE recreateCastorFile(cfId IN INTEGER,
                                               srId IN INTEGER,
                                               dcId OUT INTEGER,
                                               rstatus OUT INTEGER,
                                               rmountPoint OUT VARCHAR2,
                                               rdiskServer OUT VARCHAR2) AS
  rpath VARCHAR2(2048);
  nbRes INTEGER;
  fid INTEGER;
  nh VARCHAR2(2048);
  fclassId INTEGER;
  sclassId INTEGER;
  putSC INTEGER;
  pputSC INTEGER;
  contextPIPP INTEGER;
  ouid INTEGER;
  ogid INTEGER;
BEGIN
  -- Get data and lock access to the CastorFile
  -- This, together with triggers will avoid new TapeCopies
  -- or DiskCopies to be added
  SELECT fileclass INTO fclassId FROM CastorFile WHERE id = cfId FOR UPDATE;
  -- Determine the context (Put inside PrepareToPut ?)
  BEGIN
    -- check that we are a Put/Update
    SELECT Request.svcClass INTO putSC
      FROM (SELECT id, svcClass FROM StagePutRequest UNION ALL
            SELECT id, svcClass FROM StageUpdateRequest) Request, SubRequest
     WHERE SubRequest.id = srId
       AND Request.id = SubRequest.request;
    BEGIN
      -- check that there is a PrepareToPut/Update going on. There can be only a single one
      -- or none. If there was a PrepareTo, any subsequent PPut would be rejected and any
      -- subsequent PUpdate would be directly archived (cf. processPrepareRequest).
      SELECT SubRequest.diskCopy, PrepareRequest.svcClass INTO dcId, pputSC
        FROM (SELECT id, svcClass FROM StagePrepareToPutRequest UNION ALL
              SELECT id, svcClass FROM StagePrepareToUpdateRequest) PrepareRequest, SubRequest
       WHERE SubRequest.CastorFile = cfId
         AND PrepareRequest.id = SubRequest.request
         AND SubRequest.status = 6;  -- READY
      -- if we got here, we are a Put/Update inside a PrepareToPut
      -- however, are we in the same service class ?
      IF putSC != pputSC THEN
        -- No, this means we are a Put/Update and another PrepareToPut
        -- is already running in a different service class. This is forbidden
        dcId := 0;
        UPDATE SubRequest
           SET status = 7, -- FAILED
               errorCode = 16, -- EBUSY
               errorMessage = 'A prepareToPut is running in another service class for this file'
         WHERE id = srId;
        COMMIT;
        RETURN;
      END IF;
      -- if we got here, we are a Put/Update inside a PrepareToPut
      -- both running in the same service class
      contextPIPP := 1;
    EXCEPTION WHEN NO_DATA_FOUND THEN
      -- if we got here, we are a standalone Put/Update
      contextPIPP := 0;
    END;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    DECLARE
      nbPReqs NUMBER;
    BEGIN
      -- we are either a prepareToPut, or a prepareToUpdate and it's the only one (file is being created).
      -- In case of prepareToPut we need to check that we are we the only one
      SELECT count(SubRequest.diskCopy) INTO nbPReqs
        FROM (SELECT id FROM StagePrepareToPutRequest UNION ALL
              SELECT id FROM StagePrepareToUpdateRequest) PrepareRequest, SubRequest
       WHERE SubRequest.castorFile = cfId
         AND PrepareRequest.id = SubRequest.request
         AND SubRequest.status = 6;  -- READY
      -- Note that we did not select ourselves (we are in status 3)
      IF nbPReqs > 0 THEN
        -- this means we are a PrepareToPut and another PrepareToPut/Update
        -- is already running. This is forbidden
        dcId := 0;
        UPDATE SubRequest
           SET status = 7, -- FAILED
               errorCode = 16, -- EBUSY
               errorMessage = 'Another prepareToPut/Update is ongoing for this file'
         WHERE id = srId;
        COMMIT;
        RETURN;
      END IF;
      -- Everything is ok then
      contextPIPP := 0;
    END;
  END;
  IF contextPIPP = 0 THEN
    -- check if there is space in the diskpool in case of
    -- disk only pool
    -- get the svcclass and the user
    SELECT svcClass, euid, egid INTO sclassId, ouid, ogid
      FROM Subrequest,
           (SELECT id, svcClass, euid, egid FROM StagePutRequest UNION ALL
            SELECT id, svcClass, euid, egid FROM StageUpdateRequest UNION ALL
            SELECT id, svcClass, euid, egid FROM StagePrepareToPutRequest UNION ALL
            SELECT id, svcClass, euid, egid FROM StagePrepareToUpdateRequest) Request
     WHERE SubRequest.id = srId
       AND Request.id = SubRequest.request;
    IF checkFailJobsWhenNoSpace(sclassId) = 1 THEN
      -- The svcClass is declared disk only and has no space
      -- thus we cannot recreate the file
      dcId := 0;
      UPDATE SubRequest
         SET status = 7, -- FAILED
             errorCode = 28, -- ENOSPC
             errorMessage = 'File creation canceled since diskPool is full'
       WHERE id = srId;
      COMMIT;
      RETURN;
    END IF;
    -- check if the file existed in advance with a fileclass incompatible with this svcClass
    IF checkFailPutWhenTape0(sclassId, fclassId) = 1 THEN
      -- The svcClass is disk only and the file being overwritten asks for tape copy.
      -- This is impossible, so we deny the operation
      dcId := 0;
      UPDATE SubRequest
         SET status = 7, -- FAILED
             errorCode = 22, -- EINVAL
             errorMessage = 'File recreation canceled since this service class doesn''t provide tape backend'
       WHERE id = srId;
      COMMIT;
      RETURN;
    END IF;
    -- check if recreation is possible for TapeCopies
    SELECT count(*) INTO nbRes FROM TapeCopy
     WHERE status = 3 -- TAPECOPY_SELECTED
      AND castorFile = cfId;
    IF nbRes > 0 THEN
      -- We found something, thus we cannot recreate
      dcId := 0;
      UPDATE SubRequest
         SET status = 7, -- FAILED
             errorCode = 16, -- EBUSY
             errorMessage = 'File recreation canceled since file is being migrated'
        WHERE id = srId;
      COMMIT;
      RETURN;
    END IF;
    -- check if recreation is possible for DiskCopies
    SELECT count(*) INTO nbRes FROM DiskCopy
     WHERE status IN (1, 2, 5, 6) -- WAITDISK2DISKCOPY, WAITTAPERECALL, WAITFS, STAGEOUT
       AND castorFile = cfId;
    IF nbRes > 0 THEN
      -- We found something, thus we cannot recreate
      dcId := 0;
      UPDATE SubRequest
         SET status = 7, -- FAILED
             errorCode = 16, -- EBUSY
             errorMessage = 'File recreation canceled since file is either being recalled, or replicated or created by another user'
       WHERE id = srId;
      COMMIT;
      RETURN;
    END IF;
    -- delete all tapeCopies
    deleteTapeCopies(cfId);
    -- set DiskCopies to INVALID
    UPDATE DiskCopy SET status = 7 -- INVALID
     WHERE castorFile = cfId AND status IN (0, 10); -- STAGED, CANBEMIGR
    -- create new DiskCopy
    SELECT fileId, nsHost INTO fid, nh FROM CastorFile WHERE id = cfId;
    SELECT ids_seq.nextval INTO dcId FROM DUAL;
    buildPathFromFileId(fid, nh, dcId, rpath);
    INSERT INTO DiskCopy (path, id, FileSystem, castorFile, status, creationTime, lastAccessTime, gcWeight, diskCopySize, nbCopyAccesses, owneruid, ownergid)
         VALUES (rpath, dcId, 0, cfId, 5, getTime(), getTime(), 0, 0, 0, ouid, ogid); -- status WAITFS
    INSERT INTO Id2Type (id, type) VALUES (dcId, 5); -- OBJ_DiskCopy
    rstatus := 5; -- WAITFS
    rmountPoint := '';
    rdiskServer := '';
  ELSE
    DECLARE
      fsId INTEGER;
      dsId INTEGER;
    BEGIN
      -- Retrieve the infos about the DiskCopy to be used
      SELECT fileSystem, status INTO fsId, rstatus FROM DiskCopy WHERE id = dcId;
      -- retrieve mountpoint and filesystem if any
      IF fsId = 0 THEN
        rmountPoint := '';
        rdiskServer := '';
      ELSE
        SELECT mountPoint, diskServer INTO rmountPoint, dsId
          FROM FileSystem WHERE FileSystem.id = fsId;
        SELECT name INTO rdiskServer FROM DiskServer WHERE id = dsId;
      END IF;
      -- See whether we should wait on another concurrent Put|Update request
      IF rstatus = 11 THEN  -- WAITFS_SCHEDULING
        -- another Put|Update request was faster than us, so we have to wait on it
        -- to be scheduled; we will be restarted at PutStart of that one
        DECLARE
          srParent NUMBER;
        BEGIN
          -- look for it
          SELECT SubRequest.id INTO srParent
            FROM SubRequest, Id2Type
           WHERE request = Id2Type.id
             AND type IN (40, 44)  -- Put, Update
             AND diskCopy = dcId
             AND status IN (13, 14, 6)  -- READYFORSCHED, BEINGSCHED, READY
             AND ROWNUM < 2;   -- if we have more than one just take one of them
          UPDATE SubRequest
             SET status = 5, parent = srParent  -- WAITSUBREQ
           WHERE id = srId;
        EXCEPTION WHEN NO_DATA_FOUND THEN
          -- we didn't find that request: let's assume it failed, we override the status 11
          rstatus := 5;  -- WAITFS
        END;
      ELSE
        -- we are the first, we change status as we are about to go to the scheduler
        UPDATE DiskCopy SET status = 11  -- WAITFS_SCHEDULING
         WHERE castorFile = cfId
           AND status = 5;  -- WAITFS
        -- and we still return 5 = WAITFS to the stager so to go on
      END IF;
    END;
  END IF;
  -- reset svcClass to the request's one and filesize to 0
  -- we are truncating the file and we want to use the new svcClass for migration
  UPDATE CastorFile SET fileSize = 0, svcClass = sclassId
   WHERE id = cfId;
  -- link SubRequest and DiskCopy
  UPDATE SubRequest SET diskCopy = dcId,
                        lastModificationTime = getTime()
   WHERE id = srId;
  -- we don't commit here, the stager will do that when
  -- the subRequest status will be updated to 6
END;
/

/* PL/SQL method implementing prepareForMigration */
CREATE OR REPLACE PROCEDURE prepareForMigration (srId IN INTEGER,
                                                 fs IN INTEGER,
                                                 ts IN NUMBER,
                                                 fId OUT NUMBER,
                                                 nh OUT VARCHAR2,
                                                 userId OUT INTEGER,
                                                 groupId OUT INTEGER,
                                                 errorCode OUT INTEGER) AS
  cfId INTEGER;
  dcId INTEGER;
  fsId INTEGER;
  scId INTEGER;
  realFileSize INTEGER;
  unused INTEGER;
  contextPIPP INTEGER;
BEGIN
  errorCode := 0;
  -- Get CastorFile
  SELECT castorFile, diskCopy INTO cfId, dcId FROM SubRequest WHERE id = srId;
  -- Lock the CastorFile
  SELECT id INTO cfId FROM CastorFile WHERE id = cfId FOR UPDATE;
  -- Determine the context (Put inside PrepareToPut or not)
  -- check that we are a Put or an Update
  SELECT Request.id INTO unused
    FROM SubRequest,
       (SELECT id FROM StagePutRequest UNION ALL
        SELECT id FROM StageUpdateRequest) Request
   WHERE SubRequest.id = srId
     AND Request.id = SubRequest.request;
  BEGIN
    -- Check that there is a PrepareToPut/Update going on. There can be only a single one
    -- or none. If there was a PrepareTo, any subsequent PPut would be rejected and any
    -- subsequent PUpdate would be directly archived (cf. processPrepareRequest).
    SELECT SubRequest.diskCopy INTO unused
      FROM SubRequest,
       (SELECT id FROM StagePrepareToPutRequest UNION ALL
        SELECT id FROM StagePrepareToUpdateRequest) Request
     WHERE SubRequest.CastorFile = cfId
       AND Request.id = SubRequest.request
       AND SubRequest.status = 6; -- READY
    -- if we got here, we are a Put inside a PrepareToPut
    contextPIPP := 0;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    -- here we are a standalone Put
    contextPIPP := 1;
  END;
  -- Check whether the diskCopy is still in STAGEOUT. If not, the file
  -- was deleted via stageRm while being written to. Thus, we should just give up
  BEGIN
    SELECT status INTO unused
      FROM DiskCopy WHERE id = dcId AND status = 6; -- STAGEOUT
  EXCEPTION WHEN NO_DATA_FOUND THEN
    -- So we are in the case, we give up
    errorCode := 1;
    -- But we still would like to have the fileId and nameserver
    -- host for logging reasons
    SELECT fileId, nsHost INTO fId, nh
      FROM CastorFile WHERE id = cfId;
    RETURN;
  END;
  -- Now we can safely update CastorFile's file size
  UPDATE CastorFile SET fileSize = fs, lastUpdateTime = ts
   WHERE id = cfId AND (lastUpdateTime IS NULL OR ts >= lastUpdateTime);
  -- If ts < lastUpdateTime, we were late and another job already updated the
  -- CastorFile. So we nevertheless retrieve the real file size, and
  -- we take a lock on the CastorFile. Together with triggers, this insures that
  -- we are the only ones to deal with its copies.
  SELECT fileId, nsHost, fileSize INTO fId, nh, realFileSize
    FROM CastorFile
    WHERE id = cfId
    FOR UPDATE;
  -- Get uid, gid and svcclass from Request
  SELECT euid, egid, svcClass INTO userId, groupId, scId
    FROM SubRequest,
      (SELECT euid, egid, id, svcClass from StagePutRequest UNION ALL
       SELECT euid, egid, id, svcClass from StageUpdateRequest UNION ALL
       SELECT euid, egid, id, svcClass from StagePutDoneRequest) Request
   WHERE SubRequest.request = Request.id AND SubRequest.id = srId;
  -- Get the filesystem of the DiskCopy
  SELECT fileSystem INTO fsId from DiskCopy
   WHERE castorFile = cfId AND status = 6;
  IF contextPIPP != 0 THEN
    -- If not a put inside a PrepareToPut/Update, create TapeCopies
    -- and update DiskCopy status
    putDoneFunc(cfId, realFileSize, contextPIPP, scId);
  ELSE
    -- If put inside PrepareToPut/Update, restart any PutDone currently
    -- waiting on this put/update
    UPDATE SubRequest SET status = 1, parent = 0 -- RESTART
     WHERE id IN
      (SELECT SubRequest.id FROM SubRequest, Id2Type
        WHERE SubRequest.request = Id2Type.id
          AND Id2Type.type = 39       -- PutDone
          AND SubRequest.castorFile = cfId
          AND SubRequest.status = 5); -- WAITSUBREQ
  END IF;
  -- Update filesystem status
  updateFsFileClosed(fsId);
  -- Archive Subrequest
  archiveSubReq(srId);
  COMMIT;
END;
/

/* PL/SQL method implementing selectCastorFile */
CREATE OR REPLACE PROCEDURE selectCastorFile (fId IN INTEGER,
                                              nh IN VARCHAR2,
                                              sc IN INTEGER,
                                              fc IN INTEGER,
                                              fs IN INTEGER,
                                              fn IN VARCHAR2,
                                              rid OUT INTEGER,
                                              rfs OUT INTEGER) AS
  CONSTRAINT_VIOLATED EXCEPTION;
  PRAGMA EXCEPTION_INIT(CONSTRAINT_VIOLATED, -1);
  nsHostName VARCHAR2(2048);
BEGIN
  -- Get the stager/nsHost configuration option
  nsHostName := getConfigOption('stager', 'nsHost', nh);
  BEGIN
    -- try to find an existing file and lock it
    SELECT id, fileSize INTO rid, rfs FROM CastorFile
     WHERE fileId = fid AND nsHost = nsHostName FOR UPDATE;
    -- update lastAccess time
    UPDATE CastorFile SET LastAccessTime = getTime(),
                          lastKnownFileName = REGEXP_REPLACE(fn,'(/){2,}','/')
     WHERE id = rid;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    -- insert new row
    INSERT INTO CastorFile (id, fileId, nsHost, svcClass, fileClass, fileSize,
                            creationTime, lastAccessTime, lastUpdateTime, lastKnownFileName)
      VALUES (ids_seq.nextval, fId, nsHostName, sc, fc, fs, getTime(), getTime(), getTime()-10, REGEXP_REPLACE(fn,'(/){2,}','/'))
      RETURNING id, fileSize INTO rid, rfs;
    INSERT INTO Id2Type (id, type) VALUES (rid, 2); -- OBJ_CastorFile
  END;
EXCEPTION WHEN CONSTRAINT_VIOLATED THEN
  -- retry the select since a creation was done in between
  SELECT id, fileSize INTO rid, rfs FROM CastorFile
    WHERE fileId = fid AND nsHost = nsHostName FOR UPDATE;
END;
/

/* PL/SQL method implementing stageRelease */
CREATE OR REPLACE PROCEDURE stageRelease (fid IN INTEGER,
                                          nh IN VARCHAR2,
                                          ret OUT INTEGER) AS
  cfId INTEGER;
  nbRes INTEGER;
  nsHostName VARCHAR2(2048);
BEGIN
  -- Get the stager/nsHost configuration option
  nsHostName := getConfigOption('stager', 'nsHost', nh);
  -- Lock the access to the CastorFile
  -- This, together with triggers will avoid new TapeCopies
  -- or DiskCopies to be added
  SELECT id INTO cfId FROM CastorFile
   WHERE fileId = fid AND nsHost = nsHostName FOR UPDATE;
  -- check if removal is possible for TapeCopies
  SELECT count(*) INTO nbRes FROM TapeCopy
   WHERE status = 3 -- TAPECOPY_SELECTED
     AND castorFile = cfId;
  IF nbRes > 0 THEN
    -- We found something, thus we cannot recreate
    ret := 1;
    RETURN;
  END IF;
  -- check if recreation is possible for SubRequests
  SELECT count(*) INTO nbRes FROM SubRequest
   WHERE status != 11 AND castorFile = cfId;   -- ARCHIVED
  IF nbRes > 0 THEN
    -- We found something, thus we cannot recreate
    ret := 2;
    RETURN;
  END IF;
  -- set DiskCopies to INVALID
  UPDATE DiskCopy SET status = 7 -- INVALID
   WHERE castorFile = cfId AND status = 0; -- STAGED
  ret := 0;
END;
/


/* PL/SQL method implementing stageForcedRm */
CREATE OR REPLACE PROCEDURE stageForcedRm (fid IN INTEGER,
                                           nh IN VARCHAR2,
                                           result OUT NUMBER) AS
  cfId INTEGER;
  nbRes INTEGER;
  dcsToRm "numList";
  nsHostName VARCHAR2(2048);
BEGIN
  -- by default, we are successful
  result := 1;
  -- Get the stager/nsHost configuration option
  nsHostName := getConfigOption('stager', 'nsHost', nh);
  -- Lock the access to the CastorFile
  -- This, together with triggers will avoid new TapeCopies
  -- or DiskCopies to be added
  SELECT id INTO cfId FROM CastorFile
   WHERE fileId = fid AND nsHost = nsHostName FOR UPDATE;
  -- list diskcopies
  SELECT id BULK COLLECT INTO dcsToRm
    FROM DiskCopy
   WHERE castorFile = cfId
     AND status IN (0, 5, 6, 10, 11);  -- STAGED, WAITFS, STAGEOUT, CANBEMIGR, WAITFS_SCHEDULING
  -- If nothing found, report ENOENT
  IF dcsToRm.COUNT = 0 THEN
    result := 0;
    RETURN;
  END IF;
  -- Stop ongoing recalls
  deleteTapeCopies(cfId);
  -- mark all get/put requests for those diskcopies
  -- and the ones waiting on them as failed
  -- so that clients eventually get an answer
  FOR sr IN (SELECT id, status FROM SubRequest
              WHERE diskcopy IN
                (SELECT /*+ CARDINALITY(dcidTable 5) */ *
                   FROM TABLE(dcsToRm) dcidTable)
                AND status IN (0, 1, 2, 5, 6, 13)) LOOP   -- START, WAITSUBREQ, READY, READYFORSCHED
    UPDATE SubRequest
       SET status = 7,  -- FAILED
           errorCode = 4,  -- EINTR
           errorMessage = 'Canceled by another user request',
           parent = 0
     WHERE (id = sr.id) OR (parent = sr.id);
  END LOOP;
  -- Set selected DiskCopies to INVALID
  FORALL i IN dcsToRm.FIRST .. dcsToRm.LAST
    UPDATE DiskCopy SET status = 7 -- INVALID
     WHERE id = dcsToRm(i);
END;
/


/* PL/SQL method implementing stageRm */
CREATE OR REPLACE PROCEDURE stageRm (srId IN INTEGER,
                                     fid IN INTEGER,
                                     nh IN VARCHAR2,
                                     svcClassId IN INTEGER,
                                     ret OUT INTEGER) AS
  cfId INTEGER;
  scId INTEGER;
  nbRes INTEGER;
  dcsToRm "numList";
  sr "numList";
  dcStatus INTEGER;
  nsHostName VARCHAR2(2048);
BEGIN
  ret := 0;
  -- Get the stager/nsHost configuration option
  nsHostName := getConfigOption('stager', 'nsHost', nh);
  BEGIN
    -- Lock the access to the CastorFile
    -- This, together with triggers will avoid new TapeCopies
    -- or DiskCopies to be added
    SELECT id INTO cfId FROM CastorFile
     WHERE fileId = fid AND nsHost = nsHostName FOR UPDATE;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    -- This file does not exist in the stager catalog
    -- so we just fail the request
    UPDATE SubRequest
       SET status = 7,  -- FAILED
           errorCode = 2,  -- ENOENT
           errorMessage = 'File not found on disk cache'
     WHERE id = srId;
    RETURN;
  END;
  -- First select involved diskCopies
  scId := svcClassId;
  dcStatus := 0;
	IF scId > 0 THEN
    SELECT id BULK COLLECT INTO dcsToRm FROM (
      -- first physical diskcopies
      SELECT DC.id
        FROM DiskCopy DC, FileSystem, DiskPool2SvcClass DP2SC
       WHERE DC.castorFile = cfId
         AND DC.status IN (0, 1, 2, 6, 10)  -- STAGED, STAGEOUT, CANBEMIGR, WAITDISK2DISKCOPY, WAITTAPERECALL
         AND DC.fileSystem = FileSystem.id
         AND FileSystem.diskPool = DP2SC.parent
         AND DP2SC.child = scId)
    UNION ALL (
      -- and then diskcopies resulting from PrepareToPut|Update requests
      SELECT DC.id
        FROM (SELECT id, svcClass FROM StagePrepareToPutRequest UNION ALL
              SELECT id, svcClass FROM StagePrepareToUpdateRequest) PrepareRequest,
             SubRequest, DiskCopy DC
       WHERE SubRequest.diskCopy = DC.id
         AND PrepareRequest.id = SubRequest.request
         AND PrepareRequest.svcClass = scId
         AND DC.castorFile = cfId
         AND DC.status IN (5, 11)  -- WAITFS, WAITFS_SCHEDULING
      );
    IF dcsToRm.COUNT = 0 THEN
      -- We didn't find anything on this svcClass, fail and return
      UPDATE SubRequest
         SET status = 7,  -- FAILED
             errorCode = 2,  -- ENOENT
             errorMessage = 'File not found on this service class'
       WHERE id = srId;
      RETURN;
    END IF;
    -- Check whether something else is left: if not, do as
    -- we are performing a stageRm everywhere.
    -- First select current status of the diskCopies: if CANBEMIGR,
    -- make sure we don't drop the last remaining valid migrable copy,
    -- i.e. drop the disk only copies from the count.
    SELECT status INTO dcStatus
      FROM DiskCopy
     WHERE id = dcsToRm(1);
    IF dcStatus = 10 THEN  -- CANBEMIGR
      SELECT count(*) INTO nbRes FROM DiskCopy
       WHERE castorFile = cfId
         AND status IN (1, 10)  -- WAITDISK2DISKCOPY, CANBEMIGR
         AND id NOT IN (
           (SELECT /*+ CARDINALITY(dcidTable 5) */ *
              FROM TABLE(dcsToRm) dcidTable)
           UNION
           (SELECT DC.id     -- all diskcopies in Tape0 pools
              FROM DiskCopy DC, FileSystem, DiskPool2SvcClass D2S, SvcClass, FileClass
             WHERE DC.castorFile = cfId
               AND DC.fileSystem = FileSystem.id
               AND FileSystem.diskPool = D2S.parent
               AND D2S.child = SvcClass.id
               AND SvcClass.forcedFileClass = FileClass.id
               AND FileClass.nbCopies = 0));
    ELSE
      SELECT count(*) INTO nbRes FROM DiskCopy
         WHERE castorFile = cfId
           AND status IN (0, 1, 2, 5, 6, 10, 11)  -- WAITDISK2DISKCOPY, WAITTAPERECALL, STAGED, STAGEOUT, CANBEMIGR, WAITFS, WAITFS_SCHEDULING
           AND id NOT IN (SELECT /*+ CARDINALITY(dcidTable 5) */ *
                            FROM TABLE(dcsToRm) dcidTable);
    END IF;
    IF nbRes = 0 THEN
      scId := 0;
    END IF;
  END IF;
  IF scId = 0 THEN
    SELECT id BULK COLLECT INTO dcsToRm
      FROM DiskCopy
     WHERE castorFile = cfId
       AND status IN (0, 1, 2, 5, 6, 10, 11);  -- WAITDISK2DISKCOPY, WAITTAPERECALL, STAGED, WAITFS, STAGEOUT, CANBEMIGR, WAITFS_SCHEDULING
  END IF;

  IF scId = 0 THEN
    -- full cleanup is to be performed, do all necessary checks beforehand
    DECLARE
      segId INTEGER;
      unusedIds "numList";
    BEGIN
      -- check if removal is possible for migration
      SELECT count(*) INTO nbRes FROM TapeCopy
       WHERE status IN (0, 1, 2, 3) -- CREATED, TOBEMIGRATED, WAITINSTREAMS, SELECTED
         AND castorFile = cfId;
      IF nbRes > 0 THEN
        -- We found something, thus we cannot remove
        UPDATE SubRequest
           SET status = 7,  -- FAILED
               errorCode = 16,  -- EBUSY
               errorMessage = 'The file is not yet migrated'
         WHERE id = srId;
        RETURN;
      END IF;
      -- check if removal is possible for Disk2DiskCopy
      SELECT count(*) INTO nbRes FROM DiskCopy
       WHERE status = 1 -- DISKCOPY_WAITDISK2DISKCOPY
         AND castorFile = cfId;
      IF nbRes > 0 THEN
        -- We found something, thus we cannot remove
        UPDATE SubRequest
           SET status = 7,  -- FAILED
               errorCode = 16,  -- EBUSY
               errorMessage = 'The file is being replicated'
         WHERE id = srId;
        RETURN;
      END IF;
      -- Stop ongoing recalls if stageRm either everywhere or the only available diskcopy.
      -- This is not entirely clean: a proper operation here should be to
      -- drop the SubRequest waiting for recall but keep the recall if somebody
      -- else is doing it, and taking care of other WAITSUBREQ requests as well...
      -- but it's fair enough, provided that the last stageRm will cleanup everything.
      -- First lock all segments for the file
      SELECT segment.id BULK COLLECT INTO unusedIds
        FROM Segment, TapeCopy
       WHERE TapeCopy.castorfile = cfId
         AND TapeCopy.id = Segment.copy
      FOR UPDATE;
      -- Check whether we have any segment in SELECTED
      SELECT segment.id INTO segId
        FROM Segment, TapeCopy
       WHERE TapeCopy.castorfile = cfId
         AND TapeCopy.id = Segment.copy
         AND Segment.status = 7 -- SELECTED
         AND ROWNUM < 2;
      -- Something is running, so give up
      UPDATE SubRequest
         SET status = 7,  -- FAILED
             errorCode = 16,  -- EBUSY
             errorMessage = 'The file is being recalled from tape'
       WHERE id = srId;
      RETURN;
    EXCEPTION WHEN NO_DATA_FOUND THEN
      -- Nothing running
      deleteTapeCopies(cfId);
      -- Invalidate the DiskCopies
      UPDATE DiskCopy
         SET status = 7  -- INVALID
       WHERE status = 2  -- WAITTAPERECALL
         AND castorFile = cfId;
      -- Mark the 'recall' SubRequests as failed
      -- so that clients eventually get an answer
      UPDATE SubRequest
         SET status = 7,  -- FAILED
             errorCode = 4,  -- EINTR
             errorMessage = 'Recall canceled by another user request'
       WHERE castorFile = cfId and status IN (4, 5);   -- WAITTAPERECALL, WAITSUBREQ
    END;
  END IF;

  -- Now perform the remove:
  -- mark all get/put requests for those diskcopies
  -- and the ones waiting on them as failed
  -- so that clients eventually get an answer
  SELECT id BULK COLLECT INTO sr
    FROM SubRequest
   WHERE diskcopy IN (SELECT /*+ CARDINALITY(dcidTable 5) */ *
                        FROM TABLE(dcsToRm) dcidTable)
     AND status IN (0, 1, 2, 5, 6, 13); -- START, WAITSUBREQ, READY, READYFORSCHED
  FORALL i IN sr.FIRST..sr.LAST
    UPDATE SubRequest
       SET status = 7, parent = 0,  -- FAILED
           errorCode = 4,  -- EINTR
           errorMessage = 'Canceled by another user request'
     WHERE id = sr(i) OR parent = sr(i);
  -- Set selected DiskCopies to INVALID. In any case keep
  -- WAITTAPERECALL diskcopies so that recalls can continue.
  -- Note that WAITFS and WAITFS_SCHEDULING DiskCopies don't exist on disk
  -- so they will only be taken by the cleaning daemon for the failed DCs.
  FORALL i IN dcsToRm.FIRST .. dcsToRm.LAST
    UPDATE DiskCopy SET status = 7 -- INVALID
     WHERE id = dcsToRm(i);
  ret := 1;  -- ok
END;
/


/* PL/SQL method implementing a setFileGCWeight request */
CREATE OR REPLACE PROCEDURE setFileGCWeightProc
(fid IN NUMBER, nh IN VARCHAR2, svcClassId IN NUMBER, weight IN FLOAT, ret OUT INTEGER) AS
  CURSOR dcs IS
  SELECT DiskCopy.id, gcWeight
    FROM DiskCopy, CastorFile
   WHERE castorFile.id = diskcopy.castorFile
     AND fileid = fid
     AND nshost = getConfigOption('stager', 'nsHost', nh)
     AND fileSystem IN (
       SELECT FileSystem.id
         FROM FileSystem, DiskPool2SvcClass D2S
        WHERE FileSystem.diskPool = D2S.parent
          AND D2S.child = svcClassId);
  gcwProc VARCHAR(2048);
  gcw NUMBER;
BEGIN
  ret := 0;
  -- get gc userSetGCWeight function to be used, if any
  gcwProc := castorGC.getUserSetGCWeight(svcClassId);
  -- loop over diskcopies and update them
  FOR dc in dcs LOOP
    gcw := dc.gcWeight;
    -- compute actual gc weight to be used
    IF gcwProc IS NOT NULL THEN
      EXECUTE IMMEDIATE 'BEGIN :newGcw := ' || gcwProc || '(:oldGcw, :delta); END;'
        USING OUT gcw, IN gcw, weight;
    END IF;
    -- update DiskCopy
    UPDATE DiskCopy SET gcWeight = gcw WHERE id = dc.id;
    ret := 1;   -- some diskcopies found, ok
  END LOOP;
END;
/


/*
 * PL/SQL method implementing the stager_qry based on file id
 */
CREATE OR REPLACE PROCEDURE fileIdStageQuery
 (fid IN NUMBER,
  nh IN VARCHAR2,
  svcClassId IN INTEGER,
  fileName IN VARCHAR2,
  result OUT castor.QueryLine_Cur) AS
  cfs "numList";
  currentFileName VARCHAR2(2048);
  nsHostName VARCHAR2(2048);
BEGIN
  -- Get the stager/nsHost configuration option
  nsHostName := getConfigOption('stager', 'nsHost', nh);
  -- Extract CastorFile ids from the fileid
  SELECT id BULK COLLECT INTO cfs FROM CastorFile 
   WHERE fileId = fid AND nshost = nsHostName;
  -- Check and fix when needed the LastKnownFileNames
  IF (cfs.COUNT > 0) THEN
    SELECT lastKnownFileName INTO currentFileName
      FROM CastorFile
     WHERE id = cfs(cfs.FIRST);
    IF currentFileName != fileName THEN
      UPDATE CastorFile SET lastKnownFileName = fileName
       WHERE id = cfs(cfs.FIRST);
      COMMIT;
    END IF;
  END IF;
  -- Finally issue the actual query
  internalStageQuery(cfs, svcClassId, result);
END;
/


/* PL/SQL method implementing rtcpclientdCleanUp */
CREATE OR REPLACE PROCEDURE rtcpclientdCleanUp AS
  tpIds "numList";
BEGIN
  -- Deal with Migrations
  -- 1) Ressurect tapecopies for migration
  UPDATE TapeCopy SET status = 1 WHERE status = 3;
  -- 2) Clean up the streams
  UPDATE Stream SET status = 0 
   WHERE status NOT IN (0, 5, 6, 7) --PENDING, CREATED, STOPPED, WAITPOLICY
  RETURNING tape BULK COLLECT INTO tpIds;
  UPDATE Stream SET tape = 0 WHERE tape != 0;
  -- 3) Reset the tape for migration
  FORALL i IN tpIds.FIRST .. tpIds.LAST  
    UPDATE tape SET stream = 0, status = 0 WHERE status IN (2, 3, 4) AND id = tpIds(i);

  -- Deal with Recalls
  UPDATE Segment SET status = 0 WHERE status = 7; -- Resurrect SELECTED segment
  UPDATE Tape SET status = 1 WHERE tpmode = 0 AND status IN (2, 3, 4); -- Resurrect the tapes running for recall
  UPDATE Tape A SET status = 8 
   WHERE status IN (0, 6, 7) AND EXISTS
    (SELECT id FROM Segment WHERE status = 0 AND tape = A.id);
  COMMIT;
END;
/


/* PL/SQL method implementing fileRecallFailed */
CREATE OR REPLACE PROCEDURE fileRecallFailed(tapecopyId IN INTEGER) AS
 SubRequestId NUMBER;
 dci NUMBER;
 fsId NUMBER;
 fileSize NUMBER;
BEGIN
  SELECT SubRequest.id, DiskCopy.id, CastorFile.filesize
    INTO SubRequestId, dci, fileSize
    FROM TapeCopy, SubRequest, DiskCopy, CastorFile
   WHERE TapeCopy.id = tapecopyId
     AND CastorFile.id = TapeCopy.castorFile
     AND DiskCopy.castorFile = TapeCopy.castorFile
     AND SubRequest.diskcopy(+) = DiskCopy.id
     AND DiskCopy.status = 2; -- DISKCOPY_WAITTAPERECALL
  UPDATE DiskCopy SET status = 4 
   WHERE id = dci RETURNING fileSystem INTO fsid; -- DISKCOPY_FAILED
  IF SubRequestId IS NOT NULL THEN
    UPDATE SubRequest SET status = 7, -- SUBREQUEST_FAILED
                          getNextStatus = 1, -- GETNEXTSTATUS_FILESTAGED (not strictly correct but the request is over anyway)
                          lastModificationTime = getTime()
     WHERE id = SubRequestId;
    UPDATE SubRequest SET status = 7, -- SUBREQUEST_FAILED
                          getNextStatus = 1, -- GETNEXTSTATUS_FILESTAGED
                          lastModificationTime = getTime(),
                          parent = 0
     WHERE parent = SubRequestId;
  END IF;
END;
/


/** Cleanup before starting a new MigHunterDaemon **/
CREATE OR REPLACE PROCEDURE migHunterCleanUp(svcName IN VARCHAR2)
AS
  svcId NUMBER;
BEGIN
  SELECT id INTO svcId FROM SvcClass WHERE name = svcName;
  -- clean up tapecopies , WAITPOLICY reset into TOBEMIGRATED
  UPDATE /*+ BEGIN_OUTLINE_DATA
             IGNORE_OPTIM_EMBEDDED_HINTS
             ALL_ROWS
             OUTLINE_LEAF(@"SEL3FF8579E")
             UNNEST(@"SEL1")
             OUTLINE(@"UPD1")
             OUTLINE(@"SEL1")
             INDEX_RS_ASC(@"SEL3FF8579E" "TC"@"UPD1" ("TAPECOPY"."STATUS"))
             INDEX_RS_ASC(@"SEL3FF8579E" "CF"@"SEL1" ("CASTORFILE"."ID"))
             LEADING(@"SEL3FF8579E" "TC"@"UPD1" "CF"@"SEL1")
             USE_NL(@"SEL3FF8579E" "CF"@"SEL1") */
         TapeCopy TC
     SET status = 1
   WHERE status = 7 
     AND EXISTS (
       SELECT 'x' 
         FROM CastorFile CF
        WHERE TC.castorFile = CF.id
          AND CF.svcclass = svcId);
  -- clean up streams, WAITPOLICY reset into CREATED
  UPDATE Stream SET status = 5 WHERE status = 7 AND tapepool IN
   (SELECT svcclass2tapepool.child
      FROM svcclass2tapepool
     WHERE svcId = svcclass2tapepool.parent);
  COMMIT;
END;
/


/* Get input for python migration policy */
CREATE OR REPLACE PROCEDURE inputForMigrationPolicy
(svcclassName IN VARCHAR2,
 policyName OUT VARCHAR2,
 svcId OUT NUMBER,
 dbInfo OUT castor.DbMigrationInfo_Cur) AS
 tcIds "numList";
BEGIN
  -- do the same operation of getMigrCandidate and return the dbInfoMigrationPolicy
  -- we look first for repack condidates for this svcclass
  -- we update atomically WAITPOLICY
  SELECT SvcClass.migratorpolicy, SvcClass.id INTO policyName, svcId
    FROM SvcClass
   WHERE SvcClass.name = svcClassName;

  UPDATE /*+ BEGIN_OUTLINE_DATA
             IGNORE_OPTIM_EMBEDDED_HINTS
             ALL_ROWS
             OUTLINE_LEAF(@"SEL3FF8579E")
             UNNEST(@"SEL1")
             OUTLINE(@"UPD1")
             OUTLINE(@"SEL1")
             INDEX_RS_ASC(@"SEL3FF8579E" "TC"@"UPD1" ("TAPECOPY"."STATUS"))
             INDEX_RS_ASC(@"SEL3FF8579E" "CF"@"SEL1" ("CASTORFILE"."ID"))
             LEADING(@"SEL3FF8579E" "TC"@"UPD1" "CF"@"SEL1")
             USE_NL(@"SEL3FF8579E" "CF"@"SEL1") */
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
           AND CF.svcClass = svcId))
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


/* PL/SQL method implementing nsFilesDeletedProc */
CREATE OR REPLACE PROCEDURE nsFilesDeletedProc
(nh IN VARCHAR2,
 fileIds IN castor."cnumList",
 orphans OUT castor.IdRecord_Cur) AS
  unused NUMBER;
  nsHostName VARCHAR2(2048);
BEGIN
  IF fileIds.COUNT <= 0 THEN
    RETURN;
  END IF;
  -- Get the stager/nsHost configuration option
  nsHostName := getConfigOption('stager', 'nsHost', nh);
  -- Loop over the deleted files and split the orphan ones
  -- from the normal ones
  FOR fid in fileIds.FIRST .. fileIds.LAST LOOP
    BEGIN
      SELECT id INTO unused FROM CastorFile
       WHERE fileid = fileIds(fid) AND nsHost = nsHostName;
      stageForcedRm(fileIds(fid), nsHostName, unused);
    EXCEPTION WHEN NO_DATA_FOUND THEN
      -- this file was dropped from nameServer AND stager
      -- and still exists on disk. We put it into the list
      -- of orphan fileids to return
      INSERT INTO NsFilesDeletedOrphans VALUES(fileIds(fid));
    END;
  END LOOP;
  -- return orphan ones
  OPEN orphans FOR SELECT * FROM NsFilesDeletedOrphans;
END;
/


/* Search and delete old archived/failed subrequests and their requests */
CREATE OR REPLACE PROCEDURE deleteTerminatedRequests AS
  timeOut INTEGER;
  rate INTEGER;
  srIds "numList";
BEGIN
  -- select requested timeout from configuration table
  timeout := 3600*TO_NUMBER(getConfigOption('cleaning', 'terminatedRequestsTimeout', '120'));
  -- get a rough estimate of the current request processing rate
  SELECT count(*) INTO rate
    FROM SubRequest
   WHERE status IN (9, 11)  -- FAILED_FINISHED, ARCHIVED
     AND lastModificationTime > getTime() - 1800;
  IF rate > 0 AND (500000 / rate * 1800) < timeOut THEN
    timeOut := 500000 / rate * 1800;  -- keep 500k requests max
  END IF;

  -- now delete the SubRequests. Here we don't use bulkDelete
  -- to be able to pass timeout as a bind variable and make it sharable
  -- original call was:
  -- bulkDelete('SELECT id FROM SubRequest WHERE status IN (9, 11)
  --   AND lastModificationTime < getTime() - '|| timeOut ||';',
  --   'SubRequest');
  DECLARE
    CURSOR s IS SELECT id FROM SubRequest 
     WHERE status IN (9, 11) 
       AND lastModificationTime < getTime() - timeOut;
    CURSOR t IS SELECT UNIQUE castorfile FROM SubRequest 
     WHERE status IN (9, 11) 
       AND lastModificationTime < getTime() - timeOut;
    ids "numList";
  BEGIN
    OPEN t;
    LOOP
      FETCH t BULK COLLECT INTO ids LIMIT 1000;
      FOR i IN ids.FIRST..ids.LAST LOOP
        deleteCastorFile(ids(i));
      END LOOP;
      COMMIT;
      EXIT WHEN t%NOTFOUND;
    END LOOP;
    OPEN s;
    LOOP
      FETCH s BULK COLLECT INTO ids LIMIT 10000;
      FORALL i IN ids.FIRST..ids.LAST
        DELETE FROM Id2Type WHERE id = ids(i); 
      FORALL i IN ids.FIRST..ids.LAST
        DELETE FROM SubRequest WHERE id = ids(i);
      COMMIT;
      EXIT WHEN s%NOTFOUND;
    END LOOP;
  END;
  COMMIT;

  -- and then related Requests + Clients
    ---- Get ----
  bulkDeleteRequests('StageGetRequest');
    ---- Put ----
  bulkDeleteRequests('StagePutRequest');
    ---- Update ----
  bulkDeleteRequests('StageUpdateRequest');
    ---- PrepareToGet -----
  bulkDeleteRequests('StagePrepareToGetRequest');
    ---- PrepareToPut ----
  bulkDeleteRequests('StagePrepareToPutRequest');
    ---- PrepareToUpdate ----
  bulkDeleteRequests('StagePrepareToUpdateRequest');
    ---- PutDone ----
  bulkDeleteRequests('StagePutDoneRequest');
    ---- Rm ----
  bulkDeleteRequests('StageRmRequest');
    ---- Repack ----
  bulkDeleteRequests('StageRepackRequest');
    ---- DiskCopyReplica ----
  bulkDeleteRequests('StageDiskCopyReplicaRequest');
END;
/


/* Runs cleanup operations and a table shrink for maintenance purposes */
CREATE OR REPLACE PROCEDURE cleanup AS
  t INTEGER;
BEGIN
  -- First perform some cleanup of old stuff:
  -- for each, read relevant timeout from configuration table
  t := TO_NUMBER(getConfigOption('cleaning', 'outOfDateStageOutDCsTimeout', '72'));
  deleteOutOfDateStageOutDCs(t*3600);
  t := TO_NUMBER(getConfigOption('cleaning', 'failedDCsTimeout', '72'));
  deleteFailedDiskCopies(t*3600);

  -- Loop over all tables which support row movement and recover space from
  -- the object and all dependant objects. We deliberately ignore tables
  -- with function based indexes here as the 'shrink space' option is not
  -- supported.
  FOR t IN (SELECT table_name FROM user_tables
             WHERE row_movement = 'ENABLED'
               AND table_name NOT IN (
                 SELECT table_name FROM user_indexes
                  WHERE index_type LIKE 'FUNCTION-BASED%')
               AND temporary = 'N')
  LOOP
    EXECUTE IMMEDIATE 'ALTER TABLE '|| t.table_name ||' SHRINK SPACE CASCADE';
  END LOOP;
END;
/


/*
 * Database jobs
 */
BEGIN
  -- Remove database jobs before recreating them
  FOR j IN (SELECT job_name FROM user_scheduler_jobs
             WHERE job_name IN ('HOUSEKEEPINGJOB', 'CLEANUPJOB', 'BULKCHECKFSBACKINPRODJOB', 'RESTARTSTUCKRECALLSJOB'))
  LOOP
    DBMS_SCHEDULER.DROP_JOB(j.job_name, TRUE);
  END LOOP;

  -- Create a db job to be run every 20 minutes executing the deleteTerminatedRequests procedure
  DBMS_SCHEDULER.CREATE_JOB(
      JOB_NAME        => 'houseKeepingJob',
      JOB_TYPE        => 'PLSQL_BLOCK',
      JOB_ACTION      => 'BEGIN deleteTerminatedRequests(); END;',
      JOB_CLASS       => 'CASTOR_JOB_CLASS',
      START_DATE      => SYSDATE + 60/1440,
      REPEAT_INTERVAL => 'FREQ=MINUTELY; INTERVAL=20',
      ENABLED         => TRUE,
      COMMENTS        => 'Cleaning of terminated requests');

  -- Create a db job to be run twice a day executing the cleanup procedure
  DBMS_SCHEDULER.CREATE_JOB(
      JOB_NAME        => 'cleanupJob',
      JOB_TYPE        => 'PLSQL_BLOCK',
      JOB_ACTION      => 'BEGIN cleanup(); END;',
      JOB_CLASS       => 'CASTOR_JOB_CLASS',
      START_DATE      => SYSDATE + 60/1440,
      REPEAT_INTERVAL => 'FREQ=HOURLY; INTERVAL=12',
      ENABLED         => TRUE,
      COMMENTS        => 'Database maintenance');

  -- Create a db job to be run every 5 minutes executing the bulkCheckFSBackInProd procedure
  DBMS_SCHEDULER.CREATE_JOB(
      JOB_NAME        => 'bulkCheckFSBackInProdJob',
      JOB_TYPE        => 'PLSQL_BLOCK',
      JOB_ACTION      => 'BEGIN bulkCheckFSBackInProd(); END;',
      JOB_CLASS       => 'CASTOR_JOB_CLASS',
      START_DATE      => SYSDATE + 60/1440,
      REPEAT_INTERVAL => 'FREQ=MINUTELY; INTERVAL=5',
      ENABLED         => TRUE,
      COMMENTS        => 'Bulk operation to processing filesystem state changes');
  
  -- Create a db job to be run every hour executing the restartStuckRecalls workaround procedure
  DBMS_SCHEDULER.CREATE_JOB(
      JOB_NAME        => 'restartStuckRecallsJob',
      JOB_TYPE        => 'PLSQL_BLOCK',
      JOB_ACTION      => 'BEGIN restartStuckRecalls(); END;',
      JOB_CLASS       => 'CASTOR_JOB_CLASS',
      START_DATE      => SYSDATE + 60/1440,
      REPEAT_INTERVAL => 'FREQ=MINUTELY; INTERVAL=60',
      ENABLED         => TRUE,
      COMMENTS        => 'Workaround to restart stuck recalls');
END;
/


/* Recompile all procedures */
/****************************/
BEGIN
  FOR a IN (SELECT object_name, object_type
              FROM all_objects
             WHERE object_type = 'PROCEDURE'
               AND status = 'INVALID')
  LOOP
    EXECUTE IMMEDIATE 'ALTER PROCEDURE '||a.object_name||' COMPILE';
  END LOOP;
END;
/
