/*******************************************************************
 *
 * @(#)$RCSfile: oracleCommon.sql,v $ $Revision: 1.697 $ $Date: 2009/08/13 14:11:33 $ $Author: itglp $
 *
 * This file contains some common PL/SQL utilities for the stager database.
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *******************************************************************/

/* Get current time as a time_t. Not that easy in ORACLE */
CREATE OR REPLACE FUNCTION getTime RETURN NUMBER IS
  epoch            TIMESTAMP WITH TIME ZONE;
  now              TIMESTAMP WITH TIME ZONE;
  interval         INTERVAL DAY(9) TO SECOND;
  interval_days    NUMBER;
  interval_hours   NUMBER;
  interval_minutes NUMBER;
  interval_seconds NUMBER;
BEGIN
  epoch := TO_TIMESTAMP_TZ('01-JAN-1970 00:00:00 00:00',
    'DD-MON-YYYY HH24:MI:SS TZH:TZM');
  now := SYSTIMESTAMP AT TIME ZONE '00:00';
  interval         := now - epoch;
  interval_days    := EXTRACT(DAY    FROM (interval));
  interval_hours   := EXTRACT(HOUR   FROM (interval));
  interval_minutes := EXTRACT(MINUTE FROM (interval));
  interval_seconds := EXTRACT(SECOND FROM (interval));

  RETURN interval_days * 24 * 60 * 60 + interval_hours * 60 * 60 +
    interval_minutes * 60 + interval_seconds;
END;
/

/* Generate a universally unique id (UUID) */
CREATE OR REPLACE FUNCTION uuidGen RETURN VARCHAR2 IS
  ret VARCHAR2(36);
BEGIN
  -- Note: the guid generator provided by ORACLE produces sequential uuid's, not
  -- random ones. The reason for this is because random uuid's are not good for
  -- indexing!
  RETURN lower(regexp_replace(sys_guid(), '(.{8})(.{4})(.{4})(.{4})(.{12})', '\1-\2-\3-\4-\5'));
END;
/

/* Function to check if a service class exists by name. This function can return
 * the id of the named service class or raise an application error if it does
 * not exist.
 * @param svcClasName The name of the service class (Note: can be NULL)
 * @param allowNull   Flag to indicate whether NULL or '' service class names are
 *                    permitted.
 * @param raiseError  Flag to indicate whether the function should raise an
 *                    application error when the service class doesn't exist or
 *                    return a boolean value of 0.
 */
CREATE OR REPLACE FUNCTION checkForValidSvcClass
(svcClassName VARCHAR2, allowNull NUMBER, raiseError NUMBER) RETURN NUMBER IS
  ret NUMBER;
BEGIN
  -- Check if the service class name is allowed to be NULL. This is quite often
  -- the case if the calling function supports '*' (null) to indicate that all
  -- service classes are being targeted. Nevertheless, in such a case we
  -- return the id of the default one.
  IF svcClassName IS NULL OR length(svcClassName) IS NULL THEN
    IF allowNull = 1 THEN
      SELECT id INTO ret FROM SvcClass WHERE name = 'default';
      RETURN ret;
    END IF;
  END IF;
  -- We do accept '*' as being valid, as it is the wildcard
  IF svcClassName = '*' THEN
    RETURN 0;
  END IF;
  -- Check to see if service class exists by name and return its id
  BEGIN
    SELECT id INTO ret FROM SvcClass WHERE name = svcClassName;
    RETURN ret;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    -- If permitted to do so raise an application error if the service class does
    -- not exist
    IF raiseError = 1 THEN
      raise_application_error(-20113, 'Invalid service class ''' || svcClassName || '''');
    END IF;
    RETURN 0;
  END;
END;
/

/* Function to return a comma separate list of service classes that a
 * filesystem belongs to.
 */
CREATE OR REPLACE FUNCTION getSvcClassList(fsId NUMBER) RETURN VARCHAR2 IS
  svcClassList VARCHAR2(4000) := NULL;
  c INTEGER := 0;
BEGIN
  FOR a IN (SELECT Distinct(SvcClass.name)
              FROM FileSystem, DiskPool2SvcClass, SvcClass
             WHERE FileSystem.id = fsId
               AND FileSystem.diskpool = DiskPool2SvcClass.parent
               AND DiskPool2SvcClass.child = SvcClass.id
             ORDER BY SvcClass.name)
  LOOP
    svcClassList := svcClassList || ',' || a.name;
    c := c + 1;
    IF c = 5 THEN
      svcClassList := svcClassList || ',...';
      EXIT;
    END IF;
  END LOOP;
  RETURN ltrim(svcClassList, ',');
END;
/

/* Function to extract a configuration option from the castor config
 * table.
 */
CREATE OR REPLACE FUNCTION getConfigOption
(className VARCHAR2, optionName VARCHAR2, defaultValue VARCHAR2) 
RETURN VARCHAR2 IS
  returnValue VARCHAR2(2048) := defaultValue;
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

/* Function to concatenate values into a string using a specified delimiter.
 * If no delimiter is specified the default is ','.
 */
CREATE OR REPLACE FUNCTION strConcat(p_cursor SYS_REFCURSOR, p_del VARCHAR2 := ',')
RETURN VARCHAR2 IS
  l_value   VARCHAR2(2048);
  l_result  VARCHAR2(2048);
BEGIN
  LOOP
    FETCH p_cursor INTO l_value;
    EXIT WHEN p_cursor%NOTFOUND;
    IF l_result IS NOT NULL THEN
      l_result := l_result || p_del;
    END IF;
    l_result := l_result || l_value;
  END LOOP;
  RETURN l_result;
END;
/

/* Function to tokenize a string using a specified delimiter. If no delimiter
 * is specified the default is ','. The results are returned as a table e.g.
 * SELECT * FROM TABLE (strTokenizer(inputValue, delimiter))
 */
CREATE OR REPLACE FUNCTION strTokenizer(p_list VARCHAR2, p_del VARCHAR2 := ',')
  RETURN strListTable pipelined IS
  l_idx   INTEGER;
  l_list  VARCHAR2(32767) := p_list;
  l_value VARCHAR2(32767);
BEGIN
  LOOP
    l_idx := instr(l_list, p_del);
    IF l_idx > 0 THEN
      PIPE ROW(ltrim(rtrim(substr(l_list, 1, l_idx - 1))));
      l_list := substr(l_list, l_idx + length(p_del));
    ELSE
      IF l_list IS NOT NULL THEN
        PIPE ROW(ltrim(rtrim(l_list)));
      END IF;
      EXIT;
    END IF;
  END LOOP;
  RETURN;
END;
/

/* Function to normalize a filepath, i.e. to drop multiple '/'s and resolve any '..' */
CREATE OR REPLACE FUNCTION normalizePath(path IN VARCHAR2) RETURN VARCHAR2 IS
  buf VARCHAR2(2048);
  ret VARCHAR2(2048);
BEGIN
  -- drop '.'s and multiple '/'s
  ret := replace(regexp_replace(path, '[/]+', '/'), '/./', '/');
  LOOP
    buf := ret;
    -- a path component is by definition anything between two slashes, except
    -- the '..' string itself. This is not taken into account, resulting in incorrect
    -- parsing when relative paths are used (see bug #49002). We're not concerned by
    -- those cases; however this code could be fixed and improved by using string
    -- tokenization as opposed to expensive regexp parsing.
    ret := regexp_replace(buf, '/[^/]+/\.\./', '/');
    EXIT WHEN ret = buf;
  END LOOP;
  RETURN ret;
END;
/

/* Function to check if a diskserver and its given mountpoints have any files
 * attached to them.
 */
CREATE OR REPLACE
FUNCTION checkIfFilesExist(diskServerName IN VARCHAR2, mountPointName IN VARCHAR2)
RETURN NUMBER AS
  rtn NUMBER;
BEGIN
  SELECT count(*) INTO rtn
    FROM DiskCopy, FileSystem, DiskServer
   WHERE DiskCopy.fileSystem = FileSystem.id
     AND FileSystem.diskserver = DiskServer.id
     AND DiskServer.name = diskServerName
     AND (FileSystem.mountpoint = mountPointName 
      OR  length(mountPointName) IS NULL)
     AND rownum = 1;
  RETURN rtn;
END;
/

/* PL/SQL method deleting migration jobs of a castorfile */
CREATE OR REPLACE PROCEDURE deleteMigrationJobs(cfId NUMBER) AS
BEGIN
  DELETE FROM MigrationJob WHERE castorfile = cfId;
  DELETE FROM MigratedSegment WHERE castorfile = cfId;
END;
/

/* PL/SQL method deleting recall jobs (and segments) of a castorfile */
CREATE OR REPLACE PROCEDURE deleteRecallJobs(cfId NUMBER) AS
BEGIN
  -- Loop over the recall jobs
  FOR t IN (SELECT id FROM RecallJob WHERE castorfile = cfId) LOOP
    DELETE FROM Segment WHERE copy = t.id;
    DELETE FROM RecallJob WHERE id = t.id;
  END LOOP;
  -- delete migration jobs waiting on this recall
  DELETE FROM MigrationJob WHERE castorfile = cfId AND status = tconst.MIGRATIONJOB_WAITINGONRECALL;
  -- delete migrated segments if no migration jobs remain
  DECLARE
    unused NUMBER;
  BEGIN
    SELECT /*+ INDEX_RS_ASC(MigrationJob I_MigrationJob_CFCopyNb) */ castorfile INTO unused
      FROM MigrationJob WHERE castorfile = cfId;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    DELETE FROM MigratedSegment WHERE castorfile = cfId;
  END;
END;
/

/* PL/SQL method canceling a given recall */
CREATE OR REPLACE PROCEDURE cancelRecall
(cfId NUMBER, dcId NUMBER, newSubReqStatus NUMBER) AS
  srIds "numList";
  unused NUMBER;
BEGIN
  -- Lock the CastorFile
  SELECT id INTO unused FROM CastorFile
   WHERE id = cfId FOR UPDATE;
  -- Cancel the recall
  deleteRecallJobs(cfId);
  -- Invalidate the DiskCopy
  UPDATE DiskCopy SET status = 7 WHERE id = dcId; -- INVALID
  -- Look for request associated to the recall and fail
  -- it and all the waiting ones
  UPDATE /*+ INDEX(Subrequest I_Subrequest_Diskcopy)*/ SubRequest
     SET status = newSubReqStatus
   WHERE diskCopy = dcId RETURNING id BULK COLLECT INTO srIds;
  UPDATE /*+ INDEX(Subrequest I_Subrequest_Parent)*/ SubRequest
     SET status = newSubReqStatus, parent = 0 -- FAILED
   WHERE status = 5 -- WAITSUBREQ
     AND parent IN
       (SELECT /*+ CARDINALITY(sridTable 5) */ *
          FROM TABLE(srIds) sridTable)
     AND castorfile = cfId;
END;
/

/* PL/SQL method to delete a CastorFile only when no DiskCopy, no MigrationJob and no RecallJob are left for it */
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
    -- See whether it has any RecallJob
    SELECT count(*) INTO nb FROM RecallJob
     WHERE castorFile = cfId AND status != tconst.RECALLJOB_FAILED;
    -- If any RecallJob, give up
    IF nb = 0 THEN
      -- See whether it has any MigrationJob
      SELECT count(*) INTO nb FROM MigrationJob
       WHERE castorFile = cfId;
      -- If any MigrationJob, give up
      IF nb = 0 THEN
        -- See whether pending SubRequests exist
        SELECT /*+ INDEX(Subrequest I_Subrequest_Castorfile)*/ count(*) INTO nb
          FROM SubRequest
         WHERE castorFile = cfId
           AND status IN (0, 1, 2, 3, 4, 5, 6, 7, 10, 12, 13, 14);   -- All but FINISHED, FAILED_FINISHED, ARCHIVED
        -- If any SubRequest, give up
        IF nb = 0 THEN
          DECLARE
            fid NUMBER;
            fc NUMBER;
            nsh VARCHAR2(2048);
          BEGIN
            -- Delete the failed Recalls
            deleteRecallJobs(cfId);
            -- Delete the failed Migrations
            deleteMigrationJobs(cfId);
            -- Delete the CastorFile
            DELETE FROM CastorFile WHERE id = cfId
              RETURNING fileId, nsHost, fileClass
              INTO fid, nsh, fc;
            -- check whether this file potentially had copies on tape
            SELECT nbCopies INTO nb FROM FileClass WHERE id = fc;
            IF nb = 0 THEN
              -- This castorfile was created with no copy on tape
              -- So removing it from the stager means erasing
              -- it completely. We should thus also remove it
              -- from the name server
              INSERT INTO FilesDeletedProcOutput VALUES (fid, nsh);
            END IF;
          END;
        END IF;
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
