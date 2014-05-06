/*******************************************************************
 *
 *
 * This file contains some common PL/SQL utilities for the stager database.
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *******************************************************************/

/* Returns a time interval in seconds */
CREATE OR REPLACE FUNCTION getSecs(startTime IN TIMESTAMP, endTime IN TIMESTAMP) RETURN NUMBER IS
BEGIN
  RETURN TRUNC(EXTRACT(SECOND FROM (endTime - startTime)), 6);
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

/* Function to return the name of a given file class */
CREATE OR REPLACE FUNCTION getFileClassName(fileClassId NUMBER) RETURN VARCHAR2 IS
  varFileClassName VARCHAR2(2048);
BEGIN
  SELECT name INTO varFileClassName FROM FileClass WHERE id = fileClassId;
  RETURN varFileClassName;
EXCEPTION WHEN NO_DATA_FOUND THEN
  RETURN 'Unknown(' || fileClassId || ')';
END;
/

/* Function to return the name of a given service class */
CREATE OR REPLACE FUNCTION getSvcClassName(svcClassId NUMBER) RETURN VARCHAR2 IS
  varSvcClassName VARCHAR2(2048);
BEGIN
  SELECT name INTO varSvcClassName FROM SvcClass WHERE id = svcClassId;
  RETURN varSvcClassName;
EXCEPTION WHEN NO_DATA_FOUND THEN
  RETURN 'Unknown(' || svcClassId || ')';
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

/* Function to return a comma separate list of service classes that a
 * DataPool belongs to.
 */
CREATE OR REPLACE FUNCTION getSvcClassListDP(dpId NUMBER) RETURN VARCHAR2 IS
  svcClassList VARCHAR2(4000) := NULL;
  c INTEGER := 0;
BEGIN
  FOR a IN (SELECT Distinct(SvcClass.name)
              FROM DataPool2SvcClass, SvcClass
             WHERE DataPool2SvcClass.parent = dpId
               AND DataPool2SvcClass.child = SvcClass.id
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

/* parse a path to give back the FileSystem and path */
CREATE OR REPLACE PROCEDURE parsePath(inFullPath IN VARCHAR2,
                                      outFileSystem OUT INTEGER,
                                      outDataPool OUT INTEGER,
                                      outPath OUT VARCHAR2,
                                      outDcId OUT INTEGER,
                                      outFileId OUT INTEGER,
                                      outNsHost OUT VARCHAR2) AS
  varPathPos INTEGER;
  varLastDotPos INTEGER;
  varLastSlashPos INTEGER;
  varAtPos INTEGER;
  varColonPos INTEGER;
  varDiskServerName VARCHAR2(2048);
  varMountPoint VARCHAR2(2048);
  varDataPool VARCHAR2(2048);
BEGIN
  -- DcId is the part after the last '.'
  varLastDotPos := INSTR(inFullPath, '.', -1, 1);
  outDcId := TO_NUMBER(SUBSTR(inFullPath, varLastDotPos+1));
  -- the fileid is between last / and '@'
  varLastSlashPos := INSTR(inFullPath, '/', -1, 1);
  varAtPos := INSTR(inFullPath, '@', 1, 1);
  outFileId := TO_NUMBER(SUBSTR(inFullPath, varLastSlashPos+1, varAtPos-varLastSlashPos-1));
  -- the nsHost is between '@' and last '.'
  outNsHost := SUBSTR(inFullPath, varAtPos+1, varLastDotPos-varAtPos-1);
  -- the diskserver is before the ':'
  varColonPos := INSTR(inFullPath, ':', 1, 1);
  varDiskServerName := SUBSTR(inFullPath, 1, varColonPos-1);
  -- path starts after the second '/' from the end if we are dealing with a diskpool
  varPathPos := INSTR(inFullPath, '/', -1, 2);
  -- if the second '/' was not found, then we are dealing with a data pool
  IF 0 = varPathPos THEN
    -- the data pool id between the ':' and the only '/'
    varPathPos := INSTR(inFullPath, '/', -1, 1);
    varDataPool := SUBSTR(inFullPath, varColonPos+1, varPathPos-varColonPos);
    -- find out the dataPool Id
    SELECT id INTO outDataPool FROM DataPool WHERE name = varDataPool;
    outFileSystem := NULL;
  ELSE
    -- regular disk pool
    outPath := SUBSTR(inFullPath, varPathPos+1);
    -- the mountPoint is between the ':' and the start of the path
    varMountPoint := SUBSTR(inFullPath, varColonPos+1, varPathPos-varColonPos);
    -- find out the filesystem Id
    SELECT FileSystem.id INTO outFileSystem
      FROM DiskServer, FileSystem
     WHERE DiskServer.name = varDiskServerName
       AND FileSystem.diskServer = DiskServer.id
       AND FileSystem.mountPoint = varMountPoint;
    outDataPool := NULL;
  END IF;
END;
/

/* PL/SQL method deleting disk2diskCopyJobs jobs of a castorfile */
CREATE OR REPLACE PROCEDURE deleteDisk2DiskCopyJobs(inCfId INTEGER) AS
BEGIN
  DELETE FROM Disk2DiskCopyJob WHERE castorfile = inCfId;
END;
/

/* PL/SQL method deleting migration jobs of a castorfile */
CREATE OR REPLACE PROCEDURE deleteMigrationJobs(cfId NUMBER) AS
BEGIN
  DELETE /*+ INDEX (MigrationJob I_MigrationJob_CFVID) */
    FROM MigrationJob
   WHERE castorfile = cfId;
  DELETE /*+ INDEX (MigratedSegment I_MigratedSegment_CFCopyNbVID) */
    FROM MigratedSegment
   WHERE castorfile = cfId;
END;
/

/* PL/SQL method deleting migration jobs of a castorfile that was being recalled */
CREATE OR REPLACE PROCEDURE deleteMigrationJobsForRecall(cfId NUMBER) AS
BEGIN
  -- delete migration jobs waiting on this recall
  DELETE /*+ INDEX (MigrationJob I_MigrationJob_CFVID) */
    FROM MigrationJob
   WHERE castorFile = cfId AND status = tconst.MIGRATIONJOB_WAITINGONRECALL;
  -- delete migrated segments if no migration jobs remain
  DECLARE
    unused NUMBER;
  BEGIN
    SELECT /*+ INDEX_RS_ASC(MigrationJob I_MigrationJob_CFCopyNb) */ castorFile INTO unused
      FROM MigrationJob WHERE castorFile = cfId AND ROWNUM < 2;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    DELETE FROM MigratedSegment WHERE castorfile = cfId;
  END;
END;
/

/* PL/SQL method deleting recall jobs of a castorfile */
CREATE OR REPLACE PROCEDURE deleteRecallJobs(cfId NUMBER) AS
BEGIN
  -- Loop over the recall jobs
  DELETE FROM RecallJob WHERE castorfile = cfId;
  -- deal with potential waiting migrationJobs
  deleteMigrationJobsForRecall(cfId);
END;
/

/* PL/SQL method to delete a CastorFile only when no DiskCopy, no MigrationJob and no RecallJob are left for it */
/* Internally used in filesDeletedProc, putFailedProc and deleteOutOfDateDiskCopies */
CREATE OR REPLACE PROCEDURE deleteCastorFile(cfId IN NUMBER) AS
  nb NUMBER;
  LockError EXCEPTION;
  PRAGMA EXCEPTION_INIT (LockError, -54);
  CONSTRAINT_VIOLATED EXCEPTION;
  PRAGMA EXCEPTION_INIT(CONSTRAINT_VIOLATED, -2292);
BEGIN
  -- First try to lock the castorFile
  SELECT id INTO nb FROM CastorFile
   WHERE id = cfId FOR UPDATE NOWAIT;

  -- See whether pending SubRequests exist
  SELECT /*+ INDEX(Subrequest I_Subrequest_Castorfile)*/ count(*) INTO nb
    FROM SubRequest
   WHERE castorFile = cfId
     AND status IN (1, 2, 3, 4, 5, 6, 7, 10, 12, 13);   -- All but START, FINISHED, FAILED_FINISHED, ARCHIVED
  -- If any SubRequest, give up
  IF nb = 0 THEN
    DECLARE
      fid NUMBER;
      fc NUMBER;
      nsh VARCHAR2(2048);
    BEGIN
      -- Delete the CastorFile
      -- this will raise a constraint violation if any DiskCopy, MigrationJob, RecallJob or Disk2DiskCopyJob
      -- still exists for this CastorFile. It is caught and we give up with the deletion in such a case.
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
        INSERT INTO FilesDeletedProcOutput (fileId, nsHost) VALUES (fid, nsh);
      END IF;
    END;
  END IF;
EXCEPTION
  WHEN CONSTRAINT_VIOLATED THEN
    -- ignore, this means we still have some DiskCopy or Job around
    NULL;
  WHEN NO_DATA_FOUND THEN
    -- ignore, this means that the castorFile did not exist.
    -- There is thus no way to find out whether to remove the
    -- file from the nameserver. For safety, we thus keep it
    NULL;
  WHEN LockError THEN
    -- ignore, somebody else is dealing with this castorFile
    NULL;
END;
/

/* automatic logging procedure. The logs are then processed by the stager and sent to the rsyslog streams.
   Note that the log will be commited at the same time as the rest of the transaction */
CREATE OR REPLACE PROCEDURE logToDLFWithTime(logTime NUMBER,
                                             uuid VARCHAR2,
                                             priority INTEGER,
                                             msg VARCHAR2,
                                             fileId NUMBER,
                                             nsHost VARCHAR2,
                                             source VARCHAR2,
                                             params VARCHAR2) AS
  PRAGMA AUTONOMOUS_TRANSACTION;
BEGIN
  INSERT INTO DLFLogs (timeinfo, uuid, priority, msg, fileId, nsHost, source, params)
         VALUES (logTime, uuid, priority, msg, fileId, nsHost, source, params);
  COMMIT;
END;
/

/* automatic logging procedure. The logs are then processed by the stager and sent to the rsyslog streams.
   Note that the log will be commited at the same time as the rest of the transaction */
CREATE OR REPLACE PROCEDURE logToDLF(uuid VARCHAR2,
                                     priority INTEGER,
                                     msg VARCHAR2,
                                     fileId NUMBER,
                                     nsHost VARCHAR2,
                                     source VARCHAR2,
                                     params VARCHAR2) AS
BEGIN
  logToDLFWithTime(getTime(), uuid, priority, msg, fileId, nsHost, source, params);
END;
/

/* Small utility function to convert an hexadecimal string (8 digits) into a RAW(4) type */
CREATE OR REPLACE FUNCTION strToRaw4(v VARCHAR2) RETURN RAW IS
BEGIN
  RETURN hexToRaw(ltrim(to_char(to_number(v, 'XXXXXXXX'), '0XXXXXXX')));
END;
/

/* A wrapper to run DB jobs and catch+log any exception they may throw */
CREATE OR REPLACE PROCEDURE startDbJob(jobCode VARCHAR2, source VARCHAR2) AS
BEGIN
  EXECUTE IMMEDIATE jobCode;
EXCEPTION WHEN OTHERS THEN
  logToDLF(NULL, dlf.LVL_ALERT, dlf.DBJOB_UNEXPECTED_EXCEPTION, 0, '', source,
    'jobCode="'|| jobCode ||'" errorCode=' || to_char(SQLCODE) ||' errorMessage="' || SQLERRM
    ||'" stackTrace="' || dbms_utility.format_error_backtrace ||'"');
END;
/

/* A wrapper procedure to execute DBMS_ALERT.SIGNAL() without taking a lock if
 * already another session did it. Helps reducing contention on DBMS_ALERT_INFO.
 */
CREATE OR REPLACE PROCEDURE alertSignalNoLock(inName IN VARCHAR2) AS
  unused INTEGER;
BEGIN
  SELECT 1 INTO unused
    FROM SYS.DBMS_ALERT_INFO
   WHERE name = upper(inName) AND changed = 'Y'
     AND ROWNUM < 2;
EXCEPTION WHEN NO_DATA_FOUND THEN
  DBMS_ALERT.SIGNAL(inName, '');
END;
/

/* useful procedure to recompile all invalid items in the DB
   as many times as needed, until nothing can be improved anymore.
   Also reports the list of invalid items if any */
CREATE OR REPLACE PROCEDURE recompileAll AS
  varNbInvalids INTEGER;
  varNbInvalidsLastRun INTEGER := -1;
BEGIN
  WHILE varNbInvalidsLastRun != 0 LOOP
    varNbInvalids := 0;
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
    -- check how many invalids are still around
    SELECT count(*) INTO varNbInvalids FROM user_objects
     WHERE object_type IN ('PROCEDURE', 'TRIGGER', 'FUNCTION', 'VIEW', 'PACKAGE BODY') AND status = 'INVALID';
    -- should we give up ?
    IF varNbInvalids = varNbInvalidsLastRun THEN
      DECLARE
        varInvalidItems VARCHAR(2048);
      BEGIN
        -- yes, as we did not move forward on this run
        SELECT LISTAGG(object_name, ', ') WITHIN GROUP (ORDER BY object_name) INTO varInvalidItems
          FROM user_objects
         WHERE object_type IN ('PROCEDURE', 'TRIGGER', 'FUNCTION', 'VIEW', 'PACKAGE BODY') AND status = 'INVALID';
        raise_application_error(-20000, 'Revalidation of PL/SQL code failed. Still ' ||
                                        varNbInvalids || ' invalid items : ' || varInvalidItems);
      END;
    END IF;
    -- prepare for next loop
    varNbInvalidsLastRun := varNbInvalids;
    varNbInvalids := 0;
  END LOOP;
END;
/
