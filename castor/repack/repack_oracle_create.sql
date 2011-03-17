/* Stop on errors */
WHENEVER SQLERROR EXIT FAILURE;

/* SQL statements for type RepackRequest */
CREATE TABLE RepackRequest (machine VARCHAR2(2048), userName VARCHAR2(2048), creationTime INTEGER, pool VARCHAR2(2048), pid INTEGER, svcclass VARCHAR2(2048), stager VARCHAR2(2048), userId NUMBER, groupId NUMBER, retryMax INTEGER, reclaim NUMBER, finalPool VARCHAR2(2048), id INTEGER CONSTRAINT PK_RepackRequest_Id PRIMARY KEY, command INTEGER) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

/* SQL statements for type RepackSegment */
CREATE TABLE RepackSegment (fileid INTEGER, segsize INTEGER, compression NUMBER, filesec NUMBER, copyno NUMBER, blockid INTEGER, fileseq INTEGER, errorCode NUMBER, errorMessage VARCHAR2(2048), id INTEGER CONSTRAINT PK_RepackSegment_Id PRIMARY KEY, repacksubrequest INTEGER) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

/* SQL statements for type RepackSubRequest */
CREATE TABLE RepackSubRequest (vid VARCHAR2(2048), xsize INTEGER, filesMigrating NUMBER, filesStaging NUMBER, files NUMBER, filesFailed NUMBER, cuuid VARCHAR2(2048), submitTime INTEGER, filesStaged NUMBER, filesFailedSubmit NUMBER, retryNb INTEGER, id INTEGER CONSTRAINT PK_RepackSubRequest_Id PRIMARY KEY, repackrequest INTEGER, status INTEGER) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;


/* SQL statements for table UpgradeLog */
CREATE TABLE UpgradeLog (Username VARCHAR2(64) DEFAULT sys_context('USERENV', 'OS_USER') CONSTRAINT NN_UpgradeLog_Username NOT NULL, SchemaName VARCHAR2(64) DEFAULT 'REPACK' CONSTRAINT NN_UpgradeLog_SchemaName NOT NULL, Machine VARCHAR2(64) DEFAULT sys_context('USERENV', 'HOST') CONSTRAINT NN_UpgradeLog_Machine NOT NULL, Program VARCHAR2(48) DEFAULT sys_context('USERENV', 'MODULE') CONSTRAINT NN_UpgradeLog_Program NOT NULL, StartDate TIMESTAMP(6) WITH TIME ZONE DEFAULT sysdate, EndDate TIMESTAMP(6) WITH TIME ZONE, FailureCount NUMBER DEFAULT 0, Type VARCHAR2(20) DEFAULT 'NON TRANSPARENT', State VARCHAR2(20) DEFAULT 'INCOMPLETE', SchemaVersion VARCHAR2(20) CONSTRAINT NN_UpgradeLog_SchemaVersion NOT NULL, Release VARCHAR2(20) CONSTRAINT NN_UpgradeLog_Release NOT NULL);

/* SQL statements for check constraints on the UpgradeLog table */
ALTER TABLE UpgradeLog
  ADD CONSTRAINT CK_UpgradeLog_State
  CHECK (state IN ('COMPLETE', 'INCOMPLETE'));
  
ALTER TABLE UpgradeLog
  ADD CONSTRAINT CK_UpgradeLog_Type
  CHECK (type IN ('TRANSPARENT', 'NON TRANSPARENT'));

/* SQL statement to populate the intial release value */
INSERT INTO UpgradeLog (schemaVersion, release) VALUES ('-', '2_1_11_9001');

/* SQL statement to create the CastorVersion view */
CREATE OR REPLACE VIEW CastorVersion
AS
  SELECT decode(type, 'TRANSPARENT', schemaVersion,
           decode(state, 'INCOMPLETE', state, schemaVersion)) schemaVersion,
         decode(type, 'TRANSPARENT', release,
           decode(state, 'INCOMPLETE', state, release)) release,
         schemaName
    FROM UpgradeLog
   WHERE startDate =
     (SELECT max(startDate) FROM UpgradeLog);

/*******************************************************************
 *
 * @(#)RCSfile: oracleTrailer.sql,v  Revision: 1.28  Release Date: 2009/08/11 15:25:34  Author: gtaur 
 *
 * This file contains SQL code that is not generated automatically
 * and is inserted at the end of the generated code
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *******************************************************************/

/* SQL statement to populate the intial schema version */
UPDATE UpgradeLog SET schemaVersion = '2_1_8_0';

/* Sequence for indices */
CREATE SEQUENCE ids_seq CACHE 300;

/* SQL statements for object types */
CREATE TABLE Id2Type (id INTEGER CONSTRAINT PK_Id2Type_Id PRIMARY KEY, type NUMBER);
CREATE INDEX I_Id2Type_TypeId ON Id2Type (type, id);

CREATE INDEX I_RepackSegment_FileId ON RepackSegment (fileId);
CREATE INDEX I_RepackSeg_RepackSubReq ON RepackSegment (repackSubRequest);
CREATE INDEX I_RepackSubRequest_Status ON RepackSubRequest (status);

/* Get current time as a time_t. Not that easy in ORACLE */
CREATE OR REPLACE FUNCTION getTime RETURN NUMBER IS
  ret NUMBER;
BEGIN
  SELECT (SYSDATE - to_date('01-jan-1970 01:00:00','dd-mon-yyyy HH:MI:SS')) * (24*60*60) INTO ret FROM DUAL;
  RETURN ret;
END;
/

/* Global tables */
CREATE GLOBAL TEMPORARY TABLE listOfStrs(id VARCHAR(2048)) ON COMMIT DELETE ROWS;
CREATE GLOBAL TEMPORARY TABLE listOfIds(id NUMBER) ON COMMIT DELETE ROWS;

/* Packages and types */
CREATE OR REPLACE PACKAGE repack AS
  TYPE RepackRequest_Cur IS REF CURSOR RETURN RepackRequest%ROWTYPE;
  TYPE RepackSubRequest_Cur IS REF CURSOR RETURN RepackSubRequest%ROWTYPE;
  TYPE RepackSegment_Cur IS REF CURSOR RETURN RepackSegment%ROWTYPE;
  TYPE "cnumList" IS TABLE OF NUMBER INDEX BY binary_integer;
  TYPE "strList" IS TABLE OF VARCHAR2(2048) INDEX BY binary_integer;
END repack;
/

CREATE OR REPLACE TYPE "numList" IS TABLE OF INTEGER;
/

/* Table for cleaning  with default values */

/* Define a table for some configuration key-value pairs and populate it */
CREATE TABLE RepackConfig
  (class VARCHAR2(2048) CONSTRAINT NN_RepackConfig_Class NOT NULL, 
   key VARCHAR2(2048) CONSTRAINT NN_RepackConfig_Key NOT NULL, 
   value VARCHAR2(2048) CONSTRAINT NN_RepackConfig_Value NOT NULL, 
   description VARCHAR2(2048));

INSERT INTO RepackConfig 
  VALUES ('Repack', 'CleaningTimeout', '72', 'Timeout to clean archived repacksubrequest');


/* SQL procedures */


/* repack cleanup cronjob */

CREATE OR REPLACE PROCEDURE repackCleanup AS
  t INTEGER;
  srIds "numList";
  segIds "numList";
  rIds "numList";
BEGIN
  -- First perform some cleanup of old stuff:
  -- for each, read relevant timeout from configuration table
  SELECT TO_NUMBER(value) INTO t FROM RepackConfig
   WHERE class = 'Repack'
     AND key = 'CleaningTimeout'
     AND ROWNUM < 2;
  SELECT id BULK COLLECT INTO srIds
    FROM RepackSubrequest
   WHERE status = 8
     AND submittime < gettime() + t * 3600;

  IF srIds.COUNT > 0 THEN
    -- Delete segments
    FOR i IN srIds.FIRST .. srIds.LAST LOOP
      DELETE FROM RepackSegment WHERE RepackSubrequest = srIds(i)
        RETURNING id BULK COLLECT INTO segIds;
      FORALL j IN segIds.FIRST .. segIds.LAST 
        DELETE FROM Id2Type WHERE id = segIds(j);
    END LOOP;
    COMMIT;
  END IF;
  
  -- Delete subrequests
  FORALL i IN srIds.FIRST .. srIds.LAST 
    DELETE FROM RepackSubrequest WHERE id = srIds(i);
  FORALL i IN srIds.FIRST .. srIds.LAST  
    DELETE FROM id2type WHERE id = srIds(i);

  -- Delete requests without any other subrequest
  DELETE FROM RepackRequest A WHERE NOT EXISTS 
    (SELECT 'x' FROM RepackSubRequest WHERE A.id = repackrequest)
    RETURNING A.id BULK COLLECT INTO rIds;
  FORALL i IN rIds.FIRST .. rIds.LAST
    DELETE FROM Id2Type WHERE id = rIds(i);
  COMMIT;

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
             WHERE job_name IN ('REPACKCLEANUPJOB'))
  LOOP
    DBMS_SCHEDULER.DROP_JOB(j.job_name, TRUE);
  END LOOP;

  -- Create a db job to be run twice a day executing the cleanup procedure
  DBMS_SCHEDULER.CREATE_JOB(
      JOB_NAME        => 'repackCleanupJob',
      JOB_TYPE        => 'PLSQL_BLOCK',
      JOB_ACTION      => 'BEGIN repackCleanup(); END;',
      JOB_CLASS       => 'CASTOR_JOB_CLASS',
      START_DATE      => SYSDATE + 60/1440,
      REPEAT_INTERVAL => 'FREQ=HOURLY; INTERVAL=12',
      ENABLED         => TRUE,
      COMMENTS        => 'Database maintenance');
END;
/


/* PL/SQL method implementing changeAllSubRequestsStatus */

CREATE OR REPLACE PROCEDURE changeAllSubRequestsStatus
(st IN INTEGER, rsr OUT repack.RepackSubRequest_Cur) AS
  srIds "numList";
BEGIN
  -- RepackWorker subrequests -> ARCHIVED  
  IF st = 8 THEN
    -- JUST IF IT IS FINISHED OR FAILED
    UPDATE RepackSubrequest SET Status = 8 WHERE Status IN (4, 5) 
    RETURNING id BULK COLLECT INTO srIds;
  END IF; 
  OPEN rsr FOR
    SELECT vid, xsize, status, filesmigrating, filesstaging, files, filesfailed, cuuid, submittime, filesstaged, filesfailedsubmit, retrynb, id, repackrequest
      FROM RepackSubRequest WHERE id member of srIds;
END;
/

/* PL/SQL method implementing changeSubRequestsStatus */

CREATE OR REPLACE
PROCEDURE changeSubRequestsStatus
(tapeVids IN repack."strList", st IN INTEGER, rsr OUT repack.RepackSubRequest_Cur) AS
srId NUMBER;
BEGIN
 COMMIT; -- to flush the temporary table
 -- RepackWorker remove subrequests -> TOBEREMOVED 
 IF st = 6 THEN 
  	FOR i IN tapeVids.FIRST .. tapeVids.LAST LOOP
    --	 IF TOBECHECKED or TOBESTAGED or ONHOLD -> TOBECLEANED
    		UPDATE RepackSubrequest SET Status=3 WHERE Status in (0, 1, 9, 10, 11, 12) AND vid=tapeVids(i) AND ROWNUM <2  RETURNING id INTO srId; 
    		INSERT INTO listOfIds (id) VALUES (srId);  
    		
    --	 ONGOING -> TOBEREMOVED
    		UPDATE RepackSubrequest SET Status=6 WHERE Status=2 AND vid=tapeVids(i) AND ROWNUM <2  RETURNING id INTO srId;
    		INSERT INTO listOfIds (id) VALUES (srId);  
        
    	END LOOP;
  END IF;
  
 -- RepackWorker subrequests -> TOBERESTARTED 
  IF st = 7 THEN
  	FOR i IN tapeVids.FIRST .. tapeVids.LAST LOOP
    	-- JUST IF IT IS FINISHED OR FAILED
    	    	 UPDATE RepackSubrequest SET Status=7 WHERE Status IN (4, 5) AND vid=tapeVids(i) AND ROWNUM <2  RETURNING id INTO srId;
             INSERT INTO listOfIds (id) VALUES (srId);
    	END LOOP;
  END IF; 

 -- RepackWorker subrequests -> ARCHIVED  
  IF st = 8 THEN
  	FOR i IN tapeVids.FIRST .. tapeVids.LAST LOOP
    	-- JUST IF IT IS FINISHED OR FAILED 
    	    	UPDATE RepackSubrequest SET Status=8 WHERE Status IN (4, 5) AND vid=tapeVids(i) AND ROWNUM < 2 RETURNING id INTO srId;
    	    	INSERT INTO listOfIds(id) VALUES (srId);	
    	END LOOP;
  END IF;  
  OPEN rsr FOR
     SELECT vid, xsize, status, filesmigrating, filesstaging,files,filesfailed,cuuid,submittime,filesstaged,filesfailedsubmit,retrynb,id,repackrequest
      	FROM RepackSubRequest WHERE id in (select id from listOfIds); 
END;
/


/* PL/SQL method implementing getAllSubRequests */

CREATE OR REPLACE PROCEDURE getAllSubRequests (rsr OUT repack.RepackSubRequest_Cur ) AS
BEGIN 
 OPEN rsr FOR
   SELECT vid, xsize, status, filesmigrating, filesstaging, files, filesfailed, cuuid, submittime, filesstaged, filesfailedsubmit, retrynb, id, repackrequest
     FROM RepackSubRequest WHERE status != 8 ORDER BY submittime DESC; -- not ARCHIVED
END;
/


/* PL/SQL method implementing getSegmentsForSubRequest */

CREATE OR REPLACE PROCEDURE getSegmentsForSubRequest
( srId NUMBER,  rs OUT repack.RepackSegment_Cur) AS
BEGIN
 OPEN rs FOR
     SELECT fileid, segsize, compression, filesec, copyno, blockid, fileseq, errorcode, errormessage, id, repacksubrequest
       	FROM RepackSegment WHERE repacksubrequest=srId; -- not archived 
       	      	
END;
/

/* PL/SQL method implementing getSubRequestByVid */

CREATE OR REPLACE PROCEDURE getSubRequestByVid
( rvid IN VARCHAR2, rsr OUT repack.RepackSubRequest_Cur) AS
BEGIN
 OPEN rsr FOR
     SELECT vid, xsize, status, filesmigrating, filesstaging,files,filesfailed,cuuid,submittime,filesstaged,filesfailedsubmit,retrynb,id,repackrequest
       	FROM RepackSubRequest WHERE vid=rvid AND status!=8; -- not archived       	      	
END;
/


/* PL/SQL method implementing getSubRequestsByStatus */

CREATE OR REPLACE PROCEDURE getSubRequestsByStatus(st IN INTEGER, srs OUT repack.RepackSubRequest_Cur) AS
srIds "numList";
BEGIN 
-- File Checker st = TOBECHECKED
-- File Cleaner st = TOBECLEANED 
-- File Stager st = TOBESTAGED 
-- File Stager st = TOBEREMOVED
-- File Stager st = TOBERESTARTED
-- Repack Monitor st = ONGOING 
  OPEN srs FOR
     SELECT RepackSubRequest.vid, RepackSubRequest.xsize, RepackSubRequest.status,  RepackSubRequest.filesmigrating, RepackSubRequest.filesstaging, RepackSubRequest.files,RepackSubRequest.filesfailed,RepackSubRequest.cuuid,RepackSubRequest.submittime,RepackSubRequest.filesstaged,RepackSubRequest.filesfailedsubmit,RepackSubRequest.retrynb,RepackSubRequest.id,RepackSubRequest.repackrequest
       	FROM RepackSubRequest, RepackRequest WHERE RepackSubRequest.status=st and RepackRequest.id=RepackSubRequest.repackrequest ORDER BY RepackRequest.creationtime; 
END;
/

/* PL/SQL method implementing restartSubRequest */

CREATE OR REPLACE PROCEDURE restartSubRequest (srId IN NUMBER) AS
 oldVid VARCHAR2(2048);
 oldCuuid VARCHAR2(2048);
 oldRetrynb NUMBER;
 oldRepackrequest NUMBER;
 newSrId NUMBER;
BEGIN
  -- archive the old repacksubrequest
  UPDATE RepackSubRequest SET status=8 WHERE id=srId;
  -- attach a new repacksubrequest in TOBECHECKED
  SELECT  vid,cuuid, retrynb,repackrequest INTO oldVid, oldCuuid, oldRetrynb,oldRepackrequest
   FROM RepackSubRequest WHERE id=srId;
  INSERT INTO RepackSubrequest (vid, xsize, status, filesmigrating, filesstaging,files,filesfailed,cuuid,submittime,filesstaged,filesfailedsubmit,retrynb,id,repackrequest) 
    VALUES (oldVid, 0, 0, 0, 0, 0, 0,oldCuuid,0,0,0,oldRetrynb,ids_seq.nextval,oldRepackrequest) RETURN id INTO newSrId;
  INSERT INTO id2type (id,type) VALUES (newSrId,97); -- new repacksubrequest
  COMMIT;
END;
/

/* PL/SQL method implementing storeRequest */

CREATE OR REPLACE PROCEDURE storeRequest
( rmachine IN VARCHAR2, ruserName IN VARCHAR2, rcreationTime IN NUMBER, rpool IN VARCHAR2, rpid IN NUMBER,
  rsvcclass IN VARCHAR2, rcommand IN INTEGER, rstager IN VARCHAR2, 
  ruserid IN NUMBER, rgroupid IN NUMBER, rretrymax IN NUMBER, rreclaim IN INTEGER, rfinalPool IN VARCHAR2, listVids IN repack."strList",rsr  OUT repack.RepackSubRequest_Cur) AS
  rId NUMBER;
  srId NUMBER;
  unused NUMBER;
  counter INTEGER;
BEGIN
  COMMIT; -- to flush the temporary table
  INSERT INTO RepackRequest (machine, username, creationtime, pool, pid, svcclass, command, stager, userid, groupid, retryMax, reclaim, finalPool ,id) VALUES
  (rmachine,rusername,rcreationTime,rpool,rpid,rsvcclass,rcommand,rstager,ruserid,rgroupid,rretryMax,rreclaim,rfinalPool, ids_seq.nextval) RETURNING id INTO rId; 
  counter:=0; 
  FOR i IN listVids.FIRST .. listVids.LAST LOOP
  	BEGIN
  	  SELECT id INTO unused FROM RepackSubRequest WHERE vid=listVids(i) AND STATUS != 8 AND ROWNUM <2; -- ARCHIVED
  	EXCEPTION WHEN NO_DATA_FOUND THEN
  		INSERT INTO RepackSubRequest (vid,xsize,status,filesMigrating,filesstaging,files,filesfailed,cuuid,submittime,filesstaged,filesfailedsubmit,retrynb,id,repackrequest) VALUES
    	  	(listVids(i),0,0,0,0,0,0,0,0,0,0,rretryMax,ids_seq.nextval,rId) RETURNING id INTO srId;
    		INSERT INTO id2type (id,type) VALUES (srId, 97);
    		counter:=counter+1;
    	END;
    	INSERT INTO listOfStrs (id) VALUES (listVids(i));
  END LOOP;
  -- if there are no repack subrequest valid I delete the request
  IF counter <> 0 THEN 
  	INSERT INTO id2type (id,type) VALUES (rId, 96);
  ELSE 
        DELETE FROM RepackRequest WHERE id=rId;
  END IF;
  OPEN rsr FOR
     SELECT vid, xsize, status, 
     filesmigrating, filesstaging,files,filesfailed,cuuid,submittime,
     filesstaged,filesfailedsubmit,retrynb,id,repackrequest
       	FROM RepackSubRequest
       	 WHERE vid IN (SELECT id FROM listOfStrs ) AND status=0; -- TOBECHECKED
END;
/


/* PL/SQL method implementing updateSubRequestSegments */


create or replace procedure updatesubrequestsegments
(srId IN NUMBER, fileIds IN repack."cnumList",
errorCodes IN repack."cnumList",
errorMessages IN repack."strList") AS
BEGIN
   FORALL i in fileIds.FIRST .. fileIds.LAST
       UPDATE RepackSegment SET errorCode=errorCodes(i),errorMessage=errorMessages(i)
	  WHERE (fileid=fileIds(i) OR fileIds(i)=0) AND repacksubrequest=srId;
   COMMIT;
END;
/

CREATE OR REPLACE FUNCTION updateOnHoldSubRequestStatus(
/**
 * This procedure updates the status of the repack sub-request with the
 * specified database ID and returns the new state.
 *
 * @param inMaxFiles     The maximum number of files repack should be processing
 *                       at any single moment in time.
 * @param inMaxTapes     The maximum number of tapes epack should be processing
 *                       at any single moment in time.
 * @param inFilesOnGoing The total number of files currently being processed by
 *                       repack.
 * @param inTapesOnGoing The total number of tape currently being processed by
 *                       repack.
 * @param inSubRequest   The repack sub-request whose status should be updated.
 * @return               The new status of the repack sub-request.
 */
  inMaxFiles        INTEGER,
  inMaxTapes        INTEGER,
  inFilesOnGoing    INTEGER,
  inTapesOnGoing    INTEGER,
  inSubRequest      RepackSubRequest%ROWTYPE)
RETURN INTEGER AS
  varNewStatus INTEGER := NULL;
  varTmpStatus INTEGER := NULL;
BEGIN
  -- By default assume no state change
  varNewStatus := inSubRequest.status;

  -- The precedence of the on-hold statuses is ONHOLD_MAXTAPES,
  -- ONHOLD_MAXFILES and then ONHOLD_MULTICOPY where ONHOLD_MAXTAPES has the
  -- highest precedence.

  -- Determine whether status should be changed to ONHOLD_MAXTAPES,
  -- ONHOLD_MAXFILES or neither.
  IF inTapesOnGoing + 1 > inMaxTapes THEN
    varTmpStatus := 10; -- ONHOLD_MAXTAPES
  ELSIF inFilesOnGoing + inSubRequest.files > inMaxFiles THEN
    varTmpStatus := 11; -- ONHOLD_MAXFILES
  ELSE
    varTmpStatus := NULL;
  END IF;

  -- Update the status if it should be changed to either ONHOLD_MAXTAPES or
  -- ONHOLD_MAXFILES
  IF varTmpStatus is not NULL THEN
    -- Update the status checking the orginal is still on-hold due to possible
    -- multi-threaded access
    UPDATE RepackSubRequest
       SET RepackSubRequest.status = varTmpStatus
     WHERE RepackSubRequest.status IN (9, 10, 11, 12) -- All ONHOLD statuses
       AND RepackSubRequest.id = inSubRequest.id
    RETURNING RepackSubRequest.status INTO varNewStatus;
  ELSE
    -- Try to change the status to TOBESTAGED and get the number of additional
    -- new files that will be processed by repack as a result.
    --
    -- This state change may fail due to two reasons.  Firstly the tape may
    -- contain files that have copies that are currently being processed by
    -- repack.  Secondly the sub-request may have been modified by another
    -- thread.
    UPDATE RepackSubRequest 
      SET RepackSubRequest.status = 1 -- TOBESTAGED
    WHERE RepackSubRequest.id = inSubRequest.id
      AND RepackSubRequest.status IN (9, 10, 11, 12) -- On-hold statuses
      AND inFilesOnGoing + RepackSubRequest.files <= inMaxFiles
      AND inTapesOnGoing + 1 <= inMaxTapes
      AND NOT EXISTS (
            SELECT 'x' 
              FROM RepackSegment
             WHERE RepackSegment.RepackSubRequest = inSubRequest.id 
               AND RepackSegment.fileId IN (
                     SELECT DISTINCT RepackSegment.fileid 
                       FROM RepackSegment
                      WHERE RepackSegment.RepackSubrequest IN (
                              SELECT RepackSubRequest.id 
                                FROM RepackSubRequest
                               WHERE RepackSubRequest.id != inSubRequest.id
                                 AND RepackSubRequest.status NOT IN (4,5,8,9,10,11,12)))) -- FINISHED FAILED ARCHIVED ONHOLD ONHOLD_MAXTAPES ONHOLD_MAXFILES ONHOLD_MULTICOPY
    RETURNING RepackSubRequest.status
         INTO varNewStatus;

    -- If we failed to change the status to TOBESTAGED then try to change it to
    -- ONHOLD_MULTICOPY which may fail if the sub-request has been modified by
    -- another thread.
    IF SQL%NOTFOUND THEN
      UPDATE RepackSubRequest
         SET RepackSubRequest.status = 12 -- ONHOLD_MULTICOPY
       WHERE id = inSubRequest.id
         AND RepackSubRequest.status IN (9, 10, 11, 12) -- On-hold statuses
         AND EXISTS (
            SELECT 'x'
              FROM RepackSegment
             WHERE RepackSegment.RepackSubRequest = inSubRequest.id
               AND RepackSegment.fileId IN (
                     SELECT DISTINCT RepackSegment.fileid
                       FROM RepackSegment
                      WHERE RepackSegment.RepackSubrequest IN (
                              SELECT RepackSubRequest.id
                                FROM RepackSubRequest
                               WHERE RepackSubRequest.id != inSubRequest.id
                                 AND RepackSubRequest.status NOT IN (4,5,8,9,10,11,12)))) -- FINISHED FAILED ARCHIVED ONHOLD ONHOLD_MAXTAPES ONHOLD_MAXFILES ONHOLD_MULTICOPY
      RETURNING RepackSubRequest.status INTO varNewStatus;
    END IF;

  END IF;

  RETURN varNewStatus;

END updateOnHoldSubRequestStatus;
/


CREATE OR REPLACE PROCEDURE resurrectTapesOnHold(
/**
 * This procedure updates the status of all the subrequests (tapes) currently
 * on-hold.  Some sub-requests will remain on-hold, maybe changing the reason
 * they are on-hold (max tapes, max files or multi-copy).  Some tapes may be
 * updated to status TOBESTAGED.
 *
 * @param inMaxFiles The maximum number of files repack should be processing at
 *                   any single moment in time.
 * @param inMaxTapes The maximum number of tapes epack should be processing at
 *                   any single moment in time.
 */
  inMaxFiles IN INTEGER,
  inMaxTapes IN INTEGER)
AS
  varFilesOnGoing INTEGER := 0;
  varTapesOnGoing INTEGER := 0;
  varNewStatus    NUMBER  := 0;
BEGIN
  -- Determine the total number of tapes and files currently being processed
  -- by repack
  SELECT COUNT(vid),
         NVL(SUM(filesStaging) + SUM(filesMigrating), 0)
    INTO varTapesOnGoing, varFilesOnGoing
    FROM RepackSubrequest
   WHERE status IN (
           1,  -- TOBESTAGED
           2); -- ONGOING

  -- For each on-hold sub-request order by creation time
  FOR curSubRequest IN (
    SELECT RepackSubRequest.*
      FROM RepackSubRequest
     INNER JOIN RepackRequest
        ON (RepackSubRequest.repackRequest = RepackRequest.id)
     WHERE RepackSubRequest.status IN (9, 10, 11, 12) -- All ONHOLD statuses 
     ORDER BY RepackRequest.creationTime)
  LOOP

    varNewStatus := updateOnHoldSubRequestStatus(
      inMaxFiles,
      inMaxTapes,
      varFilesOnGoing,
      varTapesOnGoing,
      curSubRequest);

    IF varNewStatus = 1 /* TOBESTAGED */ THEN
      varFilesOnGoing := varFilesOnGoing + curSubRequest.files;
      varTapesOnGoing := varTapesOnGoing + 1;
    END IF;

    COMMIT;
  END LOOP;
END resurrectTapesOnHold;
/


CREATE OR REPLACE PROCEDURE validateRepackSubRequest(
/**
* Validates and commits a repack subrequest after submition , i.e. should we
* process it or should we put it ONHOLD
*
* Please not this procedure will intentionally commit the insert of a previous
* function.  Probably a very bad design.
*
* @param inSubRequestId Subrequest id to validate.
* @param inMaxFiles     Repack MAXFILES limit from castor.conf
* @param inMaxTapes     Repack MAXTAPES limit from castor.conf
*
* @param outRet         Returns true if the validation is successful or false
*                       in other case.
*/
 inSubRequestId IN NUMBER,
 inMaxFiles     IN INTEGER,
 inMaxTapes     IN INTEGER,
 outRet        OUT INT)
AS
 varFilesOnGoing INTEGER := 0;
 varTapesOnGoing INTEGER := 0;
 varSubRequest   RepackSubRequest%ROWTYPE;
 varNewStatus    INTEGER := NULL;
BEGIN
  outRet := 0; -- Assume the request is invalid until proven otherwise

  BEGIN
    -- Determine the total number of tapes and files currently being processed
    -- by repack
    SELECT COUNT(vid),
           NVL(SUM(filesStaging) + SUM(filesMigrating), 0)
      INTO varTapesOnGoing, varFilesOnGoing
      FROM RepackSubrequest
     WHERE status IN (
             1,  -- TOBESTAGED
             2); -- ONGOING

    -- Retrieve the subrequest information for the update procedure
    SELECT RepackSubRequest.*
      INTO varSubRequest
      FROM RepackSubRequest
     WHERE id = inSubRequestId;

    varNewStatus := updateOnHoldSubRequestStatus(
      inMaxFiles,
      inMaxTapes,
      varFilesOnGoing,
      varTapesOnGoing,
      varSubRequest);

  EXCEPTION  WHEN NO_DATA_FOUND THEN
    NULL; -- Do nothing
  END;

  COMMIT;

  IF varNewStatus = 1 /* TOBESTAGED */ THEN
    outRet := 1;
  END IF;

END validateRepackSubRequest;
/


/* Flag the schema creation as COMPLETE */
UPDATE UpgradeLog SET endDate = sysdate, state = 'COMPLETE';
COMMIT;
