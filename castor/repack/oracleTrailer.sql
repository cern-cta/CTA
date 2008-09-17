/*******************************************************************
 *
 * @(#)$RCSfile: oracleTrailer.sql,v $ $Revision: 1.10 $ $Release$ $Date: 2008/09/17 15:27:39 $ $Author: gtaur $
 *
 * This file contains SQL code that is not generated automatically
 * and is inserted at the end of the generated code
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *******************************************************************/

/* A small table used to cross check code and DB versions */
UPDATE CastorVersion SET schemaVersion = '2_1_8_0';

/* Sequence for indices */
CREATE SEQUENCE ids_seq CACHE 300;

/* SQL statements for object types */
CREATE TABLE Id2Type (id INTEGER PRIMARY KEY, type NUMBER);
CREATE INDEX I_Id2Type_typeId on Id2Type (type, id);

/* Get current time as a time_t. Not that easy in ORACLE */
CREATE OR REPLACE FUNCTION getTime RETURN NUMBER IS
  ret NUMBER;
BEGIN
  SELECT (SYSDATE - to_date('01-jan-1970 01:00:00','dd-mon-yyyy HH:MI:SS')) * (24*60*60) INTO ret FROM DUAL;
  RETURN ret;
END;

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

CREATE OR REPLACE TYPE "numList" IS TABLE OF INTEGER;

/* SQL procedures */

/* repack cleanup cronjob */

create or replace PROCEDURE repackCleanup AS
  t INTEGER;
  srIds "numList";
  rIds "numList";
BEGIN
  -- First perform some cleanup of old stuff:
  -- for each, read relevant timeout from configuration table
  SELECT TO_NUMBER(value) INTO t FROM RepackConfig
   WHERE class = 'Repack' AND key = 'CleaningTimeout' AND ROWNUM < 2;
  SELECT id BULK COLLECT INTO srIds FROM RepackSubrequest WHERE status=8 AND submittime < gettime() + t*3600 FOR UPDATE;
  DELETE FROM id2type where id IN (Select id FROM RepackSegment WHERE RepackSubrequest MEMBER OF srIds);
  DELETE FROM RepackSegment WHERE RepackSubrequest MEMBER OF srIds;
  DELETE FROM id2type WHERE id MEMBER OF srIds;
  DELETE FROM RepackSubrequest WHERE id MEMBER OF srIds;
  DELETE FROM RepackRequest A WHERE NOT EXISTS (SELECT id FROM RepackSubRequest WHERE A.id=RepackSubRequest.repackrequest) RETURNING A.id BULK COLLECT INTO rIds;
  DELETE FROM id2type WHERE id MEMBER OF rIds;
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
  COMMIT;
END;



/* PL/SQL method implementing changeAllSubRequestsStatus */

create or replace PROCEDURE changeAllSubRequestsStatus
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

/* PL/SQL method implementing changeSubRequestsStatus */

create or replace PROCEDURE changeSubRequestsStatus
(tapeVids IN repack."strList", st IN INTEGER, rsr OUT repack.RepackSubRequest_Cur) AS
srId NUMBER;
BEGIN
 COMMIT; -- to flush the temporary table
 -- RepackWorker remove subrequests -> TOBEREMOVED 
 IF st = 6 THEN 
  	FOR i IN tapeVids.FIRST .. tapeVids.LAST LOOP
    --	 IF TOBECHECKED or TOBESTAGED -> TOBECLEANED
    		UPDATE RepackSubrequest SET Status=3 WHERE Status in (0, 1) AND vid=tapeVids(i) RETURNING id INTO srId; 
    		INSERT INTO listOfIds (id) VALUES (srId);  
    		
    --	 ONGOING -> TOBEREMOVED
    		UPDATE RepackSubrequest SET Status=6 WHERE Status=2 AND vid=tapeVids(i) RETURNING id INTO srId;
    		INSERT INTO listOfIds (id) VALUES (srId);  
    	END LOOP;
  END IF;
  
 -- RepackWorker subrequests -> TOBERESTARTED 
  IF st = 7 THEN
  	FOR i IN tapeVids.FIRST .. tapeVids.LAST LOOP
    	-- IF IT IS NOT ARCHIVED, NOT ONGOING
    	    	 UPDATE RepackSubrequest SET Status=7 WHERE Status NOT IN ( 2, 8 ) AND vid=tapeVids(i) RETURNING id INTO srId;
    	         INSERT INTO listOfIds (id) VALUES (srId);
    	END LOOP;
  END IF; 

 -- RepackWorker subrequests -> ARCHIVED  
  IF st = 8 THEN
  	FOR i IN tapeVids.FIRST .. tapeVids.LAST LOOP
    	-- JUST IF IT IS FINISHED OR FAILED
    	    	UPDATE RepackSubrequest SET Status=8 WHERE Status IN (4, 5, 9) AND vid=tapeVids(i) RETURNING id INTO srId;
    	    	INSERT INTO listOfIds(id) VALUES (srId);	
    	END LOOP;
  END IF;  
  OPEN rsr FOR
     SELECT vid, xsize, status, filesmigrating, filesstaging,files,filesfailed,cuuid,submittime,filesstaged,filesfailedsubmit,retrynb,id,repackrequest
      	FROM RepackSubRequest WHERE id in (select id from listOfIds); 
END;


/* PL/SQL method implementing getAllSubRequests */

create or replace PROCEDURE getAllSubRequests (rsr OUT repack.RepackSubRequest_Cur ) AS
BEGIN 
 OPEN rsr FOR
   SELECT vid, xsize, status, filesmigrating, filesstaging, files, filesfailed, cuuid, submittime, filesstaged, filesfailedsubmit, retrynb, id, repackrequest
     FROM RepackSubRequest WHERE status != 8 ORDER BY submittime DESC; -- not ARCHIVED
END;


/* PL/SQL method implementing getSegmentsForSubRequest */

create or replace PROCEDURE getSegmentsForSubRequest
( srId NUMBER,  rs OUT repack.RepackSegment_Cur) AS
BEGIN
 OPEN rs FOR
     SELECT fileid, segsize, compression, filesec, copyno, blockid, fileseq, errorcode, errormessage, id, repacksubrequest
       	FROM RepackSegment WHERE repacksubrequest=srId; -- not archived 
       	      	
END;

/* PL/SQL method implementing getSubRequestByVid */

create or replace PROCEDURE getSubRequestByVid
( rvid IN VARCHAR2, rsr OUT repack.RepackSubRequest_Cur) AS
BEGIN
 OPEN rsr FOR
     SELECT vid, xsize, status, filesmigrating, filesstaging,files,filesfailed,cuuid,submittime,filesstaged,filesfailedsubmit,retrynb,id,repackrequest
       	FROM RepackSubRequest WHERE vid=rvid AND status!=8; -- not archived       	      	
END;


/* PL/SQL method implementing getSubRequestsByStatus */

create or replace PROCEDURE getSubRequestsByStatus(st IN INTEGER, srs OUT repack.RepackSubRequest_Cur) AS
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

/* PL/SQL method implementing restartSubRequest */

create or replace PROCEDURE restartSubRequest (srId IN NUMBER) AS
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

/* PL/SQL method implementing resurrectTapesOnHold */
create or replace PROCEDURE              resurrectTapesOnHold (maxFiles IN INTEGER, maxTapes IN INTEGER)AS
filesOnGoing INTEGER;
tapesOnGoing INTEGER;
newFiles NUMBER;
BEGIN
	SELECT count(vid), sum(filesStaging) + sum(filesMigrating) INTO  tapesOnGoing, filesOnGoing FROM RepackSubrequest WHERE  status IN (1,2); -- TOBESTAGED ONGOING 
-- Set the subrequest to TOBESTAGED FROM ON-HOLD if there is no ongoing repack for any of the files on the tape
	FOR sr IN (SELECT RepackSubRequest.id FROM RepackSubRequest,RepackRequest WHERE  RepackRequest.id=RepackSubrequest.repackrequest AND RepackSubRequest.status=9 ORDER BY RepackRequest.creationTime ) LOOP 
		BEGIN 
			UPDATE RepackSubRequest SET status=1 WHERE id=sr.id AND status=9
			AND filesOnGoing + files <= maxFiles AND tapesOnGoing+1 <= maxTapes
			AND NOT EXISTS (SELECT 'x' FROM RepackSegment WHERE 
				RepackSegment.RepackSubRequest=sr.id AND 
				RepackSegment.fileid IN (SELECT DISTINCT RepackSegment.fileid FROM RepackSegment
			             WHERE RepackSegment.RepackSubrequest 
			             	IN (SELECT RepackSubRequest.id FROM RepackSubRequest WHERE RepackSubRequest.id<>sr.id AND RepackSubRequest.status NOT IN (4,5,8,9) 
			             	 )
				) 
			) RETURNING files INTO newFiles; -- FINISHED ARCHIVED FAILED ONHOLD
			COMMIT;  
			filesOnGoing:=filesOnGoing+newFiles;
			tapesOnGoing:=tapesOnGoing+1;
		EXCEPTION WHEN NO_DATA_FOUND THEN
		NULL;
		END;
	END LOOP;
END;


/* PL/SQL method implementing storeRequest */

create or replace PROCEDURE storeRequest
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


/* PL/SQL method implementing updateSubRequestSegments */

create or replace PROCEDURE updateSubRequestSegments 
 (srId IN NUMBER, fileIds IN repack."cnumList",
  errorCodes IN repack."cnumList",
  errorMessages IN repack."strList") AS 
 BEGIN
 	FOR i in fileIds.FIRST .. fileIds.LAST LOOP	
 		UPDATE RepackSegment SET errorCode=errorCodes(i), errorMessage=errorMessages(i)
 			WHERE (fileid=fileIds(i) OR fileIds(i)=0) AND repacksubrequest=srId;
 	END LOOP;
 	COMMIT;
 END;


/* PL/SQL method implementing validateRepackSubRequest */

create or replace PROCEDURE  validateRepackSubRequest(srId IN NUMBER, maxFiles IN INTEGER, maxTapes IN INTEGER, ret OUT INT) AS
unused NUMBER;
filesOnGoing INTEGER;
tapesOnGoing INTEGER;
BEGIN
SELECT count(vid), sum(filesStaging) + sum(filesMigrating) INTO  tapesOnGoing, filesOnGoing FROM RepackSubrequest WHERE  status IN (1,2); -- TOBESTAGED ONGOING 
IF filesongoing is NULL THEN
  filesongoing:=0;
END IF;
-- Set the subrequest to TOBESTAGED FROM ON-HOLD if there is no ongoing repack for any of the files on the tape
	UPDATE RepackSubRequest SET status=1 WHERE id=srId  
		AND NOT EXISTS (SELECT 'x' FROM RepackSegment WHERE 
			RepackSegment.RepackSubRequest=srId AND 
			RepackSegment.fileid IN (SELECT DISTINCT RepackSegment.fileid FROM RepackSegment
			             WHERE RepackSegment.RepackSubrequest 
			             	IN (SELECT RepackSubRequest.id FROM RepackSubRequest WHERE RepackSubRequest.id<>srId AND RepackSubRequest.status NOT IN(4,5,8,9) 
			             	 )
			) 
		) AND filesOnGoing + files <= maxFiles AND tapesOnGoing+1 <= maxTapes RETURNING id INTO unused; -- FINISHED ARCHIVED FAILED ONHOLD
		ret:=1;
		COMMIT;
EXCEPTION  WHEN NO_DATA_FOUND THEN
  ret := 0;
END;


