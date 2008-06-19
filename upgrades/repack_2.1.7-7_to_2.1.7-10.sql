/******************************************************************************
 *              repack_2.1.7-7_to_2.1.7-9.sql
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
 * @(#)$RCSfile: repack_2.1.7-7_to_2.1.7-10.sql,v $ $Release: 1.2 $ $Release$ $Date: 2008/06/19 15:08:56 $ $Author: gtaur $
 *
 * This script upgrades a CASTOR v2.1.7-7 REPACK database to 2.1.7-8
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

/* Stop on errors - this only works from sqlplus and sql developer */
WHENEVER SQLERROR EXIT FAILURE;

/* Version cross check and update */
DECLARE
  unused VARCHAR(100);
BEGIN
  SELECT release INTO unused FROM CastorVersion WHERE release LIKE '2_1_7_7%';
EXCEPTION WHEN NO_DATA_FOUND THEN
  -- Error, we can't apply this script
 raise_application_error(-20000, 'PL/SQL revision mismatch. Please run previous upgrade scripts before this one.');
END;

UPDATE CastorVersion SET schemaversion='2_1_7_9';
UPDATE CastorVersion SET release = '2_1_7_9';

/* Schema changes */

ALTER TABLE RepackSegment ADD errorCode NUMBER;
ALTER TABLE RepackSegment ADD errorMessage VARCHAR2(2048);
UPDATE RepackSegment SET  errorCode=0 WHERE errorCode IS NULL;

-- SUBREQUEST_READYFORSTAGING => RSUBREQUEST_TOBECHECKED 
UPDATE RepackSubRequest SET status=0 WHERE status =1;

-- SUBREQUEST_STAGING => RSUBREQUEST_ONGOING
UPDATE RepackSubRequest SET status=2 WHERE status = 2;

-- SUBREQUEST_MIGRATING => RSUBREQUEST_ONGOING
UPDATE RepackSubRequest SET status=2 WHERE status  = 3;

-- SUBREQUEST_READYFORCLEANUP => RSUBREQUEST_TOBECLEANED
UPDATE RepackSubRequest SET status=3 WHERE status = 4;

-- SUBREQUEST_DONE  =>  RSUBREQUEST_DONE         
UPDATE RepackSubRequest SET status=4  WHERE status = 5;

-- SUBREQUEST_TOBESTAGED =>  RSUBREQUEST_TOBESTAGED      
UPDATE RepackSubRequest SET status=1 WHERE status = 8;

--SUBREQUEST_ARCHIVED =>  RSUBREQUEST_ARCHIVED       
UPDATE RepackSubRequest SET status=8 WHERE status = 6;
    
--SUBREQUEST_RESTART => RSUBREQUEST_TOBERESTARTED         
UPDATE RepackSubRequest SET status=7 WHERE status = 7;


--SUBREQUEST_FAILED =>  RSUBREQUEST_FAILED
UPDATE RepackSubRequest SET status=5 WHERE status = 9;

--SUBREQUEST_TOBEREMOVED => RSUBREQUEST_TOBEREMOVED
UPDATE RepackSubRequest SET status=6 WHERE status = 10;

-- REPACK

UPDATE RepackRequest SET  command=1  WHERE command=2;
   
--RESTART 
UPDATE RepackRequest SET  command=2  WHERE command in (3,12);

-- REMOVE
UPDATE RepackRequest SET  command=3  WHERE command=4;

-- GET_STATUS 
UPDATE RepackRequest SET  command=4 WHERE command=6;

--GET_STATUS_ALL 
UPDATE RepackRequest SET  command=5 WHERE command= 8;

--ARCHIVE
UPDATE RepackRequest SET  command=6 WHERE command=14;

--ARCHIVE_ALL 
UPDATE RepackRequest SET  command=7 WHERE command=15;

--  GET_NS_STATUS 
UPDATE RepackRequest SET  command = 8 WHERE command=16;

-- GET_ERROR 
UPDATE RepackRequest SET  command = 9 WHERE command=17;

/* Code  */

 /* Global table */

CREATE GLOBAL TEMPORARY TABLE listOfStrs(id VARCHAR(2048))ON COMMIT DELETE ROWS;
CREATE GLOBAL TEMPORARY TABLE listOfIds(id NUMBER)ON COMMIT DELETE ROWS;



/* Package and type */

CREATE OR REPLACE PACKAGE repack AS
  TYPE RepackRequest_Cur IS REF CURSOR RETURN RepackRequest%ROWTYPE;
  TYPE RepackSubRequest_Cur IS REF CURSOR RETURN RepackSubRequest%ROWTYPE;
  TYPE RepackSegment_Cur IS REF CURSOR RETURN RepackSegment%ROWTYPE;
  TYPE "cnumList" IS TABLE OF NUMBER index by binary_integer;
  TYPE "strList" IS TABLE OF VARCHAR2(2048) index by binary_integer;
END repack;

CREATE OR REPLACE TYPE "numList" IS TABLE OF INTEGER;

/* SQL procedure */

CREATE OR REPLACE PROCEDURE changeAllSubRequestsStatus
(st IN INTEGER, rsr OUT repack.RepackSubRequest_Cur) AS
 srIds "numList";
BEGIN
 -- RepackWorker subrequests -> ARCHIVED  
  IF st = 8 THEN
    -- JUST IF IT IS FINISHED OR FAILED
  	UPDATE RepackSubrequest SET Status=8 WHERE Status IN (4, 5) 
  	RETURNING id BULK COLLECT INTO srIds;
  END IF; 
  OPEN rsr FOR
     SELECT vid, xsize, status, filesmigrating, filesstaging,files,filesfailed,cuuid,submittime,filesstaged,filesfailedsubmit,retrynb,id,repackrequest
       	FROM RepackSubRequest WHERE id member of srIds;
END;

/* */

CREATE OR REPLACE PROCEDURE changeSubRequestsStatus
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
    	    	UPDATE RepackSubrequest SET Status=8 WHERE Status IN (4, 5) AND vid=tapeVids(i) RETURNING id INTO srId;
    	    	INSERT INTO listOfIds(id) VALUES (srId);	
    	END LOOP;
  END IF;  
  OPEN rsr FOR
     SELECT vid, xsize, status, filesmigrating, filesstaging,files,filesfailed,cuuid,submittime,filesstaged,filesfailedsubmit,retrynb,id,repackrequest
      	FROM RepackSubRequest WHERE id in (select id from listOfIds); 
END;

/* */

CREATE OR REPLACE PROCEDURE getAllSubRequests (rsr OUT repack.RepackSubRequest_Cur ) AS
BEGIN 
 OPEN rsr FOR
     SELECT vid, xsize, status, filesmigrating, filesstaging,files,filesfailed,cuuid,submittime,filesstaged,filesfailedsubmit,retrynb,id,repackrequest
       	FROM RepackSubRequest WHERE status !=8 ORDER BY submittime; -- not ARCHIVED 
END;

/* */


CREATE OR REPLACE PROCEDURE getSegmentsForSubRequest
( srId NUMBER,  rs OUT repack.RepackSegment_Cur) AS
BEGIN
 OPEN rs FOR
     SELECT fileid, segsize, compression, filesec, copyno, blockid, fileseq, errorcode, errormessage, id, repacksubrequest
       	FROM RepackSegment WHERE repacksubrequest=srId; -- not archived 
       	      	
END;

/* */

CREATE OR REPLACE PROCEDURE getSubRequestByVid
( rvid IN VARCHAR2, rsr OUT repack.RepackSubRequest_Cur) AS
BEGIN
 OPEN rsr FOR
     SELECT vid, xsize, status, filesmigrating, filesstaging,files,filesfailed,cuuid,submittime,filesstaged,filesfailedsubmit,retrynb,id,repackrequest
       	FROM RepackSubRequest WHERE vid=rvid AND status!=8; -- not archived       	      	
END;

/* */

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
     SELECT vid, xsize, status, filesmigrating, filesstaging,files,filesfailed,cuuid,submittime,filesstaged,filesfailedsubmit,retrynb,id,repackrequest
       	FROM RepackSubRequest WHERE status=st; 
END;

/* */

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

/* */

CREATE OR REPLACE PROCEDURE resurrectTapesOnHold AS
BEGIN
-- Set the subrequest to TOBESTAGED FROM ON-HOLD if there is no ongoing repack for any of the files on the tape
	FOR sr IN (SELECT id FROM RepackSubRequest WHERE  status=9 ) LOOP 
		UPDATE RepackSubRequest SET status=1 WHERE id=sr.id AND status=9
		AND NOT EXISTS (SELECT 'x' FROM RepackSegment WHERE 
			RepackSegment.RepackSubRequest=sr.id AND 
			RepackSegment.fileid IN (SELECT DISTINCT RepackSegment.fileid FROM RepackSegment
			             WHERE RepackSegment.RepackSubrequest 
			             	IN (SELECT RepackSubRequest.id FROM RepackSubRequest WHERE RepackSubRequest.id<>sr.id AND RepackSubRequest.status NOT IN (4,5,8,9) 
			             	 )
			) 
		); -- FINISHED ARCHIVED FAILED ONHOLD
		COMMIT;  
	END LOOP;
END;

/* */

CREATE OR REPLACE PROCEDURE storeRequest
( rmachine IN VARCHAR2, ruserName IN VARCHAR2, rcreationTime IN NUMBER, rpool IN VARCHAR2, rpid IN NUMBER,
  rsvcclass IN VARCHAR2, rcommand IN INTEGER, rstager IN VARCHAR2, 
  ruserid IN NUMBER, rgroupid IN NUMBER, rretrymax IN NUMBER, listVids IN repack."strList",rsr  OUT repack.RepackSubRequest_Cur) AS
  rId NUMBER;
  srId NUMBER;
  unused NUMBER;
  counter INTEGER;
BEGIN
  COMMIT; -- to flush the temporary table
  INSERT INTO RepackRequest (machine, username, creationtime, pool, pid, svcclass, command, stager, userid, groupid, retryMax,id) VALUES
  (rmachine,rusername,rcreationTime,rpool,rpid,rsvcclass,rcommand,rstager,ruserid,rgroupid,rretryMax, ids_seq.nextval) RETURNING id INTO rId; 
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

/* */

CREATE OR REPLACE PROCEDURE updateSubRequestSegments 
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

/* */

CREATE OR REPLACE PROCEDURE validateRepackSubRequest(srId IN NUMBER, ret OUT INT) AS
unused NUMBER;
BEGIN
-- Set the subrequest to TOBESTAGED FROM ON-HOLD if there is no ongoing repack for any of the files on the tape
	UPDATE RepackSubRequest SET status=1 WHERE id=srId  
		AND NOT EXISTS (SELECT 'x' FROM RepackSegment WHERE 
			RepackSegment.RepackSubRequest=srId AND 
			RepackSegment.fileid IN (SELECT DISTINCT RepackSegment.fileid FROM RepackSegment
			             WHERE RepackSegment.RepackSubrequest 
			             	IN (SELECT RepackSubRequest.id FROM RepackSubRequest WHERE RepackSubRequest.id<>srId AND RepackSubRequest.status NOT IN(4,5,8,9) 
			             	 )
			) 
		) RETURNING id INTO unused; -- FINISHED ARCHIVED FAILED ONHOLD
		ret:=1;
		COMMIT;
EXCEPTION  WHEN NO_DATA_FOUND THEN
  ret := 0;
END;

COMMIT;


