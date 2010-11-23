/******************************************************************************
 *              repack_2.1.9-10_to_2.1.10-0.sql
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
 * This script upgrades a CASTOR v2.1.9-10 REPACK database to v2.1.10-0
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

/* Stop on errors */
WHENEVER SQLERROR EXIT FAILURE
BEGIN
  UPDATE UpgradeLog
     SET failureCount = failureCount + 1
   WHERE schemaVersion = '2_1_8_0'
     AND release = '2_1_10_0'
     AND state != 'COMPLETE';
  COMMIT;
END;
/

/* Verify that the script is running against the correct schema and version */
DECLARE
  unused VARCHAR(100);
BEGIN
  SELECT release INTO unused FROM CastorVersion
   WHERE schemaName = 'REPACK'
     AND release LIKE '2_1_9_10%';
EXCEPTION WHEN NO_DATA_FOUND THEN
  -- Error, we can't apply this script
  raise_application_error(-20000, 'PL/SQL release mismatch. Please run previous upgrade scripts for the REPACK before this one.');
END;
/

INSERT INTO UpgradeLog (schemaVersion, release, type)
VALUES ('2_1_8_0', '2_1_10_0', 'TRANSPARENT');
COMMIT;

/* Update and revalidation of PL-SQL code */
/******************************************/

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

/* Recompile all invalid procedures, triggers and functions */
/************************************************************/
BEGIN
  FOR a IN (SELECT object_name, object_type
              FROM user_objects
             WHERE object_type IN ('PROCEDURE', 'TRIGGER', 'FUNCTION')
               AND status = 'INVALID')
  LOOP
    IF a.object_type = 'PROCEDURE' THEN
      EXECUTE IMMEDIATE 'ALTER PROCEDURE '||a.object_name||' COMPILE';
    ELSIF a.object_type = 'TRIGGER' THEN
      EXECUTE IMMEDIATE 'ALTER TRIGGER '||a.object_name||' COMPILE';
    ELSE
      EXECUTE IMMEDIATE 'ALTER FUNCTION '||a.object_name||' COMPILE';
    END IF;
  END LOOP;
END;
/

/* Flag the schema upgrade as COMPLETE */
/***************************************/
UPDATE UpgradeLog SET endDate = sysdate, state = 'COMPLETE'
 WHERE release = '2_1_10_0';
COMMIT;
