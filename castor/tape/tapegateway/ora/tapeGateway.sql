
/* trigger replacing streamsToDo */

create or replace
TRIGGER tr_Stream_Pending
AFTER UPDATE of status ON Stream
FOR EACH ROW
WHEN (new.status = 0 )
DECLARE reqId NUMBER;
BEGIN
  INSERT INTO TapeRequestState (accessMode, pid, startTime, lastVdqmPingTime, vdqmVolReqId, id, streamMigration, TapeRecall, Status) VALUES (1,null,null,null,null,ids_seq.nextval,:new.id,null,0) RETURNING id into reqId;
  INSERT INTO id2type (id,type) VALUES (reqId,172);
END;

/* trigger replacing tapesToDo */

create or replace
TRIGGER tr_Tape_Pending
AFTER UPDATE of status ON Tape
FOR EACH ROW
WHEN (new.status = 1 and new.tpmode=0)
DECLARE reqId NUMBER;
BEGIN
  INSERT INTO TapeRequestState (accessMode, pid, startTime, lastVdqmPingTime, vdqmVolReqId, id, streamMigration, TapeRecall, Status) VALUES (0,null,null,null,null,ids_seq.nextval,null,:new.id,1) RETURNING id into reqId;
  INSERT INTO id2type (id,type) VALUES (reqId,172);
END;
/

/* SQL Function getStreamsToResolve */
create or replace
PROCEDURE getStreamsToResolve
(strList OUT castor.Stream_Cur) AS
BEGIN
 OPEN strList FOR
    SELECT stream.id, stream.initialsizetotransfer, stream.status, stream.tapepool, tapepool.name
      FROM Stream,Tapepool
     WHERE stream.id in (select streamMigration from TapeRequestState WHERE status=0) and stream.tapepool=tapepool.id ORDER BY stream.id FOR UPDATE;   
END;  
/
/* SQL Function resolveStreams */

create or replace
PROCEDURE resolveStreams (strIds IN castor."cnumList", tapeVids IN castor."strList") AS
tapeId NUMBER;
strId NUMBER;
BEGIN
 FOR i IN strIds.FIRST .. strIds.LAST LOOP
     BEGIN
     	SELECT id INTO tapeId FROM Tape WHERE tpmode=1 AND vid=tapeVids(i) FOR UPDATE; 
     EXCEPTION WHEN NO_DATA_FOUND THEN
	       INSERT INTO Tape (vid,side,tpmode,errmsgtxt,errorcode,severity,vwaddress,id,stream,status) values (tapeVids(i),0,1,null,0,0,null,ids_seq.nextval,0,0) returning id into tapeId;
         INSERT INTO id2type (id,type) values (tapeId,29);
     END;
     UPDATE Stream SET tape=tapeId WHERE Stream.id=strIds(i) RETURNING id into strId;
     UPDATE Tape SET stream=strId , status=2 WHERE id=tapeId; 
     UPDATE taperequeststate SET status=1 WHERE streammigration= strids(i);
 END LOOP;
 COMMIT;
END;
/

/* SQL Function  getTapesToSubmit */

create or replace
PROCEDURE getTapesToSubmit
(tapesToSubmit OUT castor.TapeRequestStateCore_Cur) AS
ids "numList";
BEGIN
SELECT id BULK COLLECT INTO ids FROM taperequeststate WHERE status=1 ORDER BY id FOR UPDATE; 
 OPEN tapesToSubmit FOR  
   SELECT  tape.tpmode, tape.side, tape.vid, taperequeststate.id 
      FROM Stream,TapeRequestState,Tape 
     WHERE TapeRequestState.id MEMBER OF ids  AND 
	 Stream.id = TapeRequestState.streamMigration AND Stream.tape=Tape.id AND TapeRequestState.accessMode=1 
 UNION ALL  SELECT tape.tpmode, tape.side, tape.vid, taperequeststate.id 
      FROM  TapeRequestState,Tape WHERE TapeRequestState.id MEMBER OF ids AND
	Tape.id=TapeRequestState.tapeRecall AND TapeRequestState.accessMode=0;
END;
/

/* SQL Function getTapesToCheck */

create or replace PROCEDURE getTapesToCheck
(timeLimit IN NUMBER, taperequest OUT castor.TapeRequestState_Cur) AS
trIds "numList";
BEGIN 
UPDATE taperequeststate SET lastVdqmPingTime = gettime() WHERE status=2 AND  gettime() - TapeRequestState.lastVdqmPingTime > timeLimit RETURNING id BULK COLLECT into trIds;
 OPEN taperequest FOR
    SELECT TapeRequestState.accessMode, TapeRequestState.id,TapeRequestState.starttime, TapeRequestState.lastvdqmpingtime, TapeRequestState.vdqmvolreqid,TapeRequestState.status, Tape.vid  
      FROM Stream,TapeRequestState,Tape 
     WHERE taperequeststate.id MEMBER OF trids  AND  Stream.id = TapeRequestState.streamMigration AND Stream.tape=Tape.id AND TapeRequestState.accessMode=1
    UNION ALL
     SELECT TapeRequestState.accessMode, TapeRequestState.id,TapeRequestState.starttime, TapeRequestState.lastvdqmpingtime, TapeRequestState.vdqmvolreqid,TapeRequestState.status, Tape.vid  
      FROM TapeRequestState,Tape WHERE 
	  Tape.id=TapeRequestState.tapeRecall AND TapeRequestState.accessMode=0 AND  taperequeststate.id MEMBER OF trids;
END;
/

/* SQL Function updateSubmittedTapes */

create or replace
PROCEDURE updateSubmittedTapes(tapeRequestIds IN castor."cnumList", vdqmIds IN castor."cnumList" ) AS
BEGIN
-- update taperequeststate which have been submitted to vdqm
  FORALL i IN tapeRequestIds.FIRST .. tapeRequestIds.LAST
    UPDATE taperequeststate set status=2, lastvdqmpingtime=gettime(), starttime= gettime(), vdqmvolreqid=vdqmIds(i) WHERE id=tapeRequestIds(i); -- these are the ones waiting for vdqm to be sent again
-- update tape for recall  
  FORALL i IN tapeRequestIds.FIRST .. tapeRequestIds.LAST
    UPDATE tape SET status=2 WHERE id IN (SELECT taperecall FROM taperequeststate WHERE accessmode=0 AND  id=tapeRequestIds(i));
-- update tape for migration    
  FORALL i IN tapeRequestIds.FIRST .. tapeRequestIds.LAST
    UPDATE tape SET status=2 WHERE id IN (SELECT tape FROM stream WHERE id IN ( select streammigration FROM taperequeststate WHERE accessmode=1 AND  id=tapeRequestIds(i)));
-- update stream for migration    
  FORALL i IN tapeRequestIds.FIRST .. tapeRequestIds.LAST
    UPDATE stream SET status=1 WHERE id IN (select streammigration FROM taperequeststate WHERE accessmode=1 AND  id=tapeRequestIds(i));
  COMMIT;
END;
/


/* SQL Function updateCheckedTapes */

create or replace
PROCEDURE updateCheckedTapes(trIds IN castor."cnumList") AS
BEGIN
 FORALL i IN trIds.FIRST .. trIds.LAST
        UPDATE taperequeststate set lastvdqmpingtime = null, starttime = null, status=1  WHERE id=trIds(i); -- these are the lost ones 
 COMMIT;      
END;
/


/* SQL Fuction fileToMigrate */

create or replace
PROCEDURE fileToMigrate (tapeVid IN VARCHAR2, retFileId OUT NUMBER,  retNsHost OUT VARCHAR2, retFileSize OUT NUMBER, diskServerName OUT VARCHAR2, mountPoint OUT  VARCHAR2, path OUT  VARCHAR2, retTime OUT NUMBER) AS
strId NUMBER;
dci NUMBER;
tcId NUMBER;
castorFileId NUMBER;
BEGIN 
  SELECT id INTO strId FROM Stream WHERE tape in (select id from tape where vid=tapeVid and tpmode=1);
  UPDATE TapeCopy SET status=3 WHERE id=tcId;
  DELETE FROM Stream2TapeCopy WHERE child=tcId;
	COMMIT;
END;
/

/* SQL Function fileMigrationUpdate */

create or replace
PROCEDURE fileMigrationUpdate (inputFileId  IN NUMBER,inputNsHost IN VARCHAR2, inputCopyNb IN INTEGER, inputErrorCode IN INTEGER) AS
tcNumb INTEGER;
cfId NUMBER;
reqId NUMBER;
tcIds "numList";

BEGIN

  IF inputErrorcode = 0 THEN
    SELECT id INTO cfId FROM castorfile WHERE fileid=inputFileId AND nshost=inputNsHost FOR UPDATE;
    UPDATE tapecopy SET status=5 WHERE  castorfile=cfId AND copynb = inputCopyNb;
    SELECT count(*) INTO tcNumb FROM tapecopy WHERE castorfile = cfId  AND STATUS != 5;
    -- let's check if another copy should be done
    IF tcNumb = 0 THEN
     UPDATE diskcopy SET status=0 WHERE status=2 AND castorfile = cfId;
     DELETE FROM tapecopy WHERE castorfile=cfId returning id BULK COLLECT INTO tcIds;
     FORALL i IN tcIds.FIRST .. tcIds.LAST
       DELETE FROM id2type WHERE id=tcIds(i);
    END IF;
  -- repack case subrequest sucessfully put to 8
  
   UPDATE subrequest SET status=8 WHERE id IN ( SELECT SubRequest.id FROM SubRequest,CastorFile
    WHERE 
      subrequest.status = 12 -- SUBREQUEST_REPACK
      AND castorfile.fileid = inputFileId
      AND castorfile.nshost = inputNsHost
      AND subrequest.castorfile = castorfile.id )
      AND ROWNUM < 2  returning request INTO reqId;
   UPDATE subrequest SET status = 11 WHERE status=8 AND NOT EXISTS ( SELECT 'x' FROM subrequest WHERE request = reqId AND status NOT IN (8,9) ) AND request=reqId; -- archive if all are done   
 ELSE 
   --retry MIG_RETRY
   UPDATE tapecopy  SET status=9, errorcode=inputErrorCode, nbretry=0 WHERE castorfile IN (SELECT id  FROM castorfile WHERE fileid=inputFileId and nshost=inputNsHost) and copynb=inputCopyNb; 
 END IF;
 COMMIT;
END;
/

/* SQL Function fileToRecall */
-- TODO nocopy compiler hint

create or replace
PROCEDURE fileToRecall (tapeVid IN VARCHAR2, outputFileId OUT NUMBER, outputNsHost OUT VARCHAR2,  diskServerName OUT VARCHAR2, mountPoint OUT  VARCHAR2, path OUT  VARCHAR2) AS
segId NUMBER;
dcId NUMBER;
BEGIN 
  SELECT id INTO segId from segment where  tape IN (SELECT id FROM tape WHERE vid=tapeVid AND tpmode=0)  AND status=0 AND rownum<2 order by fseq FOR UPDATE; 
  UPDATE Segment set status=7 WHERE id=segId;
	SELECT fileid, nshost INTO  outputFileId, outputNsHost FROM castorfile  WHERE id IN (SELECT castorfile FROM tapecopy WHERE id IN (SELECT copy FROM segment WHERE id=segId));
  bestFileSystemForSegment(segId,diskServerName,mountPoint, path,dcid);
  COMMIT; 
END;
/

/* SQL Function fileRecallUpdate */

create or replace
PROCEDURE fileRecallUpdate (inputFileId  IN NUMBER,inputNsHost IN VARCHAR2, inputFileSize IN INTEGER,  inputCopyNb IN INTEGER, inputFseq IN NUMBER, inputVid IN VARCHAR2, inputErrorCode IN INTEGER ) AS
tcId NUMBER;
cfId NUMBER;
dcId NUMBER;
segIds "numList";
originalFileSize NUMBER;
segId NUMBER;
numSegs INTEGER; 
BEGIN
  IF inputErrorCode = 0 THEN 
    -- everything is fine
    SELECT id, filesize INTO cfId, originalFileSize FROM castorfile WHERE fileid=inputFileId AND nshost=inputNshost FOR UPDATE; 
    SELECT id BULK COLLECT INTO segIds  FROM Segment WHERE copy IN (SELECT id from TapeCopy where castorfile=cfId);  
    SELECT id INTO tcId FROM tapecopy WHERE castorfile = cfId;
    
    IF segIds.COUNT > 1 THEN
    -- multiple segmented file ... this is for sure an old format file so fseq is unique in the tape
      UPDATE Segment SET status=5 WHERE copy=tcId AND inputFseq=fseq AND tape IN (SELECT id FROM tape WHERE vid=inputVid AND tpMode=0)   RETURNING id INTO segId;
      SELECT count(id) INTO numSegs FROM segment WHERE status NOT IN (5,8) AND copy=tcId;
      IF numsegs=0 THEN
        -- finished delete them all
          fileRecalled(tcId);
          DELETE FROM id2type WHERE id IN (SELECT id FROM Segment WHERE copy=tcId);
          DELETE FROM segment   WHERE copy=tcId;
          DELETE FROM tapecopy WHERE  id=tcId;
          DELETE FROM id2type WHERE id=tcId;
      END IF;
      
    ELSE
    -- standard case   
      IF  originalFileSize != inputFileSize THEN
      -- retry wrong filesize
         UPDATE Segment SET status=6, errorCode=-1, errMsgTxt='wrong file size' WHERE copy = tcId;
         UPDATE Tapecopy SET status=8, errorCode=-1, nbretry=0 WHERE id=tcId;
      ELSE        
          fileRecalled(tcId);
          DELETE FROM id2type WHERE id IN (SELECT id FROM Segment WHERE copy=tcId);
          DELETE FROM segment   WHERE copy=tcId;
          DELETE FROM tapecopy WHERE  id=tcId;
         DELETE FROM id2type WHERE id=tcId;
      END IF;
    END IF;
    
  ELSE 
    -- retry REC_RETRY
     UPDATE tapecopy  SET status=8, errorcode=inputErrorCode WHERE castorfile IN (SELECT id  FROM castorfile WHERE fileid=inputFileId and nshost=inputNsHost) and copynb=inputCopyNb;  
     UPDATE segment SET status=6, severity=inputErrorCode, errorCode=-1 WHERE copy=tcId; -- failed the segment /segments
  END IF;
  COMMIT;
END;
/

/* SQL Function updateMigRetryPolicy */

create or replace
PROCEDURE  updateMigRetryPolicy
(tcToRetry IN castor."cnumList", tcToFail IN castor."cnumList"  ) AS
reqId NUMBER;
BEGIN
 -- restarted the one to be retried
  FORALL i IN tctoretry.FIRST .. tctoretry.LAST 
    UPDATE TapeCopy SET status=1,nbretry = nbretry+1  WHERE id = tcToRetry(i);
  -- fail the tapecopies   
  FORALL i IN tctofail.FIRST .. tctofail.LAST 
    UPDATE TapeCopy SET status=6 WHERE id = tcToFail(i);
 -- fail the diskcopy (FAIL TO MIGRATE STATE)
  FORALL i IN tctofail.FIRST .. tctofail.LAST 
    UPDATE DiskCopy SET status=12 WHERE castorfile in (SELECT castorfile FROM tapecopy WHERE  id = tcToFail(i)) AND status=10;
 -- fail repack subrequests
  FOR i IN tcToFail.FIRST .. tcToFail.LAST LOOP
      UPDATE subrequest SET status=9 WHERE castorfile in (SELECT castorfile FROM tapecopy WHERE id= tctofail(i)  ) AND status=12  RETURNING request INTO reqId;
     -- archive if necessary 
      UPDATE subrequest SET status=11 WHERE request = reqId AND NOT EXISTS ( select 'x' FROM subrequest WHERE  request=reqId AND status NOT IN (8, 9, 11) ); 
  END LOOP;
  COMMIT;
END;
/

/* SQL Function inputForRecallRetryPolicy */

create or replace
PROCEDURE  inputForRecallRetryPolicy
(tcs OUT castor.TapeCopy_Cur) AS
ids "numList";
BEGIN
  OPEN tcs FOR
    SELECT copynb, id, castorfile, status,errorCode, nbretry 
      FROM TapeCopy
     WHERE status=8 ORDER BY id FOR UPDATE; 
END;
/

/* SQL Function updateRecRetryPolicy */

create or replace
PROCEDURE  updateRecRetryPolicy 
(tcToRetry IN castor."cnumList", tcToFail IN castor."cnumList"  ) AS
segIds "numList";
tcId NUMBER;
BEGIN
  -- I restart the recall that I want to retry
  FOR i IN tcToRetry.FIRST .. tcToRetry.LAST  LOOP
    UPDATE TapeCopy SET status=4, errorcode=0, nbretry= nbretry+1 WHERE id=tcToRetry(i);
    UPDATE Segment SET status=0  WHERE copy=tcToRetry(i) returning id bulk collect into segIds;
    UPDATE tape SET status=8 WHERE status in (0,6) AND id in (SELECT tape from segment where id member of segIds);
-- if the tape status is 0 or 6 the taperequest state has been deleted already
  END LOOP;
-- I mark as failed the hopeless recall
  FOR i IN tcToFail.FIRST .. tcToFail.LAST  LOOP
    -- clean up diskcopy and subrequest
    fileRecallFailed(tcToFail(i)); 
    -- clean up tapecopy and segment
    DELETE FROM tapecopy where id= tcToFail(i);
    DELETE FROM id2type WHERE id =  tcToFail(i);
    DELETE FROM segment WHERE copy= tcToFail(i) RETURNING id BULK COLLECT INTO segIds;
    FORALL j IN segIds.FIRST .. segIds.LAST 
     DELETE FROM id2type WHERE id=segIds(j);    
  END LOOP;
  COMMIT;
END;
/

/* SQL Function getRepackVid */


create or replace
PROCEDURE
getRepackVid( inputFileId IN NUMBER, inputNsHost IN VARCHAR2, outRepackVid OUT VARCHAR2 ) AS 
BEGIN
  -- Get the repackvid field from the existing request (if none, then we are not in a repack process)
  SELECT StageRepackRequest.repackvid
    INTO outRepackVid
    FROM SubRequest, DiskCopy, CastorFile, StageRepackRequest
   WHERE stagerepackrequest.id = subrequest.request
     AND diskcopy.id = subrequest.diskcopy
     AND diskcopy.status = 10 -- CANBEMIGR
     AND subrequest.status = 12 -- SUBREQUEST_REPACK
     AND diskcopy.castorfile = castorfile.id
     AND castorfile.fileid = inputFileId AND castorfile.nshost = inputNsHost
     AND ROWNUM < 2;
EXCEPTION WHEN NO_DATA_FOUND THEN
  NULL;
END;
/

/* SQL Function updateDbStartTape */

create or replace
PROCEDURE  updateDbStartTape
( inputVdqmReqId IN INTEGER, inputMode  IN INTEGER, inputHost IN  VARCHAR2, outRequest OUT castor.TapeRequestState_Cur ) AS
reqId NUMBER;
tpId NUMBER;
BEGIN
  -- set the request to ONGOING
  UPDATE taperequeststate SET status=3 WHERE vdqmVolreqid = inputVdqmReqId AND accessmode= inputmode AND status =2 RETURNING id INTO reqId;
  IF inputmode = 0 THEN
    -- read tape mounted
    UPDATE tape set status=4, vwaddress= inputhost WHERE id IN ( SELECT taperecall FROM taperequeststate WHERE id=reqId ); 
  ELSE
    -- write
    UPDATE stream set status=3 WHERE id IN ( SELECT streammigration FROM taperequeststate WHERE id=reqId ) returning id INTO tpId;
    UPDATE tape set status=4, vwaddress= inputhost WHERE id = tpId and tpmode=1;
  END IF; 
   OPEN outRequest FOR 
    SELECT taperequeststate.accessMode, taperequeststate.id, taperequeststate.starttime,taperequeststate.lastvdqmpingtime, 
    taperequeststate.vdqmvolreqid,taperequeststate.status,tape.vid FROM taperequeststate,tape
    WHERE
    (taperequeststate.accessmode = 0 AND tape.id= taperequeststate.taperecall AND taperequeststate.id=reqId) OR  
    (taperequeststate.accessmode = 1 AND tape.id IN (SELECT stream.tape FROM stream, taperequeststate WHERE stream.id= taperequeststate.streammigration AND taperequeststate.id=reqId));
  COMMIT;
END;	
/

/* SQL Function updateDbEndTape */

create or replace PROCEDURE  updateDbEndTape  
( inputVdqmReqId IN INTEGER, inputMode  IN INTEGER ) AS
tpId NUMBER;
trId NUMBER;
strId NUMBER;
BEGIN
  IF inputmode = 0 THEN 
    --read
    -- delete taperequest
     DELETE FROM taperequeststate WHERE accessmode=0 AND vdqmvolreqid = inputvdqmreqid RETURNING id,taperecall INTO trId,tpId;
     DELETE FROM id2type WHERE id=trId;
     -- release tape
     UPDATE tape set status = 0 WHERE tpmode=0 AND status=4 AND NOT EXISTS(SELECT 'x' FROM Segment WHERE tape=tpId AND status=0)  AND id=tpId;
     -- still segment around ... last minute candidates
     UPDATE tape set status=8 WHERE status=4 AND tpmode=0 AND id=tpId; -- still segment in zero ... to be restarted   
  ELSE 
    -- write
    -- delete taperequest
    DELETE FROM taperequeststate WHERE accessmode=1 AND vdqmvolreqid=inputVdqmReqId RETURNING streammigration, id INTO strId, trId;
    DELETE FROM id2type WHERE id=trId;
    resetstream(strId);
  END IF;
  COMMIT;
END;
/

/* SQL Function invalidateSegment */

create or replace
PROCEDURE invalidateSegment(inputFileId IN NUMBER, inputNs IN VARCHAR2, inputCopyNb IN NUMBER) AS
 tcId NUMBER;
BEGIN
 -- failed the recall associated to the the segment which desn't exist anymore, the file can still exist
-- I mark as failed the hopeless recall
BEGIN
 SELECT id INTO tcId FROM tapecopy WHERE castorfile IN (SELECT id FROM castorfile where fileid=inputFileId and nshost = inputNs ) and copynb=inputCopynb FOR UPDATE; 
 fileRecallFailed(tcId); -- the user should try again
 COMMIT;

 EXCEPTION
  WHEN NO_DATA_FOUND THEN
  NULL;
 END;
END;
/

/* SQL Function tapeGatewayCleanUp */

create or replace
PROCEDURE tapeGatewayCleanUp (tapeToFree OUT castor.Tape_cur) AS
trIds "numList";
tIds "numList";
strIds "numList";
BEGIN
 -- delete ongoing requests
 DELETE FROM taperequeststate WHERE status=3 RETURNING id BULK COLLECT INTO trIds;
 FORALL i IN trIds.FIRST .. trids.LAST 
  DELETE FROM id2type WHERE id= trIds(i);
 -- clean up ongoing recalls
 UPDATE tape SET status=0 WHERE tpmode=0 AND status=4 RETURNING id BULK COLLECT INTO tIds;
 FORALL i IN tIds.FIRST .. tIds.LAST
   UPDATE Segment set status=0 WHERE status = 7 AND tape=tIds(i); -- resurrect SELECTED segment 
 FORALL i IN tIds.FIRST .. tIds.LAST 
   UPDATE tape SET status=8 WHERE id=tIds(i) AND EXISTS (SELECT 'x' FROM Segment WHERE tape =tIds(i) AND status=0);
 -- clean up ongoing migrations
 UPDATE stream  A set status=6, tape=null WHERE status=3 AND EXISTS (SELECT 'x' FROM stream2tapecopy WHERE A.id= stream2tapecopy.parent ) returning tape BULK COLLECT INTO tIds;
 FORALL i IN tIds.FIRST .. tIds.LAST
  UPDATE tape SET status=0, stream=null WHERE id=tIds(i); -- detach the tape of resurrected streams
 DELETE FROM stream A WHERE status=3 AND NOT EXISTS (SELECT 'x' FROM stream2tapecopy WHERE A.id= stream2tapecopy.parent) RETURNING id, tape BULK COLLECT INTO strIds, tIds;
 FORALL i IN strIds.FIRST .. strIds.LAST 
  DELETE FROM id2type WHERE id=strIds(i);
 FORALL i IN tIds.FIRST .. tIds.LAST
  UPDATE tape set status=0, stream=null WHERE id=tIds(i);
 UPDATE TapeCopy SET status=1 WHERE status=3; -- resurrect detached tapecopies;
OPEN tapeToFree FOR
 SELECT vid, side, tpmode, errmsgtxt, errorCode, severity, vwaddress, id,  stream,status FROM tape WHERE id MEMBER OF tIds;
END;
/

/***********************************************************************************/

/***********************************************************************************/


/* Function Recycled from rtcpcld */

/* Used to create a row INTO NbTapeCopiesInFS whenever a new
   Stream is created */
CREATE OR REPLACE TRIGGER tr_Stream_Insert
BEFORE INSERT ON Stream
FOR EACH ROW
BEGIN
  FOR item in (SELECT id FROM FileSystem) LOOP
    INSERT INTO NbTapeCopiesInFS (FS, Stream, NbTapeCopies) VALUES (item.id, :new.id, 0);
  END LOOP;
END;

/* Used to delete rows IN NbTapeCopiesInFS whenever a
   Stream is deleted */
CREATE OR REPLACE TRIGGER tr_Stream_Delete
BEFORE DELETE ON Stream
FOR EACH ROW
BEGIN
  DELETE FROM NbTapeCopiesInFS WHERE Stream = :old.id;
END;

/* Updates the count of tapecopies in NbTapeCopiesInFS
   whenever a TapeCopy is linked to a Stream */
CREATE OR REPLACE TRIGGER tr_Stream2TapeCopy_Insert
AFTER INSERT ON STREAM2TAPECOPY
REFERENCING NEW AS NEW OLD AS OLD
FOR EACH ROW
DECLARE
  cfSize NUMBER;
BEGIN
  -- added this lock because of several copies of different file systems
  -- from different streams which can cause deadlock
  LOCK TABLE NbTapeCopiesInFS IN ROW SHARE MODE;
  UPDATE NbTapeCopiesInFS SET NbTapeCopies = NbTapeCopies + 1
   WHERE FS IN (SELECT DiskCopy.FileSystem
                  FROM DiskCopy, TapeCopy
                 WHERE DiskCopy.CastorFile = TapeCopy.castorFile
                   AND TapeCopy.id = :new.child
                   AND DiskCopy.status = 10) -- CANBEMIGR
     AND Stream = :new.parent;
END;

/* Updates the count of tapecopies in NbTapeCopiesInFS
   whenever a TapeCopy is unlinked from a Stream */
CREATE OR REPLACE TRIGGER tr_Stream2TapeCopy_Delete
BEFORE DELETE ON STREAM2TAPECOPY
REFERENCING NEW AS NEW OLD AS OLD
FOR EACH ROW
DECLARE cfSize NUMBER;
BEGIN
  -- added this lock because of several copies of different file systems
  -- from different streams which can cause deadlock
  LOCK TABLE NbTapeCopiesInFS IN ROW SHARE MODE;
  UPDATE NbTapeCopiesInFS SET NbTapeCopies = NbTapeCopies - 1
   WHERE FS IN (SELECT DiskCopy.FileSystem
                  FROM DiskCopy, TapeCopy
                 WHERE DiskCopy.CastorFile = TapeCopy.castorFile
                   AND TapeCopy.id = :old.child
                   AND DiskCopy.status = 10) -- CANBEMIGR
     AND Stream = :old.parent;
END;


/************************************************/
/* Triggers to keep NbTapeCopiesInFS consistent */
/************************************************/

/* Used to create a row in NbTapeCopiesInFS and FileSystemsToCheck
   whenever a new FileSystem is created */
CREATE OR REPLACE TRIGGER tr_FileSystem_Insert
BEFORE INSERT ON FileSystem
FOR EACH ROW
BEGIN
  FOR item in (SELECT id FROM Stream) LOOP
    INSERT INTO NbTapeCopiesInFS (FS, Stream, NbTapeCopies) VALUES (:new.id, item.id, 0);
  END LOOP;
  INSERT INTO FileSystemsToCheck (FileSystem, ToBeChecked) VALUES (:new.id, 0);
END;

/* Used to delete rows in NbTapeCopiesInFS and FileSystemsToCheck
   whenever a FileSystem is deleted */
CREATE OR REPLACE TRIGGER tr_FileSystem_Delete
BEFORE DELETE ON FileSystem
FOR EACH ROW
BEGIN
  DELETE FROM NbTapeCopiesInFS WHERE FS = :old.id;
  DELETE FROM FileSystemsToCheck WHERE FileSystem = :old.id;
END;

/* Updates the count of tapecopies in NbTapeCopiesInFS
   whenever a DiskCopy has been replicated and the new one
   is put into CANBEMIGR status from the
   WAITDISK2DISKCOPY status */
CREATE OR REPLACE TRIGGER tr_DiskCopy_Update
AFTER UPDATE of status ON DiskCopy
FOR EACH ROW
WHEN (old.status = 1 AND -- WAITDISK2DISKCOPY
      new.status = 10) -- CANBEMIGR
BEGIN
  -- added this lock because of severval copies on different file systems
  -- from different streams which can cause deadlock
  LOCK TABLE  NbTapeCopiesInFS IN ROW SHARE MODE;
  UPDATE NbTapeCopiesInFS SET NbTapeCopies = NbTapeCopies + 1
   WHERE FS = :new.fileSystem
     AND Stream IN (SELECT Stream2TapeCopy.parent
                      FROM Stream2TapeCopy, TapeCopy
                     WHERE TapeCopy.castorFile = :new.castorFile
                       AND Stream2TapeCopy.child = TapeCopy.id
                       AND TapeCopy.status = 2); -- WAITINSTREAMS
END;


/* Used to create a row INTO LockTable whenever a new
   DiskServer is created */
CREATE OR REPLACE TRIGGER tr_DiskServer_Insert
BEFORE INSERT ON DiskServer
FOR EACH ROW
BEGIN
  INSERT INTO LockTable (DiskServerId, TheLock) VALUES (:new.id, 0);
END;


/* Used to delete rows IN LockTable whenever a
   DiskServer is deleted */
CREATE OR REPLACE TRIGGER tr_DiskServer_Delete
BEFORE DELETE ON DiskServer
FOR EACH ROW
BEGIN
  DELETE FROM LockTable WHERE DiskServerId = :old.id;
END;



/* PL/SQL methods to update FileSystem weight for new migrator streams */
CREATE OR REPLACE PROCEDURE updateFsMigratorOpened
(ds IN INTEGER, fs IN INTEGER, fileSize IN INTEGER) AS
BEGIN
  /* We lock first the diskserver in order to lock all the
     filesystems of this DiskServer in an atomical way */
  UPDATE DiskServer SET nbMigratorStreams = nbMigratorStreams + 1 WHERE id = ds;
  UPDATE FileSystem SET nbMigratorStreams = nbMigratorStreams + 1 WHERE id = fs;
END;

/* PL/SQL methods to update FileSystem weight for new recaller streams */
CREATE OR REPLACE PROCEDURE updateFsRecallerOpened
(ds IN INTEGER, fs IN INTEGER, fileSize IN INTEGER) AS
BEGIN
  /* We lock first the diskserver in order to lock all the
     filesystems of this DiskServer in an atomical way */
  UPDATE DiskServer SET nbRecallerStreams = nbRecallerStreams + 1 WHERE id = ds;
  UPDATE FileSystem SET nbRecallerStreams = nbRecallerStreams + 1,
                        free = free - fileSize   -- just an evaluation, monitoring will update it
   WHERE id = fs;
END;

/* PL/SQL method implementing bestTapeCopyForStream */
CREATE OR REPLACE PROCEDURE bestTapeCopyForStream(streamId IN INTEGER,
                                                  diskServerName OUT VARCHAR2, mountPoint OUT VARCHAR2,
                                                  path OUT VARCHAR2, dci OUT INTEGER,
                                                  castorFileId OUT INTEGER, fileId OUT INTEGER,
                                                  nsHost OUT VARCHAR2, fileSize OUT INTEGER,
                                                  tapeCopyId OUT INTEGER, optimized IN INTEGER) AS
  policy VARCHAR(2048);
BEGIN
  -- get the policy name
  BEGIN
    SELECT migrSelectPolicy INTO policy
      FROM Stream, TapePool
     WHERE Stream.id = streamId
       AND Stream.tapePool = TapePool.id;
    -- check for NULL value
    IF policy IS NULL THEN
      policy := 'defaultMigrSelPolicy';
    END IF;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    policy := 'defaultMigrSelPolicy';
  END;
  EXECUTE IMMEDIATE 'BEGIN ' || policy || '(:streamId, :diskServerName, :mountPoint, :path, :dci, :castorFileId, :fileId, :nsHost, :fileSize, :tapeCopyId, :optimized); END;'
    USING IN streamId, OUT diskServerName, OUT mountPoint, OUT path, OUT dci, OUT castorFileId, OUT fileId, OUT nsHost, OUT fileSize, OUT tapeCopyId, IN optimized;
END;

/* default migration candidate selection policy */
CREATE OR REPLACE PROCEDURE defaultMigrSelPolicy(streamId IN INTEGER,
                                                 diskServerName OUT VARCHAR2, mountPoint OUT VARCHAR2,
                                                 path OUT VARCHAR2, dci OUT INTEGER,
                                                 castorFileId OUT INTEGER, fileId OUT INTEGER,
                                                 nsHost OUT VARCHAR2, fileSize OUT INTEGER,
                                                 tapeCopyId OUT INTEGER, optimized IN INTEGER) AS
  fileSystemId INTEGER := 0;
  dsid NUMBER;
  fsDiskServer NUMBER;
  lastFSChange NUMBER;
  lastFSUsed NUMBER;
  lastButOneFSUsed NUMBER;
  findNewFS NUMBER := 1;
  nbMigrators NUMBER := 0;
  unused "numList";
BEGIN
  -- First try to see whether we should resuse the same filesystem as last time
  SELECT lastFileSystemChange, lastFileSystemUsed, lastButOneFileSystemUsed
    INTO lastFSChange, lastFSUsed, lastButOneFSUsed
    FROM Stream WHERE id = streamId;
  IF getTime() < lastFSChange + 900 THEN
    SELECT (SELECT count(*) FROM stream WHERE lastFileSystemUsed = lastButOneFSUsed) +
           (SELECT count(*) FROM stream WHERE lastButOneFileSystemUsed = lastButOneFSUsed)
      INTO nbMigrators FROM DUAL;
    -- only go if we are the only migrator on the box
    IF nbMigrators = 1 THEN
      BEGIN
        -- check states of the diskserver and filesystem and get mountpoint and diskserver name
        SELECT name, mountPoint, FileSystem.id INTO diskServerName, mountPoint, fileSystemId
          FROM FileSystem, DiskServer
         WHERE FileSystem.diskServer = DiskServer.id
           AND FileSystem.id = lastButOneFSUsed
           AND FileSystem.status IN (0, 1)  -- PRODUCTION, DRAINING
           AND DiskServer.status IN (0, 1); -- PRODUCTION, DRAINING
        -- we are within the time range, so we try to reuse the filesystem
        SELECT /*+ FIRST_ROWS(1)  LEADING(D T ST) */
               DiskCopy.path, DiskCopy.id, DiskCopy.castorfile, TapeCopy.id
          INTO path, dci, castorFileId, tapeCopyId
          FROM DiskCopy, TapeCopy, Stream2TapeCopy
         WHERE DiskCopy.status = 10 -- CANBEMIGR
           AND DiskCopy.filesystem = lastButOneFSUsed
           AND Stream2TapeCopy.parent = streamId
           AND TapeCopy.status = 2 -- WAITINSTREAMS
           AND Stream2TapeCopy.child = TapeCopy.id
           AND TapeCopy.castorfile = DiskCopy.castorfile
           AND ROWNUM < 2;
        SELECT CastorFile.FileId, CastorFile.NsHost, CastorFile.FileSize
          INTO fileId, nsHost, fileSize
          FROM CastorFile
         WHERE Id = castorFileId;
        -- we found one, no need to go for new filesystem
        findNewFS := 0;
      EXCEPTION WHEN NO_DATA_FOUND THEN
        -- found no tapecopy or diskserver, filesystem are down. We'll go through the normal selection
        NULL;
      END;
    END IF;
  END IF;
  IF findNewFS = 1 THEN
    -- We lock here a given DiskServer. See the comment for the creation of the LockTable
    -- table for a full explanation of why we need such a stupid UPDATE statement
    UPDATE LockTable SET theLock = 1
     WHERE diskServerId = (
       SELECT DS.diskserver_id
         FROM (
           -- The double level of selects is due to the fact that ORACLE is unable
           -- to use ROWNUM and ORDER BY at the same time. Thus, we have to first compute
           -- the maxRate and then select on it.
           SELECT diskserver.id diskserver_id
             FROM FileSystem FS, NbTapeCopiesInFS, DiskServer
            WHERE FS.id = NbTapeCopiesInFS.FS
              AND NbTapeCopiesInFS.NbTapeCopies > 0
              AND NbTapeCopiesInFS.Stream = StreamId
              AND FS.status IN (0, 1)         -- PRODUCTION, DRAINING
              AND DiskServer.id = FS.diskserver
              AND DiskServer.status IN (0, 1) -- PRODUCTION, DRAINING
              -- Ignore diskservers where a migrator already exists
              AND DiskServer.id NOT IN (
                SELECT DISTINCT(FileSystem.diskServer)
                  FROM FileSystem, Stream
                 WHERE FileSystem.id = Stream.lastfilesystemused
                   AND Stream.status IN (3)   -- SELECTED
                   AND optimized = 1
              )
            ORDER BY DiskServer.nbMigratorStreams ASC,
                     FileSystemRate(FS.readRate, FS.writeRate, FS.nbReadStreams, FS.nbWriteStreams,
                     FS.nbReadWriteStreams, FS.nbMigratorStreams, FS.nbRecallerStreams) DESC, dbms_random.value
            ) DS
        WHERE ROWNUM < 2)
    RETURNING diskServerId INTO dsid;
    -- Now we got our Diskserver but we lost all other data (due to the fact we had
    -- to do an update for the lock and we could not do a join in the update).
    -- So here we select all we need
    SELECT FN.name, FN.mountPoint, FN.diskserver, FN.id
      INTO diskServerName, mountPoint, fsDiskServer, fileSystemId
      FROM (
        SELECT DiskServer.name, FS.mountPoint, FS.diskserver, FS.id
          FROM FileSystem FS, NbTapeCopiesInFS, Diskserver
         WHERE FS.id = NbTapeCopiesInFS.FS
           AND DiskServer.id = FS.diskserver
           AND NbTapeCopiesInFS.NbTapeCopies > 0
           AND NbTapeCopiesInFS.Stream = StreamId
           AND FS.status IN (0, 1)    -- PRODUCTION, DRAINING
           AND FS.diskserver = dsId
         ORDER BY DiskServer.nbMigratorStreams ASC,
                  FileSystemRate(FS.readRate, FS.writeRate, FS.nbReadStreams, FS.nbWriteStreams,
                  FS.nbReadWriteStreams, FS.nbMigratorStreams, FS.nbRecallerStreams) DESC, dbms_random.value
         ) FN
     WHERE ROWNUM < 2;
    SELECT /*+ FIRST_ROWS(1)  LEADING(D T ST) */
           DiskCopy.path, DiskCopy.id, DiskCopy.castorfile, TapeCopy.id
      INTO path, dci, castorFileId, tapeCopyId
      FROM DiskCopy, TapeCopy, Stream2TapeCopy
     WHERE DiskCopy.status = 10 -- CANBEMIGR
       AND DiskCopy.filesystem = fileSystemId
       AND Stream2TapeCopy.parent = streamId
       AND TapeCopy.status = 2 -- WAITINSTREAMS
       AND Stream2TapeCopy.child = TapeCopy.id
       AND TapeCopy.castorfile = DiskCopy.castorfile
       AND ROWNUM < 2;
    SELECT CastorFile.FileId, CastorFile.NsHost, CastorFile.FileSize
      INTO fileId, nsHost, fileSize
      FROM CastorFile
     WHERE id = castorFileId;
  END IF;
  -- update status of selected tapecopy and stream
  UPDATE TapeCopy SET status = 3 -- SELECTED
   WHERE id = tapeCopyId;
  -- get locks on all the stream we will handle in order to avoid a
  -- deadlock with the attachTapeCopiesToStreams procedure
  --
  -- the deadlock would occur with updates to the NbTapeCopiesInFS
  -- table performed by the tr_stream2tapecopy_delete trigger triggered
  -- by the "DELETE FROM Stream2TapeCopy" statement below
  SELECT 1 BULK COLLECT
    INTO unused
    FROM Stream
    WHERE id IN (SELECT parent FROM Stream2TapeCopy WHERE child = tapeCopyId)
    ORDER BY id
    FOR UPDATE;
  UPDATE Stream
     SET status = 3, -- RUNNING
         lastFileSystemUsed = fileSystemId, lastButOneFileSystemUsed = lastFileSystemUsed
   WHERE id = streamId;
  IF findNewFS = 1 THEN
    -- time only if we changed FS
    UPDATE Stream SET lastFileSystemChange = getTime() WHERE id = streamId;
  END IF;
  -- detach the tapecopy from the stream now that it is SELECTED
  -- the triggers will update NbTapeCopiesInFS accordingly
  DELETE FROM Stream2TapeCopy
   WHERE child = tapeCopyId;

  -- Update Filesystem state
  updateFSMigratorOpened(fsDiskServer, fileSystemId, 0);

  EXCEPTION WHEN NO_DATA_FOUND THEN
    -- If the procedure was called with optimization enabled,
    -- rerun it again with optimization disabled to make sure
    -- there is really nothing to migrate!! Why? optimization
    -- excludes filesystems as being migration candidates if
    -- a migration stream is already running there. This allows
    -- us to maximise bandwidth to tape and not to saturate a
    -- diskserver with too many streams. However, in small disk
    -- pools this behaviour results in mounting, writing one
    -- file and dismounting of tapes as the tpdaemon reads ahead
    -- many files at a time! (#28097)
    IF optimized = 1 THEN
      bestTapeCopyForStream(streamId,
                            diskServerName,
                            mountPoint,
                            path,
                            dci,
                            castorFileId,
                            fileId,
                            nsHost,
                            fileSize,
                            tapeCopyId,
			    0);
      -- Since we recursively called ourselves, we should not do
      -- any update in the outer call
      RETURN;
    END IF;
    -- Reset last filesystems used
    UPDATE Stream
       SET lastFileSystemUsed = 0, lastButOneFileSystemUsed = 0
     WHERE id = streamId;
    -- No data found means the selected filesystem has no
    -- tapecopies to be migrated. Thus we go to next one
    -- However, we reset the NbTapeCopiesInFS row that failed
    -- This is not 100% safe but is far better than retrying
    -- in the same conditions
    IF 0 != fileSystemId THEN
      UPDATE NbTapeCopiesInFS
         SET NbTapeCopies = 0
       WHERE Stream = StreamId
         AND NbTapeCopiesInFS.FS = fileSystemId;
      bestTapeCopyForStream(streamId,
                            diskServerName,
                            mountPoint,
                            path,
                            dci,
                            castorFileId,
                            fileId,
                            nsHost,
                            fileSize,
                            tapeCopyId,
			    optimized);
    END IF;
END;

/* drain disk migration candidate selection policy */
CREATE OR REPLACE PROCEDURE drainDiskMigrSelPolicy(streamId IN INTEGER,
                                                   diskServerName OUT VARCHAR2, mountPoint OUT VARCHAR2,
                                                   path OUT VARCHAR2, dci OUT INTEGER,
                                                   castorFileId OUT INTEGER, fileId OUT INTEGER,
                                                   nsHost OUT VARCHAR2, fileSize OUT INTEGER,
                                                   tapeCopyId OUT INTEGER, optimized IN INTEGER) AS
  fileSystemId INTEGER := 0;
  dsid NUMBER;
  fsDiskServer NUMBER;
  lastFSChange NUMBER;
  lastFSUsed NUMBER;
  lastButOneFSUsed NUMBER;
  findNewFS NUMBER := 1;
  nbMigrators NUMBER := 0;
  unused "numList";
BEGIN
  -- First try to see whether we should resuse the same filesystem as last time
  SELECT lastFileSystemChange, lastFileSystemUsed, lastButOneFileSystemUsed
    INTO lastFSChange, lastFSUsed, lastButOneFSUsed
    FROM Stream WHERE id = streamId;
  IF getTime() < lastFSChange + 1800 THEN
    SELECT (SELECT count(*) FROM stream WHERE lastFileSystemUsed = lastFSUsed)
      INTO nbMigrators FROM DUAL;
    -- only go if we are the only migrator on the box
    IF nbMigrators = 1 THEN
      BEGIN
        -- check states of the diskserver and filesystem and get mountpoint and diskserver name
	SELECT diskserver.id, name, mountPoint, FileSystem.id INTO dsid, diskServerName, mountPoint, fileSystemId
          FROM FileSystem, DiskServer
         WHERE FileSystem.diskServer = DiskServer.id
           AND FileSystem.id = lastFSUsed
           AND FileSystem.status IN (0, 1)  -- PRODUCTION, DRAINING
           AND DiskServer.status IN (0, 1); -- PRODUCTION, DRAINING
        -- we are within the time range, so we try to reuse the filesystem
        SELECT P.path, P.diskcopy_id, P.castorfile,
             C.fileId, C.nsHost, C.fileSize, P.id
        INTO path, dci, castorFileId, fileId, nsHost, fileSize, tapeCopyId
        FROM (SELECT /*+ ORDERED USE_NL(D T) INDEX(T I_TAPECOPY_CF_STATUS_2) INDEX(ST I_PK_STREAM2TAPECOPY) */
              D.path, D.diskcopy_id, D.castorfile, T.id
                FROM (SELECT /*+ INDEX(DK I_DISKCOPY_FS_STATUS_10) */
                             DiskCopy.path path, DiskCopy.id diskcopy_id, DiskCopy.castorfile
                        FROM DiskCopy
                       WHERE decode(DiskCopy.status,10,DiskCopy.status,null) = 10 -- CANBEMIGR
                         AND DiskCopy.filesystem = lastFSUsed) D,
                      TapeCopy T, Stream2TapeCopy ST
               WHERE T.castorfile = D.castorfile
                 AND ST.child = T.id
                 AND ST.parent = streamId
                 AND decode(T.status,2,T.status,null) = 2 -- WAITINSTREAMS
                 AND ROWNUM < 2) P, castorfile C
         WHERE P.castorfile = C.id;
        -- we found one, no need to go for new filesystem
        findNewFS := 0;
      EXCEPTION WHEN NO_DATA_FOUND THEN
        -- found no tapecopy or diskserver, filesystem are down. We'll go through the normal selection
        NULL;
      END;
    END IF;
  END IF;
  IF findNewFS = 1 THEN
    -- We lock here a given DiskServer. See the comment for the creation of the LockTable
    -- table for a full explanation of why we need such a stupid UPDATE statement

    -- The first idea is to reuse the diskserver of the lastFSUsed, even if we change filesystem
    dsId := -1;
    UPDATE LockTable SET theLock = 1
     WHERE diskServerId = (
             SELECT lastButOneFSUsed
               FROM FileSystem FS, NbTapeCopiesInFS, DiskServer
              WHERE FS.id = NbTapeCopiesInFS.FS
                AND NbTapeCopiesInFS.NbTapeCopies > 0
                AND NbTapeCopiesInFS.Stream = StreamId
                AND FS.status IN (0, 1)         -- PRODUCTION, DRAINING
                AND DiskServer.id = FS.diskserver
                AND DiskServer.status IN (0, 1) -- PRODUCTION, DRAINING
                AND DiskServer.id = lastButOneFSUsed
                AND ROWNUM < 2)
    RETURNING diskServerId INTO dsid;
    IF dsId = -1 THEN
      -- We could not reuse the same diskserver, so we jump to another one,
      -- We try to find one with no stream on it
      UPDATE LockTable SET theLock = 1
       WHERE diskServerId = (
         SELECT DS.diskserver_id
           FROM (
             -- The double level of selects is due to the fact that ORACLE is unable
             -- to use ROWNUM and ORDER BY at the same time. Thus, we have to first compute
             -- the maxRate and then select on it.
             SELECT diskserver.id diskserver_id
               FROM FileSystem FS, NbTapeCopiesInFS, DiskServer
              WHERE FS.id = NbTapeCopiesInFS.FS
                AND NbTapeCopiesInFS.NbTapeCopies > 0
                AND NbTapeCopiesInFS.Stream = StreamId
                AND FS.status IN (0, 1)         -- PRODUCTION, DRAINING
                AND DiskServer.id = FS.diskserver
                AND DiskServer.status IN (0, 1) -- PRODUCTION, DRAINING
                -- Ignore diskservers where a migrator already exists
                AND DiskServer.id NOT IN (
                  SELECT DISTINCT(FileSystem.diskServer)
                    FROM FileSystem, Stream
                   WHERE FileSystem.id = Stream.lastfilesystemused
                     AND Stream.status IN (3)   -- SELECTED
                     AND optimized = 1
                )
              ORDER BY FileSystemRate(FS.readRate, FS.writeRate, FS.nbReadStreams, FS.nbWriteStreams, FS.nbReadWriteStreams, FS.nbMigratorStreams, FS.nbRecallerStreams) DESC, dbms_random.value
              ) DS
          WHERE ROWNUM < 2)
      RETURNING diskServerId INTO dsid;
    END IF;
    -- Now we got our Diskserver but we lost all other data (due to the fact we had
    -- to do an update for the lock and we could not do a join in the update).
    -- So here we select all we need
    SELECT FN.name, FN.mountPoint, FN.diskserver, FN.id
      INTO diskServerName, mountPoint, fsDiskServer, fileSystemId
      FROM (
        SELECT DiskServer.name, FS.mountPoint, FS.diskserver, FS.id
          FROM FileSystem FS, NbTapeCopiesInFS, Diskserver
         WHERE FS.id = NbTapeCopiesInFS.FS
           AND DiskServer.id = FS.diskserver
           AND NbTapeCopiesInFS.NbTapeCopies > 0
           AND NbTapeCopiesInFS.Stream = StreamId
           AND FS.status IN (0, 1)    -- PRODUCTION, DRAINING
           AND FS.diskserver = dsId
         ORDER BY FileSystemRate(FS.readRate, FS.writeRate, FS.nbReadStreams, FS.nbWriteStreams, FS.nbReadWriteStreams, FS.nbMigratorStreams, FS.nbRecallerStreams) DESC, dbms_random.value
         ) FN
     WHERE ROWNUM < 2;
    SELECT P.path, P.diskcopy_id, P.castorfile,
           C.fileId, C.nsHost, C.fileSize, P.id
      INTO path, dci, castorFileId, fileId, nsHost, fileSize, tapeCopyId
      FROM (SELECT /*+ ORDERED USE_NL(D T) INDEX(T I_TAPECOPY_CF_STATUS_2) INDEX(ST I_PK_STREAM2TAPECOPY) */
            D.path, D.diskcopy_id, D.castorfile, T.id
              FROM (SELECT /*+ INDEX(DK I_DISKCOPY_FS_STATUS_10) */
                           DiskCopy.path path, DiskCopy.id diskcopy_id, DiskCopy.castorfile
                      FROM DiskCopy
                     WHERE decode(DiskCopy.status,10,DiskCopy.status,null) = 10 -- CANBEMIGR
                       AND DiskCopy.filesystem = fileSystemId) D,
                    TapeCopy T, Stream2TapeCopy ST
             WHERE T.castorfile = D.castorfile
               AND ST.child = T.id
               AND ST.parent = streamId
               AND decode(T.status,2,T.status,null) = 2 -- WAITINSTREAMS
               AND ROWNUM < 2) P, castorfile C
     WHERE P.castorfile = C.id;

  END IF;
  -- update status of selected tapecopy and stream
  UPDATE TapeCopy SET status = 3 -- SELECTED
   WHERE id = tapeCopyId;
  -- get locks on all the stream we will handle in order to avoid a
  -- deadlock with the attachTapeCopiesToStreams procedure
  --
  -- the deadlock would occur with updates to the NbTapeCopiesInFS
  -- table performed by the tr_stream2tapecopy_delete trigger triggered
  -- by the "DELETE FROM Stream2TapeCopy" statement below
  SELECT 1
    BULK COLLECT INTO unused
    FROM Stream
    WHERE id IN (SELECT parent FROM Stream2TapeCopy WHERE child = tapeCopyId)
    ORDER BY id
    FOR UPDATE;
  UPDATE Stream
     SET status = 3, -- RUNNING
         lastFileSystemUsed = fileSystemId, lastButOneFileSystemUsed = dsid
   WHERE id = streamId;
  IF findNewFS = 1 THEN
    -- time only if we changed FS
    UPDATE Stream SET lastFileSystemChange = getTime() WHERE id = streamId;
  END IF;
  -- detach the tapecopy from the stream now that it is SELECTED
  -- the triggers will update NbTapeCopiesInFS accordingly
  DELETE FROM Stream2TapeCopy
   WHERE child = tapeCopyId;

  -- Update Filesystem state
  updateFSMigratorOpened(fsDiskServer, fileSystemId, 0);

  EXCEPTION WHEN NO_DATA_FOUND THEN
    -- If the procedure was called with optimization enabled,
    -- rerun it again with optimization disabled to make sure
    -- there is really nothing to migrate!! Why? optimization
    -- excludes filesystems as being migration candidates if
    -- a migration stream is already running there. This allows
    -- us to maximise bandwidth to tape and not to saturate a
    -- diskserver with too many streams. However, in small disk
    -- pools this behaviour results in mounting, writing one
    -- file and dismounting of tapes as the tpdaemon reads ahead
    -- many files at a time! (#28097)
    IF optimized = 1 THEN
      bestTapeCopyForStream(streamId,
                            diskServerName,
                            mountPoint,
                            path,
                            dci,
                            castorFileId,
                            fileId,
                            nsHost,
                            fileSize,
                            tapeCopyId,
			    0);
      -- Since we recursively called ourselves, we should not do
      -- any update in the outer call
      RETURN;
    END IF;
    -- Reset last filesystems used
    UPDATE Stream
       SET lastFileSystemUsed = 0, lastButOneFileSystemUsed = 0
     WHERE id = streamId;
    -- No data found means the selected filesystem has no
    -- tapecopies to be migrated. Thus we go to next one
    -- However, we reset the NbTapeCopiesInFS row that failed
    -- This is not 100% safe but is far better than retrying
    -- in the same conditions
    IF 0 != fileSystemId THEN
      UPDATE NbTapeCopiesInFS
         SET NbTapeCopies = 0
       WHERE Stream = StreamId
         AND NbTapeCopiesInFS.FS = fileSystemId;
      bestTapeCopyForStream(streamId,
                            diskServerName,
                            mountPoint,
                            path,
                            dci,
                            castorFileId,
                            fileId,
                            nsHost,
                            fileSize,
                            tapeCopyId,
			    optimized);
    END IF;
END;

/* repack migration candidate selection policy */
CREATE OR REPLACE PROCEDURE repackMigrSelPolicy(streamId IN INTEGER,
                                                diskServerName OUT VARCHAR2, mountPoint OUT VARCHAR2,
                                                path OUT VARCHAR2, dci OUT INTEGER,
                                                castorFileId OUT INTEGER, fileId OUT INTEGER,
                                                nsHost OUT VARCHAR2, fileSize OUT INTEGER,
                                                tapeCopyId OUT INTEGER, optimized IN INTEGER) AS
  fileSystemId INTEGER := 0;
  dsid NUMBER;
  fsDiskServer NUMBER;
  lastFSChange NUMBER;
  lastFSUsed NUMBER;
  lastButOneFSUsed NUMBER;
  nbMigrators NUMBER := 0;
  unused "numList";
BEGIN
  -- We lock here a given DiskServer. See the comment for the creation of the LockTable
  -- table for a full explanation of why we need such a stupid UPDATE statement
  UPDATE LockTable SET theLock = 1
   WHERE diskServerId = (
     SELECT DS.diskserver_id
       FROM (
         -- The double level of selects is due to the fact that ORACLE is unable
         -- to use ROWNUM and ORDER BY at the same time. Thus, we have to first compute
         -- the maxRate and then select on it.
         SELECT diskserver.id diskserver_id
           FROM FileSystem FS, NbTapeCopiesInFS, DiskServer
          WHERE FS.id = NbTapeCopiesInFS.FS
            AND NbTapeCopiesInFS.NbTapeCopies > 0
            AND NbTapeCopiesInFS.Stream = StreamId
            AND FS.status IN (0, 1)         -- PRODUCTION, DRAINING
            AND DiskServer.id = FS.diskserver
            AND DiskServer.status IN (0, 1) -- PRODUCTION, DRAINING
            -- Ignore diskservers where a migrator already exists
            AND ((FS.nbMigratorStreams = 0 AND FS.nbRecallerStreams = 0) OR (optimized != 1))
          ORDER BY DiskServer.nbMigratorStreams ASC,
                   FileSystemRate(FS.readRate, FS.writeRate, FS.nbReadStreams, FS.nbWriteStreams,
                   FS.nbReadWriteStreams, FS.nbMigratorStreams, FS.nbRecallerStreams) DESC, dbms_random.value
          ) DS
      WHERE ROWNUM < 2)
  RETURNING diskServerId INTO dsid;
  -- Now we got our Diskserver but we lost all other data (due to the fact we had
  -- to do an update for the lock and we could not do a join in the update).
  -- So here we select all we need
  SELECT FN.name, FN.mountPoint, FN.diskserver, FN.id
    INTO diskServerName, mountPoint, fsDiskServer, fileSystemId
    FROM (
      SELECT DiskServer.name, FS.mountPoint, FS.diskserver, FS.id
        FROM FileSystem FS, NbTapeCopiesInFS, Diskserver
       WHERE FS.id = NbTapeCopiesInFS.FS
         AND DiskServer.id = FS.diskserver
         AND NbTapeCopiesInFS.NbTapeCopies > 0
         AND NbTapeCopiesInFS.Stream = StreamId
         AND FS.status IN (0, 1)    -- PRODUCTION, DRAINING
         AND FS.diskserver = dsId
         AND ((FS.nbMigratorStreams = 0 AND FS.nbRecallerStreams = 0) OR (optimized != 1))
       ORDER BY DiskServer.nbMigratorStreams ASC,
                FileSystemRate(FS.readRate, FS.writeRate, FS.nbReadStreams, FS.nbWriteStreams,
                FS.nbReadWriteStreams, FS.nbMigratorStreams, FS.nbRecallerStreams) DESC, dbms_random.value
       ) FN
   WHERE ROWNUM < 2;
  SELECT /*+ FIRST_ROWS(1)  LEADING(D T ST) */
         DiskCopy.path, DiskCopy.id, DiskCopy.castorfile, TapeCopy.id
    INTO path, dci, castorFileId, tapeCopyId
    FROM DiskCopy, TapeCopy, Stream2TapeCopy
   WHERE DiskCopy.status = 10 -- CANBEMIGR
     AND DiskCopy.filesystem = fileSystemId
     AND Stream2TapeCopy.parent = streamId
     AND TapeCopy.status = 2 -- WAITINSTREAMS
     AND Stream2TapeCopy.child = TapeCopy.id
     AND TapeCopy.castorfile = DiskCopy.castorfile
     AND ROWNUM < 2;
  SELECT CastorFile.FileId, CastorFile.NsHost, CastorFile.FileSize
    INTO fileId, nsHost, fileSize
    FROM CastorFile
   WHERE id = castorFileId;
  -- update status of selected tapecopy and stream
  UPDATE TapeCopy SET status = 3 -- SELECTED
   WHERE id = tapeCopyId;
  -- get locks on all the stream we will handle in order to avoid a
  -- deadlock with the attachTapeCopiesToStreams procedure
  --
  -- the deadlock would occur with updates to the NbTapeCopiesInFS
  -- table performed by the tr_stream2tapecopy_delete trigger triggered
  -- by the "DELETE FROM Stream2TapeCopy" statement below
  SELECT 1 BULK COLLECT
    INTO unused
    FROM Stream
    WHERE id IN (SELECT parent FROM Stream2TapeCopy WHERE child = tapeCopyId)
    ORDER BY id
    FOR UPDATE;
  UPDATE Stream
     SET status = 3, -- RUNNING
         lastFileSystemUsed = fileSystemId, lastButOneFileSystemUsed = lastFileSystemUsed
   WHERE id = streamId;
  -- time only if we changed FS
  UPDATE Stream SET lastFileSystemChange = getTime() WHERE id = streamId;
  -- detach the tapecopy from the stream now that it is SELECTED
  -- the triggers will update NbTapeCopiesInFS accordingly
  DELETE FROM Stream2TapeCopy
   WHERE child = tapeCopyId;

  -- Update Filesystem state
  updateFSMigratorOpened(fsDiskServer, fileSystemId, 0);

  EXCEPTION WHEN NO_DATA_FOUND THEN
    -- If the procedure was called with optimization enabled,
    -- rerun it again with optimization disabled to make sure
    -- there is really nothing to migrate!! Why? optimization
    -- excludes filesystems as being migration candidates if
    -- a migration stream is already running there. This allows
    -- us to maximise bandwidth to tape and not to saturate a
    -- diskserver with too many streams. However, in small disk
    -- pools this behaviour results in mounting, writing one
    -- file and dismounting of tapes as the tpdaemon reads ahead
    -- many files at a time! (#28097)
    IF optimized = 1 THEN
      bestTapeCopyForStream(streamId,
                            diskServerName,
                            mountPoint,
                            path,
                            dci,
                            castorFileId,
                            fileId,
                            nsHost,
                            fileSize,
                            tapeCopyId,
			    0);
      -- Since we recursively called ourselves, we should not do
      -- any update in the outer call
      RETURN;
    END IF;
    -- Reset last filesystems used
    UPDATE Stream
       SET lastFileSystemUsed = 0, lastButOneFileSystemUsed = 0
     WHERE id = streamId;
    -- No data found means the selected filesystem has no
    -- tapecopies to be migrated. Thus we go to next one
    -- However, we reset the NbTapeCopiesInFS row that failed
    -- This is not 100% safe but is far better than retrying
    -- in the same conditions
    IF 0 != fileSystemId THEN
      UPDATE NbTapeCopiesInFS
         SET NbTapeCopies = 0
       WHERE Stream = StreamId
         AND NbTapeCopiesInFS.FS = fileSystemId;
      bestTapeCopyForStream(streamId,
                            diskServerName,
                            mountPoint,
                            path,
                            dci,
                            castorFileId,
                            fileId,
                            nsHost,
                            fileSize,
                            tapeCopyId,
			    optimized);
    END IF;
END;

/* PL/SQL method implementing bestFileSystemForSegment */
CREATE OR REPLACE PROCEDURE bestFileSystemForSegment(segmentId IN INTEGER, diskServerName OUT VARCHAR2,
                                                     rmountPoint OUT VARCHAR2, rpath OUT VARCHAR2,
                                                     dcid OUT INTEGER, optimized IN INTEGER) AS
  fileSystemId NUMBER;
  cfid NUMBER;
  fsDiskServer NUMBER;
  fileSize NUMBER;
  nb NUMBER;
BEGIN
  -- First get the DiskCopy and see whether it already has a fileSystem
  -- associated (case of a multi segment file)
  -- We also select on the DiskCopy status since we know it is
  -- in WAITTAPERECALL status and there may be other ones
  -- INVALID, GCCANDIDATE, DELETED, etc...
  SELECT DiskCopy.fileSystem, DiskCopy.path, DiskCopy.id, DiskCopy.CastorFile
    INTO fileSystemId, rpath, dcid, cfid
    FROM TapeCopy, Segment, DiskCopy
   WHERE Segment.id = segmentId
     AND Segment.copy = TapeCopy.id
     AND DiskCopy.castorfile = TapeCopy.castorfile
     AND DiskCopy.status = 2; -- WAITTAPERECALL
  -- Check if the DiskCopy had a FileSystem associated
  IF fileSystemId > 0 THEN
    BEGIN
      -- It had one, force filesystem selection, unless it was disabled.
      SELECT DiskServer.name, DiskServer.id, FileSystem.mountPoint
        INTO diskServerName, fsDiskServer, rmountPoint
        FROM DiskServer, FileSystem
       WHERE FileSystem.id = fileSystemId
         AND FileSystem.status = 0 -- FILESYSTEM_PRODUCTION
         AND DiskServer.id = FileSystem.diskServer
         AND DiskServer.status = 0; -- DISKSERVER_PRODUCTION
      updateFsRecallerOpened(fsDiskServer, fileSystemId, 0);
    EXCEPTION WHEN NO_DATA_FOUND THEN
      -- Error, the filesystem or the machine was probably disabled in between
      raise_application_error(-20101, 'In a multi-segment file, FileSystem or Machine was disabled before all segments were recalled');
    END;
  ELSE
    fileSystemId := 0;
    -- The DiskCopy had no FileSystem associated with it which indicates that
    -- This is a new recall. We try and select a good FileSystem for it!
    FOR a IN (SELECT DiskServer.name, FileSystem.mountPoint, FileSystem.id,
                     FileSystem.diskserver, CastorFile.fileSize
                FROM DiskServer, FileSystem, DiskPool2SvcClass,
                     (SELECT id, svcClass from StageGetRequest UNION ALL
                      SELECT id, svcClass from StagePrepareToGetRequest UNION ALL
                      SELECT id, svcClass from StageRepackRequest UNION ALL
                      SELECT id, svcClass from StageGetNextRequest UNION ALL
                      SELECT id, svcClass from StageUpdateRequest UNION ALL
                      SELECT id, svcClass from StagePrepareToUpdateRequest UNION ALL
                      SELECT id, svcClass from StageUpdateNextRequest) Request,
                      SubRequest, CastorFile
               WHERE CastorFile.id = cfid
                 AND SubRequest.castorfile = cfid
                 AND SubRequest.status = 4
                 AND Request.id = SubRequest.request
                 AND Request.svcclass = DiskPool2SvcClass.child
                 AND FileSystem.diskpool = DiskPool2SvcClass.parent
                 AND FileSystem.free - FileSystem.minAllowedFreeSpace * FileSystem.totalSize > CastorFile.fileSize
                 AND FileSystem.status = 0 -- FILESYSTEM_PRODUCTION
                 AND DiskServer.id = FileSystem.diskServer
                 AND DiskServer.status = 0 -- DISKSERVER_PRODUCTION
                 -- Ignore diskservers where a recaller already exists
                 AND DiskServer.id NOT IN (
                   SELECT DISTINCT(FileSystem.diskServer)
                     FROM FileSystem
                    WHERE nbRecallerStreams != 0
                      AND optimized = 1
                 )
		 -- Ignore filesystems where a migrator is running
                 AND FileSystem.id NOT IN (
                   SELECT DISTINCT(FileSystem.id)
                     FROM FileSystem
                    WHERE nbMigratorStreams != 0
                      AND optimized = 1
                 )
            ORDER BY FileSystemRate(FileSystem.readRate, FileSystem.writeRate, FileSystem.nbReadStreams, FileSystem.nbWriteStreams, FileSystem.nbReadWriteStreams, FileSystem.nbMigratorStreams, FileSystem.nbRecallerStreams) DESC, dbms_random.value)
    LOOP
      diskServerName := a.name;
      rmountPoint    := a.mountPoint;
      fileSystemId   := a.id;
      -- Check that we don't already have a copy of this file on this filesystem.
      -- This will never happen in normal operations but may be the case if a filesystem
      -- was disabled and did come back while the tape recall was waiting.
      -- Even if we could optimize by cancelling remaining unneeded tape recalls when a
      -- fileSystem comes backs, the ones running at the time of the come back will have
      SELECT count(*) INTO nb
        FROM DiskCopy
       WHERE fileSystem = a.id
         AND castorfile = cfid
         AND status = 0; -- STAGED
      IF nb != 0 THEN
        raise_application_error(-20103, 'Recaller could not find a FileSystem in production in the requested SvcClass and without copies of this file');
      END IF;
      -- Set the diskcopy's filesystem
      UPDATE DiskCopy
         SET fileSystem = a.id
       WHERE id = dcid;
      updateFsRecallerOpened(a.diskServer, a.id, a.fileSize);
      RETURN;
    END LOOP;

    -- If we didn't find a filesystem rerun with optimization disabled
    IF fileSystemId = 0 THEN
      IF optimized = 1 THEN
        bestFileSystemForSegment(segmentId, diskServerName, rmountPoint, rpath, dcid, 0);
	RETURN;
      ELSE
        RAISE NO_DATA_FOUND; -- we did not find any suitable FS, even without optimization
      END IF;
    END IF;
  END IF;

  EXCEPTION WHEN NO_DATA_FOUND THEN
    -- Just like in bestTapeCopyForStream if we were called with optimization enabled
    -- and nothing was found, rerun the procedure again with optimization disabled to
    -- truly make sure nothing is found!
    IF optimized = 1 THEN
      bestFileSystemForSegment(segmentId, diskServerName, rmountPoint, rpath, dcid, 0);
      -- Since we recursively called ourselves, we should not do
      -- any update in the outer call
      RETURN;
    ELSE
      RAISE;
    END IF;
END;

/* PL/SQL method implementing fileRecallFailed (changed for the tapegateway) */

/* PL/SQL method implementing fileRecallFailed (changed for the tapegateway) */

create or replace
PROCEDURE fileRecallFailed(tapecopyId IN INTEGER) AS
 SubRequestId NUMBER;
 dci NUMBER;
 fsId NUMBER;
 fileSize NUMBER;
BEGIN
  BEGIN
    SELECT SubRequest.id, DiskCopy.id, CastorFile.filesize
      INTO SubRequestId, dci, fileSize FROM TapeCopy, SubRequest, DiskCopy, CastorFile
      WHERE TapeCopy.id = tapecopyId
      AND CastorFile.id = TapeCopy.castorFile
      AND DiskCopy.castorFile = TapeCopy.castorFile
      AND SubRequest.diskcopy(+) = DiskCopy.id
      AND DiskCopy.status = 2; -- DISKCOPY_WAITTAPERECALL
    UPDATE DiskCopy SET status = 4 WHERE id = dci RETURNING fileSystem INTO fsid; -- DISKCOPY_FAILED
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
  EXCEPTION WHEN NO_DATA_FOUND THEN
    NULL; -- I give -1 to the databuffer because it cannot be NULL
  END;
END;
/

/* PL/SQL method implementing streamsToDo */
CREATE OR REPLACE PROCEDURE streamsToDo(res OUT castor.Stream_Cur) AS
  streams "numList";
BEGIN
  SELECT UNIQUE Stream.id BULK COLLECT INTO streams
    FROM Stream, NbTapeCopiesInFS, FileSystem, DiskServer
   WHERE Stream.status = 0 -- PENDING
     AND NbTapeCopiesInFS.stream = Stream.id
     AND NbTapeCopiesInFS.NbTapeCopies > 0
     AND FileSystem.id = NbTapeCopiesInFS.FS
     AND FileSystem.status IN (0, 1) -- PRODUCTION, DRAINING
     AND DiskServer.id = FileSystem.DiskServer
     AND DiskServer.status IN (0, 1); -- PRODUCTION, DRAINING
  FORALL i in streams.FIRST..streams.LAST
    UPDATE Stream SET status = 1 -- WAITDRIVE
    WHERE id = streams(i);

  OPEN res FOR
    SELECT Stream.id, Stream.InitialSizeToTransfer, Stream.status,
           TapePool.id, TapePool.name
      FROM Stream, TapePool
     WHERE Stream.id MEMBER OF streams
       AND Stream.TapePool = TapePool.id;
END;

/* PL/SQL method implementing fileRecalled */
CREATE OR REPLACE PROCEDURE fileRecalled(tapecopyId IN INTEGER) AS
  subRequestId NUMBER;
  requestId NUMBER;
  dci NUMBER;
  reqType NUMBER;
  cfId NUMBER;
  fsId NUMBER;
  fs NUMBER;
  gcw NUMBER;
  gcwProc VARCHAR(2048);
  ouid INTEGER;
  ogid INTEGER;
BEGIN
  SELECT SubRequest.id, SubRequest.request, DiskCopy.id, CastorFile.id, DiskCopy.fileSystem, Castorfile.FileSize
    INTO subRequestId, requestId, dci, cfId, fsId, fs
    FROM TapeCopy, SubRequest, DiskCopy, CastorFile
   WHERE TapeCopy.id = tapecopyId
     AND CastorFile.id = TapeCopy.castorFile
     AND DiskCopy.castorFile = TapeCopy.castorFile
     AND SubRequest.diskcopy(+) = DiskCopy.id
     AND DiskCopy.status = 2;  -- DISKCOPY_WAITTAPERECALL
  -- cleanup:
  -- delete any previous failed diskcopy for this castorfile (due to failed recall attempts for instance)
  DELETE FROM Id2Type WHERE id IN (SELECT id FROM DiskCopy WHERE castorFile = cfId AND status = 4);
  DELETE FROM DiskCopy WHERE castorFile = cfId AND status = 4;  -- FAILED
  -- mark as invalid any previous diskcopy in disabled filesystems, which may have triggered this recall
  UPDATE DiskCopy SET status = 7  -- INVALID
   WHERE fileSystem IN (SELECT id FROM FileSystem WHERE status = 2)  -- DISABLED
     AND castorFile = cfId
     AND status = 0;  -- STAGED

  SELECT type INTO reqType FROM Id2Type WHERE id =
    (SELECT request FROM SubRequest WHERE id = subRequestId);
  -- Repack handling:
  -- create the tapecopies for waiting subrequests and update diskcopies
  IF reqType = 119 THEN  -- OBJ_StageRepackRequest
    startRepackMigration(subRequestId, cfId, dci, ouid, ogid);
  ELSE
    DECLARE
      svcClassId NUMBER;
      gcw VARCHAR2(2048);
    BEGIN
      SELECT Request.svcClass, euid, egid INTO svcClassId, ouid, ogid
        FROM (SELECT id, svcClass, euid, egid FROM StageGetRequest UNION ALL
              SELECT id, svcClass, euid, egid FROM StagePrepareToGetRequest UNION ALL
              SELECT id, svcClass, euid, egid FROM StageUpdateRequest UNION ALL
              SELECT id, svcClass, euid, egid FROM StagePrepareToUpdateRequest) Request
       WHERE Request.id = requestId;
      gcwProc := castorGC.getRecallWeight(svcClassId);
      EXECUTE IMMEDIATE 'BEGIN :newGcw := ' || gcwProc || '(:size); END;'
        USING OUT gcw, IN fs;
      UPDATE DiskCopy
         SET status = 0,  -- DISKCOPY_STAGED
             lastAccessTime = getTime(),  -- for the GC, effective lifetime of this diskcopy starts now
             gcWeight = gcw,
             diskCopySize = fs
       WHERE id = dci;
    END;
    -- restart this subrequest if it's not a repack one
    UPDATE SubRequest
       SET status = 1, lastModificationTime = getTime(), parent = 0  -- SUBREQUEST_RESTART
     WHERE id = subRequestId;
  END IF;
  -- restart other requests waiting on this recall
  UPDATE SubRequest
     SET status = 1, lastModificationTime = getTime(), parent = 0  -- SUBREQUEST_RESTART
   WHERE parent = subRequestId;
  -- update filesystem status
  updateFsFileClosed(fsId);
  -- Trigger the creation of additional copies of the file, if necessary.
  replicateOnClose(cfId, ouid, ogid);
END;


/* PL/SQL method implementing resetStream */
CREATE OR REPLACE PROCEDURE resetStream (sid IN INTEGER) AS
  nbRes NUMBER;
  unused NUMBER;
BEGIN
  BEGIN
    -- First lock the stream
    SELECT id INTO unused FROM Stream WHERE id = sid FOR UPDATE;
    -- Selecting any column with hint FIRST_ROW and relying
    -- on the exception mechanism in case nothing is found is
    -- far better than issuing a SELECT count(*) because ORACLE
    -- will then ignore the FIRST_ROWS and take ages...
    SELECT /*+ FIRST_ROWS */ Tapecopy.id INTO nbRes
      FROM Stream2TapeCopy, TapeCopy
     WHERE Stream2TapeCopy.Parent = sid
       AND Stream2TapeCopy.Child = TapeCopy.id
       AND status = 2 -- TAPECOPY_WAITINSTREAMS
       AND ROWNUM < 2;
    -- We'we found one, update stream status
    UPDATE Stream SET status = 6, tape = 0, lastFileSystemChange = 0
     WHERE id = sid; -- STREAM_STOPPED
    -- to avoid to by-pass the stream policy if it is used
  EXCEPTION  WHEN NO_DATA_FOUND THEN
    -- We've found nothing, delete stream
    DELETE FROM Stream2TapeCopy WHERE Parent = sid;
    DELETE FROM Id2Type WHERE id = sid;
    DELETE FROM Stream WHERE id = sid;
  END;
  -- in any case, unlink tape and stream
  UPDATE Tape SET Status=0, Stream = 0 WHERE Stream = sid;
END;

/* PL/SQL method implementing rtcpclientdCleanUp */
CREATE OR REPLACE PROCEDURE rtcpclientdCleanUp AS
BEGIN
  EXECUTE IMMEDIATE 'TRUNCATE TABLE Stream2TapeCopy';
  UPDATE NbTapeCopiesInFS SET NbTapeCopies = 0;
  UPDATE TapeCopy SET status = 1 WHERE status IN (2, 3, 7);
  UPDATE Stream SET status = 0;
  UPDATE tape SET status = 1 WHERE status IN (2, 3, 4);
  UPDATE tape SET stream = 0 WHERE stream != 0;
  UPDATE Stream SET tape = 0 WHERE tape != 0;
END;


