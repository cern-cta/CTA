/* trigger to be on just if the tapegateway is running */

/* trigger recall */

create or replace TRIGGER tr_Tape_Pending
AFTER UPDATE of status ON Tape
FOR EACH ROW
WHEN (new.status = 1 and new.tpmode=0)
DECLARE 
  reqId NUMBER;
  unused NUMBER;
BEGIN
  BEGIN
    SELECT id INTO unused FROM tapegatewayrequest WHERE taperecall=:new.id; 
  EXCEPTION WHEN NO_DATA_FOUND THEN
    INSERT INTO TapeGatewayRequest(accessMode, startTime, lastVdqmPingTime, vdqmVolReqId, id, streamMigration, TapeRecall, Status) VALUES (0,null,null,null,ids_seq.nextval,null,:new.id,1)  RETURNING id into reqId;
    INSERT INTO id2type (id,type) VALUES (reqId,172); 
  END; 
END;


/* trigger migration */

create or replace TRIGGER tr_Stream_Pending
AFTER UPDATE of status ON Stream
FOR EACH ROW
WHEN (new.status = 0 )
DECLARE 
  reqId NUMBER;
  unused NUMBER;
BEGIN 
  BEGIN
    SELECT id INTO unused FROM tapegatewayrequest WHERE streammigration=:new.id; 
  EXCEPTION WHEN NO_DATA_FOUND THEN
    INSERT INTO TapeGatewayRequest (accessMode, startTime, lastVdqmPingTime, vdqmVolReqId, id, streamMigration, TapeRecall, Status) VALUES (1,null,null,null,ids_seq.nextval,:new.id,null,0) RETURNING id into reqId;
    INSERT INTO id2type (id,type) VALUES (reqId,172);
  END;
END;

/* PROCEDURE */

create or replace PROCEDURE tg_attachDriveReqsToTapes(tapeRequestIds IN castor."cnumList", vdqmIds IN castor."cnumList", dgns IN castor."strList", labels IN castor."strList", densities IN castor."strList" ) AS
BEGIN
-- update tapegatewayrequest which have been submitted to vdqm =>  WAITING_TAPESERVER
  FORALL i IN tapeRequestIds.FIRST .. tapeRequestIds.LAST
    UPDATE tapegatewayrequest set status=2, lastvdqmpingtime=gettime(), starttime= gettime(), vdqmvolreqid=vdqmIds(i) 
      WHERE id=tapeRequestIds(i); -- these are the ones waiting for vdqm to be sent again
-- update stream for migration    
  FORALL i IN tapeRequestIds.FIRST .. tapeRequestIds.LAST
    UPDATE stream SET status=1 
      WHERE id = 
        (select streammigration FROM tapegatewayrequest WHERE accessmode=1 AND  id=tapeRequestIds(i));  
-- update tape for migration and recall    
  FORALL i IN tapeRequestIds.FIRST .. tapeRequestIds.LAST
    UPDATE tape SET status=2, dgn=dgns(i), label=labels(i), density= densities(i) 
      WHERE id = 
        (SELECT tape FROM stream, tapegatewayrequest 
          WHERE stream.id = tapegatewayrequest.streammigration AND tapegatewayrequest.accessmode=1 
          AND tapegatewayrequest.id=tapeRequestIds(i)) 
      OR id =
        (SELECT taperecall FROM tapegatewayrequest  WHERE accessmode=0 AND  id=tapeRequestIds(i));  
      
  COMMIT;
END;
/

create or replace PROCEDURE tg_attachTapesToStreams (startFseqs IN castor."cnumList", strIds IN castor."cnumList", tapeVids IN castor."strList") AS
 CONSTRAINT_VIOLATED EXCEPTION;
  PRAGMA EXCEPTION_INIT(CONSTRAINT_VIOLATED, -1);
  nsHostName VARCHAR2(2048);
  tapeId NUMBER;
  strId NUMBER;
BEGIN
 FOR i IN strIds.FIRST .. strIds.LAST LOOP
      -- update tapegatewayrequest
      UPDATE tapegatewayrequest SET status=1, lastfseq= startfseqs(i) WHERE streammigration= strids(i); -- TO_BE_SENT_TO_VDQM
      UPDATE Tape SET stream=strids(i), status=2 WHERE tpmode=1 AND vid=tapeVids(i) RETURNING id INTO tapeId; 
      IF tapeId IS NULL THEN 
        BEGIN
        -- insert a tape if it is not already in the database
          INSERT INTO Tape (vid,side,tpmode,errmsgtxt,errorcode,severity,vwaddress,id,stream,status) values (tapeVids(i),0,1,null,0,0,null,ids_seq.nextval,0,2) returning id into tapeId;
          INSERT INTO id2type (id,type) values (tapeId,29);
      
        EXCEPTION WHEN CONSTRAINT_VIOLATED THEN
       -- retry the select since a creation was done in between
          UPDATE Tape SET stream=strids(i), status=2 WHERE tpmode=1 AND vid=tapeVids(i) RETURNING id INTO tapeId;
        END;
      END IF;  
    -- update the stream
    UPDATE Stream SET tape=tapeId WHERE Stream.id=strIds(i) RETURNING id into strId;

 END LOOP;
 COMMIT;
END;
/

create or replace PROCEDURE tg_endTapeSession(transactionId IN NUMBER, inputErrorCode IN INTEGER)  AS
CONSTRAINT_VIOLATED EXCEPTION;
  PRAGMA EXCEPTION_INIT(CONSTRAINT_VIOLATED, -02292);
  tpId NUMBER;
  trId NUMBER;
  strId NUMBER;
  reqMode INTEGER;
  segNum INTEGER;
  cfId NUMBER;
  tcIds "numList";
  srIds "numList";
  outMode INTEGER;
BEGIN 
  BEGIN
  -- delete taperequest
    DELETE FROM tapegatewayrequest WHERE vdqmvolreqid = transactionId RETURNING id,taperecall, streammigration,accessmode INTO trId,tpId, strid, reqMode;
    DELETE FROM id2type WHERE id=trId;
   -- delete taperequest   
    DELETE FROM tapegatewaysubrequest WHERE request=trId RETURNING tapecopy,id bulk collect INTO tcIds, srIds;
    FORALL i in srIds.FIRST .. srIds.LAST 
     DELETE FROM id2type WHERE id=srIds(i);

    IF outmode = 0 THEN
     -- read
      SELECT id INTO tpId FROM tape WHERE id =tpId FOR UPDATE;
      IF inputErrorCode != 0 THEN  
        -- if a failure is reported
        -- fail all the segments
        FORALL i in tcIds.FIRST .. tcIds.LAST 
          UPDATE segment SET status=6 WHERE copy = tcIds(i);
        -- mark tapecopies as  REC_RETRY
        FORALL i in tcIds.FIRST .. tcIds.LAST 
          UPDATE tapecopy  SET status=8, errorcode=inputErrorCode WHERE id=tcIds(i);
      END IF;
      -- resurrect lost tapesubrequest
      UPDATE segment SET status=0 WHERE status = 7 AND tape=tpId;
      -- check if there is work for this tape
      SELECT count(*) into segNum from segment where tape=tpId AND status=0;
       -- still segments in zero ... to be restarted
      IF segNum > 0 THEN  
       UPDATE tape set status = 8 WHERE id=tpId; --WAIT POLICY for rechandler
      ELSE
       UPDATE tape set status = 0 WHERE id=tpId; --UNUSED
      END IF;
              
    ELSE 
      -- write        
      -- reset stream
      BEGIN  
        DELETE FROM Stream WHERE id = strId;
        DELETE FROM id2type WHERE id=strId;
      EXCEPTION WHEN CONSTRAINT_VIOLATED THEN
      -- constraint violation and we cannot delete the stream because there is an entry in Stream2TapeCopy
        UPDATE Stream SET status = 6, tape = null, lastFileSystemChange = null WHERE id = strId; 
      END;
      -- reset tape
      UPDATE Tape SET status=0, Stream = null WHERE Stream = strId;
      IF inputErrorCode != 0 THEN  
        -- if a failure is reported
        --retry MIG_RETRY     
        FORALL i in tcIds.FIRST .. tcIds.LAST 
           UPDATE tapecopy  SET status=9, errorcode=inputErrorCode, nbretry=0 WHERE id=tcIds(i); 
      END IF;
      
    END IF;
    EXCEPTION WHEN  NO_DATA_FOUND THEN
      null;
    END;  
    
   COMMIT;
END;
/



create or replace PROCEDURE tg_failFileTransfer(transactionId IN NUMBER,inputFileId IN NUMBER, inputNsHost IN VARCHAR2, inputFseq IN INTEGER, inputErrorCode IN INTEGER)  AS
cfId NUMBER;
trId NUMBER;
strId NUMBER;
tpId NUMBER;
tcId NUMBER;
srId NUMBER;
outMode INTEGER;

BEGIN
  SELECT accessmode, id, taperecall, streammigration INTO outMode, trId, tpId, strId FROM  tapegatewayrequest 
         WHERE transactionid = vdqmvolreqid FOR UPDATE;   
  SELECT id INTO cfId from castorfile WHERE fileid=inputFileId and nshost=inputNsHost FOR UPDATE;
 -- delete tape subrequest
  DELETE FROM tapegatewaysubrequest WHERE fseq=inputFseq AND request=trId RETURNING tapecopy,id  INTO tcId, srId;
  DELETE FROM id2type WHERE id=srId;
  
  IF outmode = 0 THEN
     -- read
     -- retry REC_RETRY 
     -- failed the segment on that tape
     UPDATE segment SET status=6, severity=inputErrorCode, errorCode=-1 WHERE fseq= inputfseq and tape=tpId returning copy INTO tcId; 
     -- mark tapecopy as REC_RETRY
     UPDATE tapecopy  SET status=8, errorcode=inputErrorCode WHERE id=tcId;   
  ELSE 
     -- write
     SELECT tape.id INTO tpId FROM tape, stream where tape.id = stream.tape and stream.id = strId;    
     --retry MIG_RETRY
     -- mark tapecopy as MIG_RETRY
     UPDATE tapecopy  SET status=9, errorcode=inputErrorCode WHERE id=tcId; 
  END IF;  
EXCEPTION WHEN  NO_DATA_FOUND THEN
    null;
END;
/

create or replace PROCEDURE  tg_getFailedMigrations
(tcs OUT castor.TapeCopy_Cur) AS
ids "numList";
BEGIN
  -- get TAPECOPY_MIG_RETRY
  OPEN tcs FOR
    SELECT copynb, id, castorfile, status,errorCode, nbretry 
      FROM TapeCopy
     WHERE status=9 AND ROWNUM < 1000 FOR UPDATE SKIP LOCKED; 
END;
/

create or replace PROCEDURE  tg_getFailedRecalls
(tcs OUT castor.TapeCopy_Cur) AS
ids "numList";
BEGIN
  -- get TAPECOPY_REC_RETRY
  OPEN tcs FOR
    SELECT copynb, id, castorfile, status,errorCode, nbretry 
      FROM TapeCopy
     WHERE status=8 AND ROWNUM < 1000 FOR UPDATE SKIP LOCKED; 
END;
/

create or replace PROCEDURE tg_getFileToMigrate (transactionId IN NUMBER, ret OUT INTEGER, outVid OUT NOCOPY VARCHAR2, outputFile OUT castorTape.FileToMigrateCore_cur)  AS
  strId NUMBER;
  ds VARCHAR2(2048);
  mp VARCHAR2(2048);
  path  VARCHAR2(2048);
  dcid NUMBER;
  cfId NUMBER;
  fileId NUMBER;
  nsHost VARCHAR2(2048);
  fileSize  INTEGER;
  tcId  INTEGER;
  lastTime NUMBER;
  knownName VARCHAR2(2048);
  newFseq NUMBER;
  trId NUMBER;
  srId NUMBER;
  tcNum INTEGER;
BEGIN
   ret:=0;
   BEGIN 
    -- last fseq is the first fseq value available for this migration
      SELECT streammigration,lastfseq,id INTO strId,newFseq,trId from tapegatewayrequest  WHERE vdqmvolreqid=transactionId FOR UPDATE;
      SELECT vid INTO outVid FROM tape WHERE id IN (SELECT tape FROM stream WHERE id=strId);
   EXCEPTION WHEN NO_DATA_FOUND THEN
    -- stream is over
    ret:=-2;
    COMMIT;
    RETURN;
   END;  
   
   SELECT COUNT(*) INTO tcNum FROM stream2tapecopy WHERE parent=strId;
   IF tcnum = 0 THEN 
    ret:=-1; -- NO MORE FILES
    COMMIT;
    RETURN;
   END IF;
   
   besttapecopyforstream(strId,ds,mp,path,dcid,cfId,fileid,nshost,filesize,tcid,lastTime);
   
   IF tcId = 0 THEN
      ret:=-1; -- besttapecopyforstream cannot find a suitable candidate
      COMMIT;
      RETURN;
   END IF;
  
   SELECT lastknownfilename INTO knownName FROM castorfile WHERE id=cfId; -- we rely on the check done before
   
   INSERT INTO tapegatewaysubrequest (fseq, id, tapecopy, request,diskcopy) VALUES (newFseq, ids_seq.nextval, tcId,trId, dcid) RETURNING id INTO srId;
   INSERT INTO id2type (id,type) VALUES (srId,180);    
   UPDATE tapegatewayrequest SET lastfseq=lastfseq+1 WHERE id=trId; --increased when we have the proper value
   OPEN outputFile FOR 
    SELECT fileId,nshost,lastTime,ds,mp,path,knownName,fseq,filesize,id FROM tapegatewaysubrequest WHERE id =srId;
END;
/

create or replace PROCEDURE tg_getFileToRecall (transactionId IN NUMBER, ret OUT INTEGER, vidOut OUT NOCOPY VARCHAR2, outputFile OUT castorTape.FileToRecallCore_Cur) AS
trId INTEGER;
ds VARCHAR2(2048);
mp VARCHAR2(2048);
path VARCHAR2(2048); 
segId NUMBER;
dcId NUMBER;
tcId NUMBER;
tapeId NUMBER;
srId NUMBER;
newFseq INTEGER;
cfId NUMBER;
BEGIN 
  ret:=0;
  BEGIN
  -- master lock on the taperequeststate
    SELECT id, taperecall INTO trId, tapeid FROM tapegatewayrequest WHERE vdqmVolReqId=transactionid FOR UPDATE;  
    SELECT vid INTO vidOut FROM tape WHERE id=tapeId;
  EXCEPTION WHEN  NO_DATA_FOUND THEN
     ret:=-2; -- ERROR
     RETURN;
  END; 
  
  BEGIN   
     SELECT id INTO segId FROM 
      (select id FROM Segment WHERE  tape = tapeId  AND status=0 ORDER BY fseq ASC) 
    WHERE ROWNUM<2;
    SELECT fseq, copy INTO  newFseq,tcId FROM Segment WHERE id = segId;
    SELECT castorfile.id INTO cfId FROM castorfile,tapecopy 
      WHERE  castorfile.id=tapecopy.castorfile AND tapecopy.id=tcId FOR UPDATE OF castorfile.id;
  EXCEPTION WHEN NO_DATA_FOUND THEN
     ret:=-1; -- NO MORE FILES
     COMMIT;
     RETURN;
  END;
  DECLARE
    application_error EXCEPTION;
    PRAGMA EXCEPTION_INIT(application_error,-20115);
  BEGIN
    bestFileSystemForSegment(segId,ds,mp,path,dcId);
  EXCEPTION WHEN  application_error THEN 
    ret:=-1;
    COMMIT;
    RETURN;
  END;
 
  INSERT INTO tapegatewaysubrequest (fseq, id, tapecopy, request,diskcopy) VALUES (newFseq, ids_seq.nextval, tcId,trId, dcid) 
	RETURNING id INTO srId;
  INSERT INTO id2type (id,type) VALUES (srId,180);    
  
  UPDATE Segment SET status=7 WHERE id=segId AND status = 0;
  OPEN outputFile FOR 
   SELECT castorfile.fileid, castorfile.nshost, ds, mp, path, newFseq, srId FROM tapecopy,castorfile  WHERE tcId=tapecopy.id 
         AND castorfile.id=tapecopy.castorfile;
END;
/

create or replace PROCEDURE
tg_getRepackVidAndFileInfo( inputFileId IN NUMBER, inputNsHost IN VARCHAR2, inFseq IN INTEGER, transactionId IN NUMBER, outRepackVid OUT NOCOPY VARCHAR2, strVid OUT NOCOPY VARCHAR, outCopyNb OUT INTEGER,outLastTime OUT NUMBER) AS 
cfId NUMBER;
BEGIN
  outrepackvid:=NULL;
  BEGIN 
    -- Get the repackvid field from the existing request (if none, then we are not in a repack process
     
     SELECT StageRepackRequest.repackvid, castorfile.lastupdatetime, castorfile.id
    INTO outRepackVid, outlasttime, cfId
    FROM SubRequest, DiskCopy, CastorFile, StageRepackRequest
   WHERE stagerepackrequest.id = subrequest.request
     AND diskcopy.id = subrequest.diskcopy
     AND diskcopy.status = 10 -- CANBEMIGR
     AND subrequest.status = 12 -- SUBREQUEST_REPACK
     AND diskcopy.castorfile = castorfile.id
     AND castorfile.fileid = inputFileId AND castorfile.nshost = inputNsHost
     AND ROWNUM < 2;
      
  EXCEPTION WHEN NO_DATA_FOUND THEN
    -- ignore the repack state
     SELECT  castorfile.lastupdatetime, castorfile.id
    INTO outlasttime, cfId
    FROM CastorFile WHERE castorfile.fileid = inputFileId AND castorfile.nshost = inputNsHost AND ROWNUM < 2;
  END;
  SELECT copynb INTO outcopynb FROM tapecopy WHERE castorfile=cfId AND id IN 
      (SELECT tapecopy FROM tapegatewaysubrequest WHERE tapegatewaysubrequest.fseq= inFseq AND tapegatewaysubrequest.request 
          IN (SELECT id FROM tapegatewayrequest WHERE vdqmVolReqId = transactionId ));
  SELECT vid INTO strVid FROM tape where id IN (SELECT tape from stream WHERE
    id IN (SELECT streammigration FROM tapegatewayrequest WHERE vdqmVolReqId = transactionId));
END;
/create or replace PROCEDURE tg_getSegmentInfo (transactionId IN NUMBER, inFileId IN NUMBER, inHost IN VARCHAR2,inFseq IN INTEGER, inPath IN VARCHAR2 , outVid OUT NOCOPY VARCHAR2, outCopy OUT INTEGER ) AS
filePath VARCHAR2(2048);
fsId NUMBER;
cfId NUMBER;
fileDiskserver VARCHAR2(2048);
fileMountpoint VARCHAR2(2048);
givenPath VARCHAR2(2048);
BEGIN
  -- check also the path
  SELECT path,filesystem,castorfile INTO filePath,fsId,cfId FROM diskcopy,castorfile 
    WHERE castorfile.id = diskcopy.castorfile AND castorfile.id IN 
      (SELECT id FROM castorfile WHERE fileid=inFileId AND nshost=inHost) AND diskcopy.status=2;
  SELECT diskserver.name, filesystem.mountpoint INTO fileDiskserver, fileMountpoint FROM diskserver,filesystem 
    WHERE diskserver.id= filesystem.diskserver AND filesystem.id=fsid;
  SELECT filediskserver||':'|| filemountpoint||filepath INTO givenPath FROM dual;
  SELECT copynb,vid INTO outCopy, outvid FROM tape,segment,tapecopy 
    WHERE tape.id=segment.tape AND fseq=inFseq AND segment.copy = tapecopy.id AND tapecopy.castorfile=cfId AND inPath = givenPath; 
END;
/

create or replace PROCEDURE tg_getStreamsWithoutTapes
(strList OUT castorTape.Stream_Cur) AS
BEGIN
 -- get request in status TO_BE_RESOLVED
 OPEN strList FOR
    SELECT stream.id, stream.initialsizetotransfer, stream.status, stream.tapepool, tapepool.name
      FROM Stream,Tapepool WHERE stream.id IN 
        (select streamMigration from TapegatewayRequest WHERE status=0 ) 
      AND stream.tapepool=tapepool.id FOR UPDATE OF Stream.id SKIP LOCKED;   
END;
/

create or replace PROCEDURE tg_getTapesWithDriveReqs
(timeLimit IN NUMBER, taperequest OUT castorTape.tapegatewayrequest_Cur) AS
trIds "numList";
BEGIN 
-- get requests	 in  WAITING_TAPESERVER and ONGOING
	SELECT id BULK COLLECT into trIds FROM tapegatewayrequest 
    WHERE status IN (2,3) 
    AND  gettime() - TapeGatewayRequest.lastVdqmPingTime > timeLimit FOR UPDATE SKIP LOCKED; 
	
  FORALL i IN trids.FIRST .. trids.LAST
	 UPDATE tapegatewayrequest SET lastVdqmPingTime = gettime() WHERE id = trIds(i);
 
	OPEN taperequest FOR
    SELECT TapeGatewayRequest.accessMode, TapeGatewayRequest.id,TapeGatewayRequest.starttime, tapegatewayrequest.lastvdqmpingtime, tapegatewayrequest.vdqmvolreqid,tapegatewayrequest.status, Tape.vid  
      FROM Stream,tapegatewayrequest,Tape WHERE tapegatewayrequest.id  IN (SELECT /*+ CARDINALITY(trTable 5) */ * FROM TABLE(trIds) trTable)  AND  Stream.id = tapegatewayrequest.streamMigration AND Stream.tape=Tape.id AND tapegatewayrequest.accessMode=1
    		UNION ALL
    SELECT tapegatewayrequest.accessMode, tapegatewayrequest.id,tapegatewayrequest.starttime, tapegatewayrequest.lastvdqmpingtime, tapegatewayrequest.vdqmvolreqid,tapegatewayrequest.status, Tape.vid  FROM tapegatewayrequest,Tape WHERE 
	  Tape.id=tapegatewayrequest.tapeRecall AND tapegatewayrequest.accessMode=0 AND  tapegatewayrequest.id IN (SELECT /*+ CARDINALITY(trTable 5) */ * FROM TABLE(trIds) trTable);
END;
/

create or replace PROCEDURE tg_getTapesWithoutDriveReqs
(tapesToSubmit OUT castorTape.TapeGatewayRequestCore_Cur) AS
ids "numList";
BEGIN
-- get requests in TO_BE_SENT_TO_VDQM
SELECT id BULK COLLECT INTO ids FROM tapegatewayrequest WHERE status=1 FOR UPDATE SKIP LOCKED; 
 OPEN tapesToSubmit FOR  
   SELECT  tape.tpmode, tape.side, tape.vid, tapegatewayrequest.id 
      FROM Stream,TapeGatewayRequest,Tape 
     WHERE TapeGatewayrequest.id  IN (SELECT /*+ CARDINALITY(trTable 5) */  * FROM TABLE(ids) trTable)  AND 
	 Stream.id = TapeGatewayRequest.streamMigration AND Stream.tape=Tape.id AND TapeGatewayRequest.accessMode=1 
 UNION ALL  SELECT tape.tpmode, tape.side, tape.vid, tapegatewayrequest.id 
      FROM  TapeGatewayRequest,Tape WHERE TapeGatewayRequest.id IN  (SELECT /*+ CARDINALITY(trTable 5) */ * FROM TABLE(ids) trTable ) AND
	Tape.id=TapeGatewayRequest.tapeRecall AND TapeGatewayRequest.accessMode=0;
END;
/

create or replace PROCEDURE  tg_getTapeToRelease  
( inputVdqmReqId IN INTEGER, outputTape OUT NOCOPY VARCHAR2, outputMode OUT INTEGER ) AS
tpId NUMBER;
trId NUMBER;
strId NUMBER;
reqMode INTEGER;
mIds "numList";
tcIds "numList";
segNum INTEGER;
BEGIN
  SELECT tape.vid, tapegatewayrequest.accessmode INTO outputtape, outputmode 
    FROM tapegatewayrequest,tape,stream 
    WHERE vdqmvolreqid = inputvdqmreqid 
    AND (tape.id=tapegatewayrequest.taperecall 
    OR (stream.id=tapegatewayrequest.streammigration AND stream.tape=tape.id));
END;
/

create or replace PROCEDURE tg_invalidateFile(transactionId IN NUMBER,inputFileId IN NUMBER, inputNsHost IN VARCHAR2, inputFseq IN INTEGER, inputErrorCode IN INTEGER)  AS
BEGIN
     UPDATE tapegatewayrequest SET lastfseq=lastfseq-1 WHERE vdqmvolreqid=transactionid; 
     tg_failfiletransfer(transactionid,inputfileid, inputNsHost, inputFseq, inputErrorCode);
END;
/

create or replace PROCEDURE tg_restartLostReqs(trIds IN castor."cnumList") AS
trId NUMBER;
vdqmId INTEGER;
BEGIN
 FORALL  i IN trIds.FIRST .. trIds.LAST 
     --  STATUS WAITING_TAPESERVER:  these are the lost ones => TO_BE_SENT_TO_VDQM
    UPDATE tapegatewayrequest SET lastvdqmpingtime = null, starttime = null, status=1  WHERE id=trIds(i) AND status =2;

 FOR  i IN trIds.FIRST .. trIds.LAST LOOP   
    BEGIN 
     --  STATUS ONGOING: these are the ones which were ongoing ... crash clean up needed 
      SELECT vdqmvolreqid INTO vdqmId  FROM  tapegatewayrequest WHERE id=trIds(i) AND status = 3; 
      tg_endTapeSession(vdqmId,0);
    EXCEPTION WHEN NO_DATA_FOUND THEN
     NULL;
    END;
 END LOOP;
 COMMIT;
END;
/

create or replace PROCEDURE tg_setFileMigrated (transactionId IN NUMBER, inputFileId  IN NUMBER,inputNsHost IN VARCHAR2, inputFseq IN INTEGER, streamReport OUT castor.StreamReport_Cur) AS
trId NUMBER;
tcNumb INTEGER;
cfId NUMBER;
tcId NUMBER;
srId NUMBER;
tcIds "numList";
srIds "numList";
dcId NUMBER;
BEGIN
 
  SELECT id  INTO trId from tapegatewayrequest WHERE tapegatewayrequest.vdqmvolreqid=transactionid FOR UPDATE;
  SELECT id  INTO cfId FROM castorfile WHERE  fileid=inputFileId AND nshost=inputNsHost FOR UPDATE;
   
  DELETE FROM tapegatewaysubrequest WHERE request =trId AND fseq=inputFseq RETURNING tapecopy,id, diskcopy INTO tcId, srId, dcId;
  DELETE FROM id2type WHERE id= srId;

  UPDATE tapecopy SET status=5 WHERE id=tcId;
   
  SELECT count(*) INTO tcNumb FROM tapecopy WHERE castorfile = cfId  AND STATUS != 5;
  -- let's check if another copy should be done
  IF tcNumb = 0 THEN
     UPDATE diskcopy SET status=0 WHERE castorfile = cfId AND status=10;
     DELETE FROM tapecopy WHERE castorfile=cfId returning id BULK COLLECT INTO tcIds;
   
     FORALL i IN tcIds.FIRST .. tcIds.LAST
       DELETE FROM id2type WHERE id=tcIds(i);
  END IF;

  FOR i IN ( SELECT id FROM subrequest WHERE castorfile=cfId AND status=12) LOOP
        archivesubreq( i.id ,8);
  END LOOP;
  
  OPEN streamReport FOR
   SELECT diskserver.name,filesystem.mountpoint FROM diskserver,filesystem,diskcopy 
    WHERE diskcopy.id=dcId AND diskcopy.filesystem=filesystem.id AND filesystem.diskserver = diskserver.id;
 COMMIT;
END;
/

create or replace PROCEDURE tg_setFileRecalled (transactionId IN NUMBER, inputFileId  IN NUMBER,inputNsHost IN VARCHAR2, inputFseq IN NUMBER, streamReport OUT castor.StreamReport_Cur) AS
  tcId NUMBER;
  dcId NUMBER;
  cfId NUMBER;
  unused NUMBER;
  segId NUMBER;
  tpId NUMBER;
  srId NUMBER;
  subRequestId NUMBER;
  requestId NUMBER;
  reqType NUMBER;
  fs NUMBER;
  gcw NUMBER;
  gcwProc VARCHAR(2048);
  ouid INTEGER;
  ogid INTEGER;
  svcClassId NUMBER;
  trId NUMBER;
BEGIN

  SELECT id, taperecall  INTO trId, tpId FROM tapegatewayrequest WHERE tapegatewayrequest.vdqmvolreqid=transactionid FOR UPDATE;
  SELECT id, fileSize into cfId, fs FROM castorfile WHERE  fileid=inputFileId AND nshost=inputNsHost FOR UPDATE;
   
  DELETE FROM tapegatewaysubrequest WHERE request =trId AND fseq=inputFseq RETURNING tapecopy,id, diskcopy INTO tcId, srId, dcId;
  DELETE FROM id2type WHERE id= srId;
 
  SELECT SubRequest.id, SubRequest.request
    INTO subRequestId, requestId
    FROM SubRequest
   WHERE SubRequest.diskcopy(+) = dcId;
   
  -- delete any previous failed diskcopy for this castorfile (due to failed recall attempts for instance)
  DELETE FROM Id2Type WHERE id IN (SELECT id FROM DiskCopy WHERE castorFile = cfId AND status = 4);
  DELETE FROM DiskCopy WHERE castorFile = cfId AND status = 4;  -- FAILED
  
  -- update diskcopy size and gweight
  SELECT Request.svcClass, euid, egid INTO svcClassId, ouid, ogid
    FROM (SELECT id, svcClass, euid, egid FROM StageGetRequest UNION ALL
          SELECT id, svcClass, euid, egid FROM StagePrepareToGetRequest UNION ALL
          SELECT id, svcClass, euid, egid FROM StageUpdateRequest UNION ALL
          SELECT id, svcClass, euid, egid FROM StagePrepareToUpdateRequest UNION ALL
          SELECT id, svcClass, euid, egid FROM StageRepackRequest) Request
   WHERE Request.id = requestId;
  gcwProc := castorGC.getRecallWeight(svcClassId);
  EXECUTE IMMEDIATE 'BEGIN :newGcw := ' || gcwProc || '(:size); END;'
    USING OUT gcw, IN fs;
  UPDATE DiskCopy
     SET status = 0,  -- DISKCOPY_STAGED
         lastAccessTime = getTime(),  -- for the GC, effective lifetime of this diskcopy starts now
         gcWeight = gcw,
         diskCopySize = fs
   WHERE id = dcid;
  -- determine the type of the request
  SELECT type INTO reqType FROM Id2Type WHERE id =
    (SELECT request FROM SubRequest WHERE id = subRequestId);
  IF reqType = 119 THEN  -- OBJ_StageRepackRequest
    startRepackMigration(subRequestId, cfId, dcid, ouid, ogid);
  ELSE
    -- restart this subrequest if it's not a repack one
    UPDATE SubRequest
       SET status = 1, getNextStatus = 1,  -- SUBREQUEST_RESTART, GETNEXTSTATUS_FILESTAGED
           lastModificationTime = getTime(), parent = 0
     WHERE id = subRequestId;
  END IF;
  -- restart other requests waiting on this recall
  UPDATE SubRequest
       SET status = 1, getNextStatus = 1,  -- SUBREQUEST_RESTART, GETNEXTSTATUS_FILESTAGED
           lastModificationTime = getTime(), parent = 0
   WHERE parent = subRequestId;
  -- Trigger the creation of additional copies of the file, if necessary.
  replicateOnClose(cfId, ouid, ogid);
  -- delete segment
  DELETE FROM id2type WHERE id IN (SELECT id FROM Segment WHERE copy=tcId);
  DELETE FROM segment   WHERE copy=tcId;
 
  -- delete tapecopy
  DELETE FROM tapecopy WHERE  id=tcId;
  DELETE FROM id2type WHERE id=tcId; 

  OPEN streamReport FOR
  select diskserver.name,filesystem.mountpoint from diskserver,filesystem,diskcopy
     WHERE diskcopy.castorfile=cfId AND diskcopy.status=2 AND diskcopy.filesystem=filesystem.id AND filesystem.diskserver = diskserver.id;
  COMMIT;
END;
/


create or replace PROCEDURE  tg_setMigRetryResult
(tcToRetry IN castor."cnumList", tcToFail IN castor."cnumList"  ) AS
srId NUMBER;
cfId NUMBER;
BEGIN
   -- check because oracle cannot handle empty buffer
  IF tcToRetry(1) != -1 THEN 
    -- restarted the one to be retried
    FORALL i IN tctoretry.FIRST .. tctoretry.LAST 
      UPDATE TapeCopy SET status=1,nbretry = nbretry+1  WHERE id = tcToRetry(i);    
  END IF;
  
  -- check because oracle cannot handle empty buffer
  IF tcToFail(1) != -1 THEN 
    -- fail the tapecopies   
    FORALL i IN tctofail.FIRST .. tctofail.LAST 
    UPDATE TapeCopy SET status=6 WHERE id = tcToFail(i);
    -- fail repack subrequests
    FOR i IN tcToFail.FIRST .. tcToFail.LAST LOOP
        BEGIN 
        -- we don't need a lock on castorfile because we cannot have a parallel migration of the same file using repack
          SELECT subrequest.id, subrequest.castorfile into srId, cfId FROM subrequest,tapecopy 
            WHERE tapecopy.id = tcToFail(i) AND  subrequest.castorfile = tapecopy.castorfile AND subrequest.status = 12;
          UPDATE diskcopy SET status=0 WHERE castorfile = cfId AND status=10; -- otherwise repack will wait forever
          -- STAGED because the copy on disk most probably is valid and the failure of repack happened during the migration 
          archivesubreq(srId,9);
        EXCEPTION WHEN NO_DATA_FOUND THEN
          NULL;
        END;
     END LOOP;
  END IF;
  
  COMMIT;
END;
/

create or replace PROCEDURE  tg_setRecRetryResult 
(tcToRetry IN castor."cnumList", tcToFail IN castor."cnumList"  ) AS
segIds "numList";
tcId NUMBER;
cfId NUMBER;
BEGIN
  -- I restart the recall that I want to retry
  -- check because oracle cannot handle empty buffer
  IF tcToRetry(1) != -1 THEN 
    FOR i IN tcToRetry.FIRST .. tcToRetry.LAST  LOOP
    
      -- tapecopy => TOBERECALLED
      -- segment => UNPROCESSED
      -- tape => PENDIN if UNUSED OR FAILED with still segments unprocessed		
      UPDATE TapeCopy SET status=4, errorcode=0, nbretry= nbretry+1 WHERE id=tcToRetry(i);
      UPDATE Segment SET status=0  WHERE copy=tcToRetry(i) RETURNING id BULK COLLECT INTO segIds;
      UPDATE tape SET status=8 WHERE status IN (0,6) AND id IN (SELECT tape FROM segment WHERE id  IN 
      (SELECT /*+ CARDINALITY(segTable 5) */ * FROM TABLE(segIds) segTable ));  
    END LOOP;
  END IF;
  
  -- I mark as failed the hopeless recall
  -- check because oracle cannot handle empty buffer
  IF tcToFail(1) != -1 THEN
    FOR i IN tcToFail.FIRST .. tcToFail.LAST  LOOP
        -- clean up diskcopy and subrequest	
      SELECT castorFile INTO cfId FROM TapeCopy,CastorFile
        WHERE TapeCopy.id = tcToFail(i) AND castorfile.id = tapecopy.castorfile FOR UPDATE OF castorfile.id;
      UPDATE DiskCopy SET status = 4 -- DISKCOPY_FAILED
        WHERE castorFile = cfId AND status = 2; -- DISKCOPY_WAITTAPERECALL   
      -- Drop tape copies. Ideally, we should keep some track that
      -- the recall failed in order to prevent future recalls until some
      -- sort of manual intervention. For the time being, as we can't
      -- say whether the failure is fatal or not, we drop everything
      -- and we won't deny a future request for recall.
      deleteTapeCopies(cfId);
      UPDATE SubRequest 
       SET status = 7, -- SUBREQUEST_FAILED
           getNextStatus = 1, -- GETNEXTSTATUS_FILESTAGED (not strictly correct but the request is over anyway)
           lastModificationTime = getTime(),
           errorCode = 1015,  -- SEINTERNAL
           errorMessage = 'File recall from tape has failed, please try again later',
           parent = 0
      WHERE castorFile = cfId AND status IN (4, 5); -- WAITTAPERECALL, WAITSUBREQ
    END LOOP;
  END IF;
  COMMIT;
END;
/

create or replace PROCEDURE  tg_startTapeSession
( inputVdqmReqId IN NUMBER, outVid OUT NOCOPY VARCHAR2, outMode OUT INTEGER, ret OUT INTEGER, outDensity OUT NOCOPY VARCHAR2, outLabel OUT NOCOPY VARCHAR2 ) AS
reqId NUMBER;
tpId NUMBER;
segToDo INTEGER;
tcToDo INTEGER;
BEGIN
  ret:=-2;
  -- set the request to ONGOING
  UPDATE tapegatewayrequest SET status=3 WHERE vdqmVolreqid = inputVdqmReqId  AND status =2 RETURNING id, accessmode INTO reqId, outMode;
  IF reqId IS NULL THEN
    ret:=-2; -- UNKNOWN request
  ELSE  
    IF outMode = 0 THEN
      -- read tape mounted
      SELECT count(*) INTO segToDo FROM segment WHERE status=0 AND tape = (SELECT taperecall FROM tapegatewayrequest WHERE id=reqId); 
      IF segToDo = 0 THEN
        UPDATE tapegatewayrequest set lastvdqmpingtime=0 WHERE id=reqId; -- to force the cleanup
        ret:=-1; --NO MORE FILES
      ELSE
        UPDATE tape set status=4 WHERE id = ( SELECT taperecall FROM tapegatewayrequest WHERE id=reqId ) and tpmode=0  
            RETURNING vid, label, density INTO outVid, outLabel, outDensity; -- tape is MOUNTED 
        ret:=0;
      END IF;
    ELSE 
      -- write
      SELECT COUNT(*) INTO tcToDo FROM stream2tapecopy WHERE parent IN ( SELECT streammigration FROM tapegatewayrequest WHERE id=reqId );
      IF tcToDo = 0 THEN
        UPDATE tapegatewayrequest set lastvdqmpingtime=0 WHERE id=reqId; -- to force the cleanup
        ret:=-1; --NO MORE FILES
        outVid:=NULL;
      ELSE
        UPDATE stream set status=3 WHERE id = ( SELECT streammigration FROM tapegatewayrequest WHERE id=reqId ) 
            RETURNING tape INTO tpId; -- RUNNING
        UPDATE tape set status=4  WHERE id = tpId 
            RETURNING vid, label, density INTO outVid, outLabel, outDensity; -- MOUNTED
        ret:=0;
      END IF;    
    END IF; 
  END IF;
  COMMIT;
END;
/


