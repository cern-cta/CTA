/*******************************************************************
 *
 * @(#)$RCSfile: oracleDebug.sql,v $ $Revision: 1.14 $ $Date: 2009/08/13 13:34:16 $ $Author: itglp $
 *
 * Some SQL code to ease support and debugging
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *******************************************************************/

/* PL/SQL declaration for the castorDebug package */
CREATE OR REPLACE PACKAGE castorDebug AS
  TYPE DiskCopyDebug_typ IS RECORD (
    id INTEGER,
    diskPool VARCHAR2(2048),
    location VARCHAR2(2048),
    available CHAR(1),
    status NUMBER,
    creationtime VARCHAR2(2048),
    diskCopySize NUMBER,
    castorFileSize NUMBER,
    gcWeight NUMBER);
  TYPE DiskCopyDebug IS TABLE OF DiskCopyDebug_typ;
  TYPE SubRequestDebug IS TABLE OF SubRequest%ROWTYPE;
  TYPE MigrationJobDebug IS TABLE OF MigrationJob%ROWTYPE;
  TYPE RequestDebug_typ IS RECORD (
    creationtime VARCHAR2(2048),
    SubReqId NUMBER,
    SubReqParentId NUMBER,
    Status NUMBER,
    username VARCHAR2(2048),
    machine VARCHAR2(2048),
    svcClassName VARCHAR2(2048),
    ReqId NUMBER,
    ReqType VARCHAR2(20));
  TYPE RequestDebug IS TABLE OF RequestDebug_typ;
  TYPE RecallJobDebug_typ IS RECORD (
    RJId NUMBER,
    RJStatus NUMBER,
    RJMissing NUMBER,
    RJNbRetry NUMBER,
    SegId NUMBER,
    SegStatus NUMBER,
    SegErrCode NUMBER,
    VID VARCHAR2(2048),
    tpMode NUMBER,
    TapeStatus NUMBER,
    SegErr VARCHAR2(2048));
  TYPE RecallJobDebug IS TABLE OF RecallJobDebug_typ;
END;
/


/* Return the castor file id associated with the reference number */
CREATE OR REPLACE FUNCTION getCF(ref NUMBER) RETURN NUMBER AS
  t NUMBER;
  cfId NUMBER;
BEGIN
  SELECT id INTO cfId FROM CastorFile WHERE id = ref OR fileId = ref;
  RETURN cfId;
EXCEPTION WHEN NO_DATA_FOUND THEN -- DiskCopy?
BEGIN
  SELECT castorFile INTO cfId FROM DiskCopy WHERE id = ref;
  RETURN cfId;
EXCEPTION WHEN NO_DATA_FOUND THEN -- SubRequest?
BEGIN
  SELECT castorFile INTO cfId FROM SubRequest WHERE id = ref;
  RETURN cfId;
EXCEPTION WHEN NO_DATA_FOUND THEN -- RecallJob?
BEGIN
  SELECT castorFile INTO cfId FROM RecallJob WHERE id = ref;
  RETURN cfId;
EXCEPTION WHEN NO_DATA_FOUND THEN -- MigrationJob?
BEGIN
  SELECT castorFile INTO cfId FROM MigrationJob WHERE id = ref;
  RETURN cfId;
EXCEPTION WHEN NO_DATA_FOUND THEN -- Segment?
BEGIN
  SELECT castorFile INTO cfId FROM RecallJob, Segment
   WHERE Segment.id = ref AND RecallJob.id = Segment.copy;
  RETURN cfId;
EXCEPTION WHEN NO_DATA_FOUND THEN -- nothing found
  RAISE_APPLICATION_ERROR (-20000, 'Could not find any CastorFile, SubRequest, DiskCopy, MigrationJob, RecallJob or Segment with id = ' || ref);
END; END; END; END; END; END;
/


/* Get the diskcopys associated with the reference number */
CREATE OR REPLACE FUNCTION getDCs(ref number) RETURN castorDebug.DiskCopyDebug PIPELINED AS
BEGIN
  FOR d IN (SELECT DiskCopy.id,
                   DiskPool.name AS diskpool,
                   DiskServer.name || ':' || FileSystem.mountPoint || DiskCopy.path AS location,
                   decode(DiskServer.status, 2, 'N', decode(FileSystem.status, 2, 'N', 'Y')) AS available,
                   DiskCopy.status AS status,
                   getTimeString(DiskCopy.creationtime) AS creationtime,
                   DiskCopy.diskCopySize AS diskcopysize,
                   CastorFile.fileSize AS castorfilesize,
                   trunc(DiskCopy.gcWeight, 2) AS gcweight
              FROM DiskCopy, FileSystem, DiskServer, DiskPool, CastorFile
             WHERE DiskCopy.fileSystem = FileSystem.id(+)
               AND FileSystem.diskServer = diskServer.id(+)
               AND DiskPool.id(+) = fileSystem.diskPool
               AND DiskCopy.castorFile = getCF(ref)
               AND DiskCopy.castorFile = CastorFile.id) LOOP
     PIPE ROW(d);
  END LOOP;
END;
/


/* Get the recalljobs, segments and tapes associated with the reference number */
CREATE OR REPLACE FUNCTION getRJs(ref number) RETURN castorDebug.RecallJobDebug PIPELINED AS
BEGIN
  FOR t IN (SELECT RecallJob.id AS RJId, RecallJob.status AS RJStatus,
                   RecallJob.missingCopies AS RJmissing, RecallJob.nbRetry AS RJNbRetry,
                   Segment.Id, Segment.status AS SegStatus, Segment.errorCode AS SegErrCode,
                   Tape.vid AS VID, Tape.tpMode AS tpMode, Tape.Status AS TapeStatus,
                   Segment.errMsgTxt AS SegErr
              FROM RecallJob, Segment, Tape
             WHERE RecallJob.id = Segment.copy(+)
               AND Segment.tape = Tape.id(+)
               AND RecallJob.castorfile = getCF(ref)) LOOP
     PIPE ROW(t);
  END LOOP;
END;
/


/* Get the migration jobs associated with the reference number */
CREATE OR REPLACE FUNCTION getMJs(ref number) RETURN castorDebug.MigrationJobDebug PIPELINED AS
BEGIN
  FOR t IN (SELECT *
              FROM MigrationJob
             WHERE castorfile = getCF(ref)) LOOP
     PIPE ROW(t);
  END LOOP;
END;
/


/* Get the subrequests associated with the reference number. */
CREATE OR REPLACE FUNCTION getSRs(ref number) RETURN castorDebug.SubRequestDebug PIPELINED AS
BEGIN
  FOR d IN (SELECT * FROM SubRequest WHERE castorfile = getCF(ref)) LOOP
     PIPE ROW(d);
  END LOOP;
END;
/


/* Get the requests associated with the reference number. */
CREATE OR REPLACE FUNCTION getRs(ref number) RETURN castorDebug.RequestDebug PIPELINED AS
BEGIN
  FOR d IN (SELECT getTimeString(creationtime) AS creationtime,
                   SubRequest.id AS SubReqId, SubRequest.parent AS SubReqParentId, SubRequest.Status,
                   username, machine, svcClassName, Request.id AS ReqId, Request.type AS ReqType
              FROM SubRequest,
                    (SELECT /*+ INDEX(StageGetRequest PK_StageGetRequest_Id) */ id, username, machine, svcClassName, 'Get' AS type FROM StageGetRequest UNION ALL
                     SELECT /*+ INDEX(StagePrepareToGetRequest PK_StagePrepareToGetRequest_Id) */ id, username, machine, svcClassName, 'PGet' AS type FROM StagePrepareToGetRequest UNION ALL
                     SELECT /*+ INDEX(StagePutRequest PK_StagePutRequest_Id) */ id, username, machine, svcClassName, 'Put' AS type FROM StagePutRequest UNION ALL
                     SELECT /*+ INDEX(StagePrepareToPutRequest PK_StagePrepareToPutRequest_Id) */ id, username, machine, svcClassName, 'PPut' AS type FROM StagePrepareToPutRequest UNION ALL
                     SELECT /*+ INDEX(StageUpdateRequest PK_StageUpdateRequest_Id) */ id, username, machine, svcClassName, 'Upd' AS type FROM StageUpdateRequest UNION ALL
                     SELECT /*+ INDEX(StagePrepareToUpdateRequest PK_StagePrepareToUpdateRequ_Id) */ id, username, machine, svcClassName, 'PUpd' AS type FROM StagePrepareToUpdateRequest UNION ALL
                     SELECT /*+ INDEX(StageRepackRequest PK_StageRepackRequest_Id) */ id, username, machine, svcClassName, 'Repack' AS type FROM StageRepackRequest UNION ALL
                     SELECT /*+ INDEX(StagePutDoneRequest PK_StagePutDoneRequest_Id) */ id, username, machine, svcClassName, 'PutDone' AS type FROM StagePutDoneRequest UNION ALL
                     SELECT /*+ INDEX(StageDiskCopyReplicaRequest PK_StageDiskCopyReplicaRequ_Id) */ id, username, machine, svcClassName, 'DCRepl' AS type FROM StageDiskCopyReplicaRequest UNION ALL
                     SELECT /*+ INDEX(SetFileGCWeight PK_SetFileGCWeight_Id) */ id, username, machine, svcClassName, 'SetGCW' AS type FROM SetFileGCWeight) Request
             WHERE castorfile = getCF(ref)
               AND Request.id = SubRequest.request) LOOP
     PIPE ROW(d);
  END LOOP;
END;
/
