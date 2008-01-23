/*******************************************************************
 *
 * @(#)$RCSfile: oracleDebug.sql,v $ $Revision: 1.4 $ $Date: 2008/01/23 11:36:01 $ $Author: itglp $
 *
 * Some SQL code to ease support and debugging
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *******************************************************************/

CREATE OR REPLACE PACKAGE castor_debug AS
  TYPE DiskCopyDebug_typ IS RECORD (
    id INTEGER,
    diskPool VARCHAR2(2048),
    location VARCHAR2(2048),
    status NUMBER,
    creationtime DATE);
  TYPE DiskCopyDebug IS TABLE OF DiskCopyDebug_typ;
  TYPE SubRequestDebug IS TABLE OF SubRequest%ROWTYPE;
  TYPE RequestDebug_typ IS RECORD (
    creationtime DATE,
    SubReqId NUMBER,
    SubReqParentId NUMBER,
    Status NUMBER,
    username VARCHAR2(2048),
    machine VARCHAR2(2048),
    svcClassName VARCHAR2(2048),
    ReqId NUMBER,
    ReqType VARCHAR2(20));
  TYPE RequestDebug IS TABLE OF RequestDebug_typ;
  TYPE TapeCopyDebug_typ IS RECORD (
    TCId NUMBER,
    TCStatus NUMBER,
    SegId NUMBER,
    SegStatus NUMBER,
    SegErrCode NUMBER,
    VID VARCHAR2(2048),
    tpMode NUMBER,
    TapeStatus NUMBER,
    nbStreams NUMBER,
    SegErr VARCHAR2(2048));
  TYPE TapeCopyDebug IS TABLE OF TapeCopyDebug_typ;
END;


/* Return the castor file id associated with the reference number */
CREATE OR REPLACE FUNCTION getCF(ref NUMBER) RETURN NUMBER AS
  t NUMBER;
  cfId NUMBER;
BEGIN
  SELECT type INTO t FROM id2Type WHERE id = ref;
  IF (t = 2) THEN -- CASTORFILE
    RETURN ref;
  ELSIF (t = 5) THEN -- DiskCopy
    SELECT castorFile INTO cfId FROM DiskCopy WHERE id = ref;
  ELSIF (t = 27) THEN -- SubRequest
    SELECT castorFile INTO cfId FROM SubRequest WHERE id = ref;
  ELSIF (t = 30) THEN -- TapeCopy
    SELECT castorFile INTO cfId FROM TapeCopy WHERE id = ref;
  ELSIF (t = 18) THEN -- Segment
    SELECT castorFile INTO cfId FROM TapeCopy, Segment WHERE Segment.id = ref AND TapeCopy.id = Segment.copy;
  END IF;
  RETURN cfId;
EXCEPTION WHEN NO_DATA_FOUND THEN -- fileid ?
  SELECT id INTO cfId FROM CastorFile WHERE fileId = ref;
  RETURN cfId;
END;


/* Get the diskcopys associated with the reference number */
CREATE OR REPLACE FUNCTION getDCs(ref number) RETURN castor_debug.DiskCopyDebug PIPELINED AS
BEGIN
  FOR d IN (SELECT diskCopy.id,
                   diskPool.name as diskpool,
                   diskServer.name || ':' || fileSystem.mountPoint || diskCopy.path as location,
                   diskCopy.status as status,
                   to_date('01011970','ddmmyyyy') + 1/24/60/60 * creationtime as creationtime
              FROM DiskCopy, FileSystem, DiskServer, DiskPool
             WHERE DiskCopy.fileSystem = FileSystem.id(+)
               AND FileSystem.diskServer = diskServer.id(+)
               AND DiskPool.id(+) = fileSystem.diskPool
               AND DiskCopy.castorfile = getCF(ref)) LOOP
     PIPE ROW(d);
  END LOOP;
END;


/* Get the tapecopys, segments and streams associated with the reference number */
CREATE OR REPLACE FUNCTION getTCs(ref number) RETURN castor_debug.TapeCopyDebug PIPELINED AS
BEGIN
  FOR t IN (SELECT TapeCopy.id as TCId, TapeCopy.status as TCStatus,
                   Segment.Id, Segment.status as SegStatus, Segment.errorCode as SegErrCode,
                   Tape.vid as VID, Tape.tpMode as tpMode, Tape.Status as TapeStatus,
                   count(*) as nbStreams, Segment.errMsgTxt as SegErr
              FROM TapeCopy, Segment, Tape, Stream2TapeCopy
             WHERE TapeCopy.id = Segment.copy(+)
               AND Segment.tape = Tape.id(+)
               AND TapeCopy.castorfile = getCF(ref)
               AND Stream2TapeCopy.child = TapeCopy.id
              GROUP BY TapeCopy.id, TapeCopy.status, Segment.id, Segment.status, Segment.errorCode,
                       Tape.vid, Tape.tpMode, Tape.Status, Segment.errMsgTxt) LOOP
     PIPE ROW(t);
  END LOOP;
END;


/* Get the subrequests associated with the reference number. (By castorfile/diskcopy/
 * subrequest/tapecopy or fileid
 */
CREATE OR REPLACE FUNCTION getSRs(ref number) RETURN castor_debug.SubRequestDebug PIPELINED AS
BEGIN
  FOR d IN (SELECT * FROM SubRequest WHERE castorfile = getCF(ref)) LOOP
     PIPE ROW(d);
  END LOOP;
END;


/* Get the requests associated with the reference number. (By castorfile/diskcopy/
 * subrequest/tapecopy or fileid
 */
CREATE OR REPLACE FUNCTION getRs(ref number) RETURN castor_debug.RequestDebug PIPELINED AS
BEGIN
  FOR d IN (SELECT to_date('01011970','ddmmyyyy') + 1/24/60/60 * creationtime as creationtime,
                   SubRequest.id as SubReqId, SubRequest.parent as SubReqParentId, SubRequest.Status,
                   username, machine, svcClassName, Request.id as ReqId, Request.type as ReqType
              FROM SubRequest,
                    (SELECT id, username, machine, svcClassName, 'Get' as type from StageGetRequest UNION ALL
                     SELECT id, username, machine, svcClassName, 'PGet' as type from StagePrepareToGetRequest UNION ALL
                     SELECT id, username, machine, svcClassName, 'Put' as type from StagePutRequest UNION ALL
                     SELECT id, username, machine, svcClassName, 'PPut' as type from StagePrepareToPutRequest UNION ALL
                     SELECT id, username, machine, svcClassName, 'Upd' as type from StageUpdateRequest UNION ALL
                     SELECT id, username, machine, svcClassName, 'PUpd' as type from StagePrepareToUpdateRequest UNION ALL
                     SELECT id, username, machine, svcClassName, 'Repack' as type from StageRepackRequest UNION ALL
                     SELECT id, username, machine, svcClassName, 'GetNext' as type from StageGetNextRequest UNION ALL
                     SELECT id, username, machine, svcClassName, 'UpdNext' as type from StageUpdateNextRequest UNION ALL
                     SELECT id, username, machine, svcClassName, 'PutDone' as type from StagePutDoneRequest UNION ALL
                     SELECT id, username, machine, svcClassName, 'DCRepl' as type from StageDiskCopyReplicaRequest) Request
             WHERE castorfile = getCF(ref)
               AND Request.id = SubRequest.request) LOOP
     PIPE ROW(d);
  END LOOP;
END;
