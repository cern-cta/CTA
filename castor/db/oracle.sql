/* SQL statements for object types */
DROP INDEX I_RH_ID2TYPE_FULL;
DROP TABLE RH_ID2TYPE;
CREATE TABLE RH_ID2TYPE (id INTEGER PRIMARY KEY, type NUMBER);
CREATE INDEX I_RH_ID2TYPE_FULL on RH_ID2TYPE (id, type);

/* SQL statements for indices */
DROP INDEX I_RH_INDICES_FULL;
DROP TABLE RH_INDICES;
CREATE TABLE RH_INDICES (name CHAR(8), value NUMBER);
CREATE INDEX I_RH_INDICES_FULL on RH_INDICES (name, value);
INSERT INTO RH_INDICES (name, value) VALUES ('next_id', 1);

/* SQL statements for requests status */
DROP INDEX I_RH_REQUESTSSTATUS_FULL;
DROP TABLE RH_REQUESTSSTATUS;
CREATE TABLE RH_REQUESTSSTATUS (id INTEGER PRIMARY KEY, status CHAR(8), creation DATE, lastChange DATE);
CREATE INDEX I_RH_REQUESTSSTATUS_FULL on RH_REQUESTSSTATUS (id, status);

/* PL/SQL procedure for getting the next request to handle */
CREATE OR REPLACE PROCEDURE getNRStatement(reqid OUT INTEGER) AS
BEGIN
  SELECT ID INTO reqid FROM rh_requestsStatus WHERE status = 'NEW' AND rownum <=1;
  UPDATE rh_requestsStatus SET status = 'RUNNING', lastChange = SYSDATE WHERE ID = reqid;
END;
/* SQL statements for type Client */
DROP TABLE rh_Client;
CREATE TABLE rh_Client (ipAddress NUMBER, port NUMBER, id INTEGER PRIMARY KEY, request INTEGER);

/* SQL statements for type StageInRequest */
DROP TABLE rh_StageInRequest;
CREATE TABLE rh_StageInRequest (flags INTEGER, userName VARCHAR(255), euid NUMBER, egid NUMBER, mask NUMBER, pid NUMBER, machine VARCHAR(255), svcClassName VARCHAR(255), openflags NUMBER, id INTEGER PRIMARY KEY, svcClass INTEGER, client INTEGER);

/* SQL statements for type StageQryRequest */
DROP TABLE rh_StageQryRequest;
CREATE TABLE rh_StageQryRequest (flags INTEGER, userName VARCHAR(255), euid NUMBER, egid NUMBER, mask NUMBER, pid NUMBER, machine VARCHAR(255), svcClassName VARCHAR(255), id INTEGER PRIMARY KEY, svcClass INTEGER, client INTEGER);

/* SQL statements for type StageOutRequest */
DROP TABLE rh_StageOutRequest;
CREATE TABLE rh_StageOutRequest (flags INTEGER, userName VARCHAR(255), euid NUMBER, egid NUMBER, mask NUMBER, pid NUMBER, machine VARCHAR(255), svcClassName VARCHAR(255), openmode NUMBER, id INTEGER PRIMARY KEY, svcClass INTEGER, client INTEGER);

/* SQL statements for type StageClrRequest */
DROP TABLE rh_StageClrRequest;
CREATE TABLE rh_StageClrRequest (flags INTEGER, userName VARCHAR(255), euid NUMBER, egid NUMBER, mask NUMBER, pid NUMBER, machine VARCHAR(255), svcClassName VARCHAR(255), id INTEGER PRIMARY KEY, svcClass INTEGER, client INTEGER);

/* SQL statements for type StageFilChgRequest */
DROP TABLE rh_StageFilChgRequest;
CREATE TABLE rh_StageFilChgRequest (flags INTEGER, userName VARCHAR(255), euid NUMBER, egid NUMBER, mask NUMBER, pid NUMBER, machine VARCHAR(255), svcClassName VARCHAR(255), id INTEGER PRIMARY KEY, svcClass INTEGER, client INTEGER);

/* SQL statements for type ReqId */
DROP TABLE rh_ReqId;
CREATE TABLE rh_ReqId (value VARCHAR(255), id INTEGER PRIMARY KEY, request INTEGER);

/* SQL statements for type SubRequest */
DROP TABLE rh_SubRequest;
CREATE TABLE rh_SubRequest (retryCounter NUMBER, fileName VARCHAR(255), protocol VARCHAR(255), poolName VARCHAR(255), xsize INTEGER, priority NUMBER, id INTEGER PRIMARY KEY, diskcopy INTEGER, castorFile INTEGER, parent INTEGER, request INTEGER, status INTEGER);

/* SQL statements for type StageUpdcRequest */
DROP TABLE rh_StageUpdcRequest;
CREATE TABLE rh_StageUpdcRequest (flags INTEGER, userName VARCHAR(255), euid NUMBER, egid NUMBER, mask NUMBER, pid NUMBER, machine VARCHAR(255), svcClassName VARCHAR(255), id INTEGER PRIMARY KEY, svcClass INTEGER, client INTEGER);

/* SQL statements for type Tape */
DROP TABLE rh_Tape;
CREATE TABLE rh_Tape (vid VARCHAR(255), side NUMBER, tpmode NUMBER, errMsgTxt VARCHAR(255), errorCode NUMBER, severity NUMBER, vwAddress VARCHAR(255), id INTEGER PRIMARY KEY, stream INTEGER, status INTEGER);

/* SQL statements for type Segment */
DROP TABLE rh_Segment;
CREATE TABLE rh_Segment (blockid CHAR(4), fseq NUMBER, offset INTEGER, bytes_in INTEGER, bytes_out INTEGER, host_bytes INTEGER, segmCksumAlgorithm VARCHAR(255), segmCksum NUMBER, errMsgTxt VARCHAR(255), errorCode NUMBER, severity NUMBER, id INTEGER PRIMARY KEY, tape INTEGER, copy INTEGER, status INTEGER);

/* SQL statements for type TapePool */
DROP TABLE rh_TapePool;
CREATE TABLE rh_TapePool (name VARCHAR(255), id INTEGER PRIMARY KEY);

/* SQL statements for type TapeCopy */
DROP TABLE rh_TapeCopy;
CREATE TABLE rh_TapeCopy (id INTEGER PRIMARY KEY, castorFile INTEGER, status INTEGER);

/* SQL statements for type CastorFile */
DROP TABLE rh_CastorFile;
CREATE TABLE rh_CastorFile (fileId INTEGER, nsHost VARCHAR(255), fileSize INTEGER, id INTEGER PRIMARY KEY, svcClass INTEGER, fileClass INTEGER);

/* SQL statements for type DiskCopy */
DROP TABLE rh_DiskCopy;
CREATE TABLE rh_DiskCopy (path VARCHAR(255), id INTEGER PRIMARY KEY, fileSystem INTEGER, castorFile INTEGER, status INTEGER);

/* SQL statements for type FileSystem */
DROP TABLE rh_FileSystem;
CREATE TABLE rh_FileSystem (free INTEGER, weight float, fsDeviation float, mountPoint VARCHAR(255), id INTEGER PRIMARY KEY, diskPool INTEGER, diskserver INTEGER);

/* SQL statements for type SvcClass */
DROP TABLE rh_SvcClass;
CREATE TABLE rh_SvcClass (policy VARCHAR(255), nbDrives NUMBER, name VARCHAR(255), id INTEGER PRIMARY KEY);
DROP TABLE rh_SvcClass2TapePool;
CREATE TABLE rh_SvcClass2TapePool (Parent INTEGER, Child INTEGER);

/* SQL statements for type DiskPool */
DROP TABLE rh_DiskPool;
CREATE TABLE rh_DiskPool (name VARCHAR(255), id INTEGER PRIMARY KEY);
DROP TABLE rh_DiskPool2SvcClass;
CREATE TABLE rh_DiskPool2SvcClass (Parent INTEGER, Child INTEGER);

/* SQL statements for type Stream */
DROP TABLE rh_Stream;
CREATE TABLE rh_Stream (initialSizeToTransfer INTEGER, id INTEGER PRIMARY KEY, tape INTEGER, tapePool INTEGER, status INTEGER);
DROP TABLE rh_Stream2TapeCopy;
CREATE TABLE rh_Stream2TapeCopy (Parent INTEGER, Child INTEGER);

/* SQL statements for type FileClass */
DROP TABLE rh_FileClass;
CREATE TABLE rh_FileClass (name VARCHAR(255), minFileSize NUMBER, maxFileSize NUMBER, nbCopies NUMBER, id INTEGER PRIMARY KEY);

/* SQL statements for type DiskServer */
DROP TABLE rh_DiskServer;
CREATE TABLE rh_DiskServer (name VARCHAR(255), id INTEGER PRIMARY KEY, status INTEGER);

