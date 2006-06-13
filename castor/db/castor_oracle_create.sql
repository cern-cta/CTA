/* SQL statements for type BaseAddress */
CREATE TABLE BaseAddress (objType NUMBER, cnvSvcName VARCHAR2(2048), cnvSvcType NUMBER, target INTEGER, id INTEGER PRIMARY KEY) INITRANS 50 PCTFREE 50;

/* SQL statements for type Client */
CREATE TABLE Client (ipAddress NUMBER, port NUMBER, id INTEGER PRIMARY KEY) INITRANS 50 PCTFREE 50;

/* SQL statements for type ClientIdentification */
CREATE TABLE ClientIdentification (machine VARCHAR2(2048), userName VARCHAR2(2048), port NUMBER, euid NUMBER, egid NUMBER, magic NUMBER, id INTEGER PRIMARY KEY) INITRANS 50 PCTFREE 50;

/* SQL statements for type Disk2DiskCopyDoneRequest */
CREATE TABLE Disk2DiskCopyDoneRequest (flags INTEGER, userName VARCHAR2(2048), euid NUMBER, egid NUMBER, mask NUMBER, pid NUMBER, machine VARCHAR2(2048), svcClassName VARCHAR2(2048), userTag VARCHAR2(2048), reqId VARCHAR2(2048), creationTime INTEGER, lastModificationTime INTEGER, diskCopyId INTEGER, status NUMBER, id INTEGER PRIMARY KEY, svcClass INTEGER, client INTEGER) INITRANS 50 PCTFREE 50;

/* SQL statements for type GetUpdateDone */
CREATE TABLE GetUpdateDone (flags INTEGER, userName VARCHAR2(2048), euid NUMBER, egid NUMBER, mask NUMBER, pid NUMBER, machine VARCHAR2(2048), svcClassName VARCHAR2(2048), userTag VARCHAR2(2048), reqId VARCHAR2(2048), creationTime INTEGER, lastModificationTime INTEGER, subReqId INTEGER, id INTEGER PRIMARY KEY, svcClass INTEGER, client INTEGER) INITRANS 50 PCTFREE 50;

/* SQL statements for type GetUpdateFailed */
CREATE TABLE GetUpdateFailed (flags INTEGER, userName VARCHAR2(2048), euid NUMBER, egid NUMBER, mask NUMBER, pid NUMBER, machine VARCHAR2(2048), svcClassName VARCHAR2(2048), userTag VARCHAR2(2048), reqId VARCHAR2(2048), creationTime INTEGER, lastModificationTime INTEGER, subReqId INTEGER, id INTEGER PRIMARY KEY, svcClass INTEGER, client INTEGER) INITRANS 50 PCTFREE 50;

/* SQL statements for type PutFailed */
CREATE TABLE PutFailed (flags INTEGER, userName VARCHAR2(2048), euid NUMBER, egid NUMBER, mask NUMBER, pid NUMBER, machine VARCHAR2(2048), svcClassName VARCHAR2(2048), userTag VARCHAR2(2048), reqId VARCHAR2(2048), creationTime INTEGER, lastModificationTime INTEGER, subReqId INTEGER, id INTEGER PRIMARY KEY, svcClass INTEGER, client INTEGER) INITRANS 50 PCTFREE 50;

/* SQL statements for type Files2Delete */
CREATE TABLE Files2Delete (flags INTEGER, userName VARCHAR2(2048), euid NUMBER, egid NUMBER, mask NUMBER, pid NUMBER, machine VARCHAR2(2048), svcClassName VARCHAR2(2048), userTag VARCHAR2(2048), reqId VARCHAR2(2048), creationTime INTEGER, lastModificationTime INTEGER, diskServer VARCHAR2(2048), id INTEGER PRIMARY KEY, svcClass INTEGER, client INTEGER) INITRANS 50 PCTFREE 50;

/* SQL statements for type FilesDeleted */
CREATE TABLE FilesDeleted (flags INTEGER, userName VARCHAR2(2048), euid NUMBER, egid NUMBER, mask NUMBER, pid NUMBER, machine VARCHAR2(2048), svcClassName VARCHAR2(2048), userTag VARCHAR2(2048), reqId VARCHAR2(2048), creationTime INTEGER, lastModificationTime INTEGER, id INTEGER PRIMARY KEY, svcClass INTEGER, client INTEGER) INITRANS 50 PCTFREE 50;

/* SQL statements for type FilesDeletionFailed */
CREATE TABLE FilesDeletionFailed (flags INTEGER, userName VARCHAR2(2048), euid NUMBER, egid NUMBER, mask NUMBER, pid NUMBER, machine VARCHAR2(2048), svcClassName VARCHAR2(2048), userTag VARCHAR2(2048), reqId VARCHAR2(2048), creationTime INTEGER, lastModificationTime INTEGER, id INTEGER PRIMARY KEY, svcClass INTEGER, client INTEGER) INITRANS 50 PCTFREE 50;

/* SQL statements for type GCFile */
CREATE TABLE GCFile (diskCopyId INTEGER, id INTEGER PRIMARY KEY, request INTEGER) INITRANS 50 PCTFREE 50;

/* SQL statements for type GCLocalFile */
CREATE TABLE GCLocalFile (fileName VARCHAR2(2048), diskCopyId INTEGER, id INTEGER PRIMARY KEY) INITRANS 50 PCTFREE 50;

/* SQL statements for type MoverCloseRequest */
CREATE TABLE MoverCloseRequest (flags INTEGER, userName VARCHAR2(2048), euid NUMBER, egid NUMBER, mask NUMBER, pid NUMBER, machine VARCHAR2(2048), svcClassName VARCHAR2(2048), userTag VARCHAR2(2048), reqId VARCHAR2(2048), creationTime INTEGER, lastModificationTime INTEGER, subReqId INTEGER, fileSize INTEGER, id INTEGER PRIMARY KEY, svcClass INTEGER, client INTEGER) INITRANS 50 PCTFREE 50;

/* SQL statements for type PutStartRequest */
CREATE TABLE PutStartRequest (subreqId INTEGER, diskServer VARCHAR2(2048), fileSystem VARCHAR2(2048), flags INTEGER, userName VARCHAR2(2048), euid NUMBER, egid NUMBER, mask NUMBER, pid NUMBER, machine VARCHAR2(2048), svcClassName VARCHAR2(2048), userTag VARCHAR2(2048), reqId VARCHAR2(2048), creationTime INTEGER, lastModificationTime INTEGER, id INTEGER PRIMARY KEY, svcClass INTEGER, client INTEGER) INITRANS 50 PCTFREE 50;

/* SQL statements for type PutDoneStart */
CREATE TABLE PutDoneStart (subreqId INTEGER, diskServer VARCHAR2(2048), fileSystem VARCHAR2(2048), flags INTEGER, userName VARCHAR2(2048), euid NUMBER, egid NUMBER, mask NUMBER, pid NUMBER, machine VARCHAR2(2048), svcClassName VARCHAR2(2048), userTag VARCHAR2(2048), reqId VARCHAR2(2048), creationTime INTEGER, lastModificationTime INTEGER, id INTEGER PRIMARY KEY, svcClass INTEGER, client INTEGER) INITRANS 50 PCTFREE 50;

/* SQL statements for type GetUpdateStartRequest */
CREATE TABLE GetUpdateStartRequest (subreqId INTEGER, diskServer VARCHAR2(2048), fileSystem VARCHAR2(2048), flags INTEGER, userName VARCHAR2(2048), euid NUMBER, egid NUMBER, mask NUMBER, pid NUMBER, machine VARCHAR2(2048), svcClassName VARCHAR2(2048), userTag VARCHAR2(2048), reqId VARCHAR2(2048), creationTime INTEGER, lastModificationTime INTEGER, id INTEGER PRIMARY KEY, svcClass INTEGER, client INTEGER) INITRANS 50 PCTFREE 50;

/* SQL statements for type QueryParameter */
CREATE TABLE QueryParameter (value VARCHAR2(2048), id INTEGER PRIMARY KEY, query INTEGER, queryType INTEGER) INITRANS 50 PCTFREE 50;

/* SQL statements for type StagePrepareToGetRequest */
CREATE TABLE StagePrepareToGetRequest (flags INTEGER, userName VARCHAR2(2048), euid NUMBER, egid NUMBER, mask NUMBER, pid NUMBER, machine VARCHAR2(2048), svcClassName VARCHAR2(2048), userTag VARCHAR2(2048), reqId VARCHAR2(2048), creationTime INTEGER, lastModificationTime INTEGER, id INTEGER PRIMARY KEY, svcClass INTEGER, client INTEGER) INITRANS 50 PCTFREE 50;

/* SQL statements for type StagePrepareToPutRequest */
CREATE TABLE StagePrepareToPutRequest (flags INTEGER, userName VARCHAR2(2048), euid NUMBER, egid NUMBER, mask NUMBER, pid NUMBER, machine VARCHAR2(2048), svcClassName VARCHAR2(2048), userTag VARCHAR2(2048), reqId VARCHAR2(2048), creationTime INTEGER, lastModificationTime INTEGER, id INTEGER PRIMARY KEY, svcClass INTEGER, client INTEGER) INITRANS 50 PCTFREE 50;

/* SQL statements for type StagePrepareToUpdateRequest */
CREATE TABLE StagePrepareToUpdateRequest (flags INTEGER, userName VARCHAR2(2048), euid NUMBER, egid NUMBER, mask NUMBER, pid NUMBER, machine VARCHAR2(2048), svcClassName VARCHAR2(2048), userTag VARCHAR2(2048), reqId VARCHAR2(2048), creationTime INTEGER, lastModificationTime INTEGER, id INTEGER PRIMARY KEY, svcClass INTEGER, client INTEGER) INITRANS 50 PCTFREE 50;

/* SQL statements for type StageGetRequest */
CREATE TABLE StageGetRequest (flags INTEGER, userName VARCHAR2(2048), euid NUMBER, egid NUMBER, mask NUMBER, pid NUMBER, machine VARCHAR2(2048), svcClassName VARCHAR2(2048), userTag VARCHAR2(2048), reqId VARCHAR2(2048), creationTime INTEGER, lastModificationTime INTEGER, id INTEGER PRIMARY KEY, svcClass INTEGER, client INTEGER) INITRANS 50 PCTFREE 50;

/* SQL statements for type StagePutRequest */
CREATE TABLE StagePutRequest (flags INTEGER, userName VARCHAR2(2048), euid NUMBER, egid NUMBER, mask NUMBER, pid NUMBER, machine VARCHAR2(2048), svcClassName VARCHAR2(2048), userTag VARCHAR2(2048), reqId VARCHAR2(2048), creationTime INTEGER, lastModificationTime INTEGER, id INTEGER PRIMARY KEY, svcClass INTEGER, client INTEGER) INITRANS 50 PCTFREE 50;

/* SQL statements for type StageUpdateRequest */
CREATE TABLE StageUpdateRequest (flags INTEGER, userName VARCHAR2(2048), euid NUMBER, egid NUMBER, mask NUMBER, pid NUMBER, machine VARCHAR2(2048), svcClassName VARCHAR2(2048), userTag VARCHAR2(2048), reqId VARCHAR2(2048), creationTime INTEGER, lastModificationTime INTEGER, id INTEGER PRIMARY KEY, svcClass INTEGER, client INTEGER) INITRANS 50 PCTFREE 50;

/* SQL statements for type StageRmRequest */
CREATE TABLE StageRmRequest (flags INTEGER, userName VARCHAR2(2048), euid NUMBER, egid NUMBER, mask NUMBER, pid NUMBER, machine VARCHAR2(2048), svcClassName VARCHAR2(2048), userTag VARCHAR2(2048), reqId VARCHAR2(2048), creationTime INTEGER, lastModificationTime INTEGER, id INTEGER PRIMARY KEY, svcClass INTEGER, client INTEGER) INITRANS 50 PCTFREE 50;

/* SQL statements for type StagePutDoneRequest */
CREATE TABLE StagePutDoneRequest (flags INTEGER, userName VARCHAR2(2048), euid NUMBER, egid NUMBER, mask NUMBER, pid NUMBER, machine VARCHAR2(2048), svcClassName VARCHAR2(2048), userTag VARCHAR2(2048), reqId VARCHAR2(2048), creationTime INTEGER, lastModificationTime INTEGER, parentUuid VARCHAR2(2048), id INTEGER PRIMARY KEY, svcClass INTEGER, client INTEGER, parent INTEGER) INITRANS 50 PCTFREE 50;

/* SQL statements for type StageFileQueryRequest */
CREATE TABLE StageFileQueryRequest (flags INTEGER, userName VARCHAR2(2048), euid NUMBER, egid NUMBER, mask NUMBER, pid NUMBER, machine VARCHAR2(2048), svcClassName VARCHAR2(2048), userTag VARCHAR2(2048), reqId VARCHAR2(2048), creationTime INTEGER, lastModificationTime INTEGER, fileName VARCHAR2(2048), id INTEGER PRIMARY KEY, svcClass INTEGER, client INTEGER) INITRANS 50 PCTFREE 50;

/* SQL statements for type StageRequestQueryRequest */
CREATE TABLE StageRequestQueryRequest (flags INTEGER, userName VARCHAR2(2048), euid NUMBER, egid NUMBER, mask NUMBER, pid NUMBER, machine VARCHAR2(2048), svcClassName VARCHAR2(2048), userTag VARCHAR2(2048), reqId VARCHAR2(2048), creationTime INTEGER, lastModificationTime INTEGER, id INTEGER PRIMARY KEY, svcClass INTEGER, client INTEGER) INITRANS 50 PCTFREE 50;

/* SQL statements for type StageFindRequestRequest */
CREATE TABLE StageFindRequestRequest (flags INTEGER, userName VARCHAR2(2048), euid NUMBER, egid NUMBER, mask NUMBER, pid NUMBER, machine VARCHAR2(2048), svcClassName VARCHAR2(2048), userTag VARCHAR2(2048), reqId VARCHAR2(2048), creationTime INTEGER, lastModificationTime INTEGER, id INTEGER PRIMARY KEY, svcClass INTEGER, client INTEGER) INITRANS 50 PCTFREE 50;

/* SQL statements for type SubRequest */
CREATE TABLE SubRequest (retryCounter NUMBER, fileName VARCHAR2(2048), protocol VARCHAR2(2048), xsize INTEGER, priority NUMBER, subreqId VARCHAR2(2048), flags NUMBER, modeBits NUMBER, creationTime INTEGER, lastModificationTime INTEGER, answered NUMBER, repackVid VARCHAR2(2048), id INTEGER PRIMARY KEY, diskcopy INTEGER, castorFile INTEGER, parent INTEGER, status INTEGER, request INTEGER, getNextStatus INTEGER) INITRANS 50 PCTFREE 50;

/* SQL statements for type StageReleaseFilesRequest */
CREATE TABLE StageReleaseFilesRequest (flags INTEGER, userName VARCHAR2(2048), euid NUMBER, egid NUMBER, mask NUMBER, pid NUMBER, machine VARCHAR2(2048), svcClassName VARCHAR2(2048), userTag VARCHAR2(2048), reqId VARCHAR2(2048), creationTime INTEGER, lastModificationTime INTEGER, id INTEGER PRIMARY KEY, svcClass INTEGER, client INTEGER) INITRANS 50 PCTFREE 50;

/* SQL statements for type StageAbortRequest */
CREATE TABLE StageAbortRequest (parentUuid VARCHAR2(2048), flags INTEGER, userName VARCHAR2(2048), euid NUMBER, egid NUMBER, mask NUMBER, pid NUMBER, machine VARCHAR2(2048), svcClassName VARCHAR2(2048), userTag VARCHAR2(2048), reqId VARCHAR2(2048), creationTime INTEGER, lastModificationTime INTEGER, id INTEGER PRIMARY KEY, parent INTEGER, svcClass INTEGER, client INTEGER) INITRANS 50 PCTFREE 50;

/* SQL statements for type StageGetNextRequest */
CREATE TABLE StageGetNextRequest (parentUuid VARCHAR2(2048), flags INTEGER, userName VARCHAR2(2048), euid NUMBER, egid NUMBER, mask NUMBER, pid NUMBER, machine VARCHAR2(2048), svcClassName VARCHAR2(2048), userTag VARCHAR2(2048), reqId VARCHAR2(2048), creationTime INTEGER, lastModificationTime INTEGER, id INTEGER PRIMARY KEY, parent INTEGER, svcClass INTEGER, client INTEGER) INITRANS 50 PCTFREE 50;

/* SQL statements for type StagePutNextRequest */
CREATE TABLE StagePutNextRequest (parentUuid VARCHAR2(2048), flags INTEGER, userName VARCHAR2(2048), euid NUMBER, egid NUMBER, mask NUMBER, pid NUMBER, machine VARCHAR2(2048), svcClassName VARCHAR2(2048), userTag VARCHAR2(2048), reqId VARCHAR2(2048), creationTime INTEGER, lastModificationTime INTEGER, id INTEGER PRIMARY KEY, parent INTEGER, svcClass INTEGER, client INTEGER) INITRANS 50 PCTFREE 50;

/* SQL statements for type StageUpdateNextRequest */
CREATE TABLE StageUpdateNextRequest (parentUuid VARCHAR2(2048), flags INTEGER, userName VARCHAR2(2048), euid NUMBER, egid NUMBER, mask NUMBER, pid NUMBER, machine VARCHAR2(2048), svcClassName VARCHAR2(2048), userTag VARCHAR2(2048), reqId VARCHAR2(2048), creationTime INTEGER, lastModificationTime INTEGER, id INTEGER PRIMARY KEY, parent INTEGER, svcClass INTEGER, client INTEGER) INITRANS 50 PCTFREE 50;

/* SQL statements for type Tape */
CREATE TABLE Tape (vid VARCHAR2(2048), side NUMBER, tpmode NUMBER, errMsgTxt VARCHAR2(2048), errorCode NUMBER, severity NUMBER, vwAddress VARCHAR2(2048), id INTEGER PRIMARY KEY, stream INTEGER, status INTEGER) INITRANS 50 PCTFREE 50;

/* SQL statements for type Segment */
CREATE TABLE Segment (fseq NUMBER, offset INTEGER, bytes_in INTEGER, bytes_out INTEGER, host_bytes INTEGER, segmCksumAlgorithm VARCHAR2(2048), segmCksum NUMBER, errMsgTxt VARCHAR2(2048), errorCode NUMBER, severity NUMBER, blockId0 INTEGER, blockId1 INTEGER, blockId2 INTEGER, blockId3 INTEGER, id INTEGER PRIMARY KEY, tape INTEGER, copy INTEGER, status INTEGER) INITRANS 50 PCTFREE 50;

/* SQL statements for type TapePool */
CREATE TABLE TapePool (name VARCHAR2(2048), id INTEGER PRIMARY KEY) INITRANS 50 PCTFREE 50;

/* SQL statements for type TapeCopy */
CREATE TABLE TapeCopy (copyNb NUMBER, id INTEGER PRIMARY KEY, castorFile INTEGER, status INTEGER) INITRANS 50 PCTFREE 50;

/* SQL statements for type CastorFile */
CREATE TABLE CastorFile (fileId INTEGER, nsHost VARCHAR2(2048), fileSize INTEGER, creationTime INTEGER, lastAccessTime INTEGER, nbAccesses NUMBER, lastKnownFileName VARCHAR2(2048), id INTEGER PRIMARY KEY, svcClass INTEGER, fileClass INTEGER) INITRANS 50 PCTFREE 50;

/* SQL statements for type DiskCopy */
CREATE TABLE DiskCopy (path VARCHAR2(2048), gcWeight float, creationTime INTEGER, id INTEGER PRIMARY KEY, fileSystem INTEGER, castorFile INTEGER, status INTEGER) INITRANS 50 PCTFREE 50;

/* SQL statements for type FileSystem */
CREATE TABLE FileSystem (free INTEGER, weight float, fsDeviation float, mountPoint VARCHAR2(2048), deltaWeight float, deltaFree NUMBER, reservedSpace NUMBER, minFreeSpace float, minAllowedFreeSpace float, maxFreeSpace float, spaceToBeFreed INTEGER, totalSize INTEGER, id INTEGER PRIMARY KEY, diskPool INTEGER, diskserver INTEGER, status INTEGER) INITRANS 50 PCTFREE 50;

/* SQL statements for type SvcClass */
CREATE TABLE SvcClass (nbDrives NUMBER, name VARCHAR2(2048), defaultFileSize INTEGER, maxReplicaNb NUMBER, replicationPolicy VARCHAR2(2048), gcPolicy VARCHAR2(2048), migratorPolicy VARCHAR2(2048), recallerPolicy VARCHAR2(2048), id INTEGER PRIMARY KEY) INITRANS 50 PCTFREE 50;
CREATE TABLE SvcClass2TapePool (Parent INTEGER, Child INTEGER) INITRANS 50 PCTFREE 50;
CREATE INDEX I_SvcClass2TapePool_C on SvcClass2TapePool (child);
CREATE INDEX I_SvcClass2TapePool_P on SvcClass2TapePool (parent);

/* SQL statements for type DiskPool */
CREATE TABLE DiskPool (name VARCHAR2(2048), id INTEGER PRIMARY KEY) INITRANS 50 PCTFREE 50;
CREATE TABLE DiskPool2SvcClass (Parent INTEGER, Child INTEGER) INITRANS 50 PCTFREE 50;
CREATE INDEX I_DiskPool2SvcClass_C on DiskPool2SvcClass (child);
CREATE INDEX I_DiskPool2SvcClass_P on DiskPool2SvcClass (parent);

/* SQL statements for type Stream */
CREATE TABLE Stream (initialSizeToTransfer INTEGER, id INTEGER PRIMARY KEY, tape INTEGER, tapePool INTEGER, status INTEGER) INITRANS 50 PCTFREE 50;
CREATE TABLE Stream2TapeCopy (Parent INTEGER, Child INTEGER) INITRANS 50 PCTFREE 50;
CREATE INDEX I_Stream2TapeCopy_C on Stream2TapeCopy (child);
CREATE INDEX I_Stream2TapeCopy_P on Stream2TapeCopy (parent);

/* SQL statements for type FileClass */
CREATE TABLE FileClass (name VARCHAR2(2048), minFileSize INTEGER, maxFileSize INTEGER, nbCopies NUMBER, id INTEGER PRIMARY KEY) INITRANS 50 PCTFREE 50;

/* SQL statements for type DiskServer */
CREATE TABLE DiskServer (name VARCHAR2(2048), id INTEGER PRIMARY KEY, status INTEGER) INITRANS 50 PCTFREE 50;

/* SQL statements for type SetFileGCWeight */
CREATE TABLE SetFileGCWeight (flags INTEGER, userName VARCHAR2(2048), euid NUMBER, egid NUMBER, mask NUMBER, pid NUMBER, machine VARCHAR2(2048), svcClassName VARCHAR2(2048), userTag VARCHAR2(2048), reqId VARCHAR2(2048), creationTime INTEGER, lastModificationTime INTEGER, weight float, id INTEGER PRIMARY KEY, svcClass INTEGER, client INTEGER) INITRANS 50 PCTFREE 50;

/* SQL statements for type TapeAccessSpecification */
CREATE TABLE TapeAccessSpecification (accessMode NUMBER, density VARCHAR2(2048), tapeModel VARCHAR2(2048), id INTEGER PRIMARY KEY) INITRANS 50 PCTFREE 50;

/* SQL statements for type TapeServer */
CREATE TABLE TapeServer (serverName VARCHAR2(2048), id INTEGER PRIMARY KEY, actingMode INTEGER) INITRANS 50 PCTFREE 50;

/* SQL statements for type TapeRequest */
CREATE TABLE TapeRequest (priority NUMBER, modificationTime INTEGER, creationTime INTEGER, id INTEGER PRIMARY KEY, tape INTEGER, tapeAccessSpecification INTEGER, requestedSrv INTEGER, tapeDrive INTEGER, deviceGroupName INTEGER, client INTEGER) INITRANS 50 PCTFREE 50;

/* SQL statements for type TapeDrive */
CREATE TABLE TapeDrive (jobID NUMBER, modificationTime INTEGER, resettime INTEGER, usecount NUMBER, errcount NUMBER, transferredMB NUMBER, totalMB INTEGER, driveName VARCHAR2(2048), tapeAccessMode NUMBER, id INTEGER PRIMARY KEY, tape INTEGER, runningTapeReq INTEGER, deviceGroupName INTEGER, status INTEGER, tapeServer INTEGER) INITRANS 50 PCTFREE 50;
CREATE TABLE TapeDrive2TapeDriveComp (Parent INTEGER, Child INTEGER) INITRANS 50 PCTFREE 50;
CREATE INDEX I_TapeDrive2TapeDriveComp_C on TapeDrive2TapeDriveComp (child);
CREATE INDEX I_TapeDrive2TapeDriveComp_P on TapeDrive2TapeDriveComp (parent);

/* SQL statements for type ErrorHistory */
CREATE TABLE ErrorHistory (errorMessage VARCHAR2(2048), timeStamp INTEGER, id INTEGER PRIMARY KEY, tapeDrive INTEGER, tape INTEGER) INITRANS 50 PCTFREE 50;

/* SQL statements for type TapeDriveDedication */
CREATE TABLE TapeDriveDedication (clientHost VARCHAR2(2048), euid NUMBER, egid NUMBER, vid VARCHAR2(2048), accessMode NUMBER, startTime INTEGER, endTime INTEGER, reason VARCHAR2(2048), id INTEGER PRIMARY KEY, tapeDrive INTEGER) INITRANS 50 PCTFREE 50;

/* SQL statements for type TapeDriveCompatibility */
CREATE TABLE TapeDriveCompatibility (tapeDriveModel VARCHAR2(2048), priorityLevel NUMBER, id INTEGER PRIMARY KEY, tapeAccessSpecification INTEGER) INITRANS 50 PCTFREE 50;

/* SQL statements for type DeviceGroupName */
CREATE TABLE DeviceGroupName (dgName VARCHAR2(2048), libraryName VARCHAR2(2048), id INTEGER PRIMARY KEY) INITRANS 50 PCTFREE 50;

ALTER TABLE SvcClass2TapePool
  ADD CONSTRAINT fk_SvcClass2TapePool_P FOREIGN KEY (Parent) REFERENCES SvcClass (id)
  ADD CONSTRAINT fk_SvcClass2TapePool_C FOREIGN KEY (Child) REFERENCES TapePool (id);
ALTER TABLE DiskPool2SvcClass
  ADD CONSTRAINT fk_DiskPool2SvcClass_P FOREIGN KEY (Parent) REFERENCES DiskPool (id)
  ADD CONSTRAINT fk_DiskPool2SvcClass_C FOREIGN KEY (Child) REFERENCES SvcClass (id);
ALTER TABLE Stream2TapeCopy
  ADD CONSTRAINT fk_Stream2TapeCopy_P FOREIGN KEY (Parent) REFERENCES Stream (id)
  ADD CONSTRAINT fk_Stream2TapeCopy_C FOREIGN KEY (Child) REFERENCES TapeCopy (id);
ALTER TABLE TapeDrive2TapeDriveComp
  ADD CONSTRAINT fk_TapeDrive2TapeDriveComp_P FOREIGN KEY (Parent) REFERENCES TapeDrive (id)
  ADD CONSTRAINT fk_TapeDrive2TapeDriveComp_C FOREIGN KEY (Child) REFERENCES TapeDriveCompatibility (id);
/*******************************************************************
 *
 * @(#)$RCSfile: castor_oracle_create.sql,v $ $Revision: 1.62 $ $Release$ $Date: 2006/06/13 08:36:54 $ $Author: sponcec3 $
 *
 * This file contains SQL code that is not generated automatically
 * and is inserted at the end of the generated code
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *******************************************************************/

/* A small table used to cross check code and DB versions */
CREATE TABLE CastorVersion (version VARCHAR2(2048));
INSERT INTO CastorVersion VALUES ('2_0_3_0');

/* Sequence for indices */
CREATE SEQUENCE ids_seq;

/* SQL statements for object types */
CREATE TABLE Id2Type (id INTEGER PRIMARY KEY, type NUMBER);
CREATE INDEX I_Id2Type_typeId on Id2Type (type, id);

/* SQL statements for requests status */
/* Partitioning enables faster response (more than indexing) for the most frequent queries - credits to Nilo Segura */
CREATE TABLE newRequests (type NUMBER(38) NOT NULL, id NUMBER(38) NOT NULL, creation DATE NOT NULL, PRIMARY KEY (type, id))
organization index
compress
partition by list (type)
 (
  partition type_16 values (16)  tablespace stager_data,
  partition type_21 values (21)  tablespace stager_data,
  partition type_33 values (33)  tablespace stager_data,
  partition type_34 values (34)  tablespace stager_data,
  partition type_35 values (35)  tablespace stager_data,
  partition type_36 values (36)  tablespace stager_data,
  partition type_37 values (37)  tablespace stager_data,
  partition type_38 values (38)  tablespace stager_data,
  partition type_39 values (39)  tablespace stager_data,
  partition type_40 values (40)  tablespace stager_data,
  partition type_41 values (41)  tablespace stager_data,
  partition type_42 values (42)  tablespace stager_data,
  partition type_43 values (43)  tablespace stager_data,
  partition type_44 values (44)  tablespace stager_data,
  partition type_45 values (45)  tablespace stager_data,
  partition type_46 values (46)  tablespace stager_data,
  partition type_48 values (48)  tablespace stager_data,
  partition type_49 values (49)  tablespace stager_data,
  partition type_50 values (50)  tablespace stager_data,
  partition type_51 values (51)  tablespace stager_data,
  partition type_60 values (60)  tablespace stager_data,
  partition type_64 values (64)  tablespace stager_data,
  partition type_65 values (65)  tablespace stager_data,
  partition type_66 values (66)  tablespace stager_data,
  partition type_67 values (67)  tablespace stager_data,
  partition type_78 values (78)  tablespace stager_data,
  partition type_79 values (79)  tablespace stager_data,
  partition type_80 values (80)  tablespace stager_data,
  partition type_84 values (84)  tablespace stager_data,
  partition type_90 values (90)  tablespace stager_data,
  partition notlisted values (default) tablespace stager_data
 );

/* Indexes related to CastorFiles */
CREATE UNIQUE INDEX I_DiskServer_name on DiskServer (name);
CREATE UNIQUE INDEX I_CastorFile_fileIdNsHost on CastorFile (fileId, nsHost);
CREATE INDEX I_CastorFile_lastKnownFileName on CastorFile (lastKnownFileName);

CREATE INDEX I_DiskCopy_Castorfile on DiskCopy (castorFile);
CREATE INDEX I_DiskCopy_FileSystem on DiskCopy (fileSystem);
CREATE INDEX I_TapeCopy_Castorfile on TapeCopy (castorFile);

CREATE INDEX I_FileSystem_DiskPool on FileSystem (diskPool);
CREATE INDEX I_FileSystem_DiskServer on FileSystem(diskServer);

CREATE INDEX I_SubRequest_Castorfile on SubRequest (castorFile);
CREATE INDEX I_SubRequest_DiskCopy on SubRequest (diskCopy);
CREATE INDEX I_SubRequest_Request on SubRequest (request);
CREATE INDEX I_SubRequest_Parent on SubRequest (parent);
CREATE INDEX I_SubRequest_GetNextStatus on SubRequest (decode(getNextStatus,1,getNextStatus,NULL));

/* A primary key for better scan of Stream2TapeCopy */
CREATE UNIQUE INDEX I_pk_Stream2TapeCopy ON Stream2TapeCopy (parent, child);

/* some constraints */
/* Can not be added since some code (stager_fs_service) creates FileSystems with no DiskServer
ALTER TABLE FileSystem ADD CONSTRAINT diskserver_fk FOREIGN KEY (diskServer) REFERENCES DiskServer(id);
*/
ALTER TABLE FileSystem MODIFY (status NOT NULL);
ALTER TABLE FileSystem MODIFY (diskServer NOT NULL);
ALTER TABLE DiskServer MODIFY (status NOT NULL);

/* enable row movements in Diskcopy */
ALTER TABLE DiskCopy ENABLE ROW MOVEMENT;

/* A little function base index to speed up subrequestToDo */
CREATE INDEX I_SubRequest_Status on SubRequest (decode(status,0,status,1,status,2,status,NULL));

/* Same kind of index to speed up subRequestFailedToDo */
CREATE INDEX I_SubRequest_Status7 on SubRequest (decode(status,7,status,null));

/* an index to speed up queries in FileQueryRequest, FindRequestRequest, RequestQueryRequest */
CREATE INDEX I_QueryParameter_Query on QueryParameter (query);

/* an index to speed the queries on Segments by copy */
CREATE INDEX I_Segment_Copy on Segment (copy);

/* Constraint on FileClass name */
ALTER TABLE FileClass ADD UNIQUE (name); 

/* Add unique constraint on castorFiles */
ALTER TABLE CastorFile ADD UNIQUE (fileId, nsHost); 

/* Add unique constraint on tapes */
ALTER TABLE Tape ADD UNIQUE (VID, side, tpMode); 


/* get current time as a time_t. Not that easy in ORACLE */
CREATE OR REPLACE FUNCTION getTime RETURN NUMBER IS
  ret NUMBER;
BEGIN
  SELECT (SYSDATE - to_date('01-jan-1970 01:00:00','dd-mon-yyyy HH:MI:SS')) * (24*60*60) INTO ret FROM DUAL;
  RETURN ret;
END;

/* Global temporary table to handle output of the filesDeletedProc procedure */
CREATE GLOBAL TEMPORARY TABLE FilesDeletedProcOutput
  (fileid NUMBER, nshost VARCHAR2(2048))
ON COMMIT DELETE ROWS;

/****************************************************************/
/* NbTapeCopiesInFS to work around ORACLE missing optimizations */
/****************************************************************/

/* This table keeps track of the number of TapeCopy waiting for migration
 * which have a diskCopy on this fileSystem. This table only exist
 * because Oracle is not able to optimize the following query :
 * SELECT max(A) from A, B where A.pk = B.fk;
 * Such a query is needed in bestTapeCopyForStream in order to
 * select filesystems effectively having tapecopies.
 * As a work around, we keep track of the tapecopies for each
 * filesystem. The cost is an increase of complexity and especially
 * of the number of triggers ensuring consistency of the whole database */
CREATE TABLE NbTapeCopiesInFS (FS NUMBER, Stream NUMBER, NbTapeCopies NUMBER);
CREATE UNIQUE INDEX I_NbTapeCopiesInFS_FSStream on NbTapeCopiesInFS(FS, Stream);
CREATE INDEX I_NbTapeCopiesInFS_Stream on NbTapeCopiesInFS(Stream);

/* Used to create a row INTO NbTapeCopiesInFS whenever a new
   FileSystem is created */
CREATE OR REPLACE TRIGGER tr_FileSystem_Insert
BEFORE INSERT ON FileSystem
FOR EACH ROW
BEGIN
  FOR item in (SELECT id FROM Stream) LOOP
    INSERT INTO NbTapeCopiesInFS (FS, Stream, NbTapeCopies) VALUES (:new.id, item.id, 0);
  END LOOP;
END;

/* Used to delete rows IN NbTapeCopiesInFS whenever a
   FileSystem is deleted */
CREATE OR REPLACE TRIGGER tr_FileSystem_Delete
BEFORE DELETE ON FileSystem
FOR EACH ROW
BEGIN
  DELETE FROM NbTapeCopiesInFS WHERE FS = :old.id;
END;

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
AFTER INSERT ON Stream2TapeCopy
FOR EACH ROW
BEGIN
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
BEFORE DELETE ON Stream2TapeCopy
FOR EACH ROW
BEGIN
  UPDATE NbTapeCopiesInFS SET NbTapeCopies = NbTapeCopies - 1
   WHERE FS IN (SELECT DiskCopy.FileSystem
                  FROM DiskCopy, TapeCopy
                 WHERE DiskCopy.CastorFile = TapeCopy.castorFile
                   AND TapeCopy.id = :new.child
                   AND DiskCopy.status = 10) -- CANBEMIGR
     AND Stream = :new.parent;
END;

/* Updates the count of tapecopies in NbTapeCopiesInFS
   whenever a TapeCopy has failed to be migrated and is
   put back in WAITINSTREAM from the SELECTED status */
CREATE OR REPLACE TRIGGER tr_TapeCopy_Update
AFTER UPDATE of status ON TapeCopy
FOR EACH ROW
WHEN (old.status = 3 AND new.status = 2) -- SELECTED AND WAITINSTREAMS
BEGIN
  UPDATE NbTapeCopiesInFS SET NbTapeCopies = NbTapeCopies + 1
   WHERE FS IN (SELECT FileSystem FROM DiskCopy
                 WHERE CastorFile = :new.castorFile AND status = 10) -- CANBEMIGR
     AND Stream IN (SELECT parent FROM Stream2TapeCopy WHERE child = :new.id);
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
  UPDATE NbTapeCopiesInFS SET NbTapeCopies = NbTapeCopies + 1
   WHERE FS = :new.fileSystem
     AND Stream IN (SELECT Stream2TapeCopy.parent
                      FROM Stream2TapeCopy, TapeCopy
                     WHERE TapeCopy.castorFile = :new.castorFile
                       AND Stream2TapeCopy.child = TapeCopy.id
                       AND TapeCopy.status = 2); -- WAITINSTREAMS
END;

/***************************************/
/* Some triggers to prevent dead locks */
/***************************************/

/* Used to avoid LOCK TABLE TapeCopy whenever someone wants
   to deal with the tapeCopies on a CastorFile.
   Due to this trigger, locking the CastorFile is enough
   to be safe */
CREATE OR REPLACE TRIGGER tr_TapeCopy_CastorFile
BEFORE INSERT OR UPDATE OF castorFile ON TapeCopy
FOR EACH ROW WHEN (new.castorFile > 0)
DECLARE
  unused CastorFile%ROWTYPE;
BEGIN
  SELECT * INTO unused FROM CastorFile
   WHERE id = :new.castorFile FOR UPDATE;
END;

/* Used to avoid LOCK TABLE TapeCopy whenever someone wants
   to deal with the tapeCopies on a CastorFile.
   Due to this trigger, locking the CastorFile is enough
   to be safe */
CREATE OR REPLACE TRIGGER tr_DiskCopy_CastorFile
BEFORE INSERT OR UPDATE OF castorFile ON DiskCopy
FOR EACH ROW WHEN (new.castorFile > 0)
DECLARE
  unused CastorFile%ROWTYPE;
BEGIN
  SELECT * INTO unused FROM CastorFile
   WHERE id = :new.castorFile FOR UPDATE;
END;

/* Used to avoid LOCK TABLE Stream2TapeCopy whenever someone wants
   to deal with the TapeCopies of a Stream.
   Due to this trigger, locking the Stream is enough
   to be safe */
CREATE OR REPLACE TRIGGER tr_Stream2TapeCopy_Stream
BEFORE INSERT OR UPDATE ON Stream2TapeCopy
FOR EACH ROW
DECLARE
  unused Stream%ROWTYPE;
BEGIN
  SELECT * INTO unused FROM Stream
   WHERE id = :new.Parent FOR UPDATE;
END;

/*********************/
/* FileSystem rating */
/*********************/

/* Computes a 'rate' for the filesystem which is an agglomeration
   of weight and fsDeviation. The goal is to be able to classify
   the fileSystems using a single value and to put an index on it */
CREATE OR REPLACE FUNCTION FileSystemRate
(weight IN NUMBER,
 deltaWeight IN NUMBER,
 fsDeviation IN NUMBER)
RETURN NUMBER DETERMINISTIC IS
BEGIN
  RETURN 1000*(weight + deltaWeight) - fsDeviation;
END;

/* FileSystem index based on the rate. */
CREATE INDEX I_FileSystem_Rate ON FileSystem(FileSystemRate(weight, deltaWeight, fsDeviation));



/*****************************/
/*                           */
/*   Procedure definitions   */
/*                           */
/*****************************/

/* PL/SQL method to make a SubRequest wait on another one, linked to the given DiskCopy */
CREATE OR REPLACE PROCEDURE makeSubRequestWait(srId IN INTEGER, dci IN INTEGER) AS
BEGIN
 -- all wait on the original one
 UPDATE SubRequest
  SET parent = (SELECT SubRequest.id
                  FROM SubRequest, DiskCopy
                 WHERE SubRequest.diskCopy = DiskCopy.id
                   AND DiskCopy.id = dci
                   AND SubRequest.parent = 0
                   AND DiskCopy.status IN (1, 2, 5, 6, 11) -- WAITDISK2DISKCOPY, WAITTAPERECALL, WAITFS, STAGEOUT, WAITFS_SCHEDULING
                   AND SubRequest.status IN (0, 1, 2, 3, 4, 5, 6)), -- START, RESTART, RETRY, WAIT*, READY
      status = 5,
      lastModificationTime = getTime() -- WAITSUBREQ
  WHERE SubRequest.id = srId;
END;

/*  PL/SQL method to archive a SubRequest */
CREATE OR REPLACE PROCEDURE archiveSubReq(srId IN INTEGER) AS
  rid INTEGER;
  rtype INTEGER;
  rclient INTEGER;
  nb INTEGER;
BEGIN
  -- update status of SubRequest
  UPDATE SubRequest SET status = 8 -- FINISHED
   WHERE id = srId RETURNING request INTO rid;

  -- Try to see whether another subrequest in the same
  -- request is still processing
  SELECT count(*) INTO nb FROM SubRequest
   WHERE request = rid AND status NOT IN (8); -- all but FINISHED

  -- Archive request if all subrequests have finished
  IF nb = 0 THEN
    UPDATE SubRequest SET status=11 WHERE request=rid and status=8;  -- ARCHIVED 
  END IF;
END;


/* PL/SQL method to delete request */

CREATE OR REPLACE PROCEDURE deleteRequest(rId IN INTEGER) AS
  rtype INTEGER;
  rclient INTEGER;
BEGIN  -- delete Request, Client and SubRequests
  -- delete request from Id2Type
  DELETE FROM Id2Type WHERE id = rId RETURNING type INTO rtype;
 
  -- delete request and get client id
  IF rtype = 35 THEN -- StageGetRequest
    DELETE FROM StageGetRequest WHERE id = rId RETURNING client into rclient;
  ELSIF rtype = 40 THEN -- StagePutRequest
    DELETE FROM StagePutRequest WHERE id = rId RETURNING client INTO rclient;
  ELSIF rtype = 44 THEN -- StageUpdateRequest
    DELETE FROM StageUpdateRequest WHERE id = rId RETURNING client INTO rclient;
  ELSIF rtype = 39 THEN -- StagePutDoneRequest
    DELETE FROM StagePutDoneRequest WHERE id = rId RETURNING client INTO rclient;
  ELSIF rtype = 42 THEN -- StageRmRequest
    DELETE FROM StageRmRequest WHERE id = rId RETURNING client INTO rclient;
  ELSIF rtype = 51 THEN -- StageReleaseFilesRequest
    DELETE FROM StageReleaseFilesRequest WHERE id = rId RETURNING client INTO rclient;
  ELSIF rtype = 36 THEN -- StagePrepareToGetRequest
    DELETE FROM StagePrepareToGetRequest WHERE id = rId RETURNING client INTO rclient;
  ELSIF rtype = 37 THEN -- StagePrepareToPutRequest
    DELETE FROM StagePrepareToPutRequest WHERE id = rId RETURNING client INTO rclient;
  ELSIF rtype = 38 THEN -- StagePrepareToUpdateRequest
    DELETE FROM StagePrepareToUpdateRequest WHERE id = rId RETURNING client INTO rclient;
  END IF;

  -- Delete Client
  DELETE FROM Id2Type WHERE id = rclient;
  DELETE FROM Client WHERE id = rclient;
  
  -- Delete SubRequests
  DELETE FROM Id2Type WHERE id IN
        (SELECT id FROM SubRequest WHERE request = rId);
  DELETE FROM SubRequest WHERE request = rId;
END;


/* Search and delete too old archived subrequests and its request */

CREATE OR REPLACE PROCEDURE deleteArchivedRequests(timeOut IN NUMBER) AS
  myReq SubRequest.request%TYPE;
  CURSOR cur IS 
   SELECT DISTINCT request FROM SubRequest
    WHERE status=11 AND getTime() - lastModificationTime >= timeOut;
  counter NUMBER := 0;
BEGIN
  OPEN cur;
    LOOP
      counter := counter + 1;
      FETCH cur into myReq;
        EXIT WHEN cur%NOTFOUND;
      deleteRequest(myReq);
      IF (counter = 100) THEN
        COMMIT;
        counter := 0;
      END IF;
    END LOOP;
    COMMIT;
  CLOSE cur;
END;

/* Search and delete "out of date" subrequests and its request */

CREATE OR REPLACE PROCEDURE deleteOutOfDateRequests(timeOut IN NUMBER) AS
  myReq SubRequest.request%TYPE;
  CURSOR cur IS
   SELECT DISTINCT request FROM SubRequest
    WHERE getTime() - lastModificationTime >= timeOut;
  counter NUMBER := 0;
BEGIN
  OPEN cur;
    LOOP
      counter := counter + 1;
      FETCH cur into myReq;
        EXIT WHEN cur%NOTFOUND;
      deleteRequest(myReq);
      IF (counter = 100) THEN
        COMMIT;
        counter := 0;
      END IF;
    END LOOP;
    COMMIT;
  CLOSE cur;
END;

/* PL/SQL method implementing anyTapeCopyForStream.
 * This implementation is not the original one. It uses NbTapeCopiesInFS
 * because a join on the tables between DiskServer and Stream2TapeCopy
 * costs too much. It should actually not be the case but ORACLE is unable
 * to optimize correctly queries having a ROWNUM clause. It procesed the
 * the query without it (yes !!!) and apply the clause afterwards.
 * Here is the previous select in case ORACLE improves some day :
 * SELECT \/*+ FIRST_ROWS *\/ TapeCopy.id INTO unused
 * FROM DiskServer, FileSystem, DiskCopy, CastorFile, TapeCopy, Stream2TapeCopy
 *  WHERE DiskServer.id = FileSystem.diskserver
 *   AND DiskServer.status IN (0, 1) -- DISKSERVER_PRODUCTION, DISKSERVER_DRAINING
 *   AND FileSystem.id = DiskCopy.filesystem
 *   AND FileSystem.status IN (0, 1) -- FILESYSTEM_PRODUCTION, FILESYSTEM_DRAINING
 *   AND DiskCopy.castorfile = CastorFile.id
 *   AND TapeCopy.castorfile = Castorfile.id
 *   AND Stream2TapeCopy.child = TapeCopy.id
 *   AND Stream2TapeCopy.parent = streamId
 *   AND TapeCopy.status = 2 -- WAITINSTREAMS
 *   AND ROWNUM < 2;  */
CREATE OR REPLACE PROCEDURE anyTapeCopyForStream(streamId IN INTEGER, res OUT INTEGER) AS
  unused INTEGER;
BEGIN
  SELECT NbTapeCopiesInFS.NbTapeCopies INTO unused
    FROM NbTapeCopiesInFS
   WHERE NbTapeCopiesInFS.stream = streamId
     AND NbTapeCopiesInFS.NbTapeCopies > 0
     AND ROWNUM < 2;
  res := 1;
EXCEPTION
 WHEN NO_DATA_FOUND THEN
  res := 0;
END;

/* PL/SQL method to update FileSystem weight for new streams */
CREATE OR REPLACE PROCEDURE updateFsFileOpened
(ds IN INTEGER, fs IN INTEGER,
 deviation IN INTEGER, fileSize IN INTEGER) AS
BEGIN
 UPDATE FileSystem SET deltaWeight = deltaWeight - deviation
  WHERE diskServer = ds;
 UPDATE FileSystem SET fsDeviation = LEAST(2 * deviation, 1000),
                       reservedSpace = reservedSpace + fileSize
  WHERE id = fs;
END;

/* PL/SQL method to update FileSystem free space when file are closed */
CREATE OR REPLACE PROCEDURE updateFsFileClosed
(fs IN INTEGER, reservation IN INTEGER, fileSize IN INTEGER) AS
  deviation NUMBER;
  ds INTEGER;
  unused "numList";
BEGIN
 /* We have to do this first in order to take locks on all the filesystems
    of the DiskServer in an atomical way. Otherwise, the fact that
    we update the "single" filesystem fs first and then all the others
    leads to a dead lock if 2 threads are running this in parallel :
    they will both lock one filesystem to start with and try to get the
    others locks afterwards. */
 SELECT id BULK COLLECT INTO unused FROM FileSystem WHERE diskServer = ds FOR UPDATE;
 /* now we can safely go */
 UPDATE FileSystem SET deltaWeight = deltaWeight + deviation
  WHERE diskServer = ds;
 UPDATE FileSystem SET fsDeviation = fsdeviation / 2,
                       deltaFree = deltaFree - fileSize,
                       reservedSpace = reservedSpace - reservation
  WHERE id = fs RETURNING fsDeviation, diskServer INTO deviation, ds;
END;

/* This table is needed to insure that bestTapeCopyForStream works Ok.
 * It basically serializes the queries ending to the same diskserver.
 * This is only needed because of lack of funstionnality in ORACLE.
 * The original goal was to lock the selected filesystem in the first
 * query of bestTapeCopyForStream. But a SELECT FOR UPDATE was not enough
 * because it does not revalidate the inner select and we were thus selecting
 * n times the same filesystem when n queries were processed in parallel.
 * (take care, they were processed sequentially due to the lock, but they
 * were still locking the same filesystem). Thus an UPDATE was needed but
 * UPDATE cannot use joins. Thus it was impossible to lock DiskServer
 * and FileSystem at the same time (we need to avoid the DiskServer to
 * be chosen again (with a different filesystem) before we update the
 * weight of all filesystems). Locking the diskserver only was fine but
 * was introducing a possible deadlock with a place where the FileSystem
 * is locked before the DiskServer. Thus this table..... */
CREATE TABLE LockTable (DiskServerId NUMBER PRIMARY KEY, TheLock NUMBER);
INSERT INTO LockTable SELECT id, id FROM DiskServer;

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


/* PL/SQL method implementing updateFileSystemForJob */
CREATE OR REPLACE PROCEDURE updateFileSystemForJob
(fs IN VARCHAR2, ds IN VARCHAR2,
 fileSize IN NUMBER) AS
  fsID NUMBER;
  dsId NUMBER;
  dev NUMBER;
  unused NUMBER;
BEGIN
  SELECT FileSystem.id, FileSystem.fsDeviation,
         DiskServer.id INTO fsId, dev, dsId
    FROM FileSystem, DiskServer
   WHERE FileSystem.diskServer = DiskServer.id
     AND FileSystem.mountPoint = fs
     AND DiskServer.name = ds;
  -- We have to lock the DiskServer in the LockTable TABLE if we want
  -- to avoid dead locks with bestTapeCopyForStream. See the definition
  -- of the table for a complete explanation on why it exists
  SELECT TheLock INTO unused FROM LockTable WHERE DiskServerId = dsId FOR UPDATE;
  updateFsFileOpened(dsId, fsId, dev, fileSize);
END;


/* PL/SQL method implementing bestTapeCopyForStream */
CREATE OR REPLACE PROCEDURE bestTapeCopyForStream(streamId IN INTEGER,
                                                  diskServerName OUT VARCHAR2, mountPoint OUT VARCHAR2,
                                                  path OUT VARCHAR2, dci OUT INTEGER,
                                                  castorFileId OUT INTEGER, fileId OUT INTEGER,
                                                  nsHost OUT VARCHAR2, fileSize OUT INTEGER,
                                                  tapeCopyId OUT INTEGER) AS
 fileSystemId INTEGER;
 dsid NUMBER;
 deviation NUMBER;
 fsDiskServer NUMBER;
BEGIN
  -- We lock here a given DiskServer. See the comment for the creation of the LockTable
  -- table for a full explanation of why we need such a stupid UPDATE statement
  UPDATE LockTable SET theLock = 1
   WHERE diskServerId = (
     SELECT DS.diskserver_id
       FROM (
         -- The double level of selects is due to the fact that ORACLE is unable
         -- to use ROWNUM and ORDER BY at the same time. Thus, we have to first computes
         -- the maxRate and then select on it.
         SELECT diskserver.id diskserver_id
           FROM FileSystem FS, NbTapeCopiesInFS, DiskServer
          WHERE FS.id = NbTapeCopiesInFS.FS
            AND NbTapeCopiesInFS.NbTapeCopies > 0
            AND NbTapeCopiesInFS.Stream = StreamId
            AND FS.status IN (0, 1)
            AND DiskServer.id = FS.diskserver
            AND DiskServer.status IN (0, 1)
          ORDER BY FileSystemRate(FS.weight, FS.deltaWeight, FS.fsDeviation) DESC
          ) DS
      WHERE ROWNUM < 2)
  RETURNING diskServerId INTO dsid;

  -- Now we got our Diskserver but we lost all other data (due to the fact we had
  -- to do an update for the lock and we could not do a join in the update).
  -- So here we select all we need
  SELECT FN.name, FN.mountPoint, FN.fsDeviation, FN.diskserver, FN.id
    INTO diskServerName, mountPoint, deviation, fsDiskServer, fileSystemId
    FROM (
      SELECT DiskServer.name, FS.mountPoint, FS.fsDeviation,
             FS.diskserver, FS.id
        FROM FileSystem FS, NbTapeCopiesInFS, Diskserver
       WHERE FS.id = NbTapeCopiesInFS.FS
         AND DiskServer.id = FS.diskserver
         AND NbTapeCopiesInFS.NbTapeCopies > 0
         AND NbTapeCopiesInFS.Stream = StreamId
         AND FS.status IN (0, 1)    -- FILESYSTEM_PRODUCTION, FILESYSTEM_DRAINING
         AND FS.diskserver = dsId
       ORDER BY FileSystemRate(FS.weight, FS.deltaWeight, FS.fsDeviation) DESC
       ) FN
  WHERE ROWNUM < 2;   

  SELECT /*+ ORDERED */ DC.path, DC.diskcopy_id, DC.castorfile_id,   -- here the ORDERED hint forces a faster join (credits to Nilo)
         DC.fileId, DC.nsHost, DC.fileSize, TapeCopy.id
  INTO path, dci, castorFileId, fileId, nsHost, fileSize, tapeCopyId
  FROM (SELECT DiskCopy.path path, DiskCopy.id diskcopy_id, CastorFile.id castorfile_id,
               CastorFile.fileId, CastorFile.nsHost, CastorFile.fileSize
          FROM DiskCopy, CastorFile
         WHERE DiskCopy.castorfile = CastorFile.id
           AND DiskCopy.status = 10 -- CANBEMIGR
           AND DiskCopy.filesystem = fileSystemId 
       ) DC,
       TapeCopy, Stream2TapeCopy
 WHERE TapeCopy.castorfile = DC.castorfile_id
   AND Stream2TapeCopy.child = TapeCopy.id
   AND Stream2TapeCopy.parent = streamId
   AND TapeCopy.status = 2 -- WAITINSTREAMS
   AND ROWNUM < 2;

  -- update status of selected tapecopy and stream
  UPDATE TapeCopy SET status = 3 -- SELECTED
   WHERE id = tapeCopyId;
  UPDATE Stream SET status = 3 -- RUNNING
   WHERE id = streamId;

  -- update NbTapeCopiesInFS accordingly. Take care to remove the
  -- TapeCopy from all streams and all filesystems
  UPDATE NbTapeCopiesInFS NTC 
     SET NTC.NbTapeCopies = NTC.NbTapeCopies - 1
   WHERE EXISTS (
       SELECT 'x' 
         FROM DiskCopy, TapeCopy
        WHERE DiskCopy.CastorFile = TapeCopy.castorFile
          AND TapeCopy.id = tapeCopyId
          AND DiskCopy.status = 10 -- CANBEMIGR
          AND DiskCopy.FileSystem = NTC.FS
       )
     AND EXISTS (
       SELECT 'x' 
         FROM Stream2TapeCopy 
        WHERE child = tapeCopyId
          AND parent = NTC.Stream
       );

  -- Update Filesystem state
  updateFSFileOpened(fsDiskServer, fileSystemId, deviation, 0);

  EXCEPTION WHEN NO_DATA_FOUND THEN
    -- No data found means the selected filesystem has no
    -- tapecopies to be migrated. Thus we go to next one
    NULL;
END;


/* PL/SQL method implementing bestFileSystemForSegment */
CREATE OR REPLACE PROCEDURE bestFileSystemForSegment(segmentId IN INTEGER, diskServerName OUT VARCHAR2,
                                                     rmountPoint OUT VARCHAR2, rpath OUT VARCHAR2,
                                                     dci OUT INTEGER) AS
 fileSystemId NUMBER;
 castorFileId NUMBER;
 deviation NUMBER;
 fsDiskServer NUMBER;
 fileSize NUMBER;
BEGIN
 -- First get the DiskCopy and see whether it already has a fileSystem
 -- associated (case of a multi segment file)
 -- We also select on the DiskCopy status since we know it is
 -- in WAITTAPERECALL status and there may be other ones
 -- INVALID, GCCANDIDATE, DELETED, etc...
 SELECT DiskCopy.fileSystem, DiskCopy.path, DiskCopy.id, DiskCopy.CastorFile
   INTO fileSystemId, rpath, dci, castorFileId
   FROM TapeCopy, Segment, DiskCopy
    WHERE Segment.id = segmentId
     AND Segment.copy = TapeCopy.id
     AND DiskCopy.castorfile = TapeCopy.castorfile
     AND DiskCopy.status = 2; -- WAITTAPERECALL
 -- Check if the DiskCopy had a FileSystem associated
 IF fileSystemId > 0 THEN
   BEGIN
     -- it had one, force filesystem selection, unless it was disabled.
     SELECT DiskServer.name, DiskServer.id, FileSystem.mountPoint, FileSystem.fsDeviation
     INTO diskServerName, fsDiskServer, rmountPoint, deviation
     FROM DiskServer, FileSystem
      WHERE FileSystem.id = fileSystemId
       AND FileSystem.status = 0 -- FILESYSTEM_PRODUCTION
       AND DiskServer.id = FileSystem.diskServer
       AND DiskServer.status = 0; -- DISKSERVER_PRODUCTION
     updateFsFileOpened(fsDiskServer, fileSystemId, deviation, 0);
   EXCEPTION WHEN NO_DATA_FOUND THEN
     -- Error, the filesystem or the machine was probably disabled in between
     raise_application_error(-20101, 'In a multi-segment file, FileSystem or Machine was disabled before all segments were recalled');
   END;
 ELSE
   DECLARE
     CURSOR c1 IS SELECT DiskServer.name, FileSystem.mountPoint, FileSystem.id,
                       FileSystem.fsDeviation, FileSystem.diskserver, CastorFile.fileSize
                    FROM DiskServer, FileSystem, DiskPool2SvcClass,
                         (SELECT id, svcClass from StageGetRequest UNION ALL
                          SELECT id, svcClass from StagePrepareToGetRequest UNION ALL
                          SELECT id, svcClass from StageGetNextRequest UNION ALL
                          SELECT id, svcClass from StageUpdateRequest UNION ALL
                          SELECT id, svcClass from StagePrepareToUpdateRequest UNION ALL
                          SELECT id, svcClass from StageUpdateNextRequest) Request,
                         SubRequest, CastorFile
                   WHERE CastorFile.id = castorfileId
                     AND SubRequest.castorfile = castorfileId
                     AND Request.id = SubRequest.request
                     AND Request.svcclass = DiskPool2SvcClass.child
                     AND FileSystem.diskpool = DiskPool2SvcClass.parent
                     AND FileSystem.free + FileSystem.deltaFree - FileSystem.reservedSpace > CastorFile.fileSize
                     AND FileSystem.status = 0 -- FILESYSTEM_PRODUCTION
                     AND DiskServer.id = FileSystem.diskServer
                     AND DiskServer.status = 0 -- DISKSERVER_PRODUCTION
                   ORDER BY FileSystem.weight + FileSystem.deltaWeight DESC, FileSystem.fsDeviation ASC;
    BEGIN
      OPEN c1;
      FETCH c1 INTO diskServerName, rmountPoint, fileSystemId, deviation, fsDiskServer, fileSize;
      CLOSE c1;
      UPDATE DiskCopy SET fileSystem = fileSystemId WHERE id = dci;
      updateFsFileOpened(fsDiskServer, fileSystemId, deviation, fileSize);
    END;
  END IF;
END;

/* PL/SQL method implementing fileRecallFailed */
CREATE OR REPLACE PROCEDURE fileRecallFailed(tapecopyId IN INTEGER) AS
 SubRequestId NUMBER;
 dci NUMBER;
 fsId NUMBER;
 fileSize NUMBER;
BEGIN
  SELECT SubRequest.id, DiskCopy.id, CastorFile.filesize
    INTO SubRequestId, dci, fileSize
    FROM TapeCopy, SubRequest, DiskCopy, CastorFile
   WHERE TapeCopy.id = tapecopyId
     AND CastorFile.id = TapeCopy.castorFile
     AND DiskCopy.castorFile = TapeCopy.castorFile
     AND SubRequest.diskcopy(+) = DiskCopy.id;
  UPDATE DiskCopy SET status = 4 WHERE id = dci RETURNING fileSystem into fsid; -- DISKCOPY_FAILED
  IF SubRequestId IS NOT NULL THEN
    UPDATE SubRequest SET status = 7, -- SUBREQUEST_FAILED
                          getNextStatus = 1, -- GETNEXTSTATUS_FILESTAGED (not strictly correct but the request is over anyway)
                          lastModificationTime = getTime()
     WHERE id = SubRequestId;
    UPDATE SubRequest SET status = 7, -- SUBREQUEST_FAILED
                          getNextStatus = 1, -- GETNEXTSTATUS_FILESTAGED
                          lastModificationTime = getTime()
     WHERE parent = SubRequestId;
  END IF;
END;

/* PL/SQL method implementing castor package */
CREATE OR REPLACE PACKAGE castor AS
  TYPE DiskCopyCore IS RECORD (
        id INTEGER,
        path VARCHAR2(2048),
        status NUMBER,
        fsWeight NUMBER,
        mountPoint VARCHAR2(2048),
        diskServer VARCHAR2(2048));
  TYPE DiskCopy_Cur IS REF CURSOR RETURN DiskCopyCore;
  TYPE Segment_Cur IS REF CURSOR RETURN Segment%ROWTYPE;
  TYPE GCLocalFileCore IS RECORD (
        fileName VARCHAR2(2048),
        diskCopyId INTEGER);
  TYPE GCLocalFiles_Cur IS REF CURSOR RETURN GCLocalFileCore;
  TYPE "strList" IS TABLE OF VARCHAR2(2048) index by binary_integer;
  TYPE "cnumList" IS TABLE OF NUMBER index by binary_integer;
  TYPE QueryLine IS RECORD (
        fileid INTEGER,
        nshost VARCHAR2(2048),
        diskCopyId INTEGER,
        diskCopyPath VARCHAR2(2048),
        filesize INTEGER,
        diskCopyStatus INTEGER,
        diskServerName VARCHAR2(2048),
        fileSystemMountPoint VARCHAR2(2048),
        nbaccesses INTEGER,
        lastKnownFileName VARCHAR2(2048));
  TYPE QueryLine_Cur IS REF CURSOR RETURN QueryLine;
  TYPE FileList_Cur IS REF CURSOR RETURN FilesDeletedProcOutput%ROWTYPE;
  TYPE DiskPoolQueryLine IS RECORD (
        isDP INTEGER,
        isDS INTEGER,
        diskServerName VARCHAR(2048),
	diskServerStatus INTEGER,
        fileSystemmountPoint VARCHAR(2048),
	fileSystemfreeSpace INTEGER,
	fileSystemtotalSpace INTEGER,
	fileSystemreservedSpace INTEGER,
	fileSystemminfreeSpace INTEGER,
        fileSystemmaxFreeSpace INTEGER,
        fileSystemStatus INTEGER);
  TYPE DiskPoolQueryLine_Cur IS REF CURSOR RETURN DiskPoolQueryLine;
  TYPE DiskPoolsQueryLine IS RECORD (
        isDP INTEGER,
        isDS INTEGER,
        diskPoolName VARCHAR(2048),
        diskServerName VARCHAR(2048),
	diskServerStatus INTEGER,
        fileSystemmountPoint VARCHAR(2048),
	fileSystemfreeSpace INTEGER,
	fileSystemtotalSpace INTEGER,
	fileSystemreservedSpace INTEGER,
	fileSystemminfreeSpace INTEGER,
        fileSystemmaxFreeSpace INTEGER,
        fileSystemStatus INTEGER);
  TYPE DiskPoolsQueryLine_Cur IS REF CURSOR RETURN DiskPoolsQueryLine;
END castor;
CREATE OR REPLACE TYPE "numList" IS TABLE OF INTEGER;

/* PL/SQL method implementing isSubRequestToSchedule */
CREATE OR REPLACE PROCEDURE isSubRequestToSchedule
        (rsubreqId IN INTEGER, result OUT INTEGER,
         sources OUT castor.DiskCopy_Cur) AS
  stat "numList";
  dci "numList";
  svcClassId NUMBER;
  reqId NUMBER;
  cfId NUMBER;
BEGIN
  -- First see whether we should wait on an ongoing request
  SELECT DiskCopy.status, DiskCopy.id
    BULK COLLECT INTO stat, dci
    FROM DiskCopy, SubRequest, FileSystem, DiskServer
   WHERE SubRequest.id = rsubreqId
     AND SubRequest.castorfile = DiskCopy.castorfile
     AND DiskCopy.fileSystem = FileSystem.id
     AND FileSystem.status = 0 -- PRODUCTION
     AND FileSystem.diskserver = DiskServer.id
     AND DiskServer.status = 0 -- PRODUCTION
     AND DiskCopy.status IN (1, 2, 5, 6, 11); -- WAITDISK2DISKCOPY, WAITTAPERECALL, WAITFS, STAGEOUT, WAITFS_SCHEDULING
  IF stat.COUNT > 0 THEN
    -- Only DiskCopy is in WAIT*, make SubRequest wait on previous subrequest and do not schedule
    makeSubRequestWait(rsubreqId, dci(1));
    result := 0;  -- no schedule
    COMMIT;
    RETURN;
  END IF;
  -- Get the svcclass for this subrequest
  SELECT Request.id, Request.svcclass, SubRequest.castorfile
    INTO reqId, svcClassId, cfId
    FROM (SELECT id, svcClass from StageGetRequest UNION ALL
          SELECT id, svcClass from StagePrepareToGetRequest UNION ALL
          SELECT id, svcClass from StageUpdateRequest UNION ALL
          SELECT id, svcClass from StagePrepareToUpdateRequest UNION ALL
          SELECT id, svcClass from StagePutDoneRequest) Request,
         SubRequest
   WHERE Subrequest.request = Request.id
     AND Subrequest.id = rsubreqId;
  -- Try to see whether we have available DiskCopies
  SELECT DiskCopy.status, DiskCopy.id
    BULK COLLECT INTO stat, dci
    FROM DiskCopy, FileSystem, DiskServer, DiskPool2SvcClass
   WHERE DiskCopy.castorfile = cfId
     AND DiskCopy.fileSystem = FileSystem.id
     AND FileSystem.diskpool = DiskPool2SvcClass.parent
     AND DiskPool2SvcClass.child = svcClassId
     AND FileSystem.status = 0 -- PRODUCTION
     AND FileSystem.diskserver = DiskServer.id
     AND DiskServer.status = 0 -- PRODUCTION
     AND DiskCopy.status IN (0, 10); -- STAGED, STAGEOUT, CANBEMIGR
  IF stat.COUNT > 0 THEN
    -- In case of PutDone, Check there is no put going on
    -- If any, we'll wait on one of them
    DECLARE
      ty NUMBER;
    BEGIN
      SELECT type INTO ty FROM Id2Type WHERE id = reqId;
      IF ty = 39 THEN -- PutDone
        DECLARE
          putSubReq NUMBER;
        BEGIN
          -- Try to find a running put
          SELECT subrequest.id INTO putSubReq
            FROM SubRequest, Id2Type
           WHERE SubRequest.castorfile = cfId
             AND SubRequest.request = Id2Type.id
             AND Id2Type.type = 40 -- Put
             AND SubRequest.status IN (0, 1, 2, 3, 6) -- START, RESTART, RETRY, WAITSCHED, READY
             AND ROWNUM < 2;
          -- we've found one, putDone will have to wait
          UPDATE SubRequest
             SET parent = putSubReq,
                 status = 5, -- WAITSUBREQ
                 lastModificationTime = getTime()
           WHERE id = rsubreqId;
          result := 0;  -- no schedule
          RETURN;
        EXCEPTION WHEN NO_DATA_FOUND THEN
          -- no put waiting, let continue
          NULL;
        END;
      END IF;
    END;
    -- We are not in the case of a putDone with a running put
    -- so we should schedule and give a list of sources
    result := 1;  -- schedule and try to use available diskcopies
    OPEN sources
      FOR SELECT DiskCopy.id, DiskCopy.path, DiskCopy.status,
                  FileSystem.weight, FileSystem.mountPoint,
                  DiskServer.name
            FROM DiskCopy, SubRequest, FileSystem, DiskServer, DiskPool2SvcClass
           WHERE SubRequest.id = rsubreqId
             AND SubRequest.castorfile = DiskCopy.castorfile
             AND FileSystem.diskpool = DiskPool2SvcClass.parent
             AND DiskPool2SvcClass.child = svcClassId
             AND DiskCopy.status IN (0, 10) -- STAGED, STAGEOUT, CANBEMIGR
             AND FileSystem.id = DiskCopy.fileSystem
             AND FileSystem.status = 0 -- PRODUCTION
             AND DiskServer.id = FileSystem.diskServer
             AND DiskServer.status = 0; -- PRODUCTION
  ELSE
    -- We found no diskcopies for our svcclass, schedule anywhere
    -- Note that this could mean a tape recall or a disk2disk copy
    -- from an existing diskcopy not available to this svcclass
    result := 2;
  END IF;
END;

/* Build diskCopy path from fileId */
CREATE OR REPLACE PROCEDURE buildPathFromFileId(fid IN INTEGER,
                                                nsHost IN VARCHAR2,
                                                dcid IN INTEGER,
                                                path OUT VARCHAR2) AS
BEGIN
  path := CONCAT(CONCAT(CONCAT(CONCAT(TO_CHAR(MOD(fid,100),'FM09'), '/'),
                        CONCAT(TO_CHAR(fid), '@')),
                 nsHost), CONCAT('.', TO_CHAR(dcid)));
END;

/* PL/SQL method implementing getUpdateStart */
CREATE OR REPLACE PROCEDURE getUpdateStart
        (srId IN INTEGER, fileSystemId IN INTEGER,
         dci OUT INTEGER, rpath OUT VARCHAR2,
         rstatus OUT NUMBER, sources OUT castor.DiskCopy_Cur,
         reuid OUT INTEGER, regid OUT INTEGER) AS
  cfid INTEGER;
  fid INTEGER;
  nh VARCHAR2(2048);
  unused CastorFile%ROWTYPE;
BEGIN
 -- Get and uid, gid
 SELECT euid, egid INTO reuid, regid FROM SubRequest,
      (SELECT id, euid, egid from StageGetRequest UNION ALL
       SELECT id, euid, egid from StagePrepareToGetRequest UNION ALL
       SELECT id, euid, egid from StageUpdateRequest UNION ALL
       SELECT id, euid, egid from StagePrepareToUpdateRequest) Request
  WHERE SubRequest.request = Request.id AND SubRequest.id = srId;
 -- Take a lock on the CastorFile. Associated with triggers,
 -- this guarantee we are the only ones dealing with its copies
 SELECT CastorFile.* INTO unused FROM CastorFile, SubRequest
  WHERE CastorFile.id = SubRequest.castorFile
    AND SubRequest.id = srId FOR UPDATE;
 -- Try to find local DiskCopy
 dci := 0;
 SELECT DiskCopy.id, DiskCopy.path, DiskCopy.status
  INTO dci, rpath, rstatus
  FROM DiskCopy, SubRequest
  WHERE SubRequest.id = srId
    AND SubRequest.castorfile = DiskCopy.castorfile
    AND DiskCopy.filesystem = fileSystemId
    AND DiskCopy.status IN (0, 1, 2, 5, 6, 10, 11); -- STAGED, WAITDISKTODISKCOPY, WAITTAPERECALL, WAIFS, STAGEOUT, CANBEMIGR, WAITFS_SCHEDULING
 -- If found local one, check whether to wait on it
 IF rstatus IN (1, 2, 5, 11) THEN -- WAITDISK2DISKCOPY, WAITTAPERECALL, WAITFS, WAITFS_SCHEDULING, Make SubRequest Wait
   makeSubRequestWait(srId, dci);
   dci := 0;
   rpath := '';
 END IF;
EXCEPTION WHEN NO_DATA_FOUND THEN
 -- No disk copy found on selected FileSystem, look in others
 BEGIN
  -- Try to see whether we should wait on some DiskCopy
  SELECT DiskCopy.id, DiskCopy.path, DiskCopy.status
  INTO dci, rpath, rstatus
  FROM DiskCopy, SubRequest, FileSystem, DiskServer
  WHERE SubRequest.id = srId
    AND SubRequest.castorfile = DiskCopy.castorfile
    AND DiskCopy.status IN (1, 2, 5, 11) -- WAITDISKTODISKCOPY, WAITTAPERECALL, WAIFS, WAITFS_SCHEDULING
    AND FileSystem.id(+) = DiskCopy.fileSystem
    AND FileSystem.status(+) = 0 -- PRODUCTION
    AND DiskServer.id(+) = FileSystem.diskserver
    AND DiskServer.status(+) = 0 -- PRODUCTION
    AND ROWNUM < 2;
  -- Found a DiskCopy, we have to wait on it
  makeSubRequestWait(srId, dci);
  dci := 0;
  rpath := '';
 EXCEPTION WHEN NO_DATA_FOUND THEN
   -- No disk copy foundin WAIT*, we don't have to wait
  BEGIN
   -- Check whether there are any diskcopy available
   SELECT DiskCopy.id, DiskCopy.path, DiskCopy.status
   INTO dci, rpath, rstatus
   FROM DiskCopy, SubRequest, FileSystem, DiskServer
   WHERE SubRequest.id = srId
     AND SubRequest.castorfile = DiskCopy.castorfile
     AND DiskCopy.status IN (0, 6, 10) -- STAGED, STAGEOUT, CANBEMIGR
     AND FileSystem.id = DiskCopy.fileSystem
     AND FileSystem.status IN (0, 1) -- PRODUCTION, DRAINING
     AND DiskServer.id = FileSystem.diskserver
     AND DiskServer.status IN (0, 1) -- PRODUCTION, DRAINING
     AND ROWNUM < 2;
   -- We found at least a DiskCopy. Let's list all of them
   OPEN sources
    FOR SELECT DiskCopy.id, DiskCopy.path, DiskCopy.status,
               FileSystem.weight, FileSystem.mountPoint,
               DiskServer.name
    FROM DiskCopy, SubRequest, FileSystem, DiskServer
    WHERE SubRequest.id = srId
      AND SubRequest.castorfile = DiskCopy.castorfile
      AND DiskCopy.status IN (0, 6, 10) -- STAGED, STAGEOUT, CANBEMIGR
      AND FileSystem.id = DiskCopy.fileSystem
      AND FileSystem.status IN (0, 1) -- PRODUCTION, DRAINING
      AND DiskServer.id = FileSystem.diskServer
      AND DiskServer.status IN (0, 1); -- PRODUCTION, DRAINING
    -- create DiskCopy for Disk to Disk copy
    UPDATE SubRequest SET diskCopy = ids_seq.nextval,
                          lastModificationTime = getTime() WHERE id = srId
     RETURNING castorFile, diskCopy INTO cfid, dci;
    SELECT fileId, nsHost INTO fid, nh FROM CastorFile WHERE id = cfid;
    buildPathFromFileId(fid, nh, dci, rpath);
    INSERT INTO DiskCopy (path, id, FileSystem, castorFile, status, creationTime)
     VALUES (rpath, dci, fileSystemId, cfid, 1, getTime()); -- status WAITDISK2DISKCOPY
    INSERT INTO Id2Type (id, type) VALUES (dci, 5); -- OBJ_DiskCopy
    rstatus := 1; -- status WAITDISK2DISKCOPY
  EXCEPTION WHEN NO_DATA_FOUND THEN
   -- No disk copy found on any FileSystem
   -- create one for recall
   UPDATE SubRequest SET diskCopy = ids_seq.nextval, status = 4,
                         lastModificationTime = getTime() -- WAITTAPERECALL
    WHERE id = srId RETURNING castorFile, diskCopy INTO cfid, dci;
   SELECT fileId, nsHost INTO fid, nh FROM CastorFile WHERE id = cfid;
   buildPathFromFileId(fid, nh, dci, rpath);
   INSERT INTO DiskCopy (path, id, FileSystem, castorFile, status, creationTime)
    VALUES (rpath, dci, 0, cfid, 2, getTime()); -- status WAITTAPERECALL
   INSERT INTO Id2Type (id, type) VALUES (dci, 5); -- OBJ_DiskCopy
   rstatus := 99; -- WAITTAPERECALL, NEWLY CREATED
  END;
 END;
END;

/* PL/SQL method implementing putStart */
CREATE OR REPLACE PROCEDURE putStart
        (srId IN INTEGER, fileSystemId IN INTEGER,
         rdcId OUT INTEGER, rdcStatus OUT INTEGER,
         rdcPath OUT VARCHAR2) AS
BEGIN
 -- Get diskCopy Id
 SELECT diskCopy INTO rdcId FROM SubRequest WHERE SubRequest.id = srId;
 -- In case the DiskCopy was in WAITFS_SCHEDULING, PUT the
 -- waiting SubRequests in RESTART
 UPDATE SubRequest SET status = 1, lastModificationTime = getTime(), parent = 0 -- SUBREQUEST_RESTART
  WHERE parent = srId;
 -- link DiskCopy and FileSystem and update DiskCopyStatus
 UPDATE DiskCopy SET status = 6, -- DISKCOPY_STAGEOUT
                     fileSystem = fileSystemId
  WHERE id = rdcId
  RETURNING status, path
  INTO rdcStatus, rdcPath;
END;

CREATE OR REPLACE PROCEDURE putDoneStartProc
        (srId IN INTEGER, dcId OUT INTEGER,
         dcStatus OUT INTEGER, dcPath OUT VARCHAR2) AS
  reqId NUMBER;
  cfId NUMBER;
  ty NUMBER;
BEGIN
  -- Check there is no put going on
  -- If any, we'll wait on one of them
  SELECT request, castorFile INTO reqId, cfId
    FROM SubRequest WHERE id = srId;
  SELECT type INTO ty FROM Id2Type WHERE id = reqId;
  IF ty = 39 THEN -- PutDone
    DECLARE
      putSubReq NUMBER;
    BEGIN
      -- Try to find a running put
      SELECT subrequest.id INTO putSubReq
        FROM SubRequest, Id2Type
       WHERE SubRequest.castorfile = cfId
         AND SubRequest.request = Id2Type.id
         AND Id2Type.type = 40 -- Put
         AND SubRequest.status IN (0, 1, 2, 3) -- START, RESTART, RETRY, WAITSCHED
         AND ROWNUM < 2;
      -- we've found one, putDone will have to wait
      UPDATE SubRequest
         SET parent = putSubReq,
             status = 5, -- WAITSUBREQ
             lastModificationTime = getTime()
       WHERE id = srId;
      -- return an invalid diskcopy
      dcId := 0;
      dcStatus := 7; -- INVALID
      dcPath := '';
      RETURN;
    EXCEPTION WHEN NO_DATA_FOUND THEN
      -- no put waiting, let continue
      NULL;
    END;
  END IF;
  -- No put, we can safely go
  SELECT DiskCopy.id, DiskCopy.status, DiskCopy.path
    INTO dcId, dcStatus, dcPath
    FROM SubRequest, DiskCopy
   WHERE SubRequest.id = srId AND DiskCopy.castorfile = SubRequest.castorfile AND DiskCopy.status = 6; -- STAGEOUT
END;

/* PL/SQL method implementing updateAndCheckSubRequest */
CREATE OR REPLACE PROCEDURE updateAndCheckSubRequest(srId IN INTEGER, newStatus IN INTEGER, result OUT INTEGER) AS
  reqId INTEGER;
BEGIN
 -- Lock the access to the Request
 SELECT Id2Type.id INTO reqId
  FROM SubRequest, Id2Type
  WHERE SubRequest.id = srId
  AND Id2Type.id = SubRequest.request
  FOR UPDATE;
 -- Update Status
 UPDATE SubRequest SET status = newStatus,
                       answered = 1,
                       lastModificationTime = getTime() WHERE id = srId;
 IF newStatus = 6 THEN  -- READY
   UPDATE SubRequest SET getNextStatus = 1 -- GETNEXTSTATUS_FILESTAGED
    WHERE id = srId;
 END IF;
 -- Check whether it was the last subrequest in the request
 SELECT id INTO result FROM SubRequest
  WHERE request = reqId
    AND status IN (0, 1, 2, 3, 4, 5)   -- START, RESTART, RETRY, WAITSCHED, WAITTAPERECALL, WAITSUBREQ
    AND answered = 0
    AND ROWNUM < 2;
EXCEPTION WHEN NO_DATA_FOUND THEN -- No data found means we were last
  result := 0;
END;

/* PL/SQL method implementing disk2DiskCopyDone */
CREATE OR REPLACE PROCEDURE disk2DiskCopyDone
(dcId IN INTEGER, dcStatus IN INTEGER) AS
  srid INTEGER;
BEGIN
  -- update DiskCopy
  UPDATE DiskCopy set status = dcStatus WHERE id = dcId;
  -- update SubRequest
  UPDATE SubRequest set status = 6, -- SUBREQUEST_READY
                        getNextStatus = 1, -- GETNEXTSTATUS_FILESTAGED
                        lastModificationTime = getTime()
   WHERE diskCopy = dcId
  RETURNING id INTO srid;
  -- Wake up waiting subrequests
  UPDATE SubRequest SET status = 1, lastModificationTime = getTime(), parent = 0
   WHERE parent = srid; -- SUBREQUEST_RESTART
END;

/* PL/SQL method implementing recreateCastorFile */
CREATE OR REPLACE PROCEDURE recreateCastorFile(cfId IN INTEGER,
                                               srId IN INTEGER,
                                               dcId OUT INTEGER,
                                               rstatus OUT INTEGER,
                                               rmountPoint OUT VARCHAR2,
                                               rdiskServer OUT VARCHAR2) AS
  rpath VARCHAR2(2048);
  nbRes INTEGER;
  fid INTEGER;
  nh VARCHAR2(2048);
  unused INTEGER;
  contextPIPP INTEGER;
BEGIN
 -- Lock the access to the CastorFile
 -- This, together with triggers will avoid new TapeCopies
 -- or DiskCopies to be added
 SELECT id INTO unused FROM CastorFile WHERE id = cfId FOR UPDATE;
 -- Determine the context (Put inside PrepareToPut ?)
 BEGIN
   -- check that we are a Put
   SELECT StagePutRequest.id INTO unused
     FROM StagePutRequest, SubRequest
    WHERE SubRequest.id = srId
      AND StagePutRequest.id = SubRequest.request;
   BEGIN
     -- check that there is a PrepareToPut going on
     SELECT SubRequest.diskCopy INTO dcId
       FROM StagePrepareToPutRequest, SubRequest
      WHERE SubRequest.CastorFile = cfId
        AND StagePrepareToPutRequest.id = SubRequest.request
        AND SubRequest.status IN (0, 1, 2, 3, 4, 5, 6, 7, 10);  -- All but FINISHED, FAILED_FINISHED, ARCHIVED
     -- if we got here, we are a Put inside a PrepareToPut
     contextPIPP := 1;
   EXCEPTION WHEN TOO_MANY_ROWS THEN
     -- this means we are a PrepareToPut and another PrepareToPut
     -- is already running. This is forbidden
     archiveSubReq(srId);
     RAISE;
   END;   
 EXCEPTION WHEN NO_DATA_FOUND THEN
   contextPIPP := 0;
 END;
 IF contextPIPP = 0 THEN
  -- check if recreation is possible for TapeCopies
  SELECT count(*) INTO nbRes FROM TapeCopy
    WHERE status = 3 -- TAPECOPY_SELECTED
    AND castorFile = cfId;
  IF nbRes > 0 THEN
    -- We found something, thus we cannot recreate
    dcId := 0;
    COMMIT;
    RETURN;
  END IF;
  -- check if recreation is possible for DiskCopies
  SELECT count(*) INTO nbRes FROM DiskCopy
    WHERE status IN (1, 2, 5, 6) -- WAITDISK2DISKCOPY, WAITTAPERECALL, WAITFS, STAGEOUT
    AND castorFile = cfId;
  IF nbRes > 0 THEN
    -- We found something, thus we cannot recreate
    dcId := 0;
    COMMIT;
    RETURN;
  END IF;
  -- delete all tapeCopies
  DELETE from Stream2TapeCopy WHERE child IN
    (SELECT id FROM TapeCopy WHERE castorFile = cfId);
  DELETE FROM Id2Type WHERE id IN 
    (SELECT id FROM TapeCopy WHERE castorFile = cfId);
  DELETE from TapeCopy WHERE castorFile = cfId;
  -- set DiskCopies to INVALID
  UPDATE DiskCopy SET status = 7 -- INVALID
   WHERE castorFile = cfId AND status IN (0, 10); -- STAGED, CANBEMIGR
  -- create new DiskCopy
  SELECT fileId, nsHost INTO fid, nh FROM CastorFile WHERE id = cfId;
  SELECT ids_seq.nextval INTO dcId FROM DUAL;
  buildPathFromFileId(fid, nh, dcId, rpath);
  INSERT INTO DiskCopy (path, id, FileSystem, castorFile, status, creationTime)
   VALUES (rpath, dcId, 0, cfId, 5, getTime()); -- status WAITFS
  rstatus := 5; -- WAITFS
  rmountPoint := '';
  rdiskServer := '';
  INSERT INTO Id2Type (id, type) VALUES (dcId, 5); -- OBJ_DiskCopy
  COMMIT;
 ELSE
  DECLARE
   fsId INTEGER;
   dsId INTEGER;
  BEGIN
   -- Retrieve the infos about the DiskCopy to be used
   SELECT fileSystem, status INTO fsId, rstatus FROM DiskCopy WHERE id = dcId;
   -- retrieve mountpoint and filesystem if any
   if fsId = 0 THEN
     rmountPoint := '';
     rdiskServer := '';
   ELSE
     SELECT mountPoint, diskServer INTO rmountPoint, dsId
       FROM FileSystem WHERE FileSystem.id = fsId;
     SELECT name INTO rdiskServer FROM DiskServer WHERE id = dsId;
   END IF;
   -- See whether we should wait on the previous Put Request
   IF rstatus = 11 THEN -- WAITFS_SCHEDULING
    makeSubRequestWait(srId, dcId);
   END IF;
  END;
 END IF; 
 -- link SubRequest and DiskCopy
 UPDATE SubRequest SET diskCopy = dcId,
                       lastModificationTime = getTime() WHERE id = srId;
 COMMIT;
END;

/* PL/SQL method putDoneFunc */
CREATE OR REPLACE PROCEDURE putDoneFunc (cfId IN INTEGER,
                                         fs IN INTEGER,
					 context IN INTEGER) AS
  nc INTEGER;
  tcId INTEGER;
  fsId INTEGER;
  dcId INTEGER;
BEGIN
 -- get number of TapeCopies to create
 SELECT nbCopies INTO nc FROM FileClass, CastorFile
  WHERE CastorFile.id = cfId AND CastorFile.fileClass = FileClass.id;
 -- if no tape copy or 0 length file, no migration
 -- so we go directly to status STAGED
 IF nc = 0 OR fs = 0 THEN
   UPDATE DiskCopy SET status = 0 -- STAGED
    WHERE castorFile = cfId AND status = 6 -- STAGEOUT
   RETURNING fileSystem, id INTO fsId, dcId;
 ELSE
   -- update the DiskCopy status to CANBEMIGR
   UPDATE DiskCopy SET status = 10 -- CANBEMIGR
    WHERE castorFile = cfId AND status = 6 -- STAGEOUT
    RETURNING fileSystem INTO fsId;
   -- Create TapeCopies
   FOR i IN 1..nc LOOP
    INSERT INTO TapeCopy (id, copyNb, castorFile, status)
      VALUES (ids_seq.nextval, i, cfId, 0) -- TAPECOPY_CREATED
      RETURNING id INTO tcId;
    INSERT INTO Id2Type (id, type) VALUES (tcId, 30); -- OBJ_TapeCopy
   END LOOP;
   -- Restart any waiting Disk2DiskCopies
   UPDATE SubRequest -- SUBREQUEST_RESTART
      SET status = 1, lastModificationTime = getTime(), parent = 0
    WHERE status = 5 -- WAIT_SUBREQ
      AND parent IN (SELECT id from SubRequest WHERE diskCopy = dcId);
 END IF;
 -- If we are a real PutDone (and not a put outside of a prepareToPut)
 -- then we have to archive the original preapareToPut subRequest
 IF context = 2 THEN
   DECLARE
     srId NUMBER;
   BEGIN
     SELECT SubRequest.id INTO srId
       FROM SubRequest, StagePrepareToPutRequest
      WHERE SubRequest.castorFile = cfId
        AND SubRequest.request = StagePrepareToPutRequest.id;
     archiveSubReq(srId);
   END;
 END IF;
END;


/* PL/SQL method implementing prepareForMigration */
CREATE OR REPLACE PROCEDURE prepareForMigration (srId IN INTEGER,
                                                 fs IN INTEGER,
                                                 fId OUT NUMBER,
                                                 nh OUT VARCHAR2,
                                                 userId OUT INTEGER,
                                                 groupId OUT INTEGER) AS
  cfId INTEGER;
  fsId INTEGER;
  reservedSpace NUMBER;
  unused INTEGER;
  contextPIPP INTEGER;
BEGIN
 -- get CastorFile
 SELECT castorFile INTO cfId FROM SubRequest where id = srId;
 -- update CastorFile. This also takes a lock on it, insuring
 -- with triggers that we are the only ones to deal with its copies
 UPDATE CastorFile set fileSize = fs WHERE id = cfId
  RETURNING fileId, nsHost INTO fId, nh;
 -- Determine the context (Put inside PrepareToPut or not)
 BEGIN
   -- check that we are a Put
   SELECT StagePutRequest.id INTO unused
     FROM StagePutRequest, SubRequest
    WHERE SubRequest.id = srId
      AND StagePutRequest.id = SubRequest.request;
   BEGIN
     -- check that there is a PrepareToPut going on
     SELECT SubRequest.diskCopy INTO unused
       FROM StagePrepareToPutRequest, SubRequest
      WHERE SubRequest.CastorFile = cfId
        AND StagePrepareToPutRequest.id = SubRequest.request
        AND SubRequest.status IN (0, 1, 2, 3, 4, 5, 6, 7, 10);  -- All but FINISHED, FAILED_FINISHED, ARCHIVED
     -- if we got here, we are a Put inside a PrepareToPut
     contextPIPP := 0;
   EXCEPTION WHEN NO_DATA_FOUND THEN
     -- here we are a standalone Put
     contextPIPP := 1;
   END;
 EXCEPTION WHEN NO_DATA_FOUND THEN
   -- here we are a PutDone
   contextPIPP := 2;
 END;
 -- get uid, gid and reserved space from Request
 SELECT euid, egid, xsize INTO userId, groupId, reservedSpace FROM SubRequest,
     (SELECT euid, egid, id from StagePutRequest UNION ALL
      SELECT euid, egid, id from StagePutDoneRequest) Request
  WHERE SubRequest.request = Request.id AND SubRequest.id = srId;
 -- If not a put inside a PrepareToPut, update the FileSystem free space
 IF contextPIPP != 0 THEN
   SELECT fileSystem into fsId from DiskCopy
    WHERE castorFile = cfId AND status = 6;
   updateFsFileClosed(fsId, reservedSpace, fs);
 END IF;
 -- archive Subrequest
 archiveSubReq(srId);
 --  If not a put inside a PrepareToPut, create TapeCopies and update DiskCopy status
 IF contextPIPP != 0 THEN
   putDoneFunc(cfId, fs, contextPIPP);
 END IF;
 -- If put inside PrepareToPut, restart any PutDone currently
 -- waiting on this put
 IF contextPIPP = 0 THEN
   UPDATE SubRequest SET status = 1 -- RESTART
    WHERE id IN
     (SELECT SubRequest.id FROM SubRequest, Id2Type
       WHERE SubRequest.request = Id2Type.id
         AND Id2Type.type = 39       -- PutDone
         AND SubRequest.castorFile = cfId
         AND SubRequest.status = 5); -- WAITSUBREQ
 END IF;
 COMMIT;
END;

/* PL/SQL method implementing fileRecalled */
CREATE OR REPLACE PROCEDURE fileRecalled(tapecopyId IN INTEGER) AS
  SubRequestId NUMBER;
  dci NUMBER;
  fsId NUMBER;
  fileSize NUMBER;
  repackVid VARCHAR2(2048);
  cfid NUMBER;
BEGIN
  SELECT SubRequest.id, DiskCopy.id, CastorFile.filesize, SubRequest.repackVid, CastorFile.id
    INTO SubRequestId, dci, fileSize, repackVid, cfid
    FROM TapeCopy, SubRequest, DiskCopy, CastorFile
   WHERE TapeCopy.id = tapecopyId
     AND CastorFile.id = TapeCopy.castorFile
     AND DiskCopy.castorFile = TapeCopy.castorFile
     AND SubRequest.diskcopy(+) = DiskCopy.id
     AND DiskCopy.status = 2;
  UPDATE DiskCopy SET status = decode(repackVid, NULL,0, 6)  -- DISKCOPY_STAGEOUT if repackVid != NULL, else DISKCOPY_STAGED 
   WHERE id = dci RETURNING fileSystem INTO fsid;
  IF repackVid IS NOT NULL THEN
  	putDoneFunc(cfid, fsId, 0);
  END IF;
  IF subRequestId IS NOT NULL THEN
    UPDATE SubRequest SET status = 1, lastModificationTime = getTime(), parent = 0  -- SUBREQUEST_RESTART
     WHERE id = SubRequestId; 
    UPDATE SubRequest SET status = 1, lastModificationTime = getTime(), parent = 0  -- SUBREQUEST_RESTART
     WHERE parent = SubRequestId;
  END IF;
  updateFsFileClosed(fsId, fileSize, fileSize);
END;

/* PL/SQL method implementing selectCastorFile */
CREATE OR REPLACE PROCEDURE selectCastorFile (fId IN INTEGER,
                                              nh IN VARCHAR2,
                                              sc IN INTEGER,
                                              fc IN INTEGER,
                                              fs IN INTEGER,
                                              fn IN VARCHAR2,
                                              rid OUT INTEGER,
                                              rfs OUT INTEGER) AS
  CONSTRAINT_VIOLATED EXCEPTION;
  PRAGMA EXCEPTION_INIT(CONSTRAINT_VIOLATED, -1);
BEGIN
  BEGIN
    -- try to find an existing file
    SELECT id, fileSize INTO rid, rfs FROM CastorFile
      WHERE fileId = fid AND nsHost = nh;
    -- update lastAccess time
    UPDATE CastorFile SET LastAccessTime = getTime(),
                          nbAccesses = nbAccesses + 1,
                          lastKnownFileName = fn
      WHERE id = rid;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    -- insert new row
    INSERT INTO CastorFile (id, fileId, nsHost, svcClass, fileClass, fileSize,
                            creationTime, lastAccessTime, nbAccesses, lastKnownFileName)
      VALUES (ids_seq.nextval, fId, nh, sc, fc, fs, getTime(), getTime(), 1, fn)
      RETURNING id, fileSize INTO rid, rfs;
    INSERT INTO Id2Type (id, type) VALUES (rid, 2); -- OBJ_CastorFile
  END;
EXCEPTION WHEN CONSTRAINT_VIOLATED THEN
  -- retry the select since a creation was done in between
  SELECT id, fileSize INTO rid, rfs FROM CastorFile
    WHERE fileId = fid AND nsHost = nh;
END;

/* PL/SQL method implementing resetStream */
CREATE OR REPLACE PROCEDURE resetStream (sid IN INTEGER) AS
  nbRes NUMBER;
  unused Stream%ROWTYPE;
BEGIN
  BEGIN
    -- First lock the stream
    SELECT * INTO unused from Stream where id = sid FOR UPDATE;
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
    UPDATE Stream SET status = 0, tape = 0 WHERE id = sid; -- STREAM_PENDING
  EXCEPTION  WHEN NO_DATA_FOUND THEN
    -- We've found nothing, delete stream
    DELETE FROM Stream2TapeCopy WHERE Parent = sid;
    DELETE FROM Stream WHERE id = sid;
  END;
  -- in any case, unlink tape and stream
  UPDATE Tape SET Stream = 0 WHERE Stream = sid;
END;

/* PL/SQL method implementing bestFileSystemForJob */
CREATE OR REPLACE PROCEDURE bestFileSystemForJob (fileSystems IN castor."strList",
             machines IN castor."strList", minFree IN castor."cnumList",
             rMountPoint OUT VARCHAR2, rDiskServer OUT VARCHAR2) AS
 ds NUMBER;
 fs NUMBER;
 dev NUMBER;
 TYPE cursorContent IS RECORD
   (mountPoint VARCHAR2(2048), dsName VARCHAR2(2048),
    dsId NUMBER, fsId NUMBER, deviation NUMBER);
 TYPE AnyCursor IS REF CURSOR RETURN cursorContent;
 c1 AnyCursor;
 
 BEGIN
 IF fileSystems.COUNT > 0 THEN
  -- here machines AND filesystems should be given
  DECLARE
   fsIds "numList" := "numList"();
   nextIndex NUMBER := 1;
  BEGIN
   fsIds.EXTEND(fileSystems.COUNT);
   FOR i in fileSystems.FIRST .. fileSystems.LAST LOOP
    BEGIN
      SELECT FileSystem.id INTO fsIds(nextIndex)
        FROM FileSystem, DiskServer
       WHERE FileSystem.mountPoint = fileSystems(i)
         AND DiskServer.name = machines(i)
         AND FileSystem.diskServer = DiskServer.id
         AND FileSystem.free - FileSystem.reservedSpace + FileSystem.deltaFree - minFree(i) 
             > FileSystem.minAllowedFreeSpace * FileSystem.totalSize
         AND DiskServer.status = 0 -- DISKSERVER_PRODUCTION
         AND FileSystem.status = 0; -- FILESYSTEM_PRODUCTION
      nextIndex := nextIndex + 1;
    EXCEPTION  WHEN NO_DATA_FOUND THEN
      NULL;
    END;  
   END LOOP;
   OPEN c1 FOR
    SELECT FileSystem.mountPoint, Diskserver.name,
           DiskServer.id, FileSystem.id, FileSystem.fsDeviation
    FROM FileSystem, DiskServer
    WHERE FileSystem.diskserver = DiskServer.id
      AND FileSystem.id MEMBER OF fsIds
    ORDER by FileSystem.weight + FileSystem.deltaWeight DESC,
             FileSystem.fsDeviation ASC;
  END;
 ELSE
  -- No fileSystems given, there may still be machines given
  IF machines.COUNT > 0 THEN
   DECLARE
    mIds "numList" := "numList"();
    nextIndex NUMBER := 1;
   BEGIN
    mIds.EXTEND(machines.COUNT);
    FOR i in machines.FIRST .. machines.LAST LOOP
     BEGIN
      SELECT DiskServer.id INTO mIds(nextIndex)
        FROM DiskServer
       WHERE DiskServer.name = machines(i)
         AND DiskServer.status = 0; -- DISKSERVER_PRODUCTION
      nextIndex := nextIndex + 1;
     EXCEPTION  WHEN NO_DATA_FOUND THEN
      NULL;
     END;  
    END LOOP;
    OPEN c1 FOR
     SELECT FileSystem.mountPoint, Diskserver.name,
            DiskServer.id, FileSystem.id, FileSystem.fsDeviation
     FROM FileSystem, DiskServer
     WHERE FileSystem.diskserver = DiskServer.id
       AND DiskServer.id MEMBER OF mIds
       AND FileSystem.free - FileSystem.reservedSpace + FileSystem.deltaFree - minFree(1) 
           > FileSystem.minAllowedFreeSpace * FileSystem.totalSize
       AND FileSystem.status = 0 -- FILESYSTEM_PRODUCTION       
     ORDER by FileSystem.weight + FileSystem.deltaWeight DESC,
              FileSystem.fsDeviation ASC;
   END;
  ELSE
   OPEN c1 FOR
    SELECT FileSystem.mountPoint, Diskserver.name,
           DiskServer.id, FileSystem.id, FileSystem.fsDeviation
    FROM FileSystem, DiskServer
    WHERE FileSystem.diskserver = DiskServer.id
      AND FileSystem.free - FileSystem.reservedSpace + FileSystem.deltaFree - minFree(1) 
          > FileSystem.minAllowedFreeSpace * FileSystem.totalSize
      AND DiskServer.status = 0 -- DISKSERVER_PRODUCTION
      AND FileSystem.status = 0 -- FILESYSTEM_PRODUCTION
    ORDER by FileSystem.weight + FileSystem.deltaWeight DESC,
             FileSystem.fsDeviation ASC;
  END IF;
 END IF;
 FETCH c1 INTO rMountPoint, rDiskServer, ds, fs, dev;
 CLOSE c1;
END;

/* PL/SQL method implementing anySegmentsForTape */
CREATE OR REPLACE PROCEDURE anySegmentsForTape
(tapeId IN INTEGER, nb OUT INTEGER) AS
BEGIN
  SELECT count(*) INTO nb FROM Segment
  WHERE Segment.tape = tapeId
    AND Segment.status = 0;
  IF nb > 0 THEN
    UPDATE Tape SET status = 3 -- WAITMOUNT
    WHERE id = tapeId;
  END IF;
END;

/* PL/SQL method implementing segmentsForTape */
CREATE OR REPLACE PROCEDURE segmentsForTape
(tapeId IN INTEGER, segments OUT castor.Segment_Cur) AS
  segs "numList";
BEGIN
  SELECT Segment.id BULK COLLECT INTO segs FROM Segment
   WHERE Segment.tape = tapeId
     AND Segment.status = 0 FOR UPDATE;
  IF segs.COUNT > 0 THEN
    UPDATE Tape SET status = 4 -- MOUNTED
     WHERE id = tapeId;
    UPDATE Segment set status = 7 -- SELECTED
     WHERE id MEMBER OF segs;
  END IF;
  OPEN segments FOR SELECT * FROM Segment 
                     where id MEMBER OF segs;
END;

/* PL/SQL method implementing selectFiles2Delete */
CREATE OR REPLACE PROCEDURE updateFiles2Delete
(dcIds IN castor."cnumList") AS
BEGIN
  FOR i in dcIds.FIRST .. dcIds.LAST LOOP
    UPDATE DiskCopy set status = 9 -- BEING_DELETED
     WHERE id = i;
  END LOOP;
END;

/*
 * PL/SQL method implementing filesDeleted
 * Note that we don't increase the freespace of the fileSystem.
 * This is done by the monitoring daemon, that knows the
 * exact amount of free space. However, we decrease the
 * spaceToBeFreed counter so that a next GC knows the status
 * of the FileSystem
 * dcIds gives the list of diskcopies to delete.
 * fileIds returns the list of castor files to be removed
 * from the name server
 */
CREATE OR REPLACE PROCEDURE filesDeletedProc
(dcIds IN castor."cnumList",
 fileIds OUT castor.FileList_Cur) AS
  cfId NUMBER;
  fsId NUMBER;
  fsize NUMBER;
  nb NUMBER;
BEGIN
 IF dcIds.COUNT > 0 THEN
  -- Loop over the deleted files
  FOR i in dcIds.FIRST .. dcIds.LAST LOOP
    -- delete the DiskCopy
    DELETE FROM Id2Type WHERE id = dcIds(i);
    DELETE FROM DiskCopy WHERE id = dcIds(i)
      RETURNING castorFile, fileSystem INTO cfId, fsId;
    -- Lock the Castor File and retrieve size
    SELECT fileSize INTO fsize FROM CastorFile where id = cfID FOR UPDATE;
    -- update the FileSystem
    UPDATE FileSystem
       SET spaceToBeFreed = spaceToBeFreed - fsize
     WHERE id = fsId;
    -- See whether the castorfile has no other DiskCopy
    SELECT count(*) INTO nb FROM DiskCopy
     WHERE castorFile = cfId;
    -- If any DiskCopy, give up
    IF nb = 0 THEN
      -- See whether the castorfile has any TapeCopy
      SELECT count(*) INTO nb FROM TapeCopy
       WHERE castorFile = cfId;
      -- If any TapeCopy, give up
      IF nb = 0 THEN
        -- See whether the castorfile has any pending SubRequest
        SELECT count(*) INTO nb FROM SubRequest
         WHERE castorFile = cfId
           AND status IN (0, 1, 2, 3, 4, 5, 6, 7, 10);   -- All but FINISHED, FAILED_FINISHED, ARCHIVED
        -- If any SubRequest, give up
        IF nb = 0 THEN
          DECLARE
            fid NUMBER;
            fc NUMBER;
            nsh VARCHAR2(2048);
          BEGIN
            -- Delete the CastorFile
            DELETE FROM id2Type WHERE id = cfId;
            DELETE FROM CastorFile WHERE id = cfId
              RETURNING fileId, nsHost, fileClass
              INTO fid, nsh, fc;
            -- check whether this file potentially had TapeCopies
            SELECT nbCopies INTO nb FROM FileClass WHERE id = fc;
            IF nb = 0 THEN
              -- This castorfile was created with no TapeCopy
              -- So removing it from the stager means erasing
              -- it completely. We should thus also remove it
              -- from the name server
              INSERT INTO FilesDeletedProcOutput VALUES (fid, nsh);
            END IF;
          END;
        END IF;
      END IF;
    END IF;
  END LOOP;
 END IF;
 OPEN fileIds FOR SELECT * FROM FilesDeletedProcOutput;
END;

/*
 * PL/SQL method removing completely a file from the stager
 * including all its related objects (diskcopy, tapecopy, segments...)
 * The given files are supposed to already have been removed from the
 * name server
 * Note that we don't increase the freespace of the fileSystem.
 * This is done by the monitoring daemon, that knows the
 * exact amount of free space. However, we decrease the
 * spaceToBeFreed counter so that a next GC knows the status
 * of the FileSystem
 * fsIds gives the list of files to delete.
 */
CREATE OR REPLACE PROCEDURE filesClearedProc
(cfIds IN castor."cnumList") AS
  fsize NUMBER;
  fsid NUMBER;
BEGIN
  -- Loop over the deleted files
  FOR i in cfIds.FIRST .. cfIds.LAST LOOP
    -- Lock the Castor File and retrieve size
    SELECT fileSize INTO fsize FROM CastorFile WHERE id = cfIds(i);
    -- delete the DiskCopies
    FOR d in (SELECT id FROM Diskcopy WHERE castorfile = cfIds(i)) LOOP
      DELETE FROM Id2Type WHERE id = d.id;
      DELETE FROM DiskCopy WHERE id = d.id
        RETURNING fileSystem INTO fsId;
      -- update the FileSystem
      UPDATE FileSystem
         SET spaceToBeFreed = spaceToBeFreed - fsize
       WHERE id = fsId;
    END LOOP;
    -- put SubRequests into FAILED (for non FINISHED ONES)
    UPDATE SubRequest SET status = 7 WHERE castorfile = cfIds(i) AND status < 7;
    -- TapeCopy part
    FOR t IN (SELECT id FROM TapeCopy WHERE castorfile = cfIds(i)) LOOP
      FOR s IN (SELECT id FROM Segment WHERE copy = t.id) LOOP
        -- Delete the segment(s)
        DELETE FROM Id2Type WHERE id = s.id;
        DELETE FROM Segment WHERE id = s.id;
      END LOOP;
      -- Delete from Stream2TapeCopy
      DELETE FROM Stream2TapeCopy WHERE child = t.id;
      -- Delete the TapeCopy
      DELETE FROM Id2Type WHERE id = t.id;
      DELETE FROM TapeCopy WHERE id = t.id;
    END LOOP;
    DELETE FROM Id2Type WHERE id = cfIds(i);
    DELETE FROM CastorFile WHERE id = cfIds(i);
  END LOOP;
END;

/* PL/SQL method implementing filesDeleted */
CREATE OR REPLACE PROCEDURE filesDeletionFailedProc
(dcIds IN castor."cnumList") AS
  cfId NUMBER;
  fsId NUMBER;
  fsize NUMBER;
BEGIN
 IF dcIds.COUNT > 0 THEN
  -- Loop over the deleted files
  FOR i in dcIds.FIRST .. dcIds.LAST LOOP
    -- set status of DiskCopy to FAILED
    UPDATE DiskCopy SET status = 4 -- FAILED
     WHERE id = dcIds(i)
    RETURNING fileSystem, castorFile INTO fsId, cfId;
    -- Retrieve the file size
    SELECT fileSize INTO fsize FROM CastorFile where id = cfId;
    -- update the FileSystem
    UPDATE FileSystem
       SET spaceToBeFreed = spaceToBeFreed - fsize
     WHERE id = fsId;
  END LOOP;
 END IF;
END;

/* PL/SQL method implementing getUpdateDone */
CREATE OR REPLACE PROCEDURE getUpdateDoneProc
(subReqId IN NUMBER) AS
BEGIN
  archiveSubReq(subReqId);
END;

/* PL/SQL method implementing getUpdateFailed */
CREATE OR REPLACE PROCEDURE getUpdateFailedProc
(srId IN NUMBER) AS
BEGIN
  UPDATE SubRequest SET status = 7 -- FAILED
   WHERE id = srId;
END;

/* PL/SQL method implementing putFailedProc */
CREATE OR REPLACE PROCEDURE putFailedProc(srId IN NUMBER) AS
  dcId INTEGER;
  fsId INTEGER;
  cfId INTEGER;
  unused INTEGER;
  reservedSpace INTEGER;
BEGIN
  -- Set SubRequest in FAILED status
  UPDATE SubRequest
     SET status = 7 -- FAILED
   WHERE id = srId
  RETURNING diskCopy, xsize, castorFile
    INTO dcId, reservedSpace, cfId;
  SELECT fileSystem INTO fsId FROM DiskCopy WHERE id = dcId;
  -- free reserved space
  updateFsFileClosed(fsId, reservedSpace, 0);
  -- Determine the context (Put inside PrepareToPut ?)
  BEGIN
    -- check that there is a PrepareToPut going on
    SELECT SubRequest.diskCopy INTO unused
      FROM StagePrepareToPutRequest, SubRequest
     WHERE SubRequest.CastorFile = cfId
       AND StagePrepareToPutRequest.id = SubRequest.request;
    -- the select worked out, so we have a prepareToPut
    -- In such a case, we do not cleanup DiskCopy and CastorFile
  EXCEPTION WHEN NO_DATA_FOUND THEN
    -- this means we are a standalone put
    -- thus cleanup DiskCopy and CastorFile
    DELETE FROM DiskCopy WHERE id = dcId;
    DELETE FROM CastorFile WHERE id = cfId;
  END;   
END;

/* PL/SQL method implementing failedSegments */
CREATE OR REPLACE PROCEDURE failedSegments
(segments OUT castor.Segment_Cur) AS
BEGIN
  OPEN segments FOR SELECT * FROM Segment
                     WHERE Segment.status = 6; -- SEGMENT_FAILED
END;

/* PL/SQL method implementing stageRelease */
CREATE OR REPLACE PROCEDURE stageRelease (fid IN INTEGER,
                                          nh IN VARCHAR2,
                                          ret OUT INTEGER) AS
  cfId INTEGER;
  nbRes INTEGER;
BEGIN
 -- Lock the access to the CastorFile
 -- This, together with triggers will avoid new TapeCopies
 -- or DiskCopies to be added
 SELECT id INTO cfId FROM CastorFile
  WHERE fileId = fid AND nsHost = nh FOR UPDATE;
 -- check if removal is possible for TapeCopies
 SELECT count(*) INTO nbRes FROM TapeCopy
  WHERE status = 3 -- TAPECOPY_SELECTED
    AND castorFile = cfId;
 IF nbRes > 0 THEN
   -- We found something, thus we cannot recreate
   ret := 1;
   RETURN;
 END IF;
 -- check if recreation is possible for SubRequests
 SELECT count(*) INTO nbRes FROM SubRequest
  WHERE status != 11 AND castorFile = cfId;   -- ARCHIVED
 IF nbRes > 0 THEN
   -- We found something, thus we cannot recreate
   ret := 2;
   RETURN;
 END IF;
 -- set DiskCopies to GCCANDIDATE
 UPDATE DiskCopy SET status = 8 -- GCCANDIDATE
  WHERE castorFile = cfId AND status = 0; -- STAGED
 ret := 0;
END;

/* PL/SQL method implementing stageRm */
CREATE OR REPLACE PROCEDURE stageRm (fid IN INTEGER,
                                     nh IN VARCHAR2,
                                     ret OUT INTEGER) AS
  cfId INTEGER;
  nbRes INTEGER;
BEGIN
 -- Lock the access to the CastorFile
 -- This, together with triggers will avoid new TapeCopies
 -- or DiskCopies to be added
 SELECT id INTO cfId FROM CastorFile
  WHERE fileId = fid AND nsHost = nh FOR UPDATE;
 -- check if removal is possible for Migration
 SELECT count(*) INTO nbRes FROM TapeCopy
  WHERE status IN (0, 1, 2, 3) -- CREATED, TOBEMIGRATED, WAITINSTREAMS, SELECTED
    AND castorFile = cfId;
 IF nbRes > 0 THEN
   -- We found something, thus we cannot recreate
   ret := 1;
   RETURN;
 END IF;
 -- check if removal is possible for Disk2DiskCopy
 SELECT count(*) INTO nbRes FROM DiskCopy
  WHERE status = 1 -- DISKCOPY_WAITDISK2DISKCOPY
    AND castorFile = cfId;
 IF nbRes > 0 THEN
   -- We found something, thus we cannot recreate
   ret := 2;
   RETURN;
 END IF;
 -- mark all requests for the file as failed
 -- so the clients eventually get an answer
 FOR sr IN (SELECT id, status
              FROM SubRequest
             WHERE castorFile = cfId) LOOP
   IF sr.status IN (0, 1, 2, 3, 4, 5, 6, 7, 10) THEN  -- All but FINISHED, FAILED_FINISHED, ARCHIVED
     UPDATE SubRequest SET status = 7 WHERE id = sr.id;  -- FAILED
   END IF;
 END LOOP;
 -- set DiskCopies to GCCANDIDATE. Note that we keep
 -- WAITTAPERECALL diskcopies so that recalls can continue
 UPDATE DiskCopy SET status = 8 -- GCCANDIDATE
  WHERE castorFile = cfId
    AND status IN (0, 6, 10); -- STAGED, STAGEOUT, CANBEMIGR
 DECLARE
  segId INTEGER;
 BEGIN
   -- First lock all segments for the file
   SELECT segment.id INTO segId
     FROM Segment, TapeCopy
    WHERE TapeCopy.castorfile = cfId
      AND TapeCopy.id = Segment.copy
      FOR UPDATE;
   -- Check whether we have any segment in SELECTED
   SELECT segment.id INTO segId
     FROM Segment, TapeCopy
    WHERE TapeCopy.castorfile = cfId
      AND TapeCopy.id = Segment.copy
      AND Segment.status = 7 -- SELECTED
      AND ROWNUM < 2;
   -- Something is running, so give up
 EXCEPTION WHEN NO_DATA_FOUND THEN
   -- Nothing running
   FOR t IN (SELECT id FROM TapeCopy WHERE castorfile = cfId) LOOP
     FOR s IN (SELECT id FROM Segment WHERE copy = t.id) LOOP
       -- Delete the segment(s)
       DELETE FROM Id2Type WHERE id = s.id;
       DELETE FROM Segment WHERE id = s.id;
     END LOOP;
     -- Delete the TapeCopy
     DELETE FROM Id2Type WHERE id = t.id;
     DELETE FROM TapeCopy WHERE id = t.id;
   END LOOP;
   -- Delete the DiskCopies
   UPDATE DiskCopy
      SET status = 8  -- GCCANDIDATE
    WHERE status = 2  -- WAITTAPERECALL
      AND castorFile = cfId;
 END;
 ret := 0;
END;

CREATE OR REPLACE PACKAGE castorGC AS
  TYPE GCItem IS RECORD (dcId INTEGER, fileSize NUMBER, utility NUMBER);
  TYPE GCItem_Table is TABLE OF GCItem;
  TYPE GCItem_Cur IS REF CURSOR RETURN GCItem;
  --TYPE GCItem_CurTable is VARRAY(10) OF GCItem_Cur;
  TYPE policiesList IS TABLE OF VARCHAR2(2048);
END castorGC;

/*
 * GC policy that mimic the old GC :
 * a mix of oldest first and biggest first
 */
CREATE OR REPLACE FUNCTION defaultGCPolicy
(fsId INTEGER, garbageSize INTEGER)
  RETURN castorGC.GCItem_Cur AS
  result castorGC.GCItem_Cur;
BEGIN
  OPEN result FOR
    SELECT /*+ INDEX(CF) INDEX(DS) */ DS.id, CF.fileSize,
         CASE status
           WHEN 7 THEN 100000000000    -- INVALID, they go the very first
           WHEN 0 THEN getTime() - CF.lastAccessTime + greatest(0,86400*ln((CF.fileSize+1)/1024))
         END
     FROM DiskCopy DS, CastorFile CF
    WHERE CF.id = DS.castorFile
      AND DS.fileSystem = fsId
      AND NOT EXISTS (
        SELECT 'x' FROM SubRequest 
         WHERE DS.status = 0 AND diskcopy = DS.id 
           AND SubRequest.status IN (0, 1, 2, 3, 4, 5, 6, 7, 10))   -- All but FINISHED, FAILED_FINISHED, ARCHIVED
      AND DS.status in (0,7)
    ORDER BY 3 DESC;
  return result;
END;

/*
 * GC policy that mimic the old GC and takes into account
 * the number of accesses to the file. Namely we garbage
 * collect the oldest and biggest file that had the less
 * accesses but not 0, as a file having 0 accesses will
 * probably be read soon !
 */
CREATE OR REPLACE FUNCTION nopinGCPolicy
(fsId INTEGER, garbageSize INTEGER)
  RETURN castorGC.GCItem_Cur AS
  result castorGC.GCItem_Cur;
BEGIN
  OPEN result FOR
    SELECT DiskCopy.id, CastorFile.fileSize, 0
      FROM DiskCopy, CastorFile
     WHERE CastorFile.id = DiskCopy.castorFile
       AND DiskCopy.fileSystem = fsId
       AND DiskCopy.status = 7 -- INVALID
    UNION
    SELECT DiskCopy.id, CastorFile.fileSize,
           getTime() - CastorFile.LastAccessTime -- oldest first
           + GREATEST(0,86400*LN((CastorFile.fileSize+1)/1024)) -- biggest first
           + CASE CastorFile.nbAccesses
               WHEN 0 THEN 86400 -- non accessed last
               ELSE 20000 * CastorFile.nbAccesses -- most accessed last
             END
      FROM DiskCopy, CastorFile, SubRequest
     WHERE CastorFile.id = DiskCopy.castorFile
       AND DiskCopy.fileSystem = fsId
       AND DiskCopy.status = 0 -- STAGED
       AND DiskCopy.id = SubRequest.DiskCopy (+)
       AND Subrequest.id IS NULL
     ORDER BY 3 DESC;
  return result;
END;

/*
 * dummy GC policy for disk-only file systems
 */
CREATE OR REPLACE FUNCTION nullGCPolicy
(fsId INTEGER, garbageSize INTEGER)
  RETURN castorGC.GCItem_Cur AS
  result castorGC.GCItem_Cur;
  badValue NUMBER;
BEGIN
  badValue := 2**31;
  OPEN result FOR
    SELECT 0, -1, badValue
      FROM Dual;
  return result;
END;

/*
 * Runs Garbage collection on the specified FileSystem
 */
CREATE OR REPLACE PROCEDURE garbageCollectFS(fsId INTEGER) AS
  toBeFreed INTEGER;
  freed INTEGER := 0;
  dpID INTEGER;
  policies castorGC.policiesList;
  --cursors castor.castorGC.GCItem_CurTable;
  cur castorGC.GCItem_Cur;
  cur1 castorGC.GCItem_Cur;
  cur2 castorGC.GCItem_Cur;
  cur3 castorGC.GCItem_Cur;
  nextItems castorGC.GCItem_Table := castorGC.GCItem_Table();
  bestCandidate INTEGER;
  bestValue NUMBER;
BEGIN
  -- Get the DiskPool and the minFree space we want to achieve
  SELECT diskPool, maxFreeSpace * totalSize - free - deltaFree + reservedSpace - spaceToBeFreed
    INTO dpId, toBeFreed
    FROM FileSystem
   WHERE FileSystem.id = fsId
     FOR UPDATE;
  -- Don't do anything if toBeFreed <= 0
  IF toBeFreed <= 0 THEN
    COMMIT;
    RETURN;
  END IF;
  UPDATE FileSystem
     SET spaceToBeFreed = spaceToBeFreed + toBeFreed
   WHERE FileSystem.id = fsId;
  -- release the filesystem lock so that other threads can go on
  COMMIT;
  -- List policies to be applied
  SELECT CASE
          WHEN svcClass.gcPolicy IS NULL THEN 'defaultGCPolicy'
          ELSE svcClass.gcPolicy
         END BULK COLLECT INTO policies
    FROM SvcClass, DiskPool2SvcClass
   WHERE DiskPool2SvcClass.Parent = dpId
     AND SvcClass.Id = DiskPool2SvcClass.Child;
  -- Get candidates for each policy
  nextItems.EXTEND(policies.COUNT);
  bestCandidate := -1;
  bestValue := 2**31;
  IF policies.COUNT > 0 THEN
    EXECUTE IMMEDIATE 'BEGIN :1 := '||policies(policies.FIRST)||'(:2, :3); END;'
      USING OUT cur1, IN fsId, IN toBeFreed;
    FETCH cur1 INTO nextItems(1);
    IF policies.COUNT > 1 THEN
      EXECUTE IMMEDIATE 'BEGIN :1 := '||policies(policies.FIRST+1)||'(:2, :3); END;'
        USING OUT cur2, IN fsId, IN toBeFreed;
      FETCH cur2 INTO nextItems(2);
      IF policies.COUNT > 2 THEN
        EXECUTE IMMEDIATE 'BEGIN :1 := '||policies(policies.FIRST+2)||'(:2, :3); END;'
          USING OUT cur3, IN fsId, IN toBeFreed;
        FETCH cur3 INTO nextItems(3);
      END IF;    
    END IF;    
  END IF;    
  FOR i IN policies.FIRST .. policies.LAST LOOP
    -- maximum 3 policies allowed
    IF i > policies.FIRST+2 THEN EXIT; END IF;
    IF nextItems(i).utility < bestValue THEN
      bestCandidate := i;
      bestValue := nextItems(i).utility;
    END IF;
  END LOOP;
  -- if no candidate has been found (this can happen with the null GC policy) just give up
  IF bestCandidate = -1 THEN
    -- update Filesystem toBeFreed space back to the previous value
    UPDATE FileSystem
       SET spaceToBeFreed = spaceToBeFreed - toBeFreed
     WHERE FileSystem.id = fsId;
    COMMIT;
    RETURN;
  END IF;
  -- Now extract the diskcopies that will be garbaged and
  -- mark them GCCandidate
  LOOP
    -- Mark the DiskCopy
    UPDATE DISKCOPY SET status = 8 -- GCCANDIDATE
     WHERE id = nextItems(bestCandidate).dcId;
    IF 1 = bestCandidate THEN
      cur := cur1;
    ELSIF 2 = bestCandidate THEN
      cur := cur2;
    ELSE
      cur := cur3;
    END IF;
    FETCH cur INTO nextItems(bestCandidate);
    EXIT WHEN cur%NOTFOUND;
    -- Update toBeFreed
    freed := freed + nextItems(bestCandidate).fileSize;
    -- Shall we continue ?
    IF toBeFreed > freed THEN
      -- find next candidate
      bestValue := 2**31;
      FOR i IN nextItems.FIRST .. nextItems.LAST LOOP
        IF nextItems(i).utility < bestValue THEN
          bestCandidate := i;
          bestValue := nextItems(i).utility;
        END IF;
      END LOOP;
    ELSE
      -- enough space freed
      EXIT;
    END IF;
  END LOOP;
  -- Update Filesystem toBeFreed space to the exact value
  UPDATE FileSystem
     SET spaceToBeFreed = spaceToBeFreed + freed - toBeFreed
   WHERE FileSystem.id = fsId;
  -- Close cursors
  IF policies.COUNT > 0 THEN
    CLOSE cur1;
    IF policies.COUNT > 1 THEN
      CLOSE cur2;
      IF policies.COUNT > 2 THEN
        CLOSE cur3;
      END IF;
    END IF;
  END IF;
  -- commit everything
  COMMIT;
END;

/*
 * Runs only default policy garbage collection on the specified FileSystem
 *
CREATE PROCEDURE defGarbageCollectFS(fsId INTEGER) AS
  toBeFreed INTEGER;
  freed INTEGER := 0;
  dpID INTEGER;
  cur castorGC.GCItem_Cur;
  nextItem castorGC.GCItem;
BEGIN
  -- Get the DiskPool and the minFree space we want to achieve
  SELECT diskPool, maxFreeSpace * totalSize - free - deltaFree + reservedSpace - spaceToBeFreed
    INTO dpId, toBeFreed
    FROM FileSystem
   WHERE FileSystem.id = fsId
     FOR UPDATE;
  -- Don't do anything if toBeFreed <= 0
  IF toBeFreed <= 0 THEN
    COMMIT;
    RETURN;
  END IF;
  UPDATE FileSystem
     SET spaceToBeFreed = spaceToBeFreed + toBeFreed
   WHERE FileSystem.id = fsId;
  -- release the filesystem lock so that other threads can go on
  COMMIT;
  -- policy to be applied defaults to defaultGCPolicy
  cur := defaultGCPolicy(fsId, toBeFreed);
  -- Now extract the diskcopies that will be garbaged and
  -- mark them GCCandidate
  LOOP
    FETCH cur INTO nextItem;
    EXIT WHEN cur%NOTFOUND;
    -- Mark the DiskCopy
    UPDATE DISKCOPY SET status = 8 -- GCCANDIDATE
     WHERE id = nextItem.dcId;
    -- Update toBeFreed
    freed := freed + nextItem.fileSize;
    -- Shall we continue ?
    IF freed > toBeFreed THEN
      -- enough space freed
      EXIT;
    END IF;
  END LOOP;
  -- Update Filesystem toBeFreed space to the exact value
  UPDATE FileSystem
     SET spaceToBeFreed = spaceToBeFreed + freed - toBeFreed
   WHERE FileSystem.id = fsId;
  -- Close cursor
  CLOSE cur;
  -- commit everything
  COMMIT;
END;
*/


/* This table keeps the current queue of the garbage collector:
 * whenever a new filesystem has to be garbage collected, a row is inserted
 * in this table and a db job runs on it to actually execute the gc.
 * In low load conditions this table will be empty and the db job is fired
 * for each new entered filesystem. In high load conditions a single db job
 * will keep running and no other jobs will be fired */
CREATE TABLE FileSystemGC (fsid NUMBER PRIMARY KEY, submissionTime NUMBER);

/*
 * Runs Garbage collection anywhere needed.
 * This is fired as a DBMS JOB from the FS trigger.
 */
CREATE OR REPLACE PROCEDURE garbageCollect AS
  fs NUMBER;
BEGIN
  LOOP
    -- get the oldest FileSystem to be garbage collected
    SELECT fsid INTO fs FROM
      (SELECT fsid FROM FileSystemGC ORDER BY submissionTime ASC)
    WHERE ROWNUM < 2;
    garbageCollectFS(fs);
    DELETE FROM FileSystemGC WHERE fsid = fs;
    -- yield to other jobs/transactions
    DBMS_LOCK.sleep(seconds => 1.0);
  END LOOP;
  EXCEPTION
    WHEN NO_DATA_FOUND THEN NULL;  -- terminate the job
END;

/*
 * Trigger launching garbage collection whenever needed
 * Note that we only launch it when at least 5 gigs are to be deleted
 */
CREATE OR REPLACE TRIGGER tr_FileSystem_Update
AFTER UPDATE OF free, deltaFree, reservedSpace ON FileSystem
FOR EACH ROW
DECLARE
  freeSpace NUMBER;
  jobid NUMBER;
  gccount INTEGER;
  CONSTRAINT_VIOLATED EXCEPTION;
  PRAGMA EXCEPTION_INIT(CONSTRAINT_VIOLATED, -1);
BEGIN
  -- compute the actual free space taking into account reservations (reservedSpace)
  -- and already running GC processes (spaceToBeFreed)
  freeSpace := :new.free + :new.deltaFree - :new.reservedSpace + :new.spaceToBeFreed;
  -- shall we launch a new GC?
  IF :new.minFreeSpace * :new.totalSize  > freeSpace AND
     -- is it really worth launching it? (some other GCs maybe are already running
     -- so we accept it only if it will free more than 5 Gb)
     :new.maxFreeSpace * :new.totalSize > freeSpace + 5000000000 THEN
    -- ok, we queue this filesystem for being garbage collected
    SELECT count(*) INTO gccount FROM FileSystemGC;
    INSERT INTO FileSystemGC VALUES (:new.id, getTime());
    -- is it the only filesystem waiting for GC?
    IF gccount = 0 THEN
      -- we spawn a job to do the real work. This avoids mutating table error
      -- and ensures that the current update does not fail if GC fails
      DBMS_JOB.SUBMIT(jobid,'garbageCollect();');
    END IF;
    -- otherwise, a job is already running and will take over this file system too
    -- (the check is not thread-safe, no problem as the only effect
    -- could be to spawn more than one job)
  END IF;
  EXCEPTION
    -- the filesystem was already selected for GC, do nothing
    WHEN CONSTRAINT_VIOLATED THEN NULL;
END;


/*
 * PL/SQL method implementing the core part of stage queries
 * It takes a list of castorfile ids as input
 */
CREATE OR REPLACE PROCEDURE internalStageQuery
 (cfs IN "numList",
  svcClassId IN NUMBER,
  result OUT castor.QueryLine_Cur) AS
BEGIN
  OPEN result FOR
    -- Here we get the status for each cf as follows: if a valid diskCopy is found,
    -- its status is returned, else if a (prepareTo)Get request is found and no diskCopy is there,
    -- WAITTAPERECALL is returned, else -1 (INVALID) is returned
    SELECT /*+ INDEX (CastorFile) INDEX (DiskCopy) INDEX (FileSystem) INDEX (DiskServer) INDEX (SubRequest) */
           -- we need to give these hints to the optimizer otherwise it goes for a full table scan (!)
           UNIQUE castorfile.fileid, castorfile.nshost, DiskCopy.id,
           DiskCopy.path, CastorFile.filesize,
           nvl(DiskCopy.status, decode(SubRequest.status, 0,2, 3,2, -1)), 
               -- SubRequest in status 0,3 (START,WAITSCHED) => 2 = DISKCOPY_WAITTAPERECALL is returned
           DiskServer.name, FileSystem.mountPoint,
           CastorFile.nbaccesses, CastorFile.lastKnownFileName
      FROM CastorFile, DiskCopy, FileSystem, DiskServer,
           DiskPool2SvcClass, SubRequest,
           (SELECT id, svcClass FROM StagePrepareToGetRequest UNION ALL
            SELECT id, svcClass FROM StageGetRequest) Req
     WHERE CastorFile.id IN (SELECT * FROM TABLE(cfs))
       AND CastorFile.id = DiskCopy.castorFile(+)    -- search for valid diskcopy
       AND FileSystem.id(+) = DiskCopy.fileSystem
       AND nvl(FileSystem.status, 0) = 0 -- PRODUCTION
       AND DiskServer.id(+) = FileSystem.diskServer
       AND nvl(DiskServer.status, 0) = 0 -- PRODUCTION
       AND DiskPool2SvcClass.parent(+) = FileSystem.diskPool 
       AND CastorFile.id = SubRequest.castorFile(+)  -- or search for a get request
       AND SubRequest.request = Req.id(+)
       AND (svcClassId = 0                  -- no svcClass given
         OR DiskPool2SvcClass.child = svcClassId    -- found diskcopy on the given svcClass
         OR ((DiskCopy.fileSystem = 0       -- diskcopy not yet associated with filesystem...
             OR DiskCopy.id IS NULL)        -- or diskcopy not yet created at all...
           AND Req.svcClass = svcClassId))    -- ...but found stagein request
  ORDER BY fileid, nshost;
END;

/*
 * PL/SQL method implementing the core part of stage queries full version (for admins)
 * It takes a list of castorfile ids as input
 *
CREATE PROCEDURE internalFullStageQuery
 (cfs IN "numList",
  svcClassId IN NUMBER,
  result OUT castor.QueryLine_Cur) AS
BEGIN
 OPEN result FOR
    SELECT UNIQUE castorfile.fileid, castorfile.nshost, DiskCopy.id,
           DiskCopy.path, CastorFile.filesize,
           nvl(DiskCopy.status, -1), nvl(tpseg.tstatus, -1),
           nvl(tpseg.sstatus, -1), DiskServer.name,
           FileSystem.mountPoint
      FROM CastorFile, DiskCopy,
           -- Tape and Segment part
           (SELECT tapecopy.castorfile, tapecopy.id,
                   tapecopy.status AS tstatus, segment.status AS sstatus
              FROM tapecopy, segment
             WHERE tapecopy.id = segment.copy (+)) tpseg,
           FileSystem, DiskServer, DiskPool2SvcClass
     WHERE CastorFile.id IN (SELECT * FROM TABLE(cfs))
       AND CastorFile.id = DiskCopy.castorFile (+)
       AND castorfile.id = tpseg.castorfile(+)
       AND FileSystem.id(+) = DiskCopy.fileSystem
       AND nvl(FileSystem.status,0) = 0 -- PRODUCTION
       AND DiskServer.id(+) = FileSystem.diskServer
       AND nvl(DiskServer.status,0) = 0 -- PRODUCTION
       AND DiskPool2SvcClass.parent(+) = FileSystem.diskPool
       AND (DiskPool2SvcClass.child = svcClassId OR DiskPool2SvcClass.child IS NULL)
  ORDER BY fileid, nshost;
END;
*/

/*
 * PL/SQL method implementing the stager_qry based on file name
 */
CREATE OR REPLACE PROCEDURE fileNameStageQuery
 (fn IN VARCHAR2,
  svcClassId IN INTEGER,
  maxNbResponses IN INTEGER,
  result OUT castor.QueryLine_Cur) AS
  cfs "numList";
BEGIN
 IF substr(fn, 1, 7) = 'regexp:' THEN  -- posix-syle regular expressions
   SELECT id BULK COLLECT INTO cfs
     FROM CastorFile
    WHERE REGEXP_LIKE(lastKnownFileName,substr(fn, 8)) 
      AND ROWNUM <= maxNbResponses + 1;
 ELSIF substr(fn, -1, 1) = '/' THEN    -- files in a 'subdirectory'
   SELECT /*+ INDEX(CastorFile I_CastorFile_LastKnownFileName) */ id BULK COLLECT INTO cfs
     FROM CastorFile 
    WHERE lastKnownFileName LIKE fn||'%'
      AND ROWNUM <= maxNbResponses + 1;
 ELSE                                  -- exact match
   SELECT /*+ INDEX(CastorFile I_CastorFile_LastKnownFileName) */ id BULK COLLECT INTO cfs
     FROM CastorFile 
    WHERE lastKnownFileName = fn;
 END IF;
 IF cfs.COUNT > maxNbResponses THEN
   -- We have too many rows, we just give up
   raise_application_error(-20102, 'Too many matching files');
 END IF;
 internalStageQuery(cfs, svcClassId, result);
END;

/*
 * PL/SQL method implementing the stager_qry based on file id
 */
CREATE OR REPLACE PROCEDURE fileIdStageQuery
 (fid IN NUMBER,
  nh IN VARCHAR2,
  svcClassId IN INTEGER,
  result OUT castor.QueryLine_Cur) AS
  cfs "numList";
BEGIN
 SELECT id BULK COLLECT INTO cfs FROM CastorFile WHERE fileId = fid AND nshost = nh;
 internalStageQuery(cfs, svcClassId, result);
END;

/*
 * PL/SQL method implementing the stager_qry based on request id
 */
CREATE OR REPLACE PROCEDURE reqIdStageQuery
 (rid IN VARCHAR2,
  svcClassId IN INTEGER,
  notfound OUT INTEGER,
  result OUT castor.QueryLine_Cur) AS
  cfs "numList";
BEGIN
  SELECT sr.castorfile BULK COLLECT INTO cfs
    FROM SubRequest sr,
         (SELECT id
            FROM StagePreparetogetRequest
           WHERE reqid LIKE rid
          UNION ALL
          SELECT id
            FROM StagePreparetoputRequest
           WHERE reqid LIKE rid
          UNION ALL
          SELECT id
            FROM StagePreparetoupdateRequest
           WHERE reqid LIKE rid
          UNION ALL
          SELECT id
            FROM stageGetRequest
           WHERE reqid LIKE rid
          UNION ALL
          SELECT id
            FROM stagePutRequest
           WHERE reqid LIKE rid) reqlist
   WHERE sr.request = reqlist.id;
  IF cfs.COUNT > 0 THEN
    internalStageQuery(cfs, svcClassId, result);
  ELSE
    notfound := 1;
  END IF;
END;

/*
 * PL/SQL method implementing the stager_qry based on user tag
 */
CREATE OR REPLACE PROCEDURE userTagStageQuery
 (tag IN VARCHAR2,
  svcClassId IN INTEGER,
  notfound OUT INTEGER,
  result OUT castor.QueryLine_Cur) AS
  cfs "numList";
BEGIN
  SELECT sr.castorfile BULK COLLECT INTO cfs
    FROM SubRequest sr,
         (SELECT id
            FROM StagePreparetogetRequest
           WHERE userTag LIKE tag
          UNION ALL
          SELECT id
            FROM StagePreparetoputRequest
           WHERE userTag LIKE tag
          UNION ALL
          SELECT id
            FROM StagePreparetoupdateRequest
           WHERE userTag LIKE tag
          UNION ALL
          SELECT id
            FROM stageGetRequest
           WHERE userTag LIKE tag
          UNION ALL
          SELECT id
            FROM stagePutRequest
           WHERE userTag LIKE tag) reqlist
   WHERE sr.request = reqlist.id;
  IF cfs.COUNT > 0 THEN
    internalStageQuery(cfs, svcClassId, result);
  ELSE
    notfound := 1;
  END IF;
END;

/*
 * PL/SQL method implementing the LastRecalls stager_qry based on request id
 */
CREATE OR REPLACE PROCEDURE reqIdLastRecallsStageQuery
 (rid IN VARCHAR2,
  svcClassId IN INTEGER,
  notfound OUT INTEGER,
  result OUT castor.QueryLine_Cur) AS
  cfs "numList";
  reqs "numList";
BEGIN
  SELECT id BULK COLLECT INTO reqs
    FROM (SELECT id
            FROM StagePreparetogetRequest
           WHERE reqid LIKE rid
          UNION ALL
          SELECT id
            FROM StagePreparetoupdateRequest
           WHERE reqid LIKE rid
          );
  IF reqs.COUNT > 0 THEN
    UPDATE SubRequest
       SET getNextStatus = 2 -- GETNEXTSTATUS_NOTIFIED
     WHERE decode(getNextStatus,1,getNextStatus,NULL) = 1 -- GETNEXTSTATUS_FILESTAGED
       AND request IN (SELECT * FROM TABLE(reqs))
    RETURNING castorfile BULK COLLECT INTO cfs;
    internalStageQuery(cfs, svcClassId, result);
  ELSE
    notfound := 1;
  END IF;
END;

/*
 * PL/SQL method implementing the LastRecalls stager_qry based on user tag
 */
CREATE OR REPLACE PROCEDURE userTagLastRecallsStageQuery
 (tag IN VARCHAR2,
  svcClassId IN INTEGER,
  notfound OUT INTEGER,
  result OUT castor.QueryLine_Cur) AS
  cfs "numList";
  reqs "numList";
BEGIN
  SELECT id BULK COLLECT INTO reqs
    FROM (SELECT id
            FROM StagePreparetogetRequest
           WHERE userTag LIKE tag
          UNION ALL
          SELECT id
            FROM StagePreparetoupdateRequest
           WHERE userTag LIKE tag
          );
  IF reqs.COUNT > 0 THEN
    UPDATE SubRequest
       SET getNextStatus = 2 -- GETNEXTSTATUS_NOTIFIED
     WHERE decode(getNextStatus,1,getNextStatus,NULL) = 1 -- GETNEXTSTATUS_FILESTAGED
       AND request IN (SELECT * FROM TABLE(reqs))
    RETURNING castorfile BULK COLLECT INTO cfs;
    internalStageQuery(cfs, svcClassId, result);
  ELSE
    notfound := 1;
  END IF;
END;



/* PL/SQL code for castorVdqm package */
CREATE OR REPLACE PACKAGE castorVdqm AS
  TYPE Drive2Req IS RECORD (
        tapeDrive NUMBER,
        tapeRequest NUMBER);
  TYPE Drive2Req_Cur IS REF CURSOR RETURN Drive2Req;
	TYPE TapeDrive_Cur IS REF CURSOR RETURN TapeDrive%ROWTYPE;
	TYPE TapeRequest_Cur IS REF CURSOR RETURN TapeRequest%ROWTYPE;
END castorVdqm;


/**
 * PL/SQL method to dedicate a tape to a tape drive.
 * First it checks the preconditions that a tapeDrive must meet in order to be
 * assigned. The couples (drive,requests) are then orderd by the priorityLevel 
 * and by the modification time and processed one by one to verify
 * if any dedication exists and has to be applied.
 * Returns the relevant IDs if a couple was found, (0,0) otherwise.
 */  
CREATE OR REPLACE PROCEDURE matchTape2TapeDrive
 (tapeDriveID OUT NUMBER, tapeRequestID OUT NUMBER) AS
  d2rCur castorVdqm.Drive2Req_Cur;
  d2r castorVdqm.Drive2Req;
  countDed INTEGER;
BEGIN
  tapeDriveID := 0;
  tapeRequestID := 0;
  
  -- Check all preconditions a tape drive must meet in order to be used by pending tape requests
  OPEN d2rCur FOR
  SELECT TapeDrive.id, TapeRequest.id
    FROM TapeDrive, TapeRequest, TapeDrive2TapeDriveComp, TapeDriveCompatibility, TapeServer
   WHERE TapeDrive.status = 0  -- UNIT_UP
     AND TapeDrive.runningTapeReq = 0  -- not associated with tapeReq
     AND TapeDrive.tapeServer = TapeServer.id 
     AND TapeServer.actingMode = 0  -- ACTIVE
     AND TapeRequest.tapeDrive = 0
     AND (TapeRequest.requestedSrv = TapeServer.id OR TapeRequest.requestedSrv = 0)
     AND TapeDrive2TapeDriveComp.parent = TapeDrive.id 
     AND TapeDrive2TapeDriveComp.child = TapeDriveCompatibility.id 
     AND TapeDriveCompatibility.tapeAccessSpecification = TapeRequest.tapeAccessSpecification
     AND TapeDrive.deviceGroupName = TapeRequest.deviceGroupName
     /*
     AND TapeDrive.deviceGroupName = tapeDriveDgn.id 
     AND TapeRequest.deviceGroupName = tapeRequestDgn.id 
     AND tapeDriveDgn.libraryName = tapeRequestDgn.libraryName 
         -- in case we want to match by libraryName only
     */
     ORDER BY TapeDriveCompatibility.priorityLevel ASC, 
              TapeRequest.modificationTime ASC;

  LOOP
    -- For each candidate couple, verify that the dedications (if any) are met
    FETCH d2rCur INTO d2r;
    EXIT WHEN d2rCur%NOTFOUND;

    SELECT count(*) INTO countDed
      FROM TapeDriveDedication
     WHERE tapeDrive = d2r.tapeDrive
       AND getTime() BETWEEN startTime AND endTime;
    IF countDed = 0 THEN    -- no dedications valid for this TapeDrive
      tapeDriveID := d2r.tapeDrive;   -- fine, we can assign it
      tapeRequestID := d2r.tapeRequest;
      EXIT;
    END IF;

    -- We must check if the request matches the dedications for this tape drive
    SELECT count(*) INTO countDed
      FROM TapeDriveDedication tdd, Tape, TapeRequest, ClientIdentification, 
           TapeAccessSpecification, TapeDrive
     WHERE tdd.tapeDrive = d2r.tapeDrive
       AND getTime() BETWEEN startTime AND endTime
       AND tdd.clientHost(+) = ClientIdentification.machine
       AND tdd.euid(+) = ClientIdentification.euid
       AND tdd.egid(+) = ClientIdentification.egid
       AND tdd.vid(+) = Tape.vid
       AND tdd.accessMode(+) = TapeAccessSpecification.accessMode
       AND TapeRequest.id = d2r.tapeRequest
       AND TapeRequest.tape = Tape.id
       AND TapeRequest.tapeAccessSpecification = TapeAccessSpecification.id
       AND TapeRequest.client = ClientIdentification.id;
    IF countDed > 0 THEN  -- there's a matching dedication for at least a criterium
      tapeDriveID := d2r.tapeDrive;   -- fine, we can assign it
      tapeRequestID := d2r.tapeRequest;
      EXIT;
    END IF;
    -- else the tape drive is dedicated to other request(s) and we can't use it, go on
  END LOOP;
  -- if the loop has been fully completed without assignment,
  -- no free tape drive has been found. 
  CLOSE d2rCur;
END;


/**
 * This is the main select statement to dedicate a tape to a tape drive.
 * It respects the old and of course the new way to select a tape for a 
 * tape drive.
 * The old way is to look, if the tapeDrive and the tapeRequest have the same
 * dgn.
 * The new way is to look if the TapeAccessSpecification can be served by a 
 * specific tapeDrive. The tape Request are then orderd by the priorityLevel (for 
 * the new way) and by the modification time.
 * Returns 1 if a couple was found, 0 otherwise.
 *
CREATE PROCEDURE matchTape2TapeDrive
 (tapeDriveID OUT NUMBER, tapeRequestID OUT NUMBER) AS
BEGIN
  SELECT TapeDrive.id, TapeRequest.id INTO tapeDriveID, tapeRequestID
    FROM TapeDrive, TapeRequest, TapeServer, DeviceGroupName tapeDriveDgn, DeviceGroupName tapeRequestDgn
    WHERE TapeDrive.status=0
          AND TapeDrive.runningTapeReq=0
          AND TapeDrive.tapeServer=TapeServer.id
          AND TapeRequest.tape NOT IN (SELECT tape FROM TapeDrive WHERE status!=0)
          AND TapeServer.actingMode=0
          AND TapeRequest.tapeDrive IS NULL
          AND (TapeRequest.requestedSrv=TapeDrive.tapeServer OR TapeRequest.requestedSrv=0)
          AND (TapeDrive.deviceGroupName=TapeRequest.deviceGroupName)
          AND rownum < 2
          ORDER BY TapeRequest.modificationTime ASC; 
  EXCEPTION
    WHEN NO_DATA_FOUND THEN
    tapeDriveID := 0;
    tapeRequestID := 0;
END;
*/


/*
 * PL/SQL method implementing the select from tapereq queue
 */
CREATE OR REPLACE PROCEDURE selectTapeRequestQueue
 (dgn IN VARCHAR2, server IN VARCHAR2, tapeRequests OUT castorVdqm.TapeRequest_Cur) AS
BEGIN
  IF dgn IS NULL AND server IS NULL THEN
    OPEN tapeRequests FOR SELECT * FROM TapeRequest
		  ORDER BY TapeRequest.modificationTime ASC;
  ELSIF dgn IS NULL THEN
    OPEN tapeRequests FOR SELECT TapeRequest.* FROM TapeRequest, TapeServer
      WHERE TapeServer.serverName = server
            AND TapeServer.id = TapeRequest.requestedSrv
			ORDER BY TapeRequest.modificationTime ASC;
  ELSIF server IS NULL THEN
    OPEN tapeRequests FOR SELECT TapeRequest.* FROM TapeRequest, DeviceGroupName
      WHERE DeviceGroupName.dgName = dgn
            AND DeviceGroupName.id = TapeRequest.deviceGroupName
			ORDER BY TapeRequest.modificationTime ASC;
  ELSE 
    OPEN tapeRequests FOR SELECT TapeRequest.* FROM TapeRequest, DeviceGroupName, TapeServer
      WHERE DeviceGroupName.dgName = dgn
            AND DeviceGroupName.id = TapeRequest.deviceGroupName
            AND TapeServer.serverName = server
            AND TapeServer.id = TapeRequest.requestedSrv
			ORDER BY TapeRequest.modificationTime ASC;
  END IF;
END;


/*
 * PL/SQL method implementing the select from tapedrive queue
 */
CREATE OR REPLACE PROCEDURE selectTapeDriveQueue
 (dgn IN VARCHAR2, server IN VARCHAR2, tapeDrives OUT castorVdqm.TapeDrive_Cur) AS
BEGIN
  IF dgn IS NULL AND server IS NULL THEN
    OPEN tapeDrives FOR SELECT * FROM TapeDrive
		  ORDER BY TapeDrive.driveName ASC;
  ELSIF dgn IS NULL THEN
    OPEN tapeDrives FOR SELECT TapeDrive.* FROM TapeDrive, TapeServer
      WHERE TapeServer.serverName = server
            AND TapeServer.id = TapeDrive.tapeServer
			ORDER BY TapeDrive.driveName ASC;
  ELSIF server IS NULL THEN
    OPEN tapeDrives FOR SELECT TapeDrive.* FROM TapeDrive, DeviceGroupName
      WHERE DeviceGroupName.dgName = dgn
            AND DeviceGroupName.id = TapeDrive.deviceGroupName
			ORDER BY TapeDrive.driveName ASC;
  ELSE 
    OPEN tapeDrives FOR SELECT TapeDrive.* FROM TapeDrive, DeviceGroupName, TapeServer
      WHERE DeviceGroupName.dgName = dgn
            AND DeviceGroupName.id = TapeDrive.deviceGroupName
            AND TapeServer.serverName = server
            AND TapeServer.id = TapeDrive.tapeServer
			ORDER BY TapeDrive.driveName ASC;
  END IF;
END;


/*
 * PL/SQL method implementing the diskPoolQuery when listing pools
 */
CREATE OR REPLACE PROCEDURE describeDiskPools
(svcClassName IN VARCHAR2,
 result OUT castor.DiskPoolsQueryLine_Cur) AS
BEGIN
  -- We use here analytic functions and the grouping sets
  -- functionnality to get both the list of filesystems
  -- and a summary per diskserver and per diskpool
  -- The grouping analytic function also allows to mark
  -- the summary lines for easy detection in the C++ code
  OPEN result FOR
    SELECT grouping(ds.name) as IsDSGrouped,
           grouping(fs.mountPoint) as IsFSGrouped,
           dp.name,
           ds.name, ds.status, fs.mountPoint,
           sum(fs.free + fs.deltaFree - fs.reservedSpace + fs.spaceToBeFreed) as freeSpace,
           sum(fs.totalSize), sum(fs.reservedSpace),
           fs.minFreeSpace, fs.maxFreeSpace, fs.status
      FROM FileSystem fs, DiskServer ds, DiskPool dp,
           DiskPool2SvcClass d2s, SvcClass sc
     WHERE (sc.name = svcClassName OR svcClassName is NULL)
       AND sc.id = d2s.child
       AND d2s.parent = dp.id
       AND dp.id = fs.diskPool
       AND ds.id = fs.diskServer
       group by grouping sets(
           (dp.name, ds.name, ds.status, fs.mountPoint,
             fs.free + fs.deltaFree - fs.reservedSpace + fs.spaceToBeFreed,
             fs.totalSize, fs.reservedSpace,
             fs.minFreeSpace, fs.maxFreeSpace, fs.status),
           (dp.name, ds.name, ds.status),
           (dp.name)
          )
       order by dp.name, IsDSGrouped desc, ds.name, IsFSGrouped desc, fs.mountpoint;
END;

/*
 * PL/SQL method implementing the diskPoolQuery for a given pool
 */
CREATE OR REPLACE PROCEDURE describeDiskPool
(diskPoolName IN VARCHAR2,
 result OUT castor.DiskPoolQueryLine_Cur) AS
BEGIN
  -- We use here analytic functions and the grouping sets
  -- functionnality to get both the list of filesystems
  -- and a summary per diskserver and per diskpool
  -- The grouping analytic function also allows to mark
  -- the summary lines for easy detection in the C++ code
  OPEN result FOR 
    SELECT grouping(ds.name) as IsDSGrouped,
           grouping(fs.mountPoint) as IsGrouped,
           ds.name, ds.status, fs.mountPoint,
           sum(fs.free + fs.deltaFree - fs.reservedSpace + fs.spaceToBeFreed) as freeSpace,
           sum(fs.totalSize), sum(fs.reservedSpace),
           fs.minFreeSpace, fs.maxFreeSpace, fs.status
      FROM FileSystem fs, DiskServer ds, DiskPool dp
     WHERE dp.id = fs.diskPool
       AND dp.name = diskPoolName
       AND ds.id = fs.diskServer
       group by grouping sets(
           (ds.name, ds.status, fs.mountPoint,
             fs.free + fs.deltaFree - fs.reservedSpace + fs.spaceToBeFreed,
             fs.totalSize, fs.reservedSpace,
             fs.minFreeSpace, fs.maxFreeSpace, fs.status),
           (ds.name, ds.status),
           (dp.name)
          )
       order by IsDSGrouped desc, ds.name, IsGrouped desc, fs.mountpoint;
END;

