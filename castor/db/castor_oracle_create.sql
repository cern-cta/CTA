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
CREATE TABLE SubRequest (retryCounter NUMBER, fileName VARCHAR2(2048), protocol VARCHAR2(2048), xsize INTEGER, priority NUMBER, subreqId VARCHAR2(2048), flags NUMBER, modeBits NUMBER, creationTime INTEGER, lastModificationTime INTEGER, answered NUMBER, id INTEGER PRIMARY KEY, diskcopy INTEGER, castorFile INTEGER, parent INTEGER, status INTEGER, request INTEGER) INITRANS 50 PCTFREE 50;

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
CREATE TABLE CastorFile (fileId INTEGER, nsHost VARCHAR2(2048), fileSize INTEGER, creationTime INTEGER, lastAccessTime INTEGER, nbAccesses NUMBER, id INTEGER PRIMARY KEY, svcClass INTEGER, fileClass INTEGER) INITRANS 50 PCTFREE 50;

/* SQL statements for type DiskCopy */
CREATE TABLE DiskCopy (path VARCHAR2(2048), gcWeight float, creationTime INTEGER, id INTEGER PRIMARY KEY, fileSystem INTEGER, castorFile INTEGER, status INTEGER) INITRANS 50 PCTFREE 50;

/* SQL statements for type FileSystem */
CREATE TABLE FileSystem (free INTEGER, weight float, fsDeviation float, mountPoint VARCHAR2(2048), deltaWeight float, deltaFree NUMBER, reservedSpace NUMBER, minFreeSpace INTEGER, maxFreeSpace INTEGER, spaceToBeFreed INTEGER, id INTEGER PRIMARY KEY, diskPool INTEGER, diskserver INTEGER, status INTEGER) INITRANS 50 PCTFREE 50;

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

/* SQL statements for type TapeAccessSpecification */
CREATE TABLE TapeAccessSpecification (accessMode NUMBER, density VARCHAR2(2048), tapeModel VARCHAR2(2048), id INTEGER PRIMARY KEY) INITRANS 50 PCTFREE 50;

/* SQL statements for type TapeServer */
CREATE TABLE TapeServer (serverName VARCHAR2(2048), status NUMBER, id INTEGER PRIMARY KEY, actingMode INTEGER) INITRANS 50 PCTFREE 50;

/* SQL statements for type TapeRequest */
CREATE TABLE TapeRequest (priority NUMBER, modificationTime NUMBER, id INTEGER PRIMARY KEY, tape INTEGER, tapeAccessSpecification INTEGER, requestedSrv INTEGER, tapeDrive INTEGER, deviceGroupName INTEGER, client INTEGER) INITRANS 50 PCTFREE 50;

/* SQL statements for type TapeDrive */
CREATE TABLE TapeDrive (jobID NUMBER, modificationTime NUMBER, resettime NUMBER, usecount NUMBER, errcount NUMBER, transferredMB NUMBER, totalMB INTEGER, driveName VARCHAR2(2048), tapeAccessMode NUMBER, id INTEGER PRIMARY KEY, tape INTEGER, runningTapeReq INTEGER, deviceGroupName INTEGER, status INTEGER, tapeServer INTEGER) INITRANS 50 PCTFREE 50;
CREATE TABLE TapeDrive2TapeDriveComp (Parent INTEGER, Child INTEGER) INITRANS 50 PCTFREE 50;
CREATE INDEX I_TapeDrive2TapeDriveComp_C on TapeDrive2TapeDriveComp (child);
CREATE INDEX I_TapeDrive2TapeDriveComp_P on TapeDrive2TapeDriveComp (parent);

/* SQL statements for type ErrorHistory */
CREATE TABLE ErrorHistory (errorMessage VARCHAR2(2048), timeStamp NUMBER, id INTEGER PRIMARY KEY, tapeDrive INTEGER, tape INTEGER) INITRANS 50 PCTFREE 50;

/* SQL statements for type TapeDriveDedication */
CREATE TABLE TapeDriveDedication (clientHost NUMBER, euid NUMBER, egid NUMBER, vid VARCHAR2(2048), accessMode NUMBER, timePeriods VARCHAR2(2048), id INTEGER PRIMARY KEY, tapeDrive INTEGER) INITRANS 50 PCTFREE 50;

/* SQL statements for type TapeDriveCompatibility */
CREATE TABLE TapeDriveCompatibility (tapeDriveModel VARCHAR2(2048), priorityLevel NUMBER, id INTEGER PRIMARY KEY, tapeAccessSpecification INTEGER) INITRANS 50 PCTFREE 50;

/* SQL statements for type DeviceGroupName */
CREATE TABLE DeviceGroupName (dgName VARCHAR2(2048), id INTEGER PRIMARY KEY) INITRANS 50 PCTFREE 50;

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
/* This file contains SQL code that is not generated automatically */
/* and is inserted at the end of the generated code                */

/* A small table used to cross check code and DB versions */
CREATE TABLE CastorVersion (version VARCHAR2(2048));
INSERT INTO CastorVersion VALUES ('2_0_0_17');

/* Sequence for indices */
CREATE SEQUENCE ids_seq;

/* SQL statements for object types */
CREATE TABLE Id2Type (id INTEGER PRIMARY KEY, type NUMBER);
CREATE INDEX I_Id2Type_typeId on Id2Type (type, id);

/* SQL statements for requests status */
/* Partitioning enables faster response (more than indexing) for the most frequent queries - credits to Nilo Segura */
CREATE TABLE newRequests (type NUMBER(38) NOT NULL, id NUMBER(38) NOT NULL, creation DATE NOT NULL,
                 PRIMARY KEY (type, id))
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
CREATE INDEX I_DiskCopy_Castorfile on DiskCopy (castorFile);
CREATE INDEX I_DiskCopy_FileSystem on DiskCopy (fileSystem);
CREATE INDEX I_TapeCopy_Castorfile on TapeCopy (castorFile);
CREATE INDEX I_SubRequest_Castorfile on SubRequest (castorFile);
CREATE INDEX I_FileSystem_DiskPool on FileSystem (diskPool);
CREATE INDEX I_SubRequest_DiskCopy on SubRequest (diskCopy);
CREATE INDEX I_SubRequest_Request on SubRequest (request);

/* A little function base index to speed up subrequestToDo */
CREATE INDEX I_SubRequest_Status on SubRequest (decode(status,0,status,1,status,2,status,NULL));

/* Same kind of index to speed up subRequestFailedToDo */
CREATE INDEX I_SubRequest_Status7 on SubRequest (decode(status,7,status,null));

/* an index to speed up queries in FileQueryRequest, FindRequestRequest, RequestQueryRequest */
CREATE INDEX I_QueryParameter_Query on QueryParameter (query);

/* Constraint on FileClass name */
ALTER TABLE FileClass ADD UNIQUE (name); 

/* Add unique constraint on castorFiles */
ALTER TABLE CastorFile ADD UNIQUE (fileId, nsHost); 

/* get current time as a time_t. Not that easy in ORACLE */
CREATE OR REPLACE FUNCTION getTime RETURN NUMBER IS
  ret NUMBER;
BEGIN
  SELECT (SYSDATE - to_date('01-jan-1970 01:00:00','dd-mon-yyyy HH:MI:SS')) * (24*60*60) INTO ret FROM DUAL;
  RETURN ret;
END;

/****************************************************************/
/* NbTapeCopiesInFS to work around ORACLE missing optimizations */
/****************************************************************/

/* This table keeps track of the number of TapeCopy to migrate
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
                   AND TapeCopy.id = :new.child)
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
                   AND TapeCopy.id = :new.child)
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
   WHERE FS IN (SELECT FileSystem FROM DiskCopy WHERE CastorFile = :new.castorFile)
     AND Stream IN (SELECT parent FROM Stream2TapeCopy WHERE child = :new.id);
END;

/* Updates the count of tapecopies in NbTapeCopiesInFS
   whenever a DiskCopy has been replicated and the new one
   is put into STAGEOUT status from the
   WAITDISK2DISKCOPY status */
CREATE OR REPLACE TRIGGER tr_DiskCopy_Update
AFTER UPDATE of status ON DiskCopy
FOR EACH ROW
WHEN (old.status = 1 AND -- WAITDISK2DISKCOPY
      new.status = 6) -- STAGEOUT
BEGIN
  UPDATE NbTapeCopiesInFS SET NbTapeCopies = NbTapeCopies + 1
   WHERE FS = :new.fileSystem
     AND Stream IN (SELECT Stream2TapeCopy.parent
                      FROM Stream2TapeCopy, TapeCopy
                     WHERE TapeCopy.castorFile = :new.castorFile
                       AND Stream2TapeCopy.child = TapeCopy.id);
END;

/* XXX update count into NbTapeCopiesInFS when a Disk2Disk copy occurs
   FOR a file in CANBEMIGR */

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

/*************************/
/* Procedure definitions */
/*************************/

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
                   AND DiskCopy.status IN (1, 2, 5, 11)), -- WAITDISK2DISKCOPY, WAITTAPERECALL, WAITFS, WAITFS_SCHEDULING
      status = 5,
      lastModificationTime = getTime() -- WAITSUBREQ
  WHERE SubRequest.id = srId;
END;

/* PL/SQL method to archive a SubRequest and its request if needed */
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
   WHERE request = rid AND status NOT IN (8, 9); -- FINISHED, FAILED_FINISHED
  -- Archive request, client and SubRequests if needed
  IF nb = 0 THEN
    -- DELETE request from Id2Type
    DELETE FROM Id2Type WHERE id = rid RETURNING type INTO rtype;
    -- delete request and get client id
    IF rtype = 35 THEN -- StageGetRequest
      DELETE FROM StageGetRequest WHERE id = rid RETURNING client into rclient;
    ELSIF rtype = 40 THEN -- StagePutRequest
      DELETE FROM StagePutRequest WHERE id = rid RETURNING client INTO rclient;
    ELSIF rtype = 44 THEN -- StageUpdateRequest
      DELETE FROM StageUpdateRequest WHERE id = rid RETURNING client INTO rclient;
    ELSIF rtype = 39 THEN -- StagePutDoneRequest
      DELETE FROM StagePutDoneRequest WHERE id = rid RETURNING client INTO rclient;
    ELSIF rtype = 42 THEN -- StageRmRequest
      DELETE FROM StageRmRequest WHERE id = rid RETURNING client INTO rclient;
    ELSIF rtype = 51 THEN -- StageReleaseFilesRequest
      DELETE FROM StageReleaseFilesRequest WHERE id = rid RETURNING client INTO rclient;
    ELSIF rtype = 36 THEN -- StagePrepareToGetRequest
      DELETE FROM StagePrepareToGetRequest WHERE id = rid RETURNING client INTO rclient;
    ELSIF rtype = 37 THEN -- StagePrepareToPutRequest
      DELETE FROM StagePrepareToPutRequest WHERE id = rid RETURNING client INTO rclient;
    ELSIF rtype = 38 THEN -- StagePrepareToUpdateRequest
      DELETE FROM StagePrepareToUpdateRequest WHERE id = rid RETURNING client INTO rclient;
    END IF;
    -- DELETE Client
    DELETE FROM Id2Type WHERE id = rclient;
    DELETE FROM Client WHERE id = rclient;
    -- Delete SubRequests
    DELETE FROM Id2Type WHERE id IN
      (SELECT id FROM SubRequest WHERE request = rid);
    DELETE FROM SubRequest WHERE request = rid;
  END IF;
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
BEGIN
 UPDATE FileSystem SET deltaFree = deltaFree - fileSize,
                       reservedSpace = reservedSpace - reservation
  WHERE id = fs;
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
  -- table for a full explanation of why we need such a stupid UPDATE statement.
  UPDATE LockTable SET TheLock = 1
   WHERE DiskServerId =
   (SELECT DiskServer.id 
      FROM FileSystem, NbTapeCopiesInFS, DiskServer
     WHERE FileSystemRate(FileSystem.weight, FileSystem.deltaWeight, FileSystem.fsDeviation) =
     -- The double level of subselects is due to the fact that ORACLE is unable
     -- to use ROWNUM and ORDER BY at the same time. Thus, we have to first computes
     -- the maxRate and then select on it.
     (SELECT MAX(FileSystemRate(FileSystem.weight, FileSystem.deltaWeight, FileSystem.fsDeviation))
        FROM FileSystem, NbTapeCopiesInFS, DiskServer
       WHERE FileSystem.id = NbTapeCopiesInFS.FS
         AND NbTapeCopiesInFS.NbTapeCopies > 0
         AND NbTapeCopiesInFS.Stream = StreamId
         AND FileSystem.status IN (0, 1) -- FILESYSTEM_PRODUCTION, FILESYSTEM_DRAINING
         AND DiskServer.id = FileSystem.diskserver
         AND DiskServer.status IN (0, 1)) -- DISKSERVER_PRODUCTION, DISKSERVER_DRAINING
       -- here we need to put all the check again in case we have 2 filesystems
       -- with the same rate and one is not eligible !
       AND FileSystem.id = NbTapeCopiesInFS.FS
       AND NbTapeCopiesInFS.NbTapeCopies > 0
       AND NbTapeCopiesInFS.Stream = StreamId
       AND FileSystem.status IN (0, 1) -- FILESYSTEM_PRODUCTION, FILESYSTEM_DRAINING
       AND DiskServer.id = FileSystem.diskserver
       AND DiskServer.status IN (0, 1) -- DISKSERVER_PRODUCTION, DISKSERVER_DRAINING
       AND ROWNUM < 2)
   RETURNING DiskServerId INTO dsid;
  -- Now we got our Diskserver but we lost all other data (due to the fact we had
  -- to do an update and we could not do a join in the update)
  -- So let's get again the best filesystem on the diskServer (we could not get it straight
  -- due to the need of an update on the LockTable
  SELECT FileSystem.id INTO fileSystemId
    FROM FileSystem, NbTapeCopiesInFS
   WHERE FileSystemRate(FileSystem.weight, FileSystem.deltaWeight, FileSystem.fsDeviation) =
     (SELECT MAX(FileSystemRate(FileSystem.weight, FileSystem.deltaWeight, FileSystem.fsDeviation))
        FROM FileSystem, NbTapeCopiesInFS
       WHERE FileSystem.id = NbTapeCopiesInFS.FS
         AND NbTapeCopiesInFS.NbTapeCopies > 0
         AND NbTapeCopiesInFS.Stream = StreamId
         AND FileSystem.status IN (0, 1) -- FILESYSTEM_PRODUCTION, FILESYSTEM_DRAINING
         AND FileSystem.diskserver = dsId)
     -- Again, we need to put all the check again in case we have 2 filesystems
     -- with the same rate and one is not eligible !
     AND FileSystem.id = NbTapeCopiesInFS.FS
     AND NbTapeCopiesInFS.NbTapeCopies > 0
     AND NbTapeCopiesInFS.Stream = StreamId
     AND FileSystem.status IN (0, 1) -- FILESYSTEM_PRODUCTION, FILESYSTEM_DRAINING
     AND FileSystem.diskserver = dsId
     AND ROWNUM < 2;
  -- Now select what we need
  SELECT DiskServer.name, FileSystem.mountPoint, FileSystem.fsDeviation, FileSystem.diskserver, FileSystem.id
    INTO diskServerName, mountPoint, deviation, fsDiskServer, fileSystemId
    FROM FileSystem, DiskServer
   WHERE FileSystem.id = fileSystemId
     AND DiskServer.id = FileSystem.diskserver;
  SELECT /*+ FIRST_ROWS */
    DiskCopy.path, DiskCopy.id, CastorFile.id, CastorFile.fileId, CastorFile.nsHost, CastorFile.fileSize, TapeCopy.id
    INTO path, dci, castorFileId, fileId, nsHost, fileSize, tapeCopyId
    FROM DiskCopy, CastorFile, TapeCopy, Stream2TapeCopy
   WHERE DiskCopy.filesystem = fileSystemId
     AND DiskCopy.castorfile = CastorFile.id
     AND DiskCopy.status = 10 -- CANBEMIGR
     AND TapeCopy.castorfile = Castorfile.id
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
  UPDATE NbTapeCopiesInFS SET NbTapeCopies = NbTapeCopies - 1
   WHERE FS IN (SELECT DiskCopy.FileSystem
                  FROM DiskCopy, TapeCopy
                 WHERE DiskCopy.CastorFile = TapeCopy.castorFile
                   AND TapeCopy.id = tapeCopyId)
     AND Stream IN (SELECT parent FROM Stream2TapeCopy WHERE child = tapeCopyId);
  -- Update Filesystem state
  updateFsFileOpened(fsDiskServer, fileSystemId, deviation, 0);
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
                         (SELECT id, svcClass from StageGetRequest UNION
                          SELECT id, svcClass from StagePrepareToGetRequest UNION
                          SELECT id, svcClass from StageGetNextRequest UNION
                          SELECT id, svcClass from StageUpdateRequest UNION
                          SELECT id, svcClass from StagePrepareToUpdateRequest UNION
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

/* PL/SQL method implementing fileRecalled */
CREATE OR REPLACE PROCEDURE fileRecalled(tapecopyId IN INTEGER) AS
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
     AND SubRequest.diskcopy(+) = DiskCopy.id
     AND DiskCopy.status = 2;
  UPDATE DiskCopy SET status = 0 WHERE id = dci RETURNING fileSystem INTO fsid; -- DISKCOPY_STAGED
  IF SubRequestId IS NOT NULL THEN
    UPDATE SubRequest SET status = 1, lastModificationTime = getTime(), parent = 0
     WHERE id = SubRequestId; -- SUBREQUEST_RESTART
    UPDATE SubRequest SET status = 1, lastModificationTime = getTime(), parent = 0
     WHERE parent = SubRequestId; -- SUBREQUEST_RESTART
  END IF;
  updateFsFileClosed(fsId, fileSize, fileSize);
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
                          lastModificationTime = getTime()
     WHERE id = SubRequestId;
    UPDATE SubRequest SET status = 7, -- SUBREQUEST_FAILED
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
        tapeStatus INTEGER,
        segmentStatus INTEGER,
        diskServerName VARCHAR2(2048),
        fileSystemMountPoint VARCHAR2(2048));
  TYPE QueryLine_Cur IS REF CURSOR RETURN QueryLine;
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
     AND DiskCopy.status IN (1, 2, 5, 11); -- WAITDISK2DISKCOPY, WAITTAPERECALL, WAITFS, WAITFS_SCHEDULING
  IF stat.COUNT > 0 THEN
    -- Only DiskCopy is in WAIT*, make SubRequest wait on previous subrequest and do not schedule
    makeSubRequestWait(rsubreqId, dci(1));
    result := 0;  -- no schedule
    RETURN;
  END IF;
  -- Get the svcclass for this subrequest
  SELECT Request.id, Request.svcclass, SubRequest.castorfile
    INTO reqId, svcClassId, cfId
    FROM (SELECT id, svcClass from StageGetRequest UNION
          SELECT id, svcClass from StagePrepareToGetRequest UNION
          SELECT id, svcClass from StageUpdateRequest UNION
          SELECT id, svcClass from StagePrepareToUpdateRequest UNION
          SELECT id, svcClass from StagePutDoneRequest) Request,
         SubRequest
   WHERE Subrequest.request = Request.id
     AND Subrequest.id = rsubreqId;
  -- Try to see whether we have available DiskCopies
  SELECT DiskCopy.status, DiskCopy.id
    BULK COLLECT INTO stat, dci
    FROM DiskCopy, FileSystem, DiskServer, DiskPool2SvcClass, SvcClass
   WHERE DiskCopy.castorfile = cfId
     AND DiskCopy.fileSystem = FileSystem.id
     AND FileSystem.diskpool = DiskPool2SvcClass.parent
     AND DiskPool2SvcClass.child = svcClassId
     AND FileSystem.status = 0 -- PRODUCTION
     AND FileSystem.diskserver = DiskServer.id
     AND DiskServer.status = 0 -- PRODUCTION
     AND DiskCopy.status IN (0, 6, 10); -- STAGED, STAGEOUT, CANBEMIGR
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
             AND SubRequest.status IN (0, 1, 2, 3) -- START, RESTART, RETRY, WAITSCHED
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
            FROM DiskCopy, SubRequest, FileSystem, DiskServer, DiskPool2SvcClass, SvcClass
           WHERE SubRequest.id = rsubreqId
             AND SubRequest.castorfile = DiskCopy.castorfile
             AND FileSystem.diskpool = DiskPool2SvcClass.parent
             AND DiskPool2SvcClass.child = svcClassId
             AND DiskCopy.status IN (0, 6, 10) -- STAGED, STAGEOUT, CANBEMIGR
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
      (SELECT id, euid, egid from StageGetRequest UNION
       SELECT id, euid, egid from StagePrepareToGetRequest UNION
       SELECT id, euid, egid from StageUpdateRequest UNION
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
EXCEPTION WHEN NO_DATA_FOUND THEN -- No disk copy found on selected FileSystem, look in others
 BEGIN
  -- Try to find remote DiskCopies
  SELECT DiskCopy.id, DiskCopy.path, DiskCopy.status
  INTO dci, rpath, rstatus
  FROM DiskCopy, SubRequest, FileSystem, DiskServer
  WHERE SubRequest.id = srId
    AND SubRequest.castorfile = DiskCopy.castorfile
    AND DiskCopy.status IN (0, 1, 2, 5, 6, 10, 11) -- STAGED, WAITDISKTODISKCOPY, WAITTAPERECALL, WAIFS, STAGEOUT, CANBEMIGR, WAITFS_SCHEDULING
    AND FileSystem.id = DiskCopy.fileSystem
    AND FileSystem.status = 0 -- PRODUCTION
    AND DiskServer.id = FileSystem.diskserver
    AND DiskServer.status = 0 -- PRODUCTION
    AND ROWNUM < 2;
  -- Found a DiskCopy, Check whether to wait on it
  IF rstatus IN (2,5,11) THEN -- WAITTAPERECALL, WAITFS, WAITFS_SCHEDULING, Make SubRequest Wait
    makeSubRequestWait(srId, dci);
    dci := 0;
    rpath := '';
    close sources;
  ELSE
    OPEN sources
    FOR SELECT DiskCopy.id, DiskCopy.path, DiskCopy.status,
               FileSystem.weight, FileSystem.mountPoint,
               DiskServer.name
    FROM DiskCopy, SubRequest, FileSystem, DiskServer
    WHERE SubRequest.id = srId
      AND SubRequest.castorfile = DiskCopy.castorfile
      AND DiskCopy.status IN (0, 1, 2, 5, 6, 10, 11) -- STAGED, WAITDISKTODISKCOPY, WAITTAPERECALL, WAIFS, STAGEOUT, CANBEMIGR, WAITFS_SCHEDULING
      AND FileSystem.id = DiskCopy.fileSystem
      AND FileSystem.status = 0 -- PRODUCTION
      AND DiskServer.id = FileSystem.diskServer
      AND DiskServer.status = 0; -- PRODUCTION
    -- create DiskCopy for Disk to Disk copy
    UPDATE SubRequest SET diskCopy = ids_seq.nextval,
                          lastModificationTime = getTime() WHERE id = srId
     RETURNING castorFile, diskCopy INTO cfid, dci;
    INSERT INTO DiskCopy (path, id, FileSystem, castorFile, status, creationTime)
     VALUES (rpath, dci, fileSystemId, cfid, 1, getTime()); -- status WAITDISK2DISKCOPY
    INSERT INTO Id2Type (id, type) VALUES (dci, 5); -- OBJ_DiskCopy
    rstatus := 1; -- status WAITDISK2DISKCOPY
  END IF;
 EXCEPTION WHEN NO_DATA_FOUND THEN -- No disk copy found on any FileSystem
  -- create one for recall
  UPDATE SubRequest SET diskCopy = ids_seq.nextval, status = 4,
                        lastModificationTime = getTime() -- WAITTAPERECALL
   WHERE id = srId RETURNING castorFile, diskCopy INTO cfid, dci;
  SELECT fileId, nsHost INTO fid, nh FROM CastorFile WHERE id = cfid;
  buildPathFromFileId(fid, nh, dci, rpath);
  INSERT INTO DiskCopy (path, id, FileSystem, castorFile, status, creationTime)
   VALUES (rpath, dci, fileSystemId, cfid, 2, getTime()); -- status WAITTAPERECALL
  INSERT INTO Id2Type (id, type) VALUES (dci, 5); -- OBJ_DiskCopy
  rstatus := 99; -- WAITTAPERECALL, NEWLY CREATED
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
 -- Check whether it was the last subrequest in the request
 SELECT id INTO result FROM SubRequest
  WHERE request = reqId
    AND status NOT IN (6, 7, 8, 9, 10) -- READY, FAILED, FINISHED, FAILED_FINISHED, FAILED_ANSWERING
    AND answered = 0
    AND ROWNUM < 2;
EXCEPTION WHEN NO_DATA_FOUND THEN -- No data found means we were last
  result := 0;
END;

/* PL/SQL method implementing disk2DiskCopyDone */
CREATE OR REPLACE PROCEDURE disk2DiskCopyDone
(dcId IN INTEGER, dcStatus IN INTEGER) AS
BEGIN
  -- update DiskCopy
  UPDATE DiskCopy set status = dcStatus WHERE id = dcId; -- status DISKCOPY_STAGED
  -- update SubRequest
  UPDATE SubRequest set status = 6,
                        lastModificationTime = getTime()
   WHERE diskCopy = dcId; -- status SUBREQUEST_READY
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
        AND StagePrepareToPutRequest.id = SubRequest.request;
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
BEGIN
 -- get number of TapeCopies to create
 SELECT nbCopies INTO nc FROM FileClass, CastorFile
  WHERE CastorFile.id = cfId AND CastorFile.fileClass = FileClass.id;
 -- if no tape copy or 0 length file, no migration
 -- so we go directly to status STAGED
 IF nc = 0 OR fs = 0 THEN
   UPDATE DiskCopy SET status = 0 -- STAGED
    WHERE castorFile = cfId AND status = 6 -- STAGEOUT
   RETURNING fileSystem INTO fsId;
 ELSE
   -- update the DiskCopy status TO CANBEMIGR
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
        AND StagePrepareToPutRequest.id = SubRequest.request;
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
     (SELECT euid, egid, id from StagePutRequest UNION
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
   UPDATE SubRequest SET status = restart WHERE id IN
     (SELECT SubRequest.id FROM SubRequest, Id2Type
       WHERE SubRequest.request = Id2Type.id
         AND Id2Type.type = 39       -- PutDone
         AND SubRequest.castorFile = cfId
         AND SubRequest,status = 5); -- WAITSUBREQ
 END IF;
 COMMIT;
END;

/* PL/SQL method implementing selectCastorFile */
CREATE OR REPLACE PROCEDURE selectCastorFile (fId IN INTEGER,
                                              nh IN VARCHAR2,
                                              sc IN INTEGER,
                                              fc IN INTEGER,
                                              fs IN INTEGER,
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
    UPDATE CastorFile SET LastAccessTime = getTime(), nbAccesses = nbAccesses + 1
      WHERE id = rid;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    -- insert new row
    INSERT INTO CastorFile (id, fileId, nsHost, svcClass, fileClass, fileSize,
                            creationTime, lastAccessTime, nbAccesses)
      VALUES (ids_seq.nextval, fId, nh, sc, fc, fs, getTime(), getTime(), 1)
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
CREATE OR REPLACE PROCEDURE bestFileSystemForJob
(fileSystems IN castor."strList", machines IN castor."strList",
 minFree IN castor."cnumList", rMountPoint OUT VARCHAR2,
 rDiskServer OUT VARCHAR2) AS
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
         AND minFree(i) <= FileSystem.free + FileSystem.deltaFree - FileSystem.reservedSpace
         AND DiskServer.status IN (0, 1) -- DISKSERVER_PRODUCTION, DISKSERVER_DRAINING
         AND FileSystem.status IN (0, 1); -- FILESYSTEM_PRODUCTION, FILESYSTEM_DRAINING
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
         AND DiskServer.status IN (0, 1); -- DISKSERVER_PRODUCTION, DISKSERVER_DRAINING
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
       AND FileSystem.free + FileSystem.deltaFree - FileSystem.reservedSpace >= minFree(1)
       AND FileSystem.status IN (0, 1) -- FILESYSTEM_PRODUCTION, FILESYSTEM_DRAINING
     ORDER by FileSystem.weight + FileSystem.deltaWeight DESC,
              FileSystem.fsDeviation ASC;
   END;
  ELSE
   OPEN c1 FOR
    SELECT FileSystem.mountPoint, Diskserver.name,
           DiskServer.id, FileSystem.id, FileSystem.fsDeviation
    FROM FileSystem, DiskServer
    WHERE FileSystem.diskserver = DiskServer.id
     AND FileSystem.free + FileSystem.deltaFree - FileSystem.reservedSpace >= minFree(1)
     AND DiskServer.status IN (0, 1) -- DISKSERVER_PRODUCTION, DISKSERVER_DRAINING
     AND FileSystem.status IN (0, 1) -- FILESYSTEM_PRODUCTION, FILESYSTEM_DRAINING
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
CREATE OR REPLACE PROCEDURE selectFiles2Delete
(DiskServerName IN VARCHAR2,
 GCLocalFiles OUT castor.GCLocalFiles_Cur) AS
  files "numList";
BEGIN
  
  SELECT DiskCopy.id BULK COLLECT INTO files
    FROM DiskCopy, FileSystem, DiskServer
   WHERE DiskCopy.fileSystem = FileSystem.id
     AND FileSystem.DiskServer = DiskServer.id
     AND DiskServer.name = DiskServerName
     AND DiskCopy.status = 8 -- GC_CANDIDATE
     FOR UPDATE;
  IF files.COUNT > 0 THEN
    UPDATE DiskCopy set status = 9 -- BEING_DELETED
     WHERE id MEMBER OF files;
  END IF;
  OPEN GCLocalFiles FOR
    SELECT FileSystem.mountPoint||DiskCopy.path, DiskCopy.id 
      FROM DiskCopy, FileSystem
     WHERE DiskCopy.fileSystem = FileSystem.id
       AND DiskCopy.id MEMBER OF files;
END;

/*
 * PL/SQL method implementing filesDeleted
 * Note that we don't increase the freespace of the fileSystem.
 * This is done by the monitoring daemon, that knows the
 * exact amount of free space. However, we decrease the
 * spaceToBeFreed counter so that a next GC knows the status
 * of the FileSystem
 */
CREATE OR REPLACE PROCEDURE filesDeletedProc
(fileIds IN castor."cnumList") AS
  cfId NUMBER;
  fsId NUMBER;
  fsize NUMBER;
  nb NUMBER;
BEGIN
 IF fileIds.COUNT > 0 THEN
  -- Loop over the deleted files
  FOR i in fileIds.FIRST .. fileIds.LAST LOOP
    -- delete the DiskCopy
    DELETE FROM DiskCopy WHERE id = fileIds(i)
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
        -- See whether the castorfile has any SubRequest
        SELECT count(*) INTO nb FROM SubRequest
         WHERE castorFile = cfId;
        -- If any SubRequest, give up
        IF nb = 0 THEN
          -- Delete the CastorFile
          DELETE FROM CastorFile WHERE id = cfId;
        END IF;
      END IF;
    END IF;
  END LOOP;
 END IF;
END;

/* PL/SQL method implementing filesDeleted */
CREATE OR REPLACE PROCEDURE filesDeletionFailedProc
(fileIds IN castor."cnumList") AS
  cfId NUMBER;
  fsId NUMBER;
  fsize NUMBER;
BEGIN
 IF fileIds.COUNT > 0 THEN
  -- Loop over the deleted files
  FOR i in fileIds.FIRST .. fileIds.LAST LOOP
    -- set status of DiskCopy to FAILED
    UPDATE DiskCopy SET status = 4 -- FAILED
     WHERE id = fileIds(i)
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
  EXCEPTION WHEN TOO_MANY_ROWS THEN
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
  WHERE castorFile = cfId;
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
  WHERE status = 3 -- TAPECOPY_SELECTED
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
 -- drop all requests for the file
 FOR sr IN (SELECT id
              FROM SubRequest
             WHERE castorFile = cfId) LOOP
   archiveSubReq(sr.id);
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
   -- Something is running, so give uu
 EXCEPTION WHEN NO_DATA_FOUND THEN
   -- Nothing running
   -- Delete the segment(s)
   DELETE FROM Segment WHERE copy IN (SELECT id FROM TapeCopy WHERE castorfile = cfId); 
   -- Delete the TapeCopy
   DELETE FROM TapeCopy WHERE castorfile = cfId;
   -- Delete the DiskCopies
   UPDATE DiskCopy
      SET status = 8
    WHERE status = 2
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
    SELECT DiskCopy.id, CastorFile.fileSize, 0
      FROM DiskCopy, CastorFile
     WHERE CastorFile.id = DiskCopy.castorFile
       AND DiskCopy.fileSystem = fsId
       AND DiskCopy.status = 7 -- INVALID
    UNION
    SELECT DiskCopy.id, CastorFile.fileSize,
           getTime() - CastorFile.LastAccessTime - GREATEST(0,86400*LN((CastorFile.fileSize+1)/1024))
      FROM DiskCopy, CastorFile, SubRequest
     WHERE CastorFile.id = DiskCopy.castorFile
       AND DiskCopy.fileSystem = fsId
       AND DiskCopy.status = 0 -- STAGED
       AND DiskCopy.id = SubRequest.DiskCopy (+)
       AND Subrequest.id IS NULL
     ORDER BY 3;
  return result;
END;

/*
 * GC policy that mimic the old GC and takes into account
 * the number of accesses to the file. Namely we garbage
 * collect the oldest and biggest file that add the less
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
           getTime() - CastorFile.LastAccessTime -- older first
           - GREATEST(0,86400*LN((CastorFile.fileSize+1)/1024)) -- biggest first
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
     ORDER BY 3;
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
  SELECT diskPool, maxFreeSpace - free - deltaFree + reservedSpace - spaceToBeFreed
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
 * Trigger launching garbage collection whenever needed
 * Note that we only launch it when at least 10 gigs are to be deleted
 */
CREATE OR REPLACE TRIGGER tr_FileSystem_Update
AFTER UPDATE ON FileSystem
FOR EACH ROW
DECLARE
  freeSpace NUMBER;
BEGIN
  -- compute the actual free space taking into account reservations (reservedSpace)
  -- and already running GC processes (spaceToBeFreed)
  freeSpace := :new.free + :new.deltaFree - :new.reservedSpace + :new.spaceToBeFreed;
     -- shall we launch a new GC?
  IF :new.minFreeSpace > freeSpace AND 
     -- is it really worth launching it? (some other GCs maybe are already running
     -- so we accept it only if it will free more than 10 Gb)
     :new.maxFreeSpace > freeSpace + 10000000000 THEN
    garbageCollectFS(:new.id);
  END IF;
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
  -- external select is for the order by
 OPEN result FOR
    -- First internal select for files having a filesystem
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
     WHERE castorfile.id IN (SELECT * FROM TABLE(cfs)) AND Castorfile.id = DiskCopy.castorFile (+)
       AND castorfile.id = tpseg.castorfile(+) AND FileSystem.id(+) = DiskCopy.fileSystem
       AND DiskServer.id(+) = FileSystem.diskServer AND DiskPool2SvcClass.parent(+) = FileSystem.diskPool
       AND (DiskPool2SvcClass.child = svcClassId OR DiskPool2SvcClass.child IS NULL)
  ORDER BY fileid, nshost;
END;

/*
 * PL/SQL method implementing the stage_query based on fileId
 */
CREATE OR REPLACE PROCEDURE fileIdStageQuery
 (fid IN NUMBER,
  nh IN VARCHAR2,
  svcClassId IN NUMBER,
  result OUT castor.QueryLine_Cur) AS
  cfs "numList";
BEGIN
  SELECT id BULK COLLECT INTO cfs FROM CastorFile WHERE fileId = fid AND nshost = nh;
  internalStageQuery(cfs, svcClassId, result);
END;

/*
 * PL/SQL method implementing the stage_query based on request id
 */
CREATE OR REPLACE PROCEDURE reqIdStageQuery
 (rid IN VARCHAR2,
  svcClassId IN NUMBER,
  result OUT castor.QueryLine_Cur) AS
  cfs "numList";
BEGIN
  SELECT sr.castorfile BULK COLLECT INTO cfs
    FROM SubRequest sr,
         (SELECT id
            FROM StagePreparetogetRequest
           WHERE reqid LIKE rid
          UNION
          SELECT id
            FROM StagePreparetoputRequest
           WHERE reqid LIKE rid
          UNION
          SELECT id
            FROM StagePreparetoupdateRequest
           WHERE reqid LIKE rid
          UNION
          SELECT id
            FROM stageGetRequest
           WHERE reqid LIKE rid
          UNION
          SELECT id
            FROM stagePutRequest
           WHERE reqid LIKE rid) reqlist
   WHERE sr.request = reqlist.id;
  internalStageQuery(cfs, svcClassId, result);
END;

/*
 * PL/SQL method implementing the stage_query based on user tag
 */
CREATE OR REPLACE PROCEDURE userTagStageQuery
 (tag IN VARCHAR2,
  svcClassId IN NUMBER,
  result OUT castor.QueryLine_Cur) AS
  cfs "numList";
BEGIN
  SELECT sr.castorfile BULK COLLECT INTO cfs
    FROM SubRequest sr,
         (SELECT id
            FROM StagePreparetogetRequest
           WHERE userTag LIKE tag
          UNION
          SELECT id
            FROM StagePreparetoputRequest
           WHERE userTag LIKE tag
          UNION
          SELECT id
            FROM StagePreparetoupdateRequest
           WHERE userTag LIKE tag
          UNION
          SELECT id
            FROM stageGetRequest
           WHERE userTag LIKE tag
          UNION
          SELECT id
            FROM stagePutRequest
           WHERE userTag LIKE tag) reqlist
   WHERE sr.request = reqlist.id;
  internalStageQuery(cfs, svcClassId, result);
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
 */  
CREATE OR REPLACE PROCEDURE matchTape2TapeDrive
 (tapeDriveID OUT NUMBER, tapeRequestID OUT NUMBER) AS
BEGIN
  SELECT TapeDrive.id, TapeRequest.id 
    INTO tapeDriveID, tapeRequestID
    FROM TapeDrive, TapeRequest, TapeDrive2TapeDriveComp, TapeDriveCompatibility, 
         TapeServer, DeviceGroupName tapeDriveDgn, DeviceGroupName tapeRequestDgn 
		WHERE TapeDrive.status=0 
		      AND TapeDrive.runningTapeReq=0 
					AND TapeDrive.tapeServer=TapeServer.id 
					AND TapeServer.actingMode=0
					AND (TapeRequest.tapeDrive(+)=TapeDrive.id) 
					AND (TapeRequest.requestedSrv(+)=TapeDrive.tapeServer) 
					AND ((TapeDrive2TapeDriveComp.parent=TapeDrive.id 
					      AND TapeDrive2TapeDriveComp.child=TapeDriveCompatibility.id 
								AND TapeDriveCompatibility.tapeAccessSpecification=TapeRequest.tapeAccessSpecification 
								AND TapeDrive.deviceGroupName=tapeDriveDgn.id 
								AND TapeRequest.deviceGroupName=tapeRequestDgn.id 
								AND tapeDriveDgn.libraryName=tapeRequestDgn.libraryName) 
				       OR TapeDrive.deviceGroupName=TapeRequest.deviceGroupName) 
				  AND rownum < 2 
					ORDER BY TapeDriveCompatibility.priorityLevel DESC, 
					         TapeRequest.id ASC 
				  FOR UPDATE;
  EXCEPTION
    WHEN NO_DATA_FOUND THEN
    tapeDriveID := 0;
    tapeRequestID := 0;
END;

