-- This file contains SQL code that is not generated automatically
-- and is inserted at the beginning of the generated code

-- SQL statements for object types */
DROP INDEX I_Id2Type_type ON Id2Type;
DROP INDEX I_Id2Type_full ON Id2Type;
DROP TABLE Id2Type;
CREATE TABLE Id2Type (id BIGINT, type INTEGER, PRIMARY KEY(id));
CREATE INDEX I_Id2Type_full ON Id2Type (id, type);
CREATE INDEX I_Id2Type_type ON Id2Type (type);

-- SQL statements for requests status
DROP INDEX I_newRequests_type ON newRequests;
DROP TABLE newRequests;
CREATE TABLE newRequests (id INTEGER AUTO_INCREMENT, type INTEGER, creation DATE, PRIMARY KEY(id));
CREATE INDEX I_newRequests_type on newRequests (type);

ALTER TABLE SvcClass2TapePool
  DROP CONSTRAINT fk_SvcClass2TapePool_P
  DROP CONSTRAINT fk_SvcClass2TapePool_C;
ALTER TABLE DiskPool2SvcClass
  DROP CONSTRAINT fk_DiskPool2SvcClass_P
  DROP CONSTRAINT fk_DiskPool2SvcClass_C;
ALTER TABLE Stream2TapeCopy
  DROP CONSTRAINT fk_Stream2TapeCopy_P
  DROP CONSTRAINT fk_Stream2TapeCopy_C;
ALTER TABLE TapeDrive2ExtendedDevic
  DROP CONSTRAINT fk_TapeDrive2ExtendedDevic_P
  DROP CONSTRAINT fk_TapeDrive2ExtendedDevic_C;
/* SQL statements for type BaseAddress */
DROP TABLE BaseAddress;
CREATE TABLE BaseAddress (objType INT, cnvSvcName VARCHAR(1000), cnvSvcType INT, target BIGINT, id BIGINT, PRIMARY KEY (id));

/* SQL statements for type Client */
DROP TABLE Client;
CREATE TABLE Client (ipAddress INT, port INT, id BIGINT, PRIMARY KEY (id));

/* SQL statements for type ClientIdentification */
DROP TABLE ClientIdentification;
CREATE TABLE ClientIdentification (machine VARCHAR(1000), userName VARCHAR(1000), port INT, euid INT, egid INT, magic INT, id BIGINT, PRIMARY KEY (id));

/* SQL statements for type Disk2DiskCopyDoneRequest */
DROP TABLE Disk2DiskCopyDoneRequest;
CREATE TABLE Disk2DiskCopyDoneRequest (flags BIGINT, userName VARCHAR(1000), euid INT, egid INT, mask INT, pid INT, machine VARCHAR(1000), svcClassName VARCHAR(1000), userTag VARCHAR(1000), reqId VARCHAR(1000), creationTime BIGINT, lastModificationTime BIGINT, diskCopyId BIGINT, status INT, id BIGINT, PRIMARY KEY (id), svcClass INT, client INT);

/* SQL statements for type GetUpdateDone */
DROP TABLE GetUpdateDone;
CREATE TABLE GetUpdateDone (flags BIGINT, userName VARCHAR(1000), euid INT, egid INT, mask INT, pid INT, machine VARCHAR(1000), svcClassName VARCHAR(1000), userTag VARCHAR(1000), reqId VARCHAR(1000), creationTime BIGINT, lastModificationTime BIGINT, subReqId BIGINT, id BIGINT, PRIMARY KEY (id), svcClass INT, client INT);

/* SQL statements for type GetUpdateFailed */
DROP TABLE GetUpdateFailed;
CREATE TABLE GetUpdateFailed (flags BIGINT, userName VARCHAR(1000), euid INT, egid INT, mask INT, pid INT, machine VARCHAR(1000), svcClassName VARCHAR(1000), userTag VARCHAR(1000), reqId VARCHAR(1000), creationTime BIGINT, lastModificationTime BIGINT, subReqId BIGINT, id BIGINT, PRIMARY KEY (id), svcClass INT, client INT);

/* SQL statements for type PutFailed */
DROP TABLE PutFailed;
CREATE TABLE PutFailed (flags BIGINT, userName VARCHAR(1000), euid INT, egid INT, mask INT, pid INT, machine VARCHAR(1000), svcClassName VARCHAR(1000), userTag VARCHAR(1000), reqId VARCHAR(1000), creationTime BIGINT, lastModificationTime BIGINT, subReqId BIGINT, id BIGINT, PRIMARY KEY (id), svcClass INT, client INT);

/* SQL statements for type Files2Delete */
DROP TABLE Files2Delete;
CREATE TABLE Files2Delete (flags BIGINT, userName VARCHAR(1000), euid INT, egid INT, mask INT, pid INT, machine VARCHAR(1000), svcClassName VARCHAR(1000), userTag VARCHAR(1000), reqId VARCHAR(1000), creationTime BIGINT, lastModificationTime BIGINT, diskServer VARCHAR(1000), id BIGINT, PRIMARY KEY (id), svcClass INT, client INT);

/* SQL statements for type FilesDeleted */
DROP TABLE FilesDeleted;
CREATE TABLE FilesDeleted (flags BIGINT, userName VARCHAR(1000), euid INT, egid INT, mask INT, pid INT, machine VARCHAR(1000), svcClassName VARCHAR(1000), userTag VARCHAR(1000), reqId VARCHAR(1000), creationTime BIGINT, lastModificationTime BIGINT, id BIGINT, PRIMARY KEY (id), svcClass INT, client INT);

/* SQL statements for type FilesDeletionFailed */
DROP TABLE FilesDeletionFailed;
CREATE TABLE FilesDeletionFailed (flags BIGINT, userName VARCHAR(1000), euid INT, egid INT, mask INT, pid INT, machine VARCHAR(1000), svcClassName VARCHAR(1000), userTag VARCHAR(1000), reqId VARCHAR(1000), creationTime BIGINT, lastModificationTime BIGINT, id BIGINT, PRIMARY KEY (id), svcClass INT, client INT);

/* SQL statements for type GCFile */
DROP TABLE GCFile;
CREATE TABLE GCFile (diskCopyId BIGINT, id BIGINT, PRIMARY KEY (id), request INT);

/* SQL statements for type GCLocalFile */
DROP TABLE GCLocalFile;
CREATE TABLE GCLocalFile (fileName VARCHAR(1000), diskCopyId BIGINT, id BIGINT, PRIMARY KEY (id));

/* SQL statements for type MoverCloseRequest */
DROP TABLE MoverCloseRequest;
CREATE TABLE MoverCloseRequest (flags BIGINT, userName VARCHAR(1000), euid INT, egid INT, mask INT, pid INT, machine VARCHAR(1000), svcClassName VARCHAR(1000), userTag VARCHAR(1000), reqId VARCHAR(1000), creationTime BIGINT, lastModificationTime BIGINT, subReqId BIGINT, fileSize BIGINT, id BIGINT, PRIMARY KEY (id), svcClass INT, client INT);

/* SQL statements for type PutStartRequest */
DROP TABLE PutStartRequest;
CREATE TABLE PutStartRequest (subreqId BIGINT, diskServer VARCHAR(1000), fileSystem VARCHAR(1000), flags BIGINT, userName VARCHAR(1000), euid INT, egid INT, mask INT, pid INT, machine VARCHAR(1000), svcClassName VARCHAR(1000), userTag VARCHAR(1000), reqId VARCHAR(1000), creationTime BIGINT, lastModificationTime BIGINT, id BIGINT, PRIMARY KEY (id), svcClass INT, client INT);

/* SQL statements for type PutDoneStart */
DROP TABLE PutDoneStart;
CREATE TABLE PutDoneStart (subreqId BIGINT, diskServer VARCHAR(1000), fileSystem VARCHAR(1000), flags BIGINT, userName VARCHAR(1000), euid INT, egid INT, mask INT, pid INT, machine VARCHAR(1000), svcClassName VARCHAR(1000), userTag VARCHAR(1000), reqId VARCHAR(1000), creationTime BIGINT, lastModificationTime BIGINT, id BIGINT, PRIMARY KEY (id), svcClass INT, client INT);

/* SQL statements for type GetUpdateStartRequest */
DROP TABLE GetUpdateStartRequest;
CREATE TABLE GetUpdateStartRequest (subreqId BIGINT, diskServer VARCHAR(1000), fileSystem VARCHAR(1000), flags BIGINT, userName VARCHAR(1000), euid INT, egid INT, mask INT, pid INT, machine VARCHAR(1000), svcClassName VARCHAR(1000), userTag VARCHAR(1000), reqId VARCHAR(1000), creationTime BIGINT, lastModificationTime BIGINT, id BIGINT, PRIMARY KEY (id), svcClass INT, client INT);

/* SQL statements for type QueryParameter */
DROP TABLE QueryParameter;
CREATE TABLE QueryParameter (value VARCHAR(1000), id BIGINT, PRIMARY KEY (id), query INT, queryType INT);

/* SQL statements for type StagePrepareToGetRequest */
DROP TABLE StagePrepareToGetRequest;
CREATE TABLE StagePrepareToGetRequest (flags BIGINT, userName VARCHAR(1000), euid INT, egid INT, mask INT, pid INT, machine VARCHAR(1000), svcClassName VARCHAR(1000), userTag VARCHAR(1000), reqId VARCHAR(1000), creationTime BIGINT, lastModificationTime BIGINT, id BIGINT, PRIMARY KEY (id), svcClass INT, client INT);

/* SQL statements for type StagePrepareToPutRequest */
DROP TABLE StagePrepareToPutRequest;
CREATE TABLE StagePrepareToPutRequest (flags BIGINT, userName VARCHAR(1000), euid INT, egid INT, mask INT, pid INT, machine VARCHAR(1000), svcClassName VARCHAR(1000), userTag VARCHAR(1000), reqId VARCHAR(1000), creationTime BIGINT, lastModificationTime BIGINT, id BIGINT, PRIMARY KEY (id), svcClass INT, client INT);

/* SQL statements for type StagePrepareToUpdateRequest */
DROP TABLE StagePrepareToUpdateRequest;
CREATE TABLE StagePrepareToUpdateRequest (flags BIGINT, userName VARCHAR(1000), euid INT, egid INT, mask INT, pid INT, machine VARCHAR(1000), svcClassName VARCHAR(1000), userTag VARCHAR(1000), reqId VARCHAR(1000), creationTime BIGINT, lastModificationTime BIGINT, id BIGINT, PRIMARY KEY (id), svcClass INT, client INT);

/* SQL statements for type StageGetRequest */
DROP TABLE StageGetRequest;
CREATE TABLE StageGetRequest (flags BIGINT, userName VARCHAR(1000), euid INT, egid INT, mask INT, pid INT, machine VARCHAR(1000), svcClassName VARCHAR(1000), userTag VARCHAR(1000), reqId VARCHAR(1000), creationTime BIGINT, lastModificationTime BIGINT, id BIGINT, PRIMARY KEY (id), svcClass INT, client INT);

/* SQL statements for type StagePutRequest */
DROP TABLE StagePutRequest;
CREATE TABLE StagePutRequest (flags BIGINT, userName VARCHAR(1000), euid INT, egid INT, mask INT, pid INT, machine VARCHAR(1000), svcClassName VARCHAR(1000), userTag VARCHAR(1000), reqId VARCHAR(1000), creationTime BIGINT, lastModificationTime BIGINT, id BIGINT, PRIMARY KEY (id), svcClass INT, client INT);

/* SQL statements for type StageUpdateRequest */
DROP TABLE StageUpdateRequest;
CREATE TABLE StageUpdateRequest (flags BIGINT, userName VARCHAR(1000), euid INT, egid INT, mask INT, pid INT, machine VARCHAR(1000), svcClassName VARCHAR(1000), userTag VARCHAR(1000), reqId VARCHAR(1000), creationTime BIGINT, lastModificationTime BIGINT, id BIGINT, PRIMARY KEY (id), svcClass INT, client INT);

/* SQL statements for type StageRmRequest */
DROP TABLE StageRmRequest;
CREATE TABLE StageRmRequest (flags BIGINT, userName VARCHAR(1000), euid INT, egid INT, mask INT, pid INT, machine VARCHAR(1000), svcClassName VARCHAR(1000), userTag VARCHAR(1000), reqId VARCHAR(1000), creationTime BIGINT, lastModificationTime BIGINT, id BIGINT, PRIMARY KEY (id), svcClass INT, client INT);

/* SQL statements for type StagePutDoneRequest */
DROP TABLE StagePutDoneRequest;
CREATE TABLE StagePutDoneRequest (flags BIGINT, userName VARCHAR(1000), euid INT, egid INT, mask INT, pid INT, machine VARCHAR(1000), svcClassName VARCHAR(1000), userTag VARCHAR(1000), reqId VARCHAR(1000), creationTime BIGINT, lastModificationTime BIGINT, parentUuid VARCHAR(1000), id BIGINT, PRIMARY KEY (id), svcClass INT, client INT, parent INT);

/* SQL statements for type StageFileQueryRequest */
DROP TABLE StageFileQueryRequest;
CREATE TABLE StageFileQueryRequest (flags BIGINT, userName VARCHAR(1000), euid INT, egid INT, mask INT, pid INT, machine VARCHAR(1000), svcClassName VARCHAR(1000), userTag VARCHAR(1000), reqId VARCHAR(1000), creationTime BIGINT, lastModificationTime BIGINT, fileName VARCHAR(1000), id BIGINT, PRIMARY KEY (id), svcClass INT, client INT);

/* SQL statements for type StageRequestQueryRequest */
DROP TABLE StageRequestQueryRequest;
CREATE TABLE StageRequestQueryRequest (flags BIGINT, userName VARCHAR(1000), euid INT, egid INT, mask INT, pid INT, machine VARCHAR(1000), svcClassName VARCHAR(1000), userTag VARCHAR(1000), reqId VARCHAR(1000), creationTime BIGINT, lastModificationTime BIGINT, id BIGINT, PRIMARY KEY (id), svcClass INT, client INT);

/* SQL statements for type StageFindRequestRequest */
DROP TABLE StageFindRequestRequest;
CREATE TABLE StageFindRequestRequest (flags BIGINT, userName VARCHAR(1000), euid INT, egid INT, mask INT, pid INT, machine VARCHAR(1000), svcClassName VARCHAR(1000), userTag VARCHAR(1000), reqId VARCHAR(1000), creationTime BIGINT, lastModificationTime BIGINT, id BIGINT, PRIMARY KEY (id), svcClass INT, client INT);

/* SQL statements for type SubRequest */
DROP TABLE SubRequest;
CREATE TABLE SubRequest (retryCounter INT, fileName VARCHAR(1000), protocol VARCHAR(1000), xsize BIGINT, priority INT, subreqId VARCHAR(1000), flags INT, modeBits INT, creationTime BIGINT, lastModificationTime BIGINT, answered INT, id BIGINT, PRIMARY KEY (id), diskcopy INT, castorFile INT, parent INT, status INT, request INT);

/* SQL statements for type StageReleaseFilesRequest */
DROP TABLE StageReleaseFilesRequest;
CREATE TABLE StageReleaseFilesRequest (flags BIGINT, userName VARCHAR(1000), euid INT, egid INT, mask INT, pid INT, machine VARCHAR(1000), svcClassName VARCHAR(1000), userTag VARCHAR(1000), reqId VARCHAR(1000), creationTime BIGINT, lastModificationTime BIGINT, id BIGINT, PRIMARY KEY (id), svcClass INT, client INT);

/* SQL statements for type StageAbortRequest */
DROP TABLE StageAbortRequest;
CREATE TABLE StageAbortRequest (parentUuid VARCHAR(1000), flags BIGINT, userName VARCHAR(1000), euid INT, egid INT, mask INT, pid INT, machine VARCHAR(1000), svcClassName VARCHAR(1000), userTag VARCHAR(1000), reqId VARCHAR(1000), creationTime BIGINT, lastModificationTime BIGINT, id BIGINT, PRIMARY KEY (id), parent INT, svcClass INT, client INT);

/* SQL statements for type StageGetNextRequest */
DROP TABLE StageGetNextRequest;
CREATE TABLE StageGetNextRequest (parentUuid VARCHAR(1000), flags BIGINT, userName VARCHAR(1000), euid INT, egid INT, mask INT, pid INT, machine VARCHAR(1000), svcClassName VARCHAR(1000), userTag VARCHAR(1000), reqId VARCHAR(1000), creationTime BIGINT, lastModificationTime BIGINT, id BIGINT, PRIMARY KEY (id), parent INT, svcClass INT, client INT);

/* SQL statements for type StagePutNextRequest */
DROP TABLE StagePutNextRequest;
CREATE TABLE StagePutNextRequest (parentUuid VARCHAR(1000), flags BIGINT, userName VARCHAR(1000), euid INT, egid INT, mask INT, pid INT, machine VARCHAR(1000), svcClassName VARCHAR(1000), userTag VARCHAR(1000), reqId VARCHAR(1000), creationTime BIGINT, lastModificationTime BIGINT, id BIGINT, PRIMARY KEY (id), parent INT, svcClass INT, client INT);

/* SQL statements for type StageUpdateNextRequest */
DROP TABLE StageUpdateNextRequest;
CREATE TABLE StageUpdateNextRequest (parentUuid VARCHAR(1000), flags BIGINT, userName VARCHAR(1000), euid INT, egid INT, mask INT, pid INT, machine VARCHAR(1000), svcClassName VARCHAR(1000), userTag VARCHAR(1000), reqId VARCHAR(1000), creationTime BIGINT, lastModificationTime BIGINT, id BIGINT, PRIMARY KEY (id), parent INT, svcClass INT, client INT);

/* SQL statements for type Tape */
DROP TABLE Tape;
CREATE TABLE Tape (vid VARCHAR(1000), side INT, tpmode INT, errMsgTxt VARCHAR(1000), errorCode INT, severity INT, vwAddress VARCHAR(1000), id BIGINT, PRIMARY KEY (id), stream INT, status INT);

/* SQL statements for type Segment */
DROP TABLE Segment;
CREATE TABLE Segment (fseq INT, offset BIGINT, bytes_in BIGINT, bytes_out BIGINT, host_bytes BIGINT, segmCksumAlgorithm VARCHAR(1000), segmCksum INT, errMsgTxt VARCHAR(1000), errorCode INT, severity INT, blockId0 INT, blockId1 INT, blockId2 INT, blockId3 INT, id BIGINT, PRIMARY KEY (id), tape INT, copy INT, status INT);

/* SQL statements for type TapePool */
DROP TABLE TapePool;
CREATE TABLE TapePool (name VARCHAR(1000), id BIGINT, PRIMARY KEY (id));

/* SQL statements for type TapeCopy */
DROP TABLE TapeCopy;
CREATE TABLE TapeCopy (copyNb INT, id BIGINT, PRIMARY KEY (id), castorFile INT, status INT);

/* SQL statements for type CastorFile */
DROP TABLE CastorFile;
CREATE TABLE CastorFile (fileId BIGINT, nsHost VARCHAR(1000), fileSize BIGINT, creationTime BIGINT, lastAccessTime BIGINT, nbAccesses INT, id BIGINT, PRIMARY KEY (id), svcClass INT, fileClass INT);

/* SQL statements for type DiskCopy */
DROP TABLE DiskCopy;
CREATE TABLE DiskCopy (path VARCHAR(1000), gcWeight float, creationTime BIGINT, id BIGINT, PRIMARY KEY (id), fileSystem INT, castorFile INT, status INT);

/* SQL statements for type FileSystem */
DROP TABLE FileSystem;
CREATE TABLE FileSystem (free BIGINT, weight float, fsDeviation float, mountPoint VARCHAR(1000), deltaWeight float, deltaFree INT, reservedSpace INT, id BIGINT, PRIMARY KEY (id), diskPool INT, diskserver INT, status INT);

/* SQL statements for type SvcClass */
DROP TABLE SvcClass;
CREATE TABLE SvcClass (nbDrives INT, name VARCHAR(1000), defaultFileSize BIGINT, maxReplicaNb INT, replicationPolicy VARCHAR(1000), gcPolicy VARCHAR(1000), migratorPolicy VARCHAR(1000), recallerPolicy VARCHAR(1000), id BIGINT, PRIMARY KEY (id));
DROP INDEX I_SvcClass2TapePool_C;
DROP INDEX I_SvcClass2TapePool_P;
DROP TABLE SvcClass2TapePool;
CREATE TABLE SvcClass2TapePool (Parent BIGINT, Child BIGINT);
CREATE INDEX I_SvcClass2TapePool_C on SvcClass2TapePool (child);
CREATE INDEX I_SvcClass2TapePool_P on SvcClass2TapePool (parent);

/* SQL statements for type DiskPool */
DROP TABLE DiskPool;
CREATE TABLE DiskPool (name VARCHAR(1000), id BIGINT, PRIMARY KEY (id));
DROP INDEX I_DiskPool2SvcClass_C;
DROP INDEX I_DiskPool2SvcClass_P;
DROP TABLE DiskPool2SvcClass;
CREATE TABLE DiskPool2SvcClass (Parent BIGINT, Child BIGINT);
CREATE INDEX I_DiskPool2SvcClass_C on DiskPool2SvcClass (child);
CREATE INDEX I_DiskPool2SvcClass_P on DiskPool2SvcClass (parent);

/* SQL statements for type Stream */
DROP TABLE Stream;
CREATE TABLE Stream (initialSizeToTransfer BIGINT, id BIGINT, PRIMARY KEY (id), tape INT, tapePool INT, status INT);
DROP INDEX I_Stream2TapeCopy_C;
DROP INDEX I_Stream2TapeCopy_P;
DROP TABLE Stream2TapeCopy;
CREATE TABLE Stream2TapeCopy (Parent BIGINT, Child BIGINT);
CREATE INDEX I_Stream2TapeCopy_C on Stream2TapeCopy (child);
CREATE INDEX I_Stream2TapeCopy_P on Stream2TapeCopy (parent);

/* SQL statements for type FileClass */
DROP TABLE FileClass;
CREATE TABLE FileClass (name VARCHAR(1000), minFileSize BIGINT, maxFileSize BIGINT, nbCopies INT, id BIGINT, PRIMARY KEY (id));

/* SQL statements for type DiskServer */
DROP TABLE DiskServer;
CREATE TABLE DiskServer (name VARCHAR(1000), id BIGINT, PRIMARY KEY (id), status INT);

/* SQL statements for type ExtendedDeviceGroup */
DROP TABLE ExtendedDeviceGroup;
CREATE TABLE ExtendedDeviceGroup (dgName VARCHAR(1000), mode INT, id BIGINT, PRIMARY KEY (id));

/* SQL statements for type TapeServer */
DROP TABLE TapeServer;
CREATE TABLE TapeServer (serverName VARCHAR(1000), status INT, id BIGINT, PRIMARY KEY (id));

/* SQL statements for type TapeRequest */
DROP TABLE TapeRequest;
CREATE TABLE TapeRequest (priority INT, creationTime INT, id BIGINT, PRIMARY KEY (id), tape INT, client INT, reqExtDevGrp INT, requestedSrv INT);

/* SQL statements for type TapeDrive */
DROP TABLE TapeDrive;
CREATE TABLE TapeDrive (jobID INT, creationTime INT, resettime INT, usecount INT, errcount INT, transferredMB INT, totalMB BIGINT, dedicate VARCHAR(1000), newDedicate VARCHAR(1000), is_uid INT, is_gid INT, is_name INT, no_uid INT, no_gid INT, no_name INT, no_host INT, no_vid INT, no_mode INT, no_date INT, no_time INT, no_age INT, uid INT, gid INT, name VARCHAR(1000), id BIGINT, PRIMARY KEY (id), tape INT, status INT, tapeServer INT);
DROP INDEX I_TapeDrive2ExtendedDevic_C;
DROP INDEX I_TapeDrive2ExtendedDevic_P;
DROP TABLE TapeDrive2ExtendedDevic;
CREATE TABLE TapeDrive2ExtendedDevic (Parent BIGINT, Child BIGINT);
CREATE INDEX I_TapeDrive2ExtendedDevic_C on TapeDrive2ExtendedDevic (child);
CREATE INDEX I_TapeDrive2ExtendedDevic_P on TapeDrive2ExtendedDevic (parent);

ALTER TABLE SvcClass2TapePool
  ADD CONSTRAINT fk_SvcClass2TapePool_P FOREIGN KEY (Parent) REFERENCES SvcClass (id)
  ADD CONSTRAINT fk_SvcClass2TapePool_C FOREIGN KEY (Child) REFERENCES TapePool (id);
ALTER TABLE DiskPool2SvcClass
  ADD CONSTRAINT fk_DiskPool2SvcClass_P FOREIGN KEY (Parent) REFERENCES DiskPool (id)
  ADD CONSTRAINT fk_DiskPool2SvcClass_C FOREIGN KEY (Child) REFERENCES SvcClass (id);
ALTER TABLE Stream2TapeCopy
  ADD CONSTRAINT fk_Stream2TapeCopy_P FOREIGN KEY (Parent) REFERENCES Stream (id)
  ADD CONSTRAINT fk_Stream2TapeCopy_C FOREIGN KEY (Child) REFERENCES TapeCopy (id);
ALTER TABLE TapeDrive2ExtendedDevic
  ADD CONSTRAINT fk_TapeDrive2ExtendedDevic_P FOREIGN KEY (Parent) REFERENCES TapeDrive (id)
  ADD CONSTRAINT fk_TapeDrive2ExtendedDevic_C FOREIGN KEY (Child) REFERENCES ExtendedDeviceGroup (id);
-- This file contains SQL code that is not generated automatically
-- and is inserted at the end of the generated code

-- A small table used to cross check code and DB versions
DROP TABLE CastorVersion;
CREATE TABLE CastorVersion (version VARCHAR(10));
INSERT INTO CastorVersion VALUES ('2_0_1_0');


/* Indexes related to CastorFiles */
CREATE UNIQUE INDEX I_DiskServer_name on DiskServer (name);
-- CREATE UNIQUE INDEX I_CastorFile_fileIdNsHost on CastorFile (fileId, nsHost);    doesn't work on field with length > 1000
CREATE INDEX I_DiskCopy_Castorfile on DiskCopy (castorFile);
CREATE INDEX I_DiskCopy_FileSystem on DiskCopy (fileSystem);
CREATE INDEX I_TapeCopy_Castorfile on TapeCopy (castorFile);
CREATE INDEX I_SubRequest_Castorfile on SubRequest (castorFile);
CREATE INDEX I_FileSystem_DiskPool on FileSystem (diskPool);
CREATE INDEX I_SubRequest_DiskCopy on SubRequest (diskCopy);
CREATE INDEX I_SubRequest_Request on SubRequest (request);

/* A little function base index to speed up subrequestToDo */    -- not supported in MySQL
-- CREATE INDEX I_SubRequest_Status on SubRequest
--  (CASE status WHEN 0 THEN status WHEN 1 THEN status WHEN 2 THEN status ELSE NULL end);

/* Constraint on FileClass name */
ALTER TABLE FileClass ADD UNIQUE (name); 

/* Add unique constraint on castorFiles */
ALTER TABLE CastorFile ADD UNIQUE (fileId, nsHost); 


DELIMITER //

-- Sequence for indices: not available in MySQL => using an AUTO_INCREMENT field on a dedicated table
DROP TABLE Sequence//
CREATE TABLE Sequence (value BIGINT AUTO_INCREMENT, PRIMARY KEY(value))//

CREATE PROCEDURE seqNextVal(OUT value BIGINT)
BEGIN
  START TRANSACTION;
  INSERT INTO Sequence VALUES (NULL);
  SELECT LAST_INSERT_ID() INTO value;
  DELETE FROM Sequence;
  COMMIT;
END//


/* get current time as a time_t. A bit easier in MySQL than in Oracle :-) */
CREATE FUNCTION getTime() RETURNS BIGINT
BEGIN
  RETURN TIMESTAMPDIFF(SECOND, '1970-01-01 01:00:00', NOW());   -- @todo is SECOND precision enough?
END//


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
DROP TABLE NbTapeCopiesInFS//
CREATE TABLE NbTapeCopiesInFS (FS BIGINT, Stream BIGINT, NbTapeCopies BIGINT)//
CREATE UNIQUE INDEX I_NbTapeCopiesInFS_FSStream on NbTapeCopiesInFS(FS, Stream)//

/* Used to create a row INTO NbTapeCopiesInFS whenever a new
   FileSystem is created 
CREATE TRIGGER tr_FileSystem_Insert
BEFORE INSERT ON FileSystem
FOR EACH ROW
BEGIN
  FOR item in (SELECT id FROM Stream) LOOP
    INSERT INTO NbTapeCopiesInFS (FS, Stream, NbTapeCopies) VALUES (:new.id, item.id, 0);
  END LOOP;
END;

/* Used to delete rows IN NbTapeCopiesInFS whenever a
   FileSystem is deleted 
CREATE TRIGGER tr_FileSystem_Delete
BEFORE DELETE ON FileSystem
FOR EACH ROW
BEGIN
  DELETE FROM NbTapeCopiesInFS WHERE FS = :old.id;
END;

/* Used to create a row INTO NbTapeCopiesInFS whenever a new
   Stream is created 
CREATE TRIGGER tr_Stream_Insert
BEFORE INSERT ON Stream
FOR EACH ROW
BEGIN
  FOR item in (SELECT id FROM FileSystem) LOOP
    INSERT INTO NbTapeCopiesInFS (FS, Stream, NbTapeCopies) VALUES (item.id, :new.id, 0);
  END LOOP;
END;

/* Used to delete rows IN NbTapeCopiesInFS whenever a
   Stream is deleted 
CREATE TRIGGER tr_Stream_Delete
BEFORE DELETE ON Stream
FOR EACH ROW
BEGIN
  DELETE FROM NbTapeCopiesInFS WHERE Stream = :old.id;
END;

/* Updates the count of tapecopies in NbTapeCopiesInFS
   whenever a TapeCopy is linked to a Stream 
CREATE TRIGGER tr_Stream2TapeCopy_Insert
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
   whenever a TapeCopy has failed to be migrated and is
   put back in WAITINSTREAM from the SELECTED status 
CREATE TRIGGER tr_TapeCopy_Update
AFTER UPDATE of status ON TapeCopy
FOR EACH ROW
WHEN (old.status = 3 AND new.status = 2) -- SELECTED AND WAITINSTREAMS
BEGIN
  UPDATE NbTapeCopiesInFS SET NbTapeCopies = NbTapeCopies + 1
   WHERE FS IN (SELECT DiskCopy.FileSystem
                  FROM DiskCopy, TapeCopy
                 WHERE DiskCopy.CastorFile = TapeCopy.castorFile
                   AND TapeCopy.id = :new.id)
     AND Stream IN (SELECT parent FROM Stream2TapeCopy WHERE child = :new.id);
END;

/* XXX update count into NbTapeCopiesInFS when a Disk2Disk copy occurs
   FOR a file in CANBEMIGR */

   
/***************************************/
/* Some triggers to prevent dead locks */
/***************************************/

/* Used to avoid LOCK TABLE TapeCopy whenever someone wants
   to deal with the tapeCopies on a CastorFile.
   Due to this trigger, locking the CastorFile is enough
   to be safe 
CREATE TRIGGER tr_TapeCopy_CastorFile
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
   to be safe
CREATE TRIGGER tr_DiskCopy_CastorFile
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
   to be safe
CREATE TRIGGER tr_Stream2TapeCopy_Stream
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
CREATE FUNCTION FileSystemRate(weight BIGINT, deltaWeight BIGINT, fsDeviation DOUBLE)
RETURNS DOUBLE DETERMINISTIC
BEGIN
  RETURN 1000*(weight + deltaWeight) - fsDeviation;
END//

/* FileSystem index based on the rate. */
-- CREATE INDEX I_FileSystem_Rate ON FileSystem(FileSystemRate(weight, deltaWeight, fsDeviation));    not supported



/*************************/
/* Procedure definitions */
/*************************/

/* MySQL method to make a SubRequest wait on another one, linked to the given DiskCopy */
CREATE PROCEDURE makeSubRequestWait(subreqId INT, dci INT)
BEGIN
 UPDATE SubRequest
  SET parent = (SELECT id FROM SubRequest WHERE diskCopy = dci AND parent = 0), -- all wait on the original one
      status = 5, lastModificationTime = getTime() -- WAITSUBREQ
  WHERE SubRequest.id = subreqId;
END//


/* MySQL method to archive a SubRequest and its request if needed */
CREATE PROCEDURE archiveSubReq(srId INT)
BEGIN
DECLARE rid INT;
DECLARE rtype INT;
DECLARE rclient INT;
DECLARE nb INT;
  -- update status of SubRequest
  UPDATE SubRequest SET status = 8 -- FINISHED
   WHERE id = srId;
  SELECT request INTO rid FROM SubRequest
   WHERE id = srId;
  -- Try to see whether another subrequest in the same
  -- request is still procesing
  SELECT count(*) INTO nb FROM SubRequest
   WHERE request = rid AND status != 8; -- FINISHED
  -- Archive request, client and SubRequests if needed
  IF nb = 0 THEN
    -- DELETE request from Id2Type
	SELECT type INTO rtype FROM Id2Type WHERE id = rid;
    DELETE FROM Id2Type WHERE id = rid;
    -- delete request and get client id
    CASE rtype
      WHEN 35 THEN -- StageGetRequest
	    SELECT client into rclient FROM StageGetRequest WHERE id = rid;
        DELETE FROM StageGetRequest WHERE id = rid;
      WHEN 40 THEN -- StagePutRequest
	    SELECT client into rclient FROM StagePutRequest WHERE id = rid;
        DELETE FROM StagePutRequest WHERE id = rid;
      WHEN 44 THEN -- StageUpdateRequest
	    SELECT client into rclient FROM StageUpdateRequest WHERE id = rid;
        DELETE FROM StageUpdateRequest WHERE id = rid;
      WHEN 39 THEN -- StagePutDoneRequest
	    SELECT client into rclient FROM StagePutDoneRequest WHERE id = rid;
        DELETE FROM StagePutDoneRequest WHERE id = rid;
      WHEN 42 THEN -- StageRmRequest
	    SELECT client into rclient FROM StageRmRequest WHERE id = rid;
        DELETE FROM StageRmRequest WHERE id = rid;
      WHEN 51 THEN -- StageReleaseFilesRequest
	    SELECT client into rclient FROM StageReleaseFilesRequest WHERE id = rid;
        DELETE FROM StageReleaseFilesRequest WHERE id = rid;
      WHEN 36 THEN -- StagePrepareToGetRequest
	    SELECT client into rclient FROM StagePrepareToGetRequest WHERE id = rid;
        DELETE FROM StagePrepareToGetRequest WHERE id = rid;
      WHEN 37 THEN -- StagePrepareToPutRequest
	    SELECT client into rclient FROM StagePrepareToPutRequest WHERE id = rid;
        DELETE FROM StagePrepareToPutRequest WHERE id = rid;
      WHEN 38 THEN -- StagePrepareToUpdateRequest
	    SELECT client into rclient FROM StagePrepareToUpdateRequest WHERE id = rid;
        DELETE FROM StagePrepareToUpdateRequest WHERE id = rid;
    END CASE;
    -- DELETE Client
    DELETE FROM Id2Type WHERE id = rclient;
    DELETE FROM Client WHERE id = rclient;
    -- Delete SubRequests
    DELETE FROM Id2Type WHERE id IN
      (SELECT id FROM SubRequest WHERE request = rid);
    DELETE FROM SubRequest WHERE request = rid;
  END IF;
END//


/* MySQL method implementing anyTapeCopyForStream.
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
 *   LIMIT 1;  */
CREATE PROCEDURE anyTapeCopyForStream(streamId INT, OUT res INT)
BEGIN
  DECLARE EXIT HANDLER FOR NOT FOUND  SET res = 0;
   SELECT NbTapeCopiesInFS.NbTapeCopies
     FROM NbTapeCopiesInFS
    WHERE NbTapeCopiesInFS.stream = streamId
      AND NbTapeCopiesInFS.NbTapeCopies > 0
    LIMIT 1;
   SET res = 1;
END//


/* MySQL method to update FileSystem weight for new streams */
CREATE PROCEDURE updateFsFileOpened(ds INT, fs INT, deviation INT, fileSize INT)
BEGIN
 UPDATE FileSystem SET deltaWeight = deltaWeight - deviation
  WHERE diskServer = ds;
 UPDATE FileSystem SET fsDeviation = 2 * deviation,
                       reservedSpace = reservedSpace + fileSize
  WHERE id = fs;
END//


/* MySQL method to update FileSystem free space when file are closed */
CREATE PROCEDURE updateFsFileClosed(fs INT, reservation INT, fileSize INT)
BEGIN
 UPDATE FileSystem SET deltaFree = deltaFree - fileSize,
                       reservedSpace = reservedSpace - reservation
  WHERE id = fs;
END//


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
DROP TABLE LockTable//
CREATE TABLE LockTable (DiskServerId BIGINT, TheLock BIGINT, PRIMARY KEY(DiskServerId))//
INSERT INTO LockTable SELECT id, id FROM DiskServer//

/* Used to create a row INTO LockTable whenever a new
   DiskServer is created 
CREATE TRIGGER tr_DiskServer_Insert
BEFORE INSERT ON DiskServer
FOR EACH ROW
BEGIN
  INSERT INTO LockTable (DiskServerId, TheLock) VALUES (:new.id, 0);
END;

/* Used to delete rows IN LockTable whenever a
   DiskServer is deleted 
CREATE TRIGGER tr_DiskServer_Delete
BEFORE DELETE ON DiskServer
FOR EACH ROW
BEGIN
  DELETE FROM LockTable WHERE DiskServerId = :old.id;
END;


/* MySQL method implementing updateFileSystemForJob */
CREATE PROCEDURE updateFileSystemForJob(fs VARCHAR(2048), ds VARCHAR(2048), fileSize BIGINT)
BEGIN
DECLARE fsID BIGINT;
DECLARE dsId BIGINT;
DECLARE dev BIGINT;
  SELECT FileSystem.id, FileSystem.fsDeviation,
         DiskServer.id INTO fsId, dev, dsId
    FROM FileSystem, DiskServer
   WHERE FileSystem.diskServer = DiskServer.id
     AND FileSystem.mountPoint = fs
     AND DiskServer.name = ds;
  -- We have to lock the DiskServer in the LockTable TABLE if we want
  -- to avoid dead locks with bestTapeCopyForStream. See the definition
  -- of the table for a complete explanation on why it exists
  SELECT TheLock FROM LockTable WHERE DiskServerId = dsId LOCK IN SHARE MODE;
  CALL updateFsFileOpened(dsId, fsId, dev, fileSize);
END//


/* MySQL method implementing bestTapeCopyForStream */
CREATE PROCEDURE bestTapeCopyForStream(streamId INT, OUT diskServerName VARCHAR(2048), OUT mountPoint VARCHAR(2048),
                                       OUT path VARCHAR(2048), OUT dci INT, OUT castorFileId INT, OUT fileId INT,
                                       OUT nsHost VARCHAR(2048), OUT fileSize INT, OUT tapeCopyId INT)
BEGIN
DECLARE fileSystemId INT;
DECLARE dsid BIGINT;
DECLARE deviation BIGINT;
DECLARE fsDiskServer BIGINT;
  -- We lock here a given DiskServer. See the comment for the creation of the LockTable
  -- table for a full explanation of why we need such a stupid UPDATE statement.
DECLARE EXIT HANDLER FOR NOT FOUND
  BEGIN
    -- No data found means the selected filesystem has no
    -- tapecopies to be migrated. Thus we go to next one
  END;
  UPDATE LockTable SET TheLock = 1
   WHERE DiskServerId =
   (SELECT DiskServer.id INTO dsid
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
       LIMIT 1);
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
     LIMIT 1;
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
     LIMIT 1;
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
  CALL updateFsFileOpened(fsDiskServer, fileSystemId, deviation, 0);
END//


/* MySQL method implementing bestFileSystemForSegment */
CREATE PROCEDURE bestFileSystemForSegment(segmentId BIGINT, OUT diskServerName VARCHAR(2048),
                                          OUT rmountPoint VARCHAR(2048), OUT rpath VARCHAR(2048), OUT dci BIGINT)
-- @todo doesn't compile?? cannot make the union select?
BEGIN
DECLARE fileSystemId BIGINT;
DECLARE castorFileId BIGINT;
DECLARE deviation BIGINT;
DECLARE fsDiskServer BIGINT;
DECLARE fileSize BIGINT;
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
   DECLARE EXIT HANDLER FOR NOT FOUND
       BEGIN
       -- Error, the filesystem or the machine was probably disabled in between
	   SET rpath = "DISABLED";
	   SET dci = -1;
       -- raise_application_error(-20101, 'In a multi-segment file, FileSystem or Machine was disabled before all segments were recalled');
       END;
   -- it had one, force filesystem selection, unless it was disabled.
   SELECT DiskServer.name, DiskServer.id, FileSystem.mountPoint, FileSystem.fsDeviation
     INTO diskServerName, fsDiskServer, rmountPoint, deviation
     FROM DiskServer, FileSystem
    WHERE FileSystem.id = fileSystemId
      AND FileSystem.status = 0 -- FILESYSTEM_PRODUCTION
      AND DiskServer.id = FileSystem.diskServer
      AND DiskServer.status = 0; -- DISKSERVER_PRODUCTION
   CALL updateFsFileOpened(fsDiskServer, fileSystemId, deviation, 0);
   END;
 ELSE
   DECLARE CURSOR c1 FOR SELECT DiskServer.name, FileSystem.mountPoint, FileSystem.id,
                       FileSystem.fsDeviation, FileSystem.diskserver, SubRequest.xsize
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
   OPEN c1;
   FETCH c1 INTO diskServerName, rmountPoint, fileSystemId, deviation, fsDiskServer, fileSize;
   CLOSE c1;
   UPDATE DiskCopy SET fileSystem = fileSystemId WHERE id = dci;
   CALL updateFsFileOpened(fsDiskServer, fileSystemId, deviation, fileSize);
 END IF;
END//


/* MySQL method implementing fileRecalled */
CREATE PROCEDURE fileRecalled(tapecopyId BIGINT)
BEGIN
DECLARE SubRequestId BIGINT;
DECLARE dci BIGINT;
DECLARE fsId BIGINT;
DECLARE fileSize BIGINT;
 SELECT SubRequest.id, DiskCopy.id, SubRequest.xsize INTO SubRequestId, dci, fileSize
   FROM TapeCopy, SubRequest, DiskCopy
  WHERE TapeCopy.id = tapecopyId
    AND DiskCopy.castorFile = TapeCopy.castorFile
    AND SubRequest.diskcopy = DiskCopy.id;
 UPDATE DiskCopy SET status = 0 WHERE id = dci;  -- DISKCOPY_STAGED
 SELECT fileSystem INTO fsid FROM DiskCopy WHERE id = dci;
 UPDATE SubRequest SET status = 1, lastModificationTime = getTime(), parent = 0
  WHERE id = SubRequestId;     -- SUBREQUEST_RESTART
 UPDATE SubRequest SET status = 1, lastModificationTime = getTime(), parent = 0
  WHERE parent = SubRequestId; -- SUBREQUEST_RESTART
 CALL updateFsFileClosed(fsId, fileSize, fileSize);
END//


/* MySQL method implementing fileRecallFailed */
CREATE PROCEDURE fileRecallFailed(tapecopyId BIGINT)
BEGIN
DECLARE SubRequestId BIGINT;
DECLARE dci BIGINT;
DECLARE fsId BIGINT;
DECLARE fileSize BIGINT;
SELECT SubRequest.id, DiskCopy.id, SubRequest.xsize INTO SubRequestId, dci, fileSize
  FROM TapeCopy, SubRequest, DiskCopy
 WHERE TapeCopy.id = tapecopyId
   AND DiskCopy.castorFile = TapeCopy.castorFile
   AND SubRequest.diskcopy = DiskCopy.id;
 UPDATE DiskCopy SET status = 4 WHERE id = dci;  -- DISKCOPY_FAILED
 SELECT fileSystem INTO fsid FROM DiskCopy WHERE id = dci;
 UPDATE SubRequest SET status = 7, -- SUBREQUEST_FAILED
                      lastModificationTime = getTime()
  WHERE id = SubRequestId;
 UPDATE SubRequest SET status = 7, -- SUBREQUEST_FAILED
                      lastModificationTime = getTime()
  WHERE parent = SubRequestId;
END//


/* custom package not supported in MySQL 
CREATE PACKAGE castor AS
  TYPE DiskCopyCore IS RECORD (id INT, path VARCHAR(2048), status BIGINT, fsWeight BIGINT, mountPoint VARCHAR(2048), diskServer VARCHAR(2048));
  TYPE DiskCopy_Cur IS REF CURSOR RETURN DiskCopyCore;
  TYPE Segment_Cur IS REF CURSOR RETURN Segment%ROWTYPE;
  TYPE GCLocalFileCore IS RECORD (fileName VARCHAR(2048), diskCopyId INT);
  TYPE GCLocalFiles_Cur IS REF CURSOR RETURN GCLocalFileCore;
  TYPE "strList" IS TABLE OF VARCHAR(2048) index by binary_integer;
  TYPE "cnumList" IS TABLE OF BIGINT index by binary_integer;
END castor;
CREATE TYPE "numList" IS TABLE OF INT;

/* MySQL method implementing isSubRequestToSchedule */
CREATE PROCEDURE isSubRequestToSchedule(rsubreqId BIGINT, OUT result INT /*, sources OUT Cursor */)
BEGIN
DECLARE dcCount INT;
DECLARE dci BIGINT;
 SELECT COUNT(*) INTO dcCount FROM DiskCopy, SubRequest, FileSystem, DiskServer
  WHERE SubRequest.id = rsubreqId
   AND SubRequest.castorfile = DiskCopy.castorfile
   AND DiskCopy.fileSystem = FileSystem.id
   AND FileSystem.status = 0 -- PRODUCTION
   AND FileSystem.diskserver = DiskServer.id
   AND DiskServer.status = 0 -- PRODUCTION
   AND DiskCopy.status IN (0, 6, 10); -- STAGED, STAGEOUT, CANBEMIGR
 IF dcCount > 0 THEN
   SET result = 1;  -- schedule and diskcopies available
   -- OPEN sources FOR
   SELECT DiskCopy.id, DiskCopy.path, DiskCopy.status, FileSystem.weight, FileSystem.mountPoint, DiskServer.name
     FROM DiskCopy, SubRequest, FileSystem, DiskServer
    WHERE SubRequest.id = rsubreqId
      AND SubRequest.castorfile = DiskCopy.castorfile
      AND DiskCopy.status IN (0, 6, 10) -- STAGED, STAGEOUT, CANBEMIGR
      AND FileSystem.id = DiskCopy.fileSystem
      AND FileSystem.status = 0 -- PRODUCTION
      AND DiskServer.id = FileSystem.diskServer
      AND DiskServer.status = 0; -- PRODUCTION
  ELSE
     BEGIN
     DECLARE EXIT HANDLER FOR NOT FOUND
	    BEGIN
		-- If the subsequent query fails, i.e. no disk copies in WAIT*, schedule for recall
		SET result = 2;
		END;
	 SELECT DiskCopy.id INTO dci FROM DiskCopy, SubRequest, FileSystem, DiskServer
	  WHERE SubRequest.id = rsubreqId
	   AND SubRequest.castorfile = DiskCopy.castorfile
	   AND DiskCopy.fileSystem = FileSystem.id
	   AND FileSystem.status = 0 -- PRODUCTION
	   AND FileSystem.diskserver = DiskServer.id
	   AND DiskServer.status = 0 -- PRODUCTION
       AND DiskCopy.status IN (1, 2, 5, 11); -- WAITDISK2DISKCOPY, WAITTAPERECALL, WAITFS, WAITFS_SCHEDULING
     -- Only DiskCopy is in WAIT*, make SubRequest wait on previous subrequest and do not schedule
     UPDATE SubRequest
        SET parent = (SELECT id FROM SubRequest WHERE diskCopy = dci),
            status = 5, -- WAITSUBREQ
            lastModificationTime = getTime()
      WHERE id = rsubreqId;
     SET result = 0;  -- no schedule
	 END;
   END IF;
END//


/* Build diskCopy path from fileId */
CREATE PROCEDURE buildPathFromFileId(fid BIGINT, nsHost VARCHAR(2048), dcid BIGINT, OUT path VARCHAR(2048))
BEGIN
  SET path = CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(MOD(fid,100),'FM09'), '/'), CONCAT(fid, '@')), nsHost), CONCAT('.', dcid));
END//


/* MySQL method implementing getUpdateStart */
CREATE PROCEDURE getUpdateStart(srId INT, fileSystemId INT, OUT dci BIGINT, OUT rpath VARCHAR(2048),
                                OUT rstatus INT, /*sources OUT castor.DiskCopy_Cur,*/ OUT reuid BIGINT, OUT regid BIGINT)
BEGIN
DECLARE cfid INT;
DECLARE fid INT;
DECLARE nh VARCHAR(2048);
DECLARE EXIT HANDLER FOR NOT FOUND
  BEGIN -- No disk copy found on selected FileSystem, look in others

  DECLARE EXIT HANDLER FOR NOT FOUND
     BEGIN -- No disk copy found on any FileSystem
     -- create one for recall
	 CALL seqNextVal(dci);
     UPDATE SubRequest SET diskCopy = dci, status = 4,
                        lastModificationTime = getTime() -- WAITTAPERECALL
       WHERE id = srId;
	 SELECT castorFile INTO cfid FROM SubRequest WHERE id = srId;
     SELECT fileId, nsHost INTO fid, nh FROM CastorFile WHERE id = cfid;
     CALL buildPathFromFileId(fid, nh, dci, rpath);
     INSERT INTO DiskCopy (path, id, FileSystem, castorFile, status, creationTime)
       VALUES (rpath, dci, fileSystemId, cfid, 2, getTime()); -- status WAITTAPERECALL
     INSERT INTO Id2Type (id, type) VALUES (dci, 5); -- OBJ_DiskCopy
     SET rstatus = 99; -- WAITTAPERECALL, NEWLY CREATED
     END; -- 2nd EXIT HANDLER

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
    LIMIT 1;
  -- Found a DiskCopy, Check whether to wait on it
  IF rstatus IN (2,5) THEN -- WAITTAPERECALL, WAITFS, Make SubRequest Wait
    CALL makeSubRequestWait(srId, dci);
    SET dci = 0;
    SET rpath = '';
  ELSE
    -- create DiskCopy for Disk to Disk copy
    CALL seqNextVal(dci);
    UPDATE SubRequest SET diskCopy = dci,
                          lastModificationTime = getTime() WHERE id = srId;
    SELECT castorFile INTO cfid FROM SubRequest WHERE id = srId;
    INSERT INTO DiskCopy (path, id, FileSystem, castorFile, status, creationTime)
      VALUES (rpath, dci, fileSystemId, cfid, 1, getTime()); -- status WAITDISK2DISKCOPY
    INSERT INTO Id2Type (id, type) VALUES (dci, 5); -- OBJ_DiskCopy
    SET rstatus = 1; -- status WAITDISK2DISKCOPY

	-- OPEN sources FOR
    SELECT DiskCopy.id, DiskCopy.path, DiskCopy.status, FileSystem.weight, FileSystem.mountPoint, DiskServer.name
	  FROM DiskCopy, SubRequest, FileSystem, DiskServer
     WHERE SubRequest.id = srId
       AND SubRequest.castorfile = DiskCopy.castorfile
       AND DiskCopy.status IN (0, 1, 2, 5, 6, 10, 11) -- STAGED, WAITDISKTODISKCOPY, WAITTAPERECALL, WAIFS, STAGEOUT, CANBEMIGR, WAITFS_SCHEDULING
       AND FileSystem.id = DiskCopy.fileSystem
       AND FileSystem.status = 0 -- PRODUCTION
       AND DiskServer.id = FileSystem.diskServer
       AND DiskServer.status = 0; -- PRODUCTION
   END IF;
 END;  -- 1st EXIT HANDLER
 
 -- Get and uid, gid
 SELECT euid, egid INTO reuid, regid FROM SubRequest,
      (SELECT id, euid, egid from StageGetRequest UNION
       SELECT id, euid, egid from StagePrepareToGetRequest UNION
       SELECT id, euid, egid from StageUpdateRequest UNION
       SELECT id, euid, egid from StagePrepareToUpdateRequest) Request
  WHERE SubRequest.request = Request.id AND SubRequest.id = srId;
 -- Take a lock on the CastorFile. Associated with triggers,
 -- this guarantee we are the only ones dealing with its copies
 SELECT CastorFile.* FROM CastorFile, SubRequest
  WHERE CastorFile.id = SubRequest.castorFile
    AND SubRequest.id = srId LOCK IN SHARE MODE;
 -- Try to find local DiskCopy
 SET dci = 0;
 SELECT DiskCopy.id, DiskCopy.path, DiskCopy.status
  INTO dci, rpath, rstatus
  FROM DiskCopy, SubRequest
  WHERE SubRequest.id = srId
    AND SubRequest.castorfile = DiskCopy.castorfile
    AND DiskCopy.filesystem = fileSystemId
    AND DiskCopy.status IN (0, 1, 2, 5, 6, 10, 11); -- STAGED, WAITDISKTODISKCOPY, WAITTAPERECALL, WAIFS, STAGEOUT, CANBEMIGR, WAITFS_SCHEDULING
 -- If nothing found -> 1st EXIT HANDLER
 -- If found local one, check whether to wait on it
 IF rstatus IN (1, 2, 5) THEN -- WAITDISK2DISKCOPY, WAITTAPERECALL, WAITFS, Make SubRequest Wait
   CALL makeSubRequestWait(srId, dci);
   SET dci = 0;
   SET rpath = '';
 END IF;
END//


/* MySQL method implementing putStart */
CREATE PROCEDURE putStart(srId BIGINT, fileSystemId BIGINT, OUT rdcId BIGINT, OUT rdcStatus INT, OUT rdcPath VARCHAR(2048))
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
  WHERE id = rdcId;
 SELECT status, path INTO rdcStatus, rdcPath
  FROM DiskCopy
  WHERE id = rdcId;
END//


/* MySQL method implementing putDoneStart */
CREATE PROCEDURE putDoneStart(srId BIGINT, fileSystemId BIGINT,
                              OUT rdcId BIGINT, OUT rdcStatus INTEGER, OUT rdcPath VARCHAR(1000))
BEGIN
 -- Get diskCopy Id
 SELECT DiskCopy.id, DiskCopy.status, DiskCopy.path
   INTO rdcId, rdcStatus, rdcPath
   FROM SubRequest, DiskCopy
  WHERE SubRequest.id = srId
    AND DiskCopy.id = SubRequest.diskCopy;
END//


/* MySQL method implementing updateAndCheckSubRequest */
CREATE PROCEDURE updateAndCheckSubRequest(srId BIGINT, newStatus INT, OUT result INT)
BEGIN
 DECLARE reqId INT;
 DECLARE EXIT HANDLER FOR NOT FOUND
   BEGIN  
   -- No data found means we were last
   SET result = 0;
   END;
 -- Lock the access to the Request
 SELECT Id2Type.id INTO reqId
  FROM SubRequest, Id2Type
  WHERE SubRequest.id = srId
  AND Id2Type.id = SubRequest.request
  FOR UPDATE;
 -- Update Status
 UPDATE SubRequest SET status = newStatus,
                       lastModificationTime = getTime() WHERE id = srId;
 -- Check whether it was the last subrequest in the request
 SELECT id INTO result FROM SubRequest
  WHERE request = reqId
    AND status NOT IN (6, 7, 10, 9) -- READY, FAILED, FAILED_ANSWERING, FAILED_FINISHED
    LIMIT 1;
END//


/* MySQL method implementing disk2DiskCopyDone */
CREATE PROCEDURE disk2DiskCopyDone(dcId BIGINT, dcStatus INT)
BEGIN
  -- update DiskCopy
  UPDATE DiskCopy set status = dcStatus WHERE id = dcId; -- status DISKCOPY_STAGED
  -- update SubRequest
  UPDATE SubRequest set status = 6,
                        lastModificationTime = getTime()
   WHERE diskCopy = dcId; -- status SUBREQUEST_READY
END//


/* MySQL method implementing recreateCastorFile */
CREATE PROCEDURE recreateCastorFile(cfId BIGINT, srId BIGINT, 
                                    OUT dcId BIGINT, OUT rstatus INTEGER, OUT rmountPoint VARCHAR(2048), OUT rdiskServer VARCHAR(2048))
-- @todo doesn't compile??
BEGIN
DECLARE rpath VARCHAR(2048);
DECLARE nbRes INT;
DECLARE fid BIGINT;
DECLARE nh VARCHAR(2048);
DECLARE contextPIPP INT;
LABEL recreateCastorFileLbl;
 START TRANSACTION;
 -- Lock the access to the CastorFile
 -- This, together with triggers will avoid new TapeCopies
 -- or DiskCopies to be added
 SELECT * FROM CastorFile WHERE id = cfId LOCK IN SHARE MODE;
 -- Determine the context (Put inside PrepareToPut ?)
 BEGIN
 DECLARE EXIT HANDLER FOR NOT FOUND
         SET contextPIPP = 0;
   -- check that we are a Put
   SELECT StagePutRequest.id
     FROM StagePutRequest, SubRequest
    WHERE SubRequest.id = srId
      AND StagePutRequest.id = SubRequest.request;
   -- check that there is a PrepareToPut going on
   SELECT SubRequest.diskCopy INTO dcId
     FROM StagePrepareToPutRequest, SubRequest
    WHERE SubRequest.CastorFile = cfId
      AND StagePrepareToPutRequest.id = SubRequest.request;
   -- if we got here, we are a Put inside a PrepareToPut
   SET contextPIPP = 1;
 END;
 IF contextPIPP = 0 THEN
  -- check if recreation is possible for TapeCopies
  SELECT count(*) INTO nbRes FROM TapeCopy
    WHERE status = 3 -- TAPECOPY_SELECTED
    AND castorFile = cfId;
  IF nbRes > 0 THEN
    -- We found something, thus we cannot recreate
    SET dcId = 0;
    LEAVE recreateCastorFileLbl;
    RETURN;
  END IF;
  -- check if recreation is possible for DiskCopies
  SELECT count(*) INTO nbRes FROM DiskCopy
    WHERE status IN (1, 2, 5, 6) -- WAITDISK2DISKCOPY, WAITTAPERECALL, WAITFS, STAGEOUT
    AND castorFile = cfId;
  IF nbRes > 0 THEN
    -- We found something, thus we cannot recreate
    SET dcId = 0;
    LEAVE recreateCastorFileLbl;
    RETURN;
  END IF;
  -- delete all tapeCopies
  DELETE from TapeCopy WHERE castorFile = cfId;
  -- set DiskCopies to INVALID
  UPDATE DiskCopy SET status = 7 -- INVALID
   WHERE castorFile = cfId AND status = 1; -- STAGED
  -- create new DiskCopy
  SELECT fileId, nsHost INTO fid, nh FROM CastorFile WHERE id = cfId;
  CALL seqNextVal(dcId);
  CALL buildPathFromFileId(fid, nh, dcId, rpath);
  INSERT INTO DiskCopy (path, id, FileSystem, castorFile, status, creationTime)
    VALUES (rpath, dcId, 0, cfId, 5, getTime()); -- status WAITFS
  SET rstatus = 5; -- WAITFS
  SET rmountPoint = '';
  SET rdiskServer = '';
  INSERT INTO Id2Type (id, type) VALUES (dcId, 5); -- OBJ_DiskCopy
  COMMIT;
 ELSE
  BEGIN
   DECLARE fsId BIGINT;
   DECLARE dsId BIGINT;
   -- Retrieve the infos about the DiskCopy to be used
   SELECT fileSystem, status INTO rstatus, fsId FROM DiskCopy WHERE id = dcId;
   SELECT mountPoint, diskServer INTO rmountPoint, dsId
     FROM FileSystem WHERE FileSystem.id = fsId;
   SELECT name INTO rdiskServer FROM DiskServer WHERE id = dsId;
   -- See whether we should wait on the previous Put Request
   IF rstatus = 11 THEN -- WAITFS_SCHEDULING
    CALL makeSubRequestWait(srId, dcId);
   END IF;
  END;
 END IF; 
 -- link SubRequest and DiskCopy
 UPDATE SubRequest SET diskCopy = dcId,
                       lastModificationTime = getTime() WHERE id = srId;
 COMMIT;
END//


/* MySQL method putDoneFunc */
CREATE PROCEDURE putDoneFunc(cfId BIGINT, fs BIGINT)
BEGIN
DECLARE nc INT;
DECLARE tcId INT;
DECLARE fsId INT;
DECLARE i INT;
 -- get number of TapeCopies to create
 SELECT nbCopies INTO nc FROM FileClass, CastorFile
  WHERE CastorFile.id = cfId AND CastorFile.fileClass = FileClass.id;
 -- if no tape copy or 0 length file, no migration
 -- so we go directly to status STAGED
 IF nc = 0 OR fs = 0 THEN
   UPDATE DiskCopy SET status = 0 -- STAGED
    WHERE castorFile = cfId AND status = 6; -- STAGEOUT
   SELECT fileSystem INTO fsId
    FROM DiskCopy
    WHERE castorFile = cfId AND status = 0; -- STAGED
 ELSE
   -- update the DiskCopy status to CANBEMIGR
   UPDATE DiskCopy SET status = 10 -- CANBEMIGR
    WHERE castorFile = cfId AND status = 6; -- STAGEOUT
   SELECT fileSystem INTO fsId
    FROM DiskCopy
    WHERE castorFile = cfId AND status = 10; -- CANBEMIGR
   -- Create TapeCopies
   SET i = 1;
   WHILE i <= nc DO
    CALL seqNextVal(tcId);
    INSERT INTO TapeCopy (id, copyNb, castorFile, status)
      VALUES (tcId, i, cfId, 0); -- TAPECOPY_CREATED
    INSERT INTO Id2Type (id, type) VALUES (tcId, 30); -- OBJ_TapeCopy
	SET i = i + 1;
   END WHILE;
 END IF;
END//


/* MySQL method implementing prepareForMigration */
CREATE PROCEDURE prepareForMigration(srId BIGINT, fs BIGINT, OUT fId BIGINT, OUT nh VARCHAR(2048), OUT userId BIGINT, OUT groupId BIGINT)
BEGIN
DECLARE cfId BIGINT;
DECLARE fsId BIGINT;
DECLARE reservedSpace BIGINT;
DECLARE unused INT;
 -- get CastorFile
 SELECT castorFile INTO cfId FROM SubRequest where id = srId;
 -- update CastorFile. This also takes a lock on it, insuring
 -- with triggers that we are the only ones to deal with its copies
 UPDATE CastorFile set fileSize = fs WHERE id = cfId;
 SELECT fileId, nsHost INTO fId, nh FROM CastorFile WHERE id = cfId;
 -- get uid, gid and reserved space from Request
 SELECT euid, egid, xsize INTO userId, groupId, reservedSpace FROM SubRequest,
      (SELECT euid, egid, id FROM StagePutRequest UNION
       SELECT euid, egid, id FROM StagePrepareToPutRequest) Request
  WHERE SubRequest.request = Request.id AND SubRequest.id = srId;
 -- update the FileSystem free space
 SELECT fileSystem into fsId from DiskCopy
  WHERE castorFile = cfId AND status = 6;
 CALL updateFsFileClosed(fsId, reservedSpace, fs);
 -- archive Subrequest
 CALL archiveSubReq(srId);
 -- if not inside a prepareToPut, create TapeCopies and update DiskCopy status
 BEGIN
   DECLARE EXIT HANDLER FOR NOT FOUND
     BEGIN
     CALL putDoneFunc(cfId, fs);
     END;
   SELECT StagePrepareToPutRequest.id INTO unused
     FROM StagePrepareToPutRequest, SubRequest
    WHERE StagePrepareToPutRequest.id = SubRequest.request
      AND SubRequest.castorFile = cfId;
 END;
 COMMIT;
END//


/* MySQL method implementing selectCastorFile */
CREATE PROCEDURE selectCastorFile(fId BIGINT, nh VARCHAR(2048), sc BIGINT, fc BIGINT, fs BIGINT, OUT rid BIGINT, OUT rfs BIGINT)
BEGIN
  DECLARE CONSTRAINT_VIOLATED CONDITION FOR SQLSTATE '23000';
  DECLARE EXIT HANDLER FOR CONSTRAINT_VIOLATED
    BEGIN
    -- retry the select since a creation was done in between
    SELECT id, fileSize INTO rid, rfs FROM CastorFile
      WHERE fileId = fid AND nsHost = nh;
	END;
  DECLARE EXIT HANDLER FOR NOT FOUND
	BEGIN
    -- insert new row
	CALL seqNextVal(rid);
    INSERT INTO CastorFile (id, fileId, nsHost, svcClass, fileClass, fileSize,
                            creationTime, lastAccessTime, nbAccesses)
      VALUES (rid, fId, nh, sc, fc, fs, getTime(), getTime(), 1);
      SET rfs = fileSize;
    INSERT INTO Id2Type (id, type) VALUES (rid, 2); -- OBJ_CastorFile
    END;
  -- try to find an existing file
  SELECT id, fileSize INTO rid, rfs FROM CastorFile
    WHERE fileId = fid AND nsHost = nh;
  -- update lastAccess time
  UPDATE CastorFile SET LastAccessTime = getTime(), nbAccesses = nbAccesses + 1
    WHERE id = rid;
END//


/* MySQL method implementing resetStream */
CREATE PROCEDURE resetStream(sid BIGINT)
BEGIN
  DECLARE nbRes BIGINT;
  BEGIN
  DECLARE EXIT HANDLER FOR NOT FOUND
      BEGIN  
      -- We've found nothing, delete stream
      DELETE FROM Stream2TapeCopy WHERE Parent = sid;
      DELETE FROM Stream WHERE id = sid;
      END;
    -- First lock the stream
    SELECT * FROM Stream WHERE id = sid LOCK IN SHARE MODE;
    -- Selecting any column with hint LIMIT and relying
    -- on the exception mechanism in case nothing is found is
    -- far better than issuing a SELECT count(*) because ORACLE
    -- (and probably MySQL as well) will then ignore the LIMIT and take ages...
    SELECT Tapecopy.id INTO nbRes
      FROM Stream2TapeCopy, TapeCopy
      WHERE Stream2TapeCopy.Parent = sid
        AND Stream2TapeCopy.Child = TapeCopy.id
        AND status = 2 -- TAPECOPY_WAITINSTREAMS
        LIMIT 1;
    -- We'we found one, update stream status
    UPDATE Stream SET status = 0, tape = 0 WHERE id = sid; -- STREAM_PENDING
  END;
  -- in any case, unlink tape and stream
  UPDATE Tape SET Stream = 0 WHERE Stream = sid;
END//


/* MySQL method implementing bestFileSystemForJob */
CREATE PROCEDURE bestFileSystemForJob(/*fileSystems IN castor."strList", machines IN castor."strList", minFree IN castor."cnumList",*/
									fileSystems INT, machines INT, minFree INT,            -- flag to say if the correspondent temporary table is filled or not
									OUT rMountPoint VARCHAR(2048), OUT rDiskServer VARCHAR(2048))
BEGIN
DECLARE ds BIGINT;
DECLARE fs BIGINT;
DECLARE dev BIGINT;
-- @todo convert custom types
 TYPE cursorContent IS RECORD
   (mountPoint VARCHAR(2048), dsName VARCHAR(2048),
    dsId BIGINT, fsId BIGINT, deviation BIGINT);
 TYPE AnyCursor IS REF CURSOR RETURN cursorContent;
 c1 AnyCursor;
BEGIN
 IF fileSystems > 0 THEN
  -- here machines AND filesystems should be given
  DECLARE fsIds "numList" := "numList"();
  DECLARE nextIndex BIGINT DEFAULT 1;
  BEGIN
   fsIds.EXTEND(fileSystems.COUNT);
   FOR i in fileSystems.FIRST .. fileSystems.LAST LOOP
    DECLARE CONTINUE HANDLER FOR NOT FOUND
       BEGIN
	   NULL;
	   END
     SELECT FileSystem.id INTO fsIds(nextIndex)
        FROM FileSystem, DiskServer
       WHERE FileSystem.mountPoint = fileSystems(i)
         AND DiskServer.name = machines(i)
         AND FileSystem.diskServer = DiskServer.id
         AND minFree(i) <= FileSystem.free + FileSystem.deltaFree - FileSystem.reservedSpace
         AND DiskServer.status IN (0, 1) -- DISKSERVER_PRODUCTION, DISKSERVER_DRAINING
         AND FileSystem.status IN (0, 1); -- FILESYSTEM_PRODUCTION, FILESYSTEM_DRAINING
     SET nextIndex = nextIndex + 1;
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
   DECLARE mIds "numList" := "numList"();
   DECLARE nextIndex BIGINT DEFAULT 1;
   BEGIN
    mIds.EXTEND(machines.COUNT);
    FOR i in machines.FIRST .. machines.LAST LOOP
      DECLARE CONTINUE HANDLER FOR NOT FOUND
	     BEGIN
	     NULL;
		 END;
	  BEGIN 
	  SELECT DiskServer.id INTO mIds(nextIndex)
        FROM DiskServer
       WHERE DiskServer.name = machines(i)
         AND DiskServer.status IN (0, 1); -- DISKSERVER_PRODUCTION, DISKSERVER_DRAINING
      SET nextIndex = nextIndex + 1;
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
END//


/* MySQL method implementing anySegmentsForTape */
CREATE PROCEDURE anySegmentsForTape(tapeId BIGINT, OUT nb INT)
BEGIN
  SELECT count(*) INTO nb FROM Segment
  WHERE Segment.tape = tapeId
    AND Segment.status = 0;
  IF nb > 0 THEN
    UPDATE Tape SET status = 3 -- WAITMOUNT
    WHERE id = tapeId;
  END IF;
END//


/* MySQL method implementing segmentsForTape */
CREATE PROCEDURE segmentsForTape(tapeId BIGINT /* segments OUT castor.Segment_Cur */)
BEGIN
DECLARE segCount INT;
  SELECT Segment.id FROM Segment   -- BULK COLLECT not supported in MySql
   WHERE Segment.tape = tapeId
     AND Segment.status = 0 FOR UPDATE;
  SELECT count(*) INTO segCount FROM Segment
   WHERE Segment.tape = tapeId
     AND Segment.status = 0;
  IF segCount > 0 THEN
    UPDATE Tape SET status = 4 -- MOUNTED
     WHERE id = tapeId;
    UPDATE Segment set status = 7 -- SELECTED
     WHERE tape = tapeId;
   END IF;
  -- OPEN segments FOR 
  SELECT * FROM Segment WHERE Segment.tape = tapeId;
END//

/* MySQL method implementing selectFiles2Delete */
CREATE PROCEDURE selectFiles2Delete(DiskServerName VARCHAR(2048) /* GCLocalFiles OUT castor.GCLocalFiles_Cur */)
BEGIN
  /*
  SELECT DiskCopy.id   -- BULK COLLECT not supported in MySql
    FROM DiskCopy, FileSystem, DiskServer
   WHERE DiskCopy.fileSystem = FileSystem.id
     AND FileSystem.DiskServer = DiskServer.id
     AND DiskServer.name = DiskServerName
     AND DiskCopy.status = 8 -- GC_CANDIDATE
     FOR UPDATE;
  */
  UPDATE DiskCopy, FileSystem, DiskServer 
   SET DiskCopy.status = 9 -- BEING_DELETED
   WHERE DiskCopy.fileSystem = FileSystem.id
     AND FileSystem.DiskServer = DiskServer.id
     AND DiskServer.name = DiskServerName
     AND DiskCopy.status = 8; -- GC_CANDIDATE

  SELECT FileSystem.mountPoint||DiskCopy.path, DiskCopy.id 
    FROM DiskCopy, FileSystem, DiskServer
   WHERE DiskCopy.fileSystem = FileSystem.id
     AND FileSystem.DiskServer = DiskServer.id
     AND DiskServer.name = DiskServerName
     AND DiskCopy.status = 9; -- BEING_DELETED
END//


/* MySQL method implementing filesDeleted */
CREATE PROCEDURE filesDeletedProc(/* fileIds castor."cnumList" */)
BEGIN
DECLARE cfId BIGINT;
DECLARE nb BIGINT;
DECLARE fileId BIGINT;
DECLARE done INT DEFAULT 0;
DECLARE fileIds CURSOR FOR SELECT id FROM FileIds_temp;
DECLARE EXIT HANDLER FOR NOT FOUND SET done = 1;
  OPEN fileIds;
  REPEAT
  -- Loop over the deleted files
  FETCH fileIds INTO fileId;
  IF NOT done THEN
    -- delete the DiskCopy
	SELECT castorFile INTO cfId FROM DiskCopy WHERE id = fileId;
    DELETE FROM DiskCopy WHERE id = fileId;
    -- Lock the Castor File
    SELECT id INTO cfId FROM CastorFile where id = cfID FOR UPDATE;
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
  END IF;
  UNTIL done END REPEAT;
END//


/* MySQL method implementing filesDeletionFailed */
/* @todo to be entirely moved to C++: just make the query
UPDATE DiskCopy SET status = 4 WHERE id in (<generated list of ids>)

PROCEDURE filesDeletionFailedProc(fileIds IN castor."cnumList")
BEGIN
DECLARE cfId BIGINT;
DECLARE nb INT;
 IF fileIds.COUNT > 0 THEN
  -- Loop over the deleted files
  FOR i in fileIds.FIRST .. fileIds.LAST LOOP
    -- set status of DiskCopy to FAILED
    UPDATE DiskCopy SET status = 4 -- FAILED
     WHERE id = fileIds(i);
  END LOOP;
 END IF;
END;
*/


/* MySQL method implementing getUpdateDone */
CREATE PROCEDURE getUpdateDoneProc(subReqId BIGINT)
BEGIN
  CALL archiveSubReq(subReqId);
END//

/* MySQL method implementing getUpdateFailed */
CREATE PROCEDURE getUpdateFailedProc(subReqId BIGINT)
BEGIN
  UPDATE SubRequest SET status = 7 -- FAILED
   WHERE id = subReqId;
END//

/* MySQL method implementing putFailedProc */
CREATE PROCEDURE putFailedProc(subReqId BIGINT)
BEGIN
  UPDATE SubRequest SET status = 7 -- FAILED
   WHERE id = subReqId;
END//

/* MySQL method implementing failedSegments */
PROCEDURE failedSegments(/* segments OUT castor.Segment_Cur */)
BEGIN
  -- OPEN segments FOR 
  SELECT * FROM Segment
  WHERE Segment.status = 6; -- SEGMENT_FAILED
END//


/* MySQL method implementing stageRm */
CREATE PROCEDURE stageRm (fid INT, nh VARCHAR(2048), OUT ret INT)
BEGIN
DECLARE cfId BIGINT;
DECLARE nbRes INT;
LABEL exitLbl;   -- @todo doesn't compile??
 -- Lock the access to the CastorFile
 -- This, together with triggers will avoid new TapeCopies
 -- or DiskCopies to be added  --- @todo but triggers are not implemented here
 SELECT id INTO cfId FROM CastorFile
   WHERE fileId = fid AND nsHost = nh FOR UPDATE;
 -- check if removal is possible for TapeCopies
 SELECT count(*) INTO nbRes FROM TapeCopy
   WHERE status = 3 -- TAPECOPY_SELECTED
   AND castorFile = cfId;
 IF nbRes > 0 THEN
   -- We found something, thus we cannot recreate
   SET ret = 1;
   LEAVE exitLbl;
 END IF;
 -- check if recreation is possible for SubRequests
 SELECT count(*) INTO nbRes FROM SubRequest
   WHERE castorFile = cfId;
 IF nbRes > 0 THEN
   -- We found something, thus we cannot recreate
   SET ret = 2;
   LEAVE exitLbl;
 END IF;
 -- set DiskCopies to GCCANDIDATE
 UPDATE DiskCopy SET status = 8 -- GCCANDIDATE
   WHERE castorFile = cfId AND status = 0; -- STAGED
 SET ret = 0;
END//



---------------------------------------------------------------------------------------------
-- This procedure is temporarily here to fill in some dummy data

DROP PROCEDURE IF EXIST dummyFill//
CREATE PROCEDURE dummyFill()
BEGIN
  DECLARE n INT DEFAULT 2;
  WHILE n < 20 DO
    INSERT into DiskServer VALUES (CONCAT('diskserver', n), 2000 + n, 0);
    SET n = n + 1;
  END WHILE;

  SET n = 2;
  WHILE n < 20 DO
    INSERT into FileSystem VALUES (100, 6, 0, CONCAT('/mnt/fs', n), 2, 2, 2, 3000 + n, 1006, 2000 + n, 0);
    SET n = n + 1;
  END WHILE;

  SET n = 2;
  WHILE n < 2000 DO
    INSERT into CastorFile VALUES (n, 'nsHostToto', 1024, '', '', 100, 4000 + n, 1002, 1003);
    SET n = n + 1;
  END WHILE;

  SET n = 2;
  WHILE n < 2000 DO
    INSERT into DiskCopy VALUES (CONCAT('/path/DiskCopy', n), 2, '', 5000 + n, 3000 + MOD(n,20), 4000 + n, 0);
    SET n = n + 1;
  END WHILE;

  SET n = 2;
  WHILE n < 2000 DO
    INSERT into Stream2TapeCopy VALUES (1012, 6000 + n);
    SET n = n + 1;
  END WHILE;

  INSERT INTO SvcClass VALUES (10, 'VeryFirstSvcClass', 1, 5, 'MyRepPolicy', 'MyGCPolicy', 'MyMigrPolicy', 'MyRecPolicy', 7000);

  SET n = 2;
  WHILE n < 2000 DO
    INSERT into Segment VALUES (0, 0, 0, 0, 0, '', 0, '', 0, 0, 0, 0, 0, 0, 8000 + n, 9000, 6000 + n, 0);
    SET n = n + 1;
  END WHILE;

  INSERT INTO SubRequest VALUES(0, 'file1', 'rfio', 1111, 1, 'sb1', 0, 0, 0, 0, 1245, 2002, 10002, 5002, 0, 4002);
  INSERT INTO SubRequest VALUES(0, 'file2', 'rfio', 1111, 1, 'sb2', 0, 0, 0, 0, 12456, 2003, 10003, 5003, 0, 4003);
END//

DELIMITER ;

CALL dummyFill();

