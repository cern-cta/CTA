/* This file contains SQL code that is not generated automatically */
/* and is inserted at the beginning of the generated code          */

/* A small table used to cross check code and DB versions */
DROP TABLE CastorVersion;

/* Sequence for ids */
DROP SEQUENCE ids_seq;

/* indexes for optimizing queries */
DROP INDEX I_Id2Type_typeId;
DROP INDEX I_DiskServer_name;
DROP INDEX I_CastorFile_fileIdNsHost;
DROP INDEX I_DiskCopy_Castorfile;
DROP INDEX I_DiskCopy_FileSystem;
DROP INDEX I_TapeCopy_Castorfile;
DROP INDEX I_SubRequest_Castorfile;
DROP INDEX I_FileSystem_DiskPool;
DROP INDEX I_SubRequest_DiskCopy;
DROP INDEX I_SubRequest_Request;
DROP INDEX I_SubRequest_Status;
DROP INDEX I_SubRequest_Status7;
DROP INDEX I_QueryParameter_Query;
DROP INDEX I_NbTapeCopiesInFS_FSStream;
DROP INDEX I_FileSystem_Rate;

/* Object types */
DROP TABLE Id2Type;

/* Requests status */
DROP TABLE newRequests;

/* support tables - check oracleTrailer_create.sql */
DROP TABLE NbTapeCopiesInFS;
DROP TABLE LockTable;


ALTER TABLE SvcClass2TapePool
  DROP CONSTRAINT fk_SvcClass2TapePool_P
  DROP CONSTRAINT fk_SvcClass2TapePool_C;
ALTER TABLE DiskPool2SvcClass
  DROP CONSTRAINT fk_DiskPool2SvcClass_P
  DROP CONSTRAINT fk_DiskPool2SvcClass_C;
ALTER TABLE Stream2TapeCopy
  DROP CONSTRAINT fk_Stream2TapeCopy_P
  DROP CONSTRAINT fk_Stream2TapeCopy_C;
ALTER TABLE TapeDrive2TapeDriveComp
  DROP CONSTRAINT fk_TapeDrive2TapeDriveComp_P
  DROP CONSTRAINT fk_TapeDrive2TapeDriveComp_C;
/* SQL statements for type BaseAddress */
DROP TABLE BaseAddress;

/* SQL statements for type Client */
DROP TABLE Client;

/* SQL statements for type ClientIdentification */
DROP TABLE ClientIdentification;

/* SQL statements for type Disk2DiskCopyDoneRequest */
DROP TABLE Disk2DiskCopyDoneRequest;

/* SQL statements for type GetUpdateDone */
DROP TABLE GetUpdateDone;

/* SQL statements for type GetUpdateFailed */
DROP TABLE GetUpdateFailed;

/* SQL statements for type PutFailed */
DROP TABLE PutFailed;

/* SQL statements for type Files2Delete */
DROP TABLE Files2Delete;

/* SQL statements for type FilesDeleted */
DROP TABLE FilesDeleted;

/* SQL statements for type FilesDeletionFailed */
DROP TABLE FilesDeletionFailed;

/* SQL statements for type GCFile */
DROP TABLE GCFile;

/* SQL statements for type GCLocalFile */
DROP TABLE GCLocalFile;

/* SQL statements for type MoverCloseRequest */
DROP TABLE MoverCloseRequest;

/* SQL statements for type PutStartRequest */
DROP TABLE PutStartRequest;

/* SQL statements for type PutDoneStart */
DROP TABLE PutDoneStart;

/* SQL statements for type GetUpdateStartRequest */
DROP TABLE GetUpdateStartRequest;

/* SQL statements for type QueryParameter */
DROP TABLE QueryParameter;

/* SQL statements for type StagePrepareToGetRequest */
DROP TABLE StagePrepareToGetRequest;

/* SQL statements for type StagePrepareToPutRequest */
DROP TABLE StagePrepareToPutRequest;

/* SQL statements for type StagePrepareToUpdateRequest */
DROP TABLE StagePrepareToUpdateRequest;

/* SQL statements for type StageGetRequest */
DROP TABLE StageGetRequest;

/* SQL statements for type StagePutRequest */
DROP TABLE StagePutRequest;

/* SQL statements for type StageUpdateRequest */
DROP TABLE StageUpdateRequest;

/* SQL statements for type StageRmRequest */
DROP TABLE StageRmRequest;

/* SQL statements for type StagePutDoneRequest */
DROP TABLE StagePutDoneRequest;

/* SQL statements for type StageFileQueryRequest */
DROP TABLE StageFileQueryRequest;

/* SQL statements for type StageRequestQueryRequest */
DROP TABLE StageRequestQueryRequest;

/* SQL statements for type StageFindRequestRequest */
DROP TABLE StageFindRequestRequest;

/* SQL statements for type SubRequest */
DROP TABLE SubRequest;

/* SQL statements for type StageReleaseFilesRequest */
DROP TABLE StageReleaseFilesRequest;

/* SQL statements for type StageAbortRequest */
DROP TABLE StageAbortRequest;

/* SQL statements for type StageGetNextRequest */
DROP TABLE StageGetNextRequest;

/* SQL statements for type StagePutNextRequest */
DROP TABLE StagePutNextRequest;

/* SQL statements for type StageUpdateNextRequest */
DROP TABLE StageUpdateNextRequest;

/* SQL statements for type Tape */
DROP TABLE Tape;

/* SQL statements for type Segment */
DROP TABLE Segment;

/* SQL statements for type TapePool */
DROP TABLE TapePool;

/* SQL statements for type TapeCopy */
DROP TABLE TapeCopy;

/* SQL statements for type CastorFile */
DROP TABLE CastorFile;

/* SQL statements for type DiskCopy */
DROP TABLE DiskCopy;

/* SQL statements for type FileSystem */
DROP TABLE FileSystem;

/* SQL statements for type SvcClass */
DROP TABLE SvcClass;
DROP INDEX I_SvcClass2TapePool_C;
DROP INDEX I_SvcClass2TapePool_P;
DROP TABLE SvcClass2TapePool;

/* SQL statements for type DiskPool */
DROP TABLE DiskPool;
DROP INDEX I_DiskPool2SvcClass_C;
DROP INDEX I_DiskPool2SvcClass_P;
DROP TABLE DiskPool2SvcClass;

/* SQL statements for type Stream */
DROP TABLE Stream;
DROP INDEX I_Stream2TapeCopy_C;
DROP INDEX I_Stream2TapeCopy_P;
DROP TABLE Stream2TapeCopy;

/* SQL statements for type FileClass */
DROP TABLE FileClass;

/* SQL statements for type DiskServer */
DROP TABLE DiskServer;

/* SQL statements for type SetFileGCWeight */
DROP TABLE SetFileGCWeight;

/* SQL statements for type TapeAccessSpecification */
DROP TABLE TapeAccessSpecification;

/* SQL statements for type TapeServer */
DROP TABLE TapeServer;

/* SQL statements for type TapeRequest */
DROP TABLE TapeRequest;

/* SQL statements for type TapeDrive */
DROP TABLE TapeDrive;
DROP INDEX I_TapeDrive2TapeDriveComp_C;
DROP INDEX I_TapeDrive2TapeDriveComp_P;
DROP TABLE TapeDrive2TapeDriveComp;

/* SQL statements for type ErrorHistory */
DROP TABLE ErrorHistory;

/* SQL statements for type TapeDriveDedication */
DROP TABLE TapeDriveDedication;

/* SQL statements for type TapeDriveCompatibility */
DROP TABLE TapeDriveCompatibility;

/* SQL statements for type DeviceGroupName */
DROP TABLE DeviceGroupName;

/* SQL statements for type DiskPoolQuery */
DROP TABLE DiskPoolQuery;

DROP FUNCTION getTime;
DROP TRIGGER tr_FileSystem_Insert;
DROP TRIGGER tr_FileSystem_Delete;
DROP TRIGGER tr_Stream_Insert;
DROP TRIGGER tr_Stream_Delete;
DROP TRIGGER tr_Stream2TapeCopy_Insert;
DROP TRIGGER tr_Stream2TapeCopy_Delete;
DROP TRIGGER tr_TapeCopy_Update;
DROP TRIGGER tr_DiskCopy_Update;
DROP TRIGGER tr_TapeCopy_CastorFile;
DROP TRIGGER tr_DiskCopy_CastorFile;
DROP TRIGGER tr_Stream2TapeCopy_Stream;
DROP FUNCTION FileSystemRate;
DROP PROCEDURE makeSubRequestWait;
DROP PROCEDURE archiveSubReq;
DROP PROCEDURE deleteRequest;
DROP PROCEDURE deleteArchivedRequests;
DROP PROCEDURE deleteOutOfDateRequests;
DROP PROCEDURE anyTapeCopyForStream;
DROP PROCEDURE updateFsFileOpened;
DROP PROCEDURE updateFsFileClosed;
DROP TRIGGER tr_DiskServer_Insert;
DROP TRIGGER tr_DiskServer_Delete;
DROP PROCEDURE updateFileSystemForJob;
DROP PROCEDURE bestTapeCopyForStream;
DROP PROCEDURE bestFileSystemForSegment;
DROP PROCEDURE fileRecalled;
DROP PROCEDURE fileRecallFailed;
DROP PACKAGE castor;
DROP TYPE "numList";
DROP PROCEDURE isSubRequestToSchedule;
DROP PROCEDURE buildPathFromFileId;
DROP PROCEDURE getUpdateStart;
DROP PROCEDURE putStart;
DROP PROCEDURE putDoneStartProc;
DROP PROCEDURE updateAndCheckSubRequest;
DROP PROCEDURE disk2DiskCopyDone;
DROP PROCEDURE recreateCastorFile;
DROP PROCEDURE putDoneFunc;
DROP PROCEDURE prepareForMigration;
DROP PROCEDURE selectCastorFile;
DROP PROCEDURE resetStream;
DROP PROCEDURE bestFileSystemForJob;
DROP PROCEDURE anySegmentsForTape;
DROP PROCEDURE segmentsForTape;
DROP PROCEDURE updateFiles2Delete;
DROP PROCEDURE filesDeletedProc;
DROP PROCEDURE filesClearedProc;
DROP PROCEDURE filesDeletionFailedProc;
DROP PROCEDURE getUpdateDoneProc;
DROP PROCEDURE getUpdateFailedProc;
DROP PROCEDURE putFailedProc;
DROP PROCEDURE failedSegments;
DROP PROCEDURE stageRelease;
DROP PROCEDURE stageRm;
DROP PACKAGE castorGC;
DROP FUNCTION defaultGCPolicy;
DROP FUNCTION nopinGCPolicy;
DROP FUNCTION nullGCPolicy;
DROP PROCEDURE garbageCollectFS;
DROP PROCEDURE defGarbageCollectFS;
DROP TRIGGER tr_FileSystem_Update;
DROP PROCEDURE internalStageQuery;
DROP PROCEDURE internalFullStageQuery;
DROP PROCEDURE fileNameStageQuery;
DROP PROCEDURE fileIdStageQuery;
DROP PROCEDURE reqIdStageQuery;
DROP PROCEDURE userTagStageQuery;
DROP PROCEDURE reqIdLastRecallsStageQuery;
DROP PROCEDURE userTagLastRecallsStageQuery;
DROP PACKAGE castorVdqm;
DROP PROCEDURE matchTape2TapeDrive;
DROP PROCEDURE matchTape2TapeDrive;
DROP PROCEDURE selectTapeRequestQueue;
DROP PROCEDURE selectTapeDriveQueue;
DROP PROCEDURE describeDiskPools;
DROP PROCEDURE describeDiskPool;
