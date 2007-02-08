/* This file contains SQL code that is not generated automatically */
/* and is inserted at the beginning of the generated code          */

/* A small table used to cross check code and DB versions */
DROP TABLE CastorVersion;

/* Sequence for ids */
DROP SEQUENCE ids_seq;

/* indexes for optimizing queries */
DROP INDEX I_CastorFile_LastKnownFileName;
DROP INDEX I_DiskServer_name;
DROP INDEX I_CastorFile_fileIdNsHost;
DROP INDEX I_CastorFile_svcClass;
DROP INDEX I_DiskCopy_Castorfile;
DROP INDEX I_DiskCopy_FileSystem;
DROP INDEX I_DiskCopy_Status;
DROP INDEX I_DiskCopy_Status_10;
DROP INDEX I_TapeCopy_Castorfile;
DROP INDEX I_TapeCopy_Status_2;
DROP INDEX I_FileSystem_DiskPool;
DROP INDEX I_FileSystem_DiskServer;
DROP INDEX I_SubRequest_Castorfile;
DROP INDEX I_SubRequest_DiskCopy;
DROP INDEX I_SubRequest_Request;
DROP INDEX I_SubRequest_Parent;
DROP INDEX I_SubRequest_SubReqId;
DROP INDEX I_SubRequest_Status;
DROP INDEX I_SubRequest_Status7;
DROP INDEX I_SubRequest_GetNextStatus;
DROP INDEX I_pk_Stream2TapeCopy;
DROP INDEX I_QueryParameter_Query;
DROP INDEX I_Segment_Copy;
DROP INDEX I_NbTapeCopiesInFS_Stream;
DROP INDEX I_NbTapeCopiesInFS_FSStream;
DROP INDEX I_FileSystem_Rate;

/* Object types */
DROP TABLE Id2Type;

/* Requests status */
DROP TABLE newRequests;

/* support tables - check oracleTrailer_create.sql */
DROP TABLE NbTapeCopiesInFS;
DROP TABLE LockTable;
DROP TABLE SelectFiles2DeleteProcHelper;
DROP TABLE FilesDeletedProcOutput;
DROP TABLE FileSystemGC;

