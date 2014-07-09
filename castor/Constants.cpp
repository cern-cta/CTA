/******************************************************************************
 *
 * This file is part of the Castor project.
 * See http://castor.web.cern.ch/castor
 *
 * Copyright (C) 2003  CERN
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
 *
 *
 *
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

// Include Files
#include "castor/Constants.hpp"

//=============================================================================
// ObjectsIdStrings
//=============================================================================
const char* castor::ObjectsIdStrings[ObjectsIdsNb] = {
  "INVALID",
  "Ptr", // Only used for streaming for circular dependencies
  "CastorFile",
  "DELETED TYPE",
  "Cuuid",
  "DELETED TYPE",
  "DiskFile",
  "DiskPool",
  "DELETED TYPE",
  "DELETED TYPE",
  "FileClass",
  "DELETED TYPE",
  "DELETED TYPE",
  "IClient",
  "MessageAck",
  "DELETED TYPE",
  "DELETED TYPE",
  "Request",
  "Segment",
  "DELETED TYPE",
  "DELETED TYPE",
  "DELETED TYPE",
  "DELETED TYPE",
  "DELETED TYPE",
  "DELETED TYPE",
  "DELETED TYPE",
  "DELETED TYPE",
  "SubRequest",
  "SvcClass",
  "Tape",
  "RecallJob",
  "TapePool",
  "DELETED TYPE",
  "StageFileQueryRequest",
  "DELETED TYPE",
  "StageGetRequest",
  "StagePrepareToGetRequest",
  "StagePrepareToPutRequest",
  "DELETED TYPE",
  "StagePutDoneRequest",
  "StagePutRequest",
  "DELETED TYPE",
  "StageRmRequest",
  "DELETED TYPE",
  "DELETED TYPE",
  "FileRequest",
  "QryRequest",
  "DELETED TYPE",
  "DELETED TYPE",
  "DELETED TYPE",
  "StageAbortRequest",
  "DELETED TYPE",
  "DELETED TYPE",
  "DELETED TYPE",
  "DELETED TYPE",
  "DELETED TYPE",
  "DELETED TYPE",
  "DELETED TYPE",
  "DiskCopyForRecall",
  "DELETED TYPE",
  "GetUpdateStartRequest",
  "DELETED TYPE",
  "BaseAddress",
  "DELETED TYPE",
  "DELETED TYPE",
  "MoverCloseRequest",
  "StartRequest",
  "PutStartRequest",
  "DELETED TYPE",
  "IObject",
  "IAddress",
  "QueryParameter",
  "DiskCopyInfo",
  "Files2Delete",
  "FilesDeleted",
  "DELETED TYPE",
  "GCLocalFile",
  "DELETED TYPE",
  "GetUpdateDone",
  "GetUpdateFailed",
  "PutFailed",
  "GCFile",
  "GCFileList",
  "FilesDeletionFailed",

  "TapeRequest",
  "ClientIdentification",
  "TapeServer",
  "TapeDrive",
  "DeviceGroupName",
  "DELETED TYPE",
  "TapeDriveDedication",
  "TapeAccessSpecification",
  "TapeDriveCompatibility",

  "DELETED TYPE",
  "DELETED TYPE",
  "SetFileGCWeight",
  "RepackRequest",
  "RepackSubRequest",
  "RepackSegment",
  "RepackAck",

  "DELETED TYPE",
  "DiskServerDescription",
  "FileSystemDescription",
  "DELETED TYPE",
  "EndResponse",
  "FileResponse",
  "StringResponse",
  "Response",
  "IOResponse",
  "AbortResponse",
  "DELETED TYPE",
  "DELETED TYPE",
  "DELETED TYPE",
  "GetUpdateStartResponse",
  "BasicResponse",
  "StartResponse",
  "GCFilesResponse",
  "FileQryResponse",
  "DiskPoolQueryResponse",

  "DELETED TYPE",

  "DELETED TYPE",
  "DELETED TYPE",
  "DELETED TYPE",
  "DELETED TYPE",
  "DELETED TYPE",
  "DELETED TYPE",
  "DELETED TYPE",
  "DELETED TYPE",
  "DELETED TYPE",

  "Client",

  "JobSubmissionRequest",
  "VersionQuery",
  "VersionResponse",
  "StageDiskCopyReplicaRequest",
  "RepackResponse",
  "RepackFileQry",
  "DELETED TYPE", 
  "DELETED TYPE", 
  "DELETED TYPE", 
  "DELETED TYPE", 
  "DELETED TYPE", 
  "PolicyObj",

  "NsFilesDeleted",
  "NsFilesDeletedResponse",

  "DELETED TYPE", 
  "DELETED TYPE", 
  "ThreadNotification",
  "FirstByteWritten",

  "VdqmTape",
  "StgFilesDeleted",
  "StgFilesDeletedResponse",
  "VolumePriority",
  "ChangePrivilege",
  "BWUser",
  "RequestType",
  "ListPrivileges",
  "Privilege",
  "ListPrivilegesResponse",
  "DELETED TYPE",
  "VectorAddress",
  "Tape2DriveDedication",

  "TapeRecall",
  "FileMigratedNotification",
  "FileRecalledNotification",
  "FileToMigrateRequest",
  "FileToMigrate",
  "FileToRecallRequest",
  "FileToRecall",
  "VolumeRequest",
  "Volume",
  "DELETED TYPE", 
  "DELETED TYPE", 
  "EndNotification",
  "NoMoreFiles",
  "NotificationAcknowledge",
  "FileErrorReport",
  "BaseFileInfo",
  "DELETED TYPE", 
  "DELETED TYPE",
  "EndNotificationErrorReport",
  "DELETED TYPE",
  "GatewayMessage",
  "DumpNotification",
  "PingNotification",
  "DumpParameters",
  "DumpParametersRequest",
  "RecallPolicyElement",
  "MigrationPolicyElement",
  "StreamPolicyElement",
  "RetryPolicyElement",
  "DELETED TYPE",

  "StageQueryResult",
  "NsFileId",
  "BulkRequestResult",
  "FileResult",
  "DiskPoolQuery",

  "EndNotificationFileErrorReport",
  "FileMigrationReportList",
  "FileRecallReportList",
  "FilesToMigrateList",
  "FilesToMigrateListRequest",
  "FilesToRecallListRequest",
  "FileErrorReportStruct",
  "FileMigratedNotificationStruct",
  "FileRecalledNotificationStruct",
  "FilesToRecallList",
  "FileToMigrateStruct",
  "FileToRecallStruct",
  "FilesListRequest"
};


//=============================================================================
// ServicesIdStrings
//=============================================================================
const char* castor::ServicesIdStrings[ServicesIdsNb] = {
  "INVALID",
  "DELETED SERVICE",
  "DELETED SERVICE",
  "DELETED SERVICE",
  "STREAMCNV",

  "REMOTEJOBSVC",
  "REMOTEGCSVC",

  "DBCNV",
  "DBCOMMONSVC",
  "DBSTAGERSVC",
  "DELETED SERVICE",
  "DELETED SERVICE",
  "DBJOBSVC",
  "DBGCSVC",
  "DBQUERYSVC",
  "DBVDQMSVC",

  "ORACNV",
  "ORACOMMONSVC",
  "ORASTAGERSVC",
  "DELETED SERVICE",
  "DELETED SERVICE",
  "ORAJOBSVC",
  "ORAGCSVC",
  "ORAQUERYSVC",
  "ORAVDQMSVC",
  "DELETED SERVICE",
  "DELETED SERVICE",
  "DELETED SERVICE",
  "DELETED SERVICE",
  "DELETED SERVICE",
  "DELETED SERVICE",
  "DELETED SERVICE",
  "DELETED SERVICE",
  "DELETED SERVICE",
  "DELETED SERVICE",
  "DBSRMSVC",
  "DBSRMDAEMONSVC",

  "DELETED SERVICE",
  "DELETED SERVICE",
  "DBPARAMSSVC",
  "DELETED SERVICE",

  "DBRHSVC",
  "ORARHSVC",
  "DELETED SERVICE",
  "DELETED SERVICE",
  "ORATAPEGATEWAYSVC",
  "DELETED SERVICE",
  "DELETED SERVICE",

  "ORASRMDAEMONSVC"
};

//=============================================================================
// RepresentationsIdStrings
//=============================================================================
const char* castor::RepresentationsIdStrings[RepresentationsIdsNb] = {
  "INVALID",
  "STREAM",
  "DATABASE",
  "ORACLE",
  "DELETED REPRESENTATION",
  "DELETED REPRESENTATION"
};
