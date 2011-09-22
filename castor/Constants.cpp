/******************************************************************************
 *                      Constants.cpp
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
 * @(#)$RCSfile: Constants.cpp,v $ $Revision: 1.82 $ $Release$ $Date: 2009/08/06 12:59:22 $ $Author: gtaur $
 *
 *
 *
 * @author Sebastien Ponce
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
  "DiskCopy",
  "DiskFile",
  "DiskPool",
  "DiskServer",
  "DELETED TYPE",
  "FileClass",
  "DELETED TYPE",
  "FileSystem",
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
  "Stream",
  "SubRequest",
  "SvcClass",
  "Tape",
  "TapeCopy",
  "TapePool",
  "DELETED TYPE",
  "StageFileQueryRequest",
  "DELETED TYPE",
  "StageGetRequest",
  "StagePrepareToGetRequest",
  "StagePrepareToPutRequest",
  "StagePrepareToUpdateRequest",
  "StagePutDoneRequest",
  "StagePutRequest",
  "DELETED TYPE",
  "StageRmRequest",
  "DELETED TYPE",
  "StageUpdateRequest",
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
  "TapeCopyForMigration",
  "GetUpdateStartRequest",
  "DELETED TYPE",
  "BaseAddress",
  "DELETED TYPE",
  "Disk2DiskCopyDoneRequest",
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

  "StageRepackRequest",

  "DiskServerStateReport",
  "DiskServerMetricsReport",
  "FileSystemStateReport",
  "FileSystemMetricsReport",
  "DiskServerAdminReport",
  "FileSystemAdminReport",
  "StreamReport",
  "FileSystemStateAck",
  "MonitorMessageAck",

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

  "Disk2DiskCopyStartRequest",
  "Disk2DiskCopyStartResponse",
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
  "PriorityMap",
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
  "TapeGatewayRequest",
  "DELETED TYPE", 
  "EndNotification",
  "NoMoreFiles",
  "NotificationAcknowledge",
  "FileErrorReport",
  "BaseFileInfo",
  "DELETED TYPE", 
  "RmMasterReport",
  "EndNotificationErrorReport",
  "TapeGatewaySubRequest",
  "GatewayMessage",
  "DumpNotification",
  "PingNotification",
  "DumpParameters",
  "DumpParametersRequest",
  "RecallPolicyElement",
  "MigrationPolicyElement",
  "StreamPolicyElement",
  "RetryPolicyElement",
  "VdqmTapeGatewayRequest",

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
  "DBTAPESVC",
  "DELETED SERVICE",
  "DBJOBSVC",
  "DBGCSVC",
  "DBQUERYSVC",
  "DBVDQMSVC",

  "ORACNV",
  "ORACOMMONSVC",
  "ORASTAGERSVC",
  "ORATAPESVC",
  "DELETED SERVICE",
  "ORAJOBSVC",
  "ORAGCSVC",
  "ORAQUERYSVC",
  "ORAVDQMSVC",
  "PGCNV",
  "DELETED SERVICE",
  "DELETED SERVICE",
  "DELETED SERVICE",
  "DELETED SERVICE",
  "DELETED SERVICE",
  "DELETED SERVICE",
  "DELETED SERVICE",
  "DELETED SERVICE",
  "MYCNV",  
  "DBSRMSVC",
  "DBSRMDAEMONSVC",

  "ORACLEANSVC",
  "ORARMMASTERSVC",
  "DBPARAMSSVC",
  "DELETED SERVICE",

  "DBRHSVC",
  "ORARHSVC",
  "DELETED SERVICE",
  "ORAREPACKSVC" ,
  "ORATAPEGATEWAYSVC",
  "ORAMIGHUNTERSVC",
  "ORARECHANDLERSVC",

  "ORASRMDAEMONSVC",

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
