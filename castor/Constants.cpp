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
 * @(#)$RCSfile: Constants.cpp,v $ $Revision: 1.36 $ $Release$ $Date: 2006/12/22 17:26:42 $ $Author: sponcec3 $
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
  "Client",
  "Cuuid",
  "DiskCopy",
  "DiskFile",
  "DiskPool",
  "DiskServer",
  "OldEndResponse",
  "FileClass",
  "OldFileResponse",
  "FileSystem",
  "IClient",
  "MessageAck",
  "DELETED TYPE",
  "ReqIdRequest",
  "Request",
  "Segment",
  "DELETED TYPE",
  "DELETED TYPE",
  "StageGetNextRequest",
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
  "OldStringResponse",
  "StageFileQueryRequest",
  "StageFindRequestRequest",
  "StageGetRequest",
  "StagePrepareToGetRequest",
  "StagePrepareToPutRequest",
  "StagePrepareToUpdateRequest",
  "StagePutDoneRequest",
  "StagePutRequest",
  "StageRequestQueryRequest",
  "StageRmRequest",
  "StageUpdateFileStatusRequest",
  "StageUpdateRequest",
  "FileRequest",
  "QryRequest",
  "DELETED TYPE",
  "StagePutNextRequest",
  "StageUpdateNextRequest",
  "StageAbortRequest",
  "StageReleaseFilesRequest",
  "OldResponse",
  "OldIOResponse",
  "OldAbortResponse",
  "OldRequestQueryResponse",
  "DELETED TYPE",
  "OldFindReqResponse",
  "DiskCopyForRecall",
  "TapeCopyForMigration",
  "GetUpdateStartRequest",
  "OldGetUpdateStartResponse",
  "BaseAddress",
  "OldBasicResponse",
  "Disk2DiskCopyDoneRequest",
  "MoverCloseRequest",
  "StartRequest",
  "PutStartRequest",
  "OldStartResponse",
  "IObject",
  "IAddress",
  "QueryParameter",
  "DiskCopyInfo",
  "Files2Delete",
  "FilesDeleted",
  "OldGCFilesResponse",
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
  "ErrorHistory",
  "TapeDriveDedication",	
  "TapeAccessSpecification",
  "TapeDriveCompatibility",
  
  "PutDoneStart",
  "OldFileQryResponse",
  "SetFileGCWeight",
  "RepackRequest",
  "RepackSubRequest",
  "RepackSegment",
  "RepackAck",

  "OldDiskPoolQueryResponse",
  "DiskServerDescription",
  "FileSystemDescription",
  "DiskPoolQuery",
  "EndResponse",
  "FileResponse",
  "StringResponse",
  "Response",
  "IOResponse",
  "AbortResponse",
  "RequestQueryResponse",
  "FileQueryResponse",
  "FindReqResponse",
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
  "FileSystemAdminReport"
};

//=============================================================================
// ServicesIdStrings
//=============================================================================
const char* castor::ServicesIdStrings[ServicesIdsNb] = {
  "INVALID",
  "NOMSG",
  "DLFMSG",
  "STDMSG",
  "STREAMCNV",

  "REMOTEJOBSVC",
  "REMOTEGCSVC",

  "DBCNV",
  "DBCOMMONSVC",
  "DBSTAGERSVC",
  "DBTAPESVC",
  "DBFSSVC",
  "DBJOBSVC",
  "DBGCSVC",
  "DBQUERYSVC",
  "DBVDQMSVC",

  "ORACNV",
  "ORACOMMONSVC",
  "ORASTAGERSVC",
  "ORATAPESVC",
  "ORAFSSVC",
  "ORAJOBSVC",
  "ORAGCSVC",
  "ORAQUERYSVC",
  "ORAVDQMSVC",

  "PGCNV",
  "PGCOMMONSVC",
  "PGSTAGERSVC",
  "PGTAPESVC",
  "PGFSSVC",
  "PGJOBSVC",
  "PGGCSVC",
  "PGQUERYSVC",
  "PGVDQMSVC",

  "MYCNV",
  
  "DBSRMSVC",
  "DBSRMDAEMONSVC",
  "ORACLEANSVC"

};

//=============================================================================
// RepresentationsIdStrings
//=============================================================================
const char* castor::RepresentationsIdStrings[RepresentationsIdsNb] = {
  "INVALID",
  "STREAM",
  "DATABASE",
  "ORACLE",
  "MYSQL",
  "PGSQL"
};
