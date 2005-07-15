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
 * @(#)$RCSfile: Constants.cpp,v $ $Revision: 1.23 $ $Release$ $Date: 2005/07/15 09:22:36 $ $Author: itglp $
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
const char* castor::ObjectsIdStrings[91] = {
  "INVALID",
  "Ptr", // Only used for streaming for circular dependencies
  "CastorFile",
  "Client",
  "Cuuid",
  "DiskCopy",
  "DiskFile",
  "DiskPool",
  "DiskServer",
  "EndResponse",
  "FileClass",
  "FileResponse",
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
  "StringResponse",
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
  "Response",
  "IOResponse",
  "AbortResponse",
  "RequestQueryResponse",
  "FileQueryResponse",
  "FindReqResponse",
  "DiskCopyForRecall",
  "TapeCopyForMigration",
  "GetUpdateStartRequest",
  "GetUpdateStartResponse",
  "BaseAddress",
  "BasicResponse",
  "Disk2DiskCopyDoneRequest",
  "MoverCloseRequest",
  "StartRequest",
  "PutStartRequest",
  "StartResponse",
  "IObject",
  "IAddress",
  "QueryParameter",
  "DiskCopyInfo",
  "Files2Delete",
  "FilesDeleted",
  "GCFilesResponse",
  "GCLocalFile",
  "DELETED TYPE",
  "GetUpdateDone",
  "GetUpdateFailed",
  "PutFailed",
  "GCFile",
  "GCFileList",
  "FilesDeletionFailed",
  "TapeRequest",
  "CientIdentification",
  "ExtendedDeviceGroup",
  "TapeServer",
  "TapeDrive",
  "ClientIdentification",
  "PutDoneStart"
};

//=============================================================================
// ServicesIdStrings
//=============================================================================
const char* castor::ServicesIdStrings[19] = {
  "INVALID",
  "NOMSG",
  "DLFMSG",
  "STDMSG",
  "ORACNV",
  "ODBCCNV",
  "STREAMCNV",
  "ORASTAGERSVC",
  "REMOTESTAGERSVC",
  "ORAQUERYSVC",
  "DBCNV",
  "MYCNV",
  "DBSTAGERSVC",
  "MYSTAGERSVC",
  "DBQUERYSVC",
  "MYQUERYSVC",
  "DBVDQMSVC",
  "ORAVDQMSVC",
  "MYVDQMSVC"
};

//=============================================================================
// RepresentationsIdStrings
//=============================================================================
const char* castor::RepresentationsIdStrings[5] = {
  "INVALID",
  "ORACLE",
  "ODBC",
  "STREAM",
  "MYSQL"
};
