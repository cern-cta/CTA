/******************************************************************************
 *                      Constants.hpp
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
 * @(#)$RCSfile$ $Revision$ $Release$ $Date$ $Author$
 *
 * Here are defined all constants used in Castor.
 * This includes :
 *   - Ids of objects (OBJ_***)
 *   - Ids of services (SVC_***)
 *   - Ids of persistent representations (REP_***)
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#ifndef CASTOR_CONSTANTS_HPP
#define CASTOR_CONSTANTS_HPP 1

#ifdef __cplusplus
namespace castor {
#endif

  /**
   * Ids of objects. Each persistent or serializable type has one
   */
  enum ObjectsIds {
    OBJ_INVALID = 0,
    OBJ_Ptr = 1, // Only used for streaming for circular dependencies
    OBJ_CastorFile = 2,
    OBJ_Client = 3,
    OBJ_Cuuid = 4,
    OBJ_DiskCopy = 5,
    OBJ_DiskFile = 6,
    OBJ_DiskPool = 7,
    OBJ_DiskServer = 8,
    OBJ_OldEndResponse = 9,
    OBJ_FileClass = 10,
    OBJ_OldFileResponse = 11,
    OBJ_FileSystem = 12,
    OBJ_IClient = 13,
    OBJ_MessageAck = 14,
    OBJ_ReqIdRequest = 16,
    OBJ_Request = 17,
    OBJ_Segment = 18,
    OBJ_StageGetNextRequest = 21,
    OBJ_Stream = 26,
    OBJ_SubRequest = 27,
    OBJ_SvcClass = 28,
    OBJ_Tape = 29,
    OBJ_TapeCopy = 30,
    OBJ_TapePool = 31,
    OBJ_OldStringResponse = 32,
    OBJ_StageFileQueryRequest = 33,
    OBJ_StageFindRequestRequest = 34,
    OBJ_StageGetRequest = 35,
    OBJ_StagePrepareToGetRequest = 36,
    OBJ_StagePrepareToPutRequest = 37,
    OBJ_StagePrepareToUpdateRequest = 38,
    OBJ_StagePutDoneRequest = 39,
    OBJ_StagePutRequest = 40,
    OBJ_StageRequestQueryRequest = 41,
    OBJ_StageRmRequest = 42,
    OBJ_StageUpdateFileStatusRequest = 43,
    OBJ_StageUpdateRequest = 44,
    OBJ_FileRequest = 45,
    OBJ_QryRequest = 46,
    OBJ_StagePutNextRequest = 48,
    OBJ_StageUpdateNextRequest = 49,
    OBJ_StageAbortRequest = 50,
    OBJ_StageReleaseFilesRequest = 51,
    OBJ_OldResponse = 52,
    OBJ_OldIOResponse = 53,
    OBJ_OldAbortResponse = 54,
    OBJ_OldRequestQueryResponse = 55,
    OBJ_OldFileQueryResponse = 56,
    OBJ_OldFindReqResponse = 57,
    OBJ_DiskCopyForRecall = 58,
    OBJ_TapeCopyForMigration = 59,
    OBJ_GetUpdateStartRequest = 60,
    OBJ_OldGetUpdateStartResponse = 61,
    OBJ_BaseAddress = 62,
    OBJ_OldBasicResponse = 63,
    OBJ_Disk2DiskCopyDoneRequest = 64,
    OBJ_MoverCloseRequest = 65,
    OBJ_StartRequest = 66,
    OBJ_PutStartRequest = 67,
    OBJ_OldStartResponse = 68,
    OBJ_IObject = 69,
    OBJ_IAddress = 70,
    OBJ_QueryParameter = 71,
    OBJ_DiskCopyInfo = 72,
    OBJ_Files2Delete = 73,
    OBJ_FilesDeleted = 74,
    OBJ_OldGCFilesResponse = 75,
    OBJ_GCLocalFile = 76,
    OBJ_GetUpdateDone = 78,
    OBJ_GetUpdateFailed = 79,
    OBJ_PutFailed = 80,
    OBJ_GCFile = 81,
    OBJ_GCFileList = 82,
    OBJ_FilesDeletionFailed = 83,

    //The vdqm objects
    OBJ_TapeRequest = 84,
    OBJ_ClientIdentification = 85,
    OBJ_TapeServer = 86,
    OBJ_TapeDrive = 87,
    OBJ_DeviceGroupName = 88,
    OBJ_ErrorHistory = 89,
    OBJ_TapeDriveDedication = 90,
    OBJ_TapeAccessSpecification = 91,
    OBJ_TapeDriveCompatibility = 92,

    OBJ_PutDoneStart = 93,
    OBJ_OldFileQryResponse = 94,
    OBJ_SetFileGCWeight = 95,
    OBJ_RepackRequest = 96,
    OBJ_RepackSubRequest = 97,
    OBJ_RepackSegment = 98,
    OBJ_RepackAck = 99,

    OBJ_OldDiskPoolQueryResponse = 100,
    OBJ_DiskServerDescription = 101,
    OBJ_FileSystemDescription = 102,
    OBJ_DiskPoolQuery = 103,

    // Object to replace the old response

    OBJ_EndResponse = 104,
    OBJ_FileResponse = 105,
    OBJ_StringResponse = 106,
    OBJ_Response = 107,
    OBJ_IOResponse = 108,
    OBJ_AbortResponse = 109,
    OBJ_RequestQueryResponse = 110,
    OBJ_FileQueryResponse = 111,
    OBJ_FindReqResponse = 112,
    OBJ_GetUpdateStartResponse = 113,
    OBJ_BasicResponse = 114,
    OBJ_StartResponse = 115,
    OBJ_GCFilesResponse = 116,
    OBJ_FileQryResponse = 117,
    OBJ_DiskPoolQueryResponse = 118,

    OBJ_StageRepackRequest = 119,

    // Monitoring Objects
    OBJ_DiskServerStateReport = 120,
    OBJ_DiskServerMetricsReport = 121,
    OBJ_FileSystemStateReport = 122,
    OBJ_FileSystemMetricsReport = 123,

  };

  /**
   * Ids of services
   */
  enum ServicesIds {
    SVC_INVALID = 0,
    SVC_NOMSG = 1,
    SVC_DLFMSG = 2,
    SVC_STDMSG = 3,
    SVC_STREAMCNV = 4,

    SVC_REMOTEJOBSVC = 5,
    SVC_REMOTEGCSVC = 6,

    SVC_DBCNV = 7,
    SVC_DBCOMMONSVC = 8,
    SVC_DBSTAGERSVC = 9,
    SVC_DBTAPESVC = 10,
    SVC_DBFSSVC = 11,
    SVC_DBJOBSVC = 12,
    SVC_DBGCSVC = 13,
    SVC_DBQUERYSVC = 14,
    SVC_DBVDQMSVC = 15,

    SVC_ORACNV = 16,
    SVC_ORACOMMONSVC = 17,
    SVC_ORASTAGERSVC = 18,
    SVC_ORATAPESVC = 19,
    SVC_ORAFSSVC = 20,
    SVC_ORAJOBSVC = 21,
    SVC_ORAGCSVC = 22,
    SVC_ORAQUERYSVC = 23,
    SVC_ORAVDQMSVC = 24,

    SVC_PGCNV = 25,
    SVC_PGCOMMONSVC = 26,
    SVC_PGSTAGERSVC = 27,
    SVC_PGTAPESVC = 28,
    SVC_PGFSSVC = 29,
    SVC_PGJOBSVC = 30,
    SVC_PGGCSVC = 31,
    SVC_PGQUERYSVC = 32,
    SVC_PGVDQMSVC = 33,

    SVC_MYCNV = 34,

    SVC_DBSRMSVC = 35,
    SVC_DBSRMDAEMONSVC = 36,
    SVC_ORACLEANSVC = 37


  };

  /**
   * Ids of persistent representations
   */
  enum RepresentationsIds {
    REP_INVALID = 0,
    REP_STREAM = 1,
    REP_DATABASE = 2,
    REP_ORACLE = 3,
    REP_MYSQL = 4,
    REP_PGSQL = 5
  };

  /**
   * Magic numbers
   */
#define SEND_REQUEST_MAGIC 0x1e10131

  /**
   * RH server port
   */
#define RHSERVER_PORT 9002

  /**
   * Nb of objectIds currently existing
   */
  static const unsigned int ObjectsIdsNb = 124;

  /**
   * Nb of ServicesIds currently existing
   */
  static const unsigned int ServicesIdsNb = 38;

  /**
   * Nb of RepresentationsIds currently existing
   */
  static const unsigned int RepresentationsIdsNb = 6;

  /**
   * Names of the differents objects, used to display
   * correctly the ObjectsIds enum
   */
  extern const char* ObjectsIdStrings[124];

  /**
   * Names of the differents Services, used to display
   * correctly the ServicesIds enum
   */
  extern const char* ServicesIdStrings[38];

  /**
   * Names of the differents representations, used to display
   * correctly the RepresentationsIds enum
   */
  extern const char* RepresentationsIdStrings[6];

#ifdef __cplusplus
} // end of namespace castor
#endif

#endif // CASTOR_CONSTANTS_HPP
