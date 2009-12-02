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
    OBJ_Ptr = 1, /* Only used for streaming for circular dependencies */
    OBJ_CastorFile = 2,
    OBJ_Cuuid = 4,
    OBJ_DiskCopy = 5,
    OBJ_DiskFile = 6,
    OBJ_DiskPool = 7,
    OBJ_DiskServer = 8,
    OBJ_FileClass = 10,
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
    OBJ_StageFileQueryRequest = 33,
    OBJ_StageGetRequest = 35,
    OBJ_StagePrepareToGetRequest = 36,
    OBJ_StagePrepareToPutRequest = 37,
    OBJ_StagePrepareToUpdateRequest = 38,
    OBJ_StagePutDoneRequest = 39,
    OBJ_StagePutRequest = 40,
    OBJ_StageRmRequest = 42,
    OBJ_StageUpdateRequest = 44,
    OBJ_FileRequest = 45,
    OBJ_QryRequest = 46,
    OBJ_StagePutNextRequest = 48,
    OBJ_StageUpdateNextRequest = 49,
    OBJ_StageAbortRequest = 50,
    OBJ_StageReleaseFilesRequest = 51,
    OBJ_DiskCopyForRecall = 58,
    OBJ_TapeCopyForMigration = 59,
    OBJ_GetUpdateStartRequest = 60,
    OBJ_BaseAddress = 62,
    OBJ_Disk2DiskCopyDoneRequest = 64,
    OBJ_MoverCloseRequest = 65,
    OBJ_StartRequest = 66,
    OBJ_PutStartRequest = 67,
    OBJ_IObject = 69,
    OBJ_IAddress = 70,
    OBJ_QueryParameter = 71,
    OBJ_DiskCopyInfo = 72,
    OBJ_Files2Delete = 73,
    OBJ_FilesDeleted = 74,
    OBJ_GCLocalFile = 76,
    OBJ_GetUpdateDone = 78,
    OBJ_GetUpdateFailed = 79,
    OBJ_PutFailed = 80,
    OBJ_GCFile = 81,
    OBJ_GCFileList = 82,
    OBJ_FilesDeletionFailed = 83,

    /* The vdqm objects (to be moved to a different range) */
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
    OBJ_SetFileGCWeight = 95,
    OBJ_RepackRequest = 96,
    OBJ_RepackSubRequest = 97,
    OBJ_RepackSegment = 98,
    OBJ_RepackAck = 99,

    OBJ_DiskServerDescription = 101,
    OBJ_FileSystemDescription = 102,
    OBJ_DiskPoolQuery = 103,

    /* Object to replace the old response */
    OBJ_EndResponse = 104,
    OBJ_FileResponse = 105,
    OBJ_StringResponse = 106,
    OBJ_Response = 107,
    OBJ_IOResponse = 108,
    OBJ_AbortResponse = 109,
    OBJ_FileQueryResponse = 111,
    OBJ_GetUpdateStartResponse = 113,
    OBJ_BasicResponse = 114,
    OBJ_StartResponse = 115,
    OBJ_GCFilesResponse = 116,
    OBJ_FileQryResponse = 117,
    OBJ_DiskPoolQueryResponse = 118,

    OBJ_StageRepackRequest = 119,

    /* Monitoring Objects */
    OBJ_DiskServerStateReport = 120,
    OBJ_DiskServerMetricsReport = 121,
    OBJ_FileSystemStateReport = 122,
    OBJ_FileSystemMetricsReport = 123,
    OBJ_DiskServerAdminReport = 124,
    OBJ_FileSystemAdminReport = 125,
    OBJ_StreamReport = 126,
    OBJ_FileSystemStateAck = 127,
    OBJ_MonitorMessageAck = 128,

    OBJ_Client = 129,

    OBJ_JobRequest = 130,
    OBJ_VersionQuery = 131,
    OBJ_VersionResponse = 132,
    OBJ_StageDiskCopyReplicaRequest = 133,
    OBJ_RepackResponse = 134,
    OBJ_RepackFileQry = 135,
    OBJ_CnsInfoMigrationPolicy = 136,
    OBJ_DbInfoMigrationPolicy = 137,
    OBJ_CnsInfoRecallPolicy = 138,
    OBJ_DbInfoRecallPolicy = 139,
    OBJ_DbInfoStreamPolicy = 140,
    OBJ_PolicyObj = 141,

    OBJ_NsFilesDeleted = 142,
    OBJ_NsFilesDeletedResponse = 143,

    OBJ_Disk2DiskCopyStartRequest = 144,
    OBJ_Disk2DiskCopyStartResponse = 145,
    OBJ_ThreadNotification = 146,
    OBJ_FirstByteWritten = 147,

    /* VDQM objects to be moved to a different range */
    OBJ_VdqmTape = 148,

    /* Stager GC synchronization */
    OBJ_StgFilesDeleted = 149,
    OBJ_StgFilesDeletedResponse = 150,

    /* More VDQM objects */
    OBJ_VolumePriority = 151,

    /* B/W list related objects */
    OBJ_ChangePrivilege = 152,
    OBJ_BWUser = 153,
    OBJ_RequestType = 154,
    OBJ_ListPrivileges = 155,
    OBJ_Privilege = 156,
    OBJ_ListPrivilegesResponse = 157,

    /* object for segment priority */
    OBJ_PriorityMap = 158,

    /* vector address for bulk operations */
    OBJ_VectorAddress = 159,

    /* VDQM objects to be moved to a different range */

    OBJ_Tape2DriveDedication = 160,

    /* Special constant used by the B/W list to determine if a user has rights
     * to issue tape recalls.
     */
    OBJ_TapeRecall = 161,

    /* TapeGateWay Objects */
 
    OBJ_FileMigratedNotification = 162,
    OBJ_FileRecalledNotification = 163,
    OBJ_FileToMigrateRequest = 164,
    OBJ_FileToMigrate = 165,
    OBJ_FileToRecallRequest = 166,
    OBJ_FileToRecall = 167,
    OBJ_VolumeRequest = 168,
    OBJ_Volume = 169,
    OBJ_TapeGatewayRequest = 170,
    OBJ_DbInfoRetryPolicy = 171,
    OBJ_EndNotification = 172,
    OBJ_NoMoreFiles = 173,
    OBJ_NotificationAcknowledge = 174,
    OBJ_FileErrorReport = 175,
    OBJ_BaseFileInfo = 176,
   
    OBJ_RmMasterReport = 178,
    OBJ_EndNotificationErrorReport = 179,
    OBJ_TapeGatewaySubRequest = 180,
    OBJ_GatewayMessage = 181,
    OBJ_DumpNotification = 182,
    OBJ_PingNotification = 183,
    OBJ_DumpParameters = 184,
    OBJ_DumpParametersRequest = 185,
    OBJ_RecallPolicyElement = 186,
    OBJ_MigrationPolicyElement = 187,
    OBJ_StreamPolicyElement = 188,
    OBJ_RetryPolicyElement = 189,
    OBJ_VdqmTapeGatewayRequest = 190

  };

  /**
   * Ids of services
   */
  enum ServicesIds {
    SVC_INVALID = 0,
    SVC_STREAMCNV = 4,

    SVC_REMOTEJOBSVC = 5,
    SVC_REMOTEGCSVC = 6,

    SVC_DBCNV = 7,
    SVC_DBCOMMONSVC = 8,
    SVC_DBSTAGERSVC = 9,
    SVC_DBTAPESVC = 10,
    SVC_DBJOBSVC = 12,
    SVC_DBGCSVC = 13,
    SVC_DBQUERYSVC = 14,
    SVC_DBVDQMSVC = 15,
    SVC_ORACNV = 16,
    SVC_ORACOMMONSVC = 17,
    SVC_ORASTAGERSVC = 18,
    SVC_ORATAPESVC = 19,
    SVC_ORAJOBSVC = 21,
    SVC_ORAGCSVC = 22,
    SVC_ORAQUERYSVC = 23,
    SVC_ORAVDQMSVC = 24,
    SVC_PGCNV = 25,
    SVC_MYCNV = 34,

    SVC_DBSRMSVC = 35,
    SVC_DBSRMDAEMONSVC = 36,

    SVC_ORACLEANSVC = 37,
    SVC_ORARMMASTERSVC = 38,
    SVC_DBPARAMSSVC = 39,
    SVC_ORAJOBMANAGERSVC = 40,
    SVC_DBRHSVC = 41,
    SVC_ORARHSVC = 42,
    SVC_ORAPOLICYSVC = 43,
    SVC_ORAREPACKSVC = 44,
    SVC_ORATAPEGATEWAYSVC = 45,
    SVC_ORAMIGHUNTERSVC = 46,
    SVC_ORARECHANDLERSVC = 47
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
   *
   * Note that both a macro and a constant are required because the C compiler,
   * as opposed to the C++ compiler, does not allow a variable-size type
   * declared outside of any function.
   */

#define OBJECT_IDS_NB 191

  static const unsigned int ObjectsIdsNb = OBJECT_IDS_NB;

  /**
   * Nb of ServicesIds currently existing
   *
   * Note that both a macro and a constant are required because the C compiler,
   * as opposed to the C++ compiler, does not allow a variable-size type
   * declared outside of any function.
   */

#define SERVICES_IDS_NB 48
  static const unsigned int ServicesIdsNb = SERVICES_IDS_NB;


  /**
   * Nb of RepresentationsIds currently existing
   *
   * Note that both a macro and a constant are required because the C compiler,
   * as opposed to the C++ compiler, does not allow a variable-size type
   * declared outside of any function.
   */
#define REPRESENTATIONS_IDS_NB 6
  static const unsigned int RepresentationsIdsNb = REPRESENTATIONS_IDS_NB;

  /**
   * Names of the differents objects, used to display
   * correctly the ObjectsIds enum
   */

  extern const char* ObjectsIdStrings[OBJECT_IDS_NB];


  /**
   * Names of the differents Services, used to display
   * correctly the ServicesIds enum
   */

  extern const char* ServicesIdStrings[SERVICES_IDS_NB];

  /**
   * Names of the differents representations, used to display
   * correctly the RepresentationsIds enum
   */
  extern const char* RepresentationsIdStrings[REPRESENTATIONS_IDS_NB];

#ifdef __cplusplus
} /* end of namespace castor */
#endif

#endif /* CASTOR_CONSTANTS_HPP */
