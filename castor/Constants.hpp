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
    OBJ_EndResponse = 9,
    OBJ_FileClass = 10,
    OBJ_FileResponse = 11,
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
    OBJ_StringResponse = 32,
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
    OBJ_Response = 52,
    OBJ_IOResponse = 53,
    OBJ_AbortResponse = 54,
    OBJ_RequestQueryResponse = 55,
    OBJ_FileQueryResponse = 56,
    OBJ_FindReqResponse = 57,
    OBJ_DiskCopyForRecall = 58,
    OBJ_TapeCopyForMigration = 59,
    OBJ_GetUpdateStartRequest = 60,
    OBJ_GetUpdateStartResponse = 61,
    OBJ_BaseAddress = 62,
    OBJ_BasicResponse = 63,
    OBJ_Disk2DiskCopyDoneRequest = 64,
    OBJ_MoverCloseRequest = 65,
    OBJ_StartRequest = 66,
    OBJ_PutStartRequest = 67,
    OBJ_StartResponse = 68,
    OBJ_IObject = 69,
    OBJ_IAddress = 70,
    OBJ_QueryParameter = 71,
    OBJ_DiskCopyInfo = 72
  };
    
  /**
   * Ids of services
   */
  enum ServicesIds {
    SVC_INVALID = 0,
    SVC_NOMSG = 1,
    SVC_DLFMSG = 2,
    SVC_STDMSG = 3,
    SVC_ORACNV = 4,
    SVC_ODBCCNV = 5,
    SVC_STREAMCNV = 6,
    SVC_ORASTAGERSVC = 7,
    SVC_REMOTESTAGERSVC = 8,
    SVC_ORAQUERYSVC = 9
  };

  /**
   * Ids of persistent representations
   */
  enum RepresentationsIds {
    REP_INVALID = 0,
    REP_ORACLE = 1,
    REP_ODBC = 2,
    REP_STREAM = 3
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
   * Names of the differents objects, used to display
   * correctly the ObjectsIds enum
   */
  extern const char* ObjectsIdStrings[73];

  /**
   * Names of the differents Services, used to display
   * correctly the ServicesIds enum
   */
  extern const char* ServicesIdStrings[10];

  /**
   * Names of the differents representations, used to display
   * correctly the RepresentationsIds enum
   */
  extern const char* RepresentationsIdStrings[4];
    
#ifdef __cplusplus
} // end of namespace castor
#endif

#endif // CASTOR_CONSTANTS_HPP
