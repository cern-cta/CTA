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
    OBJ_INVALID,
    OBJ_Ptr, // Only used for streaming for circular dependencies
    OBJ_CastorFile,
    OBJ_Client,
    OBJ_Cuuid,
    OBJ_DiskCopy,
    OBJ_DiskFile,
    OBJ_DiskPool,
    OBJ_DiskServer,
    OBJ_EndResponse,
    OBJ_FileClass,
    OBJ_FileResponse,
    OBJ_FileSystem,
    OBJ_IClient,
    OBJ_MessageAck,
    OBJ_ReqId,
    OBJ_ReqIdRequest,
    OBJ_Request,
    OBJ_Segment,
    OBJ_StageClrRequest,
    OBJ_StageFilChgRequest,    
    OBJ_StageGetNextRequest,    
    OBJ_StageInRequest,
    OBJ_StageOutRequest,
    OBJ_StageQryRequest,
    OBJ_StageUpdcRequest,
    OBJ_Stream,
    OBJ_SubRequest,
    OBJ_SvcClass,
    OBJ_Tape,
    OBJ_TapeCopy,
    OBJ_TapePool,
    OBJ_StringResponse,
    OBJ_StageFileQueryRequest,
    OBJ_StageFindRequestRequest,
    OBJ_StageGetRequest,
    OBJ_StagePrepareToGetRequest,
    OBJ_StagePrepareToPutRequest,
    OBJ_StagePrepareToUpdateRequest,
    OBJ_StagePutDoneRequest,
    OBJ_StagePutRequest,
    OBJ_StageRequestQueryRequest,
    OBJ_StageRmRequest,
    OBJ_StageUpdateFileStatusRequest,
    OBJ_StageUpdateRequest
  };
    
  /**
   * Ids of services
   */
  enum ServicesIds {
    SVC_INVALID,
    SVC_NOMSG,
    SVC_DLFMSG,
    SVC_STDMSG,
    SVC_ORACNV,
    SVC_ODBCCNV,
    SVC_STREAMCNV,
    SVC_ORASTAGERSVC
  };

  /**
   * Ids of persistent representations
   */
  enum RepresentationsIds {
    REP_INVALID,
    REP_ORACLE,
    REP_ODBC,
    REP_STREAM
  };

  /**
   * Magic numbers
   */
#define SEND_REQUEST_MAGIC 0x1e10131

  /**
   * RH server port
   */
#define RHSERVER_PORT 9002
    
#ifdef __cplusplus
} // end of namespace castor
#endif

#endif // CASTOR_CONSTANTS_HPP
