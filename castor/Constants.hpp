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
    OBJ_BlobRequest,
    OBJ_RequestHeader,
    OBJ_File,
    OBJ_Client,
    OBJ_Copy,
    OBJ_Segment,
    OBJ_TapeInfo,
    OBJ_Flag,
    OBJ_StageInRequest,
    OBJ_StageQryRequest,
    OBJ_MessageAck,
    OBJ_TpFileInfo,
    OBJ_TpInfo,
    OBJ_Cuuid
  };
    
  /**
   * Ids of services
   */
  enum ServicesIds {
    SVC_INVALID,
    SVC_MSG,
    SVC_ORACNV,
    SVC_ODBCCNV,
    SVC_STREAMCNV
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
    
#ifdef __cplusplus
} // end of namespace castor
#endif

#endif // CASTOR_CONSTANTS_HPP
