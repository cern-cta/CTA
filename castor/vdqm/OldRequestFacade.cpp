/******************************************************************************
 *                      OldRequestFacade.cpp
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
 * @(#)RCSfile: OldRequestFacade.cpp  Revision: 1.0  Release Date: Apr 18, 2005  Author: mbraeger 
 *
 *
 *
 * @author Matthias Braeger
 *****************************************************************************/

#include "castor/Constants.hpp"
#include "castor/exception/Internal.hpp"
#include "castor/exception/NotSupported.hpp"
#include "castor/io/ServerSocket.hpp"
#include "castor/System.hpp"
#include "castor/vdqm/DevTools.hpp"
#include "castor/vdqm/OldProtocolInterpreter.hpp"
#include "castor/vdqm/OldRequestFacade.hpp"
#include "castor/vdqm/SocketHelper.hpp"
#include "castor/vdqm/VdqmDlfMessageConstants.hpp"
#include "castor/vdqm/handler/TapeDriveHandler.hpp"
#include "castor/vdqm/handler/TapeRequestHandler.hpp"
#include "h/Cupv_api.h"
#include "h/net.h"
#include "h/vdqm_constants.h"

#include <sstream>
#include <string>

// To make the code more readable
using namespace castor::vdqm::handler;

 
//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
castor::vdqm::OldRequestFacade::OldRequestFacade(
  vdqmVolReq_t *const volumeRequest, vdqmDrvReq_t *const driveRequest,
  vdqmHdr_t *const header, castor::io::ServerSocket &socket) :
  ptr_volumeRequest(volumeRequest), ptr_driveRequest(driveRequest),
  ptr_header(header), m_socket(socket), m_reqtype(header->reqtype) {
}


//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
castor::vdqm::OldRequestFacade::~OldRequestFacade() throw() {
}


//------------------------------------------------------------------------------
// checkRequestType
//------------------------------------------------------------------------------
void castor::vdqm::OldRequestFacade::checkRequestType(const Cuuid_t cuuid) 
  throw (castor::exception::Exception) {

  int i;
  int req_values[] = VDQM_REQ_VALUES;
  std::string req_strings[] = VDQM_REQ_STRINGS;
  std::string req_string;
  
  i=0;
  //Check, if it's a right request type
  while (req_values[i] != -1 && req_values[i] != m_reqtype) i++;
  
  //reqtype as string representation 
  req_string = req_strings[i];
  if ((m_reqtype != VDQM_GET_VOLQUEUE) &&
      (m_reqtype != VDQM_GET_DRVQUEUE) &&
      (m_reqtype != VDQM_PING)) {

    castor::dlf::Param params[] = {
      castor::dlf::Param("req_string", req_string)};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_DEBUG, VDQM_NEW_VDQM_REQUEST, 1,
      params);
  }
  
  if ( !VDQM_VALID_REQTYPE(m_reqtype) ) {
    castor::exception::NotSupported ex;
    ex.getMessage() << "Invalid Request 0x"
                    << std::hex << m_reqtype << "\n";
    throw ex;
  } 
}


//------------------------------------------------------------------------------
// handleRequestType
//------------------------------------------------------------------------------
bool castor::vdqm::OldRequestFacade::handleRequestType(
  OldProtocolInterpreter* oldProtInterpreter, const Cuuid_t cuuid)
  throw (castor::exception::Exception) {
  
  bool handleRequest = true;

  switch (m_reqtype) {
  case VDQM_VOL_REQ:
    if(ptr_header == NULL || ptr_volumeRequest == NULL) {
      handleRequest = false;
    } else {
      logVolumeRequest(ptr_header, ptr_volumeRequest, cuuid, DLF_LVL_SYSTEM);

      // User root is not allowed to make a volume request
      if(ptr_volumeRequest->clientUID == 0 &&
        ptr_volumeRequest->clientGID == 0) {

        // Try to get client hostname
        std::string clientHostname;
        {
          unsigned short port;
          unsigned long  ip;

          try {
            m_socket.getPeerIp(port, ip);
            clientHostname = castor::System::ipAddressToHostname(ip);
          } catch(castor::exception::Exception &e) {
            clientHostname = "UNKNOWN";
          }
        }

        castor::exception::PermissionDenied pe;

        pe.getMessage() << "User root is not allowed to make a volume request. "
          "source_host=" << clientHostname;

        throw pe;
      }

      TapeRequestHandler requestHandler;
      requestHandler.newTapeRequest(*ptr_header, *ptr_volumeRequest, cuuid); 
    }
    break;
  case VDQM_DRV_REQ:
    if(ptr_header == NULL || ptr_driveRequest == NULL) {
      handleRequest = false;
    } else {
      logDriveRequest(ptr_header, ptr_driveRequest, cuuid, DLF_LVL_SYSTEM);
      TapeDriveHandler tapeDriveHandler(ptr_header, ptr_driveRequest, cuuid);
      tapeDriveHandler.newTapeDriveRequest();
    }
    break;
  case VDQM_DEL_VOLREQ:
    if(ptr_header == NULL || ptr_volumeRequest == NULL) {
      handleRequest = false;
    } else {        
      logVolumeRequest(ptr_header, ptr_volumeRequest, cuuid, DLF_LVL_SYSTEM);

      TapeRequestHandler requestHandler;
      requestHandler.deleteTapeRequest(ptr_volumeRequest, cuuid, m_socket); 
    }
    break;
  case VDQM_DEL_DRVREQ:
    {
      castor::exception::PermissionDenied pe;

      pe.getMessage() << "VDQM_DEL_DRVREQ message is not supported";

      throw pe;
    }
    break;
  case VDQM_GET_VOLQUEUE:
    if(ptr_header == NULL || ptr_volumeRequest == NULL) {
      handleRequest = false;
    } else {
      logVolumeRequest(ptr_header, ptr_volumeRequest, cuuid, DLF_LVL_DEBUG);
      TapeRequestHandler requestHandler;
      // Sends the tape request queue back to the client
      requestHandler.sendTapeRequestQueue(ptr_header, ptr_volumeRequest, 
        oldProtInterpreter, cuuid);
    }
    break;
  case VDQM_GET_DRVQUEUE:
    if(ptr_header == NULL || ptr_driveRequest == NULL) {
      handleRequest = false;
    } else {
      logDriveRequest(ptr_header, ptr_driveRequest, cuuid, DLF_LVL_DEBUG);
      TapeDriveHandler tapeDriveHandler(ptr_header, ptr_driveRequest, cuuid);
      tapeDriveHandler.sendTapeDriveQueue(ptr_volumeRequest,
        oldProtInterpreter);
    }
    break;
  case VDQM_DEDICATE_DRV:
    {
      castor::exception::PermissionDenied pe;

      pe.getMessage() << "VDQM_DEDICATE_DRV message is not supported";

      throw pe;
    }
    break;
  case VDQM_PING:
    if(ptr_header == NULL || ptr_volumeRequest == NULL) {
      handleRequest = false;
    } else {
      logVolumeRequest(ptr_header, ptr_volumeRequest, cuuid, DLF_LVL_DEBUG);
      int queuePosition = -1;
      TapeRequestHandler requestHandler;
      queuePosition = requestHandler.getQueuePosition(ptr_volumeRequest, cuuid);

      // Send VDQM_PING back to client
      oldProtInterpreter->sendAcknPing(queuePosition);
    }
    break;

  default:
    castor::exception::NotSupported ex;

    if ( VDQM_VALID_REQTYPE(m_reqtype) ) 
      ex.getMessage() << "Valid but not supported request 0x"
        << std::hex << m_reqtype << "\n";
    else {
      ex.getMessage() << "Invalid request 0x"
        << std::hex << m_reqtype << "\n";
    }

    throw ex;
  }
  
  return handleRequest;
}


//------------------------------------------------------------------------------
// logDriveRequest
//------------------------------------------------------------------------------
void castor::vdqm::OldRequestFacade::logDriveRequest(
  const vdqmHdr_t *const header, const vdqmDrvReq_t *const request,
  const Cuuid_t cuuid, int severity) {

  std::stringstream status;

  castor::vdqm::DevTools::printTapeDriveStatusBitset(status, request->status);

  castor::dlf::Param params[] = {
    castor::dlf::Param("magic"   ,
      castor::vdqm::DevTools::castorMagicNb2Str(header->magic)),
    castor::dlf::Param("reqtype" ,
      castor::vdqm::DevTools::vdqmReqTypeToStr(header->reqtype)),
    castor::dlf::Param("len"     , header->len),
    castor::dlf::Param("status"  , status.str()),
    castor::dlf::Param("DrvReqID", request->DrvReqID),
    castor::dlf::Param("VolReqID", request->VolReqID),
    castor::dlf::Param("jobID"   , request->jobID),
    castor::dlf::Param("recvtime", request->recvtime),
    castor::dlf::Param("usecount", request->usecount),
    castor::dlf::Param("errcount", request->errcount),
    castor::dlf::Param("MBtransf", request->MBtransf),
    castor::dlf::Param("mode"    , request->mode),
    castor::dlf::Param("TotalMB" , request->TotalMB),
    castor::dlf::Param("reqhost" , request->reqhost),
    castor::dlf::Param("volid"   , request->volid),
    castor::dlf::Param("server"  , request->server),
    castor::dlf::Param("drive"   , request->drive),
    castor::dlf::Param("dgn"     , request->dgn),
    castor::dlf::Param("dedicate", request->dedicate)};

  castor::dlf::dlf_writep(cuuid, severity, VDQM_HANDLE_DRV_REQ, 19, params);
}


//------------------------------------------------------------------------------
// logVolumeRequest
//------------------------------------------------------------------------------
void castor::vdqm::OldRequestFacade::logVolumeRequest(
  const vdqmHdr_t *const header, const vdqmVolReq_t *const request,
  const Cuuid_t cuuid, int severity) {

  castor::dlf::Param params[] = {
    castor::dlf::Param("magic"      ,
      castor::vdqm::DevTools::castorMagicNb2Str(header->magic)),
    castor::dlf::Param("reqtype"    ,
      castor::vdqm::DevTools::vdqmReqTypeToStr(header->reqtype)),
    castor::dlf::Param("len"        , header->len),
    castor::dlf::Param("VolReqID"   , request->VolReqID),
    castor::dlf::Param("DrvReqID"   , request->DrvReqID),
    castor::dlf::Param("priority"   , request->priority),
    castor::dlf::Param("client_port", request->client_port),
    castor::dlf::Param("recvtime"   , request->recvtime),
    castor::dlf::Param("clientUID"  , request->clientUID),
    castor::dlf::Param("cllentGID"  , request->clientGID),
    castor::dlf::Param("mode"       , request->mode),
    castor::dlf::Param("client_host", request->client_host),
    castor::dlf::Param("volid"      , request->volid),
    castor::dlf::Param("server"     , request->server),
    castor::dlf::Param("drive"      , request->drive),
    castor::dlf::Param("dgn"        , request->dgn),
    castor::dlf::Param("client_name", request->client_name)};
  castor::dlf::dlf_writep(cuuid, severity, VDQM_HANDLE_VOL_REQ, 17, params);
}
