/******************************************************************************
 *                      ProtocolFacade.cpp
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
 * @(#)RCSfile: ProtocolFacade.cpp  Revision: 1.0  Release Date: Nov 7, 2005  Author: mbraeger 
 *
 *
 *
 * @author Matthias Braeger
 *****************************************************************************/

#include "castor/BaseAddress.hpp"
#include "castor/Constants.hpp"
#include "castor/Services.hpp"
#include "castor/exception/InvalidArgument.hpp"
#include "castor/server/NotifierThread.hpp"
#include "castor/vdqm/DevTools.hpp"
#include "castor/vdqm/OldRequestFacade.hpp"
#include "castor/vdqm/OldProtocolInterpreter.hpp"
#include "castor/vdqm/ProtocolFacade.hpp"
#include "castor/vdqm/SocketHelper.hpp"
#include "castor/vdqm/VdqmDlfMessageConstants.hpp"
#include "castor/vdqm/VdqmMagic2ProtocolInterpreter.hpp"
#include "castor/vdqm/handler/VdqmMagic2RequestHandler.hpp"
#include "h/vdqm_constants.h"  //e.g. Magic Number of old vdqm protocol

#include <sstream>


//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::vdqm::ProtocolFacade::ProtocolFacade(
  castor::io::ServerSocket* serverSocket,
  const Cuuid_t &cuuid) throw(castor::exception::Exception) :
  m_cuuid(cuuid) {
  
  if ( 0 == serverSocket) {
    castor::exception::InvalidArgument ex;
    ex.getMessage() << "serverSocket argument is NULL";
    throw ex;
  } else {
    ptr_serverSocket = serverSocket;
  }
}


//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
castor::vdqm::ProtocolFacade::~ProtocolFacade() 
  throw() {
}


//------------------------------------------------------------------------------
// handleProtocolVersion
//------------------------------------------------------------------------------
void castor::vdqm::ProtocolFacade::handleProtocolVersion()
  throw (castor::exception::Exception) {
  
  //The magic Number of the message on the socket
  unsigned int magicNumber;  
  
  // get the incoming request
  try {
    // First check of the Protocol
    magicNumber = SocketHelper::readMagicNumber(ptr_serverSocket);
  } catch (castor::exception::Exception &e) {  
    // "Unable to read Request from socket" message
    castor::dlf::Param params[] = {
      castor::dlf::Param("Standard Message", sstrerror(e.code())),
      castor::dlf::Param("Precise Message", e.getMessage().str())};
    castor::dlf::dlf_writep(m_cuuid, DLF_LVL_ERROR, VDQM_FAILED_SOCK_READ, 2,
      params);
  }

  switch (magicNumber) {
    case VDQM_MAGIC:
      // Request has MagicNumber from old VDQM Protocol
      castor::dlf::dlf_writep(m_cuuid, DLF_LVL_DEBUG,
        VDQM_REQUEST_HAS_OLD_PROTOCOL_MAGIC_NB);
      
      try {
        handleOldVdqmRequest(magicNumber);
      } catch (castor::exception::Exception &e) {
        // most of the exceptions are handled inside the function
        
        // "Exception caught" message
        castor::dlf::Param params[] = {
          castor::dlf::Param("Message", e.getMessage().str()),
          castor::dlf::Param("errorCode", e.code())};      
        castor::dlf::dlf_writep(m_cuuid, DLF_LVL_ERROR, VDQM_EXCEPTION, 2,
          params);        
      }
      break;

    case VDQM_MAGIC2:
      try {
        handleVdqmMagic2Request(magicNumber);
      } catch (castor::exception::Exception &e) {
        // "Exception caught" message
        castor::dlf::Param params[] = {
          castor::dlf::Param("Message", e.getMessage().str()),
          castor::dlf::Param("errorCode", e.code())};
        castor::dlf::dlf_writep(m_cuuid, DLF_LVL_ERROR, VDQM_EXCEPTION, 2,
          params);
      }
      break;

    default:
      //Wrong Magic number
      castor::dlf::Param params[] = {
        castor::dlf::Param("Magic Number", magicNumber),
        castor::dlf::Param("VDQM_MAGIC", VDQM_MAGIC)};
      castor::dlf::dlf_writep(m_cuuid, DLF_LVL_ERROR, VDQM_WRONG_MAGIC_NB, 2,
        params);
  }    
}


//------------------------------------------------------------------------------
// handleOldVdqmRequest
//------------------------------------------------------------------------------
void castor::vdqm::ProtocolFacade::handleOldVdqmRequest(
  const unsigned int magicNumber) throw (castor::exception::Exception) {
   
  // Message of the old Protocol
  vdqmVolReq_t volumeRequest;
  vdqmDrvReq_t driveRequest;
  vdqmHdr_t    header;
  int reqtype; // Request type of the message
  int rc = 0; // For checking purpose
  bool reqHandled = false;
  
  // Needed for commit and rollback actions on the database
  castor::BaseAddress ad;
  ad.setCnvSvcName("DbCnvSvc");
  ad.setCnvSvcType(castor::SVC_DBCNV);   
  
  // Handles the connection to the client
  OldProtocolInterpreter oldProtInterpreter(ptr_serverSocket, &m_cuuid);
  
  // The socket read out phase
  try {
    // Allcocate memory for structs
    memset(&volumeRequest,'\0',sizeof(volumeRequest));
    memset(&driveRequest ,'\0',sizeof(driveRequest));
    memset(&header       ,'\0',sizeof(header));

    // Put the magic number into the header struct
    header.magic = magicNumber;

    // read the rest of the vdqm message    
    reqtype = oldProtInterpreter.readProtocol(&header, &volumeRequest, 
      &driveRequest);                            
  } catch (castor::exception::Exception &e) {  
    // "Unable to read Request from socket" message
    castor::dlf::Param params[] = {
      castor::dlf::Param("Message", e.getMessage().str())};
    castor::dlf::dlf_writep(m_cuuid, DLF_LVL_ERROR, VDQM_FAILED_SOCK_READ, 1,
      params);
    
    return;
  }

#ifdef PRINT_NETWORK_MESSAGES
  castor::vdqm::DevTools::printMessage(std::cout, false, false,
    ptr_serverSocket->socket(), &header);
#endif
  
  // Initialization of the OldRequestFacade, which provides the essential
  // functions
  OldRequestFacade oldRequestFacade(&volumeRequest, &driveRequest, &header);
  
  // The request handling phase
  try {

    // Handle old vdqm request type                                      
    castor::dlf::dlf_writep(m_cuuid, DLF_LVL_DEBUG,
      VDQM_HANDLE_OLD_VDQM_REQUEST_TYPE);
    
    oldRequestFacade.checkRequestType(m_cuuid);
    reqHandled = oldRequestFacade.handleRequestType(&oldProtInterpreter,
      m_cuuid);

  } catch (castor::exception::Exception &e) { 
    castor::dlf::Param params2[] = {
      castor::dlf::Param("Message", e.getMessage().str()),
      castor::dlf::Param("errorCode", e.code())};   

    // "Exception caught" message 
    if (e.code() == EVQREQASS) {
      // This error code is used, when tpread/tpdump wants to delete 
      // a tape request, which is still assigned to a tape drive or vice versa.
      // Anyway, it is not a big deal, so we just log a warning message.
      castor::dlf::dlf_writep(m_cuuid, DLF_LVL_WARNING, VDQM_EXCEPTION, 2,
        params2);
    } else {
      castor::dlf::dlf_writep(m_cuuid, DLF_LVL_ERROR, VDQM_EXCEPTION, 2,
        params2);
    }

    // "VdqmServer::handleOldVdqmRequest(): Rollback of the whole request"
    // message   
    castor::dlf::dlf_writep(m_cuuid, DLF_LVL_SYSTEM,
      VDQM_HANDLE_OLD_VDQM_REQUEST_ROLLBACK);
    svcs()->rollback(&ad); 
    
    // Tell the client about the error
    try {
      if (reqtype == VDQM_PING) {
        //This is Protocol specific, because normally it receives the positive
        // queue position number back.
        oldProtInterpreter.sendAcknPing(-e.code());
      } else {
        oldProtInterpreter.sendAcknRollback(e.code());
      }
    } catch (castor::exception::Exception &e) {  
      // "Exception caught" message
      castor::dlf::Param params[] = {
        castor::dlf::Param("Message", e.getMessage().str())};
      castor::dlf::dlf_writep(m_cuuid, DLF_LVL_ERROR, VDQM_EXCEPTION, 1,
        params);
    }
    
    // Now, we don't need to make a handshake
    return;
  }
  
  
  // Handshake phase
  try {

    // Ping requests don't need a handshake! It is already handled in 
    // OldRequestFacade.
    if (reqHandled && reqtype != VDQM_PING) {
      // Sending reply to client
      castor::dlf::dlf_writep(m_cuuid, DLF_LVL_DEBUG,
        VDQM_SEND_REPLY_TO_CLIENT);
      
      oldProtInterpreter.sendAcknCommit();
      oldProtInterpreter.sendToOldClient(&header, &volumeRequest,
        &driveRequest);
      
      // "VdqmServer::handleOldVdqmRequest(): waiting for client acknowledge"
      // message
      castor::dlf::dlf_writep(m_cuuid, DLF_LVL_DEBUG,
        VDQM_HANDLE_OLD_VDQM_REQUEST_WAITING_FOR_CLIENT_ACK);
      rc = oldProtInterpreter.recvAcknFromOldClient();

      // Now we can commit everything for the database... or not
      if ( rc  == VDQM_COMMIT) {
        svcs()->commit(&ad);

        // Notify the RTCP job submitter threads of possible work to be done
        castor::server::NotifierThread::getInstance()->doNotify('J');
        
        // "Request stored in DB" message
        castor::dlf::dlf_writep(m_cuuid, DLF_LVL_DEBUG,
          VDQM_REQUEST_STORED_IN_DB);        
      } else {
        // "Client didn't send a VDQM_COMMIT => Rollback of request in db"
        svcs()->rollback(&ad);
          
        castor::dlf::dlf_writep(m_cuuid, DLF_LVL_SYSTEM,
          VDQM_NO_VDQM_COMMIT_FROM_CLIENT);        
      }
    }
  } catch (castor::exception::Exception &e) { 
    // "Exception caught" message
    castor::dlf::Param params[] = {
      castor::dlf::Param("Message", e.getMessage().str())};
    castor::dlf::dlf_writep(m_cuuid, DLF_LVL_ERROR, VDQM_EXCEPTION, 1, params);
    
    // "VdqmServer::handleOldVdqmRequest(): Rollback of the whole request"
    // message   
    castor::dlf::dlf_writep(m_cuuid, DLF_LVL_SYSTEM,
      VDQM_HANDLE_OLD_VDQM_REQUEST_ROLLBACK);        
    svcs()->rollback(&ad);
  }
}


//------------------------------------------------------------------------------
// handleVdqmMagic2Request
//------------------------------------------------------------------------------
void castor::vdqm::ProtocolFacade::handleVdqmMagic2Request(
  const unsigned int magicNumber) throw (castor::exception::Exception) {

  vdqmHdr_t header;

  // The following protocol interpreters handle the connection to the client
  OldProtocolInterpreter oldProtInterpreter(ptr_serverSocket, &m_cuuid);
  VdqmMagic2ProtocolInterpreter vdqmMagic2ProtInterpreter(ptr_serverSocket,
    m_cuuid);

  // Needed for commit and rollback actions on the database
  castor::BaseAddress ad;
  ad.setCnvSvcName("DbCnvSvc");
  ad.setCnvSvcType(castor::SVC_DBCNV);


  // Read out the message header
  try {
    memset(&header, '\0', sizeof(header));

    vdqmMagic2ProtInterpreter.readHeader(magicNumber, &header);
  } catch (castor::exception::Exception &e) {
    // "Unable to read Request from socket" message
    castor::dlf::Param params[] = {
      castor::dlf::Param("Message", "Failed to read request type: " +
        e.getMessage().str())};
    castor::dlf::dlf_writep(m_cuuid, DLF_LVL_ERROR, VDQM_FAILED_SOCK_READ, 1,
      params);

    return;
  }

  switch(header.reqtype) {
  case VDQM2_SET_VOL_PRIORITY:
  {
    vdqmVolPriority_t vdqmVolPriority;

    vdqmMagic2ProtInterpreter.readVolPriority(header.len, &vdqmVolPriority);
    handler::VdqmMagic2RequestHandler requestHandler;
    requestHandler.handleVolPriority(m_cuuid, &vdqmVolPriority);

    break;
  }
  default: // Invalid request type
    std::stringstream oss;
    oss << "Invalid Request 0x" << std::hex << header.reqtype << std::endl;

    castor::dlf::Param params2[] = {
    castor::dlf::Param("Message", oss.str()),
    castor::dlf::Param("errorCode", SEOPNOTSUP)};

    castor::dlf::dlf_writep(m_cuuid, DLF_LVL_ERROR, VDQM_EXCEPTION, 2,
      params2);

    // Rollback of the whole request message
    castor::dlf::dlf_writep(m_cuuid, DLF_LVL_SYSTEM,
      VDQM_MAGIC2_VOL_PRIORITY_ROLLBACK);
    svcs()->rollback(&ad);

    // Tell the client about the error
    try {
      oldProtInterpreter.sendAcknRollback(SEOPNOTSUP);
    } catch (castor::exception::Exception &e) {
      // "Exception caught" message
      castor::dlf::Param params[] = {
        castor::dlf::Param("Message", e.getMessage().str())};
      castor::dlf::dlf_writep(m_cuuid, DLF_LVL_ERROR, VDQM_EXCEPTION, 1,
        params);
    }

    // Now, we don't need to make a handshake
    return;
  }

  // Handshake phase
  try {

    // Send acknowledge to client
    oldProtInterpreter.sendAcknCommit();

    // Waiting for client acknowledge
    int rc = oldProtInterpreter.recvAcknFromOldClient();

    // Now we can commit everything for the database... or not
    if ( rc  == VDQM_COMMIT) {
      svcs()->commit(&ad);
    } else {
      // "Client didn't send a VDQM_COMMIT => Rollback of request in db"
      svcs()->rollback(&ad);

      castor::dlf::dlf_writep(m_cuuid, DLF_LVL_SYSTEM,
        VDQM_NO_VDQM_COMMIT_FROM_CLIENT);
    }
  } catch (castor::exception::Exception &e) {
    // "Exception caught" message
    castor::dlf::Param params[] = {
      castor::dlf::Param("Message", e.getMessage().str())};
    castor::dlf::dlf_writep(m_cuuid, DLF_LVL_ERROR, VDQM_EXCEPTION, 1, params);

    // Rollback of the whole request message
    castor::dlf::dlf_writep(m_cuuid, DLF_LVL_SYSTEM,
      VDQM_MAGIC2_VOL_PRIORITY_ROLLBACK);
    svcs()->rollback(&ad);
  }
}
