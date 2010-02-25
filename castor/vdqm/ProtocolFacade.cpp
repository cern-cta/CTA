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
 *
 *
 *
 * @author Matthias Braeger
 *****************************************************************************/

#include "castor/BaseAddress.hpp"
#include "castor/Constants.hpp"
#include "castor/Services.hpp"
#include "castor/exception/Internal.hpp"
#include "castor/exception/InvalidArgument.hpp"
#include "castor/exception/NotSupported.hpp"
#include "castor/server/NotifierThread.hpp"
#include "castor/System.hpp"
#include "castor/vdqm/DevTools.hpp"
#include "castor/vdqm/OldRequestFacade.hpp"
#include "castor/vdqm/OldProtocolInterpreter.hpp"
#include "castor/vdqm/ProtocolFacade.hpp"
#include "castor/vdqm/SocketHelper.hpp"
#include "castor/vdqm/VdqmDlfMessageConstants.hpp"
#include "castor/vdqm/VdqmMagic2ProtocolInterpreter.hpp"
#include "castor/vdqm/VdqmMagic3ProtocolInterpreter.hpp"
#include "castor/vdqm/VdqmMagic4ProtocolInterpreter.hpp"
#include "castor/vdqm/handler/VdqmMagic2RequestHandler.hpp"
#include "castor/vdqm/handler/VdqmMagic3RequestHandler.hpp"
#include "castor/vdqm/handler/VdqmMagic4RequestHandler.hpp"
#include "h/vdqm_constants.h"

#include <sstream>


//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::vdqm::ProtocolFacade::ProtocolFacade(castor::io::ServerSocket &socket,
  const Cuuid_t &cuuid) throw(castor::exception::Exception) :
  m_socket(socket), m_cuuid(cuuid) {
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
    magicNumber = SocketHelper::readMagicNumber(m_socket);
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

    case VDQM_MAGIC3:
      try {
        handleVdqmMagic3Request(magicNumber);
      } catch (castor::exception::Exception &e) {
        // "Exception caught" message
        castor::dlf::Param params[] = {
          castor::dlf::Param("Message", e.getMessage().str()),
          castor::dlf::Param("errorCode", e.code())};
        castor::dlf::dlf_writep(m_cuuid, DLF_LVL_ERROR, VDQM_EXCEPTION, 2,
          params);
      }
      break;

    case VDQM_MAGIC4:
      try {
        handleVdqmMagic4Request(magicNumber);
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
      // Wrong Magic number
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
  OldProtocolInterpreter oldProtInterpreter(m_socket, m_cuuid);
  
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
    m_socket.socket(), &header);
#endif

  // Initialization of the OldRequestFacade, which provides the essential
  // functions
  OldRequestFacade oldRequestFacade(&volumeRequest, &driveRequest, &header,
    m_socket);
  
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
  OldProtocolInterpreter oldProtInterpreter(m_socket, m_cuuid);
  VdqmMagic2ProtocolInterpreter vdqmMagic2ProtInterpreter(m_socket,
    m_cuuid);

  // Needed for commit and rollback actions on the database
  castor::BaseAddress ad;
  ad.setCnvSvcName("DbCnvSvc");
  ad.setCnvSvcType(castor::SVC_DBCNV);


  // Read out the message header
  try {
    memset(&header, '\0', sizeof(header));

    vdqmMagic2ProtInterpreter.readHeader(magicNumber, header);
  } catch (castor::exception::Exception &e) {
    // "Unable to read Request from socket" message
    castor::dlf::Param params[] = {
      castor::dlf::Param("Message", "Failed to read request type: " +
        e.getMessage().str())};
    castor::dlf::dlf_writep(m_cuuid, DLF_LVL_ERROR, VDQM_FAILED_SOCK_READ, 1,
      params);

    return;
  }

  try {
    switch(header.reqtype) {
    case VDQM2_SET_VOL_PRIORITY:
    {
      vdqmVolPriority_t vdqmVolPriority;

      vdqmMagic2ProtInterpreter.readVolPriority(header.len, vdqmVolPriority);
      handler::VdqmMagic2RequestHandler requestHandler;
      requestHandler.handleVolPriority(m_cuuid, vdqmVolPriority);

      break;
    }
    default: // Invalid request type
      castor::exception::NotSupported ne;

      ne.getMessage() << "Invalid Request 0x" << std::hex << header.reqtype;

      throw ne;
    }
  } catch(castor::exception::Exception &e) {

    // Log the exception
    castor::dlf::Param params2[] = {
    castor::dlf::Param("Message", e.getMessage().str()),
    castor::dlf::Param("errorCode", SEOPNOTSUP)};

    castor::dlf::dlf_writep(m_cuuid, DLF_LVL_ERROR, VDQM_EXCEPTION, 2,
      params2);

    // Rollback the whole request message
    castor::dlf::dlf_writep(m_cuuid, DLF_LVL_SYSTEM, VDQM_MAGIC2_ROLLBACK);
    svcs()->rollback(&ad);

    // Tell the client about the error
    try {
      oldProtInterpreter.sendAcknRollback(e.code());
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
      VDQM_MAGIC2_ROLLBACK);
    svcs()->rollback(&ad);
  }
}


//------------------------------------------------------------------------------
// handleVdqmMagic3Request
//------------------------------------------------------------------------------
void castor::vdqm::ProtocolFacade::handleVdqmMagic3Request(
  const unsigned int magicNumber) throw (castor::exception::Exception) {

  vdqmHdr_t header;

  OldProtocolInterpreter oldProtInterpreter(m_socket, m_cuuid);
  VdqmMagic3ProtocolInterpreter protInterpreter(m_socket, m_cuuid);

  // Needed for commit and rollback actions on the database
  castor::BaseAddress ad;
  ad.setCnvSvcName("DbCnvSvc");
  ad.setCnvSvcType(castor::SVC_DBCNV);


  // Read out the message header
  try {
    memset(&header, '\0', sizeof(header));

    protInterpreter.readHeader(magicNumber, header);
  } catch (castor::exception::Exception &e) {
    // "Unable to read Request from socket" message
    castor::dlf::Param params[] = {
      castor::dlf::Param("Message", "Failed to read request type: " +
        e.getMessage().str())};
    castor::dlf::dlf_writep(m_cuuid, DLF_LVL_ERROR, VDQM_FAILED_SOCK_READ, 1,
      params);

    return;
  }

  try {
    switch(header.reqtype) {
    case VDQM3_DEL_DRV:
    {
      vdqmDelDrv_t msg;

      protInterpreter.readDelDrv(header.len, msg);
      handler::VdqmMagic3RequestHandler requestHandler;
      requestHandler.handleDelDrv(m_socket, m_cuuid, msg);

      break;
    }
    case VDQM3_DEDICATE:
    {
      vdqmDedicate_t msg;

      protInterpreter.readDedicate(header.len, msg);
      handler::VdqmMagic3RequestHandler requestHandler;
      requestHandler.handleDedicate(m_socket, m_cuuid, msg);

      break;
    }
    default: // Invalid request type
      castor::exception::NotSupported ne;

      ne.getMessage() << "Invalid Request 0x" << std::hex << header.reqtype;

      throw ne;
    }
  } catch(castor::exception::Exception &e) {

    // Log the exception
    castor::dlf::Param params2[] = {
    castor::dlf::Param("Message", e.getMessage().str()),
    castor::dlf::Param("errorCode", SEOPNOTSUP)};

    castor::dlf::dlf_writep(m_cuuid, DLF_LVL_ERROR, VDQM_EXCEPTION, 2,
      params2);

    // Rollback the whole request message
    castor::dlf::dlf_writep(m_cuuid, DLF_LVL_SYSTEM, VDQM_MAGIC3_ROLLBACK);
    svcs()->rollback(&ad);

    // Tell the client about the error
    try {
      oldProtInterpreter.sendAcknRollback(e.code());
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
    castor::dlf::dlf_writep(m_cuuid, DLF_LVL_SYSTEM, VDQM_MAGIC3_ROLLBACK);
    svcs()->rollback(&ad);
  }
}


//------------------------------------------------------------------------------
// handleVdqmMagic4Request
//------------------------------------------------------------------------------
void castor::vdqm::ProtocolFacade::handleVdqmMagic4Request(
  const unsigned int magicNumber) throw (castor::exception::Exception) {

  vdqmHdr_t    header;
  vdqmVolReq_t volReq;

  OldProtocolInterpreter oldProtInterpreter(m_socket, m_cuuid);
  VdqmMagic4ProtocolInterpreter protInterpreter(m_socket, m_cuuid);

  // Needed for commit and rollback actions on the database
  castor::BaseAddress ad;
  ad.setCnvSvcName("DbCnvSvc");
  ad.setCnvSvcType(castor::SVC_DBCNV);


  // Read out the message header
  try {
    memset(&header, '\0', sizeof(header));

    protInterpreter.readHeader(magicNumber, header);
  } catch (castor::exception::Exception &e) {
    // "Unable to read Request from socket" message
    castor::dlf::Param params[] = {
      castor::dlf::Param("Message", "Failed to read request type: " +
        e.getMessage().str())};
    castor::dlf::dlf_writep(m_cuuid, DLF_LVL_ERROR, VDQM_FAILED_SOCK_READ, 1,
      params);

    return;
  }

  try {
    switch(header.reqtype) {
    case VDQM4_TAPESERVER_VOL_REQ:
    {
      protInterpreter.readAggregatorVolReq(header.len, volReq);

      // User root is not allowed to make a volume request
      if(volReq.clientUID == 0 && volReq.clientGID == 0) {

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

      handler::VdqmMagic4RequestHandler requestHandler;
      requestHandler.handleAggregatorVolReq(m_socket, m_cuuid, header, volReq);

      break;
    }
    default: // Invalid request type
      castor::exception::NotSupported ne;

      ne.getMessage() << "Invalid Request 0x" << std::hex << header.reqtype;

      throw ne;
    }
  } catch(castor::exception::Exception &e) {

    // Log the exception
    castor::dlf::Param params2[] = {
    castor::dlf::Param("Message", e.getMessage().str()),
    castor::dlf::Param("errorCode", SEOPNOTSUP)};

    castor::dlf::dlf_writep(m_cuuid, DLF_LVL_ERROR, VDQM_EXCEPTION, 2,
      params2);

    // Rollback the whole request message
    castor::dlf::dlf_writep(m_cuuid, DLF_LVL_SYSTEM, VDQM_MAGIC4_ROLLBACK);
    svcs()->rollback(&ad);

    // Tell the client about the error
    try {
      oldProtInterpreter.sendAcknRollback(e.code());
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

    // Send request back to client
    protInterpreter.sendAggregatorVolReqToClient(header, volReq);

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
    castor::dlf::dlf_writep(m_cuuid, DLF_LVL_SYSTEM, VDQM_MAGIC4_ROLLBACK);
    svcs()->rollback(&ad);
  }
}
