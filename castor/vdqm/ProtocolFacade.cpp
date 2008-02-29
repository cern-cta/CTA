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

#include <vdqm_constants.h>  //e.g. Magic Number of old vdqm protocol
 
#include "castor/server/NotifierThread.hpp"
#include "castor/vdqm/DevTools.hpp"
#include "castor/vdqm/newVdqm.h" //Needed for the client_connection
#include "castor/vdqm/OldRequestFacade.hpp"
#include "castor/vdqm/OldProtocolInterpreter.hpp"
#include "castor/vdqm/ProtocolFacade.hpp"
#include "castor/vdqm/VdqmDlfMessageConstants.hpp"
#include "castor/vdqm/VdqmSocketHelper.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::vdqm::ProtocolFacade::ProtocolFacade(
  castor::io::ServerSocket* serverSocket,
  const Cuuid_t* cuuid) throw(castor::exception::Exception) {
  
  if ( 0 == serverSocket || 0 == cuuid) {
    castor::exception::InvalidArgument ex;
    ex.getMessage() << "One of the arguments is NULL";
    throw ex;
  }
  else {
    ptr_serverSocket = serverSocket;
    m_cuuid = cuuid;
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
    //First check of the Protocol
    magicNumber =
          VdqmSocketHelper::readMagicNumber(ptr_serverSocket->socket());
  } catch (castor::exception::Exception e) {  
    // "Unable to read Request from socket" message
    castor::dlf::Param params[] =
      {castor::dlf::Param("Standard Message", sstrerror(e.code())),
       castor::dlf::Param("Precise Message", e.getMessage().str())};
    castor::dlf::dlf_writep(*m_cuuid, DLF_LVL_ERROR, VDQM_FAILED_SOCK_READ, 2, params);
  }

  switch (magicNumber) {
    case VDQM_MAGIC:
      //Request has MagicNumber from old VDQM Protocol
      castor::dlf::dlf_writep(*m_cuuid, DLF_LVL_DEBUG, VDQM_REQUEST_HAS_OLD_PROTOCOL_MAGIC_NB );
      
      try {
        handleOldVdqmRequest(magicNumber);
      } catch (castor::exception::Exception e) {
        // most of the exceptions are handled inside the function
        
        // "Exception caught" message
        castor::dlf::Param params[] =
          {castor::dlf::Param("Message", e.getMessage().str()),
           castor::dlf::Param("errorCode", e.code())};      
        castor::dlf::dlf_writep(*m_cuuid, DLF_LVL_ERROR, VDQM_EXCEPTION, 2, params);        
      }
      break;
    
    default:
      //Wrong Magic number
      castor::dlf::Param params[] =
        {castor::dlf::Param("Magic Number", magicNumber),
         castor::dlf::Param("VDQM_MAGIC", VDQM_MAGIC)};
      castor::dlf::dlf_writep(*m_cuuid, DLF_LVL_ERROR, VDQM_WRONG_MAGIC_NB, 2, params);
  }    
}


//------------------------------------------------------------------------------
// handleOldVdqmRequest
//------------------------------------------------------------------------------
void castor::vdqm::ProtocolFacade::handleOldVdqmRequest(
  unsigned int magicNumber) 
  throw (castor::exception::Exception) {
   
   //Message of the old Protocol
  newVdqmVolReq_t volumeRequest;
  newVdqmDrvReq_t driveRequest;
  newVdqmHdr_t header;
  int reqtype; // Request type of the message
  int rc = 0; // For checking purpose
  bool reqHandled = false;
  
  // handles the connection to the client
  OldProtocolInterpreter* oldProtInterpreter;

  // Needed for commit and rollback actions on the database
  castor::BaseAddress ad;
  ad.setCnvSvcName("DbCnvSvc");
  ad.setCnvSvcType(castor::SVC_DBCNV);   
  
  
  oldProtInterpreter = new OldProtocolInterpreter(ptr_serverSocket, m_cuuid);
  
  // The socket read out phase
  try {
    //Allcocate memory for structs
    memset(&volumeRequest,'\0',sizeof(volumeRequest));
    memset(&driveRequest,'\0',sizeof(driveRequest));
    memset(&header,'\0',sizeof(header));
    
    // Put the magic number into the header struct
    header.magic = magicNumber;
    
    // read the rest of the vdqm message    
    reqtype = oldProtInterpreter->readProtocol(&header, 
                                    &volumeRequest, 
                                    &driveRequest);                            
  } catch (castor::exception::Exception e) {  
    // "Unable to read Request from socket" message
    castor::dlf::Param params[] =
      {castor::dlf::Param("Message", e.getMessage().str())};
    castor::dlf::dlf_writep(*m_cuuid, DLF_LVL_ERROR, VDQM_FAILED_SOCK_READ, 1, params);
    
    delete oldProtInterpreter;
    
    return;
  }

#ifdef PRINT_NETWORK_MESSAGES
  castor::vdqm::DevTools::printMessage(std::cout, false, false,
    ptr_serverSocket->socket(), &header);
#endif
  
  /**
   * Initialization of the OldRequestFacade, which 
   * provides the essential functions
   */
  OldRequestFacade oldRequestFacade(&volumeRequest,
                              &driveRequest,
                              &header);
  
  // The request handling phase
  try {
    // Handle old vdqm request type                                      
    castor::dlf::dlf_writep(*m_cuuid, DLF_LVL_DEBUG, VDQM_HANDLE_OLD_VDQM_REQUEST_TYPE);
    
    oldRequestFacade.checkRequestType(*m_cuuid);
    reqHandled = oldRequestFacade.handleRequestType(oldProtInterpreter, *m_cuuid);

  } catch (castor::exception::Exception e) { 
    castor::dlf::Param params2[] =
      {castor::dlf::Param("Message", e.getMessage().str()),
       castor::dlf::Param("errorCode", e.code())};   

    // "Exception caught" message 
    if (e.code() == EVQREQASS) {
      /**
       * This error code is used, when tpread/tpdump wants to delete 
       * a tape request, which is still assigned to a tape drive or vice versa.
       * Anyway, it is not a big deal, so we just log a warning message.
       */
      castor::dlf::dlf_writep(*m_cuuid, DLF_LVL_WARNING, VDQM_EXCEPTION, 2, params2);
    }
    else
      castor::dlf::dlf_writep(*m_cuuid, DLF_LVL_ERROR, VDQM_EXCEPTION, 2, params2);
    
    
    // "VdqmServer::handleOldVdqmRequest(): Rollback of the whole request" message   
    castor::dlf::dlf_writep(*m_cuuid, DLF_LVL_SYSTEM, VDQM_HANDLE_OLD_VDQM_REQUEST_ROLLBACK);
    svcs()->rollback(&ad); 
    
    /**
     * Tell the client about the error
     */
    try {
      if (reqtype == VDQM_PING) {
        //This is Protocol specific, because normally it receives the positive
        // queue position number back.
        oldProtInterpreter->sendAcknPing(-e.code());
      }
      else {
        oldProtInterpreter->sendAcknRollback(e.code());
      }
    } catch (castor::exception::Exception e) {  
      // "Exception caught" message
      castor::dlf::Param params[] =
        {castor::dlf::Param("Message", e.getMessage().str())};
      castor::dlf::dlf_writep(*m_cuuid, DLF_LVL_ERROR, VDQM_EXCEPTION, 1, params);
    }
    
    delete oldProtInterpreter;
    
    // Now, we don't need to make a handshake
    return;
  }
  
  
  /**
   *  The Handshake phase !!!
   */
  try {
    /**
     * Ping requests don't need a handshake! It is already handled in 
     * OldRequestFacade.
     */
    if (reqHandled && reqtype != VDQM_PING) {
      //Sending reply to client
      castor::dlf::dlf_writep(*m_cuuid, DLF_LVL_DEBUG, VDQM_SEND_REPLY_TO_CLIENT);
      
      oldProtInterpreter->sendAcknCommit();
      oldProtInterpreter->sendToOldClient(&header, &volumeRequest, &driveRequest);
      
      //"VdqmServer::handleOldVdqmRequest(): waiting for client acknowledge" message
      castor::dlf::dlf_writep(*m_cuuid, DLF_LVL_DEBUG, VDQM_HANDLE_OLD_VDQM_REQUEST_WAITING_FOR_CLIENT_ACK);
      rc = oldProtInterpreter->recvAcknFromOldClient();

      /**
       * Now we can commit everything for the database... or not
       */
      if ( rc  == VDQM_COMMIT) {
        svcs()->commit(&ad);

        // Notify the RTCP job submitter threads of possible work to be done
        castor::server::NotifierThread::getInstance()->doNotify('J');
        
        // "Request stored in DB" message
        castor::dlf::dlf_writep(*m_cuuid, DLF_LVL_DEBUG, VDQM_REQUEST_STORED_IN_DB);        
      }
      else {
        // "Client didn't send a VDQM_COMMIT => Rollback of request in db"
        svcs()->rollback(&ad);
          
        castor::dlf::dlf_writep(*m_cuuid, DLF_LVL_SYSTEM, VDQM_NO_VDQM_COMMIT_FROM_CLIENT);        
      }
    }
  } catch (castor::exception::Exception e) { 
    // "Exception caught" message
    castor::dlf::Param params[] =
      {castor::dlf::Param("Message", e.getMessage().str())};
    castor::dlf::dlf_writep(*m_cuuid, DLF_LVL_ERROR, VDQM_EXCEPTION, 1, params);
    
    // "VdqmServer::handleOldVdqmRequest(): Rollback of the whole request" message   
    castor::dlf::dlf_writep(*m_cuuid, DLF_LVL_SYSTEM, VDQM_HANDLE_OLD_VDQM_REQUEST_ROLLBACK);        
     svcs()->rollback(&ad);
  }
  
  delete oldProtInterpreter;
}
