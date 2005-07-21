/******************************************************************************
 *                      OldVdqmProtocol.cpp
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
 * @(#)RCSfile: OldVdqmProtocol.cpp  Revision: 1.0  Release Date: Apr 18, 2005  Author: mbraeger 
 *
 *
 *
 * @author Matthias Braeger
 *****************************************************************************/

#include <string>
//#include <Cthread_api.h>

#include "castor/Constants.hpp"

#include "castor/exception/Internal.hpp"
#include "castor/exception/NotSupported.hpp"

#include "castor/vdqm/handler/TapeRequestHandler.hpp"
#include "castor/vdqm/handler/TapeDriveHandler.hpp"

#include <net.h>
#include <vdqm_constants.h>
#include <vdqm.h>
 
// Local Includes
#include "OldVdqmProtocol.hpp"
#include "VdqmServerSocket.hpp"

// To make the code more readable
using namespace castor::vdqm::handler;
 
//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
castor::vdqm::OldVdqmProtocol::OldVdqmProtocol(vdqmVolReq_t *volumeRequest,
																								vdqmDrvReq_t *driveRequest,
																						  	vdqmHdr_t *header) {
	
	ptr_volumeRequest = volumeRequest;
	ptr_driveRequest = driveRequest;
	ptr_header = header;
	
	// The Request Type
	m_reqtype = header->reqtype;
}


//------------------------------------------------------------------------------
// checkRequestType
//------------------------------------------------------------------------------
bool castor::vdqm::OldVdqmProtocol::checkRequestType(Cuuid_t cuuid) 
	throw (castor::exception::Exception) {
	
	int i;
	int req_values[] = VDQM_REQ_VALUES;
	std::string req_strings[] = VDQM_REQ_STRINGS;
	std::string req_string;
	
	void *hold_lock = NULL;
	
	i=0;
	//Check, if it's a right request type
  while (req_values[i] != -1 && req_values[i] != m_reqtype) i++;
  
  //reqtype as string representation 
  req_string = req_strings[i];
	if ((m_reqtype != VDQM_GET_VOLQUEUE) &&
			(m_reqtype != VDQM_GET_DRVQUEUE) &&
			(m_reqtype != VDQM_PING)) {

		castor::dlf::Param params[] =
      {castor::dlf::Param("req_string", req_string)};
		castor::dlf::dlf_writep(cuuid, DLF_LVL_USAGE, 16, 1, params);
	}
  
  if ( !VDQM_VALID_REQTYPE(m_reqtype) ) {
		castor::exception::NotSupported ex;
    ex.getMessage() << "Invalid Request 0x"
                    << std::hex << m_reqtype << "\n";
    throw ex;
  } 
  else {
//    rc = 0;
//    if ( Cthread_mutex_lock_ext(hold_lock)==-1 ) {
//    		
//    	
//        log(LOG_ERR,"vdqm_ProcReq() Cthread_mutex_lock_ext(): %s\n",
//            sstrerror(serrno));
//        return((void *)&return_status);
//    }
//    if ( reqtype == VDQM_SHUTDOWN ) {
//    	  // shutdown server requested
//				castor::dlf::dlf_writep(cuuid, DLF_LVL_USAGE, 3);
//        hold = vdqm_shutdown = 1;
//        vdqm_PipeHangup();
//        rc = 1;
//    }
//    if ( reqtype == VDQM_HOLD ) {
//        if ( !hold ) log(LOG_INFO,"vdqm_ProcReq(): set server HOLD status\n");
//        hold = 1;
//        rc = 1;
//    }
//    if ( reqtype == VDQM_RELEASE ) {
//        if ( hold ) log(LOG_INFO,"vdqm_ProcReq(): set server RELEASE status\n");
//        hold = 0;
//        rc = 1;
//    }
//    if ( hold && reqtype != VDQM_REPLICA ) {
//        log(LOG_INFO,"vdqm_ProcReq(): server in HOLD status\n");
//        vdqm_SetError(EVQHOLD);
//        rc = -1;
//        
//        castor::exception::Internal ex;
//		    ex.getMessage() << "Request processing error"
//    		                << hex << reqtype << "\n";
//    }
//    if ( Cthread_mutex_unlock_ext(hold_lock)==-1 ) {
//        log(LOG_ERR,"vdqm_ProcReq() Cthread_mutex_unlock_ext(): %s\n",
//            sstrerror(serrno));
//        return((void *)&return_status);
//    }
//
//	  vdqm_ReqStarted();
  } // end else
  
  return true;
}

bool castor::vdqm::OldVdqmProtocol::handleRequestType(VdqmServerSocket* sock,
																											Cuuid_t cuuid) 
	throw (castor::exception::Exception) {
	
	bool handleRequest = true;
	

	switch (m_reqtype) {
    case VDQM_VOL_REQ:
    		if ( ptr_header == NULL || ptr_volumeRequest == NULL )
    			handleRequest = false;
    		else {
	    		// Handle VDQM_VOL_REQ
	    		castor::dlf::dlf_writep(cuuid, DLF_LVL_USAGE, 19);
					
  				TapeRequestHandler requestHandler;
					requestHandler.newTapeRequest(ptr_header, ptr_volumeRequest, cuuid); 
    		}
        break;
    case VDQM_DRV_REQ:
    		if ( ptr_header == NULL || ptr_driveRequest == NULL )
    			handleRequest = false;
    		else {
	    		// Handle VDQM_DRV_REQ
	    		castor::dlf::dlf_writep(cuuid, DLF_LVL_USAGE, 20);
					TapeDriveHandler tapeDriveHandler(ptr_header, ptr_driveRequest, cuuid);
					tapeDriveHandler.newTapeDriveRequest();
    		}
        break;
    case VDQM_DEL_VOLREQ:
				if ( ptr_volumeRequest == NULL )
    			handleRequest = false;
    		else {				
					// Handle VDQM_DEL_VOLREQ
			    castor::dlf::Param params[] =
			      {castor::dlf::Param("volreq ID", ptr_volumeRequest->VolReqID)};
	    		castor::dlf::dlf_writep(cuuid, DLF_LVL_USAGE, 21);
					
					
    			TapeRequestHandler requestHandler;
					requestHandler.deleteTapeRequest(ptr_volumeRequest, cuuid); 
    		}
        break;
    case VDQM_DEL_DRVREQ:
    		if ( ptr_driveRequest == NULL )
    			handleRequest = false;
    		else {
	    		// Handle VDQM_DEL_DRVREQ
	    		castor::dlf::Param params[] =
			      {castor::dlf::Param("tape drive", ptr_driveRequest->drive),
			       castor::dlf::Param("tape server", ptr_driveRequest->server)};
	    		castor::dlf::dlf_writep(cuuid, DLF_LVL_USAGE, 22, 2, params);
	    		
					TapeDriveHandler tapeDriveHandler(ptr_header, ptr_driveRequest, cuuid);
					tapeDriveHandler.deleteTapeDrive();
    		}
        break;
    case VDQM_PING:
    		// Handle VDQM_PING
    		castor::dlf::dlf_writep(cuuid, DLF_LVL_USAGE, 25);
    		
    		{
    			int queuePosition = -1;
    			TapeRequestHandler requestHandler;
					queuePosition = requestHandler.getQueuePosition(ptr_volumeRequest, cuuid); 
					
					// Send VDQM_PING back to client
				  castor::dlf::Param params[] =
				  	{castor::dlf::Param("Queue position", queuePosition)};
					castor::dlf::dlf_writep(cuuid, DLF_LVL_USAGE, 27, 1, params);
					sock->sendAcknPing(queuePosition);
    		}
    		break;
  	default:
  			if ( VDQM_VALID_REQTYPE(m_reqtype) ) 
  				handleRequest = false;
  			else {
	  			castor::exception::NotSupported ex;
			    ex.getMessage() << "Invalid Request 0x"
	                    << std::hex << m_reqtype << "\n";
	    		throw ex;
  			}
	}
	
	
 return handleRequest;

//
//  /*
//   * Commit failed somewhere. If it was a volume request we must
//   * remove it since client is either dead or unreachable.
//   */
//  if ( rc == -1 ) {
//     switch (reqtype) {
//	     case VDQM_VOL_REQ:
//	          log(LOG_INFO,"vdqm_ProcReq() remove volume request from queue\n");
//	          (void)vdqm_DelVolReq(&volumeRequest); 
//	          break;
//	     case VDQM_DRV_REQ:
//	          log(LOG_INFO,"vdqm_ProcReq() rollback drive request\n");
//	          (void)vdqm_QueueOpRollback(NULL,&driveRequest);
//	          break;
//	     default:
//	          break;
//     }
//  }
}
