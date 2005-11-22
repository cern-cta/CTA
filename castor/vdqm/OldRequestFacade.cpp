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

#include <string>

#include "castor/Constants.hpp"

#include "castor/exception/Internal.hpp"
#include "castor/exception/NotSupported.hpp"

#include "castor/vdqm/handler/TapeRequestHandler.hpp"
#include "castor/vdqm/handler/TapeDriveHandler.hpp"

#include <net.h>
#include <vdqm_constants.h>
 
// Local Includes
#include "newVdqm.h"
#include "OldRequestFacade.hpp"
#include "OldProtocolInterpreter.hpp"

// To make the code more readable
using namespace castor::vdqm::handler;
 
//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
castor::vdqm::OldRequestFacade::OldRequestFacade(newVdqmVolReq_t *volumeRequest,
																								newVdqmDrvReq_t *driveRequest,
																						  	newVdqmHdr_t *header) {
	
	ptr_volumeRequest = volumeRequest;
	ptr_driveRequest = driveRequest;
	ptr_header = header;
	
	// The Request Type
	m_reqtype = header->reqtype;
}


//------------------------------------------------------------------------------
// checkRequestType
//------------------------------------------------------------------------------
bool castor::vdqm::OldRequestFacade::checkRequestType(Cuuid_t cuuid) 
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
		castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM, 16, 1, params);
	}
  
  if ( !VDQM_VALID_REQTYPE(m_reqtype) ) {
		castor::exception::NotSupported ex;
    ex.getMessage() << "Invalid Request 0x"
                    << std::hex << m_reqtype << "\n";
    throw ex;
  } 
  
  return true;
}

bool castor::vdqm::OldRequestFacade::handleRequestType(
	OldProtocolInterpreter* oldProtInterpreter,
	Cuuid_t cuuid) 
	throw (castor::exception::Exception) {
	
	bool handleRequest = true;
	

	switch (m_reqtype) {
    case VDQM_VOL_REQ:
    		if ( ptr_header == NULL || ptr_volumeRequest == NULL )
    			handleRequest = false;
    		else {
	    		// Handle VDQM_VOL_REQ
	    		castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM, 19);
					
  				TapeRequestHandler requestHandler;
					requestHandler.newTapeRequest(ptr_header, ptr_volumeRequest, cuuid); 
    		}
        break;
    case VDQM_DRV_REQ:
    		if ( ptr_header == NULL || ptr_driveRequest == NULL )
    			handleRequest = false;
    		else {
	    		// Handle VDQM_DRV_REQ
	    		castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM, 20);
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
	    		castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM, 21);
					
					
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
	    		castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM, 22, 2, params);
	    		
					TapeDriveHandler tapeDriveHandler(ptr_header, ptr_driveRequest, cuuid);
					tapeDriveHandler.deleteTapeDrive();
    		}
        break;
    case VDQM_GET_VOLQUEUE:
    		// Handle VDQM_GET_VOLQUEUE
    		castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM, 54);
				{	
					TapeRequestHandler requestHandler;
					// Sends the tape request queue back to the client
					requestHandler.sendTapeRequestQueue(
					                  ptr_header,
					                  ptr_volumeRequest, 
					                  ptr_driveRequest, 
					                  oldProtInterpreter, 
					                  cuuid);
    		}
    		break;
    case VDQM_GET_DRVQUEUE:
    		// Handle VDQM_GET_DRVQUEUE
    		castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM, 55);
				{					
					TapeDriveHandler tapeDriveHandler(ptr_header, ptr_driveRequest, cuuid);
					tapeDriveHandler.sendTapeDriveQueue(
					                  ptr_volumeRequest, 
					                  oldProtInterpreter);
    		}
    		break;
    case VDQM_PING:
    		// Handle VDQM_PING
    		castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM, 25);
    		
    		{
    			int queuePosition = -1;
    			TapeRequestHandler requestHandler;
					queuePosition = requestHandler.getQueuePosition(ptr_volumeRequest, cuuid); 
					
					// Send VDQM_PING back to client
				  castor::dlf::Param params[] =
				  	{castor::dlf::Param("Queue position", queuePosition)};
					castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM, 27, 1, params);
					oldProtInterpreter->sendAcknPing(queuePosition);
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
}
