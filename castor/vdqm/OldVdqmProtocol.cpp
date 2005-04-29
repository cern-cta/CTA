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
#include <Cthread_api.h>

#include "castor/Constants.hpp"

#include "castor/exception/Exception.hpp"
#include "castor/exception/Internal.hpp"
#include "castor/exception/NotSupported.hpp"

#include "h/vdqm_constants.h"
#include "h/vdqm.h"
 
// Local Includes
#include "OldVdqmProtocol.hpp"
 
//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
castor::vdqm::OldVdqmProtocol::OldVdqmProtocol() {
	initLog("OldVdqmProtocolLog", SVC_DLFMSG);
  // Initializes the DLF logging. This includes
  // defining the predefined messages
  castor::dlf::Message messages[] =
    {{ 0, " - "},
     { 1, "New VDQM request"},
     { 2, "Request processing error"},
     { 3, "shutdown server requested"},
     {-1, ""}};
  castor::dlf::dlf_init("OldVdqmProtocolLog", messages);
}


//------------------------------------------------------------------------------
// checkRequestType
//------------------------------------------------------------------------------
bool castor::vdqm::OldVdqmProtocol::checkRequestType(int reqtype) 
	throw (castor::exception::Exception) {
	
	int i;
	int req_values[] = VDQM_REQ_VALUES;
	std::string req_string;
	
	void *hold_lock = NULL;
	
	i=0;
	//Check, if it's a right request type
  while (req_values[i] != -1 && req_values[i] != reqtype) i++;
  
  //reqtype as string representation 
  req_string = req_strings[i];
	if ((reqtype != VDQM_GET_VOLQUEUE) &&
			(reqtype != VDQM_GET_DRVQUEUE) &&
			(reqtype != VDQM_PING)) {

		castor::dlf::Param params[] =
      {castor::dlf::Param("req_string", req_string)};
		castor::dlf::dlf_writep(cuuid, DLF_LVL_USAGE, 1, 1, params);
	}
  
  if ( !VDQM_VALID_REQTYPE(reqtype) ) {
		castor::exception::NotSupported ex;
    ex.getMessage() << "Invalid Request 0x"
                    << hex << reqtype << "\n";
    throw ex;
  } 
  else {
    rc = 0;
    if ( Cthread_mutex_lock_ext(hold_lock)==-1 ) {
    		
    	
        log(LOG_ERR,"vdqm_ProcReq() Cthread_mutex_lock_ext(): %s\n",
            sstrerror(serrno));
        return((void *)&return_status);
    }
    if ( reqtype == VDQM_SHUTDOWN ) {
    	  // shutdown server requested
				castor::dlf::dlf_writep(cuuid, DLF_LVL_USAGE, 3);
        hold = vdqm_shutdown = 1;
        vdqm_PipeHangup();
        rc = 1;
    }
    if ( reqtype == VDQM_HOLD ) {
        if ( !hold ) log(LOG_INFO,"vdqm_ProcReq(): set server HOLD status\n");
        hold = 1;
        rc = 1;
    }
    if ( reqtype == VDQM_RELEASE ) {
        if ( hold ) log(LOG_INFO,"vdqm_ProcReq(): set server RELEASE status\n");
        hold = 0;
        rc = 1;
    }
    if ( hold && reqtype != VDQM_REPLICA ) {
        log(LOG_INFO,"vdqm_ProcReq(): server in HOLD status\n");
        vdqm_SetError(EVQHOLD);
        rc = -1;
        
        castor::exception::Internal ex;
		    ex.getMessage() << "Request processing error"
    		                << hex << reqtype << "\n";
    }
    if ( Cthread_mutex_unlock_ext(hold_lock)==-1 ) {
        log(LOG_ERR,"vdqm_ProcReq() Cthread_mutex_unlock_ext(): %s\n",
            sstrerror(serrno));
        return((void *)&return_status);
    }

	  vdqm_ReqStarted();
  } // end else
}

void castor::vdqm::OldVdqmProtocol::handleRequestType(int reqtype) 
	throw (castor::exception::Exception) {
	switch (reqtype) {
    case VDQM_VOL_REQ:
        rc = vdqm_NewVolReq(&hdr,&volumeRequest);
        break;
    case VDQM_DRV_REQ:
        rc = vdqm_NewDrvReq(&hdr,&driveRequest);
        break;
    case VDQM_DEL_VOLREQ:
        rc = vdqm_DelVolReq(&volumeRequest);
        break;
    case VDQM_DEL_DRVREQ:
        rc = vdqm_DelDrvReq(&driveRequest);
        /* Dumping the list of drives to file */
        if (rc == 0) {
            log(LOG_DEBUG, "vdqm_ProcReq - NOW SAVING THE REDUCED LIST OF DRIVES TO FILE\n");
            if (vdqm_save_queue() < 0) {
                log(LOG_ERR, "Could not save drive list\n");
            }
        }
        break;
	}
	
  if ( rc == -1 ) {
    log(LOG_ERR,"vdqm_ProcReq(): request processing error\n");
    if ( reqtype == VDQM_PING ) 
        (void) vdqm_AcknPing(client_connection,rc);
    else (void)vdqm_AcknRollback(client_connection);
    rc = 0;
	} 
	else {
    rc = vdqm_AcknCommit(client_connection);
    if ( rc != -1 ) {
      rc = vdqm_SendReq(client_connection,
          &hdr,&volumeRequest,&driveRequest);
      if ( rc != -1 ) rc = vdqm_RecvAckn(client_connection);
    }
    if ( rc == -1 )
      log(LOG_ERR,"vdqm_ProcReq(): client error on commit\n");
	}

  /*
   * Commit failed somewhere. If it was a volume request we must
   * remove it since client is either dead or unreachable.
   */
  if ( rc == -1 ) {
     switch (reqtype) {
	     case VDQM_VOL_REQ:
	          log(LOG_INFO,"vdqm_ProcReq() remove volume request from queue\n");
	          (void)vdqm_DelVolReq(&volumeRequest); 
	          break;
	     case VDQM_DRV_REQ:
	          log(LOG_INFO,"vdqm_ProcReq() rollback drive request\n");
	          (void)vdqm_QueueOpRollback(NULL,&driveRequest);
	          break;
	     default:
	          break;
     }
  }
}