/******************************************************************************
 *                      TapeDriveHandler.hpp
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
 * @(#)RCSfile: TapeDriveHandler.hpp  Revision: 1.0  Release Date: Jun 22, 2005  Author: mbraeger 
 *
 *
 *
 * @author Matthias Braeger
 *****************************************************************************/

#ifndef _TAPEDRIVEHANDLER_HPP_
#define _TAPEDRIVEHANDLER_HPP_

#include "BaseRequestHandler.hpp"
#include "castor/vdqm/TapeDriveStatusCodes.hpp"

typedef struct newVdqmHdr newVdqmHdr_t;
typedef struct newVdqmVolReq newVdqmVolReq_t;
typedef struct newVdqmDrvReq newVdqmDrvReq_t;

namespace castor {

  namespace vdqm {

		//Forward declaration
		class TapeServer;
		class TapeDrive;
		class OldProtocolInterpreter;

		namespace handler {
	    /**
	     * The TapeDriveHandler provides functions to handle all vdqm related
	     * tape drive issues. It handles for example the VDQM_DRV_REQ
	     */
	    class TapeDriveHandler : public BaseRequestHandler {
	
				public:
	
		      /**
		       * Constructor
		       * 
		       * @param header The header of the old Protocol
					 * @param driveRequest The TapeDriveRequest from the old protocol
					 * @param cuuid The unique id of the request. Needed for dlf
					 * @exception In case of error
		       */
					TapeDriveHandler(newVdqmHdr_t* header, 
													 newVdqmDrvReq_t* driveRequest, Cuuid_t cuuid) 
													 throw(castor::exception::Exception);
					
		      /**
		       * Destructor
		       */
					virtual ~TapeDriveHandler() throw();
					
					/**
					 * This function replaces the old vdqm_NewDrvReq() C-function and is
					 * called, when a VDQM_DRV_REQ message comes from a client. It stores
					 * the request into the data Base or updates the status of existing
					 * TapeDrives in the db.
					 * 
					 * @exception In case of error
					 */
					void newTapeDriveRequest() throw (castor::exception::Exception);
					
					/**
					 * This function replaces the old vdqm_DelDrvReq() C-function and is
					 * called, when a VDQM_DEL_DRVREQ message comes from a client. It 
					 * deletes the TapeDrive with the specified ID in the db.
					 * 
					 * @exception In case of error
					 */
					void deleteTapeDrive() throw (castor::exception::Exception);
					
					/**
					 * This function replaces the old vdqm_GetDrvQueue() C-function. 
					 * I looks into the data base for all stored tapeDrives and sends
					 * them back to the client via the OldProtocolInterpreter interface.
					 *
					 * @param volumeRequest The TapeRequest in the old protocol
					 * @param oldProtInterpreter The interface to send the queue to the client
					 * @param cuuid The unique id of the request. Needed for dlf 
					 * @exception In case of error
					 */
					void sendTapeDriveQueue(newVdqmVolReq_t *volumeRequest,
																	OldProtocolInterpreter* oldProtInterpreter) 
						throw (castor::exception::Exception);	
						
						
				private:
					// Private variables
					newVdqmHdr_t* ptr_header;
					newVdqmDrvReq_t* ptr_driveRequest;
					Cuuid_t m_cuuid;
					
					/**
					 * Deletes all TapeDrives and their old TapeRequests (if any)
					 * in the db from the given TapeServer 
					 * 
					 * @param tapeServer The tape server, which drive should be deleted
					 * @exception In case of error
					 */
					void deleteAllTapeDrvsFromSrv(TapeServer* tapeServer) 
						throw (castor::exception::Exception);
	
					/**
					 * Handles the communication with the data base to get the TapeDrive.
					 * If there is no entry in the db, a new TapeDrive Object will be created.
					 * Please notice, that this object is not stored in the db. This happens
					 * at the very end of newTapeDriveRequest()
					 * 
					 * @param tapeServer The tape server, to which the drive belong to.
					 * @exception In case of error
					 */
					TapeDrive* getTapeDrive(TapeServer* tapeServer) 
						throw (castor::exception::Exception);
						
					/**
					 * Copies the informations of the tape drive, which it has received 
					 * from the db back to the old vdqmDrvReq_t struct, to inform the
					 * old RTCPD client.
					 * 
					 * @param tapeDrive The received TapeDrive frome the db
					 * @exception In case of error
					 */
					void copyTapeDriveInformations(const TapeDrive* tapeDrive)
						throw (castor::exception::Exception);	
					
					
					/**
					 * Creates a log messages for the old and new status code. If the 
					 * value of the old status is 0, the function just prints the new 
					 * status.
					 * 
					 * @param oldProtocolStatus The status value of the old Protocol
					 * @param newActStatus The current status of the tape drive in the db
					 * @exception In case of error
					 */	
					void printStatus(const int oldProtocolStatus, 
													const int newActStatus)
						throw (castor::exception::Exception);
						
						
					/**
					 * Deletes the the tape Server and all inner objects of the 
					 * tapeDrive. Please notice, that the tapeDrive itself is not
					 * deleted.
					 */
					void freeMemory(TapeDrive* tapeDrive, TapeServer* tapeServer);										
					
					
					/**
					 * Translates the new status of a Tape drive into the old status
					 * representation.
					 * 
					 * @param newStatusCode The status value of the new Protocol
					 * @return The translation into the old status
					 * @exception In case of error
					 */	
					int translateNewStatus(TapeDriveStatusCodes newStatusCode)
						throw (castor::exception::Exception);
						
	    }; // class TapeDriveHandler
    
  	} // end of namespace handler

  } // end of namespace vdqm

} // end of namespace castor

#endif //_TAPEDRIVEHANDLER_HPP_
