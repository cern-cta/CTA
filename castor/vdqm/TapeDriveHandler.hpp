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

#include "castor/exception/Exception.hpp"
#include "BaseRequestHandler.hpp"

typedef struct vdqmHdr vdqmHdr_t;
typedef struct vdqmdDrvReq vdqmDrvReq_t;

namespace castor {
	//Forward declaration
	class IObject;

  namespace vdqm {

		//Forward declaration
		class TapeServer;
		class TapeDrive;

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
				TapeDriveHandler(vdqmHdr_t* header, 
												 vdqmDrvReq_t* driveRequest, Cuuid_t cuuid) throw();
				
	      /**
	       * Destructor
	       */
				virtual ~TapeDriveHandler() throw();
				
				/**
				 * This function replaces the old vdqm_NewDrvReq() C-function and is
				 * called, when a VDQM_DRV_REQ comes from a client. It stores
				 * the request into the data Base or updates the status of existing
				 * TapeDrives in the db.
				 * 
				 * @exception In case of error
				 */
				void newTapeDriveRequest() throw (castor::exception::Exception);
					
					
			private:
				// Private variables
				vdqmHdr_t* ptr_header;
				vdqmDrvReq_t* ptr_driveRequest;
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
				 * Does a logging for the old and new status code
				 * 
				 * @param oldProtocolStatus The status value of the old Protocol
				 * @param newActStatus The current status of the tape drive in the db
				 * @exception In case of error
				 */	
				void printStatus(const int oldProtocolStatus, 
												const int newActStatus)
					throw (castor::exception::Exception);
					
    }; // class TapeDriveHandler

  } // end of namespace vdqm

} // end of namespace castor

#endif //_TAPEDRIVEHANDLER_HPP_
