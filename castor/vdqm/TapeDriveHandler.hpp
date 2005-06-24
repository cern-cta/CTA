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
				 * Deletes all TapeDrives in the db from the given 
				 * TapeServer (ptr_driveRequest->reqhost) and their
				 * old TapeRequests (if any).
				 * 
				 * @exception In case of error
				 */
				void deleteAllTapeDrvsFromSrv() throw (castor::exception::Exception);

    }; // class TapeDriveHandler

  } // end of namespace vdqm

} // end of namespace castor

#endif //_TAPEDRIVEHANDLER_HPP_
