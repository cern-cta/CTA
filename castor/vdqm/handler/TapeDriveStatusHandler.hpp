/******************************************************************************
 *                      TapeDriveStatusHandler.hpp
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
 * @(#)RCSfile: TapeDriveStatusHandler.hpp  Revision: 1.0  Release Date: Jul 6, 2005  Author: mbraeger 
 *
 *
 *
 * @author Matthias Braeger
 *****************************************************************************/

#ifndef _TAPEDRIVESTATUSHANDLER_HPP_
#define _TAPEDRIVESTATUSHANDLER_HPP_

#include "castor/exception/Exception.hpp"
#include "BaseRequestHandler.hpp"

typedef struct vdqmdDrvReq vdqmDrvReq_t;

namespace castor {
	//Forward declaration

  namespace vdqm {

		//Forward declaration
		class TapeDrive;

		namespace handler {
	    /**
	     * The TapeDriveStatusHandler is only used by the TapeDriveHandler
	     * class. It handles the status, which has been send from the tdaemon
	     * to keep the db up to date.
		   * Note that this class is not used outside of this namespace!
	     */
	    class TapeDriveStatusHandler : public BaseRequestHandler {
				
				/**
				 * Like a real friend, TapeDriveHandler will respect the privacy 
				 * wishes of this class ;-)
				 */
				friend class TapeDriveHandler;
	
				public:
				
				  /**
				   * Entry point for handling the status, which has been send from
				   * the tpdaemon to vdqm.
				   * 
				   * @exception In case of error
				   */
					void handleOldStatus() throw (castor::exception::Exception);
	
	
				protected:
	
		      /**
		       * Constructor
		       * 
		       * @param tapeDrive The tape Drive, which needs a consistency check
					 * @param driveRequest The TapeDriveRequest from the old protocol
					 * @param cuuid The unique id of the request. Needed for dlf
					 * @exception In case of error
		       */
					TapeDriveStatusHandler(TapeDrive* tapeDrive, 
													 vdqmDrvReq_t* driveRequest, Cuuid_t cuuid) throw();
					
		      /**
		       * Destructor
		       */
					virtual ~TapeDriveStatusHandler() throw();
					
						
				private:
					// Private variables
					TapeDrive* ptr_tapeDrive;
					vdqmDrvReq_t* ptr_driveRequest;
					Cuuid_t m_cuuid;
					
					
				  /**
				   * This function is onnly used internally from handleOldStatus() to 
				   * handle the case that a VDQM_VOL_MOUNT request is beeing sent from
				   * the client.
				   * 
				   * @exception In case of error
				   */
					void handleVolMountStatus() throw (castor::exception::Exception);
												
	    }; // class TapeDriveStatusHandler
    
  	} // end of namespace handler

  } // end of namespace vdqm

} // end of namespace castor

#endif //_TAPEDRIVESTATUSHANDLER_HPP_
