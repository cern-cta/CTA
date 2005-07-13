/******************************************************************************
 *                      IVdqmSvc.hpp
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
 * @(#)RCSfile: IVdqmSvc.hpp  Revision: 1.0  Release Date: Apr 20, 2005  Author: mbraeger 
 *
 * This class provides methods to deal with the VDQM service
 *
 * @author Matthias Braeger
 *****************************************************************************/

#ifndef VDQM_IVDQMSVC_HPP
#define VDQM_IVDQMSVC_HPP 1

// Include Files
#include <string>

#include "castor/stager/ICommonSvc.hpp"
#include "castor/exception/Exception.hpp"

typedef struct vdqmdDrvReq vdqmDrvReq_t;

namespace castor {

  namespace vdqm {

    // Forward declaration
    class ExtendedDeviceGroup;
		class TapeRequest;
		class TapeDrive;
		class TapeServer;


    /**
     * This class provides methods to deal with the VDQM service
     */
    class IVdqmSvc : public virtual castor::stager::ICommonSvc {

    	public:
    	
	    	/**
	    	 * Checks, if there is an entry in the ExtendedDeviceGroup table,
	    	 * which has exactly these parameters
	    	 * @return true, if the entry exists
	    	 * @exception in case of error
	    	 */
	    	virtual bool checkExtDevGroup(const ExtendedDeviceGroup *extDevGrp)
	    		throw (castor::exception::Exception) = 0;
	    		
		    /**
	       * Retrieves a TapeServer from the database based on its serverName. 
	       * If no tapeServer is found, creates one.
	       * Note that this method creates a lock on the row of the
	       * given tapeServer and does not release it. It is the
	       * responsability of the caller to commit the transaction.
	       * The caller is also responsible for the deletion of the
	       * allocated object
	       * @param serverName The name of the server
	       * @return the tapeServer. the return value can never be 0
	       * @exception Exception in case of error (no tape server found,
	       * several tape servers found, DB problem, etc...)
	       */
	      virtual TapeServer* selectTapeServer(const std::string serverName)
	        throw (castor::exception::Exception) = 0;
	    		
	    		
	    	/**
	    	 * Checks, if there is already an entry for that tapeRequest. The entry
	    	 * must have exactly the same ID.
	    	 * 
	    	 * @return 0 : The row number, 
	    	 *         0 : The request is handled at the moment from a TapeDrive, 
	    	 *         -1: if there is no entry for it.
	    	 * @exception in case of error
	    	 */
	    	virtual int checkTapeRequest(const TapeRequest *tapeRequest)
	    		throw (castor::exception::Exception) = 0;
	    	
	    	
	    	/**
	    	 * Looks for the best fitting tape drive. If it is for example an
	    	 * older tape, it will first look if an older drive is free, before
	    	 * it chooses a newer one. This strategy should avoid, that the newer
	    	 * drive, which are able to deal with several tape models, are blocked
	    	 * if an request for a newer tape model arrives.
	    	 * Please notice that caller is responsible for deleting the object.
	    	 * @parameter the requested Extended Device Group for the tape
	    	 * @return the free TapeDrive or NULL if there is none.
	    	 * @exception in case of error
	    	 */	
	    	virtual TapeDrive* selectFreeTapeDrive(const ExtendedDeviceGroup *extDevGrp) 
	    		throw (castor::exception::Exception) = 0;
	 
	    		
//------------------ functions for TapeDriveRequestHandler ------------------

	      /**
	       * Retrieves a tapeDrive from the database based on its struct 
	       * representation. If no tapedrive is found, a Null pointer will 
	       * be returned.
	       * Note that this method creates a lock on the row of the
	       * given tapedrive and does not release it. It is the
	       * responsability of the caller to commit the transaction.
	       * The caller is also responsible for the deletion of the
	       * allocated object
	       * @param driveRequest The old struct, which represents the tapeDrive
	       * @exception Exception in case of error (several tapes drive found, 
	       * DB problem, etc...)
	       */
				virtual TapeDrive* selectTapeDrive(
					const vdqmDrvReq_t* driveRequest,
					const TapeServer* tapeServer)
	    		throw (castor::exception::Exception) = 0;
	    		
//---------------- functions for TapeDriveStatusHandler ------------------------

				/**
				 * Check whether another request is currently
				 * using the specified tape vid. Note that the tape can still
				 * be mounted on a drive if not in use by another request.
				 * The volume is also considered to be in use if it is mounted
				 * on a tape drive which has UNKNOWN or ERROR status.
				 * 
				 * @param volid the vid of the Tape
				 * @exception Exception in case of error (several tapes drive found, 
	       * DB problem, etc...)
	       * @return true if there is one       
				 */
				virtual bool existTapeDriveWithTapeInUse(
					const std::string volid)
	    		throw (castor::exception::Exception) = 0;
	    		
				/**
				 * Check whether the tape, specified by the vid is mounted
				 * on a tape drive or not.
				 * 
				 * @param volid the vid of the Tape
				 * @exception Exception in case of error (several tapes drive found, 
	       * DB problem, etc...)
	       * @return true if there is one        
				 */	    		
				virtual bool existTapeDriveWithTapeMounted(
					const std::string volid)
	    		throw (castor::exception::Exception) = 0;
	    	
	    	/**
	    	 * Returns the tape with this vid
	    	 * 
				 * @param vid the vid of the Tape
				 * @exception Exception in case of error (several tapes or no tape found, 
	       * DB problem, etc...)	
	       * @return The found TapeDrive	           
	    	 */	
	    	virtual castor::stager::Tape* selectTapeByVid(
					const std::string vid)
	    		throw (castor::exception::Exception) = 0;
	    		
	    		
	    	/**
	    	 * Looks through the tape requests, whether there is one for the
	    	 * mounted tape on the tapeDrive.
	    	 * 
				 * @param tapeDrive the tape drive, with the mounted tape
				 * @exception Exception in case of error (DB problem, no mounted Tape, 
				 * etc...)	
	       * @return The found tape request	           
	    	 */	
	    	virtual TapeRequest* selectTapeReqForMountedTape(
					const TapeDrive* tapeDrive)
	    		throw (castor::exception::Exception) = 0;	
				
	    		
    }; // end of class IVdqmSvc

  } // end of namespace vdqm

} // end of namespace castor

#endif // VDQM_IVDQMSVC_HPP
