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

// Forward declaration
typedef struct newVdqmDrvReq newVdqmDrvReq_t;

namespace castor {

  namespace vdqm {

    // Forward declaration
    class DeviceGroupName;
    class TapeAccessSpecification;
		class TapeRequest;
		class TapeDrive;
		class TapeServer;
		class TapeDriveCompatibility;

    /**
     * This class provides methods to deal with the VDQM service
     */
    class IVdqmSvc : public virtual castor::stager::ICommonSvc {

    	public:
    		    		
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
	      virtual TapeServer* selectTapeServer(const std::string serverName, bool withTapeDrive)
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
	    	 * if an request for a newer tape model arrives.<br>
	    	 * Please notice that caller is responsible for deleting the object.<br>
	    	 * This function is used by TapeRequestDedicationHandler.
	    	 * 
	    	 * @parameter freeTapeDrive An address poointer to a free tape which 
	    	 * is found in the db
	    	 * @parameter waitingTapeRequest An address pointer to a waiting 
	    	 * TapeRequest, which is the best choice for the selected tapeDrive
	    	 * @exception in case of error
	    	 */	
	     virtual void matchTape2TapeDrive(
	     	TapeDrive** freeTapeDrive, TapeRequest** waitingTapeRequest) 
    		throw (castor::exception::Exception) = 0;

	    		
	    	/**
	    	 * Looks, wether the specific tape access exist in the db. If not the
	    	 * return value is NULL.
	    	 * Please notice that caller is responsible for deleting the object.
	    	 * @parameter accessMode the access mode for the tape
	    	 * @parameter density its tape density
	    	 * @parameter tapeModel the model of the requested tape
	    	 * @return the reference in the db or NULL if it is not a right specification
	    	 * @exception in case of error
	    	 */	
	    	virtual TapeAccessSpecification* selectTapeAccessSpecification(
	    		const int accessMode,
	    		const std::string density,
	    		const std::string tapeModel) 
	    		throw (castor::exception::Exception) = 0;
	    		
	    		
	    	/**
	    	 * Looks, if the specified dgName exists in the database. 
	    	 * If it is the case, it will return the object. If not, a new entry
	    	 * will be created!
	    	 * Please notice that caller is responsible for deleting the object.
	    	 * @parameter dgName The dgn which the client has sent to vdqm
	    	 * @return the requested DeviceGroupName, or NULL if it does not exists
	    	 * @exception in case of error
	    	 */	
	    	virtual DeviceGroupName* selectDeviceGroupName(
	    		const std::string dgName) 
	    		throw (castor::exception::Exception) = 0;
	    		
	    		
	      /**
	       * Returns all the tapeRequests with their connected objects from 
	       * foreign tables with the specified dgn an server. If you don't want
	       * to specify one of the arguments, just give an empty string 
	       * instead.
	       * Please notice: The caller is responsible for the deletion of the
	       * allocated objects!
	       * @param driveRequest The old struct, which represents the tapeDrive
	       * @exception Exception in case of error (several tapes drive found, 
	       * DB problem, etc...)
	       * @return a list of files. 
         * Note that the returned vector should be deallocated
         * by the caller as well as its content
	       */
				virtual std::vector<TapeRequest*>* selectTapeRequestQueue(
					const std::string dgn, 
					const std::string requestedSrv)
	    		throw (castor::exception::Exception) = 0;	 
	    		
	    		
	      /**
	       * Returns all the tape drives with their connected objects from 
	       * foreign tables with the specified dgn an server. If you don't want
	       * to specify one of the arguments, just give an empty string 
	       * instead.
	       * Please notice: The caller is responsible for the deletion of the
	       * allocated objects!
	       * @param driveRequest The old struct, which represents the tapeDrive
	       * @exception Exception in case of error (several tapes drive found, 
	       * DB problem, etc...)
	       * @return a list of files. 
         * Note that the returned vector should be deallocated
         * by the caller as well as its content
	       */
				virtual std::vector<TapeDrive*>* selectTapeDriveQueue(
					const std::string dgn, 
					const std::string requestedSrv)
	    		throw (castor::exception::Exception) = 0;	  
		  		 	    			    			    	

				/**
				 * Manages the casting of VolReqID, to find also tape requests, which
				 * have an id bigger than 32 bit.
				 * 
				 * @param VolReqID The id, which has been sent by RTCPCopyD
				 * @return The tape request and its ClientIdentification, or NULL
				 * @exception in case of error
				 */
				virtual TapeRequest* selectTapeRequest(const int VolReqID) 
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
					const newVdqmDrvReq_t* driveRequest,
					TapeServer* tapeServer)
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
	    		
	    	
	    	/**
	    	 * Selects from the TapeDriveCompatibility table all entries for the
	    	 * specified drive model.
	    	 * 
	    	 * @param tapeDriveModel The model of the tape drive
	    	 * @exception Exception in case of error (DB problem, no mounted Tape, 
				 * etc...)	
				 * @return All entries in the table for the selected drive model
	    	 */
	    	virtual std::vector<TapeDriveCompatibility*>* 
	    		selectCompatibilitiesForDriveModel(const std::string tapeDriveModel)
	    		throw (castor::exception::Exception) = 0;
	    	
	    	/**
	    	 * Selects from the TapeAccessSpecification table all entries for the
	    	 * specified tape model.
	    	 * 
	    	 * @param tapeModel The model of the tape
	    	 * @exception Exception in case of error (DB problem, no mounted Tape, 
				 * etc...)	
				 * @return All entries in the table for the selected tape model. The 
				 * list is sorted by accessMode (first write, then read)
	    	 */	
	    	virtual std::vector<TapeAccessSpecification*>*
	    		selectTapeAccessSpecifications(const std::string tapeModel)
	    		throw (castor::exception::Exception) = 0;					    		
	    		
    }; // end of class IVdqmSvc

  } // end of namespace vdqm

} // end of namespace castor

#endif // VDQM_IVDQMSVC_HPP
