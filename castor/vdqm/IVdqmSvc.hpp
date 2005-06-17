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
 *
 *
 * @author Matthias Braeger
 *****************************************************************************/

#ifndef _IVDQMSVC_HPP_
#define _IVDQMSVC_HPP_

// Include Files
#include <string>

#include "castor/IService.hpp"
#include "castor/exception/Exception.hpp"



namespace castor {

  // Forward declaration
	class stager::Tape;

  namespace vdqm {

    // Forward declaration
    class ExtendedDeviceGroup;
		class TapeRequest;
		class TapeDrive;
		class TapeServer;


    /**
     * This class provides methods usefull to the stager to
     * deal with database queries
     */
    class IVdqmSvc : public virtual IService {

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
	    	 * Looks in the data base for the TapeServer, which has exactly 
	    	 * this name. If there is no entry in the db table with this parameters,
	    	 * the return value is NULL
	    	 * @parameter serverName The name of the tape server, which is beeing requested
	    	 * @return The object representation of the table entry or NULL
	    	 * @exception in case of error
	    	 */
	    	virtual TapeServer* selectTapeServer(const std::string serverName)
	    		throw (castor::exception::Exception) = 0;
	    		
	    		
	    	/**
	    	 * Checks, if there is already an entry for that tapeRequest. The entry
	    	 * must have exactly the same ID.
	    	 * 
	    	 * @return 0<: The row number, 
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
	    		
    }; // end of class IVdqmSvc

  } // end of namespace vdqm

} // end of namespace castor

#endif //_IVDQMSVC_HPP_
