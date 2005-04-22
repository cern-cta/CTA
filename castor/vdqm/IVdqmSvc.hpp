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
#include "castor/IService.hpp"
#include "castor/exception/Exception.hpp"


namespace castor {

  // Forward declaration
/*  class IObject;
  class IClient;
  class IAddress;*/

  namespace vdqm {

    // Forward declaration
    class ExtendedDeviceGroup;
		class TapeRequest;

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
	    	virtual bool checkDgn(const ExtendedDeviceGroup *extDevGrp)
	    		throw (castor::exception::Exception) = 0;
	    		
	    	/**
	    	 * Verifies, that the request doesn't (yet) exist.
	    	 * The requested Extended device group can be different.
	    	 * @return true, if there is already an entry for the request 
	    	 * @exception in case of error
	    	 */
	    	virtual bool checkTapeRequest(const TapeRequest *tapeRequest)
	    		throw (castor::exception::Exception) = 0;

    }; // end of class IVdqmSvc

  } // end of namespace vdqm

} // end of namespace castor

#endif //_IVDQMSVC_HPP_
