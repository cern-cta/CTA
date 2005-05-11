/******************************************************************************
 *                      AbstractRequestHandler.hpp
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
 * @(#)RCSfile: AbstractRequestHandler.hpp  Revision: 1.0  Release Date: Apr 20, 2005  Author: mbraeger 
 *
 *
 *
 * @author Matthias Braeger
 *****************************************************************************/

#ifndef _ABSTRACTREQUESTHANDLER_HPP_
#define _ABSTRACTREQUESTHANDLER_HPP_

#include "castor/BaseObject.hpp"
#include "castor/exception/Exception.hpp"

namespace castor {
	//Forward declaration
	class IObject;
	
  namespace vdqm {

    /**
     * The AbstractRequestHandler provides a set of useful functions for
     * concrete request instances.
     */
    class AbstractRequestHandler : public BaseObject {

    	public:
	      
	      /**
	       * Constructor
	       */
	      AbstractRequestHandler();
	      
    		
				/**
				 * Returns the next request from the database
				 */
				virtual castor::IObject *getRequest() 
						throw (castor::exception::Exception) = 0;
						
			protected:
			
				/**
	       * Stores the IObject into the data base.
	       * @param fr the request
	       * @param cuuid its uuid (for logging purposes only)
	       */
				void handleRequest(castor::IObject* fr, Cuuid_t cuuid)
  				throw (castor::exception::Exception);
     

    }; // class AbstractRequestHandler

  } // end of namespace vdqm

} // end of namespace castor

#endif //_ABSTRACTREQUESTHANDLER_HPP_
