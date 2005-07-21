/******************************************************************************
 *                      BaseRequestHandler.hpp
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
 * @(#)RCSfile: BaseRequestHandler.hpp  Revision: 1.0  Release Date: Apr 20, 2005  Author: mbraeger 
 *
 *
 *
 * @author Matthias Braeger
 *****************************************************************************/

#ifndef _BASEREQUESTHANDLER_HPP_
#define _BASEREQUESTHANDLER_HPP_

#include "castor/BaseObject.hpp"
#include "castor/vdqm/IVdqmSvc.hpp"

namespace castor {
	//Forward declaration
	class IObject;
	class Services;
	
  namespace vdqm {
  	
		namespace handler {
	    /**
	     * The BaseRequestHandler provides a set of useful functions for
	     * concrete request instances.
	     */
	    class BaseRequestHandler : public castor::BaseObject {
	
	    	public:
		      
		      /**
		       * Constructor
		       */
		      BaseRequestHandler() throw(castor::exception::Exception);
		      
		      /**
		       * Destructor
		       */
					virtual ~BaseRequestHandler() throw();
							
							
				protected:
					//The IServices for vdqm
					castor::Services* ptr_svcs;
				  castor::vdqm::IVdqmSvc* ptr_IVdqmService;
				
					/**
		       * Stores the IObject into the data base. Please edit this function and 
		       * add here for your concrete Class instance your concrete Objects, 
		       * which you want to have created.
		       * @param fr the request
		       * @param cuuid its uuid (for logging purposes only)
		       */
					virtual void handleRequest(castor::IObject* fr, bool commit, Cuuid_t cuuid)
	  				throw (castor::exception::Exception);
	  				
	  				
					/**
		       * Deletes the IObject in the data base. Please edit this function and 
		       * add here for your concrete Class instance your concrete Objects, 
		       * which you want to have deleted.
		       * @param fr The Object, with the ID of the row, which should be deleted
		       * @param cuuid its uuid (for logging purposes only)
		       */
					virtual void deleteRepresentation(castor::IObject* fr, Cuuid_t cuuid)
	  				throw (castor::exception::Exception);
	  				
					/**
		       * Updates the IObject representation in the data base. Please edit this function and 
		       * add here for your concrete Class instance your concrete Objects, 
		       * which you want to have deleted.
		       * @param fr The Object, with the ID of the row, which should be deleted
		       * @param cuuid its uuid (for logging purposes only)
		       */
					virtual void updateRepresentation(castor::IObject* fr, Cuuid_t cuuid)
	  				throw (castor::exception::Exception);	  			
	
	    }; // class BaseRequestHandler
		
		} // end of namespace handler
		
  } // end of namespace vdqm

} // end of namespace castor

#endif //_BASEREQUESTHANDLER_HPP_
