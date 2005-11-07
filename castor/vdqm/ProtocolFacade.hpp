/******************************************************************************
 *                      ProtocolFacade.hpp
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
 * @(#)RCSfile: ProtocolFacade.hpp  Revision: 1.0  Release Date: Nov 7, 2005  Author: mbraeger 
 *
 *
 *
 * @author Matthias Braeger
 *****************************************************************************/

#ifndef _PROTOCOLFACADE_HPP_
#define _PROTOCOLFACADE_HPP_

#include "castor/BaseObject.hpp"

namespace castor {

  namespace vdqm {
  	
		//Forward Declarations
  	class VdqmServerSocket;

    /**
     * The ProtocolFacede class is an etra layer between the VdqmServer and those
     * classes, which are implementing the protocol handling.
     * VdqmServer calls the handleProtocolVersion() function, which takes the
     * protocol decision due to the sent magic number.
     */
    class ProtocolFacade : public castor::BaseObject {

    	public:
    		/**
	       * Constructor
	       * 
	       * @param serverSocket The Object, which includes the actual socket 
	       * connection to the client
	       * @param cuuid the cuuid of the incoming request 
	       * @exception In case that one of the parameter is NULL
	       */
	    	ProtocolFacade(VdqmServerSocket* serverSocket, const Cuuid_t* cuuid)
	    	  throw (castor::exception::Exception);
    	
	    	/**
		     * Destructor
		     */  
		    virtual ~ProtocolFacade() throw();
	    	
	    	/**
		     * This function is called by the VdqmServer to handle the 
		     * different protocol versions. Please edit this function, if
		     * you want to include new protocols!
		     *
		     * @exception Throws an exception in case of errors
		     */
		    void handleProtocolVersion()
	     		throw (castor::exception::Exception);	    
		    
		    
		  private:
	      /**
	       * Internal function to handle the old Vdqm request. Puts the values into
	       * the old structs and reads out the request typr number, to delegates
	       * them to the right function.
	       * 
	       * @param magicNumber The magic Number of the used protocol
	       * @exception Throws an exception in case of errors
	       */
	      void handleOldVdqmRequest(unsigned int magicNumber)
	      	throw (castor::exception::Exception);
	      
	      	
	      /**
	  		 * The object which includes the socket connection to the client
	  		 */
	  		VdqmServerSocket* ptr_serverSocket;  
	  		
	  		/**
	  		 * The cuuid of the request
	  		 */
	  		const Cuuid_t* m_cuuid;
    
  	}; // class ProtocolFacade

  } // end of namespace vdqm

} // end of namespace castor

#endif //_PROTOCOLFACADE_HPP_
