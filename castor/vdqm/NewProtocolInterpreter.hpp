/******************************************************************************
 *                      NewProtocolInterpreter.hpp
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
 * @(#)RCSfile: NewProtocolInterpreter.hpp  Revision: 1.0  Release Date: Aug 3, 2005  Author: mbraeger 
 *
 *
 *
 * @author Matthias Braeger
 *****************************************************************************/

#ifndef _NEWPROTOCOLINTERPRETER_HPP_
#define _NEWPROTOCOLINTERPRETER_HPP_

#include "castor/BaseObject.hpp"

namespace castor {

  namespace vdqm {
  	
  	// Forward declaration
  	class VdqmServerSocket;

    /**
     * This class provides the functions to send and receive messages through a 
     * socket, due to the rules of the NEW vdqm protocol 
     * (which is not implemented yet). 
     * When a new RTCOPYClient or a new TPDAEMON will be implemented, 
     * please insert here the functions to read out the message.
     * Because it don't implements a socket, neither the functionality to handle
     * the connection, the OldProtocolInterpreter is strongly dependent from the 
     * VdqmServerSocket class.
     * This class will be used in future by the VdqmServer.
     */
    class NewProtocolInterpreter : public castor::BaseObject {

	    public:
	
	      /**
	       * Constructor
	       * 
	       * @param serverSocket The Object, which includes the actual socket 
	       * connection to the client
	       * @param cuuid the cuuid of the incoming request 
	       * @exception In case that one of the parameter is NULL
	       */
	      NewProtocolInterpreter(VdqmServerSocket* serverSocket, const Cuuid_t* cuuid) 
		      throw (castor::exception::Exception);
		      
		    /**
		     * Destructor
		     */  
		    virtual ~NewProtocolInterpreter() throw();
		    
	      /**
	       * Reads an object from the socket. Please use 
	       * this function only after having read out the magic number!
	       * Note that the deallocation of it is the responsability of the caller
	       * 
	       * @return the IObject read
	       * @exception In case of error
	       */
	      virtual castor::IObject* readObject() throw(castor::exception::Exception);		    
		    	      	

//--------------------------------------------------------------------------
// Private Part of the class
//--------------------------------------------------------------------------
	    private:	
	      /**
	       * Internal function to read from a socket the rest of the Message into 
	       * a buffer. This method is only used by readObject.
	       * @param magic the magic number expected as the first 4 bytes read.
	       * An exception is sent if the correct magic number is not found
	       * @param buf allocated by the method, it contains the data read.
	       * Note that the deallocation of this buffer is the responsability
	       * of the caller
	       * 
	       * @param buf Allocated buffer for the rest of the message waiting in the socket
	       * @param n the length of the allocated buffer
	       * @exception In case of error
	       */
	      void readRestOfBuffer(char** buf, int& n)
	        	throw (castor::exception::Exception);	    
	    
	      	
	  		/**
	  		 * The object which includes the socket connection to the client
	  		 */
	  		VdqmServerSocket* ptr_serverSocket;  
	  		
	  		/**
	  		 * The cuuid of the request
	  		 */
	  		const Cuuid_t* m_cuuid;	  		
    };

  } // end of namespace vdqm

} // end of namespace castor      


#endif //_NEWPROTOCOLINTERPRETER_HPP_
