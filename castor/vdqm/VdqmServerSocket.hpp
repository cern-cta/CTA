/******************************************************************************
 *                      VdqmServerSocket.hpp
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
 * @(#)RCSfile: VdqmServerSocket.hpp  Revision: 1.0  Release Date: Apr 12, 2005  Author: mbraeger 
 *
 *
 *
 * @author Matthias Braeger
 *****************************************************************************/
 
#ifndef _VDQMSERVERSOCKET_HPP_
#define _VDQMSERVERSOCKET_HPP_

#include "castor/io/AbstractTCPSocket.hpp"

namespace castor {

  // Forward declaration
  class IObject;

  namespace vdqm {

    /**
     * A dedicated socket class, able to deal with socket manipulation
     * and to handle sending and receiving of IObjects. It provides also a 
     * function to check the magic number of an incoming request and to deal the
     * old VDQM protocol.
     */
    class VdqmServerSocket : public castor::io::AbstractTCPSocket {

    public:

      /**
       * Constructor building a Socket objet around a regular socket
       * 
       * @param socket the regular socket used
       * @exception In case of error
       */
      VdqmServerSocket(int socket) throw ();

      /**
       * Constructor building a socket on a given local port
       * @param port the local port for this socket. Use 0 if
       * you want the system to allocate a port
       * 
       * @param port The port number, to which the socket should listen
       * @param reusable If the socket should be reusable or not
       * @exception In case of error
       */
      VdqmServerSocket(const unsigned short port,
		   const bool reusable)
        throw (castor::exception::Exception);

      /**
       * destructor
       */
      ~VdqmServerSocket() throw();


		  /**
	     * Sets the SO_REUSEADDR option on the socket
	     */
	    void reusable() throw (castor::exception::Exception);
	
		  /**
		   * Reads the first four Bytes of the header. This function was added to 
		   * support also the older VDQM protocol. The magic number defines, which
		   * protocol is used.
		   * 
		   * @return The magic number
		   * @exception In case of error
		   */
	      unsigned int readMagicNumber() throw (castor::exception::Exception);
	        

      /**
       * start listening on the socket
       * 
       * @exception In case of error
       */
      virtual void listen() throw(castor::exception::Exception);
                               
      
      /**
       * accept a connection and return the correponding Socket.
       * The deallocation of the new socket is the responsability
       * of the caller.
       * 
       * @exception In case of error
       */
      virtual VdqmServerSocket* accept() throw (castor::exception::Exception);      
      
      /**
       * This function is used internally to write the header buffer 
       * on the socket.
       * 
       * @param hdrbuf The header buffer, which contains the data for the client
       * @exception In case of error
       */
      void vdqmNetwrite(void* hdrbuf) 
        	throw (castor::exception::Exception);
        	
      /**
       * This function is used internally to read the header 
       * from the socket to the overgiven buffer.
       * 
       * @param hdrbuf The header buffer, where the data will be written to
       * @exception In case of error
       */
      void vdqmNetread(void* hdrbuf) 
        	throw (castor::exception::Exception);       
      

    protected:

      /**
       * binds the socket to the given address
       * 
       * @exception In case of error
       */
      void bind(sockaddr_in saddr)
        throw (castor::exception::Exception);       


	//--------------------------------------------------------------------------
	// Private Part of the class
	//--------------------------------------------------------------------------
    private:            	       
      
      /**
       * Tells whether listen was already called
       */
      bool m_listening;
    };

  } // end of namespace vdqm

} // end of namespace castor

#endif //_VDQMSERVERSOCKET_HPP_
