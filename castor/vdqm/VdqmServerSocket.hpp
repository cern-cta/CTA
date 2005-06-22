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

// Include Files
#include <net.h>
#include <string>
#include <netinet/in.h>

#include "castor/exception/Exception.hpp"
#include "castor/io/AbstractSocket.hpp"

// Forward declaration
typedef struct vdqmHdr vdqmHdr_t; 
typedef struct vdqmVolReq vdqmVolReq_t; 
typedef struct vdqmdDrvReq vdqmDrvReq_t;


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
    class VdqmServerSocket : public castor::io::AbstractSocket {

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
       * Reads an object from the socket. Please use 
       * this function only after having read out the magic number!
       * Note that the deallocation of it is the responsability of the caller
       * 
       * @return the IObject read
       * @exception In case of error
       */
      virtual castor::IObject* readObject() throw(castor::exception::Exception);
      
      /**
       * This function reads the old vdqm protocol out of the buffer. In fact
       * it is quite similar to the old vdqm_RecvReq() C function. Please use 
       * this function only after having read out the magic number! 
       * 
       * @return The request Type number of the old vdqm Protocol
       * @param header The old header
       * @param volumeRequest The old volumeRequest, which corresponds now 
       * to the TapeRequest
       * @param driveRequest the old driveRequest which corresponds now 
       * to the TapeDrive
       * @exception In case of error
       */
      int readOldProtocol(vdqmHdr_t *header, 
      										vdqmVolReq_t *volumeRequest, 
      										vdqmDrvReq_t *driveRequest,
      										Cuuid_t cuuid) 
      									throw (castor::exception::Exception);
      					
      /**
       * Sends the request back to the client with its corresponding ID. 
       * 
       * @return The request Type number of the old vdqm Protocol
       * @exception In case of error
       */				
      int sendToOldClient(vdqmHdr_t *header, 
      										vdqmVolReq_t *volumeRequest, 
      										vdqmDrvReq_t *driveRequest,
      										Cuuid_t cuuid) 
      									throw (castor::exception::Exception);
      									
      /**
       * Sends a VDQM_COMMIT back to the client.
       * 
       * @exception In case of error
       */									
      void acknCommitOldProtocol() 
      	throw (castor::exception::Exception);
      	
      /**
       * Waits for an acknowledgement of the old tpdaemon, that the
       * vdqm server has stored its request.
       * 
       * @return The message type, which has been send from the client
       * @exception In case of error
       */	
      int recvAcknFromOldProtocol()
      	throw (castor::exception::Exception);
      	
      /**
       * Sends a commit for a ping request back to the client and informes about
       * the queue position of the pinged TapeRequest.
       * 
       * @param queuePosition The queue position of the pinged TapeRequest
       * @exception In case of error
       */
      void sendAcknPing(int queuePosition) 
      	throw (castor::exception::Exception);
      	
     	/**
     	 * This funtion is used in case of errors to send back the error code
     	 * to the client. The similar function in the old vdqm part is 
     	 * vdqm_AcknRollback().
     	 * 
     	 * @param errorCode The errorCode which has been thrown
       * @exception In case of error
     	 */
      void sendAcknRollbackOldProtocol(int errorCode) 
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
      
      /**
       * Tells whether listen was already called
       */
      bool m_listening;
      
     	/**
     	 * Definition of SendTo and RceiveFrom for the old DO_MARSHALL
     	 */
     	typedef enum direction {SendTo, ReceiveFrom} direction_t;
    };

  } // end of namespace io

} // end of namespace castor

#endif //_VDQMSERVERSOCKET_HPP_
