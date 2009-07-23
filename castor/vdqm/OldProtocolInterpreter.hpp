/******************************************************************************
 *                      OldProtocolInterpreter.hpp
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
 * @(#)RCSfile: OldProtocolInterpreter.hpp  Revision: 1.0  Release Date: Aug 3, 2005  Author: mbraeger 
 *
 *
 *
 * @author Matthias Braeger
 *****************************************************************************/

#ifndef CASTOR_VDQM_OLDPROTOCOLINTERPRETER_HPP
#define CASTOR_VDQM_OLDPROTOCOLINTERPRETER_HPP 1

#include "castor/BaseObject.hpp"
#include "castor/io/ServerSocket.hpp"
#include "h/vdqm_messages.h"


namespace castor {

  namespace vdqm {
    
    /**
     * This class provides the functions to send and receive messages through a 
     * socket, due to the rules of the old vdqm protocol. 
     *
     * This class does not implement a socket and is therefore strongly
     * dependent on castor::io::ServerSocket and castor::vdqm::VdqmSocketHelper.
     *
     * This class is used by the VdqmServer.
     */
    class OldProtocolInterpreter : public castor::BaseObject {

    public:
  
      /**
       * Constructor
       * 
       * @param socket The Object, which includes the actual socket connection
       * to the client
       * @param cuuid the cuuid of the incoming request 
       * @exception In case that one of the parameters is NULL
       */
      OldProtocolInterpreter(castor::io::ServerSocket &socket,
        const Cuuid_t &cuuid) throw (castor::exception::Exception);
          
      /**
       * Destructor
       */  
      virtual ~OldProtocolInterpreter() throw();
      
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
      int readProtocol(vdqmHdr_t *header, vdqmVolReq_t *volumeRequest, 
        vdqmDrvReq_t *driveRequest) 
      throw (castor::exception::Exception);
                  
      /**
       * Sends the request back to the client with its corresponding ID. 
       * 
       * @return The request Type number of the old vdqm Protocol
       * @exception In case of error
       */        
      int sendToOldClient(vdqmHdr_t *header, vdqmVolReq_t *volumeRequest,
        vdqmDrvReq_t *driveRequest) 
      throw (castor::exception::Exception);
                          
      /**
       * Sends a VDQM_COMMIT back to the client.
       * 
       * @exception In case of error
       */                  
      void sendAcknCommit() 
      throw (castor::exception::Exception);
          
      /**
       * Waits for an acknowledgement of the old tape daemon, that the
       * vdqm server has stored its request.
       * 
       * @return The message type, which has been send from the client
       * @exception In case of error
       */  
      int recvAcknFromOldClient()
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
      void sendAcknRollback(int errorCode) 
      throw (castor::exception::Exception);          
          

      private:      

        /**
         * The object which includes the socket connection to the client
         */
        castor::io::ServerSocket &m_socket;  
        
        /**
         * The cuuid of the request
         */
        const Cuuid_t &m_cuuid;

    }; // class OldProtocolInterpreter

  } // namespace vdqm

} // namespace castor      

#endif // CASTOR_VDQM_OLDPROTOCOLINTERPRETER_HPP
