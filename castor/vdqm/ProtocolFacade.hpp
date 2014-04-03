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
 *
 *
 *
 * @author Matthias Braeger
 *****************************************************************************/

#pragma once

#include "castor/BaseObject.hpp"
#include "castor/io/ServerSocket.hpp"


namespace castor {

  namespace vdqm {
    
    /**
     * The ProtocolFacede class is an etra layer between the VdqmServer and
     * those classes, which are implementing the protocol handling.
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
      ProtocolFacade(castor::io::ServerSocket &socket, const Cuuid_t &cuuid)
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
       * Good-day Protocol
       * =================
       *
       * 1. CLIENT >>>> request      >>>> SERVER
       * 2. CLIENT <<<< VDQM_COMMIT  <<<< SERVER
       * 3. CLIENT <<<< request      <<<< SERVER
       * 4. CLIENT >>>> VDQM_COMMIT  >>>> SERVER
       *
       * Bad-day Protocol: Server unable to execute request
       * ==================================================
       *
       * 1. CLIENT >>>> request      >>>> SERVER
       * 2. CLIENT <<<< error code   <<<< SERVER
       *
       * Bad-day Protocol: Client rolls back request
       * ===========================================
       *
       * 1. CLIENT >>>> request      >>>> SERVER
       * 2. CLIENT <<<< VDQM_COMMIT  <<<< SERVER
       * 3. CLIENT <<<< request      <<<< SERVER
       * 4. CLIENT >>>> ~VDQM_COMMIT >>>> SERVER
       *
       * NOTE: ~VDQM_COMMIT means not a VDQM_COMMIT
       * 
       * @param magicNumber The magic Number of the used protocol.  This is used
       * to complete the message header.
       */
      void handleOldVdqmRequest(const unsigned int magicNumber)
      throw (castor::exception::Exception);

      /**
       * Handles a VDQM message with a magic number of VDQM_MAGIC2.
       *
       * Good-day Protocol
       * =================
       *
       * 1. CLIENT >>>> request      >>>> SERVER
       * 2. CLIENT <<<< VDQM_COMMIT  <<<< SERVER
       * 3. CLIENT >>>> VDQM_COMMIT  >>>> SERVER
       *
       * Bad-day Protocol: Server unable to execute request
       * ==================================================
       *
       * 1. CLIENT >>>> request      >>>> SERVER
       * 2. CLIENT <<<< error code   <<<< SERVER
       *
       * Bad-day Protocol: Client rolls back request
       * ===========================================
       *
       * 1. CLIENT >>>> request      >>>> SERVER
       * 2. CLIENT <<<< VDQM_COMMIT  <<<< SERVER
       * 3. CLIENT >>>> ~VDQM_COMMIT >>>> SERVER
       *
       * NOTE: ~VDQM_COMMIT means not a VDQM_COMMIT
       *
       * @param magicNumber The magic Number of the used protocol.  This is used
       * to complete the message header.
       */
      void handleVdqmMagic2Request(const unsigned int magicNumber)
      throw (castor::exception::Exception);

      /**
       * Handles a VDQM message with a magic number of VDQM_MAGIC3.
       *
       * Good-day Protocol
       * =================
       *
       * 1. CLIENT >>>> request      >>>> SERVER
       * 2. CLIENT <<<< VDQM_COMMIT  <<<< SERVER
       * 3. CLIENT >>>> VDQM_COMMIT  >>>> SERVER
       *
       * Bad-day Protocol: Server unable to execute request
       * ==================================================
       *
       * 1. CLIENT >>>> request      >>>> SERVER
       * 2. CLIENT <<<< error code   <<<< SERVER
       *
       * Bad-day Protocol: Client rolls back request
       * ===========================================
       *
       * 1. CLIENT >>>> request      >>>> SERVER
       * 2. CLIENT <<<< VDQM_COMMIT  <<<< SERVER
       * 3. CLIENT >>>> ~VDQM_COMMIT >>>> SERVER
       *
       * NOTE: ~VDQM_COMMIT means not a VDQM_COMMIT
       *
       * @param magicNumber The magic Number of the used protocol.  This is used
       * to complete the message header.
       */
      void handleVdqmMagic3Request(const unsigned int magicNumber)
      throw (castor::exception::Exception);

      /**
       * Handles a VDQM message with a magic number of VDQM_MAGIC4.
       *
       * Good-day Protocol
       * =================
       *
       * 1. CLIENT >>>> request      >>>> SERVER
       * 2. CLIENT <<<< VDQM_COMMIT  <<<< SERVER
       * 3. CLIENT <<<< request      <<<< SERVER
       * 4. CLIENT >>>> VDQM_COMMIT  >>>> SERVER
       *
       * Bad-day Protocol: Server unable to execute request
       * ==================================================
       *
       * 1. CLIENT >>>> request      >>>> SERVER
       * 2. CLIENT <<<< error code   <<<< SERVER
       *
       * Bad-day Protocol: Client rolls back request
       * ===========================================
       *
       * 1. CLIENT >>>> request      >>>> SERVER
       * 2. CLIENT <<<< VDQM_COMMIT  <<<< SERVER
       * 3. CLIENT <<<< request      <<<< SERVER
       * 4. CLIENT >>>> ~VDQM_COMMIT >>>> SERVER
       *
       * NOTE: ~VDQM_COMMIT means not a VDQM_COMMIT
       *
       * @param magicNumber The magic Number of the used protocol.  This is used
       * to complete the message header.
       */
      void handleVdqmMagic4Request(const unsigned int magicNumber)
      throw (castor::exception::Exception);

      /**
       * The object which includes the socket connection to the client
       */
      castor::io::ServerSocket &m_socket;  
        
      /**
       * The cuuid of the request
       */
      const Cuuid_t &m_cuuid;
    
    }; // class ProtocolFacade

  } // end of namespace vdqm

} // end of namespace castor

