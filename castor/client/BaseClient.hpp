/******************************************************************************
 *                      BaseClient.hpp
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
 * @(#)$RCSfile$ $Revision$ $Release$ $Date$ $Author$
 *
 * A base class for all castor clients. Implements many
 * useful and common parts for all clients.
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#ifndef CLIENTS_BASECLIENT_HPP
#define CLIENTS_BASECLIENT_HPP 1

// Include Files
#include <string>
#include "castor/exception/Exception.hpp"
#include "castor/BaseObject.hpp"

#define CSP_RHSERVER_PORT 9002

namespace castor {

  // Forward declaration
  class IClient;
  class IObject;

  namespace io {
    // Forward declaration
    class ServerSocket;
  }
  
  namespace stager {
    // Forward declaration
    class Request;
  }

  namespace client {

    /**
     * Name of the Service class environment variables
     */
    extern const char* HOST_ENV;;
    extern const char* HOST_ENV_ALT;
    extern const char* PORT_ENV;
    extern const char* PORT_ENV_ALT;
    extern const char* CATEGORY_CONF;
    extern const char* HOST_CONF;
    extern const char* PORT_CONF;
    extern const char* STAGE_EUID;
    extern const char* STAGE_EGID;

    // Forward declaration
    class IResponseHandler;

    class BaseClient : public BaseObject {

    public:

      /**
       * constructor
       */
      BaseClient(int acceptTimeout = 2592000) throw();

      /**
       * destructor
       */
      virtual ~BaseClient() throw();

      /**
       * Main method. This is the only one to call in the
       * "main" method after creation of a Client object.
       * It can be called any number of time to send any
       * number of queries to the castor system.
       * It is a blocking method that calls back the response
       * handler given as a parameter.
       * @param req the Request to send to the castor system
       * @param rh the IResponseHandler interface to use
       * for callbacks
       * @return The CASTOR request ID
       * @exception Exception when something goed wrong
       */
      std::string sendRequest(castor::stager::Request* req,
                              castor::client::IResponseHandler* rh)
        throw(castor::exception::Exception);
      

      /**
       * Sets the authorization ID under which the request should be sent.
       */
      int setAuthorizationId(uid_t uid, gid_t gid) throw();

    protected:

      /**
       * gets the request handler port to use and put it
       * into m_rhPort. First try environment (RH_PORT)
       * then castor.conf (RH PORT entry) and if nothing
       * available use a default.
       * May be overwritten in case this behavior should be
       * modified.
       */
      virtual void setRhPort() throw (castor::exception::Exception);

      /**
       * gets the request handler host to use and put it
       * into m_rhHost. First try environment (RH_HOST)
       * then castor.conf (RH HOST entry) and if nothing
       * available use the local machine.
       * May be overwritten in case this behavior should be
       * modified.
       */
      virtual void setRhHost() throw (castor::exception::Exception);

      /**
       * Sets the request ID in the base client
       */
      void setRequestId(std::string requestId);
      

      /**
       * Returns the request id 
       */
      std::string requestId();
      

    private:
      
      /**
       * creates a Client object from the callback socket
       * The caller is responsible for deallocating the
       * new client
       */
      virtual IClient* createClient()
        throw (castor::exception::Exception);

      /**
       * sends a request to the request handler
       */
      std::string internalSendRequest(castor::stager::Request& request)
        throw (castor::exception::Exception);

      /**
       * Waits for a call back from the request replier and
       * returns the ServerSocket created. No that the caller
       * is responsible for the deletion of the returned socket.
       * @return the socket to the requestReplier
       */
      castor::io::ServerSocket* waitForCallBack()
        throw (castor::exception::Exception);


      

    protected:
      
      /// The request handler host
      std::string m_rhHost;

      /// The request handler port
      int m_rhPort;
      
      /// The callback socket
      castor::io::ServerSocket* m_callbackSocket;

      /// The request id, if returned by the request handler
      std::string m_requestId;

      /// Timeout for the accept
      int m_acceptTimeout;

      /// Authorization ID parameters
      bool m_hasAuthorizationId;
      uid_t m_authUid;
      gid_t m_authGid;

      
    };

  } // end of namespace client
  
} // end of namespace castor

#endif // CLIENTS_BASECLIENT_HPP
