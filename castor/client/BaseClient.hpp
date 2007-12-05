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
#include "stager_client_api.h"

#define CSP_RHSERVER_PORT 9002
#define CSP_RHSERVER_SEC_PORT 9007

namespace castor {

  // Forward declaration
  class IClient;
  class IObject;

  namespace io {
    // Forward declaration
    class ServerSocket;
    class AuthServerSocket;
  }
  
  namespace stager {
    // Forward declaration
    class Request;
  }

  namespace client {

    /**
     * Constants
     */
    extern const char* HOST_ENV;
    extern const char* HOST_ENV_ALT;
    extern const char* PORT_ENV;
    extern const char* PORT_ENV_ALT;
    extern const char* SEC_PORT_ENV;
    extern const char* SEC_PORT_ENV_ALT;
    extern const char* CATEGORY_CONF;
    extern const char* HOST_CONF;
    extern const char* PORT_CONF;
    extern const char* SEC_PORT_CONF;
    extern const char* STAGE_EUID;
    extern const char* STAGE_EGID;
    extern const char* CLIENT_CONF;
    extern const char* LOWPORT_CONF;
    extern const char* HIGHPORT_CONF;
    extern const char* SEC_MECH_ENV;
    extern const char* SECURITY_ENV;
    extern const int LOW_CLIENT_PORT_RANGE;
    extern const int HIGH_CLIENT_PORT_RANGE;

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
       * Get the userid and groupid and set the authorization values.
       */
      void setAuthorizationId() throw(castor::exception::Exception);
	
      /**
       * Sets the authorization ID under which the request should be sent.
       */
      void setAuthorizationId(uid_t uid, gid_t gid) throw();
      
       
      /**
       * Set the Authorization mechanism.
       */

      void setAuthorization() throw (castor::exception::Exception );
      void setAuthorization(char *mech, char *id) throw (castor::exception::Exception );

      /**
       * gets the request handler port to use and put it
       * into m_rhPort. First try environment (RH_PORT)
       * then castor.conf (RH PORT entry) and if nothing
       * available use a default.
       * May be overwritten in case this behavior should be
       * modified.
       */
      virtual void setRhPort(int optPort) throw (castor::exception::Exception);
      virtual void setRhPort() throw (castor::exception::Exception);

      /**
       * gets the request handler host to use and put it
       * into m_rhHost. First try environment (RH_HOST)
       * then castor.conf (RH HOST entry) and if nothing
       * available use the local machine.
       * May be overwritten in case this behavior should be
       * modified.
       */

      
      virtual void setRhHost(std::string optHost) throw (castor::exception::Exception);
      virtual void setRhHost()throw (castor::exception::Exception);
      virtual void setRhSvcClass(std::string optSvcClass) throw (castor::exception::Exception);
      virtual void setRhSvcClass()throw (castor::exception::Exception);
      virtual void setOption(struct stage_options* opts)throw (castor::exception::Exception);

      /**
       * Sets the request ID in the base client
       */
      void setRequestId(std::string requestId);
      
      /**
       * Returns the request id 
       */
      std::string requestId();

      /**
       * polls the answers from stager after sending a request by 
       * internalSendRequest.  
       * @param req The StagerRequest the Client is to be added to
       * @exception in case of an error 
       */
      void pollAnswersFromStager(castor::stager::Request* req,
				 castor::client::IResponseHandler* rh)
	throw (castor::exception::Exception);
      
      /**
       * calls buildClient and internalSendRequest to create and send the
       * Request.  
       * @param req The StagerRequest to be handled
       * @exception in case of an error 
       */
      std::string createClientAndSend(castor::stager::Request *req)
	throw (castor::exception::Exception);

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

      /**
       * Builds the Client for a Request. The userid, groupid, hostname, etc
       * are set and the Client object added to the Request.
       * @param req The StagerRequest the Client is to be added to
       * @exception in case of an error 
       */
      void buildClient(castor::stager::Request* req)
	throw (castor::exception::Exception);
    public: // protected:
      
      /// The request handler host
      std::string m_rhHost;

      /// The request handler port
      int m_rhPort;

      /// The request handler svc class
      std::string m_rhSvcClass;

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
      
      /// Strong Authentication parameters
      bool m_hasSecAuthorization;
      char *m_Sec_mech;
      char *m_Csec_auth_id;
      char *m_voname;
      int  m_nbfqan;
      char **m_fqan;
      
    };

  } // end of namespace client
  
} // end of namespace castor

#endif // CLIENTS_BASECLIENT_HPP
