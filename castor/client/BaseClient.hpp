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
 * The only two methods to implement in order to get a real
 * client are buildRequest and printResult
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#ifndef CLIENTS_BASECLIENT_HPP
#define CLIENTS_BASECLIENT_HPP 1

// Include Files
#include <map>
#include <string>
#include <vector>
#include "castor/Exception.hpp"
#include "castor/BaseObject.hpp"

namespace castor {

  // Forward declaration
  class IClient;
  class IObject;

  namespace io {
    // Forward declaration
    class Socket;
  }
  
  namespace rh {
    // Forward declaration
    class Request;
  }

  namespace client {

    class BaseClient : public BaseObject {

    public:

      /**
       * constructor
       */
      BaseClient() throw();

      /**
       * destructor
       */
      virtual ~BaseClient() throw();

      /**
       * Main method. This is the only one to call in the
       * "main" method after creation of a Client object.
       */
      void run(int argc, char** argv) throw();

      /**
       * parses the input arguments and store them
       */
      void parseInput(int argc, char** argv) throw (Exception);

      /**
       * builds the actual request. This method has to be
       * reimplemented in each client.
       * Note that the caller is responsible for the deallocation
       * of the request
       * @return the request to be sent to the request handler
       */
      virtual castor::rh::Request* buildRequest() = 0;

      /**
       * builds a default description for a request
       */
      std::string getDefaultDescription() throw (Exception);

      /**
       * creates a Client object from the callback socket
       * The caller is responsible for deallocating the
       * new client
       */
      virtual IClient* createClient() throw (Exception);

      /**
       * sends a request to the request handler
       */
      void sendRequest(castor::rh::Request& request) throw (Exception);

      /**
       * sends a request to the request handler
       * @return the object sent back
       */
      IObject* waitForCallBack() throw (Exception);

      /**
       * this method is responsible for the output of the
       * request result. This method has to be reimplemented
       * in each client
       */
      virtual void printResult(IObject& result) = 0;

    protected:
      
      /// The request handler host
      std::string m_rhHost;

      /// The request handler port
      int m_rhPort;

      /// The callback socket
      castor::io::Socket* m_callbackSocket;

      /// The input flags
      std::map<std::string, std::string> m_inputFlags;

      /// The input arguments
      std::vector<std::string> m_inputArguments;
    };

  } // end of namespace client

} // end of namespace castor

#endif // CLIENTS_BASECLIENT_HPP
