/******************************************************************************
 *                      BaseCmdLineClient.hpp
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
 * Base class for command line clients. It provides parsing
 * of the input arguments and print out of the result plus
 * some canvas for the implementation of actual clients
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#ifndef CLIENTS_BASECMDLINECLIENT_HPP
#define CLIENTS_BASECMDLINECLIENT_HPP 1

// Include Files
#include <map>
#include <string>
#include <vector>
#include "castor/exception/Exception.hpp"
#include "castor/client/BaseClient.hpp"

namespace castor {

  namespace stager {
    // Forward declaration
    class Request;
  }

  namespace client {

    class BaseCmdLineClient : public BaseClient {

    public:

      /**
       * constructor
       */
      BaseCmdLineClient() throw();

      /**
       * destructor
       */
      virtual ~BaseCmdLineClient() throw();

      /**
       * Main method. This is the only one to call in the
       * "main" method after creation of a Client object.
       */
      void run(int argc, char** argv) throw();

    protected:

      /**
       * parses the input arguments and store them
       * @return whether it was successful or not
       */
      bool parseInput(int argc, char** argv)
        throw (castor::exception::Exception);

      /**
       * builds the actual request. This method has to be
       * reimplemented in each client.
       * Note that the caller is responsible for the deallocation
       * of the request
       * @return the request to be sent to the request handler
       */
      virtual castor::stager::Request* buildRequest()
        throw (castor::exception::Exception) = 0;

      /**
       * creates a responseHandler to be used for the requests.
       * A default implementation is given here.
       * It can be overwritten in the end clients in order
       * to specialize the output
       */
      virtual castor::client::IResponseHandler* responseHandler()
        throw();

      /**
       * Display an error message and
       * show usage of the executable.
       * Has to be reimplemented in each client.
       */
      virtual void usage(std::string message) throw () = 0;

      // here are some usefull methods to get the values
      // of some very common parameters

      /**
       * gets the request handler host to use and put it
       * into m_rhHost. Reimplements the default in order
       * to take the command line into account
       */
      virtual void setRhHost() throw (castor::exception::Exception);

      /**
       * returns the pool name to use
       */
      std::string getPoolName() throw (castor::exception::Exception);

      /**
       * parses the sizes option if existing and returns
       * a vector of sizes
       */
      std::vector<u_signed64> getSizes()
        throw (castor::exception::Exception);

      /**
       * rejects a given list of flags by sending an
       * exception if one of them was set
       * @param flags the flags to reject
       * @param cmdName the name of the command (for logging purposes)
       * @exception Exception in case of of the flags was set
       */
      void rejectFlags(std::vector<std::string> &flags,
                       std::string cmdName)
        throw(castor::exception::Exception);

    protected:

      /// The input flags
      std::map<std::string, std::string> m_inputFlags;

      /// The input arguments
      std::vector<std::string> m_inputArguments;

    };

  } // end of namespace client

} // end of namespace castor

#endif // CLIENTS_BASECMDLINECLIENT_HPP
