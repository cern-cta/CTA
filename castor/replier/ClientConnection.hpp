/******************************************************************************
 *                      ClientConnection.hpp
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
 * @(#)ClientConnection.hpp,v 1.6 $Release$ 2004/09/17 09:08:27 bcouturi
 *
 *
 *
 * @author Benjamin Couturier
 *****************************************************************************/


#ifndef CASTOR_CLIENTCONNECTION_HPP
#define CASTOR_CLIENTCONNECTION_HPP 1

#include "castor/BaseObject.hpp"
#include "castor/rh/Client.hpp"
#include "castor/io/biniostream.h"
#include "castor/exception/Exception.hpp"
#include <queue>
#include <string>

namespace castor {
  namespace replier {


    class NoMoreMessagesException : public castor::exception::Exception {
    public:
      NoMoreMessagesException();
    };
    
    class EndConnectionException : public castor::exception::Exception {
    public:
      EndConnectionException(); 
    };

    /**
     * Structure used client info and response to the request replier
     */
    struct ClientResponse {
      castor::rh::Client client;
      castor::io::biniostream *response;
      bool isLast;
    };

    enum RCStatus { INACTIVE, CONNECTING, CONNECTED, 
		    RESEND, SENDING, DONE_SUCCESS, 
		    DONE_FAILURE, CLOSE };

    class ClientConnection : public BaseObject {
    public:
      // members
      castor::rh::Client m_client;
      std::queue<castor::io::biniostream *> m_responses;
      int m_fd;
      int m_lastEventDate;
      RCStatus m_status;
      std::string m_errorMessage;
      bool m_terminate;

      // Constructors
      ClientConnection() throw();
      ClientConnection(ClientResponse cr) throw();

      // Method to add a new response to an existing connection
      void addResponse(ClientResponse cr) throw();

      // Destructor
      ~ClientConnection() throw() {};
      
      // Methods for status
      RCStatus getStatus() throw();
      std::string getStatusStr() throw();
      void setStatus(enum RCStatus status) throw();
      castor::rh::Client getClient() throw ();

      // Closes connections and releases the space
      void close() throw();

      // IO Methods
      void createSocket() throw(castor::exception::Exception);
      void connect() throw(castor::exception::Exception);
      void sendData() throw(castor::exception::Exception);
      void pop() throw(castor::exception::Exception);

    protected:
      void send() throw(castor::exception::Exception);

    };

  } // namespace replier
} // namespace castor

#endif // CASTOR_CLIENTCONNECTION_HPP





