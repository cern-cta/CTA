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

    /**
     * Exception returned when there are no more messages to send
     * to on the connection 
     */
    class NoMoreMessagesException : public castor::exception::Exception {
    public:
      NoMoreMessagesException();
    };

    /**
     * Exception returned when the connection has been closed
     */    
    class EndConnectionException : public castor::exception::Exception {
    public:
      EndConnectionException(); 
    };

    /**
     * Structure representing a client message queued on the connection
     */
    struct ClientResponse {
      castor::rh::Client client;
      castor::io::biniostream *response;
      bool isLast;
      int messageId;
    };
    
    /**
     * Status of a ClientConnection
     */
    enum RCStatus { INACTIVE, CONNECTING, CONNECTED, 
		    RESEND, SENDING, DONE_SUCCESS, 
		    DONE_FAILURE, CLOSE };

    /**
     * Class representing a connection between the request replier
     * and a client.
     *
     * It contains a list of responses to be sent to client.
     * The order of responses is maintained to make sure
     * that the EndResponse closes the connection.
     */
    class ClientConnection : public BaseObject {
 
    private:

      /**
       * The Client to which the responses should be sent
       */
      castor::rh::Client m_client;

      /**
       * The queue of messages to be sent to the client
       * (as ClientResponse)
       */
      std::queue<ClientResponse> m_messages;

      /**
       * The file descriptor opened for the connection.
       */
      int m_fd;

      /**
       * Date of the last modification to the connection.
       * Used for timeout and garbage collection
       */
      int m_lastEventDate;

      /**
       * Status of the Connection
       */
      RCStatus m_status;
      
      /**
       * Indicates whether the connection should be terminated
       */
      bool m_terminate;

      /**
       * Eventual error message
       */
      std::string m_errorMessage;


      /**
       * String representation of client ip:port (for faster access)
       */
      std::string m_clientStr;

      /**
       * Numbering of the messages
       */
      int m_nextMessageId;

    public:

      /**
       * Constructors for the ClientConnection.
       * The Default constructor and one taking a ClientResponse 
       * structure are provided.
       */
      ClientConnection() throw();
      ClientConnection(ClientResponse cr) throw();

      /**
       * Method to add a new response to the queue of an 
       * existing connection
       */
      void addMessage(ClientResponse cr) throw();

      /**
       * Destructor
       */
      ~ClientConnection() throw() {};
      
      /**
       * Methods to deal with the status of the connection
       */
      RCStatus getStatus() throw();
      std::string getStatusStr() throw();
      void setStatus(enum RCStatus status) throw();
      
      /**
       * returns the connections' client
       */
      castor::rh::Client client() throw ();


      /**
       * Indicates whether the connection should be terminated
       */
      bool terminate() throw ();

      /**
       * Date of last event on the connection, used by the garbage collection
       */
      int lastEventDate() throw ();

      /**
       * Get connection's file descriptor
       */
      int fd() throw ();

      /**
       * Last error message on the connection
       */
      std::string errorMessage() throw ();
      void setErrorMessage(std::string msg) throw ();
      
      /**
       * Returns the Response queue
       */
      std::queue<ClientResponse> messages() throw() ;


      /**
       * Closes connections and releases the resources
       */
      void close() throw();

      /**
       * Calls the socket system call and creates the socket
       */
      void createSocket() throw(castor::exception::Exception);

      /**
       * performs the connect system call on the connection
       */
      void connect() throw(castor::exception::Exception);
      
      /**
       * Sends one message through the connection
       */
      void sendNextMessage() throw(castor::exception::Exception);

      /**
       * Indicates whetehr there are messages to be sent, still
       */
      bool hasMessagesToSend() throw();

      /**
       * pops one response from the queue
       */
      void deleteNextMessage() throw(castor::exception::Exception);

      /**
       * Representation of the client connection
       */
      std::string toString() throw();

      
      static std::string buildClientStr(castor::rh::Client client);


    protected:
      void send() throw(castor::exception::Exception);



    };

  } // namespace replier
} // namespace castor

#endif // CASTOR_CLIENTCONNECTION_HPP





