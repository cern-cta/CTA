/******************************************************************************
 *                      RequestReplier.cpp
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
 * @(#)$RCSfile: RequestReplier.hpp,v $ $Revision: 1.1 $ $Release$ $Date: 2004/11/05 14:23:55 $ $Author: bcouturi $
 *
 *
 *
 * @author Benjamin Couturier
 *****************************************************************************/


#ifndef CASTOR_REQUESTREPLIER_HPP
#define CASTOR_REQUESTREPLIER_HPP 1

#include "castor/replier/ClientConnection.hpp"
#include "castor/BaseObject.hpp"
#include "castor/io/biniostream.h"
#include <sys/poll.h>
#include <queue>
#include <map>

namespace castor {
  
  // Forward Declarations
  class IClient;

  namespace replier {

    class RequestReplier : public BaseObject {

    public:

      /**
       * Static method that returns the instance of the RequestReplier singleton
       * @return A pointer to the RequestReplier instance
       */
      static RequestReplier *getInstance() throw();

      /**
       * Default constructor for the request replier
       */
      RequestReplier() throw();

      /**
       * Adds a client to the queue of clients waiting for a response.
       * @param client The client object indicating the client address
       */
      void replyToClient(castor::IClient *client,
                         castor::IObject *response,
			 bool isLastResponse = false) throw();


      /**
       * Finishes sending the request, and blocks until all requests are sent
       */
      void terminate() throw();


    protected:

      /**
       * Method to start the RequestReplier thread 
       * This is called by the default constructor
       */
      void start() throw();

      /**
       * Static replier thread main method
       */
      static void *staticReplierThread(void *arg) throw();

      /**
       * Instance replier threadmethod 
       */
      void *replierThread(void *arg) throw();

      /**
       * Reads the request put on the queue by the API,
       * and puts it in the map of all process requests
       * (taking the appropriate locks).
       */
      void readFromClientQueue() throw();

      /**
       * Creates a new ClientConnection object which is then stored in
       * the connections map the RequestReplier instance
       */
      void createNewClientConnection(ClientResponse cr) throw();

      /**
       * Deletes a ClientConnection from the connection map.
       */
      void deleteConnection(int fd) throw();

      /**
       * Creates the proper array for poll()
       * based on the map of connections
       */
       int buildNewPollArray(struct pollfd pl[]) throw();

      /**
       * Processes the poll array, when the poll()
       * system call returns > 0
       */
       void processPollArray(struct ::pollfd pl[], int ndfd) throw();

      /**
       * Cleans old entry from the connections map
       */
      void garbageCollect() throw();

      /**
       * Singleton instance of the RequestReplier
       */
      static castor::replier::RequestReplier *s_rr;

      /**
       * Queue of client response used to pass data between
       * the caller thread and the request replier thread.
       */
      std::queue<ClientResponse> *m_clientQueue;

      /**
       * Map of client connections, indexed by file descriptor 
       */
      std::map<int, ClientConnection *> *m_connections;

      /**
       * Pipe used to communicate between the caller threads and the 
       * request replier thread
       */
      int m_commPipe[2];
      int *m_pipeRead;
      int *m_pipeWrite;

      /**
       * Thread ID of the RequestReplier
       */
      int m_threadId;

      /**
       * Flag indicating to the RR that it should finish the
       * processing
       */
      int m_terminate;

    }; // class RequestReplier
  } // namespace replier
} // namespace castor

#endif // CASTOR_REQUESTREPLIER_HPP





