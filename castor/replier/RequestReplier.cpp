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
 * @(#)$RCSfile: RequestReplier.cpp,v $ $Revision: 1.4 $ $Release$ $Date: 2004/11/19 18:29:33 $ $Author: bcouturi $
 *
 *
 *
 * @author Benjamin Couturier
 *****************************************************************************/

#include <Cthread_api.h>

#include "RequestReplier.hpp"
#include "ClientConnection.hpp"
#include "castor/IClient.hpp"
#include "castor/rh/Client.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/Constants.hpp"
#include "castor/logstream.h"
#include "castor/io/biniostream.h"
#include "castor/io/StreamCnvSvc.hpp"
#include "castor/Services.hpp"
#include "castor/rh/EndResponse.hpp"

#include <sys/poll.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <errno.h>

#include <cstring>
#include <string>
#include <stack>


/**
 * The RequestReplier is a component of sending the replies in non-blocking mode
 * to the clients contacting the stager.
 * It is a singleton (you can get it using the static getInstance method),
 * managing a dedicated thread in charge of sending the replies.
 * The replyToClient method must be used to send replies to the clients.
 * Befire exiting the program, terminate() must be called on the RequestReplier
 * singleton: it blocks until all requests have been sent. Otherwise, the
 * process could exit before all requests are sent.
 */


//////////////////////////////////////////////////////////////////////////////
//
// Initialization and singleton lookup methods.
//
//////////////////////////////////////////////////////////////////////////////

//-----------------------------------------------------------------------------
// Space declaration for the static RequestReplier instance
//-----------------------------------------------------------------------------
castor::replier::RequestReplier *castor::replier::RequestReplier::s_rr = 0;

//-----------------------------------------------------------------------------
// Static method to get the singleton instance
//-----------------------------------------------------------------------------
castor::replier::RequestReplier *
castor::replier::RequestReplier::getInstance() throw() {
  // XXX lock ?
  if (0 == s_rr) {
    s_rr = new castor::replier::RequestReplier();
    s_rr->clog() << ALWAYS << "Created new request replier" << std::endl;
  }
  return s_rr;
}

//-----------------------------------------------------------------------------
// constructor
//-----------------------------------------------------------------------------
castor::replier::RequestReplier::RequestReplier() throw() {

  // Initializing collections
  m_clientQueue = new std::queue<ClientResponse>();
  m_connections = new std::map<int, ClientConnection *>();

  // Establishing the pipe used for notification with ReplierThread
  int rc = pipe(m_commPipe);
  if (-1 == rc) {
    clog() << ERROR << "Could not establish communication pipe !" << std::endl;
  }
  m_pipeRead = &m_commPipe[0];
  m_pipeWrite = &m_commPipe[1];

  // Setting the end processing flag to zero
  m_terminate = 0;

  // Starting the ReplierThread
  start();
}

//-----------------------------------------------------------------------------
// Routine that starts the replier thread processing
//-----------------------------------------------------------------------------
void
castor::replier::RequestReplier::start() throw() {

  if ((m_threadId = Cthread_create(&(staticReplierThread), this))<0) {
    clog() << FATAL << "Could not create thread !" << std::endl;
  }
  clog() << DEBUG << "Request Replier thread ID is: " << m_threadId << std::endl;

}

//-----------------------------------------------------------------------------
// Static method called by Cthread
//-----------------------------------------------------------------------------
void *
castor::replier::RequestReplier::staticReplierThread(void *arg) throw() {
  if (0 == arg) {
    // XXX Commented out because clog no longer a static method !
    //clog() << ERROR << "Please specify an argument to staticReplierThread" << std::endl;
    return 0;
  }

  castor::replier::RequestReplier *s_rr = static_cast<castor::replier::RequestReplier *>(arg);
  if (0 == s_rr) {
    // XXX Commented out because clog no longer a static method !
    // clog() << ERROR << "Error in dynamic cast" << std::endl;
    return 0;
  }

  return s_rr->replierThread(0);
}

//////////////////////////////////////////////////////////////////////////////
//
// Code running in the RequestReplier thread
//
//////////////////////////////////////////////////////////////////////////////

//-----------------------------------------------------------------------------
// Method to process the messages
//-----------------------------------------------------------------------------
void *
castor::replier::RequestReplier::replierThread(void *arg) throw() {

  int pollRc;
  const int pollTimeout = 5;
  int nbPollFd = 1000;
  struct ::pollfd *toPoll = new struct ::pollfd[nbPollFd];

  while (1) {

    // Getting the value of the end processing flag
    Cthread_mutex_lock(&m_terminate);
    int terminate = m_terminate;
    Cthread_mutex_unlock(&m_terminate);

    if (terminate) {
      if(m_connections->size() == 0
         && m_clientQueue->size() == 0) {
        clog() << ALWAYS << "Finished processing - Terminating"
               << std::endl;
        break;
      } else {
        clog() << ALWAYS << "Waiting to terminate - ConnQ:"
               << m_connections->size()
               << " ClientQ:"
               << m_clientQueue->size()
               << std::endl;
      }
    }

    // Adjusting the poll array size
    if ( m_connections->size() > nbPollFd) {
      delete[] toPoll;
      nbPollFd = 2*(m_connections->size());
      toPoll = new struct ::pollfd[nbPollFd];
    }

    // Build new poll array for next call
    int nbfd = buildNewPollArray(toPoll);

    clog() << DEBUG << "Polling, nbfd=" << nbfd << std::endl;
    pollRc = poll(toPoll, nbfd, pollTimeout * 1000);
    clog() << VERBOSE << "Poll returned" << std::endl;
    if (pollRc == 0) {
      // Poll timed-out
      clog() << DEBUG << "Poll timed out" << std::endl;
      garbageCollect();
      if (m_clientQueue->size() > 0) {
        clog() << VERBOSE << "Timeout with "
               << m_clientQueue->size()
               << " in queue" << std::endl;
      }

      continue;
    } else if (pollRc < 0) {
      if (errno == EINTR) {
        clog() << VERBOSE << "Poll interrupted, continuing"
               << std::endl;
        continue;
      } else {
        clog() << DEBUG << "Error in poll:"
               << strerror(errno)
               << std::endl;
        return 0;
      }
    } // End if pollrc < 0

    // Look up events
    processPollArray(toPoll, nbfd);
    garbageCollect();

  } // End while(1)

  return 0;
}


//-----------------------------------------------------------------------------
// Create connection socket
//-----------------------------------------------------------------------------
void castor::replier::RequestReplier::createNewClientConnection(ClientResponse cr)
  throw() {

  // Looking in the hash of client to find if there is one already established
  ClientConnection *r = 0;
  const int newhost = cr.client.ipAddress();
  const int newport = cr.client.port();

  for(std::map<int, ClientConnection *>::iterator iter = m_connections->begin();
      iter != m_connections->end();
      iter++) {

    castor::rh::Client c = (*iter).second->getClient();
    if (newport == c.port()
        && newhost == c.ipAddress()) {
      // Found an existing connection !
      // XXX Should there be status testing ?
      clog() << DEBUG << "Found existing connection " << (*iter).second
             << " status " << (*iter).second->getStatusStr() << std::endl;
      r = (*iter).second;
      break;
    }
  }

  // Create new connection in case none was found !
  if (0 == r) {
    r = new ClientConnection(cr);
    clog() << DEBUG << "Creating new ClientConnection " << r << std::endl;
    try {
      r->createSocket();
      r->connect();
      (*m_connections)[r->m_fd] = r;
      clog() << VERBOSE << "ClientConnection " << r << " added fd is "
             << r->m_fd << std::endl;
    } catch(castor::exception::Exception e) {
      clog() << ERROR
             << "Exeption while Creating new ClientConnection: "
             << e.getMessage().str() << std::endl;
      r->setStatus(DONE_FAILURE);
      r->m_errorMessage = "Could not connect:" + e.getMessage().str();
    }
  } else {
    // Add the new connection to the list
    r->addResponse(cr);
  }
}


//-----------------------------------------------------------------------------
// Create connection socket
//-----------------------------------------------------------------------------
void castor::replier::RequestReplier::deleteConnection(int dfd) throw() {

  ClientConnection *cr = (*m_connections)[dfd];
  clog() << VERBOSE << "ClientConnection " << cr
         << " fd:" << dfd << " being deleted."
         << std::endl;

  if (0 != cr) {
    cr->close();
    m_connections->erase(dfd);
  }

}


//-----------------------------------------------------------------------------
// Cleans old entry from the connections map
//-----------------------------------------------------------------------------
void castor::replier::RequestReplier::garbageCollect() throw() {

  int t = time(0);
  const int TIMEOUT = 60;
  std::stack<int> *toremove = new std::stack<int>();

  for(std::map<int, ClientConnection *>::iterator iter = m_connections->begin();
      iter != m_connections->end();
      iter++) {

    clog() << VERBOSE << "GC Checking " << (*iter).second
           << " status: " << (*iter).second->getStatusStr()
           << " active time: "
           << (t - (*iter).second->m_lastEventDate)
           << std::endl;

    if ((*iter).second->getStatus() == DONE_FAILURE) {
      toremove->push((*iter).second->m_fd);
      unsigned long ip = (*iter).second->m_client.ipAddress();
      clog() << DEBUG << "ClientConnection " << iter->second
             << " DONE_FAILURE <"
             << (*iter).second->m_fd << "> client "
             << castor::ip << ip << ":"
             << (*iter).second->m_client.port() << std::endl;
    } else if ((*iter).second->m_terminate == true
               && (*iter).second->m_responses.empty()) {
      toremove->push((*iter).second->m_fd);
      unsigned long ip = (*iter).second->m_client.ipAddress();
      clog() << DEBUG << "ClientConnection " << iter->second
             <<" CLOSE <"
             << (*iter).second->m_fd << "> client "
             << castor::ip << ip << ":"
             << (*iter).second->m_client.port() << std::endl;

    } else if ((t - (*iter).second->m_lastEventDate) > TIMEOUT) {
      toremove->push((*iter).second->m_fd);
      unsigned long ip = (*iter).second->m_client.ipAddress();
      clog() << DEBUG << "ClientConnection " << iter->second
             << " TIMEOUT <"
             << (*iter).second->m_fd << "> client "
             << castor::ip << ip << ":"
             << (*iter).second->m_client.port()
             << " inactive for " << (t - (*iter).second->m_lastEventDate) << " s"
             << " status " << (*iter).second->getStatusStr()
             << std::endl;
    }
  } // End for

  while (!toremove->empty()) {
    int tfd = toremove->top();
    deleteConnection(tfd);
    toremove->pop();
  } // End while
}


//-----------------------------------------------------------------------------
// Build the array that should be polled
//-----------------------------------------------------------------------------
int
castor::replier::RequestReplier::buildNewPollArray(struct ::pollfd pl[])
  throw() {

  // BEWARE: Keep entry 0 identical as this the pollfd for the messaging pipe
  pl[0].fd = *m_pipeRead;
  pl[0].events = POLLIN;
  pl[0].revents = 0;

  int i =1; // Count in the array starting at second file descriptor
  for(std::map<int, ClientConnection *>::iterator iter = m_connections->begin();
      iter != m_connections->end();
      iter++) {

    if ((*iter).second->getStatus() == CONNECTING
        || (*iter).second->getStatus() == RESEND
        || ((*iter).second->getStatus() == CONNECTED
            && !(((*iter).second->m_responses).empty()))) {
      clog() << VERBOSE << "Listening to fd: "
             << (*iter).first << std::endl;
      pl[i].fd = (*iter).first;
      pl[i].events = POLLIN|POLLOUT|POLLHUP|POLLERR;
      pl[i].revents = 0;
      i++;
    }

    //     if ((*iter).second->getStatus() == SENT) {
    //       clog() << VERBOSE << "Listening to fd: "
    //              << (*iter).first << std::endl;
    //       pl[i].fd = (*iter).first;
    //       pl[i].events = POLLIN|POLLHUP|POLLERR;
    //       pl[i].revents = 0;
    //       i++;
    //     }

  }

  clog() << DEBUG << "There are " << (i-1)
         << " client(s) to reply to ! (among "
         << m_connections->size() << " connections)"
         << std::endl;

  // i has been increased by the ++ but that's ok as we have to add
  return i;
}

//-----------------------------------------------------------------------------
// Process poll array
//-----------------------------------------------------------------------------
void
castor::replier::RequestReplier::processPollArray(struct ::pollfd pl[], int nbfd)
  throw() {

  clog() << VERBOSE << "Processing poll Array "
         << pl[0].revents <<std::endl;

  if (pl[0].revents != 0) {
    readFromClientQueue();
  }

  for(unsigned int i=1; i < (unsigned int)nbfd; i++) {
    if (pl[i].revents != 0) {
      // Fetching the corresponding ClientConnection
      ClientConnection *cr = (*m_connections)[(pl[i].fd)];
      if (0==cr) {
        clog() << ERROR
               << "Could not look up Connection for fd : "
               << pl[i].fd << " (index:" << i << ")" << std::endl;
        continue;
      }

      clog() << DEBUG << "ClientConnection " << cr << " fd active : " << pl[i].fd
             << ", status : " << cr->getStatusStr()
             << " Events:" << pl[i].revents
             << std::endl;

      if (pl[i].revents & POLLHUP) {
        clog() << ERROR << pl[i].fd
               << "Connection dropped" << std::endl;
        cr->setStatus(DONE_FAILURE);
        cr->m_errorMessage = "Peer dropped the connection!";
      }

      if (pl[i].revents & POLLERR) {
        clog() << ERROR << pl[i].fd
               << "POLLERR received" << std::endl;
        cr->setStatus(DONE_FAILURE);
        cr->m_errorMessage = "Connection error";
      }

      switch (cr->getStatus()) {
      case CONNECTING:
        cr->setStatus(CONNECTED);
        clog() << DEBUG << "Connect successful for fd: "
               << pl[i].fd << std::endl;
      case CONNECTED:
        clog() << DEBUG << "Send successful for fd: "
               << pl[i].fd << " now sending next message" << std::endl;
        try {
          cr->sendData();
        } catch (EndConnectionException ex) {
          clog() << DEBUG
                 << "Closing connection"
                 << std::endl;
          cr->setStatus(CLOSE);
        } catch (NoMoreMessagesException ex) {
          clog() << DEBUG
                 << "No more messages to send, waiting"
                 << std::endl;
        } catch (castor::exception::Exception ex) {
          clog() << ERROR
                 << "Exception caught in sending data : "
                 << sstrerror(ex.code()) << std::endl
                 << ex.getMessage().str() << std::endl;
        }
        break;
      case RESEND:
        if (pl[i].revents & POLLOUT) {
          clog() << DEBUG << "CCCR Resending data for fd"
                 << pl[i].fd << std::endl;
          try {
            cr->sendData();
          } catch (castor::exception::Exception ex) {
            clog() << ERROR << "Exception caught in sending data : "
                   << sstrerror(ex.code()) << std::endl
                   << ex.getMessage().str() << std::endl;
          }
        } else if (pl[i].revents & POLLIN) {
          clog() << DEBUG << "CCCR Resending data for fd but got POLLIN"
                 << pl[i].fd << std::endl;
          try {
            cr->sendData();
          } catch (castor::exception::Exception ex) {
            clog() << ERROR << "Exception caught in sending data : "
                   << sstrerror(ex.code()) << std::endl
                   << ex.getMessage().str() << std::endl;
          }
        }
        break;
        //       case SENT:
        //         if (pl[i].revents & POLLIN) {
        //           clog() << DEBUG
        //                  << "CCCD POLLIN data sent successfully"
        //                  << pl[i].fd << std::endl;
        //           cr->setStatus(DONE_SUCCESS);
        //           deleteConnection(pl[i].fd);
        //         } else if (pl[i].revents & POLLOUT) {
        //           clog() << DEBUG
        //                  << "CCCD POLLOUT data sent successfully "
        //                  << pl[i].fd << std::endl;
        //           //cr->status = DONE_SUCCESS;
        //           //close(pl[i].fd);
        //         }
        //         break;
      default:
        clog() << ERROR << "Should not have status "
               << cr->getStatusStr() << " fd is " << pl[i].fd << std::endl;
      } // End switch
    } // End if revents != 0
  } // End for
} // end processPollArray


//-----------------------------------------------------------------------------
// Reads data off the client queue
//-----------------------------------------------------------------------------
void
castor::replier::RequestReplier::readFromClientQueue() throw() {

  int val;
  int rc = read(*m_pipeRead, &val, sizeof(val));
  if (rc < 0) {
    clog() << ERROR << "Error reading !" << std::endl;
  }

  clog() << VERBOSE << "Taking lock on queue !" << std::endl;
  Cthread_mutex_lock(&m_clientQueue);

  clog() << VERBOSE << "Getting client from queue" << std::endl;

  if (m_clientQueue->size() == 0) {
    clog() << VERBOSE << "No client in queue, removing lock" << std::endl;
    Cthread_mutex_unlock(&m_clientQueue);
    return; // XXX Throw exception ?
  }

  ClientResponse cr = m_clientQueue->front();
  m_clientQueue->pop();

  clog() << VERBOSE << "Client2 is "
         << castor::ip << cr.client.ipAddress() << ":"
         << cr.client.port() << std::endl;

  clog() << DEBUG << "Creating connection to client" << std::endl;
  createNewClientConnection(cr);

  clog() << VERBOSE << "Removing lock on queue !" << std::endl;
  Cthread_mutex_unlock(&m_clientQueue);

}


//-----------------------------------------------------------------------------
// Method to add an entry to the client list
//-----------------------------------------------------------------------------
void
castor::replier::RequestReplier::terminate()
  throw() {

  clog() << ALWAYS << "Requesting RequestReplier termination"
         << std::endl;

  // Setting the end processing flag to 1
  Cthread_mutex_lock(&m_terminate);
  m_terminate = 1;
  Cthread_mutex_unlock(&m_terminate);

  // Waiting for the RequestReplier thread to finish
  Cthread_join(m_threadId, NULL);

  // Deleting the singleton instance
  delete s_rr;
  s_rr = 0;
}

//////////////////////////////////////////////////////////////////////////////
//
// Code running in the server threads
//
//////////////////////////////////////////////////////////////////////////////

//-----------------------------------------------------------------------------
// Method to add an entry to the client list
//-----------------------------------------------------------------------------
void
castor::replier::RequestReplier::replyToClient(castor::IClient *client,
                                               castor::IObject *response,
                                               bool isLastResponse)
  throw(castor::exception::Exception) {

  char *func = "replyToClient: ";

  clog() << DEBUG << func << "ISLASTRESPONSE:" <<  isLastResponse << std::endl;

  // Marshalling the response
  castor::io::biniostream* buffer = new castor::io::biniostream();
  castor::io::StreamAddress ad(*buffer, "StreamCnvSvc", castor::SVC_STREAMCNV);
  svcs()->createRep(&ad, response, true);

  // Adding the client to the queue, taking proper lock
  clog() << func << VERBOSE << "Taking lock on queue !" << std::endl;
  Cthread_mutex_lock(&m_clientQueue);

  ClientResponse cr;
  // XXX To be fixed. IClient must have send and close methods and
  // XXX we should use them here
  castor::rh::Client* cl = dynamic_cast<castor::rh::Client*>(client);
  cr.client = *cl;
  cr.response = buffer;
  cr.isLast = isLastResponse;

  clog() << DEBUG << func << "Adding client to queue" << std::endl;
  m_clientQueue->push(cr);


  if (isLastResponse) {
    castor::rh::EndResponse endresp;
    castor::io::biniostream* buffer = new castor::io::biniostream();
    castor::io::StreamAddress ad(*buffer, "StreamCnvSvc", castor::SVC_STREAMCNV);
    svcs()->createRep(&ad, &endresp, true);

    ClientResponse cr;
    castor::rh::Client* cl = dynamic_cast<castor::rh::Client*>(client);
    cr.client = *cl;
    cr.response = buffer;
    cr.isLast = isLastResponse;

    clog() << DEBUG << func << "Adding End Response to queue" << std::endl;
    m_clientQueue->push(cr);

  }

  // Now notifying the replierThread
  clog() << VERBOSE << func << "Sending message to replierThread" << std::endl;
  int val = 1;
  int rc = write(*m_pipeWrite, (void *)&val, sizeof(val));
  if (rc != sizeof(val)) {
    clog() << ERROR << func << "Error writing to communication pipe with RRThread" << std::endl;
  } else {
    clog() << DEBUG 
	   << func 
	   <<"Successfully written to communication pipe with RRThread" 
	   << std::endl;
  }

  // In case of the last response, notify that and end response has been added 
  if (isLastResponse) {
    int rc = write(*m_pipeWrite, (void *)&val, sizeof(val));
    if (rc != sizeof(val)) {
      clog() << ERROR << func 
	     << "Error written EndResponse to communication pipe with RRThread" 
	     << std::endl;
    } else {
      clog() << DEBUG 
	     << func 
	     <<"Successfully written EndResponse to communication pipe with RRThread" 
	     << std::endl;
    }
  }

  // Exiting...
  clog() << VERBOSE << func 
	 << "Removing lock on queue !" << std::endl;
  Cthread_mutex_unlock(&m_clientQueue);
}
