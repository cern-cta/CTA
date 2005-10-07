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
 * @(#)$RCSfile: RequestReplier.cpp,v $ $Revision: 1.21 $ $Release$ $Date: 2005/10/07 15:41:44 $ $Author: sponcec3 $
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
#include "castor/exception/Internal.hpp"
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
#include <time.h>

#include <cstring>
#include <string>
#include <stack>
#include <iomanip>
#include <sstream>

#define WD 30
#define SETW std::setw(WD) << std::left <<

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
//`
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

  char *func = "rr::getInstance";
  // XXX lock ?
  if (0 == s_rr) {
    s_rr = new castor::replier::RequestReplier();
    s_rr->clog() << IMPORTANT << SETW func  <<  "Created new request replier" << std::endl;
  }
  return s_rr;
}

//-----------------------------------------------------------------------------
// constructor
//-----------------------------------------------------------------------------
castor::replier::RequestReplier::RequestReplier() throw() {

  char *func = "rr::RequestReplier";

  // Initializing collections
  m_clientQueue = new std::queue<ClientResponse>();
  m_connections = new std::map<int, ClientConnection *>();

  // Making sure the callback is null
  m_connectionStatusCallback = 0;

  // Establishing the pipe used for notification with ReplierThread
  int rc = pipe(m_commPipe);
  if (-1 == rc) {
    clog() << ERROR << SETW func  <<  "Could not establish communication pipe !" << std::endl;
  }
  m_pipeRead = &m_commPipe[0];
  m_pipeWrite = &m_commPipe[1];

  // Setting the end processing flag to zero
  m_terminate = 0;

  // Setting the statistics values
  m_lastStatTime = 0;
  m_nbQueuedResponses =0;
  m_nbDequeuedResponses =0;
  // Starting the ReplierThread
  start();
}

//-----------------------------------------------------------------------------
// Routine that starts the replier thread processing
//-----------------------------------------------------------------------------
void
castor::replier::RequestReplier::start() throw() {
  char *func = "rr::start ";
  if ((m_threadId = Cthread_create(&(staticReplierThread), this))<0) {
    clog() << ALERT << SETW func  <<  "Could not create thread !" << std::endl;
  }
  clog() << DEBUG << SETW func  <<  "Request Replier thread ID is: " << m_threadId << std::endl;

}

//-----------------------------------------------------------------------------
// Static method called by Cthread
//-----------------------------------------------------------------------------
void *
castor::replier::RequestReplier::staticReplierThread(void *arg) throw() {
  if (0 == arg) {
    // XXX Commented out because clog no longer a static method !
    //clog() << ERROR << SETW func  <<  "Please specify an argument to staticReplierThread" << std::endl;
    return 0;
  }

  castor::replier::RequestReplier *s_rr = static_cast<castor::replier::RequestReplier *>(arg);
  if (0 == s_rr) {
    // XXX Commented out because clog no longer a static method !
    // clog() << ERROR << SETW func  <<  "Error in dynamic cast" << std::endl;
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

  char *func = "rr::replierThread ";
  m_lastStatTime = time(0);

  while (1) {


    // Display number of messages received/sent statistics !
    time_t curtime = time(0);
    if ((curtime - m_lastStatTime) >= 60) {
      clog() << IMPORTANT << SETW func
             << " STATISTICS During last " << (curtime - m_lastStatTime)
             << " s New Requests:" << m_nbQueuedResponses
             << " Processed:" <<  m_nbDequeuedResponses << std::endl;
      m_nbQueuedResponses = 0;
      m_nbDequeuedResponses = 0;
      m_lastStatTime = curtime;
    }


    // Getting the value of the end processing flag
    Cthread_mutex_lock(&m_terminate);
    int terminate = m_terminate;
    Cthread_mutex_unlock(&m_terminate);

    if (terminate) {
      if(m_connections->size() == 0
         && m_clientQueue->size() == 0) {
        clog() << IMPORTANT << SETW func  <<  "Finished processing - Terminating"
               << std::endl;
        break;
      } else {
        clog() << IMPORTANT << SETW func  <<  "Waiting to terminate - ConnQ:"
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

    clog() << VERBOSE << SETW func
           <<  "Polling, number of file descriptors:" << nbfd << std::endl;

    pollRc = poll(toPoll, nbfd, pollTimeout * 1000);

    clog() << VERBOSE << SETW func
           <<  "Poll returned " << pollRc << std::endl;


    if (pollRc == 0) {
      // POLL TIMED OUT

      clog() << DEBUG << SETW func
             <<  "Poll timed out" << std::endl;

      // Poll timed-out grabage collect to free resources
      garbageCollect();
      continue;
    } else if (pollRc < 0) {

      // ERROR IN POLL
      if (errno == EINTR) {
        clog() << VERBOSE << SETW func
               <<  "Poll interrupted, continuing"
               << std::endl;
        continue;
      } else {
        clog() << ERROR << SETW func
               <<  "Error in poll:"
               << strerror(errno)
               << std::endl;
        continue;
      }
    } // End if pollrc < 0


    // POLL RETURNED > 0
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

  char *func = "rr::createNewClientConnection ";

  // Looking in the hash of client to find if there is one already established
  ClientConnection *r = 0;
  const int newhost = cr.client.ipAddress();
  const int newport = cr.client.port();

  for(std::map<int, ClientConnection *>::iterator iter = m_connections->begin();
      iter != m_connections->end();
      iter++) {

    castor::rh::Client c = (*iter).second->client();
    if (newport == c.port()
        && newhost == c.ipAddress()) {
      // Found an existing connection !
      // XXX Should there be status testing ?
      clog() << DEBUG << SETW func  <<  "Found existing connection "
             << (*iter).second->toString() << std::endl;
      r = (*iter).second;
      break;
    }
  }

  // Create new connection in case none was found !
  if (0 == r) {
    r = new ClientConnection(cr);

    try {
      r->createSocket();
      r->connect();
      (*m_connections)[r->fd()] = r;
      clog() << VERBOSE << SETW func  <<  "ClientConnection " << r << " added fd is "
             << r->fd() << std::endl;
    } catch(castor::exception::Exception e) {
      clog() << ERROR
             << "Exeption while Creating new ClientConnection: "
             << e.getMessage().str() << std::endl;
      r->setStatus(DONE_FAILURE);
      r->setErrorMessage("Could not connect:" + e.getMessage().str());
    }
  } else {
    // Add the new connection to the list in case one was found
    r->addMessage(cr);
  }
}


//-----------------------------------------------------------------------------
// Create connection socket
//-----------------------------------------------------------------------------
void castor::replier::RequestReplier::deleteConnection(int dfd) throw() {

  char *func = "rr::deleteConnection ";

  ClientConnection *cr = (*m_connections)[dfd];
  clog() << VERBOSE << SETW func  <<  cr->toString()
         << " being deleted."
         << std::endl;

  if (0 != cr) {
    cr->close();
    m_connections->erase(dfd);
    delete cr;
  }

}


//-----------------------------------------------------------------------------
// Cleans old entry from the connections map
//-----------------------------------------------------------------------------
void castor::replier::RequestReplier::garbageCollect() throw() {

  int t = time(0);
  const int TIMEOUT = 60;
  std::stack<int> toremove;
  char *func = "rr::garbageCollect ";

  for(std::map<int, ClientConnection *>::iterator iter = m_connections->begin();
      iter != m_connections->end();
      iter++) {

    ClientConnection *cc = (*iter).second;
    castor::rh::Client client = cc->client();

    clog() << VERBOSE << SETW func  <<  cc->toString() << " GC Check  active time: "
           << (t - cc->lastEventDate())
           << std::endl;

    if ((*iter).second->getStatus() == DONE_FAILURE) {
      // This connection was a failure
      toremove.push(cc->fd());
      unsigned long ip = cc->client().ipAddress();
      clog() << DEBUG << SETW func  <<  cc->toString() << " in DONE_FAILURE - to remove" << std::endl;
      if (m_connectionStatusCallback) {
        m_connectionStatusCallback(&client, MCS_FAILURE);
      }

    } else if (cc->terminate() == true
               &&  !(cc->hasMessagesToSend()) ) {

      // Terminating requestreplier
      toremove.push(cc->fd());
      unsigned long ip = cc->client().ipAddress();
      clog() << DEBUG << SETW func  <<  cc->toString()
             << " terminate:true and no more messages - to remove" << std::endl;

      if (m_connectionStatusCallback) {
        m_connectionStatusCallback(&client, MCS_SUCCESS);
      }

    } else if ((t - cc->lastEventDate()) > TIMEOUT) {

      // Terminating requestreplier
      toremove.push(cc->fd());
      unsigned long ip = cc->client().ipAddress();
      clog() << DEBUG << SETW func  <<  cc->toString()
             << " inactive for " << (t - cc->lastEventDate()) << " s >" << TIMEOUT
             << std::endl;

      if (m_connectionStatusCallback) {
        m_connectionStatusCallback(&client, MCS_TIMEOUT);
      }
    }
  } // End for

  while (!toremove.empty()) {
    int tfd = toremove.top();
    deleteConnection(tfd);
    toremove.pop();
  } // End while
}


//-----------------------------------------------------------------------------
// Build the array that should be polled
//-----------------------------------------------------------------------------
int
castor::replier::RequestReplier::buildNewPollArray(struct ::pollfd pl[])
  throw() {

  char *func = "rr::buildNewPollArray ";

  // BEWARE: Keep entry 0 identical as this the pollfd for the messaging pipe
  pl[0].fd = *m_pipeRead;
  pl[0].events = POLLIN;
  pl[0].revents = 0;

  int i =1; // Count in the array starting at second file descriptor
  for(std::map<int, ClientConnection *>::iterator iter = m_connections->begin();
      iter != m_connections->end();
      iter++) {

    ClientConnection *cc = (*iter).second;

    if (cc->getStatus() == CONNECTING
        || cc->getStatus() == RESEND
        || (cc->getStatus() == CONNECTED
            && cc->hasMessagesToSend()) ) {
      pl[i].fd = (*iter).first;
      pl[i].events = POLLIN|POLLOUT|POLLHUP|POLLERR;
      pl[i].revents = 0;
      i++;
    }

    //     if (cc->getStatus() == SENT) {
    //       clog() << VERBOSE << SETW func  <<  "Listening to fd: "
    //              << (*iter).first << std::endl;
    //       pl[i].fd = (*iter).first;
    //       pl[i].events = POLLIN|POLLHUP|POLLERR;
    //       pl[i].revents = 0;
    //       i++;
    //     }

  }

  clog() << DEBUG << SETW func  <<  "There are " << (i-1)
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

  char *func = "rr::processPollArray ";

  clog() << VERBOSE << SETW func  <<  "Processing poll Array. (New requests waiting?: "
         << pl[0].revents << ")" << std::endl;

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

      clog() << DEBUG << SETW func  <<  cr->toString()
             << " fd active : " << pl[i].fd
             << " Events:" << pl[i].revents << "("
             <<  pollStr(pl[i].revents)  << ")"
             << std::endl;

      if (pl[i].revents & POLLHUP) {
        clog() << ERROR << SETW func  <<  cr->toString()
               << " Connection dropped " << pl[i].fd << std::endl;
        cr->setStatus(DONE_FAILURE);
        cr->setErrorMessage("Peer dropped the connection!");
      }

      if (pl[i].revents & POLLERR) {
        clog() << ERROR << SETW func  <<  cr->toString()
               << " POLLERR received " << pl[i].fd << std::endl;
        cr->setStatus(DONE_FAILURE);
        cr->setErrorMessage("Connection error");
      }

      switch (cr->getStatus()) {
      case CONNECTING:
        cr->setStatus(CONNECTED);
        break; /* XXX Was missing !*/
      case CONNECTED:
        try {
          cr->sendNextMessage();
        } catch (EndConnectionException ex) {
          clog() << ERROR  << cr->toString()
                 << " got EndConnectionException closing connection"
                 << std::endl;
          cr->setStatus(CLOSE);
        } catch (NoMoreMessagesException ex) {
          clog() << DEBUG << cr->toString()
                 << "No more messages to send, waiting"
                 << sstrerror(ex.code())
                 << ex.getMessage().str() << std::endl;
        } catch (castor::exception::Exception ex) {
          clog() << ERROR << cr->toString()
                 << "Exception caught in sending data : "
                 << sstrerror(ex.code())
                 << ex.getMessage().str() << std::endl;
        }

        // Increasing statistics counter
        m_nbDequeuedResponses++;
        break;

      case RESEND:
        if (pl[i].revents & POLLOUT) {
          clog() << DEBUG << SETW func  <<  cr->toString()
                 << " Resending data POLLOUT "
                 << pl[i].fd << std::endl;
          try {
            cr->sendNextMessage();
          } catch (castor::exception::Exception ex) {
            clog() << ERROR << SETW func  <<  cr->toString() << "Exception caught in sending data : "
                   << sstrerror(ex.code()) << std::endl
                   << ex.getMessage().str() << std::endl;
          }
        } else if (pl[i].revents & POLLIN) {
          clog() << DEBUG << SETW func  <<  cr->toString()
                 <<"CCCR Resending data POLLIN "
                 << pl[i].fd << std::endl;
          try {
            cr->sendNextMessage();
          } catch (castor::exception::Exception ex) {
            clog() << ERROR << SETW func  <<  "Exception caught in sending data : "
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
        clog() << ERROR << SETW func  <<  cr->toString() <<  "Should not have status "
               << cr->getStatusStr() << " " << pl[i].fd << std::endl;
      } // End switch
    } // End if revents != 0
  } // End for
} // end processPollArray


//-----------------------------------------------------------------------------
// Reads data off the client queue
//-----------------------------------------------------------------------------
void
castor::replier::RequestReplier::readFromClientQueue() throw() {

  char *func = "rr::readFromClientQueue ";

  clog() << VERBOSE << SETW func  <<  "Locking m_clientQueue" << std::endl;
  Cthread_mutex_lock(&m_clientQueue);

  clog() << DEBUG << SETW func
         << "*** Before processing - Client Queue size:"  << m_clientQueue->size()
         << " Connection Queue size:"  << m_connections->size()
         << std::endl;

  if (m_clientQueue->size() == 0) {
    clog() << DEBUG << SETW func  <<  "No client in queue, removing lock"
           << std::endl;
    Cthread_mutex_unlock(&m_clientQueue);
    return;
  }

  int clients_in_queue = m_clientQueue->size();

  for(int i=0; i<clients_in_queue; i++) {

    // Increasing the statistics counter
    m_nbQueuedResponses++;


    ClientResponse cr = m_clientQueue->front();
    m_clientQueue->pop();

    this->createNewClientConnection(cr);

    int val;
    int rc = read(*m_pipeRead, &val, sizeof(val));
    if (rc < 0) {
      clog() << ERROR << SETW func  <<  "Error reading !" << std::endl;
    }

  }
  clog() << VERBOSE << SETW func
         << "*** After processing - Client Queue size:"  << m_clientQueue->size()
         << " Connection Queue size:"  << m_connections->size()
         << std::endl;



  clog() << VERBOSE << SETW func  <<  "Unlocking m_clientQueue" << std::endl;
  Cthread_mutex_unlock(&m_clientQueue);

}


//-----------------------------------------------------------------------------
// Method to add an entry to the client list
//-----------------------------------------------------------------------------
void
castor::replier::RequestReplier::terminate()
  throw() {

  char *func = "rr::terminate ";

  clog() << IMPORTANT << SETW func  <<  "Requesting RequestReplier termination"
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

//-----------------------------------------------------------------------------
// Utility functions
//-----------------------------------------------------------------------------
std::string
castor::replier::RequestReplier::pollStr(int val) {

  std::ostringstream sst;
  bool found = false;

  if (val & POLLIN) {
    sst << "POLLIN";
    found = true;
  } else if (val & POLLPRI) {
    if (found) sst << "|";
    sst << "POLLPRI";
    found = true;
  } else   if (val & POLLOUT) {
    if (found) sst << "|";
    sst << "POLLOUT";
    found = true;
  } else   if (val & POLLERR) {
    if (found) sst << "|";
    sst << "POLLERR";
    found = true;
  } else   if (val & POLLHUP) {
    if (found) sst << "|";
    sst << "POLLHUP";
    found = true;
  } else   if (val & POLLNVAL) {
    if (found) sst << "|";
    sst << "POLLNVAL";
    found = true;
  }

  return sst.str();
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
  sendResponse(client, response, isLastResponse);
}


void
castor::replier::RequestReplier::setCallback(void (*callback)(castor::IClient *, MajorConnectionStatus))
  throw(castor::exception::Exception) {

  char *func = "rr::setCallback    CLIENT ";

  if (0 == callback) {
    castor::exception::Exception e(EINVAL);
    e.getMessage() << "Callback is NULL";
    throw e;
  }

  // Setting the callback function, taking proper lock
  clog() << VERBOSE << SETW func  <<  "Locking m_clientQueue" << std::endl;
  Cthread_mutex_lock(&m_clientQueue);

  m_connectionStatusCallback = callback;

  // Exiting...
  clog() << VERBOSE << SETW func
         << "Unlocking m_clientQueue" << std::endl;
  Cthread_mutex_unlock(&m_clientQueue);


}



void
castor::replier::RequestReplier::sendResponse(castor::IClient *client,
                                              castor::IObject *response,
                                              bool isLastResponse)
  throw(castor::exception::Exception) {

  char *func = "rr::sendResponse    CLIENT ";

  if (0 == client || 0 == response) {
    castor::exception::Exception e(EINVAL);
    e.getMessage() << "Parameter to sendResponse is NULL"
                   << client << "/"
                   << response;
    throw e;
  }

  // Marshalling the response
  castor::io::biniostream* buffer = new castor::io::biniostream();
  castor::io::StreamAddress ad(*buffer, "StreamCnvSvc", castor::SVC_STREAMCNV);
  svcs()->createRep(&ad, response, true);

  // Adding the client to the queue, taking proper lock
  clog() << VERBOSE << SETW func  <<  "Locking m_clientQueue" << std::endl;
  Cthread_mutex_lock(&m_clientQueue);

  ClientResponse cr;
  castor::rh::Client* cl = dynamic_cast<castor::rh::Client*>(client);
  if (0 == cl) {
    castor::exception::Internal e;
    e.getMessage() <<"Could not cast IClient to client";
    Cthread_mutex_unlock(&m_clientQueue);
    throw e;
  }

  cr.client = *cl;
  cr.response = buffer;
  cr.isLast = isLastResponse;


  clog() << SYSTEM << SETW func
         << "Adding Response for ";
  clog() << castor::ip << cr.client.ipAddress() << ":"
         << cr.client.port()
         << " to m_ClientQueue"
         << std::endl;
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

    std::ostringstream buff;

    clog() << DEBUG << SETW func
           << "Adding EndResponse for ";
    clog() << castor::ip << cr.client.ipAddress() << ":"
           << cr.client.port()
           << " to m_ClientQueue"
           << std::endl;
    m_clientQueue->push(cr);

  }

  // Now notifying the replierThread
  int val = 1;
  int rc = write(*m_pipeWrite, (void *)&val, sizeof(val));
  if (rc != sizeof(val)) {
    clog() << ERROR << SETW func  <<  "Error writing to communication pipe with RRThread" << std::endl;
  }

  // In case of the last response, notify that and end response has been added
  if (isLastResponse) {
    int rc = write(*m_pipeWrite, (void *)&val, sizeof(val));
    if (rc != sizeof(val)) {
      clog() << ERROR << SETW func
             << "Error writing to communication pipe with RRThread"
             << std::endl;
    }
  }

  // Exiting...
  clog() << VERBOSE << SETW func
         << "Unlocking m_clientQueue" << std::endl;
  Cthread_mutex_unlock(&m_clientQueue);
}


//-----------------------------------------------------------------------------
// Method to add an EndResponse
//-----------------------------------------------------------------------------
void
castor::replier::RequestReplier::sendEndResponse(castor::IClient *client)
  throw(castor::exception::Exception) {

  char *func = "rr::sendEndResponse CLIENT ";

  if (0 == client) {
    castor::exception::Exception e(EINVAL);
    e.getMessage() << "Client parameter to sendResponse is NULL";
    throw e;
  }

  // Adding the client to the queue, taking proper lock
  clog() << VERBOSE << SETW func  <<  "Locking m_clientQueue" << std::endl;
  Cthread_mutex_lock(&m_clientQueue);

  castor::rh::EndResponse endresp;
  castor::io::biniostream* buffer = new castor::io::biniostream();
  castor::io::StreamAddress ad(*buffer, "StreamCnvSvc", castor::SVC_STREAMCNV);
  svcs()->createRep(&ad, &endresp, true);

  ClientResponse cr;
  castor::rh::Client* cl = dynamic_cast<castor::rh::Client*>(client);
  cr.client = *cl;
  cr.response = buffer;
  cr.isLast = true;

  clog() << DEBUG << SETW func
         << "Adding EndResponse for ";
  clog() << castor::ip << cr.client.ipAddress() << ":"
         << cr.client.port()
         << " to m_ClientQueue" << std::endl;
  m_clientQueue->push(cr);

  int val = 1;
  int rc = write(*m_pipeWrite, (void *)&val, sizeof(val));
  if (rc != sizeof(val)) {
    clog() << ERROR << SETW func
           << "Error writing to communication pipe with RRThread"
           << std::endl;
  }

  // Exiting...
  clog() << VERBOSE << SETW func
         << "Unlocking m_clientQueue" << std::endl;
  Cthread_mutex_unlock(&m_clientQueue);
}

