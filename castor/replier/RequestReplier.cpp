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
 * @(#)RequestReplier.cpp,v 1.23 $Release$ 2005/10/11 14:14:02 bcouturi
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
  }
  return s_rr;
}

//-----------------------------------------------------------------------------
// constructor
//-----------------------------------------------------------------------------
castor::replier::RequestReplier::RequestReplier() throw() {

  const char *func = "rr::RequestReplier";

  // Initializing collections
  m_clientQueue = new std::queue<ClientResponse>();
  m_connections = new std::map<int, ClientConnection *>();

  // Making sure the callback is null
  m_connectionStatusCallback = 0;

  // Establishing the pipe used for notification with ReplierThread
  int rc = pipe(m_commPipe);
  if (-1 == rc) {
    // "RR: Could not establish communication pipe"
    castor::dlf::Param params[] =
      {castor::dlf::Param("Function", func)};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ALERT,
                            DLF_BASE_STAGERLIB + 8, 1, params);
  }
  m_pipeRead = &m_commPipe[0];
  m_pipeWrite = &m_commPipe[1];

  // Setting the end processing flag to zero
  m_terminate = 0;

  // Setting the statistics values
  m_lastStatTime = 0;
  m_nbQueuedResponses = 0;
  m_nbDequeuedResponses = 0;

  // Starting the ReplierThread
  start();
}

//-----------------------------------------------------------------------------
// Routine that starts the replier thread processing
//-----------------------------------------------------------------------------
void castor::replier::RequestReplier::start() throw() {
  const char *func = "rr::start";
  if ((m_threadId = Cthread_create(&(staticReplierThread), this))<0) {
    // "RR: Could not create thread"
    castor::dlf::Param params[] =
      {castor::dlf::Param("Function", func)};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ALERT,
                            DLF_BASE_STAGERLIB + 9, 1, params);
  }
  // "RR: Request Replier thread started"
  castor::dlf::Param params[] =
    {castor::dlf::Param("Function", func),
     castor::dlf::Param("ThreadID", m_threadId)};
  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG,
                          DLF_BASE_STAGERLIB + 10, 2, params);
}

//-----------------------------------------------------------------------------
// Static method called by Cthread
//-----------------------------------------------------------------------------
void *
castor::replier::RequestReplier::staticReplierThread(void *arg) throw() {
  if (0 == arg) {
    return 0;
  }

  castor::replier::RequestReplier *s_rr = static_cast<castor::replier::RequestReplier *>(arg);

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
castor::replier::RequestReplier::replierThread(void*) throw() {

  int pollRc;
  const int pollTimeout = 5;
  unsigned int nbPollFd = 1000;
  struct ::pollfd *toPoll = new struct ::pollfd[nbPollFd];

  const char *func = "rr::replierThread";
  m_lastStatTime = time(0);

  while (1) {

    // Display number of messages received/sent statistics !
    time_t curtime = time(0);
    if ((curtime - m_lastStatTime) >= 600) {
      // "RR: Request Replier statistics"
      castor::dlf::Param params[] =
        {castor::dlf::Param("Interval", curtime - m_lastStatTime),
         castor::dlf::Param("NewRequests", m_nbQueuedResponses),
         castor::dlf::Param("ProcessedRequests", m_nbDequeuedResponses)};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM,
                              DLF_BASE_STAGERLIB + 11, 3, params);
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
        // "RR: Finished processing - terminating"
        castor::dlf::Param params[] =
          {castor::dlf::Param("Function", func)};
        castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG,
                                DLF_BASE_STAGERLIB + 12, 1, params);
        break;
      } else {
        // "RR: Waiting to terminate connection queue"
        castor::dlf::Param params[] =
          {castor::dlf::Param("Function", func),
           castor::dlf::Param("ConnQSize", m_connections->size()),
           castor::dlf::Param("ClientQSize", m_clientQueue->size())};
        castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG,
                                DLF_BASE_STAGERLIB + 13, 3, params);
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

    // "RR: Polling file descriptors"
    castor::dlf::Param params2[] =
      {castor::dlf::Param("Function", func),
       castor::dlf::Param("NbFDs", nbfd)};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG,
                            DLF_BASE_STAGERLIB + 14, 2, params2);

    pollRc = poll(toPoll, nbfd, pollTimeout * 1000);
    if (pollRc == 0) {
      // POLL TIMED OUT

      // "RR: Poll returned - timed out"
      castor::dlf::Param params[] =
        {castor::dlf::Param("Function", func)};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG,
                              DLF_BASE_STAGERLIB + 15, 1, params);

      // Poll timed-out grabage collect to free resources
      garbageCollect();
      continue;
    } else if (pollRc < 0) {

      // ERROR IN POLL
      if (errno == EINTR) {

        // "RR: Poll returned - interrupted, continuing"
        castor::dlf::Param params[] =
          {castor::dlf::Param("Function", func)};
        castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG,
                              DLF_BASE_STAGERLIB + 16, 1, params);
        continue;
      } else {

        // "RR: Error in poll"
        castor::dlf::Param params[] =
          {castor::dlf::Param("Function", func),
           castor::dlf::Param("Error", sstrerror(errno))};
        castor::dlf::dlf_writep(nullCuuid, DLF_LVL_WARNING,
                              DLF_BASE_STAGERLIB + 17, 2, params);
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
void castor::replier::RequestReplier::createNewClientConnection
(ClientResponse cr)
  throw() {

  const char *func = "rr::createNewClientConnection";

  // Looking in the hash of client to find if there is one already established
  ClientConnection *r = 0;
  const unsigned long newhost = cr.client.ipAddress();
  const unsigned short newport = cr.client.port();

  for(std::map<int, ClientConnection *>::iterator iter = m_connections->begin();
      iter != m_connections->end();
      iter++) {

    castor::rh::Client c = (*iter).second->client();
    if (newport == c.port()
        && newhost == c.ipAddress()) {
      // Found an existing connection !
      // XXX Should there be status testing ?

      // "RR: Found existing connection"
      castor::dlf::Param params[] =
        {castor::dlf::Param("Function", func),
         castor::dlf::Param("ClientInfo", (*iter).second->toString())};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG,
                              DLF_BASE_STAGERLIB + 18, 2, params);
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

      // "RR: Added FD to client connection"
      castor::dlf::Param params[] =
        {castor::dlf::Param("Function", func),
         castor::dlf::Param("ClientInfo", r->toString()),
         castor::dlf::Param("FD", r->fd())};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG,
                              DLF_BASE_STAGERLIB + 19, 3, params);

    } catch(castor::exception::Exception& e) {

      // "RR: Exception while Creating new ClientConnection"
      castor::dlf::Param params[] =
        {castor::dlf::Param("Function", func),
         castor::dlf::Param("Message", e.getMessage().str())};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_WARNING,
                              DLF_BASE_STAGERLIB + 20, 2, params);

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

  const char *func = "rr::deleteConnection";

  ClientConnection *cr = (*m_connections)[dfd];

  // "RR: Deleting connection"
  castor::dlf::Param params[] =
    {castor::dlf::Param("Function", func),
     castor::dlf::Param("ClientInfo", cr->toString())};
  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG,
                          DLF_BASE_STAGERLIB + 21, 2, params);

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
  const int TIMEOUT = 10;
  std::stack<int> toremove;
  const char *func = "rr::garbageCollect";

  for(std::map<int, ClientConnection *>::iterator iter = m_connections->begin();
      iter != m_connections->end();
      iter++) {

    ClientConnection *cc = (*iter).second;
    castor::rh::Client client = cc->client();

    // "RR: GC Check active time"
    castor::dlf::Param params[] =
      {castor::dlf::Param("Function", func),
       castor::dlf::Param("ClientInfo", cc->toString()),
       castor::dlf::Param("Active", t - cc->lastEventDate())};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG,
                            DLF_BASE_STAGERLIB + 22, 3, params);

    if ((*iter).second->getStatus() == DONE_FAILURE) {
      // This connection was a failure
      toremove.push(cc->fd());

      // "RR: Cleanup client connection - in DONE_FAILURE - to remove"
      castor::dlf::Param params[] =
        {castor::dlf::Param("Function", func),
         castor::dlf::Param("ClientInfo", cc->toString())};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG,
                              DLF_BASE_STAGERLIB + 23, 2, params);

      if (m_connectionStatusCallback) {
        m_connectionStatusCallback(&client, MCS_FAILURE);
      }

    } else if (cc->terminate() == true
               && !(cc->hasMessagesToSend()) ) {

      // Terminating requestreplier
      toremove.push(cc->fd());

      // "RR: Cleanup client connection - terminate is true and no more
      //  messages - to remove"
      castor::dlf::Param params[] =
        {castor::dlf::Param("Function", func),
         castor::dlf::Param("ClientInfo", cc->toString())};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG,
                              DLF_BASE_STAGERLIB + 24, 2, params);

      if (m_connectionStatusCallback) {
        m_connectionStatusCallback(&client, MCS_SUCCESS);
      }

    } else if ((t - cc->lastEventDate()) > TIMEOUT) {

      // Terminating requestreplier
      toremove.push(cc->fd());

      // "RR: Cleanup client connection - timeout"
      castor::dlf::Param params[] =
        {castor::dlf::Param("Function", func),
         castor::dlf::Param("ClientInfo", cc->toString()),
         castor::dlf::Param("InActive", t - cc->lastEventDate())};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG,
                              DLF_BASE_STAGERLIB + 25, 3, params);

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
  }

  // i has been increased by the ++ but that's ok as we have to add
  return i;
}

//-----------------------------------------------------------------------------
// Process poll array
//-----------------------------------------------------------------------------
void
castor::replier::RequestReplier::processPollArray(struct ::pollfd pl[], int nbfd)
  throw() {

  const char *func = "rr::processPollArray";

  // "RR: Processing poll Array"
  castor::dlf::Param params[] =
    {castor::dlf::Param("Function", func),
     castor::dlf::Param("NewRequestsWaiting", pl[0].revents)};
  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG,
                              DLF_BASE_STAGERLIB + 26, 2, params);

  if (pl[0].revents != 0) {
    readFromClientQueue();
  }

  for(unsigned int i = 1; i < (unsigned int)nbfd; i++) {
    if (pl[i].revents != 0) {
      // Fetching the corresponding ClientConnection
      ClientConnection *cr = (*m_connections)[(pl[i].fd)];
      if (0 == cr) {
        // "RR: Could not look up Connection for FD"
        castor::dlf::Param params[] =
          {castor::dlf::Param("Function", func),
           castor::dlf::Param("FD", pl[i].fd),
           castor::dlf::Param("Index", i)};
        castor::dlf::dlf_writep(nullCuuid, DLF_LVL_WARNING,
                              DLF_BASE_STAGERLIB + 27, 3, params);
        continue;
      }

      // "RR: Processing poll Array"
      castor::dlf::Param params[] =
        {castor::dlf::Param("Function", func),
         castor::dlf::Param("ClientInfo", cr->toString()),
         castor::dlf::Param("FD", pl[i].fd),
         castor::dlf::Param("Events", pollStr(pl[i].revents))};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG,
                              DLF_BASE_STAGERLIB + 26, 4, params);

      if (pl[i].revents & POLLHUP) {

        // "RR: Connection dropped"
        castor::dlf::Param params[] =
          {castor::dlf::Param("Function", func),
           castor::dlf::Param("ClientInfo", cr->toString()),
           castor::dlf::Param("FD", pl[i].fd)};
        castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG,
                                DLF_BASE_STAGERLIB + 28, 3, params);

        cr->setStatus(DONE_FAILURE);
        cr->setErrorMessage("Peer dropped the connection!");
        continue;
      }

      if (pl[i].revents & POLLERR) {

        // "RR: POLLERR received"
        castor::dlf::Param params[] =
          {castor::dlf::Param("Function", func),
           castor::dlf::Param("ClientInfo", cr->toString()),
           castor::dlf::Param("FD", pl[i].fd)};
        castor::dlf::dlf_writep(nullCuuid, DLF_LVL_WARNING,
                                DLF_BASE_STAGERLIB + 29, 3, params);
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

          // "RR: Got EndConnectionException closing connection"
          castor::dlf::Param params[] =
            {castor::dlf::Param("Function", func),
             castor::dlf::Param("ClientInfo", cr->toString())};
          castor::dlf::dlf_writep(nullCuuid, DLF_LVL_WARNING,
                                  DLF_BASE_STAGERLIB + 30, 2, params);
          cr->setStatus(CLOSE);
        } catch (NoMoreMessagesException ex) {

          // "RR: No more messages to send, waiting"
          castor::dlf::Param params[] =
            {castor::dlf::Param("Function", func),
             castor::dlf::Param("ClientInfo", cr->toString()),
             castor::dlf::Param("Error", sstrerror(ex.code())),
             castor::dlf::Param("Message", ex.getMessage().str())};
          castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG,
                                  DLF_BASE_STAGERLIB + 31, 4, params);
        } catch (castor::exception::Exception& ex) {

          // "RR: Exception caught in sending data"
          castor::dlf::Param params[] =
            {castor::dlf::Param("Function", func),
             castor::dlf::Param("ClientInfo", cr->toString()),
             castor::dlf::Param("Error", sstrerror(ex.code())),
             castor::dlf::Param("Message", ex.getMessage().str())};
          castor::dlf::dlf_writep(nullCuuid, DLF_LVL_WARNING,
                                  DLF_BASE_STAGERLIB + 32, 4, params);
        }

        // Increasing statistics counter
        m_nbDequeuedResponses++;
        break;

      case RESEND:
        if (pl[i].revents & POLLOUT) {

          // "RR: Resending data POLLOUT"
          castor::dlf::Param params[] =
            {castor::dlf::Param("Function", func),
             castor::dlf::Param("ClientInfo", cr->toString()),
             castor::dlf::Param("FD", pl[i].fd)};
          castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG,
                                  DLF_BASE_STAGERLIB + 33, 3, params);
          try {
            cr->sendNextMessage();
          } catch (castor::exception::Exception& ex) {

            // "RR: Exception caught in sending data"
            castor::dlf::Param params[] =
              {castor::dlf::Param("Function", func),
               castor::dlf::Param("ClientInfo", cr->toString()),
               castor::dlf::Param("Error", sstrerror(ex.code())),
               castor::dlf::Param("Message", ex.getMessage().str())};
            castor::dlf::dlf_writep(nullCuuid, DLF_LVL_WARNING,
                                    DLF_BASE_STAGERLIB + 32, 4, params);
          }
        } else if (pl[i].revents & POLLIN) {

          // "RR: CCCR Resending data POLLIN"
          castor::dlf::Param params[] =
            {castor::dlf::Param("Function", func),
             castor::dlf::Param("ClientInfo", cr->toString()),
             castor::dlf::Param("FD", pl[i].fd)};
          castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG,
                                  DLF_BASE_STAGERLIB + 34, 3, params);
          try {
            cr->sendNextMessage();
          } catch (castor::exception::Exception& ex) {

            // "RR: Exception caught in sending data"
            castor::dlf::Param params[] =
              {castor::dlf::Param("Function", func),
               castor::dlf::Param("ClientInfo", cr->toString()),
               castor::dlf::Param("Error", sstrerror(ex.code())),
               castor::dlf::Param("Message", ex.getMessage().str())};
            castor::dlf::dlf_writep(nullCuuid, DLF_LVL_WARNING,
                                    DLF_BASE_STAGERLIB + 32, 4, params);
          }
        }
        break;
      case DONE_FAILURE:
        break;
      default:
        {
          // "RR: Unknown status"
          castor::dlf::Param params[] =
            {castor::dlf::Param("Function", func),
             castor::dlf::Param("ClientInfo", cr->toString()),
             castor::dlf::Param("Status", cr->getStatusStr()),
             castor::dlf::Param("FD", pl[i].fd)};
          castor::dlf::dlf_writep(nullCuuid, DLF_LVL_WARNING,
                                  DLF_BASE_STAGERLIB + 35, 4, params);
        }
        break;
      } // End switch
    } // End if revents != 0
  } // End for
} // end processPollArray


//-----------------------------------------------------------------------------
// Reads data off the client queue
//-----------------------------------------------------------------------------
void
castor::replier::RequestReplier::readFromClientQueue() throw() {

  const char *func = "rr::readFromClientQueue";

  // "RR: Locking m_clientQueue"
  castor::dlf::Param params[] =
    {castor::dlf::Param("Function", func)};
  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG,
                          DLF_BASE_STAGERLIB + 36, 1, params);

  Cthread_mutex_lock(&m_clientQueue);

  // "RR: Info before processing"
  castor::dlf::Param params1[] =
    {castor::dlf::Param("Function", func),
     castor::dlf::Param("ClientQSize", m_clientQueue->size()),
     castor::dlf::Param("ConnQSize", m_connections->size())};
  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG,
                          DLF_BASE_STAGERLIB + 37, 3, params1);

  if (m_clientQueue->size() == 0) {

    // "RR: No client in queue, removing lock"
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG,
                            DLF_BASE_STAGERLIB + 38, 1, params);

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
      // "RR: Error reading"
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG,
                              DLF_BASE_STAGERLIB + 39, 1, params);
    }
  }

  // "RR: Info after processing"
  castor::dlf::Param params2[] =
    {castor::dlf::Param("Function", func),
     castor::dlf::Param("ClientQSize", m_clientQueue->size()),
     castor::dlf::Param("ConnQSize", m_connections->size())};
  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG,
                          DLF_BASE_STAGERLIB + 40, 3, params2);

  // "RR: Unlocking m_clientQueue"
  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG,
                          DLF_BASE_STAGERLIB + 41, 1, params);
  Cthread_mutex_unlock(&m_clientQueue);
}


//-----------------------------------------------------------------------------
// Method to add an entry to the client list
//-----------------------------------------------------------------------------
void
castor::replier::RequestReplier::terminate()
  throw() {

  const char *func = "rr::terminate";

  // "RR: Requesting RequestReplier termination"
  castor::dlf::Param params[] =
    {castor::dlf::Param("Function", func)};
  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM,
                          DLF_BASE_STAGERLIB + 42, 1, params);

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
  }
  if (val & POLLPRI) {
    if (found) sst << "|";
    sst << "POLLPRI";
    found = true;
  }
  if (val & POLLOUT) {
    if (found) sst << "|";
    sst << "POLLOUT";
    found = true;
  }
  if (val & POLLERR) {
    if (found) sst << "|";
    sst << "POLLERR";
    found = true;
  }
  if (val & POLLHUP) {
    if (found) sst << "|";
    sst << "POLLHUP";
    found = true;
  }
  if (val & POLLNVAL) {
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

  const char *func = "rr::setCallback CLIENT";

  if (0 == callback) {
    castor::exception::Exception e(EINVAL);
    e.getMessage() << "Callback is NULL";
    throw e;
  }

  // Setting the callback function, taking proper lock

  // "RR: Locking m_clientQueue"
  castor::dlf::Param params[] =
    {castor::dlf::Param("Function", func)};
  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG,
                          DLF_BASE_STAGERLIB + 36, 1, params);

  Cthread_mutex_lock(&m_clientQueue);

  m_connectionStatusCallback = callback;

  // Exiting...

  // "RR: Unlocking m_clientQueue"
  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG,
                          DLF_BASE_STAGERLIB + 41, 1, params);
  Cthread_mutex_unlock(&m_clientQueue);
}


void
castor::replier::RequestReplier::sendResponse(castor::IClient *client,
                                              castor::IObject *response,
                                              bool isLastResponse)
  throw(castor::exception::Exception) {

  const char *func = "rr::sendResponse CLIENT";

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

  ClientResponse cr;
  castor::rh::Client* cl = dynamic_cast<castor::rh::Client*>(client);
  if (0 == cl) {
    castor::exception::Internal e;
    e.getMessage() << "Could not cast IClient to client";
    Cthread_mutex_unlock(&m_clientQueue);
    throw e;
  }

  cr.client = *cl;
  cr.response = buffer;
  cr.isLast = isLastResponse;

  // Adding the client to the queue, taking proper lock

  // "RR: Locking m_clientQueue"
  castor::dlf::Param params[] =
    {castor::dlf::Param("Function", func)};
  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG,
                          DLF_BASE_STAGERLIB + 36, 1, params);

  Cthread_mutex_lock(&m_clientQueue);

  // "RR: Adding Response to m_ClientQueue"
  castor::dlf::Param params1[] =
    {castor::dlf::Param("Function", func),
     castor::dlf::Param("IPAddress", cr.client.ipAddress()),
     castor::dlf::Param("Port", cr.client.port())};
  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG,
                          DLF_BASE_STAGERLIB + 44, 3, params1);

  m_clientQueue->push(cr);

  if (isLastResponse && cl->version() < 2010707) {
    // clients >= 2.1.7-7 don't need an EndResponse
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

    // "RR: Adding EndResponse to m_ClientQueue"
    castor::dlf::Param params1[] =
      {castor::dlf::Param("Function", func),
       castor::dlf::Param("IPAddress", cr.client.ipAddress()),
       castor::dlf::Param("Port", cr.client.port())};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG,
                            DLF_BASE_STAGERLIB + 45, 3, params1);

    m_clientQueue->push(cr);
  }

  // "RR: Unlocking m_clientQueue"
  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG,
                          DLF_BASE_STAGERLIB + 41, 1, params);

  Cthread_mutex_unlock(&m_clientQueue); // I release the lock here because it is used only to manage the queue

  // Now notifying the replierThread
  int val = 1;
  int rc = write(*m_pipeWrite, (void *)&val, sizeof(val));
  if (rc != sizeof(val)) {
    // "RR: Error writing to communication pipe with RRThread"
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, DLF_BASE_STAGERLIB + 46);
  }

  // In case of the last response, notify that an end response has been added
  if (isLastResponse && cl->version() < 2010707) {
    int rc = write(*m_pipeWrite, (void *)&val, sizeof(val));
    if (rc != sizeof(val)) {
      // "RR: Error writing to communication pipe with RRThread"
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, DLF_BASE_STAGERLIB + 46);
    }
  }
}


//-----------------------------------------------------------------------------
// Method to add an EndResponse
//-----------------------------------------------------------------------------
void
castor::replier::RequestReplier::sendEndResponse
(castor::IClient *client,
 std::string reqId)
  throw(castor::exception::Exception) {

  const char *func = "rr::sendEndResponse CLIENT";

  if (0 == client) {
    castor::exception::Exception e(EINVAL);
    e.getMessage() << "Client parameter to sendResponse is NULL";
    throw e;
  }

  castor::rh::Client* cl = dynamic_cast<castor::rh::Client*>(client);

  ClientResponse cr;
  cr.client = *cl;
  cr.isLast = true;

  if(cl->version() < 2010707) {
    castor::rh::EndResponse endresp;
    endresp.setReqAssociated(reqId);
    castor::io::biniostream* buffer = new castor::io::biniostream();
    castor::io::StreamAddress ad(*buffer, "StreamCnvSvc", castor::SVC_STREAMCNV);
    svcs()->createRep(&ad, &endresp, true);
    cr.response = buffer;
  }
  else {
    // clients >= 2.1.7-7 don't need an EndResponse, but we nevertheless
    // create a ClientResponse with an empty buffer and flagged as 'last'
    // to close the connection with the client.
    // XXX this logic is to be reviewed, a better option is a bulk processing
    // XXX of each multi-file request by the stager.
    cr.response = 0;
  }

  // Adding the client to the queue, taking proper lock

  // "RR: Locking m_clientQueue"
  castor::dlf::Param params[] =
    {castor::dlf::Param("Function", func)};
  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG,
                          DLF_BASE_STAGERLIB + 36, 1, params);

  Cthread_mutex_lock(&m_clientQueue);

  // "RR: Adding EndResponse to m_ClientQueue"
  castor::dlf::Param params1[] =
    {castor::dlf::Param("Function", func),
     castor::dlf::Param("IPAddress", cr.client.ipAddress()),
     castor::dlf::Param("Port", cr.client.port())};
  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG,
                          DLF_BASE_STAGERLIB + 45, 3, params1);

  m_clientQueue->push(cr);

  // "RR: Unlocking m_clientQueue"
  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG,
                          DLF_BASE_STAGERLIB + 41, 1, params);

  Cthread_mutex_unlock(&m_clientQueue);   // as in sendResponse, the lock is released here

  int val = 1;
  int rc = write(*m_pipeWrite, (void *)&val, sizeof(val));
  if (rc != sizeof(val)) {
    // "RR: Error writing to communication pipe with RRThread"
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, DLF_BASE_STAGERLIB + 46);
  }
}

