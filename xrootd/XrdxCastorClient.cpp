/*******************************************************************************
 *
 * This file is part of the Castor project.
 * See http://castor.web.cern.ch/castor
 *
 * Copyright (C) 2012  CERN
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
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *
 ******************************************************************************/

/*-----------------------------------------------------------------------------*/
#include "XrdxCastorClient.hpp"
/*-----------------------------------------------------------------------------*/
#include "XrdSfs/XrdSfsInterface.hh"
/*-----------------------------------------------------------------------------*/
#include "castor/exception/Exception.hpp"
#include "castor/exception/Communication.hpp"
#include "castor/exception/InvalidArgument.hpp"
#include "castor/System.hpp"
#include "castor/IClient.hpp"
#include "castor/IObject.hpp"
#include "castor/MessageAck.hpp"
#include "castor/rh/Client.hpp"
#include "castor/rh/Response.hpp"
#include "castor/rh/Server.hpp"
#include "castor/io/ClientSocket.hpp"
#include "castor/io/ServerSocket.hpp"
#include "castor/Constants.hpp"
#include "castor/client/BaseClient.hpp"
/*-----------------------------------------------------------------------------*/
#include "getconfent.h"
#include "serrno.h"
#include <string>
/*-----------------------------------------------------------------------------*/

XCASTORNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
XrdxCastorClient::XrdxCastorClient():
  LogId(),
  mNfds(0),
  mDoStop(false),
  mIsZombie(false)
{
  mCallbackSocket = new castor::io::ServerSocket(true);
  // Get the port range to be used
  int low_port  = castor::client::LOW_CLIENT_PORT_RANGE;
  int high_port = castor::client::HIGH_CLIENT_PORT_RANGE;
  char* sport;

  if ((sport = getconfent((char*)castor::client::CLIENT_CONF,
                          (char*)castor::client::LOWPORT_CONF, 0)) != 0)
  {
    low_port = castor::System::porttoi(sport);
  }

  if ((sport = getconfent((char*)castor::client::CLIENT_CONF,
                          (char*)castor::client::HIGHPORT_CONF, 0)) != 0)
  {
    high_port = castor::System::porttoi(sport);
  }

  // Set the socket to non blocking
  int rc;
  int nonblocking = 1;
  rc = ioctl(mCallbackSocket->socket(), FIONBIO, &nonblocking);

  if (rc == -1)
  {
    castor::exception::InvalidArgument e;
    e.getMessage() << "Could not set socket asynchronous";
    throw e;
  }

  // Bind the socket
  mCallbackSocket->bind(low_port, high_port);
  mCallbackSocket->listen();

  // Start the poller thread
  if ((rc = XrdSysThread::Run(&mPollerTid, XrdxCastorClient::StartPollerThread,
                              static_cast<void*>(this), 0, "Poller Thread")))
  {
    fprintf(stderr, "cannot start poller thread");
    mIsZombie = true;
  }
}


//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
XrdxCastorClient::~XrdxCastorClient() throw()
{
  mDoStop = true;

  // Join the poller thread
  void* ret;
  int rc = XrdSysThread::Join(mPollerTid, &ret);
  xcastor_info("poller thread join rc=%i", rc);

  if (0 != mCallbackSocket)
    delete mCallbackSocket;
}


//------------------------------------------------------------------------------
// Get new instance while also checking that the poller thread was started
// successfully
//------------------------------------------------------------------------------
XrdxCastorClient*
XrdxCastorClient::Create()
{
  XrdxCastorClient* client = new XrdxCastorClient();

  if (client->IsZombie())
  {
    delete client;
    return 0;
  }

  return client;
}


//------------------------------------------------------------------------------
// Send asyn request
//------------------------------------------------------------------------------
int
XrdxCastorClient::SendAsyncRequest(const std::string& userId,
                                   const std::string& rhHost,
                                   unsigned int rhPort,
                                   castor::stager::Request* req,
                                   castor::client::IResponseHandler* rh,
                                   std::vector<castor::rh::Response*>* respvec)

{
  int ret = SFS_OK;
  // Check if we reached the maximum allowed number of pending request and
  // if so then stall the client i.e. return a positive value which represents
  // the number of seconds of stalling; AND do a clean-up of requests for which
  // we received the responses but the client never showed up to collect them.
  {
    XrdSysMutexHelper lock(mMutexMaps);

    if (mMapUsers.size() > XCASTOR2FS_MAX_REQUESTS)
    {
      DoCleanup();
      xcastor_warning("stall the client 5 sec as request queue full");
      ret = 5;     // stall the client for 5 sec
      return ret;
    }
  }

  if (0 == mCallbackSocket)
  {
    xcastor_err("callback socket not yet opened, cannot send request");
    return SFS_ERROR;
  }

  // Create client to send the requests
  unsigned short port;
  unsigned long ip;
  mCallbackSocket->getPortIp(port, ip);
  castor::rh::Client* client = new castor::rh::Client();
  client->setIpAddress(ip);
  client->setPort(port);

  // When the request is deleted so is the client object
  req->setClient(client);

  // Machine - for convenience set it here as it throws an error
  std::string hostName;
  hostName  = castor::System::getHostName();

  if (hostName == "")
    return SFS_ERROR;

  req->setMachine(hostName);

  // Get the RH port
  if ((rhPort <= 0) || (rhPort > 65535))
    rhPort = CSP_RHSERVER_PORT;

  // Create the element to be saved in the map of requests
  struct ReqElement* elem = new ReqElement(userId, req, rh, respvec);

  // Create a socket
  castor::io::ClientSocket sock(rhPort, rhHost);
  sock.connect();

  //Send the request
  sock.sendObject(*req);

  // Lock the map as we could receive the answer for our request before registering
  // it to the map. This is fine as the request is processed only after we get the ack.
  mMutexMaps.Lock();    // -->

  try
  {
    // Wait for acknowledgment
    castor::IObject* obj = sock.readObject();
    castor::MessageAck* ack = dynamic_cast<castor::MessageAck*>(obj);

    if (0 == ack)
    {
      castor::exception::InvalidArgument e;
      e.getMessage() << "No acknowledgement from the server";
      throw e;
    }

    if (!ack->status())
    {
      castor::exception::Exception e(ack->errorCode());
      e.getMessage() << ack->errorMessage();
      delete ack;
      throw e;
    }

    xcastor_debug("ack for reqid=%s", ack->requestId().c_str());
    // Save the request id returned by the stager and add it to the map
    std::string req_id = ack->requestId();
    delete ack;
    req->setReqId(req_id);
    // Save in map the identity of the user along with the request id and response
    std::pair<AsyncReqMap::iterator, bool> req_insert;
    std::pair<AsyncUserMap::iterator, bool> user_insert;
    req_insert = mMapRequests.insert(std::make_pair(req_id, elem));

    if (req_insert.second == true)
    {
      // The request was inserted in the map, insert also the user in the map
      user_insert = mMapUsers.insert(std::make_pair(userId, req_insert.first));

      if (!user_insert.second)
      {
        // If user exists already, remove the request from the map
        mMapRequests.erase(req_insert.first);
        castor::exception::Exception e;
        e.getMessage() << "Fatal error: the user we are trying to register "
                       << "exists already in the map ";
        throw e;
      }
    }
    else
    {
      castor::exception::Exception e;
      e.getMessage() << "Fatal error: the request we are trying to submit "
                     << "exists already in the map ";
      throw e;
    }

    mMutexMaps.UnLock();  // <--
  }
  catch (castor::exception::Exception& e)
  {
    // Unlock the map and forward any exception
    mMutexMaps.UnLock(); // <--
    delete elem;
    throw e;
  }

  return ret;
}


//------------------------------------------------------------------------------
// Get response from the stager about a request sent earlier
//------------------------------------------------------------------------------
struct XrdxCastorClient::ReqElement*
XrdxCastorClient::GetResponse(const std::string& userId,
                              bool& found,
                              bool isFirstTime)
{
  int timeout = 0;
  found = false;
  struct ReqElement* elem = 0;
  AsyncUserMap::iterator user_iter;
  mMutexMaps.Lock();  // -->

  // Find the user in the map and get the response object
  if ((user_iter = mMapUsers.find(userId)) != mMapUsers.end())
  {
    xcastor_debug("Found request for user=%s", userId.c_str());
    elem = user_iter->second->second;
    found = true;
  }

  // Check if it has response and if so delete obj from maps
  if (elem)
  {
    if (elem->HasResponse())
    {
      xcastor_debug("Found also response for user=%s", userId.c_str());
      mMapRequests.erase(user_iter->second);
      mMapUsers.erase(user_iter);
    }
    else if (isFirstTime)
    {
      // If this is the first time we check for a response we give the stager
      // wait_time seconds to reply to our request before stalling the client
      int wait_time = 4;
      struct timeval start;
      struct timeval current;
      gettimeofday(&start, NULL);

      while (1)
      {
        mMutexMaps.UnLock();    // <--
        xcastor_debug("wait for response");
        timeout = mCondResponse.Wait(wait_time);
        mMutexMaps.Lock();    // -->

        // If we timeout then just exit
        if (timeout == 1)
        {
          // Return 0 if response not ready yet
          elem = 0;
          break;
        }

        if (elem->HasResponse())
        {
          xcastor_debug("found also response for user=%s", userId.c_str());
          mMapRequests.erase(user_iter->second);
          mMapUsers.erase(user_iter);
          break;
        }
        else
        {
          // Wait until timeout expires and deal with false signals from poller
          // i.e. with responses for other requests
          gettimeofday(&current, NULL);

          if (current.tv_sec - start.tv_sec >= wait_time)
          {
            xcastor_debug("waited for maximum amount of time");
            elem = 0;
            break;
          }
          else
          {
            wait_time -= (current.tv_sec - start.tv_sec);
            xcastor_debug("continue waiting for %i sec", wait_time);
          }
        }
      }
    }
    else
    {
      xcastor_debug("check after a stall failed, no waiting");
      elem = 0;
    }
  }

  mMutexMaps.UnLock();   // <--
  return elem;
}


//------------------------------------------------------------------------------
// Check if the user has already submitted the current request. If so, this
// means he is coming back to collect the response after a stall.
//------------------------------------------------------------------------------
bool
XrdxCastorClient::HasSubmittedReq(const char* path, XrdOucErrInfo& error)
{
  // Build the user id for all possible types of requests
  xcastor_debug("check if request already submitted for path=%s", path);
  std::ostringstream ostr;
  ostr << error.getErrUser() << ":" << path << ":";
  std::string get_req = ostr.str() + "get";
  std::string put_req = ostr.str() + "put";
  XrdSysMutexHelper lock(mMutexMaps);
  return ((mMapUsers.find(get_req) != mMapUsers.end()) ||
          (mMapUsers.find(put_req) != mMapUsers.end()));
}


//------------------------------------------------------------------------------
// Collect all requests belonging to the supplied user
//------------------------------------------------------------------------------
std::vector<XrdxCastorClient::ReqElement*>
XrdxCastorClient::GetUserRequests(const std::string& tident)
{
  std::vector<XrdxCastorClient::ReqElement*> req_abrt;
  size_t len = tident.length();
  XrdSysMutexHelper lock(mMutexMaps);
  AsyncUserMap::iterator iter = mMapUsers.lower_bound(tident);

  while ((iter != mMapUsers.end()) &&
         (iter->first.compare(0, len, tident) == 0))
  {
    xcastor_debug("Collect for abort usr_id=%s, req_id=%s",
                  iter->second->second->mUserId.c_str(),
                  iter->second->first.c_str());
    req_abrt.push_back(iter->second->second);
    mMapRequests.erase(iter->second);
    mMapUsers.erase(iter++);
    // Defer deletion of the request so that it is done outside the lock.
  }

  return req_abrt;
}


//------------------------------------------------------------------------------
// Clean up old requests - CALLED WITH LOCK ON THE MAPS
//------------------------------------------------------------------------------
void
XrdxCastorClient::DoCleanup()
{
  xcastor_debug("do cleanup of old requests");
  AsyncUserMap::iterator user_iter;
  struct ReqElement* elem = 0;

  for (user_iter = mMapUsers.begin();
       user_iter != mMapUsers.end(); /*no increment*/)
  {
    elem = user_iter->second->second;

    if (elem->Expired())
    {
      // Delete the ReqElement object
      delete elem;

      // Delete entry from the two maps
      mMapRequests.erase(user_iter->second);
      mMapUsers.erase(user_iter++);
    }
    else
    {
      ++user_iter;
    }
  }
}


//------------------------------------------------------------------------------
// Method executed by the poller thread
//------------------------------------------------------------------------------
void*
XrdxCastorClient::StartPollerThread(void* arg)
{
  XrdxCastorClient* client = static_cast<XrdxCastorClient*>(arg);
  client->PollResponses();
  return 0;
}


//------------------------------------------------------------------------------
// Method used by the poller thread to process responses from the stager
//------------------------------------------------------------------------------
void
XrdxCastorClient::PollResponses()
{
  unsigned int i, j;
  // Add the listening socket to the pollfd structure
  mFds[0].fd = mCallbackSocket->socket();
  mFds[0].events = POLLIN;
  mNfds = 1;
  int count = 0;

  while (1)
  {
    if (count % 10 == 0)
    {
      //xcastor_debug("async thread proccessing responses");
      count = 0;
    }

    count++;
    // Wait for a POLLIN event on the list of file descriptors
    errno = 0;
    int rc = poll(mFds, mNfds, 1000); // 1 sec timeout to detect a shutdown

    if (mDoStop)
    {
      // We're shutting down, just exit the loop
      break;
    }

    if (rc == 0)
    {
      // poll timed out, go to next iteration
      continue;
    }
    else if (rc < 0)
    {
      if (errno == EINTR || errno == EAGAIN)
      {
        // A signal was caught during poll(), ignore
        continue;
      }

      xcastor_warning("communication error reading from stager");
      continue;
    }

    // Loop over the file descriptors
    unsigned int nfds = mNfds;

    for (i = 0; i < nfds; i++)
    {
      if (mFds[i].revents == 0)
        continue;  // Nothing of interest

      // Unexpected result?
      if ((mFds[i].revents & POLLIN) != POLLIN)
      {
        xcastor_err("unexpected result from poll()");
        usleep(100000); // wait to avoid thrashing in case of looping errors
        continue;
      }

      // Is the listening descriptor readable? If so, this indicates that there
      // is a new incoming connection
      if (mFds[i].fd == mCallbackSocket->socket())
      {
        if (mNfds >= 1024)
        {
          xcastor_warning("reached maximum number of connections(1024), delay "
                          "accepting new ones until pending ones are dealt with");
          continue;
        }

        xcastor_debug("accepted incoming connection");
        // Accept the incoming connection, and set a short transfer timeout:
        // we don't want to get stuck and we know that the sender is either
        // the stager or a job, so they will normally send the data in one go.
        castor::io::ServerSocket* socket = mCallbackSocket->accept();
        socket->setTimeout(10);
        // Register the new file descriptor in the accepted connections list and
        // the pollfd structure
        mConnected.push_back(socket);
        mFds[mNfds].fd = socket->socket();
        mFds[mNfds].events = POLLIN;
        mNfds++;
      }
      else
      {
        xcastor_debug("got new data to read");
        // This is not the listening socket therefore we have new data to read.
        // Locate the socket in the list of accepted connections
        castor::io::ServerSocket* socket = 0;

        for (j = 0; j < mConnected.size(); j++)
        {
          if (mFds[i].fd == mConnected[j]->socket())
          {
            socket = mConnected[j];
            break;
          }
        }

        // The socket was not found?
        if (socket == 0)
        {
          xcastor_err("POLLIN event received for unkown socket");
          continue;
        }

        // Read object from socket
        castor::IObject* obj = 0;

        try
        {
          obj = socket->readObject();
        }
        catch (castor::exception::Exception e)
        {
          // Ignore "Connection closed by remote end" errors. This is just the
          // request replier of the stager terminating the connection because it
          // has nothing else to send.
          if (e.code() == 1016)
          {
            serrno = 0;    // Reset serrno to 0
            // Remove the socket from the accepted connections list and set the
            // file descriptor in the pollfd structures to -1. Later on the
            // structure will be cleaned up
            delete socket;
            mConnected.erase(mConnected.begin() + j);
            mFds[i].fd = -1;
            // Keep looping for other responses
            continue;
          }
        }

        // Cast response into IOResponse
        castor::rh::IOResponse* res = dynamic_cast<castor::rh::IOResponse*>(obj);

        if (res == 0)
        {
          // Not an IOResponse? This in principle cannot happen.
          // Ignore EndResponses coming from jobs and log any other
          if (obj && obj->type() != castor::OBJ_EndResponse)
          {
            // "Communication error reading from Castor" message
            xcastor_err("received response was not an IOResponse");
          }

          continue;
        }

        // Move response object from the requests map to the users map
        std::string req_id = res->reqAssociated();
        AsyncReqMap::iterator iter;
        mMutexMaps.Lock();  // -->

        if ((iter = mMapRequests.find(req_id)) == mMapRequests.end())
        {
          xcastor_warning("received requ_id that is no longer in map", req_id.c_str());
          mMutexMaps.UnLock();  // <--
        }
        else
        {
          // Handle the response
          struct ReqElement* elem = iter->second;
          elem->mRh->handleResponse(*res);
          elem->SetResponseTime();
          mMutexMaps.UnLock();  // <--
          // Signal that we received a response
          xcastor_debug("signal received response");
          mCondResponse.Broadcast();
        }
      }
    }

    // Compress the pollfd structure removing entries where the file descriptor
    // is negative. We do not need to move back the events and revents fields
    // because the events will always be POLLIN in this case, and revents is
    // output.
    for (i = 0; i < mNfds; i++)
    {
      if (mFds[i].fd == -1)
      {
        for (j = i; j < mNfds; j++)
          mFds[j].fd = mFds[j + 1].fd;

        mNfds--;
      }
    }
  }
}


//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
XrdxCastorClient::ReqElement::ReqElement(const std::string& userId,
                                         castor::stager::Request* req,
                                         castor::client::IResponseHandler* rh,
                                         std::vector<castor::rh::Response*>* respvec):
  mUserId(userId),
  mRequest(req),
  mRh(rh),
  mRespVec(respvec)
{
  // The sendTime is the same as the creation of the object
  if (gettimeofday(&mSendTime, NULL))
  {
    xcastor_static_err("could not get send time for request");
    timerclear(&mSendTime);
  }

  timerclear(&mRecvTime);
}


//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
XrdxCastorClient::ReqElement::~ReqElement()
{
  delete mRequest;
  // Delete the responses and the vector
  delete mRh;
  castor::rh::Response* ptr = 0;

  for (unsigned int i = 0; i < mRespVec->size(); i++)
  {
    ptr = mRespVec->at(i);
    delete ptr;
  }

  mRespVec->clear();
  delete mRespVec;
}


//------------------------------------------------------------------------------
// Set the time when the response arrived
//------------------------------------------------------------------------------
void
XrdxCastorClient::ReqElement::SetResponseTime()
{
  if (gettimeofday(&mRecvTime, NULL))
  {
    xcastor_static_err("could not get time for response");
    timerclear(&mRecvTime);
  }
}


//------------------------------------------------------------------------------
// Delete request if response received but user never turned up to collect
// it. Here we use XCASTO2FS_RESP_TIMEOUT value to discard responses for
// which the client never showed up to collect them.
//------------------------------------------------------------------------------
bool
XrdxCastorClient::ReqElement::Expired()
{
  bool ret = false;

  if (!HasResponse())
    return ret;

  struct timeval now;
  struct timeval res;

  if (gettimeofday(&now, NULL))
  {
    xcastor_static_err("could not get current time");
    return ret;
  }

  timersub(&now, &mRecvTime, &res);

  if (res.tv_sec > XCASTOR2FS_RESP_TIMEOUT)
  {
    xcastor_static_debug("response timeout, mark for deletion");
    ret = true;
  }

  return ret;
}


XCASTORNAMESPACE_END
