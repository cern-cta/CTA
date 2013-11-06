/******************************************************************************
 *                castor/tape/tapebridge/DummyClientProxy.hpp
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
 *
 *
 * @author Steven.Murray@cern.ch
 *****************************************************************************/

#ifndef CASTOR_TAPE_TAPEBRIDGE_DUMMYCLIENTPROXY_HPP
#define CASTOR_TAPE_TAPEBRIDGE_DUMMYCLIENTPROXY_HPP 1

#include "castor/tape/tapebridge/IClientProxy.hpp"


namespace castor     {
namespace tape       {
namespace tapebridge {


/**
 * Acts as a dummy proxy for the client of the tapebridged daemon.
 */
class DummyClientProxy: public IClientProxy {

public:

  ~DummyClientProxy() throw() {
    // Do nothing
  }
  
  tapegateway::Volume *getVolume(
    const uint64_t tapebridgeTransId)
    throw(castor::exception::Exception) {
    return NULL;
  }

  int sendFilesToMigrateListRequest(
    const uint64_t tapebridgeTransId,
    const uint64_t maxFiles,
    const uint64_t maxBytes) const
    throw(castor::exception::Exception) {
    return -1;
  }

  castor::tape::tapegateway::FilesToMigrateList
    *receiveFilesToMigrateListRequestReplyAndClose(
    const uint64_t tapebridgeTransId,
    const int      clientSock) const
    throw(castor::exception::Exception) {
    return NULL;
  }

  int sendFilesToRecallListRequest(
    const uint64_t tapebridgeTransId,
    const uint64_t maxFiles,
    const uint64_t maxBytes) const
    throw(castor::exception::Exception) {
    return -1;
  }

  castor::tape::tapegateway::FilesToRecallList
    *receiveFilesToRecallListRequestReplyAndClose(
    const uint64_t tapebridgeTransId,
    const int      clientSock) const
    throw(castor::exception::Exception) {
    return NULL;
  }

  void receiveNotificationReplyAndClose(
    const uint64_t tapebridgeTransId,
    const int      clientSock) const
    throw(castor::exception::Exception) {
    // Do nothing
  }

  tapegateway::DumpParameters *getDumpParameters(
    const uint64_t tapebridgeTransId) const
    throw(castor::exception::Exception) {
    return NULL;
  }

  void notifyDumpMessage(
    const uint64_t tapebridgeTransId,
    const char     (&message)[CA_MAXLINELEN+1]) const
    throw(castor::exception::Exception) {
    // Do nothing
  }

  void ping(
    const uint64_t tapebridgeTransId) const
    throw(castor::exception::Exception) {
    // Do nothing
  }

  IObject *receiveReplyAndClose(const int clientSock) const
    throw(castor::exception::Exception) {
    return NULL;
  }

  void checkTransactionIds(
    const char *const messageTypeName,
    const uint32_t    actualMountTransactionId,
    const uint64_t    expectedTapebridgeTransId,
    const uint64_t    actualTapebridgeTransId) const
    throw(castor::exception::Exception) {
    // Do nothing
  }

  void notifyEndOfSession(
    const uint64_t tapebridgeTransId) const
    throw(castor::exception::Exception) {
    // Do nothing
  }

  void notifyEndOfFailedSession(
    const uint64_t    tapebridgeTransId,
    const int         errorCode,
    const std::string &errorMessage) const
    throw(castor::exception::Exception) {
    // Do nothing
  }

  int connectAndSendMessage(
    IObject &message,
    timeval &connectDuration) const
    throw(castor::exception::Exception) {
    return -1;
  }
}; // class DummyClientProxy

} // namespace tapebridge
} // namespace tape
} // namespace castor

#endif // CASTOR_TAPE_TAPEBRIDGE_DUMMYCLIENTPROXY_HPP
