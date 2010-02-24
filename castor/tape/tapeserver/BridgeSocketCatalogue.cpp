/******************************************************************************
 *                      castor/tape/tapeserver/BridgeSocketCatalogue.hpp
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
 * @author Nicola.Bessone@cern.ch Steven.Murray@cern.ch
 *****************************************************************************/

#include "castor/exception/Internal.hpp"
#include "castor/tape/tapeserver/BridgeSocketCatalogue.hpp"
#include "castor/tape/tapeserver/Constants.hpp"
#include "castor/tape/utils/utils.hpp"

#include <errno.h>


//-----------------------------------------------------------------------------
// constructor
//-----------------------------------------------------------------------------
castor::tape::tapeserver::BridgeSocketCatalogue::BridgeSocketCatalogue() :
  m_listenSock(-1), m_initialRtcpdSock(-1) {
}


//-----------------------------------------------------------------------------
// destructor
//-----------------------------------------------------------------------------
castor::tape::tapeserver::BridgeSocketCatalogue::~BridgeSocketCatalogue() {

  // Note this destructor does NOT close the listen socket used to accept rtcpd
  // connections.  This is the responsibility of the VdqmRequestHandler.

  // Note this destructor does NOT close the initial rtcpd connection.  This is
  // the responsibility of the VdqmRequestHandler.

  // For each rtcpd-connection
  for(RtcpdConnectionList::const_iterator itor = m_rtcpdConnections.begin();
    itor != m_rtcpdConnections.end(); itor++) {

    // Close the rtcpd connection
    close(itor->rtcpdSock);

    // If there is an associated client connection, then close it
    if(itor->clientSock != -1) {
      close(itor->clientSock);
    }
  }
}


//-----------------------------------------------------------------------------
// addListenSock
//-----------------------------------------------------------------------------
void castor::tape::tapeserver::BridgeSocketCatalogue::addListenSock(
  const int listenSock) throw(castor::exception::Exception) {

  if(listenSock < 0) {
    TAPE_THROW_EX(castor::exception::InvalidArgument,
      ": Invalid socket-descriptor"
      ": Value is negative"
      ": listenSock=" << listenSock);
  }

  if(m_listenSock != -1) {
    TAPE_THROW_CODE(ECANCELED,
      ": Listen socket-descriptor is already set"
      ": current listenSock=" << m_listenSock <<
      ": new listenSock=" << listenSock);
  }

  m_listenSock = listenSock;
}


//-----------------------------------------------------------------------------
// addInitialRtcpdConn
//-----------------------------------------------------------------------------
void castor::tape::tapeserver::BridgeSocketCatalogue::addInitialRtcpdConn(
  const int initialRtcpdSock) throw(castor::exception::Exception) {

  if(initialRtcpdSock < 0) {
    TAPE_THROW_EX(castor::exception::InvalidArgument,
      ": Invalid socket-descriptor"
      ": Value is negative"
      ": initialRtcpdSock=" << initialRtcpdSock);
  }

  if(m_initialRtcpdSock != -1) {
    TAPE_THROW_CODE(ECANCELED,
      ": Initial rtcpd socket-descriptor is already set"
      ": current initialRtcpdSocket=" << m_initialRtcpdSock <<
      ": new initialRtcpdSocket=" << initialRtcpdSock);
  }

  m_initialRtcpdSock = initialRtcpdSock;
}


//-----------------------------------------------------------------------------
// addRtcpdDiskTapeIOControlConn
//-----------------------------------------------------------------------------
void castor::tape::tapeserver::BridgeSocketCatalogue::
  addRtcpdDiskTapeIOControlConn(const int rtcpdSock)
  throw(castor::exception::Exception) {

  if(rtcpdSock < 0) {
    TAPE_THROW_EX(castor::exception::InvalidArgument,
      ": Invalid socket-descriptor"
      ": Value is negative"
      ": rtcpdSock=" << rtcpdSock);
  }

  // For each rtcpd-connection
  for(RtcpdConnectionList::const_iterator itor = m_rtcpdConnections.begin();
    itor != m_rtcpdConnections.end(); itor++) {

    // Throw an exception if the association is already in the catalogue
    if(itor->rtcpdSock == rtcpdSock) {
      TAPE_THROW_CODE(ECANCELED,
        ": Rtcpd socket-descriptor is already in the catalogue"
        ": rtcpdSock=" << rtcpdSock);
    }
  }

  // Add the descriptor to the catalogue
  m_rtcpdConnections.push_back(RtcpdConnection(rtcpdSock));
}


//-----------------------------------------------------------------------------
// addClientConn
//-----------------------------------------------------------------------------
void castor::tape::tapeserver::BridgeSocketCatalogue::addClientConn(
  const int      rtcpdSock,
  const uint32_t rtcpdReqMagic,
  const uint32_t rtcpdReqType,
  const char     (&rtcpdReqTapePath)[CA_MAXPATHLEN+1],
  const int      clientSock,
  const uint64_t aggregatorTransactionId)
  throw(castor::exception::Exception) {

  if(rtcpdSock < 0) {
    TAPE_THROW_EX(castor::exception::InvalidArgument,
      ": Invalid rtcpd socket-descriptor"
      ": Value is negative"
      ": rtcpdSock=" << rtcpdSock);
  }

  if(clientSock < 0) {
    TAPE_THROW_EX(castor::exception::InvalidArgument,
      ": Invalid client socket-descriptor"
      ": Value is negative"
      ": clientSock=" << clientSock);
  }

  // For each rtcpd-connection
  for(RtcpdConnectionList::iterator itor = m_rtcpdConnections.begin();
    itor != m_rtcpdConnections.end(); itor++) {

    // If the specified rtcpd disk/tape IO control-connection has been found
    if(itor->rtcpdSock == rtcpdSock) {

      // Throw an exception if there is already an associated client connection
      if(itor->clientSock != -1) {
        TAPE_THROW_CODE(ECANCELED,
          ": There is already an associated client connection"
          ": current clientSock=" << itor->clientSock <<
          ": new clientSock=" << clientSock);
      }

      // Store the magic number and request type of the rtcpd request
      itor->rtcpdReqMagic = rtcpdReqMagic;
      itor->rtcpdReqType  = rtcpdReqType;

      // Store the tape path
      if(rtcpdReqTapePath != NULL) {
        itor->rtcpdReqHasTapePath = true;
        utils::copyString(itor->rtcpdReqTapePath, rtcpdReqTapePath);
      } else {
        itor->rtcpdReqHasTapePath = false;
        itor->rtcpdReqTapePath[0]    = '\0';
      }

      // Store the client-connection
      itor->clientSock = clientSock;

      // Store the tapeserver transaction ID asscoiated with the request sent
      // to the client
      itor->aggregatorTransactionId = aggregatorTransactionId;

      // Determine and store the client request timestamp
      gettimeofday(&(itor->clientReqTimeStamp), NULL);

      // Store an entry in the client request history
      m_clientReqHistory.push_back(ClientReqHistoryElement(clientSock,
        itor->clientReqTimeStamp));

      // Return because the client-connection has been added to the catalogue
      return;
    }
  }

  // If this line has been reached, then the specified rtcpd disk/tape IO
  // control-connection does not exist in the catalogue
  TAPE_THROW_CODE(ENOENT,
    ": Rtcpd socket-descriptor does not exist in the catalogue"
    ": rtcpdSock=" << rtcpdSock);
}


//-----------------------------------------------------------------------------
// getListenSock
//-----------------------------------------------------------------------------
int castor::tape::tapeserver::BridgeSocketCatalogue::getListenSock()
  throw(castor::exception::Exception) {

  // Throw an exception if the socket-descriptor of the listen socket does not
  // exist in the catalogue
  if(m_listenSock == -1) {
    TAPE_THROW_CODE(ENOENT,
      ": Listen socket-descriptor does not exist in the catalogue");
  }

  return m_listenSock;
}


//-----------------------------------------------------------------------------
// getInitialRtcpdConn
//-----------------------------------------------------------------------------
int castor::tape::tapeserver::BridgeSocketCatalogue::getInitialRtcpdConn()
  throw(castor::exception::Exception) {

  // Throw an exception if the socket-descriptor of the initial rtcpd
  // connection does not exist in the catalogue
  if(m_initialRtcpdSock == -1) {
    TAPE_THROW_CODE(ENOENT,
      ": Initial rtcpd socket-descriptor does not exist in the catalogue");
  }

  return m_initialRtcpdSock;
}


//-----------------------------------------------------------------------------
// releaseRtcpdDiskTapeIOControlConn
//-----------------------------------------------------------------------------
int castor::tape::tapeserver::BridgeSocketCatalogue::
  releaseRtcpdDiskTapeIOControlConn(const int rtcpdSock)
  throw(castor::exception::Exception) {

  if(rtcpdSock < 0) {
    TAPE_THROW_EX(castor::exception::InvalidArgument,
      ": Invalid socket-descriptor"
      ": Value is negative"
      ": rtcpdSock=" << rtcpdSock);
  }

  // For each rtcpd-connection
  for(RtcpdConnectionList::iterator itor = m_rtcpdConnections.begin();
    itor != m_rtcpdConnections.end(); itor++) {

    // If the rtcpd socket-descriptor has been found
    if(itor->rtcpdSock == rtcpdSock) {

      // Throw an exception if there is an associated client connection
      if(itor->clientSock != -1) {
        TAPE_THROW_CODE(ECANCELED,
          ": Rtcpd socket-descriptor has an associated client connection"
          ": rtcpdSock=" << rtcpdSock <<
          ": clientSock=" << itor->clientSock);
      }

      // Erase the association from the list and return the release socket
      m_rtcpdConnections.erase(itor);
      return rtcpdSock;
    }
  }

  // If this line has been reached, then the specified rtcpd socket-descriptor
  // does not exist in the catalogue
  TAPE_THROW_CODE(ENOENT,
    ": Rtcpd socket-descriptor does not exist in the catalogue"
    ": rtcpdSock=" << rtcpdSock);
}


//-----------------------------------------------------------------------------
// releaseClientConn
//-----------------------------------------------------------------------------
int castor::tape::tapeserver::BridgeSocketCatalogue::releaseClientConn(
  const int rtcpdSock, const int clientSock)
  throw(castor::exception::Exception) {

  if(rtcpdSock < 0) {
    TAPE_THROW_EX(castor::exception::InvalidArgument,
      ": Invalid rtcpd socket-descriptor"
      ": Value is negative"
      ": rtcpdSock=" << rtcpdSock);
  }

  if(clientSock < 0) {
    TAPE_THROW_EX(castor::exception::InvalidArgument,
      ": Invalid client socket-descriptor"
      ": Value is negative"
      ": clientSock=" << clientSock);
  }

  // For each rtcpd-connection
  for(RtcpdConnectionList::iterator itor = m_rtcpdConnections.begin();
    itor != m_rtcpdConnections.end(); itor++) {

    // If the association has been found, then remove and return the client
    // connection
    if(itor->rtcpdSock == rtcpdSock && itor->clientSock == clientSock) {

      // Remove the client request from the client request history
      {
        bool requestRemovedFromHistory = false;

        for(ClientReqHistoryList::iterator itor = m_clientReqHistory.begin();
          itor != m_clientReqHistory.end(); itor++) {

          // If the history element has been found, then remove it and break out
          // of the loop
          if(itor->clientSock == clientSock) {
            m_clientReqHistory.erase(itor);
            requestRemovedFromHistory = true;
            break;
          }
        }

        if(!requestRemovedFromHistory) {
          castor::exception::Internal ie;

          ie.getMessage() <<
           "Failed to remove client request from the client request history"
           ": Client socket-descriptor could not be found"
           ": clientSock=" << clientSock;

          throw(ie);
        }
      }

      // Release the socket-descriptor of the client-connection
      itor->clientSock = -1;

      // Return the socket-descriptor of the client-connection
      return clientSock;
    }
  }

  // If this line has been reached, then the specified association does not
  // exist in the catalogue
  TAPE_THROW_CODE(ENOENT,
    ": The association does not exist in the catalogue"
    ": rtcpdSock=" << rtcpdSock <<
    ": clientSock=" << clientSock);
}


//-----------------------------------------------------------------------------
// getRtcpdConn
//-----------------------------------------------------------------------------
void castor::tape::tapeserver::BridgeSocketCatalogue::getRtcpdConn(
  const int      clientSock,
  int            &rtcpdSock,
  uint32_t       &rtcpdReqMagic,
  uint32_t       &rtcpdReqType,
  char           *&rtcpdReqTapePath,
  uint64_t       &aggregatorTransactionId,
  struct timeval &clientReqTimeStamp)
  throw(castor::exception::Exception) {

  if(clientSock < 0) {
    TAPE_THROW_EX(castor::exception::InvalidArgument,
      ": Invalid client socket-descriptor"
      ": Value is negative"
      ": clientSock=" << clientSock);
  }

  // For each rtcpd-connection
  for(RtcpdConnectionList::iterator itor = m_rtcpdConnections.begin();
    itor != m_rtcpdConnections.end(); itor++) {

    // If the association has been found, then set the output parameters and
    // return
    if(itor->clientSock == clientSock) {

      rtcpdSock               = itor->rtcpdSock;
      rtcpdReqMagic           = itor->rtcpdReqMagic;
      rtcpdReqType            = itor->rtcpdReqType;
      rtcpdReqTapePath        = itor->rtcpdReqHasTapePath ?
                                  itor->rtcpdReqTapePath : NULL;
      aggregatorTransactionId = itor->aggregatorTransactionId;
      clientReqTimeStamp      = itor->clientReqTimeStamp;

      return;
    }
  }

  // If this line has been reached, then the specified client
  // socket-descriptor does not exist in the catalogue
  TAPE_THROW_CODE(ENOENT,
    ": Client socket-descriptor does not exist in the catalogue"
    ": clientSock=" << clientSock);
}


//-----------------------------------------------------------------------------
// buildReadFdSet
//-----------------------------------------------------------------------------
void castor::tape::tapeserver::BridgeSocketCatalogue::buildReadFdSet(
  fd_set &readFdSet, int &maxFd) throw() {

  // Clear the file-descriptor set
  FD_ZERO(&readFdSet);

  // If the listen socket has been set, then add it to the descriptor set and
  // update maxFd accordingly
  if(m_listenSock != -1) {
    FD_SET(m_listenSock, &readFdSet);
    maxFd = m_listenSock;
  }

  // If the initial rtcpd connection socket has been set, then add it to the
  // descriptor set and update maxFd accordingly
  if(m_initialRtcpdSock != -1) {
    FD_SET(m_initialRtcpdSock, &readFdSet);
    if(m_initialRtcpdSock > maxFd) {
      maxFd = m_initialRtcpdSock;
    }
  }

  // For each rtcpd-connection
  for(RtcpdConnectionList::iterator itor = m_rtcpdConnections.begin();
    itor != m_rtcpdConnections.end(); itor++) {

    // Add the rtcpd socket-descriptor to the descriptor set and update maxFd
    // accordingly
    FD_SET(itor->rtcpdSock, &readFdSet);
    if(itor->rtcpdSock > maxFd) {
      maxFd = itor->rtcpdSock;
    }

    // If there is an associated client socket-descriptor, then add it to
    // the descriptor set and update maxFd accordingly
    if(itor->clientSock != -1) {
      FD_SET(itor->clientSock, &readFdSet);
      if(itor->clientSock > maxFd) {
        maxFd = itor->clientSock;
      }
    }
  }
}


//-----------------------------------------------------------------------------
// getAPendingSock
//-----------------------------------------------------------------------------
int castor::tape::tapeserver::BridgeSocketCatalogue::getAPendingSock(
  fd_set &readFdSet, SocketType &sockType) throw() {

  // If the listen socket is pending, then set the socket type output parameter
  // and return the socket
  if(FD_ISSET(m_listenSock, &readFdSet)) {
    sockType = LISTEN;
    return m_listenSock;
  }

  // If the initial rtcpd connection socket is pending, then set the socket
  // type output parameter and return the socket
  if(FD_ISSET(m_initialRtcpdSock, &readFdSet)) {
    sockType = INITIAL_RTCPD;
    return m_initialRtcpdSock;
  }

  // For each rtcpd-connection
  for(RtcpdConnectionList::iterator itor = m_rtcpdConnections.begin();
    itor != m_rtcpdConnections.end(); itor++) {

    // If the rtcpd socket-descriptor is pending, then set the socket type
    // output parameter and return the socket
    if(FD_ISSET(itor->rtcpdSock, &readFdSet)) {
      sockType = RTCPD_DISK_TAPE_IO_CONTROL;
      return itor->rtcpdSock;
    }

    // If there is an associated client socket-descriptor
    if(itor->clientSock != -1) {
      // If the rtcpd socket-descriptor is pending, then set the socket type
      // output parameter and return the socket
      if(FD_ISSET(itor->clientSock, &readFdSet)) {
        sockType = CLIENT;
        return itor->clientSock;
      }
    }
  }

  // If this line has been reached, then there are no pending
  // socket-descriptors known to the catalogue
  return -1;
}


//-----------------------------------------------------------------------------
// getNbDiskTapeIOControlConnections
//-----------------------------------------------------------------------------
int castor::tape::tapeserver::BridgeSocketCatalogue::
  getNbDiskTapeIOControlConns() {

  return m_rtcpdConnections.size();
}


//-----------------------------------------------------------------------------
// checkForTimeout
//-----------------------------------------------------------------------------
void castor::tape::tapeserver::BridgeSocketCatalogue::checkForTimeout()
  throw(castor::exception::TimeOut) {

  if(!m_clientReqHistory.empty()) {

    // Get the current time
    struct timeval now = {0, 0};
    
    gettimeofday(&now, NULL);

    // Get the oldest client request
    ClientReqHistoryElement &clientRequest = m_clientReqHistory.front();

    // Calculate the age in seconds
    const int ageSecs = clientRequest.clientReqTimeStamp.tv_sec - now.tv_sec;

    // Throw an exception if the oldest client-request has been around too long
    if(ageSecs > CLIENTNETRWTIMEOUT) {

      castor::exception::TimeOut te;

      te.getMessage() <<
        "Timed out waiting for client reply"
        ": clientSock=" << clientRequest.clientSock <<
        ": ageSecs=" << ageSecs <<
        ": clientNetRWTimeout=" << CLIENTNETRWTIMEOUT;

      throw(te);
    }
  }
}
