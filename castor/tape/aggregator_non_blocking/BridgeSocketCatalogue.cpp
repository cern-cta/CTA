/******************************************************************************
 *                      castor/tape/aggregator/BridgeSocketCatalogue.hpp
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
#include "castor/tape/aggregator/BridgeSocketCatalogue.hpp"
#include "castor/tape/utils/utils.hpp"

#include <errno.h>


//-----------------------------------------------------------------------------
// constructor
//-----------------------------------------------------------------------------
castor::tape::aggregator::BridgeSocketCatalogue::BridgeSocketCatalogue() :
  m_listenSock(-1), m_initialRtcpdSock(-1) {
}


//-----------------------------------------------------------------------------
// destructor
//-----------------------------------------------------------------------------
castor::tape::aggregator::BridgeSocketCatalogue::~BridgeSocketCatalogue() {

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
void castor::tape::aggregator::BridgeSocketCatalogue::addListenSock(
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
void castor::tape::aggregator::BridgeSocketCatalogue::addInitialRtcpdConn(
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
void castor::tape::aggregator::BridgeSocketCatalogue::
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
void castor::tape::aggregator::BridgeSocketCatalogue::addClientConn(
  const int                  rtcpdSock,
  const uint32_t             rtcpdReqMagic,
  const uint32_t             rtcpdReqType,
  const char                 (&rtcpdReqTapePath)[CA_MAXPATHLEN+1],
  const int                  clientSock,
  const ClientConnectionType clientType)
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

      // Determine the next rtcpd-connection state based on the
      // client-connection type
      RtcpdConnectionStatus nextState = IDLE; // Dummy initial value
      switch(clientType) {
      case FILE_TO_MIGRATE_REPLY: nextState = WAIT_FILE_TO_MIGRATE     ; break;
      case FILE_TO_RECALL_REPLY : nextState = WAIT_FILE_TO_RECALL      ; break;
      case ACK_OF_FILE_MIGRATED : nextState = WAIT_ACK_OF_FILE_MIGRATED; break;
      case ACK_OF_FILE_RECALLED : nextState = WAIT_ACK_OF_FILE_RECALLED; break;
      default:
        TAPE_THROW_EX(exception::Internal,
          ": Unknown client-connection type"
          ": type=0x" << std::hex << clientType << std::dec)
      }

      // Throw an exception if the rtcpd-connection state transistion is invalid
      if(itor->rtcpdStatus != IDLE) {
        castor::exception::Exception ce(ECANCELED);

        ce.getMessage() <<
          "Invalid rtpcd-connection state-transition"
          ": current=" << rtcpdSockStatusToStr(itor->rtcpdStatus) <<
          ": next=" << rtcpdSockStatusToStr(nextState);

        throw(ce);
      }

      // Set the status of the rtpcd socket
      itor->rtcpdStatus = nextState;

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

      // Determine and store the client request timestamp
      gettimeofday(&(itor->clientReqTimeStamp), NULL);

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
int castor::tape::aggregator::BridgeSocketCatalogue::getListenSock()
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
int castor::tape::aggregator::BridgeSocketCatalogue::getInitialRtcpdConn()
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
int castor::tape::aggregator::BridgeSocketCatalogue::
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
int castor::tape::aggregator::BridgeSocketCatalogue::releaseClientConn(
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

      // Throw an exception if the neccesary rtcpd-connection state-transistion
      // is invalid
      if(itor->rtcpdStatus == IDLE) {
        castor::exception::Exception ce(ECANCELED);

        ce.getMessage() <<
          "Invalid state transition"
          ": current=IDLE"
          ": next=IDLE";

        throw(ce);
      }

      // Update the state of the rtcpd-connection
      itor->rtcpdStatus == IDLE;

      // Reset the magic number and request type of the rtcpd request
      itor->rtcpdReqMagic = 0;
      itor->rtcpdReqType  = 0;

      // Release the socket-descriptor of the client-connection
      itor->clientSock = -1;

      // Reset the time stamp of the client reply to 0
      itor->clientReqTimeStamp.tv_sec  = 0;
      itor->clientReqTimeStamp.tv_usec = 0;

      // Return the socket-descriptor of the client-connection
      return clientSock;
    }
  }

  // If this line has been reached, then the specified association does not
  // exist in the catalogue
  TAPE_THROW_CODE(ENOENT,
    ": The associateion does not exist in the catalogue"
    ": rtcpdSock=" << rtcpdSock <<
    ": clientSock=" << clientSock);
}


//-----------------------------------------------------------------------------
// getRtcpdConn
//-----------------------------------------------------------------------------
void castor::tape::aggregator::BridgeSocketCatalogue::getRtcpdConn(
  const int             clientSock,
  int                   &rtcpdSock,
  RtcpdConnectionStatus &rtcpdStatus,
  uint32_t              &rtcpdReqMagic,
  uint32_t              &rtcpdReqType,
  const char            *rtcpdReqTapePath,
  struct timeval        &clientReqTimeStamp)
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

      rtcpdSock          = itor->rtcpdSock;
      rtcpdStatus        = itor->rtcpdStatus;
      rtcpdReqMagic      = itor->rtcpdReqMagic;
      rtcpdReqType       = itor->rtcpdReqType;
      rtcpdReqTapePath   = itor->rtcpdReqHasTapePath ?
                             itor->rtcpdReqTapePath : NULL;
      clientReqTimeStamp = itor->clientReqTimeStamp;

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
void castor::tape::aggregator::BridgeSocketCatalogue::buildReadFdSet(
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
int castor::tape::aggregator::BridgeSocketCatalogue::getAPendingSock(
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
int castor::tape::aggregator::BridgeSocketCatalogue::
  getNbDiskTapeIOControlConns() {

  return m_rtcpdConnections.size();
}


//-----------------------------------------------------------------------------
// rtcpdSockStatusToStr
//-----------------------------------------------------------------------------
const char
  *castor::tape::aggregator::BridgeSocketCatalogue::rtcpdSockStatusToStr(
  const RtcpdConnectionStatus status) throw() {
  switch(status) {
  case IDLE                     : return "IDLE";
  case WAIT_FILE_TO_MIGRATE     : return "WAIT_FILE_TO_MIGRATE";
  case WAIT_FILE_TO_RECALL      : return "WAIT_FILE_TO_RECALL";
  case WAIT_ACK_OF_FILE_MIGRATED: return "WAIT_ACK_OF_FILE_MIGRATED";
  case WAIT_ACK_OF_FILE_RECALLED: return "WAIT_ACK_OF_FILE_RECALLED";
  default                       : return "UNKNOWN";
  }
}
