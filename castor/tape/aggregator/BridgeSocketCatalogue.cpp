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

  // Note the destructor does NOT close the listen socket

  // Close the initial rtcpd socket if it is set
  if(m_initialRtcpdSock != -1) {
    close(m_initialRtcpdSock);
  }

  // For each rtcpd / tape-gateway socket association
  for(RtcpdGatewayList::const_iterator itor = m_rtcpdGatewaySockets.begin();
    itor != m_rtcpdGatewaySockets.end(); itor++) {

    // Close the rtcpd connection
    close(itor->rtcpdSock);

    // If there is an associated tape-gateway connection, then close it
    if(itor->gatewaySock != -1) {
      close(itor->gatewaySock);
    }
  }
}


//-----------------------------------------------------------------------------
// addListenSocket
//-----------------------------------------------------------------------------
void castor::tape::aggregator::BridgeSocketCatalogue::addListenSocket(
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
// addInitialRtcpdSocket
//-----------------------------------------------------------------------------
void castor::tape::aggregator::BridgeSocketCatalogue::addInitialRtcpdSocket(
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
// addRtcpdDiskTapeIOControlSocket
//-----------------------------------------------------------------------------
void castor::tape::aggregator::BridgeSocketCatalogue::
  addRtcpdDiskTapeIOControlSocket(const int rtcpdSock)
  throw(castor::exception::Exception) {

  if(rtcpdSock < 0) {
    TAPE_THROW_EX(castor::exception::InvalidArgument,
      ": Invalid socket-descriptor"
      ": Value is negative"
      ": rtcpdSock=" << rtcpdSock);
  }

  // For each rtcpd / tape-gateway socket association
  for(RtcpdGatewayList::const_iterator itor = m_rtcpdGatewaySockets.begin();
    itor != m_rtcpdGatewaySockets.end(); itor++) {

    // Throw an exception if the association is already in the catalogue
    if(itor->rtcpdSock == rtcpdSock) {
      TAPE_THROW_CODE(ECANCELED,
        ": Rtcpd socket-descriptor is already in the catalogue"
        ": rtcpdSock=" << rtcpdSock);
    }
  }

  // Add the descriptor to the catalogue
  const int gatewaySock = -1; // There is no associated gateway connection
  m_rtcpdGatewaySockets.push_back(RtcpdGateway(rtcpdSock, gatewaySock));
}


//-----------------------------------------------------------------------------
// addGatewaySocket
//-----------------------------------------------------------------------------
void castor::tape::aggregator::BridgeSocketCatalogue::addGatewaySocket(
  const int rtcpdSock, const int gatewaySock)
  throw(castor::exception::Exception) {

  if(rtcpdSock < 0) {
    TAPE_THROW_EX(castor::exception::InvalidArgument,
      ": Invalid rtcpd socket-descriptor"
      ": Value is negative"
      ": rtcpdSock=" << rtcpdSock);
  }

  if(gatewaySock < 0) {
    TAPE_THROW_EX(castor::exception::InvalidArgument,
      ": Invalid tape-gateway socket-descriptor"
      ": Value is negative"
      ": gatewaySock=" << gatewaySock);
  }

  // For each rtcpd / tape-gateway socket association
  for(RtcpdGatewayList::iterator itor = m_rtcpdGatewaySockets.begin();
    itor != m_rtcpdGatewaySockets.end(); itor++) {

    // If the specified rtcpd disk/tape IO control-connection has been found
    if(itor->rtcpdSock == rtcpdSock) {

      // Throw an exception if there is already an associated gateway connection
      if(itor->gatewaySock != -1) {
        TAPE_THROW_CODE(ECANCELED,
          ": There is already an associated gateway connection"
          ": current gatewaySock=" << itor->gatewaySock <<
          ": new gatewaySock=" << gatewaySock);
      }

      // Store the tape-gateway connection and return
      itor->gatewaySock = gatewaySock;
      return;
    }
  }

  // If this line has been reached, then the specified rtcpd disk/tape IO
  // control-connection does not exist in the catalogue
  TAPE_THROW_CODE(ECANCELED,
    ": Rtcpd socket-descriptor does not exist in the catalogue"
    ": rtcpdSock=" << rtcpdSock);
}


//-----------------------------------------------------------------------------
// releaseRtcpdDiskTapeIOControlSocket
//-----------------------------------------------------------------------------
int castor::tape::aggregator::BridgeSocketCatalogue::
  releaseRtcpdDiskTapeIOControlSocket(const int rtcpdSock)
  throw(castor::exception::Exception) {

  if(rtcpdSock < 0) {
    TAPE_THROW_EX(castor::exception::InvalidArgument,
      ": Invalid socket-descriptor"
      ": Value is negative"
      ": rtcpdSock=" << rtcpdSock);
  }

  // For each rtcpd / tape-gateway socket association
  for(RtcpdGatewayList::iterator itor = m_rtcpdGatewaySockets.begin();
    itor != m_rtcpdGatewaySockets.end(); itor++) {

    // If the rtcpd socket-descriptor has been found
    if(itor->rtcpdSock == rtcpdSock) {

      // Throw an exception if there is an associated tape-gateway connection
      if(itor->gatewaySock != -1) {
        TAPE_THROW_CODE(ECANCELED,
          ": Rtcpd socket-descriptor has an associated tape-gateway connection"
          ": rtcpdSock=" << rtcpdSock <<
          ": gatewaySock=" << itor->gatewaySock);
      }

      // Erase the association from the list and return the release socket
      m_rtcpdGatewaySockets.erase(itor);
      return rtcpdSock;
    }
  }

  // If this line has been reached, then the specified rtcpd socket-descriptor
  // does not exist in the catalogue
  TAPE_THROW_CODE(ECANCELED,
    ": Rtcpd socket-descriptor does not exist in the catalogue"
    ": rtcpdSock=" << rtcpdSock);
}


//-----------------------------------------------------------------------------
// releaseGatewaySocket
//-----------------------------------------------------------------------------
int castor::tape::aggregator::BridgeSocketCatalogue::releaseGatewaySocket(
  const int rtcpdSock, const int gatewaySock)
  throw(castor::exception::Exception) {

  if(rtcpdSock < 0) {
    TAPE_THROW_EX(castor::exception::InvalidArgument,
      ": Invalid rtcpd socket-descriptor"
      ": Value is negative"
      ": rtcpdSock=" << rtcpdSock);
  }

  if(gatewaySock < 0) {
    TAPE_THROW_EX(castor::exception::InvalidArgument,
      ": Invalid tape-gateway socket-descriptor"
      ": Value is negative"
      ": gatewaySock=" << gatewaySock);
  }

  // For each rtcpd / tape-gateway socket association
  for(RtcpdGatewayList::iterator itor = m_rtcpdGatewaySockets.begin();
    itor != m_rtcpdGatewaySockets.end(); itor++) {

    // If the association has been found, then remove and return the gateway
    // connection
    if(itor->rtcpdSock == rtcpdSock && itor->gatewaySock == gatewaySock) {
      itor->gatewaySock = -1;
      return gatewaySock;
    }
  }

  // If this line has been reached, then the specified association does not
  // exist in the catalogue
  TAPE_THROW_CODE(ECANCELED,
    ": The associateion does not exist in the catalogue"
    ": rtcpdSock=" << rtcpdSock <<
    ": gatewaySock=" << gatewaySock);
}


//-----------------------------------------------------------------------------
// getRtcpdSocket
//-----------------------------------------------------------------------------
int castor::tape::aggregator::BridgeSocketCatalogue::getRtcpdSocket(
  const int gatewaySock) throw(castor::exception::Exception) {

  if(gatewaySock < 0) {
    TAPE_THROW_EX(castor::exception::InvalidArgument,
      ": Invalid tape-gateway socket-descriptor"
      ": Value is negative"
      ": gatewaySock=" << gatewaySock);
  }

  // For each rtcpd / tape-gateway socket association
  for(RtcpdGatewayList::iterator itor = m_rtcpdGatewaySockets.begin();
    itor != m_rtcpdGatewaySockets.end(); itor++) {

    // If the association has been found, then return the rtcpd
    // socket-descriptor
    if(itor->gatewaySock == gatewaySock) {
      return(itor->rtcpdSock);
    }
  }

  // If this line has been reached, then the specified tape-gateway
  //  socket-descriptor does not exist in the catalogue
  TAPE_THROW_CODE(ECANCELED,
    ": Tape-gateway socket-descriptor does not exist in the catalogue"
    ": gatewaySock=" << gatewaySock);

  return 0;
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

  // For each rtcpd / tape-gateway socket association
  for(RtcpdGatewayList::iterator itor = m_rtcpdGatewaySockets.begin();
    itor != m_rtcpdGatewaySockets.end(); itor++) {

    // Add the rtcpd socket-descriptor to the descriptor set and update maxFd
    // accordingly
    FD_SET(itor->rtcpdSock, &readFdSet);
    if(itor->rtcpdSock > maxFd) {
      maxFd = itor->rtcpdSock;
    }

    // If there is an associated tape-gateway socket-descriptor, then add it to
    // the descriptor set and update maxFd accordingly
    if(itor->gatewaySock != -1) {
      FD_SET(itor->gatewaySock, &readFdSet);
      if(itor->gatewaySock > maxFd) {
        maxFd = itor->gatewaySock;
      }
    }
  }
}


//-----------------------------------------------------------------------------
// getAPendingSocket
//-----------------------------------------------------------------------------
int castor::tape::aggregator::BridgeSocketCatalogue::getAPendingSocket(
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

  // For each rtcpd / tape-gateway socket association
  for(RtcpdGatewayList::iterator itor = m_rtcpdGatewaySockets.begin();
    itor != m_rtcpdGatewaySockets.end(); itor++) {

    // If the rtcpd socket-descriptor is pending, then set the socket type
    // output parameter and return the socket
    if(FD_ISSET(itor->rtcpdSock, &readFdSet)) {
      sockType = RTCPD_DISK_TAPE_IO_CONTROL;
      return itor->rtcpdSock;
    }

    // If there is an associated tape-gateway socket-descriptor
    if(itor->gatewaySock != -1) {
      // If the rtcpd socket-descriptor is pending, then set the socket type
      // output parameter and return the socket
      if(FD_ISSET(itor->gatewaySock, &readFdSet)) {
        sockType = GATEWAY;
        return itor->gatewaySock;
      }
    }
  }

  // If this line has been reached, then there are no pending
  // socket-descriptors known to the catalogue
  return -1;
}
