/******************************************************************************
 *                      castor/tape/tapebridge/BridgeSocketCatalogue.hpp
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
#include "castor/tape/tapebridge/BridgeSocketCatalogue.hpp"
#include "castor/tape/tapebridge/Constants.hpp"
#include "castor/tape/utils/utils.hpp"

#include <errno.h>


//-----------------------------------------------------------------------------
// constructor
//-----------------------------------------------------------------------------
castor::tape::tapebridge::BridgeSocketCatalogue::BridgeSocketCatalogue() :
  m_listenSock(-1), m_initialRtcpdSock(-1) {
}


//-----------------------------------------------------------------------------
// destructor
//-----------------------------------------------------------------------------
castor::tape::tapebridge::BridgeSocketCatalogue::~BridgeSocketCatalogue() {

  // Note this destructor does NOT close the listen socket used to accept rtcpd
  // connections.  This is the responsibility of the VdqmRequestHandler.

  if(0 <= m_initialRtcpdSock) {
    close(m_initialRtcpdSock);
  }

  // For each rtcpd-connection
  for(RtcpdConnectionList::const_iterator itor = m_rtcpdConnections.begin();
    itor != m_rtcpdConnections.end(); itor++) {

    // Close the rtcpd connection
    close(itor->rtcpdSock);

    // If there is an associated client connection, then close it
    if(0 <= itor->clientSock) {
      close(itor->clientSock);
    }
  }

  // If there is an open client migration-report connection, then close it
  if(0 <= m_clientMigrationReportConnection.clientSock) {
    close(m_clientMigrationReportConnection.clientSock);
  }
}


//-----------------------------------------------------------------------------
// addListenSock
//-----------------------------------------------------------------------------
void castor::tape::tapebridge::BridgeSocketCatalogue::addListenSock(
  const int listenSock) throw(castor::exception::Exception) {

  if(0 > listenSock) {
    TAPE_THROW_EX(castor::exception::InvalidArgument,
      ": Invalid socket-descriptor"
      ": Value is negative"
      ": listenSock=" << listenSock);
  }

  if(0 <= m_listenSock) {
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
void castor::tape::tapebridge::BridgeSocketCatalogue::addInitialRtcpdConn(
  const int initialRtcpdSock) throw(castor::exception::Exception) {

  if(0 > initialRtcpdSock) {
    TAPE_THROW_EX(castor::exception::InvalidArgument,
      ": Invalid socket-descriptor"
      ": Value is negative"
      ": initialRtcpdSock=" << initialRtcpdSock);
  }

  if(0 <= m_initialRtcpdSock) {
    TAPE_THROW_CODE(ECANCELED,
      ": Initial rtcpd socket-descriptor is already set"
      ": current initialRtcpdSocket=" << m_initialRtcpdSock <<
      ": new initialRtcpdSocket=" << initialRtcpdSock);
  }

  m_initialRtcpdSock = initialRtcpdSock;
}


//-----------------------------------------------------------------------------
// releaseInitialRtcpdConn
//-----------------------------------------------------------------------------
int castor::tape::tapebridge::BridgeSocketCatalogue::releaseInitialRtcpdConn()
  throw(castor::exception::Exception) {
  // Throw an exception if the socket-descriptor has not been set
  if(0 > m_initialRtcpdSock) {
    TAPE_THROW_CODE(ENOENT,
      ": Initial rtcpd socket-descriptor does not exist in the catalogue");
  }

  // Release and return the socket-descriptor
  const int tmpSock = m_initialRtcpdSock;
  m_initialRtcpdSock = -1;
  return tmpSock;
}


//-----------------------------------------------------------------------------
// addRtcpdDiskTapeIOControlConn
//-----------------------------------------------------------------------------
void castor::tape::tapebridge::BridgeSocketCatalogue::
  addRtcpdDiskTapeIOControlConn(const int rtcpdSock)
  throw(castor::exception::Exception) {

  if(0 > rtcpdSock) {
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
void castor::tape::tapebridge::BridgeSocketCatalogue::addClientConn(
  const int      rtcpdSock,
  const uint32_t rtcpdReqMagic,
  const uint32_t rtcpdReqType,
  const char     (&rtcpdReqTapePath)[CA_MAXPATHLEN+1],
  const int      clientSock,
  const uint64_t aggregatorTransactionId)
  throw(castor::exception::Exception) {

  if(0 > rtcpdSock) {
    TAPE_THROW_EX(castor::exception::InvalidArgument,
      ": Invalid rtcpd socket-descriptor"
      ": Value is negative"
      ": rtcpdSock=" << rtcpdSock);
  }

  if(0 > clientSock) {
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
      if(0 <= itor->clientSock) {
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

      // Store the tapebridge transaction ID asscoiated with the request sent
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
int castor::tape::tapebridge::BridgeSocketCatalogue::getListenSock() const
  throw(castor::exception::Exception) {

  // Throw an exception if the socket-descriptor of the listen socket does not
  // exist in the catalogue
  if(0 > m_listenSock) {
    TAPE_THROW_CODE(ENOENT,
      ": Listen socket-descriptor does not exist in the catalogue");
  }

  return m_listenSock;
}


//-----------------------------------------------------------------------------
// getInitialRtcpdConn
//-----------------------------------------------------------------------------
int castor::tape::tapebridge::BridgeSocketCatalogue::getInitialRtcpdConn()
  const throw(castor::exception::Exception) {

  // Throw an exception if the socket-descriptor of the initial rtcpd
  // connection does not exist in the catalogue
  if(0 > m_initialRtcpdSock) {
    TAPE_THROW_CODE(ENOENT,
      ": Initial rtcpd socket-descriptor does not exist in the catalogue");
  }

  return m_initialRtcpdSock;
}


//-----------------------------------------------------------------------------
// releaseRtcpdDiskTapeIOControlConn
//-----------------------------------------------------------------------------
int castor::tape::tapebridge::BridgeSocketCatalogue::
  releaseRtcpdDiskTapeIOControlConn(const int rtcpdSock)
  throw(castor::exception::Exception) {

  if(0 > rtcpdSock) {
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

      // If there is an associated client-connection
      if(-1 != itor->clientSock) {
        // Keep the rtcpd-connection entry in the socket-catalogue so that
        // the getRtcpdConn() method will still return successfully.
        //
        // Set the rtcpd socket file-descriptor of the rtcpd-connection
        // entry to -1 to indicate that it has been released from the
        // socket-catalogue
        itor->rtcpdSock = -1;

      // Else there is no associated client connection
      } else {
        // Erase the association from the socket-catalogue
        m_rtcpdConnections.erase(itor);
      }

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
int castor::tape::tapebridge::BridgeSocketCatalogue::releaseClientConn(
  const int rtcpdSock, const int clientSock)
  throw(castor::exception::Exception) {

  if(0 > clientSock) {
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
void castor::tape::tapebridge::BridgeSocketCatalogue::getRtcpdConn(
  const int      clientSock,
  int            &rtcpdSock,
  uint32_t       &rtcpdReqMagic,
  uint32_t       &rtcpdReqType,
  const char     *&rtcpdReqTapePath,
  uint64_t       &aggregatorTransactionId,
  struct timeval &clientReqTimeStamp) const
  throw(castor::exception::Exception) {

  if(0 > clientSock) {
    TAPE_THROW_EX(castor::exception::InvalidArgument,
      ": Invalid client socket-descriptor"
      ": Value is negative"
      ": clientSock=" << clientSock);
  }

  // For each rtcpd-connection
  for(RtcpdConnectionList::const_iterator itor = m_rtcpdConnections.begin();
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
void castor::tape::tapebridge::BridgeSocketCatalogue::buildReadFdSet(
  fd_set &readFdSet, int &maxFd) const throw() {

  // Clear the file-descriptor set
  FD_ZERO(&readFdSet);

  // If the listen socket has been set, then add it to the descriptor set and
  // update maxFd accordingly
  if(0 <= m_listenSock) {
    FD_SET(m_listenSock, &readFdSet);
    maxFd = m_listenSock;
  }

  // If the initial rtcpd connection socket has been set, then add it to the
  // descriptor set and update maxFd accordingly
  if(0 <= m_initialRtcpdSock) {
    FD_SET(m_initialRtcpdSock, &readFdSet);
    if(m_initialRtcpdSock > maxFd) {
      maxFd = m_initialRtcpdSock;
    }
  }

  // If the client migration-report connection socket has been set, then add it
  // to the descriptor set and update maxFd accordingly
  if(0 <= m_clientMigrationReportConnection.clientSock) {
    FD_SET(m_clientMigrationReportConnection.clientSock, &readFdSet);
    if(m_clientMigrationReportConnection.clientSock > maxFd) {
      maxFd = m_clientMigrationReportConnection.clientSock;
    }
  }

  // For each rtcpd-connection
  for(RtcpdConnectionList::const_iterator itor = m_rtcpdConnections.begin();
    itor != m_rtcpdConnections.end(); itor++) {

    // If the rtcpd connection is still open
    if(0 <= itor->rtcpdSock) {
      // Add the rtcpd socket-descriptor to the descriptor set and update
      // maxFd accordingly
      FD_SET(itor->rtcpdSock, &readFdSet);
      if(itor->rtcpdSock > maxFd) {
        maxFd = itor->rtcpdSock;
      }
    }

    // If there is an associated client socket-descriptor, then add it to
    // the descriptor set and update maxFd accordingly
    if(0 <= itor->clientSock) {
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
int castor::tape::tapebridge::BridgeSocketCatalogue::getAPendingSock(
  fd_set &readFdSet, SocketType &sockType) const {

  // If the listen socket is set and is pending, then set the socket type
  // output parameter and return the socket
  if(0 <= m_listenSock && FD_ISSET(m_listenSock, &readFdSet)) {
    sockType = LISTEN;
    return m_listenSock;
  }

  // If the initial rtcpd connection socket is set and pending, then set the
  // socket type output parameter and return the socket
  if(0 <= m_initialRtcpdSock && FD_ISSET(m_initialRtcpdSock, &readFdSet)) {
    sockType = INITIAL_RTCPD;
    return m_initialRtcpdSock;
  }

  // If the client migration-report connection is set and pending, then set the
  // socket type output parameter and return the socket
  if(0 <= m_clientMigrationReportConnection.clientSock &&
    FD_ISSET(m_clientMigrationReportConnection.clientSock, &readFdSet)) {
    sockType = CLIENT_MIGRATION_REPORT;
    return m_clientMigrationReportConnection.clientSock;
  }

  // For each rtcpd-connection
  for(RtcpdConnectionList::const_iterator itor = m_rtcpdConnections.begin();
    itor != m_rtcpdConnections.end(); itor++) {

    // If the rtcpd connection is still open and if the corresponding rtcpd
    // socket-descriptor is pending, then set the socket type output parameter
    // and return the socket
    if(0 <= itor->rtcpdSock && FD_ISSET(itor->rtcpdSock, &readFdSet)) {
      sockType = RTCPD_DISK_TAPE_IO_CONTROL;
      return itor->rtcpdSock;
    }

    // If there is an associated client socket-descriptor
    if(0 <= itor->clientSock) {
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
int castor::tape::tapebridge::BridgeSocketCatalogue::
  getNbDiskTapeIOControlConns() const {

  int nbDiskTapeIOControlConns = 0;

  for(RtcpdConnectionList::const_iterator itor = m_rtcpdConnections.begin();
    itor != m_rtcpdConnections.end(); itor++) {

    // If the rtcpd-connection is still open then include it in the count
    if(-1 != itor->rtcpdSock) {
      nbDiskTapeIOControlConns++;
    }
  }

  return nbDiskTapeIOControlConns;
}


//-----------------------------------------------------------------------------
// checkForTimeout
//-----------------------------------------------------------------------------
void castor::tape::tapebridge::BridgeSocketCatalogue::checkForTimeout() const
  throw(castor::exception::TimeOut) {

  if(!m_clientReqHistory.empty()) {

    // Get the current time
    struct timeval now = {0, 0};
    
    gettimeofday(&now, NULL);

    // Get the oldest client request
    const ClientReqHistoryElement &clientRequest = m_clientReqHistory.front();

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


//-----------------------------------------------------------------------------
// addClientMigrationReportSock
//-----------------------------------------------------------------------------
void castor::tape::tapebridge::BridgeSocketCatalogue::
  addClientMigrationReportSock(const int sock,
  const uint64_t aggregatorTransactionId) throw(castor::exception::Exception) {

  // Throw an exception if the socket-descriptor is invalid
  if(0 > sock) {
    TAPE_THROW_EX(castor::exception::InvalidArgument,
      ": Invalid socket-descriptor"
      ": Value is negative"
      ": sock=" << sock);
  }

  // Throw an exception if the client migraton-report socket-descriptor has
  // already been set
  if(0 <= m_clientMigrationReportConnection.clientSock) {
    TAPE_THROW_CODE(EEXIST,
      ": client migration-report socket-descriptor is already set"
      ": current value=" << m_clientMigrationReportConnection.clientSock <<
      ": new value =" << sock);
  }

  m_clientMigrationReportConnection.clientSock = sock;
  m_clientMigrationReportConnection.aggregatorTransactionId =
    aggregatorTransactionId;
}


//-----------------------------------------------------------------------------
// clientMigrationReportSockIsSet
//-----------------------------------------------------------------------------
bool castor::tape::tapebridge::BridgeSocketCatalogue::
  clientMigrationReportSockIsSet() const {
  return 0 <= m_clientMigrationReportConnection.clientSock;
}


//-----------------------------------------------------------------------------
// releaseClientMigrationReportSock
//-----------------------------------------------------------------------------
int castor::tape::tapebridge::BridgeSocketCatalogue::
  releaseClientMigrationReportSock(uint64_t &aggregatorTransactionId)
  throw(castor::exception::Exception) {

  // Throw an exception if the socket-descriptor has not been set
  if(0 > m_clientMigrationReportConnection.clientSock) {
    TAPE_THROW_CODE(ENOENT,
      ": Client migration-report socket-descriptor does not exist in the"
      " catalogue");
  }

  // Release and return the socket-descriptor
  aggregatorTransactionId =
    m_clientMigrationReportConnection.aggregatorTransactionId;
  const int tmpSock = m_clientMigrationReportConnection.clientSock;
  m_clientMigrationReportConnection.clientSock = -1;
  m_clientMigrationReportConnection.aggregatorTransactionId = 0;
  return tmpSock;
}
