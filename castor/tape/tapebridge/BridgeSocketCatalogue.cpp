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
castor::tape::tapebridge::BridgeSocketCatalogue::BridgeSocketCatalogue(
  IFileCloser &fileCloser):
  m_fileCloser(fileCloser),
  m_listenSock(-1),
  m_initialRtcpdSock(-1) {
  // Do nothing
}


//-----------------------------------------------------------------------------
// destructor
//-----------------------------------------------------------------------------
castor::tape::tapebridge::BridgeSocketCatalogue::~BridgeSocketCatalogue() {

  // Note this destructor does NOT close the listen socket used to accept rtcpd
  // connections.  This is the responsibility of the VdqmRequestHandler.

  if(0 <= m_initialRtcpdSock) {
    m_fileCloser.closeFd(m_initialRtcpdSock);
  }

  // Close each of the rtcpd-connections
  for(std::set<int>::const_iterator itor = m_rtcpdIOControlConns.begin();
    itor != m_rtcpdIOControlConns.end(); itor++) {
    m_fileCloser.closeFd(*itor);
  }

  // If there is an open client "get more work" connection, then close it
  if(0 <= m_getMoreWorkConnection.clientSock) {
    m_fileCloser.closeFd(m_getMoreWorkConnection.clientSock);
  }

  // If there is an open client migration-report connection, then close it
  if(0 <= m_migrationReportConnection.clientSock) {
    m_fileCloser.closeFd(m_migrationReportConnection.clientSock);
  }
}


//-----------------------------------------------------------------------------
// addListenSock
//-----------------------------------------------------------------------------
void castor::tape::tapebridge::BridgeSocketCatalogue::addListenSock(
  const int listenSock)
  throw(castor::exception::InvalidArgument, castor::exception::Exception) {

  if(0 > listenSock || FD_SETSIZE <= listenSock) {
    TAPE_THROW_EX(castor::exception::InvalidArgument,
      ": Invalid socket-descriptor"
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
  const int initialRtcpdSock)
  throw(castor::exception::InvalidArgument, castor::exception::Exception) {

  if(0 > initialRtcpdSock || FD_SETSIZE <= initialRtcpdSock) {
    TAPE_THROW_EX(castor::exception::InvalidArgument,
      ": Invalid socket-descriptor"
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
  throw(castor::exception::InvalidArgument, castor::exception::Exception) {

  if(0 > rtcpdSock || FD_SETSIZE <= rtcpdSock) {
    TAPE_THROW_EX(castor::exception::InvalidArgument,
      ": Invalid socket-descriptor"
      ": rtcpdSock=" << rtcpdSock);
  }

  // For each rtcpd-connection
  for(std::set<int>::const_iterator itor = m_rtcpdIOControlConns.begin();
    itor != m_rtcpdIOControlConns.end(); itor++) {

    // Throw an exception if the association is already in the catalogue
    if(rtcpdSock == *itor) {
      TAPE_THROW_CODE(ECANCELED,
        ": Rtcpd socket-descriptor is already in the catalogue"
        ": rtcpdSock=" << rtcpdSock);
    }
  }

  // Add the descriptor to the catalogue
  m_rtcpdIOControlConns.insert(rtcpdSock);
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
  throw(castor::exception::InvalidArgument, castor::exception::Exception) {

  if(0 > rtcpdSock) {
    TAPE_THROW_EX(castor::exception::InvalidArgument,
      ": Invalid socket-descriptor"
      ": Value is negative"
      ": rtcpdSock=" << rtcpdSock);
  }

  // Try to find the rtcpd connection
  std::set<int>::iterator itor = m_rtcpdIOControlConns.find(rtcpdSock);

  if(m_rtcpdIOControlConns.end() == itor) {
    TAPE_THROW_CODE(ENOENT,
      ": Rtcpd socket-descriptor does not exist in the catalogue"
      ": rtcpdSock=" << rtcpdSock);
  }

  m_rtcpdIOControlConns.erase(itor);

  // If the specified rtcpd disk/tape IO control-connection is associated with
  // the client "get more work" connection then the rtcpd socket
  // file-descriptor of the association will be set to -1 to indicate that it
  // has been released from the socket-catalogue.
  if(rtcpdSock == m_getMoreWorkConnection.rtcpdSock) {
    m_getMoreWorkConnection.rtcpdSock = -1;
  }

  return rtcpdSock;
}


//-----------------------------------------------------------------------------
// buildReadFdSet
//-----------------------------------------------------------------------------
void castor::tape::tapebridge::BridgeSocketCatalogue::buildReadFdSet(
  fd_set &readFdSet, int &maxFd) const
  throw(castor::exception::NoValue, castor::exception::Exception) {

  const char *const task = "Build the read set for select";

  // Clear the file-descriptor set
  FD_ZERO(&readFdSet);

  maxFd = -1;

  ifValidInsertFdIntoSet(m_listenSock, readFdSet, maxFd);
  ifValidInsertFdIntoSet(m_initialRtcpdSock, readFdSet, maxFd);
  ifValidInsertFdIntoSet(m_migrationReportConnection.clientSock, readFdSet,
    maxFd);
  ifValidInsertFdIntoSet(m_getMoreWorkConnection.clientSock, readFdSet, maxFd);

  // For each rtcpd-connection
  for(std::set<int>::const_iterator itor = m_rtcpdIOControlConns.begin();
    itor != m_rtcpdIOControlConns.end(); itor++) {

    if(0 > *itor || FD_SETSIZE <= *itor) {
      // This should never happen
      TAPE_THROW_EX(castor::exception::Internal,
        ": Failed to " << task <<
        ": Tried to add an invalid file-descriptor to the select read-set"
        ": fd=" << *itor);
    }

    ifValidInsertFdIntoSet(*itor, readFdSet, maxFd);
  }

  // Throw a NoValue exception if no file-descriptors were added
  if(-1 == maxFd) {
    TAPE_THROW_EX(castor::exception::NoValue,
      ": Failed to " << task <<
      ": Socket-catalogue contains no file-descriptors");
  }
}


//-----------------------------------------------------------------------------
// ifValidInsertFdIntoSet
//-----------------------------------------------------------------------------
bool castor::tape::tapebridge::BridgeSocketCatalogue::ifValidInsertFdIntoSet(
  const int fd,
  fd_set    &fdSet,
  int       &maxFd) const throw() {

  // If the file-descriptor is valid
  if(0 <= fd) {

    // Insert the file-descriptor into the set
    FD_SET(fd, &fdSet);

    // Update the maxiumum file-descriptor as appropriate
    if(fd > maxFd) {
      maxFd = fd;
    }

    // The file-descriptor is valid and was therefore inserted into the set
    return true;

  } else {

    // The file-descriptor is not valid and was therefore not inserted into
    // the set
    return false;

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

  // For each rtcpd-connection
  for(std::set<int>::const_iterator itor = m_rtcpdIOControlConns.begin();
    itor != m_rtcpdIOControlConns.end(); itor++) {

    // If the rtcpd connection is still open and if the corresponding rtcpd
    // socket-descriptor is pending, then set the socket type output parameter
    // and return the socket
    if(0 <= *itor && FD_ISSET(*itor, &readFdSet)) {
      sockType = RTCPD_DISK_TAPE_IO_CONTROL;
      return *itor;
    }
  }

  // If the client "get more work" connection is set and pending, then set the
  // socket type output parameter and return the socket
  if(0 <= m_getMoreWorkConnection.clientSock &&
    FD_ISSET(m_getMoreWorkConnection.clientSock, &readFdSet)) {
    sockType = CLIENT_GET_MORE_WORK;
    return m_getMoreWorkConnection.clientSock;
  }

  // If the client migration-report connection is set and pending, then set the
  // socket type output parameter and return the socket
  if(0 <= m_migrationReportConnection.clientSock &&
    FD_ISSET(m_migrationReportConnection.clientSock, &readFdSet)) {
    sockType = CLIENT_MIGRATION_REPORT;
    return m_migrationReportConnection.clientSock;
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

  return m_rtcpdIOControlConns.size();
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
  const uint64_t tapebridgeTransId)
  throw(castor::exception::InvalidArgument, castor::exception::Exception) {

  // Throw an exception if the socket-descriptor is invalid
  if(0 > sock || FD_SETSIZE <= sock) {
    TAPE_THROW_EX(castor::exception::InvalidArgument,
      ": Invalid socket-descriptor"
      ": sock=" << sock);
  }

  // Throw an exception if the socket-descriptor has already been set
  if(0 <= m_migrationReportConnection.clientSock) {
    TAPE_THROW_CODE(EEXIST,
      ": client migration-report socket-descriptor is already set"
      ": current value=" << m_migrationReportConnection.clientSock <<
      ": new value =" << sock);
  }

  m_migrationReportConnection.clientSock = sock;
  m_migrationReportConnection.tapebridgeTransId = tapebridgeTransId;
}


//-----------------------------------------------------------------------------
// clientMigrationReportSockIsSet
//-----------------------------------------------------------------------------
bool castor::tape::tapebridge::BridgeSocketCatalogue::
  clientMigrationReportSockIsSet() const {
  return 0 <= m_migrationReportConnection.clientSock;
}


//-----------------------------------------------------------------------------
// releaseClientMigrationReportSock
//-----------------------------------------------------------------------------
int castor::tape::tapebridge::BridgeSocketCatalogue::
  releaseClientMigrationReportSock(uint64_t &tapebridgeTransId)
  throw(castor::exception::Exception) {

  // Throw an exception if the socket-descriptor has not been set
  if(0 > m_migrationReportConnection.clientSock) {
    TAPE_THROW_CODE(ENOENT,
      ": Client migration-report socket-descriptor does not exist in the"
      " catalogue");
  }

  // Release and return the socket-descriptor
  tapebridgeTransId = m_migrationReportConnection.tapebridgeTransId;
  const int tmpSock = m_migrationReportConnection.clientSock;
  m_migrationReportConnection.clientSock = -1;
  m_migrationReportConnection.tapebridgeTransId = 0;
  return tmpSock;
}


//-----------------------------------------------------------------------------
// addGetMoreWorkConnection
//-----------------------------------------------------------------------------
void castor::tape::tapebridge::BridgeSocketCatalogue::
  addGetMoreWorkConnection(
  const int         rtcpdSock,
  const uint32_t    rtcpdReqMagic,
  const uint32_t    rtcpdReqType,
  const std::string &rtcpdReqTapePath,
  const int         clientSock,
  const uint64_t    tapebridgeTransId)
  throw(castor::exception::InvalidArgument, castor::exception::Exception) {

  if(0 > rtcpdSock || FD_SETSIZE <= rtcpdSock) {
    TAPE_THROW_EX(castor::exception::InvalidArgument,
      ": Invalid rtcpd socket-descriptor"
      ": rtcpdSock=" << rtcpdSock);
  }

  if(0 > clientSock || FD_SETSIZE <= clientSock) {
    TAPE_THROW_EX(castor::exception::InvalidArgument,
      ": Invalid client socket-descriptor"
      ": Value is negative"
      ": clientSock=" << clientSock);
  }

  // Throw an exception if the "get more work" socket-descriptor has already
  // been set
  if(0 <= m_getMoreWorkConnection.clientSock) {
    TAPE_THROW_CODE(EEXIST,
      ": The get more work socket-descriptor is already set"
      ": current value=" << m_getMoreWorkConnection.clientSock <<
      ": new value =" << clientSock);
  }

  // Throw an exception if the specified rtcpd disk/tape IO control-connection
  // does not exist in the catalogue
  {
    std::set<int>::iterator itor = m_rtcpdIOControlConns.find(rtcpdSock);
    if(m_rtcpdIOControlConns.end() == itor) {
      TAPE_THROW_CODE(ENOENT,
      ": Rtcpd socket-descriptor does not exist in the catalogue"
      ": rtcpdSock=" << rtcpdSock);
    }
  }

  // Set the "get more work" connection information
  m_getMoreWorkConnection.rtcpdSock         = rtcpdSock;
  m_getMoreWorkConnection.rtcpdReqMagic     = rtcpdReqMagic;
  m_getMoreWorkConnection.rtcpdReqType      = rtcpdReqType;
  m_getMoreWorkConnection.rtcpdReqTapePath  = rtcpdReqTapePath;
  m_getMoreWorkConnection.clientSock        = clientSock;
  m_getMoreWorkConnection.tapebridgeTransId = tapebridgeTransId;
  gettimeofday(&(m_getMoreWorkConnection.clientReqTimeStamp), NULL);

  // Store an entry in the client request history
  m_clientReqHistory.push_back(ClientReqHistoryElement(clientSock,
    m_getMoreWorkConnection.clientReqTimeStamp));
}


//-----------------------------------------------------------------------------
// getMoreWorkConnectionIsSet
//-----------------------------------------------------------------------------
bool castor::tape::tapebridge::BridgeSocketCatalogue::
  getMoreWorkConnectionIsSet() const {
  return 0 <= m_getMoreWorkConnection.clientSock;
}


//-----------------------------------------------------------------------------
// getGetMoreWorkConnection
//-----------------------------------------------------------------------------
const castor::tape::tapebridge::GetMoreWorkConnection
  &castor::tape::tapebridge::BridgeSocketCatalogue::getGetMoreWorkConnection()
  const throw(castor::exception::NoValue, castor::exception::Exception) {

  if(0 > m_getMoreWorkConnection.clientSock) {
    TAPE_THROW_EX(castor::exception::NoValue,
      ": The get more work connection has not been set");
  }

  return m_getMoreWorkConnection;
}


//-----------------------------------------------------------------------------
// releaseGetMoreWorkClientSock
//-----------------------------------------------------------------------------
int castor::tape::tapebridge::BridgeSocketCatalogue::
  releaseGetMoreWorkClientSock() throw(castor::exception::Exception) {

  // Throw an exception if the socket-descriptor has not been set
  if(0 > m_getMoreWorkConnection.clientSock) {
    TAPE_THROW_CODE(ENOENT,
      ": Client get more work socket-descriptor does not exist in the"
      " catalogue");
  }

  // Release and return the socket-descriptor
  const int tmpSock = m_getMoreWorkConnection.clientSock;
  m_getMoreWorkConnection.clear();
  return tmpSock;
}
