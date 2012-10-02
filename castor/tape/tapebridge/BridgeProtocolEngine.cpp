/******************************************************************************
 *                      BridgeProtocolEngine.cpp
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
 *
 * @author Nicola.Bessone@cern.ch Steven.Murray@cern.ch
 *****************************************************************************/

#include "castor/Constants.hpp"
#include "castor/dlf/Dlf.hpp"
#include "castor/exception/Internal.hpp"
#include "castor/exception/PermissionDenied.hpp"
#include "castor/exception/TimeOut.hpp"
#include "castor/tape/Constants.hpp"
#include "castor/tape/legacymsg/TapeBridgeMarshal.hpp"
#include "castor/tape/tapebridge/DlfMessageConstants.hpp"
#include "castor/tape/tapebridge/BridgeProtocolEngine.hpp"
#include "castor/tape/tapebridge/ClientProxy.hpp"
#include "castor/tape/tapebridge/Constants.hpp"
#include "castor/tape/tapebridge/DlfMessageConstants.hpp"
#include "castor/tape/tapebridge/LegacyTxRx.hpp"
#include "castor/tape/tapebridge/RequestToMigrateFile.hpp"
#include "castor/tape/tapebridge/RtcpTxRx.hpp"
#include "castor/tape/tapebridge/TapeFlushConfigParams.hpp"
#include "castor/tape/net/net.hpp"
#include "castor/tape/tapegateway/EndNotification.hpp"
#include "castor/tape/tapegateway/EndNotificationErrorReport.hpp"
#include "castor/tape/tapegateway/FileErrorReportStruct.hpp"
#include "castor/tape/tapegateway/FileMigratedNotificationStruct.hpp"
#include "castor/tape/tapegateway/FileMigrationReportList.hpp"
#include "castor/tape/tapegateway/FileRecalledNotificationStruct.hpp"
#include "castor/tape/tapegateway/FileRecallReportList.hpp"
#include "castor/tape/tapegateway/FilesToMigrateList.hpp"
#include "castor/tape/tapegateway/FilesToMigrateListRequest.hpp"
#include "castor/tape/tapegateway/FilesToRecallList.hpp"
#include "castor/tape/tapegateway/FilesToRecallListRequest.hpp"
#include "castor/tape/tapegateway/FileToMigrateStruct.hpp"
#include "castor/tape/tapegateway/FileToRecallStruct.hpp"
#include "castor/tape/tapegateway/NoMoreFiles.hpp"
#include "castor/tape/tapegateway/NotificationAcknowledge.hpp"
#include "castor/tape/utils/SmartFd.hpp"
#include "castor/tape/utils/SmartFdList.hpp"
#include "castor/tape/utils/utils.hpp"
#include "h/Ctape_constants.h"
#include "h/getconfent.h"
#include "h/rtcp_constants.h"
#include "h/rtcpd_constants.h"
#include "h/tapebridge_constants.h"
#include "h/tapeBridgeFlushedToTapeMsgBody.h"

#include <algorithm>
#include <exception>
#include <list>
#include <memory>
#include <sstream>
#include <sys/time.h>


//-----------------------------------------------------------------------------
// constructor
//-----------------------------------------------------------------------------
castor::tape::tapebridge::BridgeProtocolEngine::BridgeProtocolEngine(
  IFileCloser                         &fileCloser,
  const BulkRequestConfigParams       &bulkRequestConfigParams,
  const TapeFlushConfigParams         &tapeFlushConfigParams,
  const Cuuid_t                       &cuuid,
  const int                           listenSock,
  const int                           initialRtcpdSock,
  const legacymsg::RtcpJobRqstMsgBody &jobRequest,
  const tapegateway::Volume           &volume,
  const uint32_t                      nbFilesOnDestinationTape,
  utils::BoolFunctor                  &stoppingGracefully,
  Counter<uint64_t>                   &tapebridgeTransactionCounter,
  const bool                          logPeerOfCallbackConnectionsFromRtcpd,
  const bool                          checkRtcpdIsConnectingFromLocalHost,
  IClientProxy                        &clientProxy,
  ILegacyTxRx                         &legacyTxRx)
  throw(castor::exception::Exception) :
  m_fileCloser(fileCloser),
  m_bulkRequestConfigParams(bulkRequestConfigParams),
  m_tapeFlushConfigParams(tapeFlushConfigParams),
  m_cuuid(cuuid),
  m_sockCatalogue(fileCloser),
  m_jobRequest(jobRequest),
  m_volume(volume),
  m_nextDestinationTapeFSeq(nbFilesOnDestinationTape + 1),
  m_stoppingGracefully(stoppingGracefully),
  m_tapebridgeTransactionCounter(tapebridgeTransactionCounter),
  m_timeOfLastRtcpdPing(time(NULL)),
  m_timeOfLastClientPing(time(NULL)),
  m_nbReceivedENDOF_REQs(0),
  m_pendingTransferIds(MAXPENDINGTRANSFERS),
  m_pendingMigrationsStore(tapeFlushConfigParams),
  m_sessionWithRtcpdIsFinished(false),
  m_logPeerOfCallbackConnectionsFromRtcpd(
    logPeerOfCallbackConnectionsFromRtcpd),
  m_checkRtcpdIsConnectingFromLocalHost(checkRtcpdIsConnectingFromLocalHost),
  m_clientProxy(clientProxy),
  m_legacyTxRx(legacyTxRx),
  m_sessionErrors(cuuid, jobRequest, volume) {

  // Store the listen socket and initial rtcpd connection in the socket
  // catalogue
  m_sockCatalogue.addListenSock(listenSock);
  m_sockCatalogue.addInitialRtcpdConn(initialRtcpdSock);
}


//-----------------------------------------------------------------------------
// acceptRtcpdConnection
//-----------------------------------------------------------------------------
int castor::tape::tapebridge::BridgeProtocolEngine::acceptRtcpdConnection()
  throw(castor::exception::Exception) {

  utils::SmartFd connectedSock;
  const int timeout = 5; // Seconds

  bool connectionAccepted = false;
  for(int i=0; i<timeout && !connectionAccepted; i++) {
    try {
      const time_t timeout = 1; // Timeout in seconds
      connectedSock.reset(net::acceptConnection(m_sockCatalogue.getListenSock(),
        timeout));
      connectionAccepted = true;
    } catch(castor::exception::TimeOut &ex) {
      // Do nothing
    }

    // Throw an exception if the daemon is stopping gracefully
    if(m_stoppingGracefully()) {
      castor::exception::Exception ex(ECANCELED);

      ex.getMessage() << "Stopping gracefully";
      throw(ex);
    }
  }

  // Throw an exception if the rtcpd connection could not be accepted
  if(!connectionAccepted) {
    castor::exception::TimeOut ex;

    ex.getMessage() <<
      "Failed to accept rtcpd connection after " << timeout << " seconds";
    throw ex;
  }

  if(m_logPeerOfCallbackConnectionsFromRtcpd) {
    logCallbackConnectionFromRtcpd(connectedSock.get());
  }

  return connectedSock.release();
}


//-----------------------------------------------------------------------------
// logCallbackConnectionFromRtcpd
//-----------------------------------------------------------------------------
void castor::tape::tapebridge::BridgeProtocolEngine::
  logCallbackConnectionFromRtcpd(
  const int connectedSock)
  throw(castor::exception::Exception) {
  try {
    char hostName[net::HOSTNAMEBUFLEN];

    const net::IpAndPort peerIpAndPort = net::getPeerIpPort(connectedSock);
    net::getPeerHostName(connectedSock, hostName);

    castor::dlf::Param params[] = {
      castor::dlf::Param("mountTransactionId", m_jobRequest.volReqId       ),
      castor::dlf::Param("volReqId"          , m_jobRequest.volReqId       ),
      castor::dlf::Param("TPVID"             , m_volume.vid()              ),
      castor::dlf::Param("driveUnit"         , m_jobRequest.driveUnit      ),
      castor::dlf::Param("dgn"               , m_jobRequest.dgn            ),
      castor::dlf::Param("clientHost"        , m_jobRequest.clientHost     ),
      castor::dlf::Param("clientPort"        , m_jobRequest.clientPort     ),
      castor::dlf::Param("clientType",
        utils::volumeClientTypeToString(m_volume.clientType())),
      castor::dlf::Param("IP"                ,
        castor::dlf::IPAddress(peerIpAndPort.getIp())),
      castor::dlf::Param("Port"              , peerIpAndPort.getPort()     ),
      castor::dlf::Param("HostName"          , hostName                    ),
      castor::dlf::Param("socketFd"          , connectedSock               ),
      castor::dlf::Param("nbDiskTapeConns"   ,
        m_sockCatalogue.getNbDiskTapeIOControlConns())};
    castor::dlf::dlf_writep(m_cuuid, DLF_LVL_SYSTEM, TAPEBRIDGE_RTCPD_CALLBACK,
      params);
  } catch(castor::exception::Exception &ex) {
    TAPE_THROW_CODE(ECANCELED,
      ": Failed to get IP, port and host name"
      ": volReqId=" << m_jobRequest.volReqId <<
      " nbDiskTapeConns=" << m_sockCatalogue.getNbDiskTapeIOControlConns() <<
      ": " << ex.getMessage().str());
  } catch(std::exception &se) {
    TAPE_THROW_CODE(ECANCELED,
      ": Failed to get IP, port and host name"
      ": volReqId=" << m_jobRequest.volReqId <<
      " nbDiskTapeConns=" << m_sockCatalogue.getNbDiskTapeIOControlConns() <<
      ": " << se.what());
  } catch(...) {
    TAPE_THROW_CODE(ECANCELED,
      ": Failed to get IP, port and host name"
      ": volReqId=" << m_jobRequest.volReqId <<
      " nbDiskTapeConns=" << m_sockCatalogue.getNbDiskTapeIOControlConns() <<
      ": Caught an unknown exception");
  }
}


//-----------------------------------------------------------------------------
// runRtcpdSession
//-----------------------------------------------------------------------------
void castor::tape::tapebridge::BridgeProtocolEngine::
  runRtcpdSession() throw() {
  try {
    // Inner try and catch to convert std::exception and unknown exceptions
    // (...) to castor::exception::Exception
    try {
      processSocksInALoop();
    } catch(std::exception &se) {
      TAPE_THROW_EX(castor::exception::Internal,
        ": Caught a std::exception"
        ": what=" << se.what());
    } catch(castor::exception::Exception &ex) {
      // Simply rethrow as there is no need to convert exception type
      throw ex;
    } catch(...) {
      TAPE_THROW_EX(castor::exception::Internal,
        ": Caught an unknown exception");
    }
  } catch(castor::exception::Exception &ex) {
    castor::dlf::Param params[] = {
      castor::dlf::Param("mountTransactionId", m_jobRequest.volReqId  ),
      castor::dlf::Param("volReqId"          , m_jobRequest.volReqId  ),
      castor::dlf::Param("TPVID"             , m_volume.vid()         ),
      castor::dlf::Param("driveUnit"         , m_jobRequest.driveUnit ),
      castor::dlf::Param("dgn"               , m_jobRequest.dgn       ),
      castor::dlf::Param("clientHost"        , m_jobRequest.clientHost),
      castor::dlf::Param("clientPort"        , m_jobRequest.clientPort),
      castor::dlf::Param("clientType",
        utils::volumeClientTypeToString(m_volume.clientType())),
      castor::dlf::Param("errorCode"         , ex.code()              ),
      castor::dlf::Param("errorMessage"      , ex.getMessage().str()  )};
    castor::dlf::dlf_writep(m_cuuid, DLF_LVL_ERROR,
      TAPEBRIDGE_EXCEPTION_RUNNING_RTCPD_SESSION, params);

    // Gather the error information into an SessionError object
    SessionError sessionError;
    sessionError.setErrorCode(ex.code());
    sessionError.setErrorMessage(ex.getMessage().str());
    sessionError.setErrorScope(SessionError::SESSION_SCOPE);

    // Push the error onto the back of the list of errors generated during the
    // sesion with the rtcpd daemon
    m_sessionErrors.push_back(sessionError);
  }

  castor::dlf::Param params[] = {
    castor::dlf::Param("mountTransactionId", m_jobRequest.volReqId  ),
    castor::dlf::Param("volReqId"          , m_jobRequest.volReqId  ),
    castor::dlf::Param("TPVID"             , m_volume.vid()         ),
    castor::dlf::Param("driveUnit"         , m_jobRequest.driveUnit ),
    castor::dlf::Param("dgn"               , m_jobRequest.dgn       ),
    castor::dlf::Param("clientHost"        , m_jobRequest.clientHost),
    castor::dlf::Param("clientPort"        , m_jobRequest.clientPort),
    castor::dlf::Param("clientType",
      utils::volumeClientTypeToString(m_volume.clientType()))};
  castor::dlf::dlf_writep(m_cuuid, DLF_LVL_SYSTEM,
    TAPEBRIDGE_FINISHED_PROCESSING_SOCKETS, params);
}


//-----------------------------------------------------------------------------
// processSocksInALoop
//-----------------------------------------------------------------------------
void castor::tape::tapebridge::BridgeProtocolEngine::processSocksInALoop()
  throw(castor::exception::Exception) {

  const struct timeval selectTimeout = {1 /* tv_sec */, 0 /* tv_usec */};

  do {
    // Throw an exception if the daemon is stopping gracefully
    if(m_stoppingGracefully()) {
      castor::exception::Exception ex(ECANCELED);

      ex.getMessage() << "Stopping gracefully";
      throw(ex);
    }

    handleSelectEvents(selectTimeout);

    // Throw an exception if a timeout has occured
    m_sockCatalogue.checkForTimeout();

  } while(continueProcessingSocks());
}


//-----------------------------------------------------------------------------
// handleSelectEvents
//-----------------------------------------------------------------------------
void castor::tape::tapebridge::BridgeProtocolEngine::handleSelectEvents(
  struct timeval selectTimeout)
  throw(castor::exception::Exception) {
  int     maxFd = -1;
  fd_set  readFdSet;

  m_sockCatalogue.buildReadFdSet(readFdSet, maxFd);

  // Wait for up to 1 second to see if any read file descriptors are ready
  const int selectRc = select(maxFd + 1, &readFdSet, NULL, NULL,
    &selectTimeout);
  const int selectErrno = errno;

  switch(selectRc) {
  case 0: // Select timed out
  {
    const time_t now = time(NULL);

    // Ping rtcpd if the session with rtcpd is not finished and the
    // RTCPDPINGTIMEOUT has been reached
    if(!sessionWithRtcpdIsFinished() &&
      (now - m_timeOfLastRtcpdPing > RTCPDPINGTIMEOUT)) {
      m_timeOfLastRtcpdPing = now;
      RtcpTxRx::pingRtcpd(m_cuuid, m_jobRequest.volReqId,
        m_sockCatalogue.getInitialRtcpdConn(), RTCPDNETRWTIMEOUT);
    }

    // Ping the client if the ping timeout has been reached and the client is
    // readtp, writetp or dumptp
    if(
      (now - m_timeOfLastClientPing > CLIENTPINGTIMEOUT) &&
      (m_volume.clientType() == tapegateway::READ_TP  ||
       m_volume.clientType() == tapegateway::WRITE_TP ||
       m_volume.clientType() == tapegateway::DUMP_TP)) {

      m_timeOfLastClientPing = now;
      try {
        m_clientProxy.ping(m_tapebridgeTransactionCounter.next());
      } catch(castor::exception::Exception &ex) {
        TAPE_THROW_CODE(ex.code(),
          ": Failed to ping client"
          ": clientType=" <<
          utils::volumeClientTypeToString(m_volume.clientType()) <<
          ": " << ex.getMessage().str());
      }
    }

    break;
  }
  case -1: // Select encountered an error

    // If select was interrupted
    if(selectErrno == EINTR) {

      // Write a log message
      castor::dlf::Param params[] = {
        castor::dlf::Param("mountTransactionId", m_jobRequest.volReqId  ),
        castor::dlf::Param("volReqId"          , m_jobRequest.volReqId  ),
        castor::dlf::Param("TPVID"             , m_volume.vid()         ),
        castor::dlf::Param("driveUnit"         , m_jobRequest.driveUnit ),
        castor::dlf::Param("dgn"               , m_jobRequest.dgn       ),
        castor::dlf::Param("clientHost"        , m_jobRequest.clientHost),
        castor::dlf::Param("clientPort"        , m_jobRequest.clientPort),
        castor::dlf::Param("clientType",
      utils::volumeClientTypeToString(m_volume.clientType()))};
      castor::dlf::dlf_writep(m_cuuid, DLF_LVL_DEBUG,
        TAPEBRIDGE_SELECT_INTR, params);

    // Else select encountered an error other than an interruption
    } else {

      // Convert the error into an exception
      TAPE_THROW_CODE(selectErrno,
        ": Select encountered an error other than an interruption"
        ": " << sstrerror(selectErrno));
    }
    break;

  default: // One or more select file descriptors require attention

    processAPendingSocket(readFdSet);

  } // switch(selectRc)
}


//------------------------------------------------------------------------------
// continueProcessingSocks
//------------------------------------------------------------------------------
bool castor::tape::tapebridge::BridgeProtocolEngine::continueProcessingSocks()
  const {
  return !sessionWithRtcpdIsFinished() || !m_flushedBatches.empty() ||
    m_sockCatalogue.clientMigrationReportSockIsSet();
 }


//------------------------------------------------------------------------------
// processAPendingSocket
//------------------------------------------------------------------------------
void castor::tape::tapebridge::BridgeProtocolEngine::processAPendingSocket(
  fd_set &readFdSet) throw(castor::exception::Exception) {

  BridgeSocketCatalogue::SocketType sockType = BridgeSocketCatalogue::LISTEN;
  const int pendingSock = m_sockCatalogue.getAPendingSock(readFdSet, sockType);
  if(pendingSock == -1) {
    TAPE_THROW_EX(exception::Internal,
      ": Lost pending file descriptor");
  }

  switch(sockType) {
  case BridgeSocketCatalogue::LISTEN:
    processPendingListenSocket();
    break;
  case BridgeSocketCatalogue::INITIAL_RTCPD:
    processPendingInitialRtcpdSocket(pendingSock);
    break;
  case BridgeSocketCatalogue::RTCPD_DISK_TAPE_IO_CONTROL:
    processPendingRtcpdDiskTapeIOControlSocket(pendingSock);
    break;
  case BridgeSocketCatalogue::CLIENT_GET_MORE_WORK:
    processPendingClientGetMoreWorkSocket(pendingSock);
    break;
  case BridgeSocketCatalogue::CLIENT_MIGRATION_REPORT:
    processPendingClientMigrationReportSocket(pendingSock);
    break;
  default:
    TAPE_THROW_EX(exception::Internal,
      "Unknown socket type"
      ": socketType = " << sockType);
  }
}


//------------------------------------------------------------------------------
// processPendingListenSocket
//------------------------------------------------------------------------------
void castor::tape::tapebridge::BridgeProtocolEngine::
  processPendingListenSocket() throw(castor::exception::Exception) {
  const int acceptedConnection = acceptRtcpdConnection();

  // Accept the connection
  m_sockCatalogue.addRtcpdDiskTapeIOControlConn(acceptedConnection);

  if(m_checkRtcpdIsConnectingFromLocalHost) {
    // Throw an exception if connection is not from localhost
    checkPeerIsLocalhost(acceptedConnection);
  }
}


//-----------------------------------------------------------------------------
// checkPeerIsLocalhost
//-----------------------------------------------------------------------------
void castor::tape::tapebridge::BridgeProtocolEngine::checkPeerIsLocalhost(
  const int socketFd) throw(castor::exception::Exception) {

  const net::IpAndPort peerIpAndPort = net::getPeerIpPort(socketFd);

  // localhost = 127.0.0.1 = 0x7F000001
  if(peerIpAndPort.getIp() != 0x7F000001) {
    castor::exception::PermissionDenied ex;
    std::ostream &os = ex.getMessage();
    os <<
      "Peer is not local host"
      ": expected=127.0.0.1"
      ": actual=";
    net::writeIp(os, peerIpAndPort.getIp());

    throw ex;
  }
}


//------------------------------------------------------------------------------
// processPendingInitialRtcpdSocket
//------------------------------------------------------------------------------
void castor::tape::tapebridge::BridgeProtocolEngine::
  processPendingInitialRtcpdSocket(const int pendingSock)
  throw(castor::exception::Exception) {

  // Check function arguments
  if(pendingSock < 0) {
    TAPE_THROW_EX(castor::exception::InvalidArgument,
      ": Invalid method argument"
      ": pendingSock is an invalid socket descriptor"
      ": value=" << pendingSock);
  }

  // Determine whether rtcpd closed the connection or whether it sent some data
  bool rtcpdClosedConnection = false;
  try {
    char dummyBuf[1];
    rtcpdClosedConnection = net::readBytesFromCloseable(pendingSock,
      RTCPDNETRWTIMEOUT, sizeof(dummyBuf), dummyBuf);
  } catch(castor::exception::Exception &ex) {
    TAPE_THROW_EX(castor::exception::Internal,
      ": Failed to determine why the socket of the initial rtcpd connection"
      " has become pending"
      ": " << ex.getMessage().str());
  }

  if(rtcpdClosedConnection) {
    TAPE_THROW_CODE(ECANCELED,
      ": Initial rtcpd connection un-expectedly closed");
  } else {
    TAPE_THROW_CODE(ECANCELED,
      ": Received un-expected data from the initial rtcpd connection");
  }
}


//------------------------------------------------------------------------------
// processPendingRtcpdDiskTapeIOControlSocket
//------------------------------------------------------------------------------
void castor::tape::tapebridge::BridgeProtocolEngine::
  processPendingRtcpdDiskTapeIOControlSocket(const int pendingSock)
  throw(castor::exception::Exception) {

  // Check function arguments
  if(pendingSock < 0) {
    TAPE_THROW_EX(castor::exception::InvalidArgument,
      ": Invalid method argument"
      ": pendingSock is an invalid socket descriptor"
      ": value=" << pendingSock);
  }

  legacymsg::MessageHeader header;
  utils::setBytes(header, '\0');

  // Try to receive the message header which may not be possible; The file
  // descriptor may be ready because rtcpd has closed the connection
  {
    const bool peerClosed = m_legacyTxRx.receiveMsgHeaderFromCloseable(
      pendingSock, header);

    // If the peer closed its side of the connection, then close this side
    // of the connection and return in order to continue the RTCOPY session
    if(peerClosed) {
      m_fileCloser.closeFd(
        m_sockCatalogue.releaseRtcpdDiskTapeIOControlConn(pendingSock));

      castor::dlf::Param params[] = {
        castor::dlf::Param("mountTransactionId", m_jobRequest.volReqId  ),
        castor::dlf::Param("volReqId"          , m_jobRequest.volReqId  ),
        castor::dlf::Param("TPVID"             , m_volume.vid()         ),
        castor::dlf::Param("driveUnit"         , m_jobRequest.driveUnit ),
        castor::dlf::Param("dgn"               , m_jobRequest.dgn       ),
        castor::dlf::Param("clientHost"        , m_jobRequest.clientHost),
        castor::dlf::Param("clientPort"        , m_jobRequest.clientPort),
        castor::dlf::Param("clientType",
          utils::volumeClientTypeToString(m_volume.clientType())),
        castor::dlf::Param("socketFd"          , pendingSock            )};
      castor::dlf::dlf_writep(m_cuuid, DLF_LVL_SYSTEM,
        TAPEBRIDGE_CLOSED_RTCPD_DISK_TAPE_CONNECTION_DUE_TO_PEER, params);

      return; // Continue the RTCOPY session
    }
  }

  processRtcpdRequest(header, pendingSock);
}


//------------------------------------------------------------------------------
// processPendingClientGetMoreWorkSocket
//------------------------------------------------------------------------------
void castor::tape::tapebridge::BridgeProtocolEngine::
  processPendingClientGetMoreWorkSocket(const int pendingSock)
  throw(castor::exception::Exception) {

  // Check function arguments
  if(pendingSock < 0) {
    TAPE_THROW_EX(castor::exception::InvalidArgument,
      ": Invalid method argument"
      ": pendingSock is an invalid socket descriptor"
      ": value=" << pendingSock);
  }

  // Make a copy of the information about the "get more work" connection
  const GetMoreWorkConnection getMoreWorkConnection =
    m_sockCatalogue.getGetMoreWorkConnection();

  // Check the pending socket is the one to be expected
  if(pendingSock != getMoreWorkConnection.clientSock) {
    // This should never happen
    TAPE_THROW_EX(castor::exception::Internal,
      ": pendingSock is not equal to getMoreWorkConnection.clientSock"
      ": pendingSock=" << pendingSock <<
      " getMoreWorkConnection.clientSock=" <<
      getMoreWorkConnection.clientSock);
  }

  // Release the client-connection from the catalogue.  A copy of the value of
  // the socket-descriptor is stored in the getMoreWorkConnection local
  // variable.
  m_sockCatalogue.releaseGetMoreWorkClientSock();

  // Get the current time
  struct timeval now = {0, 0};
  gettimeofday(&now, NULL);

  std::auto_ptr<IObject> obj(m_clientProxy.receiveReplyAndClose(pendingSock));

  // Drop the client reply if the associated rtcpd-connection has been closed
  if(-1 == getMoreWorkConnection.rtcpdSock) {
    castor::dlf::Param params[] = {
      castor::dlf::Param("mountTransactionId"  , m_jobRequest.volReqId  ),
      castor::dlf::Param("volReqId"            , m_jobRequest.volReqId  ),
      castor::dlf::Param("TPVID"               , m_volume.vid()         ),
      castor::dlf::Param("driveUnit"           , m_jobRequest.driveUnit ),
      castor::dlf::Param("dgn"                 , m_jobRequest.dgn       ),
      castor::dlf::Param("clientHost"          , m_jobRequest.clientHost),
      castor::dlf::Param("clientPort"          , m_jobRequest.clientPort),
      castor::dlf::Param("clientType",
        utils::volumeClientTypeToString(m_volume.clientType()))};
    castor::dlf::dlf_writep(m_cuuid, DLF_LVL_SYSTEM,
      TAPEBRIDGE_DROPPING_CLIENT_REPLY_RTCPD_CONNECTION_CLOSED, params);
    return;
  }

  dispatchCallbackForReplyToGetMoreWork(obj.get(), getMoreWorkConnection);
}


//-----------------------------------------------------------------------------
// dispatchCallbackForReplyToGetMoreWork
//-----------------------------------------------------------------------------
void castor::tape::tapebridge::BridgeProtocolEngine::
  dispatchCallbackForReplyToGetMoreWork(
  IObject *const              obj,
  const GetMoreWorkConnection &getMoreWorkConnection)
  throw(castor::exception::Exception) {
  try {
    switch(obj->type()) {
    case OBJ_FilesToMigrateList:
      filesToMigrateListClientCallback(obj, getMoreWorkConnection);
      break;
    case OBJ_FilesToRecallList:
      filesToRecallListClientCallback(obj, getMoreWorkConnection);
      break;
    case OBJ_NoMoreFiles:
      noMoreFilesClientCallback(obj, getMoreWorkConnection);
      break;
    case OBJ_EndNotificationErrorReport:
      endNotificationErrorReportForGetMoreWorkClientCallback(obj,
        getMoreWorkConnection);
      break;
    default:
      TAPE_THROW_CODE(EBADMSG,
        ": Unknown client message object type"
        ": object type=0x"   << std::hex << obj->type() << std::dec);
    }
  } catch(castor::exception::Exception &ex) {
    TAPE_THROW_CODE(ECANCELED,
      ": message type=" << utils::objectTypeToString(obj->type()) <<
      ": " << ex.getMessage().str());
  }
}


//------------------------------------------------------------------------------
// processPendingClientMigrationReportSocket
//------------------------------------------------------------------------------
void castor::tape::tapebridge::BridgeProtocolEngine::
  processPendingClientMigrationReportSocket(const int pendingSock)
  throw(castor::exception::Exception) {

  // Check function arguments
  if(pendingSock < 0) {
    TAPE_THROW_EX(castor::exception::InvalidArgument,
      ": Invalid method argument"
      ": pendingSock is an invalid socket descriptor"
      ": value=" << pendingSock);
  }

  // Release the socket-descriptor from the catalogue
  uint64_t tapebridgeTransId = 0;
  utils::SmartFd catalogueSock(
    m_sockCatalogue.releaseClientMigrationReportSock(tapebridgeTransId));

  // Check for a mismatch between the pending and catalogue socket-decriptors
  if(pendingSock != catalogueSock.get()) {
      TAPE_THROW_EX(castor::exception::Internal,
        ": Client migration-report socket-descriptor mismatch"
        ": pendingSock=" << pendingSock <<
        ": catalogueSock=" << catalogueSock.get());
  }

  // Receive reply from client and close the connection
  const int closedClientSock = catalogueSock.release();
  try {
    m_clientProxy.receiveNotificationReplyAndClose(tapebridgeTransId,
      closedClientSock);
  } catch(castor::exception::Exception &ex) {

    // Gather the error information into an SessionError object
    SessionError sessionError;
    sessionError.setErrorCode(ex.code());
    sessionError.setErrorMessage(ex.getMessage().str());
    sessionError.setErrorScope(SessionError::SESSION_SCOPE);

    // Push the error onto the back of the list of errors generated during the
    // tape session
    m_sessionErrors.push_back(sessionError);
  }
  {
    castor::dlf::Param params[] = {
      castor::dlf::Param("tapebridgeTransId" , tapebridgeTransId      ),
      castor::dlf::Param("mountTransactionId", m_jobRequest.volReqId  ),
      castor::dlf::Param("volReqId"          , m_jobRequest.volReqId  ),
      castor::dlf::Param("TPVID"             , m_volume.vid()         ),
      castor::dlf::Param("driveUnit"         , m_jobRequest.driveUnit ),
      castor::dlf::Param("dgn"               , m_jobRequest.dgn       ),
      castor::dlf::Param("clientHost"        , m_jobRequest.clientHost),
      castor::dlf::Param("clientPort"        , m_jobRequest.clientPort),
      castor::dlf::Param("clientType",
        utils::volumeClientTypeToString(m_volume.clientType())),
      castor::dlf::Param("clientSock"        , closedClientSock       )};
    castor::dlf::dlf_writep(m_cuuid, DLF_LVL_SYSTEM,
      TAPEBRIDGE_RECEIVED_ACK_OF_NOTIFICATION, params);
  }

  // If there is a pending batch of flushed files to be reported to the client,
  // then send the batch to the client
  if(!m_flushedBatches.empty()) {
    const FileWrittenNotificationList migrations = m_flushedBatches.front();
    m_flushedBatches.pop_front();
    sendFlushedMigrationsToClient(migrations);
  }
}


//-----------------------------------------------------------------------------
// run
//-----------------------------------------------------------------------------
void castor::tape::tapebridge::BridgeProtocolEngine::run() throw() {

  // startRtcpdSession() does not throw any exceptions
  const bool rtcpdSessionStarted = startRtcpdSession();

  if(rtcpdSessionStarted) {
    // runRtcpdSession() does not throw any exceptions
    runRtcpdSession();
  }

  // notifyClientOfAnyFileSpecificErrors() does not throw any exceptions
  notifyClientOfAnyFileSpecificErrors();

  // Delay the end of the rtcpd-session to the last minute in order to delay
  // the un-mounting of the tape that in turn frees the drive.
  //
  // endRtcpdSession() does not throw any exceptions
  endRtcpdSession();

  // notifyClientEndOfSession() does not throw any exceptions
  notifyClientEndOfSession();
}


//-----------------------------------------------------------------------------
// startRtcpdSession
//-----------------------------------------------------------------------------
bool castor::tape::tapebridge::BridgeProtocolEngine::startRtcpdSession()
  throw() {

  bool rtcpdSessionStarted = false;

  try {

    // Inner try and catch to convert std::exception and unknown exceptions
    // (...) to castor::exception::Exception
    try {

      // The rtcpd-session is started differently depending on the tape
      // access-mode
      switch(m_volume.mode()) {
      case tapegateway::READ:
        startRecallSession();
        rtcpdSessionStarted = true;
        break;
      case tapegateway::WRITE:
        rtcpdSessionStarted = startMigrationSession();
        break;
      case tapegateway::DUMP:
        startDumpSession();
        rtcpdSessionStarted = true;
        break;
      default:
        TAPE_THROW_EX(castor::exception::Internal,
          ": Unknown VolumeMode"
          ": Actual=" << m_volume.mode());
      }
    } catch(std::exception &se) {
      TAPE_THROW_EX(castor::exception::Internal,
        ": Caught a std::exception"
        ": what=" << se.what());
    } catch(castor::exception::Exception &ex) {
      // Simply rethrow as there is no need to convert exception type
      throw ex;
    } catch(...) {
      TAPE_THROW_EX(castor::exception::Internal,
        ": Caught an unknown exception");
    }

  } catch(castor::exception::Exception &ex) {
    castor::dlf::Param params[] = {
      castor::dlf::Param("mountTransactionId", m_jobRequest.volReqId  ),
      castor::dlf::Param("volReqId"          , m_jobRequest.volReqId  ),
      castor::dlf::Param("TPVID"             , m_volume.vid()         ),
      castor::dlf::Param("driveUnit"         , m_jobRequest.driveUnit ),
      castor::dlf::Param("dgn"               , m_jobRequest.dgn       ),
      castor::dlf::Param("clientHost"        , m_jobRequest.clientHost),
      castor::dlf::Param("clientPort"        , m_jobRequest.clientPort),
      castor::dlf::Param("clientType",
        utils::volumeClientTypeToString(m_volume.clientType())),
      castor::dlf::Param("errorCode"         , ex.code()              ),
      castor::dlf::Param("errorMessage"      , ex.getMessage().str()  )};
    castor::dlf::dlf_writep(m_cuuid, DLF_LVL_ERROR,
      TAPEBRIDGE_FAILED_TO_START_RTCPD_SESSION, params);

    // Gather the error information into an SessionError object
    SessionError sessionError;
    sessionError.setErrorCode(ex.code());
    sessionError.setErrorMessage(ex.getMessage().str());
    sessionError.setErrorScope(SessionError::SESSION_SCOPE);

    // Push the error onto the back of the list of errors generated during the
    // sesion with the rtcpd daemon
    m_sessionErrors.push_back(sessionError);

    rtcpdSessionStarted = false;
  }

  return rtcpdSessionStarted;
}


//-----------------------------------------------------------------------------
// startMigrationSession
//-----------------------------------------------------------------------------
bool castor::tape::tapebridge::BridgeProtocolEngine::startMigrationSession()
  throw(castor::exception::Exception) {

  const char *const task = "start migration session";

  // Send the request for the first file to migrate to the client
  const uint64_t tapebridgeTransId =
    m_tapebridgeTransactionCounter.next();
  const uint64_t maxFiles = 1;
  const uint64_t maxBytes = 1;
  utils::SmartFd clientSock(m_clientProxy.sendFilesToMigrateListRequest(
    tapebridgeTransId, maxFiles, maxBytes));
  {
    castor::dlf::Param params[] = {
      castor::dlf::Param("tapebridgeTransId" , tapebridgeTransId      ),
      castor::dlf::Param("mountTransactionId", m_jobRequest.volReqId  ),
      castor::dlf::Param("volReqId"          , m_jobRequest.volReqId  ),
      castor::dlf::Param("TPVID"             , m_volume.vid()         ),
      castor::dlf::Param("driveUnit"         , m_jobRequest.driveUnit ),
      castor::dlf::Param("dgn"               , m_jobRequest.dgn       ),
      castor::dlf::Param("clientHost"        , m_jobRequest.clientHost),
      castor::dlf::Param("clientPort"        , m_jobRequest.clientPort),
      castor::dlf::Param("maxFiles"          , maxFiles               ),
      castor::dlf::Param("maxBytes"          , maxBytes               ),
      castor::dlf::Param("clientType"        ,
        utils::volumeClientTypeToString(m_volume.clientType())),
      castor::dlf::Param("clientSock"        , clientSock.get()       )};
    castor::dlf::dlf_writep(m_cuuid, DLF_LVL_SYSTEM,
      TAPEBRIDGE_SENT_FILESTOMIGRATELISTREQUEST, params);
  }

  // Receive the reply
  const int closedClientSock = clientSock.release();
  std::auto_ptr<tapegateway::FilesToMigrateList> filesFromClient(
    m_clientProxy.receiveFilesToMigrateListRequestReplyAndClose(
      tapebridgeTransId, closedClientSock));

  if(filesFromClient.get() != NULL) {
    castor::dlf::Param params[] = {
      castor::dlf::Param("tapebridgeTransId",
        filesFromClient->aggregatorTransactionId()),
      castor::dlf::Param("mountTransactionId",
        filesFromClient->mountTransactionId()),
      castor::dlf::Param("volReqId"  , m_jobRequest.volReqId  ),
      castor::dlf::Param("TPVID"     , m_volume.vid()         ),
      castor::dlf::Param("driveUnit" , m_jobRequest.driveUnit ),
      castor::dlf::Param("dgn"       , m_jobRequest.dgn       ),
      castor::dlf::Param("clientHost", m_jobRequest.clientHost),
      castor::dlf::Param("clientPort", m_jobRequest.clientPort),
      castor::dlf::Param("clientType",
        utils::volumeClientTypeToString(m_volume.clientType())),
      castor::dlf::Param("clientSock", closedClientSock       ),
      castor::dlf::Param("nbFiles"   ,
        filesFromClient->filesToMigrate().size())};
    castor::dlf::dlf_writep(m_cuuid, DLF_LVL_SYSTEM,
      TAPEBRIDGE_RECEIVED_FILESTOMIGRATELIST, params);
  } else {
    castor::dlf::Param params[] = {
      castor::dlf::Param("mountTransactionId", m_jobRequest.volReqId  ),
      castor::dlf::Param("volReqId"          , m_jobRequest.volReqId  ),
      castor::dlf::Param("TPVID"             , m_volume.vid()         ),
      castor::dlf::Param("driveUnit"         , m_jobRequest.driveUnit ),
      castor::dlf::Param("dgn"               , m_jobRequest.dgn       ),
      castor::dlf::Param("clientHost"        , m_jobRequest.clientHost),
      castor::dlf::Param("clientPort"        , m_jobRequest.clientPort),
      castor::dlf::Param("clientType",
        utils::volumeClientTypeToString(m_volume.clientType())),
      castor::dlf::Param("clientSock"        , closedClientSock       )};
    castor::dlf::dlf_writep(m_cuuid, DLF_LVL_DEBUG,
      TAPEBRIDGE_RECEIVED_NOMOREFILES, params);
  }

  // If there is no file to migrate then don't start the tape-session with the
  // rtcpd daemon
  if(filesFromClient.get() == NULL) {
    return false;
  }

  // For the very first file to migrate the FilesToMigrateList message
  // should only contain one file
  if(1 != filesFromClient->filesToMigrate().size()) {
    TAPE_THROW_CODE(ENOTSUP,
      ": Failed to " << task <<
      ": The FilesToMigrateList message for the first file to migrate should"
      " only contain one file"
      ": nbFiles=" << filesFromClient->filesToMigrate().size());
  }
  const tapegateway::FileToMigrateStruct *const firstFileToMigrate =
    filesFromClient->filesToMigrate()[0];

  if(firstFileToMigrate->fseq() != m_nextDestinationTapeFSeq) {
    castor::exception::Exception ex(ECANCELED);

    ex.getMessage() <<
      ": Failed to " << task <<
      ": Invalid tape file sequence number from client"
      ": expected=" << (m_nextDestinationTapeFSeq) <<
      ": actual=" << firstFileToMigrate->fseq();

    throw(ex);
  }

  m_nextDestinationTapeFSeq++;

  // Remember the file transaction ID and get its unique index to be passed
  // to rtcpd through the "rtcpFileRequest.disk_fseq" message field
  uint32_t diskFSeq = 0;
  try {
    diskFSeq = m_pendingTransferIds.insert(
      firstFileToMigrate->fileTransactionId());
  } catch(castor::exception::Exception &ex) {
    // Add context and rethrow
    castor::exception::Exception ex2(ex.code());
    ex2.getMessage() <<
      "Failed to " << task <<
      ": " << ex.getMessage().str();
    throw(ex2);
  }

  // Give volume to rtcpd
  legacymsg::RtcpTapeRqstErrMsgBody rtcpVolume;
  utils::setBytes(rtcpVolume, '\0');
  utils::copyString(rtcpVolume.vid    , m_volume.vid().c_str()    );
  utils::copyString(rtcpVolume.vsn    , EMPTYVSN                  );
  utils::copyString(rtcpVolume.label  , m_volume.label().c_str()  );
  utils::copyString(rtcpVolume.density, m_volume.density().c_str());
  utils::copyString(rtcpVolume.unit   , m_jobRequest.driveUnit    );
  rtcpVolume.volReqId       = m_jobRequest.volReqId;
  rtcpVolume.mode           = WRITE_ENABLE;
  rtcpVolume.tStartRequest  = time(NULL);
  rtcpVolume.err.severity   = 1;
  rtcpVolume.err.maxTpRetry = 1; // Only try once - no retries
  rtcpVolume.err.maxCpRetry = 1; // Only try once - no retries
  RtcpTxRx::giveVolumeToRtcpd(m_cuuid, m_jobRequest.volReqId,
    m_sockCatalogue.getInitialRtcpdConn(), RTCPDNETRWTIMEOUT, rtcpVolume);

  // Add the first file to migrate to the pending migrations store
  {
    RequestToMigrateFile request;

    request.fileTransactionId = firstFileToMigrate->fileTransactionId();
    request.nsHost = firstFileToMigrate->nshost();
    request.nsFileId = firstFileToMigrate->fileid();
    request.positionCommandCode =
      firstFileToMigrate->positionCommandCode();
    request.tapeFSeq = firstFileToMigrate->fseq();
    request.fileSize = firstFileToMigrate->fileSize();
    request.lastKnownFilename = firstFileToMigrate->lastKnownFilename();
    request.lastModificationTime =
      firstFileToMigrate->lastModificationTime();
    request.path = firstFileToMigrate->path();
    request.umask = firstFileToMigrate->umask();

    m_pendingMigrationsStore.receivedRequestToMigrateFile(request);
  }
  {
    castor::dlf::Param params[] = {
      castor::dlf::Param("tapebridgeTransId" ,
        filesFromClient->aggregatorTransactionId()),
      castor::dlf::Param("mountTransactionId",
        filesFromClient->mountTransactionId()),
      castor::dlf::Param("volReqId", m_jobRequest.volReqId),
      castor::dlf::Param("TPVID", m_volume.vid()),
      castor::dlf::Param("driveUnit", m_jobRequest.driveUnit),
      castor::dlf::Param("dgn", m_jobRequest.dgn),
      castor::dlf::Param("clientHost", m_jobRequest.clientHost),
      castor::dlf::Param("clientPort", m_jobRequest.clientPort),
      castor::dlf::Param("clientType",
        utils::volumeClientTypeToString(m_volume.clientType())),
      castor::dlf::Param("clientSock", closedClientSock),
      castor::dlf::Param("fileTransactionId",
        firstFileToMigrate->fileTransactionId()),
      castor::dlf::Param("NSHOSTNAME", firstFileToMigrate->nshost()),
      castor::dlf::Param("NSFILEID", firstFileToMigrate->fileid()),
      castor::dlf::Param("tapeFSeq", firstFileToMigrate->fseq())};
    castor::dlf::dlf_writep(m_cuuid, DLF_LVL_SYSTEM,
      TAPEBRIDGE_ADDED_PENDING_MIGRATION_TO_STORE, params);
  }

  // Give first file to migrate to rtcpd
  {
    char migrationTapeFileId[CA_MAXPATHLEN+1];
    generateMigrationTapeFileId((uint64_t)firstFileToMigrate->fileid(),
      migrationTapeFileId);
    unsigned char blockId[4];
    utils::setBytes(blockId, '\0');
    char nshost[CA_MAXHOSTNAMELEN+1];
    utils::copyString(nshost, firstFileToMigrate->nshost().c_str());

    RtcpTxRx::giveFileToRtcpd(
      m_cuuid,
      m_jobRequest.volReqId,
      m_sockCatalogue.getInitialRtcpdConn(),
      RTCPDNETRWTIMEOUT,
      rtcpVolume.mode,
      firstFileToMigrate->path().c_str(),
      firstFileToMigrate->fileSize(),
      "",
      RECORDFORMAT,
      migrationTapeFileId,
      RTCOPYCONSERVATIVEUMASK,
      (int32_t)firstFileToMigrate->positionCommandCode(),
      (int32_t)firstFileToMigrate->fseq(),
      diskFSeq,
      nshost,
      (uint64_t)firstFileToMigrate->fileid(),
      blockId);
  }

  // Ask rtcpd to request more work
  {
    const char *const tapePath = "";
    RtcpTxRx::askRtcpdToRequestMoreWork(m_cuuid, m_jobRequest.volReqId,
      tapePath, m_sockCatalogue.getInitialRtcpdConn(), RTCPDNETRWTIMEOUT,
      WRITE_ENABLE);
  }

  // Tell rtcpd end of file list
  RtcpTxRx::tellRtcpdEndOfFileList(m_cuuid, m_jobRequest.volReqId,
    m_sockCatalogue.getInitialRtcpdConn(), RTCPDNETRWTIMEOUT);

  return true;
}


//-----------------------------------------------------------------------------
// startRecallSession
//-----------------------------------------------------------------------------
void castor::tape::tapebridge::BridgeProtocolEngine::startRecallSession()
  throw(castor::exception::Exception) {

  // Give volume to rtcpd
  legacymsg::RtcpTapeRqstErrMsgBody rtcpVolume;
  utils::setBytes(rtcpVolume, '\0');
  utils::copyString(rtcpVolume.vid    , m_volume.vid().c_str()    );
  utils::copyString(rtcpVolume.vsn    , EMPTYVSN                  );
  utils::copyString(rtcpVolume.label  , m_volume.label().c_str()  );
  utils::copyString(rtcpVolume.density, m_volume.density().c_str());
  utils::copyString(rtcpVolume.unit   , m_jobRequest.driveUnit    );
  rtcpVolume.volReqId       = m_jobRequest.volReqId;
  rtcpVolume.mode           = WRITE_DISABLE;
  rtcpVolume.tStartRequest  = time(NULL);
  rtcpVolume.err.severity   = 1;
  rtcpVolume.err.maxTpRetry = 1; // Only try once - no retries
  rtcpVolume.err.maxCpRetry = 1; // Only try once - no retries
  RtcpTxRx::giveVolumeToRtcpd(m_cuuid, m_jobRequest.volReqId,
    m_sockCatalogue.getInitialRtcpdConn(), RTCPDNETRWTIMEOUT, rtcpVolume);

  // Ask rtcpd to request more work
  {
     const char *const tapePath = "";
    RtcpTxRx::askRtcpdToRequestMoreWork(m_cuuid, m_jobRequest.volReqId,
      tapePath, m_sockCatalogue.getInitialRtcpdConn(), RTCPDNETRWTIMEOUT,
      WRITE_DISABLE);
  }

  // Tell rtcpd end of file list
  RtcpTxRx::tellRtcpdEndOfFileList(m_cuuid, m_jobRequest.volReqId,
    m_sockCatalogue.getInitialRtcpdConn(), RTCPDNETRWTIMEOUT);
}


//-----------------------------------------------------------------------------
// startDumpSession
//-----------------------------------------------------------------------------
void castor::tape::tapebridge::BridgeProtocolEngine::startDumpSession()
  throw(castor::exception::Exception) {

  // Give volume to rtcpd
  legacymsg::RtcpTapeRqstErrMsgBody rtcpVolume;
  utils::setBytes(rtcpVolume, '\0');
  utils::copyString(rtcpVolume.vid    , m_volume.vid().c_str()    );
  utils::copyString(rtcpVolume.vsn    , EMPTYVSN                  );
  utils::copyString(rtcpVolume.label  , m_volume.label().c_str()  );
  utils::copyString(rtcpVolume.density, m_volume.density().c_str());
  utils::copyString(rtcpVolume.unit   , m_jobRequest.driveUnit    );
  rtcpVolume.volReqId       = m_jobRequest.volReqId;
  rtcpVolume.mode           = WRITE_DISABLE;
  rtcpVolume.tStartRequest  = time(NULL);
  rtcpVolume.err.severity   = 1;
  rtcpVolume.err.maxTpRetry = 1; // Only try once - no retries
  rtcpVolume.err.maxCpRetry = 1; // Only try once - no retries
  RtcpTxRx::giveVolumeToRtcpd(m_cuuid, m_jobRequest.volReqId,
    m_sockCatalogue.getInitialRtcpdConn(), RTCPDNETRWTIMEOUT, rtcpVolume);

  // Get dump parameters message
  std::auto_ptr<tapegateway::DumpParameters> dumpParameters(
    m_clientProxy.getDumpParameters(m_tapebridgeTransactionCounter.next()));

  // Tell rtcpd to dump the tape
  legacymsg::RtcpDumpTapeRqstMsgBody request;
  request.maxBytes      = dumpParameters->maxBytes();
  request.blockSize     = dumpParameters->blockSize();
  request.convert       = dumpParameters->converter();
  request.tapeErrAction = dumpParameters->errAction();
  request.startFile     = dumpParameters->startFile();
  request.maxFiles      = dumpParameters->maxFile();
  request.fromBlock     = dumpParameters->fromBlock();
  request.toBlock       = dumpParameters->toBlock();
  RtcpTxRx::tellRtcpdDumpTape(m_cuuid, m_jobRequest.volReqId,
    m_sockCatalogue.getInitialRtcpdConn(), RTCPDNETRWTIMEOUT, request);

  // Tell rtcpd end of file list
  RtcpTxRx::tellRtcpdEndOfFileList(m_cuuid, m_jobRequest.volReqId,
    m_sockCatalogue.getInitialRtcpdConn(), RTCPDNETRWTIMEOUT);
}


//-----------------------------------------------------------------------------
// endRtcpdSession
//-----------------------------------------------------------------------------
void castor::tape::tapebridge::BridgeProtocolEngine::endRtcpdSession() throw() {
  try {

    // Inner try and catch to convert std::exception and unknown exceptions
    // (...) to castor::exception::Exception
    try {

      // The tape-session is ended differently with the rtcpd daemon depending
      // on the tape access-mode
      switch(m_volume.mode()) {
      case tapegateway::READ:
      case tapegateway::WRITE:
        notifyRtcpdEndOfSession();
        break;
      case tapegateway::DUMP:
        // Do nothing
        break;
      default:
        TAPE_THROW_EX(castor::exception::Internal,
          ": Unknown VolumeMode"
          ": Actual=" << m_volume.mode());
      }

    } catch(std::exception &se) {
      TAPE_THROW_EX(castor::exception::Internal,
        ": Caught a std::exception"
        ": what=" << se.what());
    } catch(castor::exception::Exception &ex) {
      // Simply rethrow as there is no need to convert exception type
      throw ex;
    } catch(...) {
      TAPE_THROW_EX(castor::exception::Internal,
        ": Caught an unknown exception");
    }

  } catch(castor::exception::Exception &ex) {
    castor::dlf::Param params[] = {
      castor::dlf::Param("mountTransactionId", m_jobRequest.volReqId  ),
      castor::dlf::Param("volReqId"          , m_jobRequest.volReqId  ),
      castor::dlf::Param("TPVID"             , m_volume.vid()         ),
      castor::dlf::Param("driveUnit"         , m_jobRequest.driveUnit ),
      castor::dlf::Param("dgn"               , m_jobRequest.dgn       ),
      castor::dlf::Param("clientHost"        , m_jobRequest.clientHost),
      castor::dlf::Param("clientPort"        , m_jobRequest.clientPort),
      castor::dlf::Param("clientType",
        utils::volumeClientTypeToString(m_volume.clientType())),
      castor::dlf::Param("errorCode"         , ex.code()              ),
      castor::dlf::Param("errorMessage"      , ex.getMessage().str()  )};
    castor::dlf::dlf_writep(m_cuuid, DLF_LVL_ERROR,
      TAPEBRIDGE_FAILED_TO_END_RTCPD_SESSION, params);

    // Gather the error information into an SessionError object
    SessionError sessionError;
    sessionError.setErrorCode(ex.code());
    sessionError.setErrorMessage(ex.getMessage().str());
    sessionError.setErrorScope(SessionError::SESSION_SCOPE);

    // Push the error onto the back of the list of errors generated during the
    // sesion with the rtcpd daemon
    m_sessionErrors.push_back(sessionError);
  }

  // Close the initial rtcpd-connection
  {
    const int closeRc = m_fileCloser.closeFd(
      m_sockCatalogue.releaseInitialRtcpdConn());

    castor::dlf::Param params[] = {
      castor::dlf::Param("mountTransactionId", m_jobRequest.volReqId  ),
      castor::dlf::Param("volReqId"          , m_jobRequest.volReqId  ),
      castor::dlf::Param("TPVID"             , m_volume.vid()         ),
      castor::dlf::Param("driveUnit"         , m_jobRequest.driveUnit ),
      castor::dlf::Param("dgn"               , m_jobRequest.dgn       ),
      castor::dlf::Param("clientHost"        , m_jobRequest.clientHost),
      castor::dlf::Param("clientPort"        , m_jobRequest.clientPort),
      castor::dlf::Param("clientType",
        utils::volumeClientTypeToString(m_volume.clientType())),
      castor::dlf::Param("closeRc"           , closeRc                )};
    castor::dlf::dlf_writep(m_cuuid, DLF_LVL_SYSTEM,
      TAPEBRIDGE_CLOSED_INITIAL_CALLBACK_CONNECTION, params);
  }
}


//-----------------------------------------------------------------------------
// processRtcpdRequest
//-----------------------------------------------------------------------------
void castor::tape::tapebridge::BridgeProtocolEngine::processRtcpdRequest(
  const legacymsg::MessageHeader &header, const int socketFd)
  throw(castor::exception::Exception) {

  {
    std::ostringstream magicHexStream;
    magicHexStream << "0x" << std::hex << header.magic;

    const char *magicName = utils::magicToString(header.magic);

    std::ostringstream reqTypeHexStream;
    reqTypeHexStream << "0x" << std::hex << header.reqType;

    const char *reqTypeName = utils::rtcopyReqTypeToString(header.reqType);

    castor::dlf::Param params[] = {
      castor::dlf::Param("mountTransactionId", m_jobRequest.volReqId  ),
      castor::dlf::Param("volReqId"          , m_jobRequest.volReqId  ),
      castor::dlf::Param("TPVID"             , m_volume.vid()         ),
      castor::dlf::Param("driveUnit"         , m_jobRequest.driveUnit ),
      castor::dlf::Param("dgn"               , m_jobRequest.dgn       ),
      castor::dlf::Param("clientHost"        , m_jobRequest.clientHost),
      castor::dlf::Param("clientPort"        , m_jobRequest.clientPort),
      castor::dlf::Param("clientType",
        utils::volumeClientTypeToString(m_volume.clientType())),
      castor::dlf::Param("socketFd"          , socketFd               ),
      castor::dlf::Param("magic"             , magicHexStream.str()   ),
      castor::dlf::Param("magicName"         , magicName              ),
      castor::dlf::Param("reqType"           , reqTypeHexStream.str() ),
      castor::dlf::Param("reqTypeName"       , reqTypeName            ),
      castor::dlf::Param("len"               , header.lenOrStatus     )};
    castor::dlf::dlf_writep(m_cuuid, DLF_LVL_DEBUG,
      TAPEBRIDGE_PROCESSING_TAPE_DISK_RQST, params);
  }

  dispatchRtcpdRequestHandler(header, socketFd);
}


//-----------------------------------------------------------------------------
// dispatchRtcpdRequestHandler
//-----------------------------------------------------------------------------
void
  castor::tape::tapebridge::BridgeProtocolEngine::dispatchRtcpdRequestHandler(
  const legacymsg::MessageHeader &header,
  const int                      socketFd)
  throw(castor::exception::Exception) {
  const char *const task = "dispatch rtcpd request handler";

  switch(header.magic) {
  case RTCOPY_MAGIC:
    switch(header.reqType) {
    case RTCP_FILE_REQ:
      return rtcpFileReqRtcpdCallback(header, socketFd);
    case RTCP_FILEERR_REQ:
      return rtcpFileErrReqRtcpdCallback(header, socketFd);
    case RTCP_TAPE_REQ:
      return rtcpTapeReqRtcpdCallback(header, socketFd);
    case RTCP_TAPEERR_REQ:
      return rtcpTapeErrReqRtcpdCallback(header, socketFd);
    case RTCP_ENDOF_REQ:
      return rtcpEndOfReqRtcpdCallback(socketFd);
    case TAPEBRIDGE_FLUSHEDTOTAPE:
      return tapeBridgeFlushedToTapeCallback(header, socketFd);
    default:
      TAPE_THROW_EX(castor::exception::InvalidArgument,
        ": Failed to " << task <<
        ": Unknown request type"
        ": magic=" << header.magic <<
        ": reqType=" << header.reqType);
    } // switch(header.reqType)
    break;
  case RTCOPY_MAGIC_SHIFT:
    switch(header.reqType) {
    case GIVE_OUTP:
      return giveOutpRtcpdCallback(header, socketFd);
    default:
      TAPE_THROW_EX(castor::exception::InvalidArgument,
        ": Failed to " << task <<
        ": Unknown request type"
        ": magic=" << header.magic <<
        ": reqType=" << header.reqType);
    } // switch(header.reqType)
    break;
  default:
    TAPE_THROW_EX(castor::exception::InvalidArgument,
      ": Failed to " << task <<
      ": Unknown magic number"
      ": magic=" << header.magic <<
      ": reqType=" << header.reqType);
  } // switch(header.magic)
}


//-----------------------------------------------------------------------------
// rtcpFileReqRtcpdCallback
//-----------------------------------------------------------------------------
void castor::tape::tapebridge::BridgeProtocolEngine::rtcpFileReqRtcpdCallback(
  const legacymsg::MessageHeader &header, const int socketFd)
  throw(castor::exception::Exception) {

  legacymsg::RtcpFileRqstMsgBody body;

  RtcpTxRx::receiveMsgBody(m_cuuid, m_jobRequest.volReqId, socketFd,
    RTCPDNETRWTIMEOUT, header, body);

  legacymsg::RtcpFileRqstErrMsgBody bodyErr;
  memcpy(&(bodyErr.rqst), &(body.rqst), sizeof(bodyErr.rqst));
  memset(bodyErr.err.errorMsg, '\0', sizeof(bodyErr.err.errorMsg));
  bodyErr.err.severity    = RTCP_OK;
  bodyErr.err.errorCode   = 0;
  bodyErr.err.maxTpRetry  = 1; // Only try once - no retries
  bodyErr.err.maxCpRetry  = 1; // Only try once - no retries

  dispatchRtcpFileErrReq(header, bodyErr, socketFd);
}


//-----------------------------------------------------------------------------
// dispatchRtcpFileErrReq
//-----------------------------------------------------------------------------
void castor::tape::tapebridge::BridgeProtocolEngine::dispatchRtcpFileErrReq(
  const legacymsg::MessageHeader &header,
  legacymsg::RtcpFileRqstErrMsgBody &body,
  const int                         rtcpdSock)
  throw(castor::exception::Exception) {

  // Dispatch the appropriate helper method to process the message
  if(m_volume.mode() == tapegateway::DUMP) {
    processRtcpFileErrReqDump(header, rtcpdSock);
  } else {
    switch(body.rqst.procStatus) {
    case RTCP_WAITING:
      processRtcpWaiting(header, body, rtcpdSock);
      break;
    case RTCP_REQUEST_MORE_WORK:
      processRtcpRequestMoreWork(header, body, rtcpdSock);
      break;
    case RTCP_POSITIONED:
      processRtcpFilePositionedRequest(header, body, rtcpdSock);
      break;
    case RTCP_FINISHED:
      processRtcpFileFinishedRequest(header, body, rtcpdSock);
      break;
    default:
      {
        TAPE_THROW_CODE(EBADMSG,
          ": Received unexpected file request process status 0x" <<
          std::hex << body.rqst.procStatus <<
          "(" << utils::procStatusToString(body.rqst.procStatus) << ")");
      }
    }
  }
}


//-----------------------------------------------------------------------------
// rtcpFileErrReqRtcpdCallback
//-----------------------------------------------------------------------------
void
  castor::tape::tapebridge::BridgeProtocolEngine::rtcpFileErrReqRtcpdCallback(
  const legacymsg::MessageHeader &header,
  const int                      socketFd)
  throw(castor::exception::Exception) {

  legacymsg::RtcpFileRqstErrMsgBody body;

  RtcpTxRx::receiveMsgBody(m_cuuid, m_jobRequest.volReqId, socketFd,
    RTCPDNETRWTIMEOUT, header, body);

  // Throw an exception if rtcpd has reported an error that is not a failure to
  // copy  a file to or from tape
  if(0 == body.rqst.cprc) {
    if(body.err.errorCode != 0) {
      TAPE_THROW_CODE(body.err.errorCode,
        ": Received an error from rtcpd: " << body.err.errorMsg);
    }
  }

  dispatchRtcpFileErrReq(header, body, socketFd);
}


//-----------------------------------------------------------------------------
// processRtcpFileErrReqDump
//-----------------------------------------------------------------------------
void castor::tape::tapebridge::BridgeProtocolEngine::processRtcpFileErrReqDump(
  const legacymsg::MessageHeader &header, const int rtcpdSock)
  throw(castor::exception::Exception) {
  // Reply to rtcpd with a positive acknowledgement
  legacymsg::MessageHeader ackMsg;
  ackMsg.magic       = header.magic;
  ackMsg.reqType     = header.reqType;
  ackMsg.lenOrStatus = 0;
  m_legacyTxRx.sendMsgHeader(rtcpdSock, ackMsg);
}


//-----------------------------------------------------------------------------
// processRtcpWaiting
//-----------------------------------------------------------------------------
void castor::tape::tapebridge::BridgeProtocolEngine::processRtcpWaiting(
  const legacymsg::MessageHeader &header,
  legacymsg::RtcpFileRqstErrMsgBody &body, const int rtcpdSock)
  throw(castor::exception::Exception) {

  // Reply to rtcpd with a positive acknowledgement
  legacymsg::MessageHeader ackMsg;
  ackMsg.magic       = header.magic;
  ackMsg.reqType     = header.reqType;
  ackMsg.lenOrStatus = 0;
  m_legacyTxRx.sendMsgHeader(rtcpdSock, ackMsg);

  // Determine the error code and message if there is an error embedded in the
  // message
  int32_t     errorCode    = SEINTERNAL;
  std::string errorMessage = "UNKNOWN";
  if(0 != body.rqst.cprc && 0 != body.err.errorCode) {
    errorCode    = body.err.errorCode;
    errorMessage = body.err.errorMsg;
  }

  // Log the reception of the message taking into account whether or not the
  // message contains an error
  if(0 == body.rqst.cprc) {
    castor::dlf::Param params[] = {
      castor::dlf::Param("mountTransactionId", m_jobRequest.volReqId  ),
      castor::dlf::Param("volReqId"          , m_jobRequest.volReqId  ),
      castor::dlf::Param("TPVID"             , m_volume.vid()         ),
      castor::dlf::Param("driveUnit"         , m_jobRequest.driveUnit ),
      castor::dlf::Param("dgn"               , m_jobRequest.dgn       ),
      castor::dlf::Param("clientHost"        , m_jobRequest.clientHost),
      castor::dlf::Param("clientPort"        , m_jobRequest.clientPort),
      castor::dlf::Param("clientType",
        utils::volumeClientTypeToString(m_volume.clientType())),
      castor::dlf::Param("nsHost"            ,
        body.rqst.segAttr.nameServerHostName),
      castor::dlf::Param("nsFileId"          ,
        body.rqst.segAttr.castorFileId),
      castor::dlf::Param("tapeFSeq"          , body.rqst.tapeFseq     )};
    castor::dlf::dlf_writep(m_cuuid, DLF_LVL_SYSTEM,
      TAPEBRIDGE_RECEIVED_RTCP_WAITING, params);
  } else {
    castor::dlf::Param params[] = {
      castor::dlf::Param("mountTransactionId", m_jobRequest.volReqId  ),
      castor::dlf::Param("volReqId"          , m_jobRequest.volReqId  ),
      castor::dlf::Param("TPVID"             , m_volume.vid()         ),
      castor::dlf::Param("driveUnit"         , m_jobRequest.driveUnit ),
      castor::dlf::Param("dgn"               , m_jobRequest.dgn       ),
      castor::dlf::Param("clientHost"        , m_jobRequest.clientHost),
      castor::dlf::Param("clientPort"        , m_jobRequest.clientPort),
      castor::dlf::Param("clientType",
        utils::volumeClientTypeToString(m_volume.clientType())),
      castor::dlf::Param("nsHost"            ,
        body.rqst.segAttr.nameServerHostName),
      castor::dlf::Param("nsFileId"          ,
        body.rqst.segAttr.castorFileId),
      castor::dlf::Param("tapeFSeq"          , body.rqst.tapeFseq     ),
      castor::dlf::Param("errorCode"         , errorCode              ),
      castor::dlf::Param("errorMessage"      , errorMessage           )};
    castor::dlf::dlf_writep(m_cuuid, DLF_LVL_SYSTEM,
      TAPEBRIDGE_RECEIVED_RTCP_WAITING_ERROR, params);
  }

  // If the message contains an error
  if(0 != body.rqst.cprc) {
    // Gather the error information into an SessionError object
    SessionError sessionError;
    sessionError.setErrorCode(errorCode);
    sessionError.setErrorMessage(errorMessage);

    // If this is a file-specific error
    if(0 < body.rqst.tapeFseq) {

      uint64_t fileTransactonId = 0;
      try {
        fileTransactonId = m_pendingTransferIds.get(body.rqst.diskFseq);
      } catch(castor::exception::Exception &ex) {
        // Add context an re-throw
        TAPE_THROW_CODE(ex.code(),
          ": " << ex.getMessage().str());
      }

      sessionError.setErrorScope(SessionError::FILE_SCOPE);
      sessionError.setFileTransactionId(fileTransactonId);
      sessionError.setNsHost(body.rqst.segAttr.nameServerHostName);
      sessionError.setNsFileId(body.rqst.segAttr.castorFileId);
      sessionError.setTapeFSeq(body.rqst.tapeFseq);

    // Else this is a session error - don't need to fill in file specifics
    } else {
      sessionError.setErrorScope(SessionError::SESSION_SCOPE);
    }

    // Push the error onto the back of the list of errors generated during the
    // tape session
    m_sessionErrors.push_back(sessionError);
  }
}


//-----------------------------------------------------------------------------
// processRtcpRequestMoreWork
//-----------------------------------------------------------------------------
void castor::tape::tapebridge::BridgeProtocolEngine::processRtcpRequestMoreWork(
  const legacymsg::MessageHeader &header,
  legacymsg::RtcpFileRqstErrMsgBody &body, const int rtcpdSock)
  throw(castor::exception::Exception) {

  if(shuttingDownRtcpdSession()) {
    castor::dlf::Param params[] = {
      castor::dlf::Param("mountTransactionId", m_jobRequest.volReqId  ),
      castor::dlf::Param("volReqId"          , m_jobRequest.volReqId  ),
      castor::dlf::Param("TPVID"             , m_volume.vid()         ),
      castor::dlf::Param("driveUnit"         , m_jobRequest.driveUnit ),
      castor::dlf::Param("dgn"               , m_jobRequest.dgn       ),
      castor::dlf::Param("clientHost"        , m_jobRequest.clientHost),
      castor::dlf::Param("clientPort"        , m_jobRequest.clientPort),
      castor::dlf::Param("clientType",
        utils::volumeClientTypeToString(m_volume.clientType()))};
    castor::dlf::dlf_writep(m_cuuid, DLF_LVL_SYSTEM,
      TAPEBRIDGE_TELLING_RTCPD_NO_MORE_FILES_DUE_TO_SHUTTING_DOWN, params);
    tellRtcpdThereAreNoMoreFiles(rtcpdSock, header.magic, header.reqType);
    return;
  }

  // If migrating
  if(m_volume.mode() == tapegateway::WRITE) {
    processRtcpRequestMoreMigrationWork(header, body, rtcpdSock);
  // Else recalling
  } else {
    processRtcpRequestMoreRecallWork(header, body, rtcpdSock);
  }
}


//-----------------------------------------------------------------------------
// processRtcpRequestMoreMigrationWork
//-----------------------------------------------------------------------------
void castor::tape::tapebridge::BridgeProtocolEngine::
  processRtcpRequestMoreMigrationWork(
  const legacymsg::MessageHeader    &header,
  legacymsg::RtcpFileRqstErrMsgBody &body,
  const int                         rtcpdSock)
  throw(castor::exception::Exception) {

  // If the cache of files to migrate is empty then request another set of
  // files, else send to the rtcpd daemon a file to migrate from the cache.
  //
  // The rtcpd daemon will not request another file to migrate until the
  // current request has been answered by the tapebridged daemon.
  if(m_filesToMigrate.empty()) {
    if(m_sockCatalogue.getMoreWorkConnectionIsSet()) {
      // This should never happen
      TAPE_THROW_CODE(ECANCELED,
        ": The rtcpd daemon has requested a file to migrate before receiving"
        " the previous one");
    }

    sendFilesToMigrateListRequestToClient(header, body, rtcpdSock);
  } else {
    sendCachedFileToMigrateToRtcpd(
      rtcpdSock,
      header.magic,
      header.reqType);
  }
}


//-----------------------------------------------------------------------------
// sendFilesToMigrateListRequestToClient
//-----------------------------------------------------------------------------
void castor::tape::tapebridge::BridgeProtocolEngine::
  sendFilesToMigrateListRequestToClient(
  const legacymsg::MessageHeader    &header,
  legacymsg::RtcpFileRqstErrMsgBody &body,
  const int                         rtcpdSock)
  throw(castor::exception::Exception) {

  const uint64_t tapebridgeTransId = m_tapebridgeTransactionCounter.next();
  // Only the tapegatewayd daemon supports the bulk prototcol
  const uint64_t maxFiles =
    tapegateway::TAPE_GATEWAY == m_volume.clientType() ?
      m_bulkRequestConfigParams.getBulkRequestMigrationMaxFiles().getValue() :
      1;
  // Only the tapegatewayd daemon supports the bulk prototcol
  const uint64_t maxBytes =
    tapegateway::TAPE_GATEWAY == m_volume.clientType() ?
      m_bulkRequestConfigParams.getBulkRequestMigrationMaxBytes().getValue() :
      1;
  utils::SmartFd clientSock(m_clientProxy.sendFilesToMigrateListRequest(
    tapebridgeTransId, maxFiles, maxBytes));
  {
    castor::dlf::Param params[] = {
      castor::dlf::Param("tapebridgeTransId" , tapebridgeTransId      ),
    castor::dlf::Param("mountTransactionId", m_jobRequest.volReqId  ),
      castor::dlf::Param("volReqId"          , m_jobRequest.volReqId  ),
      castor::dlf::Param("TPVID"             , m_volume.vid()         ),
      castor::dlf::Param("driveUnit"         , m_jobRequest.driveUnit ),
      castor::dlf::Param("dgn"               , m_jobRequest.dgn       ),
      castor::dlf::Param("clientHost"        , m_jobRequest.clientHost),
      castor::dlf::Param("clientPort"        , m_jobRequest.clientPort),
      castor::dlf::Param("maxFiles"          , maxFiles               ),
      castor::dlf::Param("maxBytes"          , maxBytes               ),
      castor::dlf::Param("clientType",
        utils::volumeClientTypeToString(m_volume.clientType())),
      castor::dlf::Param("clientSock"        , clientSock.get()       )};
    castor::dlf::dlf_writep(m_cuuid, DLF_LVL_SYSTEM,
      TAPEBRIDGE_SENT_FILESTOMIGRATELISTREQUEST, params);
  }

  try {
    // Add the client connection to the socket catalogue so that the
    // association with the rtcpd connection is made
    m_sockCatalogue.addGetMoreWorkConnection(rtcpdSock, header.magic,
      header.reqType, body.rqst.tapePath, clientSock.release(),
      tapebridgeTransId);
  } catch(castor::exception::Exception &ex) {
    // Add context and re-throw
    TAPE_THROW_CODE(ex.code(),
      ": Failed to add client connection to socket catalogue"
      ": " << ex.getMessage().str());
  }
}


//-----------------------------------------------------------------------------
// sendCachedFileToMigrateToRtcpd
//-----------------------------------------------------------------------------
void castor::tape::tapebridge::BridgeProtocolEngine::
  sendCachedFileToMigrateToRtcpd(
  const int      rtcpdSock,
  const uint32_t rtcpdReqMagic,
  const uint32_t rtcpdReqType)
  throw(castor::exception::Exception) {

  if(m_filesToMigrate.empty()) {
    // This should never happen
    TAPE_THROW_EX(castor::exception::Internal,
      ": There are no files to migrate in the cache");
  }

  const FileToMigrate fileToMigrate = m_filesToMigrate.front();
  m_filesToMigrate.pop_front();

  sendFileToMigrateToRtcpd(
    fileToMigrate,
    rtcpdSock,
    rtcpdReqMagic,
    rtcpdReqType);
}


//-----------------------------------------------------------------------------
// processRtcpRequestMoreRecallWork
//-----------------------------------------------------------------------------
void castor::tape::tapebridge::BridgeProtocolEngine::
  processRtcpRequestMoreRecallWork(
  const legacymsg::MessageHeader    &header,
  legacymsg::RtcpFileRqstErrMsgBody &body,
  const int                         rtcpdSock)
  throw(castor::exception::Exception) {

  // If the cache of files to recall is empty then request another set of
  // files, else send to the rtcpd daemon a file to recall from the cache.
  //
  // The rtcpd daemon will not request another file to recall until the current
  // request has been answered by the tapebridged daemon.
  if(m_filesToRecall.empty()) {
    if(m_sockCatalogue.getMoreWorkConnectionIsSet()) {
      // This should never happen
      TAPE_THROW_CODE(ECANCELED,
        ": The rtcpd daemon has requested a file to recall before receiving"
        " the previous one");
    }

    sendFilesToRecallListRequestToClient(header, body, rtcpdSock);
  } else {
    sendCachedFileToRecallToRtcpd(
      rtcpdSock,
      header.magic,
      header.reqType,
      body.rqst.tapePath);
  }
}


//-----------------------------------------------------------------------------
// sendFilesToRecallListRequestToClient
//-----------------------------------------------------------------------------
void castor::tape::tapebridge::BridgeProtocolEngine::
  sendFilesToRecallListRequestToClient(
  const legacymsg::MessageHeader    &header,
  legacymsg::RtcpFileRqstErrMsgBody &body,
  const int                         rtcpdSock)
  throw(castor::exception::Exception) {

  const uint64_t tapebridgeTransId = m_tapebridgeTransactionCounter.next();
  // Only the tapegatewayd daemon supports the bulk prototcol
  const uint64_t maxFiles =
    tapegateway::TAPE_GATEWAY == m_volume.clientType() ?
      m_bulkRequestConfigParams.getBulkRequestRecallMaxFiles().getValue() :
      1;
  // Only the tapegatewayd daemon supports the bulk prototcol
  const uint64_t maxBytes =
    tapegateway::TAPE_GATEWAY == m_volume.clientType() ?
      m_bulkRequestConfigParams.getBulkRequestRecallMaxBytes().getValue() :
      1;
  utils::SmartFd clientSock(m_clientProxy.sendFilesToRecallListRequest(
    tapebridgeTransId, maxFiles, maxBytes));
  {
    castor::dlf::Param params[] = {
      castor::dlf::Param("tapebridgeTransId" , tapebridgeTransId      ),
      castor::dlf::Param("mountTransactionId", m_jobRequest.volReqId  ),
      castor::dlf::Param("volReqId"          , m_jobRequest.volReqId  ),
      castor::dlf::Param("TPVID"             , m_volume.vid()         ),
      castor::dlf::Param("driveUnit"         , m_jobRequest.driveUnit ),
      castor::dlf::Param("dgn"               , m_jobRequest.dgn       ),
      castor::dlf::Param("clientHost"        , m_jobRequest.clientHost),
      castor::dlf::Param("clientPort"        , m_jobRequest.clientPort),
      castor::dlf::Param("maxFiles"          , maxFiles               ),
      castor::dlf::Param("maxBytes"          , maxBytes               ),
      castor::dlf::Param("clientType",
        utils::volumeClientTypeToString(m_volume.clientType())),
      castor::dlf::Param("clientSock"        , clientSock.get()       )};
    castor::dlf::dlf_writep(m_cuuid, DLF_LVL_SYSTEM,
      TAPEBRIDGE_SENT_FILESTORECALLLISTREQUEST, params);
  }

  try {
    // Add the client connection to the socket catalogue so that the
    // association with the rtcpd connection is made
    m_sockCatalogue.addGetMoreWorkConnection(
      rtcpdSock,
      header.magic,
      header.reqType,
      body.rqst.tapePath,
      clientSock.release(),
      tapebridgeTransId);
  } catch(castor::exception::Exception &ex) {
    // Add context and re-throw
    TAPE_THROW_CODE(ex.code(),
      ": Failed to add client connection to socket catalogue"
      ": " << ex.getMessage().str());
  }
}


//-----------------------------------------------------------------------------
// sendCachedFileToRecallToRtcpd
//-----------------------------------------------------------------------------
void castor::tape::tapebridge::BridgeProtocolEngine::
  sendCachedFileToRecallToRtcpd(
  const int         rtcpdSock,
  const uint32_t    rtcpdReqMagic,
  const uint32_t    rtcpdReqType,
  const std::string &rtcpdReqTapePath)
  throw(castor::exception::Exception) {

  if(m_filesToRecall.empty()) {
    // This should never happen
    TAPE_THROW_EX(castor::exception::Internal,
      ": There are no files to recall in the cache");
  }

  const FileToRecall fileToRecall = m_filesToRecall.front();
  m_filesToRecall.pop_front();

  sendFileToRecallToRtcpd(
    fileToRecall,
    rtcpdSock,
    rtcpdReqMagic,
    rtcpdReqType,
    rtcpdReqTapePath);
}


//-----------------------------------------------------------------------------
// sendFileToRecallToRtcpd
//-----------------------------------------------------------------------------
void castor::tape::tapebridge::BridgeProtocolEngine::sendFileToRecallToRtcpd(
  const FileToRecall &fileToRecall,
  const int          rtcpdSock,
  const uint32_t     rtcpdReqMagic,
  const uint32_t     rtcpdReqType,
  const std::string  &rtcpdReqTapePath)
  throw(castor::exception::Exception) {

  // Remember the file transaction ID and get its unique index to be
  // passed to rtcpd through the "rtcpFileRequest.disk_fseq" message
  // field
  int diskFSeq = 0;
  try {
    diskFSeq = m_pendingTransferIds.insert(fileToRecall.fileTransactionId);
  } catch(castor::exception::Exception &ex) {
    // Add context and rethrow
    TAPE_THROW_CODE(ex.code(),
      ": " << ex.getMessage().str());
  }

  // Give file to recall to rtcpd
  {
    char tapeFileId[CA_MAXPATHLEN+1];
    utils::setBytes(tapeFileId, '\0');
    unsigned char blockId[4] = {
      fileToRecall.blockId0,
      fileToRecall.blockId1,
      fileToRecall.blockId2,
      fileToRecall.blockId3};
    char nshost[CA_MAXHOSTNAMELEN+1];
    utils::copyString(nshost, fileToRecall.nsHost.c_str());

    // The file size is not specified when recalling
    const uint64_t fileSize = 0;

    RtcpTxRx::giveFileToRtcpd(
      m_cuuid,
      m_jobRequest.volReqId,
      rtcpdSock,
      RTCPDNETRWTIMEOUT,
      WRITE_DISABLE,
      fileToRecall.path.c_str(),
      fileSize,
      rtcpdReqTapePath.c_str(),
      RECORDFORMAT,
      tapeFileId,
      (uint32_t)fileToRecall.umask,
      fileToRecall.positionMethod,
      fileToRecall.fseq,
      diskFSeq,
      nshost,
      fileToRecall.fileId,
      blockId);
  }

  // Ask rtcpd to request more work
  RtcpTxRx::askRtcpdToRequestMoreWork(
    m_cuuid,
    m_jobRequest.volReqId,
    rtcpdReqTapePath.c_str(),
    rtcpdSock,
    RTCPDNETRWTIMEOUT,
    WRITE_DISABLE);

  // Tell rtcpd end of file list
  RtcpTxRx::tellRtcpdEndOfFileList(
    m_cuuid,
    m_jobRequest.volReqId,
    rtcpdSock,
    RTCPDNETRWTIMEOUT);

  {
    castor::dlf::Param params[] = {
      castor::dlf::Param("mountTransactionId", m_jobRequest.volReqId  ),
      castor::dlf::Param("volReqId"          , m_jobRequest.volReqId  ),
      castor::dlf::Param("TPVID"             , m_volume.vid()         ),
      castor::dlf::Param("driveUnit"         , m_jobRequest.driveUnit ),
      castor::dlf::Param("dgn"               , m_jobRequest.dgn       ),
      castor::dlf::Param("clientHost"        , m_jobRequest.clientHost),
      castor::dlf::Param("clientPort"        , m_jobRequest.clientPort),
      castor::dlf::Param("clientType",
        utils::volumeClientTypeToString(m_volume.clientType())),
      castor::dlf::Param("rtcpdSock"         , rtcpdSock              )};
    castor::dlf::dlf_writep(m_cuuid, DLF_LVL_DEBUG,
      TAPEBRIDGE_SEND_DELAYED_REQUEST_MORE_WORK_ACK_TO_RTCPD, params);
  }

  // Send delayed acknowledge of the request for more work
  legacymsg::MessageHeader ackMsg;
  ackMsg.magic       = rtcpdReqMagic;
  ackMsg.reqType     = rtcpdReqType;
  ackMsg.lenOrStatus = 0;
  m_legacyTxRx.sendMsgHeader(rtcpdSock, ackMsg);

  {
    castor::dlf::Param params[] = {
      castor::dlf::Param("mountTransactionId", m_jobRequest.volReqId  ),
      castor::dlf::Param("volReqId"          , m_jobRequest.volReqId  ),
      castor::dlf::Param("TPVID"             , m_volume.vid()         ),
      castor::dlf::Param("driveUnit"         , m_jobRequest.driveUnit ),
      castor::dlf::Param("dgn"               , m_jobRequest.dgn       ),
      castor::dlf::Param("clientHost"        , m_jobRequest.clientHost),
      castor::dlf::Param("clientPort"        , m_jobRequest.clientPort),
      castor::dlf::Param("clientType",
        utils::volumeClientTypeToString(m_volume.clientType())),
      castor::dlf::Param("rtcpdSock"         , rtcpdSock              )};
    castor::dlf::dlf_writep(m_cuuid, DLF_LVL_SYSTEM,
      TAPEBRIDGE_SENT_DELAYED_REQUEST_MORE_WORK_ACK_TO_RTCPD, params);
  }
}


//-----------------------------------------------------------------------------
// processRtcpFilePositionedRequest
//-----------------------------------------------------------------------------
void castor::tape::tapebridge::BridgeProtocolEngine::
  processRtcpFilePositionedRequest(const legacymsg::MessageHeader &header,
  legacymsg::RtcpFileRqstErrMsgBody &body, const int rtcpdSock)
  throw(castor::exception::Exception) {

  // Reply to rtcpd with a positive acknowledgement
  legacymsg::MessageHeader ackMsg;
  ackMsg.magic       = header.magic;
  ackMsg.reqType     = header.reqType;
  ackMsg.lenOrStatus = 0;
  m_legacyTxRx.sendMsgHeader(rtcpdSock, ackMsg);

  // Determine the error code and message if there is an error embedded in the
  // message
  int32_t     errorCode    = SEINTERNAL;
  std::string errorMessage = "UNKNOWN";
  if(0 != body.rqst.cprc && 0 != body.err.errorCode) {
    errorCode    = body.err.errorCode;
    errorMessage = body.err.errorMsg;
  }

  // Log the reception of the message taking into acount whether or not there
  // is an embedded error message
  if(0 == body.rqst.cprc) {
    castor::dlf::Param params[] = {
      castor::dlf::Param("mountTransactionId", m_jobRequest.volReqId  ),
      castor::dlf::Param("volReqId"          , m_jobRequest.volReqId  ),
      castor::dlf::Param("TPVID"             , m_volume.vid()         ),
      castor::dlf::Param("driveUnit"         , m_jobRequest.driveUnit ),
      castor::dlf::Param("dgn"               , m_jobRequest.dgn       ),
      castor::dlf::Param("clientHost"        , m_jobRequest.clientHost),
      castor::dlf::Param("clientPort"        , m_jobRequest.clientPort),
      castor::dlf::Param("clientType",
        utils::volumeClientTypeToString(m_volume.clientType())),
      castor::dlf::Param("filePath"          , body.rqst.filePath     ),
      castor::dlf::Param("tapePath"          , body.rqst.tapePath     )};
    castor::dlf::dlf_writep(m_cuuid, DLF_LVL_SYSTEM,
      TAPEBRIDGE_RECEIVED_RTCP_POSITIONED, params);
  } else {
    castor::dlf::Param params[] = {
      castor::dlf::Param("mountTransactionId", m_jobRequest.volReqId  ),
      castor::dlf::Param("volReqId"          , m_jobRequest.volReqId  ),
      castor::dlf::Param("TPVID"             , m_volume.vid()         ),
      castor::dlf::Param("driveUnit"         , m_jobRequest.driveUnit ),
      castor::dlf::Param("dgn"               , m_jobRequest.dgn       ),
      castor::dlf::Param("clientHost"        , m_jobRequest.clientHost),
      castor::dlf::Param("clientPort"        , m_jobRequest.clientPort),
      castor::dlf::Param("clientType",
        utils::volumeClientTypeToString(m_volume.clientType())),
      castor::dlf::Param("filePath"          , body.rqst.filePath     ),
      castor::dlf::Param("tapePath"          , body.rqst.tapePath     ),
      castor::dlf::Param("errorCode"         , errorCode              ),
      castor::dlf::Param("errorMessage"      , errorMessage           )};
    castor::dlf::dlf_writep(m_cuuid, DLF_LVL_SYSTEM,
      TAPEBRIDGE_RECEIVED_RTCP_POSITIONED_ERROR, params);
  }

  // If the message contains an error
  if(0 != body.rqst.cprc) {
    // Gather the error information into an SessionError object
    SessionError sessionError;
    sessionError.setErrorCode(errorCode);
    sessionError.setErrorMessage(errorMessage);

    // If this is a file-specific error
    if(0 < body.rqst.tapeFseq) {
      uint64_t fileTransactonId = 0;
      try {
        fileTransactonId = m_pendingTransferIds.get(body.rqst.diskFseq);
      } catch(castor::exception::Exception &ex) {
        // Add context an re-throw
        TAPE_THROW_CODE(ex.code(),
          ": " << ex.getMessage().str());
      }

      sessionError.setErrorScope(SessionError::FILE_SCOPE);
      sessionError.setFileTransactionId(fileTransactonId);
      sessionError.setNsHost(body.rqst.segAttr.nameServerHostName);
      sessionError.setNsFileId(body.rqst.segAttr.castorFileId);
      sessionError.setTapeFSeq(body.rqst.tapeFseq);

    // Else this is a session error - don't need to fill in file specifics
    } else {
      sessionError.setErrorScope(SessionError::SESSION_SCOPE);
    }

    // Push the error onto the back of the list of errors received from the
    // rtcpd daemon
    m_sessionErrors.push_back(sessionError);

    // There is nothing else to be done
    return;
  }
}


//-----------------------------------------------------------------------------
// processRtcpFileFinishedRequest
//-----------------------------------------------------------------------------
void castor::tape::tapebridge::BridgeProtocolEngine::
  processRtcpFileFinishedRequest(const legacymsg::MessageHeader &header,
  legacymsg::RtcpFileRqstErrMsgBody &body, const int rtcpdSock)
  throw(castor::exception::Exception) {

  // Reply to rtcpd with a positive acknowledgement
  legacymsg::MessageHeader ackMsg;
  ackMsg.magic       = header.magic;
  ackMsg.reqType     = header.reqType;
  ackMsg.lenOrStatus = 0;
  m_legacyTxRx.sendMsgHeader(rtcpdSock, ackMsg);

  // Drop the message from rtcpd and return if the session is being shutdown
  if(shuttingDownRtcpdSession()) {
    castor::dlf::Param params[] = {
      castor::dlf::Param("mountTransactionId", m_jobRequest.volReqId  ),
      castor::dlf::Param("volReqId"          , m_jobRequest.volReqId  ),
      castor::dlf::Param("TPVID"             , m_volume.vid()         ),
      castor::dlf::Param("driveUnit"         , m_jobRequest.driveUnit ),
      castor::dlf::Param("dgn"               , m_jobRequest.dgn       ),
      castor::dlf::Param("clientHost"        , m_jobRequest.clientHost),
      castor::dlf::Param("clientPort"        , m_jobRequest.clientPort),
      castor::dlf::Param("clientType",
        utils::volumeClientTypeToString(m_volume.clientType()))};
    castor::dlf::dlf_writep(m_cuuid, DLF_LVL_SYSTEM,
      TAPEBRIDGE_DROPPING_RTCP_FINISHED_DUE_TO_SHUTTING_DOWN, params);
    return;
  }

  // Determine the error code and message if there is an error embedded in the
  // message
  int32_t     errorCode    = SEINTERNAL;
  std::string errorMessage = "UNKNOWN";
  if(0 != body.rqst.cprc && 0 != body.err.errorCode) {
    errorCode    = body.err.errorCode;
    errorMessage = body.err.errorMsg;
  }

  // Log the reception of the message taking into acount whether or not there
  // is an embedded error message
  if(0 == body.rqst.cprc) {
    castor::dlf::Param params[] = {
      castor::dlf::Param("mountTransactionId", m_jobRequest.volReqId  ),
      castor::dlf::Param("volReqID"          , m_jobRequest.volReqId  ),
      castor::dlf::Param("TPVID"             , m_volume.vid()         ),
      castor::dlf::Param("driveUnit"         , m_jobRequest.driveUnit ),
      castor::dlf::Param("dgn"               , m_jobRequest.dgn       ),
      castor::dlf::Param("clientHost"        , m_jobRequest.clientHost),
      castor::dlf::Param("clientPort"        , m_jobRequest.clientPort),
      castor::dlf::Param("clientType",
        utils::volumeClientTypeToString(m_volume.clientType())),
      castor::dlf::Param("volReqId"          , m_jobRequest.volReqId  ),
      castor::dlf::Param("filePath"          , body.rqst.filePath     ),
      castor::dlf::Param("tapePath"          , body.rqst.tapePath     ),
      castor::dlf::Param("tapeFseq"          , body.rqst.tapeFseq     ),
      castor::dlf::Param("diskFseq"          , body.rqst.diskFseq     ),
      castor::dlf::Param("bytesIn"           , body.rqst.bytesIn      ),
      castor::dlf::Param("bytesOut"          , body.rqst.bytesOut     )};
    castor::dlf::dlf_writep(m_cuuid, DLF_LVL_SYSTEM,
      TAPEBRIDGE_RECEIVED_RTCP_FINISHED, params);
  } else {
    castor::dlf::Param params[] = {
      castor::dlf::Param("mountTransactionId", m_jobRequest.volReqId  ),
      castor::dlf::Param("volReqID"          , m_jobRequest.volReqId  ),
      castor::dlf::Param("TPVID"             , m_volume.vid()         ),
      castor::dlf::Param("driveUnit"         , m_jobRequest.driveUnit ),
      castor::dlf::Param("dgn"               , m_jobRequest.dgn       ),
      castor::dlf::Param("clientHost"        , m_jobRequest.clientHost),
      castor::dlf::Param("clientPort"        , m_jobRequest.clientPort),
      castor::dlf::Param("clientType",
        utils::volumeClientTypeToString(m_volume.clientType())),
      castor::dlf::Param("volReqId"          , m_jobRequest.volReqId  ),
      castor::dlf::Param("filePath"          , body.rqst.filePath     ),
      castor::dlf::Param("tapePath"          , body.rqst.tapePath     ),
      castor::dlf::Param("tapeFseq"          , body.rqst.tapeFseq     ),
      castor::dlf::Param("diskFseq"          , body.rqst.diskFseq     ),
      castor::dlf::Param("bytesIn"           , body.rqst.bytesIn      ),
      castor::dlf::Param("bytesOut"          , body.rqst.bytesOut     ),
      castor::dlf::Param("errorCode"         , errorCode              ),
      castor::dlf::Param("errorMessage"      , errorMessage           )};
    castor::dlf::dlf_writep(m_cuuid, DLF_LVL_SYSTEM,
      TAPEBRIDGE_RECEIVED_RTCP_FINISHED_ERROR, params);
  }

  // If the message contains an error
  if(0 != body.rqst.cprc) {
    // Gather the error information into an SessionError object
    SessionError sessionError;
    sessionError.setErrorCode(errorCode);
    sessionError.setErrorMessage(errorMessage);

    // If this is a file-specific error
    if(0 < body.rqst.tapeFseq) {

      uint64_t fileTransactonId = 0;
      try {
        fileTransactonId = m_pendingTransferIds.get(body.rqst.diskFseq);
      } catch(castor::exception::Exception &ex) {
        // Add context an re-throw
        TAPE_THROW_CODE(ex.code(),
          ": " << ex.getMessage().str());
      }

      sessionError.setErrorScope(SessionError::FILE_SCOPE);
      sessionError.setFileTransactionId(fileTransactonId);
      sessionError.setNsHost(body.rqst.segAttr.nameServerHostName);
      sessionError.setNsFileId(body.rqst.segAttr.castorFileId);
      sessionError.setTapeFSeq(body.rqst.tapeFseq);

    // Else this is a session error - don't need to fill in file specifics
    } else {
      sessionError.setErrorScope(SessionError::SESSION_SCOPE);
    }

    // Push the error onto the back of the list of errors received from the
    // rtcpd daemon
    m_sessionErrors.push_back(sessionError);

    // There is nothing else to be done
    return;

  // Else the message does not contain an error
  } else {
    // Get and remove from the pending transfer ids, the file transaction ID
    // that was sent by the client (the tapegatewayd daemon or the readtp or
    // writetp command-line tool)
    uint64_t fileTransactonId = 0;
    try {
      fileTransactonId = m_pendingTransferIds.remove(body.rqst.diskFseq);
    } catch(castor::exception::Exception &ex) {
      // Add context an re-throw
      TAPE_THROW_CODE(ex.code(),
        ": " << ex.getMessage().str());
    }

    try {
      processSuccessfullRtcpFileFinishedRequest(fileTransactonId, body);
    } catch(castor::exception::Exception &ex) {
      // Gather the error information into an SessionError object
      SessionError sessionError;
      sessionError.setErrorCode(ex.code());
      sessionError.setErrorMessage(ex.getMessage().str());

      // If this is a file-specific error
      if(0 < body.rqst.tapeFseq) {
        sessionError.setErrorScope(SessionError::FILE_SCOPE);
        sessionError.setFileTransactionId(fileTransactonId);
        sessionError.setNsHost(body.rqst.segAttr.nameServerHostName);
        sessionError.setNsFileId(body.rqst.segAttr.castorFileId);
        sessionError.setTapeFSeq(body.rqst.tapeFseq);

      // Else this is a session error - don't need to fill in file specifics
      } else {
        sessionError.setErrorScope(SessionError::SESSION_SCOPE);
      }

      // Push the error onto the back of the list of errors received from the
      // rtcpd daemon
      m_sessionErrors.push_back(sessionError);

      // There is nothing else to be done
      return;
    }
  }
}


//-----------------------------------------------------------------------------
// processSuccessfullRtcpFileFinishedRequest
//-----------------------------------------------------------------------------
void castor::tape::tapebridge::BridgeProtocolEngine::
  processSuccessfullRtcpFileFinishedRequest(
  const uint64_t                          fileTransactonId,
  const legacymsg::RtcpFileRqstErrMsgBody &body)
  throw(castor::exception::Exception) {

  // If migrating
  if(m_volume.mode() == tapegateway::WRITE) {
    processSuccessfullRtcpWriteFileFinishedRequest(fileTransactonId, body);

  // Else recall (READ or DUMP)
  } else {
    processSuccessfullRtcpRecallFileFinishedRequest(fileTransactonId, body);
  }
}


//-----------------------------------------------------------------------------
// processSuccessfullRtcpWriteFileFinishedRequest
//-----------------------------------------------------------------------------
void castor::tape::tapebridge::BridgeProtocolEngine::
  processSuccessfullRtcpWriteFileFinishedRequest(
  const uint64_t                          fileTransactonId,
  const legacymsg::RtcpFileRqstErrMsgBody &body)
  throw(castor::exception::Exception) {
  // Mark the migrated file as written but not yet flushed to tape.  This
  // includes created the file-migrated notification message that will be
  // sent to the tapegatewayd daemon once the rtcpd daemon has
  // notified the tapebridged daemon that the file has been flushed to
  // tape.
  const uint64_t fileSize     = body.rqst.bytesIn; // "in" from the disk
  uint64_t compressedFileSize = fileSize; // without compression
  if(0 < body.rqst.bytesOut && body.rqst.bytesOut < fileSize) {
    compressedFileSize        = body.rqst.bytesOut; // "Out" to the tape
  }
  {
    FileWrittenNotification notification;
    notification.fileTransactionId   = fileTransactonId;
    notification.nsHost              = body.rqst.segAttr.nameServerHostName;
    notification.nsFileId            = body.rqst.segAttr.castorFileId;
    notification.tapeFSeq            = body.rqst.tapeFseq;
    notification.positionCommandCode = body.rqst.positionMethod;
    notification.fileSize            = fileSize;
    notification.checksumName        = body.rqst.segAttr.segmCksumAlgorithm;
    notification.checksum            = body.rqst.segAttr.segmCksum;
    notification.compressedFileSize  = compressedFileSize;
    notification.blockId0            = body.rqst.blockId[0];
    notification.blockId1            = body.rqst.blockId[1];
    notification.blockId2            = body.rqst.blockId[2];
    notification.blockId3            = body.rqst.blockId[3];

    // The pending-migrations store will throw an exception if it finds any
    // inconsistencies in the file-written notification
    m_pendingMigrationsStore.fileWrittenWithoutFlush(notification);
  }
  {
    castor::dlf::Param params[] = {
      castor::dlf::Param("tapeFSeq", body.rqst.tapeFseq)};
    castor::dlf::dlf_writep(m_cuuid, DLF_LVL_SYSTEM,
      TAPEBRIDGE_MARKED_PENDING_MIGRATION_WRITTEN_WITHOUT_FLUSH, params);
  }
}


//-----------------------------------------------------------------------------
// processSuccessfullRtcpRecallFileFinishedRequest
//-----------------------------------------------------------------------------
void castor::tape::tapebridge::BridgeProtocolEngine::
  processSuccessfullRtcpRecallFileFinishedRequest(
  const uint64_t                          fileTransactonId,
  const legacymsg::RtcpFileRqstErrMsgBody &body)
  throw(castor::exception::Exception) {
  const uint64_t fileSize = body.rqst.bytesOut; // "out" from the tape
  const uint64_t tapebridgeTransId = m_tapebridgeTransactionCounter.next();

  // Create the report message
  std::auto_ptr<tapegateway::FileRecalledNotificationStruct> recalledFile(
    new tapegateway::FileRecalledNotificationStruct());
  recalledFile->setFileTransactionId(fileTransactonId);
  recalledFile->setNshost(body.rqst.segAttr.nameServerHostName);
  recalledFile->setFileid(body.rqst.segAttr.castorFileId);
  recalledFile->setFseq(body.rqst.tapeFseq);
  recalledFile->setPositionCommandCode((tapegateway::PositionCommandCode)
    body.rqst.positionMethod);
  recalledFile->setPath(body.rqst.filePath);
  recalledFile->setChecksumName(body.rqst.segAttr.segmCksumAlgorithm);
  recalledFile->setChecksum(body.rqst.segAttr.segmCksum);
  recalledFile->setFileSize(fileSize);
  tapegateway::FileRecallReportList report;
  report.setMountTransactionId(m_jobRequest.volReqId);
  report.setAggregatorTransactionId(tapebridgeTransId);
  report.addSuccessfulRecalls(recalledFile.release());

  // Send the report to the client
  timeval connectDuration = {0, 0};
  utils::SmartFd clientSock(m_clientProxy.connectAndSendMessage(report,
    connectDuration));
  {
    const double connectDurationDouble =
      utils::timevalToDouble(connectDuration);
    castor::dlf::Param params[] = {
      castor::dlf::Param("tapebridgeTransId", tapebridgeTransId),
      castor::dlf::Param("mountTransactionId", m_jobRequest.volReqId),
      castor::dlf::Param("volReqId", m_jobRequest.volReqId),
      castor::dlf::Param("TPVID", m_volume.vid()),
      castor::dlf::Param("driveUnit", m_jobRequest.driveUnit),
      castor::dlf::Param("dgn", m_jobRequest.dgn),
      castor::dlf::Param("clientHost", m_jobRequest.clientHost),
      castor::dlf::Param("clientPort", m_jobRequest.clientPort),
      castor::dlf::Param("clientType",
        utils::volumeClientTypeToString(m_volume.clientType())),
      castor::dlf::Param("clientSock", clientSock.get()),
      castor::dlf::Param("connectDuration", connectDurationDouble),
      castor::dlf::Param("nbSuccessful", report.successfulRecalls().size()),
      castor::dlf::Param("nbFailed", report.failedRecalls().size())};
    castor::dlf::dlf_writep(m_cuuid, DLF_LVL_SYSTEM,
      TAPEBRIDGE_SENT_FILERECALLREPORTLIST, params);
  }

  const int closedClientSock = clientSock.release();
  try {
    m_clientProxy.receiveNotificationReplyAndClose(tapebridgeTransId,
      closedClientSock);
  } catch(castor::exception::Exception &ex) {
    // Add some context information and rethrow exception
    TAPE_THROW_CODE(ex.code(), ": " << ex.getMessage().str());
  }
  {
    castor::dlf::Param params[] = {
      castor::dlf::Param("tapebridgeTransId" , tapebridgeTransId      ),
      castor::dlf::Param("mountTransactionId", m_jobRequest.volReqId  ),
      castor::dlf::Param("volReqId"          , m_jobRequest.volReqId  ),
      castor::dlf::Param("TPVID"             , m_volume.vid()         ),
      castor::dlf::Param("driveUnit"         , m_jobRequest.driveUnit ),
      castor::dlf::Param("dgn"               , m_jobRequest.dgn       ),
      castor::dlf::Param("clientHost"        , m_jobRequest.clientHost),
      castor::dlf::Param("clientPort"        , m_jobRequest.clientPort),
      castor::dlf::Param("clientType",
        utils::volumeClientTypeToString(m_volume.clientType())),
      castor::dlf::Param("clientSock"        , closedClientSock       )};
    castor::dlf::dlf_writep(m_cuuid, DLF_LVL_SYSTEM,
      TAPEBRIDGE_RECEIVED_ACK_OF_NOTIFICATION, params);
  }
}


//-----------------------------------------------------------------------------
// rtcpTapeReqRtcpdCallback
//-----------------------------------------------------------------------------
void castor::tape::tapebridge::BridgeProtocolEngine::rtcpTapeReqRtcpdCallback(
  const legacymsg::MessageHeader &header, const int socketFd)
  throw(castor::exception::Exception) {

  legacymsg::RtcpTapeRqstMsgBody body;
  RtcpTxRx::receiveMsgBody(m_cuuid, m_jobRequest.volReqId, socketFd,
    RTCPDNETRWTIMEOUT, header, body);

  // Reply to rtcpd with a positive acknowledgement
  legacymsg::MessageHeader ackMsg;
  ackMsg.magic       = RTCOPY_MAGIC;
  ackMsg.reqType     = RTCP_TAPE_REQ;
  ackMsg.lenOrStatus = 0;
  m_legacyTxRx.sendMsgHeader(socketFd, ackMsg);
}


//-----------------------------------------------------------------------------
// rtcpTapeErrReqRtcpdCallback
//-----------------------------------------------------------------------------
void
  castor::tape::tapebridge::BridgeProtocolEngine::rtcpTapeErrReqRtcpdCallback(
  const legacymsg::MessageHeader &header,
  const int                      socketFd)
  throw(castor::exception::Exception) {

  legacymsg::RtcpTapeRqstErrMsgBody body;
  RtcpTxRx::receiveMsgBody(m_cuuid, m_jobRequest.volReqId, socketFd,
    RTCPDNETRWTIMEOUT, header, body);

  // Reply to rtcpd with a positive acknowledgement
  legacymsg::MessageHeader ackMsg;
  ackMsg.magic       = RTCOPY_MAGIC;
  ackMsg.reqType     = RTCP_TAPEERR_REQ;
  ackMsg.lenOrStatus = 0;
  m_legacyTxRx.sendMsgHeader(socketFd, ackMsg);

  // If the rtcpd daemon has reported an error
  if(body.err.errorCode != 0) {
    // Gather the error information into a SessionError object
    SessionError sessionError;
    sessionError.setErrorCode(body.err.errorCode);
    sessionError.setErrorMessage(body.err.errorMsg);
    sessionError.setErrorScope(SessionError::SESSION_SCOPE);

    // Push the error onto the back of the list of errors generated during the
    // sesion with the rtcpd daemon
    m_sessionErrors.push_back(sessionError);
  }
}


//-----------------------------------------------------------------------------
// rtcpEndOfReqRtcpdCallback
//-----------------------------------------------------------------------------
void castor::tape::tapebridge::BridgeProtocolEngine::rtcpEndOfReqRtcpdCallback(
  const int socketFd) throw(castor::exception::Exception) {

  // Reply to rtcpd with a positive acknowledgement
  legacymsg::MessageHeader ackMsg;
  ackMsg.magic       = RTCOPY_MAGIC;
  ackMsg.reqType     = RTCP_ENDOF_REQ;
  ackMsg.lenOrStatus = 0;
  m_legacyTxRx.sendMsgHeader(socketFd, ackMsg);

  m_nbReceivedENDOF_REQs++;

  {
    castor::dlf::Param params[] = {
      castor::dlf::Param("mountTransactionId"  , m_jobRequest.volReqId  ),
      castor::dlf::Param("volReqId"            , m_jobRequest.volReqId  ),
      castor::dlf::Param("TPVID"               , m_volume.vid()         ),
      castor::dlf::Param("driveUnit"           , m_jobRequest.driveUnit ),
      castor::dlf::Param("dgn"                 , m_jobRequest.dgn       ),
      castor::dlf::Param("clientHost"          , m_jobRequest.clientHost),
      castor::dlf::Param("clientPort"          , m_jobRequest.clientPort),
      castor::dlf::Param("clientType",
        utils::volumeClientTypeToString(m_volume.clientType())),
      castor::dlf::Param("socketFd"            , socketFd               ),
      castor::dlf::Param("nbReceivedENDOF_REQs", m_nbReceivedENDOF_REQs )};
    castor::dlf::dlf_writep(m_cuuid, DLF_LVL_SYSTEM,
      TAPEBRIDGE_RECEIVED_RTCP_ENDOF_REQ, params);
  }

  const int closeRc = m_fileCloser.closeFd(
    m_sockCatalogue.releaseRtcpdDiskTapeIOControlConn(socketFd));
  const int nbDiskTapeIOControlConnsAfterClose =
    m_sockCatalogue.getNbDiskTapeIOControlConns();
  {
    castor::dlf::Param params[] = {
      castor::dlf::Param("mountTransactionId", m_jobRequest.volReqId  ),
      castor::dlf::Param("volReqId"          , m_jobRequest.volReqId  ),
      castor::dlf::Param("TPVID"             , m_volume.vid()         ),
      castor::dlf::Param("driveUnit"         , m_jobRequest.driveUnit ),
      castor::dlf::Param("dgn"               , m_jobRequest.dgn       ),
      castor::dlf::Param("clientHost "       , m_jobRequest.clientHost),
      castor::dlf::Param("clientPort"        , m_jobRequest.clientPort),
      castor::dlf::Param("clientType",
        utils::volumeClientTypeToString(m_volume.clientType())),
      castor::dlf::Param("socketFd"          , socketFd               ),
      castor::dlf::Param("closeRc"           , closeRc                ),
      castor::dlf::Param("nbIOControlConns"  ,
        nbDiskTapeIOControlConnsAfterClose)};
    castor::dlf::dlf_writep(m_cuuid, DLF_LVL_SYSTEM,
      TAPEBRIDGE_CLOSED_RTCPD_DISK_TAPE_CONNECTION_DUE_TO_ENDOF_REQ, params);
  }

  // If only if the initial callback connection is open, then record the fact
  // that the session with the rtcpd daemon is over and the rtcpd daemon is
  // awaiting the final RTCP_ENDOF_REQ message from the tapebridged daemon
  if(0 == nbDiskTapeIOControlConnsAfterClose) {
    m_sessionWithRtcpdIsFinished = true;

    castor::dlf::Param params[] = {
      castor::dlf::Param("mountTransactionId"  , m_jobRequest.volReqId  ),
      castor::dlf::Param("volReqId"            , m_jobRequest.volReqId  ),
      castor::dlf::Param("TPVID"               , m_volume.vid()         ),
      castor::dlf::Param("driveUnit"           , m_jobRequest.driveUnit ),
      castor::dlf::Param("dgn"                 , m_jobRequest.dgn       ),
      castor::dlf::Param("clientHost"          , m_jobRequest.clientHost),
      castor::dlf::Param("clientPort"          , m_jobRequest.clientPort),
      castor::dlf::Param("clientType",
        utils::volumeClientTypeToString(m_volume.clientType())),
      castor::dlf::Param("socketFd"            , socketFd               ),
      castor::dlf::Param("nbReceivedENDOF_REQs", m_nbReceivedENDOF_REQs)};
    castor::dlf::dlf_writep(m_cuuid, DLF_LVL_SYSTEM,
      TAPEBRIDGE_ONLY_INITIAL_RTCPD_CONNECTION_OPEN, params);
  }
}


//-----------------------------------------------------------------------------
// tapeBridgeFlushedToTapeCallback
//-----------------------------------------------------------------------------
void castor::tape::tapebridge::BridgeProtocolEngine::
  tapeBridgeFlushedToTapeCallback(
  const legacymsg::MessageHeader &header,
  const int                      socketFd)
  throw(castor::exception::Exception) {

  const char *const task = "process TAPEBRIDGE_FLUSHEDTOTAPE message";

  tapeBridgeFlushedToTapeMsgBody_t body;

  RtcpTxRx::receiveMsgBody(m_cuuid, m_jobRequest.volReqId, socketFd,
    RTCPDNETRWTIMEOUT, header, body);

  {
    castor::dlf::Param params[] = {
      castor::dlf::Param("mountTransactionId"       , m_jobRequest.volReqId  ),
      castor::dlf::Param("volReqId"                 , m_jobRequest.volReqId  ),
      castor::dlf::Param("TPVID"                    , m_volume.vid()         ),
      castor::dlf::Param("driveUnit"                , m_jobRequest.driveUnit ),
      castor::dlf::Param("dgn"                      , m_jobRequest.dgn       ),
      castor::dlf::Param("clientHost"               , m_jobRequest.clientHost),
      castor::dlf::Param("clientPort"               , m_jobRequest.clientPort),
      castor::dlf::Param("clientType"               ,
        utils::volumeClientTypeToString(m_volume.clientType())),
      castor::dlf::Param("volReqId"                 , body.volReqId          ),
      castor::dlf::Param("tapeFseq"                 , body.tapeFseq          ),
      castor::dlf::Param("bytesWrittenToTapeByFlush",
        body.bytesWrittenToTapeByFlush)};
    castor::dlf::dlf_writep(m_cuuid, DLF_LVL_SYSTEM,
      TAPEBRIDGE_RECEIVED_TAPEBRIDGE_FLUSHEDTOTAPE, params);
  }

  // Reply to rtcpd with a positive acknowledgement
  legacymsg::MessageHeader ackMsg;
  ackMsg.magic       = RTCOPY_MAGIC;
  ackMsg.reqType     = TAPEBRIDGE_FLUSHEDTOTAPE;
  ackMsg.lenOrStatus = 0;
  m_legacyTxRx.sendMsgHeader(socketFd, ackMsg);

  // Drop the message from rtcpd and return if the session is being shutdown
  if(shuttingDownRtcpdSession()) {
    castor::dlf::Param params[] = {
      castor::dlf::Param("mountTransactionId", m_jobRequest.volReqId  ),
      castor::dlf::Param("volReqId"          , m_jobRequest.volReqId  ),
      castor::dlf::Param("TPVID"             , m_volume.vid()         ),
      castor::dlf::Param("driveUnit"         , m_jobRequest.driveUnit ),
      castor::dlf::Param("dgn"               , m_jobRequest.dgn       ),
      castor::dlf::Param("clientHost"        , m_jobRequest.clientHost),
      castor::dlf::Param("clientPort"        , m_jobRequest.clientPort),
      castor::dlf::Param("clientType",
        utils::volumeClientTypeToString(m_volume.clientType()))};
    castor::dlf::dlf_writep(m_cuuid, DLF_LVL_SYSTEM,
      TAPEBRIDGE_DROPPING_TAPEBRIDGE_FLUSHEDTOTAPE_DUE_TO_SHUTTING_DOWN,
      params);
    return;
  }

  // If there is a volume request ID mismatch
  if(m_jobRequest.volReqId != body.volReqId) {
    // Gather the error information into an SessionError object
    std::ostringstream oss;
    oss <<
      "Failed to " << task <<
      ": message contains an unexpected volReqId"
      ": expected=" << m_jobRequest.volReqId <<
      ": actual=" << body.volReqId;
    SessionError sessionError;
    sessionError.setErrorCode(EBADMSG);
    sessionError.setErrorMessage(oss.str());
    sessionError.setErrorScope(SessionError::SESSION_SCOPE);

    // Push the error onto the back of the list of errors generated during the
    // tape session
    m_sessionErrors.push_back(sessionError);

    // There is nothing else to do
    return;
  }

  // If the tape-file sequence-number is invalid
  if(0 == body.tapeFseq) {
    // Gather the error information into an SessionError object
    std::ostringstream oss;
    oss <<
      "Failed to " << task <<
      ": message contains an fseq with the illegal value of 0";
    SessionError sessionError;
    sessionError.setErrorCode(EBADMSG);
    sessionError.setErrorMessage(oss.str());
    sessionError.setErrorScope(SessionError::SESSION_SCOPE);

    // Push the error onto the back of the list of errors generated during the
    // tape session
    m_sessionErrors.push_back(sessionError);

    // There is nothing else to do
    return;
  }

  // It is an error to receive a TAPEBRIDGE_FLUSHEDTOTAPE message if the
  // current tape mount is anything other than WRITE
  if(tapegateway::WRITE != m_volume.mode()) {
    // Gather the error information into an SessionError object
    std::ostringstream oss;
    oss <<
      "Failed to " << task <<
      ": Tape is not mounted for write";
    SessionError sessionError;
    sessionError.setErrorCode(EINVAL);
    sessionError.setErrorMessage(oss.str());
    sessionError.setErrorScope(SessionError::SESSION_SCOPE);

    // Push the error onto the back of the list of errors generated during the
    // tape session
    m_sessionErrors.push_back(sessionError);

    // There is nothing else to do
    return;
  }

  try {
    // Get and remove the batch of now flushed file-migrations that have a
    // tape-file sequence-number less than or equal to that of the flush
    const FileWrittenNotificationList migrations =
      m_pendingMigrationsStore.dataFlushedToTape(body.tapeFseq);

    // Correct the compression information of each file in the batch, taking
    // into account the number of bytes written to tape by the flush and the
    // fact that the current compression information of each file was read out
    // asynchronously with respect to the physical write operations of the
    // drive

    // If there is currently no client migration-report connection, then open
    // one and send the batch of flushed file-migrations to the client
    if(!m_sockCatalogue.clientMigrationReportSockIsSet()) {
      sendFlushedMigrationsToClient(migrations);
    // Else there is a client migration-report connection, therefore put the
    // batch of flushed migrations onto the pack of the flushed batches queue
    } else {
      m_flushedBatches.push_back(migrations);
    }
  } catch(castor::exception::Exception &ex) {
    // Gather the error information into an SessionError object
    std::ostringstream oss;
    oss <<
      "Failed to " << task <<
      ": " << ex.getMessage().str();
    SessionError sessionError;
    sessionError.setErrorCode(ECANCELED);
    sessionError.setErrorMessage(oss.str());
    sessionError.setErrorScope(SessionError::SESSION_SCOPE);

    // Push the error onto the back of the list of errors generated during the
    // tape session
    m_sessionErrors.push_back(sessionError);

    // There is nothing else to do
    return;
  }
}


//-----------------------------------------------------------------------------
// correctMigrationCompressionStatistics
//-----------------------------------------------------------------------------
void castor::tape::tapebridge::BridgeProtocolEngine::
  correctMigrationCompressionStatistics(
  FileWrittenNotificationList &migrations,
  const uint64_t              bytesWrittenToTapeByFlush)
  throw(castor::exception::Exception) {
  const double compressionRatio =
    calcMigrationCompressionRatio(migrations, bytesWrittenToTapeByFlush);

  // Set each compressed file size to be the original file size multipled by
  // the compression ratio
  for(FileWrittenNotificationList::iterator itor = migrations.begin();
    itor != migrations.end(); itor++) {
    FileWrittenNotification &notification = *itor;

    notification.compressedFileSize =
      (uint64_t)(notification.fileSize * compressionRatio);
  }
}


//-----------------------------------------------------------------------------
// calcMigrationCompressionRatio
//-----------------------------------------------------------------------------
double castor::tape::tapebridge::BridgeProtocolEngine::
  calcMigrationCompressionRatio(
  const FileWrittenNotificationList &migrations,
  const uint64_t                    bytesWrittenToTapeByFlush)
  throw() {
  uint64_t totalFileSize           = 0;
  uint64_t totalCompressedFileSize = 0;

  for(FileWrittenNotificationList::const_iterator itor = migrations.begin();
    itor != migrations.end(); itor++) {
    const FileWrittenNotification &notification = *itor;

    totalFileSize += notification.fileSize;
    totalCompressedFileSize += notification.compressedFileSize;
  }

  totalCompressedFileSize += bytesWrittenToTapeByFlush;

  // Calculate the ratio taking into account the fact that if the sum of the
  // orginal file sizes is zero thenn the ratio should be 1.0
  const double compressionRatio = (double)0.0 == totalFileSize ? 1.0 :
    (double)totalCompressedFileSize / (double)totalFileSize;

  return compressionRatio;
}


//-----------------------------------------------------------------------------
// sendFlushedMigrationsToClient
//-----------------------------------------------------------------------------
void castor::tape::tapebridge::BridgeProtocolEngine::
  sendFlushedMigrationsToClient(
    const FileWrittenNotificationList &notifications)
    throw (castor::exception::Exception) {

  // Get the next tape-bridge (aggregator) transaction id
  const uint64_t tapebridgeTransId = m_tapebridgeTransactionCounter.next();

  // Create an empty report message taking into account the fact that this
  // version of tapebridged does not update the tape-file sequence-number of
  // the tape in vmgrd
  tapegateway::FileMigrationReportList report;
  report.setMountTransactionId(m_jobRequest.volReqId);
  report.setAggregatorTransactionId(tapebridgeTransId);
  report.setFseqSet(false);
  report.setFseq(0);

  // Add each flushed-to-tape file to the report message
  for(FileWrittenNotificationList::const_iterator itor = notifications.begin();
    itor != notifications.end(); itor++) {

    std::auto_ptr<tapegateway::FileMigratedNotificationStruct>
      migratedFile(new tapegateway::FileMigratedNotificationStruct());
    migratedFile->setFileTransactionId(itor->fileTransactionId);
    migratedFile->setNshost(itor->nsHost);
    migratedFile->setFileid(itor->nsFileId);
    migratedFile->setFseq(itor->tapeFSeq);
    migratedFile->setPositionCommandCode((tapegateway::PositionCommandCode)
      itor->positionCommandCode);
    migratedFile->setFileSize(itor->fileSize);
    migratedFile->setChecksumName(itor->checksumName);
    migratedFile->setChecksum(itor->checksum);
    migratedFile->setCompressedFileSize(itor->compressedFileSize);
    migratedFile->setBlockId0(itor->blockId0);
    migratedFile->setBlockId1(itor->blockId1);
    migratedFile->setBlockId2(itor->blockId2);
    migratedFile->setBlockId3(itor->blockId3);
    report.addSuccessfulMigrations(migratedFile.release());
  }

  // Send the report message to the client (the tapegewayd daemon or the
  // writetp command-line tool
  timeval connectDuration = {0, 0};
  utils::SmartFd clientSock(m_clientProxy.connectAndSendMessage(report,
    connectDuration));
  {
    const double connectDurationDouble =
      utils::timevalToDouble(connectDuration);
    castor::dlf::Param params[] = {
      castor::dlf::Param("tapebridgeTransId", tapebridgeTransId),
      castor::dlf::Param("mountTransactionId", m_jobRequest.volReqId),
      castor::dlf::Param("volReqId", m_jobRequest.volReqId),
      castor::dlf::Param("TPVID", m_volume.vid()),
      castor::dlf::Param("driveUnit", m_jobRequest.driveUnit ),
      castor::dlf::Param("dgn", m_jobRequest.dgn),
      castor::dlf::Param("clientHost", m_jobRequest.clientHost),
      castor::dlf::Param("clientPort", m_jobRequest.clientPort),
      castor::dlf::Param("clientType",
        utils::volumeClientTypeToString(m_volume.clientType())),
      castor::dlf::Param("clientSock", clientSock.get()),
      castor::dlf::Param("connectDuration", connectDurationDouble),
      castor::dlf::Param("nbSuccessful", report.successfulMigrations().size()),
      castor::dlf::Param("nbFailed", report.failedMigrations().size())};
    castor::dlf::dlf_writep(m_cuuid, DLF_LVL_SYSTEM,
      TAPEBRIDGE_SENT_FILEMIGRATIONREPORTLIST, params);
  }

  // Store the socket-descriptor of the client migration-report connection in
  // the socket catalogue
  m_sockCatalogue.addClientMigrationReportSock(clientSock.release(),
    tapebridgeTransId);
}


//-----------------------------------------------------------------------------
// giveOutpRtcpdCallback
//-----------------------------------------------------------------------------
void castor::tape::tapebridge::BridgeProtocolEngine::giveOutpRtcpdCallback(
  const legacymsg::MessageHeader &header,
  const int                      socketFd)
  throw(castor::exception::Exception) {

  legacymsg::GiveOutpMsgBody body;

  RtcpTxRx::receiveMsgBody(m_cuuid, m_jobRequest.volReqId, socketFd,
    RTCPDNETRWTIMEOUT, header, body);

  m_clientProxy.notifyDumpMessage(m_tapebridgeTransactionCounter.next(),
    body.message);
}


//-----------------------------------------------------------------------------
// filesToMigrateListClientCallback
//-----------------------------------------------------------------------------
void castor::tape::tapebridge::BridgeProtocolEngine::
  filesToMigrateListClientCallback(
  IObject *const              obj,
  const GetMoreWorkConnection &getMoreWorkConnection)
  throw(castor::exception::Exception) {

  const char *const task = "process FilesToMigrateList message from client";

  // Down cast the reply to its specific class
  tapegateway::FilesToMigrateList *const reply =
    dynamic_cast<tapegateway::FilesToMigrateList*>(obj);

  if(reply == NULL) {
    TAPE_THROW_EX(castor::exception::Internal,
      ": Failed to " << task <<
      ": Failed to down cast reply object to"
      " tapegateway::FilesToMigrateList");
  }

  {
    castor::dlf::Param params[] = {
      castor::dlf::Param("tapebridgeTransId" ,
        reply->aggregatorTransactionId()                                     ),
      castor::dlf::Param("mountTransactionId", reply->mountTransactionId()   ),
      castor::dlf::Param("volReqId"          , m_jobRequest.volReqId         ),
      castor::dlf::Param("TPVID"             , m_volume.vid()                ),
      castor::dlf::Param("driveUnit"         , m_jobRequest.driveUnit        ),
      castor::dlf::Param("dgn"               , m_jobRequest.dgn              ),
      castor::dlf::Param("clientHost"        , m_jobRequest.clientHost       ),
      castor::dlf::Param("clientPort"        , m_jobRequest.clientPort       ),
      castor::dlf::Param("clientType",
        utils::volumeClientTypeToString(m_volume.clientType())               ),
      castor::dlf::Param("clientSock"        ,
        getMoreWorkConnection.clientSock                                     ),
      castor::dlf::Param("nbFiles"           , reply->filesToMigrate().size())};
    castor::dlf::dlf_writep(m_cuuid, DLF_LVL_SYSTEM,
      TAPEBRIDGE_RECEIVED_FILESTOMIGRATELIST, params);
  }

  m_clientProxy.checkTransactionIds(
    "FileToMigrateList",
    reply->mountTransactionId(),
    getMoreWorkConnection.tapebridgeTransId,
    reply->aggregatorTransactionId());

  if(0 == reply->filesToMigrate().size()) {
    // Should never happen
    TAPE_THROW_CODE(EBADMSG,
      ": Failed to " << task <<
      ": FilesToMigrateList message contains 0 files to migrate");
  }

  addFilesToMigrateToCache(reply->filesToMigrate());

  // Write the first file of the cache to the rtcpd disk/tape IO
  // control-connection that requested a file to migrate and triggered the
  // request to the client for a list of files to migrate
  sendCachedFileToMigrateToRtcpd(
    getMoreWorkConnection.rtcpdSock,
    getMoreWorkConnection.rtcpdReqMagic,
    getMoreWorkConnection.rtcpdReqType);
}


//-----------------------------------------------------------------------------
// addFilesToMigrateToCache
//-----------------------------------------------------------------------------
void castor::tape::tapebridge::BridgeProtocolEngine::addFilesToMigrateToCache(
  const std::vector<tapegateway::FileToMigrateStruct*> &clientFilesToMigrate)
  throw(castor::exception::Exception) {

  for(std::vector<tapegateway::FileToMigrateStruct*>::const_iterator itor =
    clientFilesToMigrate.begin(); itor != clientFilesToMigrate.end(); itor++) {
    const tapegateway::FileToMigrateStruct *const clientFileToMigrate = *itor;

    if(NULL == clientFileToMigrate) {
      // Should never happen
      TAPE_THROW_EX(castor::exception::Internal,
        ": fileToMigrate is a null pointer");
    }

    addFileToMigrateToCache(*clientFileToMigrate);
  }
}


//-----------------------------------------------------------------------------
// addFileToMigrateToCache
//-----------------------------------------------------------------------------
void castor::tape::tapebridge::BridgeProtocolEngine::addFileToMigrateToCache(
  const tapegateway::FileToMigrateStruct &clientFileToMigrate)
  throw(castor::exception::Exception) {

  // Throw an exception if the tape file sequence number from the client is
  // invalid
  if(clientFileToMigrate.fseq() != m_nextDestinationTapeFSeq) {
    TAPE_THROW_CODE(ECANCELED,
      ": Invalid tape-file sequence-number from client"
      ": expected=" << (m_nextDestinationTapeFSeq) <<
      ": actual=" << clientFileToMigrate.fseq());
  }

  // Update the next expected tape file sequence number
  m_nextDestinationTapeFSeq++;

  // Push the file to migrate onto the back of the cache
  FileToMigrate fileToMigrate;
  fileToMigrate.fileTransactionId = clientFileToMigrate.fileTransactionId();
  fileToMigrate.nsHost = clientFileToMigrate.nshost();
  fileToMigrate.fileId = clientFileToMigrate.fileid();
  fileToMigrate.fseq = clientFileToMigrate.fseq();
  fileToMigrate.positionMethod = clientFileToMigrate.positionCommandCode();
  fileToMigrate.fileSize = clientFileToMigrate.fileSize();
  fileToMigrate.lastKnownFilename = clientFileToMigrate.lastKnownFilename();
  fileToMigrate.lastModificationTime =
    clientFileToMigrate.lastModificationTime();
  fileToMigrate.path = clientFileToMigrate.path();
  fileToMigrate.umask = clientFileToMigrate.umask();
  m_filesToMigrate.push_back(fileToMigrate);
}


//-----------------------------------------------------------------------------
// sendFileToMigrateToRtcpd
//-----------------------------------------------------------------------------
void castor::tape::tapebridge::BridgeProtocolEngine::sendFileToMigrateToRtcpd(
  const FileToMigrate &fileToMigrate,
  const int           rtcpdSock,
  const uint32_t      rtcpdReqMagic,
  const uint32_t      rtcpdReqType)
  throw(castor::exception::Exception) {

  // Remember the file transaction ID and get its unique index to be
  // passed to rtcpd through the "rtcpFileRequest.disk_fseq" message
  // field
  int diskFSeq = 0;
  try {
    diskFSeq = m_pendingTransferIds.insert(fileToMigrate.fileTransactionId);
  } catch(castor::exception::Exception &ex) {
    // Add context and rethrow
    TAPE_THROW_CODE(ex.code(),
      ": " << ex.getMessage().str());
  }

  // Add the file to the pending migrations store
  {
    RequestToMigrateFile request;

    request.fileTransactionId = fileToMigrate.fileTransactionId;
    request.nsHost = fileToMigrate.nsHost;
    request.nsFileId = fileToMigrate.fileId;
    request.positionCommandCode = fileToMigrate.positionMethod;
    request.tapeFSeq = fileToMigrate.fseq;
    request.fileSize = fileToMigrate.fileSize;
    request.lastKnownFilename = fileToMigrate.lastKnownFilename;
    request.lastModificationTime = fileToMigrate.lastModificationTime;
    request.path = fileToMigrate.path;
    request.umask = fileToMigrate.umask;

    m_pendingMigrationsStore.receivedRequestToMigrateFile(request);
  }

  {
    castor::dlf::Param params[] = {
      castor::dlf::Param("volReqId", m_jobRequest.volReqId),
      castor::dlf::Param("TPVID", m_volume.vid()),
      castor::dlf::Param("driveUnit", m_jobRequest.driveUnit),
      castor::dlf::Param("dgn", m_jobRequest.dgn),
      castor::dlf::Param("clientHost", m_jobRequest.clientHost),
      castor::dlf::Param("clientPort", m_jobRequest.clientPort),
      castor::dlf::Param("clientType",
        utils::volumeClientTypeToString(m_volume.clientType())),
      castor::dlf::Param("fileTransactionId",
        fileToMigrate.fileTransactionId),
      castor::dlf::Param("NSHOSTNAME", fileToMigrate.nsHost),
      castor::dlf::Param("NSFILEID", fileToMigrate.fileId),
      castor::dlf::Param("tapeFSeq", fileToMigrate.fseq)};
    castor::dlf::dlf_writep(m_cuuid, DLF_LVL_SYSTEM,
      TAPEBRIDGE_ADDED_PENDING_MIGRATION_TO_STORE, params);
  }

  // Give file to migrate to rtcpd
  {
    char migrationTapeFileId[CA_MAXPATHLEN+1];
    utils::setBytes(migrationTapeFileId, '\0');
    generateMigrationTapeFileId(fileToMigrate.fileId, migrationTapeFileId);
    unsigned char blockId[4];
    utils::setBytes(blockId, '\0');
    char nshost[CA_MAXHOSTNAMELEN+1];
    utils::copyString(nshost, fileToMigrate.nsHost.c_str());

    RtcpTxRx::giveFileToRtcpd(
      m_cuuid,
      m_jobRequest.volReqId,
      rtcpdSock,
      RTCPDNETRWTIMEOUT,
      WRITE_ENABLE,
      fileToMigrate.path.c_str(),
      fileToMigrate.fileSize,
      "",
      RECORDFORMAT,
      migrationTapeFileId,
      RTCOPYCONSERVATIVEUMASK,
      fileToMigrate.positionMethod,
      fileToMigrate.fseq,
      diskFSeq,
      nshost,
      fileToMigrate.fileId,
      blockId);
  }

  // Ask rtcpd to request more work, sending an empty string for the tape path
  {
    const char *tapePath = "";
    RtcpTxRx::askRtcpdToRequestMoreWork(
      m_cuuid,
      m_jobRequest.volReqId,
      tapePath,
      rtcpdSock,
      RTCPDNETRWTIMEOUT,
      WRITE_ENABLE);
  }

  // Tell rtcpd end of file list
  RtcpTxRx::tellRtcpdEndOfFileList(
    m_cuuid,
    m_jobRequest.volReqId,
    rtcpdSock,
    RTCPDNETRWTIMEOUT);

  {
    castor::dlf::Param params[] = {
      castor::dlf::Param("mountTransactionId", m_jobRequest.volReqId  ),
      castor::dlf::Param("volReqId"          , m_jobRequest.volReqId  ),
      castor::dlf::Param("TPVID"             , m_volume.vid()         ),
      castor::dlf::Param("driveUnit"         , m_jobRequest.driveUnit ),
      castor::dlf::Param("dgn"               , m_jobRequest.dgn       ),
      castor::dlf::Param("clientHost"        , m_jobRequest.clientHost),
      castor::dlf::Param("clientPort"        , m_jobRequest.clientPort),
      castor::dlf::Param("clientType",
        utils::volumeClientTypeToString(m_volume.clientType())),
      castor::dlf::Param("rtcpdSock"         , rtcpdSock              )};
    castor::dlf::dlf_writep(m_cuuid, DLF_LVL_DEBUG,
      TAPEBRIDGE_SEND_DELAYED_REQUEST_MORE_WORK_ACK_TO_RTCPD, params);
  }

  // Send delayed acknowledge of the request for more work
  legacymsg::MessageHeader ackMsg;
  ackMsg.magic       = rtcpdReqMagic;
  ackMsg.reqType     = rtcpdReqType;
  ackMsg.lenOrStatus = 0;
  m_legacyTxRx.sendMsgHeader(rtcpdSock, ackMsg);

  {
    castor::dlf::Param params[] = {
      castor::dlf::Param("mountTransactionId", m_jobRequest.volReqId  ),
      castor::dlf::Param("volReqId"          , m_jobRequest.volReqId  ),
      castor::dlf::Param("TPVID"             , m_volume.vid()         ),
      castor::dlf::Param("driveUnit"         , m_jobRequest.driveUnit ),
      castor::dlf::Param("dgn"               , m_jobRequest.dgn       ),
      castor::dlf::Param("clientHost"        , m_jobRequest.clientHost),
      castor::dlf::Param("clientPort"        , m_jobRequest.clientPort),
      castor::dlf::Param("clientType",
        utils::volumeClientTypeToString(m_volume.clientType())),
      castor::dlf::Param("rtcpdSock"         , rtcpdSock              )};
    castor::dlf::dlf_writep(m_cuuid, DLF_LVL_SYSTEM,
      TAPEBRIDGE_SENT_DELAYED_REQUEST_MORE_WORK_ACK_TO_RTCPD, params);
  }
}


//-----------------------------------------------------------------------------
// filesToRecallListClientCallback
//-----------------------------------------------------------------------------
void castor::tape::tapebridge::BridgeProtocolEngine::
  filesToRecallListClientCallback(
  IObject *const              obj,
  const GetMoreWorkConnection &getMoreWorkConnection)
  throw(castor::exception::Exception) {

  const char *const task = "process FilesToRecallList message from client";

  // Throw an exception if there is no tape path associated with the
  // initiating rtcpd request
  if(getMoreWorkConnection.rtcpdReqTapePath.empty()) {
    TAPE_THROW_CODE(ECANCELED,
      ": The initiating rtcpd request has no associated tape path");
  }

  // Down cast the reply to its specific class
  tapegateway::FilesToRecallList *reply =
    dynamic_cast<tapegateway::FilesToRecallList*>(obj);

  if(reply == NULL) {
    TAPE_THROW_EX(castor::exception::Internal,
      ": Failed to " << task <<
      ": Failed to down cast reply object to tapegateway::FilesToRecallList");
  }

  {
    castor::dlf::Param params[] = {
      castor::dlf::Param("tapebridgeTransId" ,
        reply->aggregatorTransactionId()                                    ),
      castor::dlf::Param("mountTransactionId", reply->mountTransactionId()  ),
      castor::dlf::Param("volReqId"          , m_jobRequest.volReqId        ),
      castor::dlf::Param("TPVID"             , m_volume.vid()               ),
      castor::dlf::Param("driveUnit"         , m_jobRequest.driveUnit       ),
      castor::dlf::Param("dgn"               , m_jobRequest.dgn             ),
      castor::dlf::Param("clientHost"        , m_jobRequest.clientHost      ),
      castor::dlf::Param("clientPort"        , m_jobRequest.clientPort      ),
      castor::dlf::Param("clientType",
        utils::volumeClientTypeToString(m_volume.clientType())              ),
      castor::dlf::Param("clientSock"        ,
        getMoreWorkConnection.clientSock),
      castor::dlf::Param("nbFiles"           , reply->filesToRecall().size())};
    castor::dlf::dlf_writep(m_cuuid, DLF_LVL_SYSTEM,
      TAPEBRIDGE_RECEIVED_FILESTORECALLLIST, params);
  }

  m_clientProxy.checkTransactionIds(
     "FileToRecallList",
     reply->mountTransactionId(),
     getMoreWorkConnection.tapebridgeTransId,
     reply->aggregatorTransactionId());

  if(0 == reply->filesToRecall().size()) {
    // Should never happen
    TAPE_THROW_EX(castor::exception::Internal,
      ": Failed to " << task <<
      ": FilesToRecallList contains 0 files to recall");
  }

  addFilesToRecallToCache(reply->filesToRecall());

  // Write the first file of the cache to the rtcpd disk/tape IO
  // control-connection that requested a file to recall and triggered the
  // request to the client for a list of files to recall
  sendCachedFileToRecallToRtcpd(
    getMoreWorkConnection.rtcpdSock,
    getMoreWorkConnection.rtcpdReqMagic,
    getMoreWorkConnection.rtcpdReqType,
    getMoreWorkConnection.rtcpdReqTapePath);
}


//-----------------------------------------------------------------------------
// addFilesToRecallToCache
//-----------------------------------------------------------------------------
void castor::tape::tapebridge::BridgeProtocolEngine::addFilesToRecallToCache(
  const std::vector<tapegateway::FileToRecallStruct*> &clientFilesToRecall)
  throw(castor::exception::Exception) {

  for(std::vector<tapegateway::FileToRecallStruct*>::const_iterator itor =
    clientFilesToRecall.begin(); itor != clientFilesToRecall.end(); itor++) {
    const tapegateway::FileToRecallStruct *const clientFileToRecall = *itor;

    if(NULL == clientFileToRecall) {
      // Should never happen
      TAPE_THROW_EX(castor::exception::Internal,
        ": fileToRecall is a null pointer");
    }

    addFileToRecallToCache(*clientFileToRecall);
  }
}


//-----------------------------------------------------------------------------
// addFileToRecallToCache
//-----------------------------------------------------------------------------
void castor::tape::tapebridge::BridgeProtocolEngine::addFileToRecallToCache(
  const tapegateway::FileToRecallStruct &clientFileToRecall)
  throw(castor::exception::Exception) {

  // Push the file to migrate onto the back of the cache
  FileToRecall fileToRecall;
  fileToRecall.fileTransactionId = clientFileToRecall.fileTransactionId();
  fileToRecall.nsHost = clientFileToRecall.nshost();
  fileToRecall.fileId = clientFileToRecall.fileid();
  fileToRecall.fseq = clientFileToRecall.fseq();
  fileToRecall.positionMethod = clientFileToRecall.positionCommandCode();
  fileToRecall.path = clientFileToRecall.path();
  fileToRecall.blockId0 = clientFileToRecall.blockId0();
  fileToRecall.blockId1 = clientFileToRecall.blockId1();
  fileToRecall.blockId2 = clientFileToRecall.blockId2();
  fileToRecall.blockId3 = clientFileToRecall.blockId3();
  fileToRecall.umask = clientFileToRecall.umask();
  m_filesToRecall.push_back(fileToRecall);
}


//-----------------------------------------------------------------------------
// noMoreFilesClientCallback
//-----------------------------------------------------------------------------
void castor::tape::tapebridge::BridgeProtocolEngine::
  noMoreFilesClientCallback(
  IObject *const              obj,
  const GetMoreWorkConnection &getMoreWorkConnection)
  throw(castor::exception::Exception) {

  // Down cast the reply to its specific class
  const tapegateway::NoMoreFiles *reply =
    dynamic_cast<const tapegateway::NoMoreFiles*>(obj);

  if(reply == NULL) {
    TAPE_THROW_EX(castor::exception::Internal,
      ": Failed to down cast reply object to tapegateway::NoMoreFiles");
  }

  castor::dlf::Param params[] = {
    castor::dlf::Param("tapebridgeTransId" , reply->aggregatorTransactionId()),
    castor::dlf::Param("mountTransactionId", reply->mountTransactionId()     ),
    castor::dlf::Param("volReqId"          , m_jobRequest.volReqId           ),
    castor::dlf::Param("TPVID"             , m_volume.vid()                  ),
    castor::dlf::Param("driveUnit"         , m_jobRequest.driveUnit          ),
    castor::dlf::Param("dgn"               , m_jobRequest.dgn                ),
    castor::dlf::Param("clientHost"        , m_jobRequest.clientHost         ),
    castor::dlf::Param("clientPort"        , m_jobRequest.clientPort         ),
    castor::dlf::Param("clientType",
      utils::volumeClientTypeToString(m_volume.clientType())),
    castor::dlf::Param("clientSock"        ,
     getMoreWorkConnection.clientSock)};
  castor::dlf::dlf_writep(m_cuuid, DLF_LVL_SYSTEM,
    TAPEBRIDGE_RECEIVED_NOMOREFILES, params);

  m_clientProxy.checkTransactionIds(
    "NoMoreFiles",
    reply->mountTransactionId(),
    getMoreWorkConnection.tapebridgeTransId,
    reply->aggregatorTransactionId());

  tellRtcpdThereAreNoMoreFiles(
    getMoreWorkConnection.rtcpdSock,
    getMoreWorkConnection.rtcpdReqMagic,
    getMoreWorkConnection.rtcpdReqType);
}


//-----------------------------------------------------------------------------
// tellRtcpdThereAreNoMoreFiles
//-----------------------------------------------------------------------------
void castor::tape::tapebridge::BridgeProtocolEngine::
  tellRtcpdThereAreNoMoreFiles(
  const int      rtcpdSock,
  const uint32_t rtcpdReqMagic,
  const uint32_t rtcpdReqType)
  throw(castor::exception::Exception) {

  // If migrating files to tape, then notify the internal pending-migration
  // store that there are no more files to migrate.
  if(m_volume.mode() == tapegateway::WRITE) {
    m_pendingMigrationsStore.noMoreFilesToMigrate();
  }

  // Tell RTCPD there is no file by sending an empty file list
  RtcpTxRx::tellRtcpdEndOfFileList(m_cuuid, m_jobRequest.volReqId, rtcpdSock,
    RTCPDNETRWTIMEOUT);

  // Send delayed acknowledge of the request for more work
  legacymsg::MessageHeader ackMsg;
  ackMsg.magic       = rtcpdReqMagic;
  ackMsg.reqType     = rtcpdReqType;
  ackMsg.lenOrStatus = 0;
  m_legacyTxRx.sendMsgHeader(rtcpdSock, ackMsg);

  {
    castor::dlf::Param params[] = {
      castor::dlf::Param("mountTransactionId", m_jobRequest.volReqId  ),
      castor::dlf::Param("volReqId"          , m_jobRequest.volReqId  ),
      castor::dlf::Param("TPVID"             , m_volume.vid()         ),
      castor::dlf::Param("driveUnit"         , m_jobRequest.driveUnit ),
      castor::dlf::Param("dgn"               , m_jobRequest.dgn       ),
      castor::dlf::Param("clientHost"        , m_jobRequest.clientHost),
      castor::dlf::Param("clientPort"        , m_jobRequest.clientPort),
      castor::dlf::Param("clientType",
        utils::volumeClientTypeToString(m_volume.clientType())),
      castor::dlf::Param("rtcpdSock"         , rtcpdSock              )};
    castor::dlf::dlf_writep(m_cuuid, DLF_LVL_SYSTEM,
      TAPEBRIDGE_SENT_DELAYED_REQUEST_MORE_WORK_ACK_TO_RTCPD, params);
  }
}


//-----------------------------------------------------------------------------
// endNotificationErrorReportForGetMoreWorkClientCallback
//-----------------------------------------------------------------------------
void castor::tape::tapebridge::BridgeProtocolEngine::
  endNotificationErrorReportForGetMoreWorkClientCallback(
  IObject *const              obj,
  const GetMoreWorkConnection &getMoreWorkConnection)
  throw(castor::exception::Exception) {

  // Down cast the reply to its specific class
  const tapegateway::EndNotificationErrorReport *reply =
    dynamic_cast<const tapegateway::EndNotificationErrorReport*>(obj);

  if(reply == NULL) {
    TAPE_THROW_EX(castor::exception::Internal,
      ": Failed to down cast reply object to "
      "tapegateway::EndNotificationErrorReport");
  }

  // Log the error report from the client
  castor::dlf::Param params[] = {
    castor::dlf::Param("tapebridgeTransId" , reply->aggregatorTransactionId()),
    castor::dlf::Param("mountTransactionId", reply->mountTransactionId()     ),
    castor::dlf::Param("volReqId"          , m_jobRequest.volReqId           ),
    castor::dlf::Param("TPVID"             , m_volume.vid()                  ),
    castor::dlf::Param("driveUnit"         , m_jobRequest.driveUnit          ),
    castor::dlf::Param("dgn"               , m_jobRequest.dgn                ),
    castor::dlf::Param("clientHost"        , m_jobRequest.clientHost         ),
    castor::dlf::Param("clientPort"        , m_jobRequest.clientPort         ),
    castor::dlf::Param("clientType",
      utils::volumeClientTypeToString(m_volume.clientType())),
    castor::dlf::Param("clientSock"        ,
      getMoreWorkConnection.clientSock),
    castor::dlf::Param("errorCode"         , reply->errorCode()              ),
    castor::dlf::Param("errorMessage"      , reply->errorMessage()           )};
  castor::dlf::dlf_writep(m_cuuid, DLF_LVL_SYSTEM,
    TAPEBRIDGE_RECEIVED_ENDNOTIFCATIONERRORREPORT, params);

  m_clientProxy.checkTransactionIds(
    "EndNotificationErrorReport",
    reply->mountTransactionId(),
    getMoreWorkConnection.tapebridgeTransId,
    reply->aggregatorTransactionId());

  {
    // Put the error report from the client into a SessionError object
    std::ostringstream oss;
    oss <<
      "Client error report"
      ": " << reply->errorMessage();
    SessionError sessionError;
    sessionError.setErrorCode(reply->errorCode());
    sessionError.setErrorMessage(oss.str());
    sessionError.setErrorScope(SessionError::SESSION_SCOPE);

    // Push the error onto the back of the list of errors generated during the
    // tape session
    m_sessionErrors.push_back(sessionError);
  }

  tellRtcpdThereAreNoMoreFiles(
    getMoreWorkConnection.rtcpdSock,
    getMoreWorkConnection.rtcpdReqMagic,
    getMoreWorkConnection.rtcpdReqType);
}


//-----------------------------------------------------------------------------
// notifyRtcpdEndOfSession
//-----------------------------------------------------------------------------
void castor::tape::tapebridge::BridgeProtocolEngine::notifyRtcpdEndOfSession()
  throw(castor::exception::Exception) {

  // Send an RTCP_ENDOF_REQ message to rtcpd via the initial callback
  // connection
  legacymsg::MessageHeader endofReqMsg;
  endofReqMsg.magic       = RTCOPY_MAGIC;
  endofReqMsg.reqType     = RTCP_ENDOF_REQ;
  endofReqMsg.lenOrStatus = 0;
  m_legacyTxRx.sendMsgHeader(m_sockCatalogue.getInitialRtcpdConn(),
    endofReqMsg);

  {
    castor::dlf::Param params[] = {
      castor::dlf::Param("mountTransactionId"  , m_jobRequest.volReqId  ),
      castor::dlf::Param("volReqId"            , m_jobRequest.volReqId  ),
      castor::dlf::Param("TPVID"               , m_volume.vid()         ),
      castor::dlf::Param("driveUnit"           , m_jobRequest.driveUnit ),
      castor::dlf::Param("dgn"                 , m_jobRequest.dgn       ),
      castor::dlf::Param("clientHost"          , m_jobRequest.clientHost),
      castor::dlf::Param("clientPort"          , m_jobRequest.clientPort),
      castor::dlf::Param("clientType",
        utils::volumeClientTypeToString(m_volume.clientType())),
      castor::dlf::Param("initialSocketFd"     ,
        m_sockCatalogue.getInitialRtcpdConn()),
      castor::dlf::Param("nbReceivedENDOF_REQs", m_nbReceivedENDOF_REQs)};
    castor::dlf::dlf_writep(m_cuuid, DLF_LVL_SYSTEM,
      TAPEBRIDGE_SEND_END_OF_SESSION_TO_RTCPD, params);
  }

  // Receive the acknowledge of the RTCP_ENDOF_REQ message
  legacymsg::MessageHeader ackMsg;
  try {
    m_legacyTxRx.receiveMsgHeader(m_sockCatalogue.getInitialRtcpdConn(),
      ackMsg);
  } catch(castor::exception::Exception &ex) {
    TAPE_THROW_CODE(EPROTO,
      ": Failed to receive acknowledge of RTCP_ENDOF_REQ from rtcpd: "
      << ex.getMessage().str());
  }
}


//-----------------------------------------------------------------------------
// notifyClientOfAnyFileSpecificErrors
//-----------------------------------------------------------------------------
void castor::tape::tapebridge::BridgeProtocolEngine::
  notifyClientOfAnyFileSpecificErrors() throw() {

  try {

    // Inner try and catch to convert std::exception and unknown exceptions
    // (...) to castor::exception::Exception
    try {

      // Create the list of file-specific errors from the list of all errors
      // received from rtcpd
      std::list<SessionError> fileSpecificSessionErrors;
      for(
        std::list<SessionError>::const_iterator itor = m_sessionErrors.begin();
        itor != m_sessionErrors.end(); itor++) {
        if(SessionError::FILE_SCOPE == itor->getErrorScope()) {
          fileSpecificSessionErrors.push_back(*itor);
        }
      }

      // If there are any file-specific errors, then notify the client
      if(!fileSpecificSessionErrors.empty()) {

        // How the client is notified of file-specific error is different
        // depending on the tape access-mode
        switch(m_volume.mode()) {
        case tapegateway::READ:
          notifyClientOfFailedRecalls(m_cuuid, fileSpecificSessionErrors);
          break;
        case tapegateway::WRITE:
          notifyClientOfFailedMigrations(m_cuuid, fileSpecificSessionErrors);
          break;
        case tapegateway::DUMP:
          // Do nothing
          break;
        default:
          TAPE_THROW_EX(castor::exception::Internal,
            ": Unknown VolumeMode"
            ": Actual=" << m_volume.mode());
        }
      }
    } catch(std::exception &se) {
      TAPE_THROW_EX(castor::exception::Internal,
        ": Caught a std::exception"
        ": what=" << se.what());
    } catch(castor::exception::Exception &ex) {
      // Simply rethrow as there is no need to convert exception type
      throw ex;
    } catch(...) {
      TAPE_THROW_EX(castor::exception::Internal,
        ": Caught an unknown exception");
    }

  } catch(castor::exception::Exception &ex) {
    castor::dlf::Param params[] = {
      castor::dlf::Param("mountTransactionId", m_jobRequest.volReqId  ),
      castor::dlf::Param("volReqId"          , m_jobRequest.volReqId  ),
      castor::dlf::Param("TPVID"             , m_volume.vid()         ),
      castor::dlf::Param("driveUnit"         , m_jobRequest.driveUnit ),
      castor::dlf::Param("dgn"               , m_jobRequest.dgn       ),
      castor::dlf::Param("clientHost"        , m_jobRequest.clientHost),
      castor::dlf::Param("clientPort"        , m_jobRequest.clientPort),
      castor::dlf::Param("clientType",
        utils::volumeClientTypeToString(m_volume.clientType())),
      castor::dlf::Param("errorCode"         , ex.code()              ),
      castor::dlf::Param("errorMessage"      , ex.getMessage().str()  )};
    castor::dlf::dlf_writep(m_cuuid, DLF_LVL_ERROR,
      TAPEBRIDGE_FAILED_TO_NOTIFY_CLIENT_OF_FILE_ERRORS, params);
  }
}


//-----------------------------------------------------------------------------
// notifyClientOfFailedMigrations
//-----------------------------------------------------------------------------
void castor::tape::tapebridge::BridgeProtocolEngine::
  notifyClientOfFailedMigrations(const Cuuid_t &cuuid,
  const std::list<SessionError> &sessionErrors)
  throw(castor::exception::Exception) {

  // Return if there are no errors to report
  if(sessionErrors.empty()) {
    return;
  }

  // Create the list report
  const uint64_t tapebridgeTransId = m_tapebridgeTransactionCounter.next();
  tapegateway::FileMigrationReportList listReport;
  listReport.setMountTransactionId(m_jobRequest.volReqId);
  listReport.setAggregatorTransactionId(tapebridgeTransId);

  // Add the individual file error-reports
  for(std::list<SessionError>::const_iterator itor = sessionErrors.begin();
    itor != sessionErrors.end(); itor++) {
    if(SessionError::FILE_SCOPE != itor->getErrorScope()) {
      TAPE_THROW_EX(castor::exception::Internal,
        ": Failed to notifyClientOfFailedMigrations"
        ": Scope of SessionError is not FILE_SCOPE");
    }
    std::auto_ptr<tapegateway::FileErrorReportStruct> fileReport(
      new tapegateway::FileErrorReportStruct());
    fileReport->setFileTransactionId(itor->getFileTransactionId());
    fileReport->setNshost(itor->getNsHost());
    fileReport->setFileid(itor->getNsFileId());
    fileReport->setFseq(itor->getTapeFSeq());
    fileReport->setErrorCode(itor->getErrorCode());
    fileReport->setErrorMessage(itor->getErrorMessage());
    listReport.addFailedMigrations(fileReport.release());
  }

  // Send the report to the client and receive the reply
  timeval connectDuration = {0, 0};
  utils::SmartFd clientSock(m_clientProxy.connectAndSendMessage(
    listReport, connectDuration));
  try {
    m_clientProxy.receiveNotificationReplyAndClose(
      tapebridgeTransId, clientSock.release());
  } catch(castor::exception::Exception &ex) {
    // Add some context information and rethrow exception
    TAPE_THROW_CODE(ex.code(), ": " << ex.getMessage().str());
  }
  {
    const double connectDurationDouble =
      utils::timevalToDouble(connectDuration);
    castor::dlf::Param params[] = {
      castor::dlf::Param("tapebridgeTransId" , tapebridgeTransId       ),
      castor::dlf::Param("mountTransactionId", m_jobRequest.volReqId   ),
      castor::dlf::Param("volReqId"          , m_jobRequest.volReqId   ),
      castor::dlf::Param("TPVID"             , m_volume.vid()          ),
      castor::dlf::Param("driveUnit"         , m_jobRequest.driveUnit  ),
      castor::dlf::Param("dgn"               , m_jobRequest.dgn        ),
      castor::dlf::Param("clientHost"        , m_jobRequest.clientHost ),
      castor::dlf::Param("clientPort"        , m_jobRequest.clientPort ),
      castor::dlf::Param("clientType"        ,
        utils::volumeClientTypeToString(m_volume.clientType())),
      castor::dlf::Param("clientSock"        , clientSock.get()        ),
      castor::dlf::Param("connectDuration"   , connectDurationDouble   ),
      castor::dlf::Param("nbSuccessful"      ,
        listReport.successfulMigrations().size()),
      castor::dlf::Param("nbFailed"          ,
        listReport.failedMigrations().size())};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM,
      TAPEBRIDGE_SENT_FILEMIGRATIONREPORTLIST, params);
  }
}


//-----------------------------------------------------------------------------
// notifyClientOfFailedRecalls
//-----------------------------------------------------------------------------
void castor::tape::tapebridge::BridgeProtocolEngine::
  notifyClientOfFailedRecalls(const Cuuid_t &cuuid,
  const std::list<SessionError> &sessionErrors)
  throw(castor::exception::Exception) {

  // Return if there are no errors to report
  if(sessionErrors.empty()) {
    return;
  }

  // Create the list report
  const uint64_t tapebridgeTransId = m_tapebridgeTransactionCounter.next();
  tapegateway::FileRecallReportList listReport;
  listReport.setMountTransactionId(m_jobRequest.volReqId);
  listReport.setAggregatorTransactionId(tapebridgeTransId);

  // Add the individual file error-reports
  for(std::list<SessionError>::const_iterator itor = sessionErrors.begin();
    itor != sessionErrors.end(); itor++) {
    if(SessionError::FILE_SCOPE != itor->getErrorScope()) {
      TAPE_THROW_EX(castor::exception::Internal,
        ": Failed to notifyClientOfFailedMigrations"
        ": Scope of SessionError is not FILE_SCOPE");
    }
    std::auto_ptr<tapegateway::FileErrorReportStruct> fileReport(
      new tapegateway::FileErrorReportStruct());
    fileReport->setFileTransactionId(itor->getFileTransactionId());
    fileReport->setNshost(itor->getNsHost());
    fileReport->setFileid(itor->getNsFileId());
    fileReport->setFseq(itor->getTapeFSeq());
    fileReport->setErrorCode(itor->getErrorCode());
    fileReport->setErrorMessage(itor->getErrorMessage());
    listReport.addFailedRecalls(fileReport.release());
  }

  // Send the report to the client and receive the reply
  timeval connectDuration = {0, 0};
  utils::SmartFd clientSock(m_clientProxy.connectAndSendMessage(
    listReport, connectDuration));
  try {
    m_clientProxy.receiveNotificationReplyAndClose(tapebridgeTransId,
      clientSock.release());
  } catch(castor::exception::Exception &ex) {
    // Add some context information and rethrow exception
    TAPE_THROW_CODE(ex.code(), ": " << ex.getMessage().str());
  }
  {
    const double connectDurationDouble =
      utils::timevalToDouble(connectDuration);
    castor::dlf::Param params[] = {
      castor::dlf::Param("tapebridgeTransId" , tapebridgeTransId       ),
      castor::dlf::Param("mountTransactionId", m_jobRequest.volReqId   ),
      castor::dlf::Param("volReqId"          , m_jobRequest.volReqId   ),
      castor::dlf::Param("TPVID"             , m_volume.vid()          ),
      castor::dlf::Param("driveUnit"         , m_jobRequest.driveUnit  ),
      castor::dlf::Param("dgn"               , m_jobRequest.dgn        ),
      castor::dlf::Param("clientHost"        , m_jobRequest.clientHost ),
      castor::dlf::Param("clientPort"        , m_jobRequest.clientPort ),
      castor::dlf::Param("clientType"        ,
        utils::volumeClientTypeToString(m_volume.clientType())),
      castor::dlf::Param("clientSock"        , clientSock.get()        ),
      castor::dlf::Param("connectDuration"   , connectDurationDouble   ),
      castor::dlf::Param("nbSuccessful"      ,
        listReport.successfulRecalls().size()),
      castor::dlf::Param("nbFailed"          ,
        listReport.failedRecalls().size())};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM,
      TAPEBRIDGE_SENT_FILERECALLREPORTLIST, params);
  }
}


//-----------------------------------------------------------------------------
// notifyClientEndOfSession
//-----------------------------------------------------------------------------
void castor::tape::tapebridge::BridgeProtocolEngine::notifyClientEndOfSession()
  throw() {

  try {
    // Inner try and catch to convert std::exception and unknown exceptions
    // (...) to castor::exception::Exception
    try {
      // An error received from the rtcpd daemon has priority over an exception
      // thrown whilst running the tape session

      // If an error was generated during the tape session
      if(!m_sessionErrors.empty()) {

        // Get the oldest and first error to have occurred
        const SessionError firstSessionError = m_sessionErrors.front();

        // Notify the client the session ended due to the error
        const uint64_t tapebridgeTransId =
          m_tapebridgeTransactionCounter.next();
        m_clientProxy.notifyEndOfFailedSession(tapebridgeTransId,
          firstSessionError.getErrorCode(),
          firstSessionError.getErrorMessage());
      } else {
        // Notify the client the session has ended with success
        const uint64_t tapebridgeTransId =
          m_tapebridgeTransactionCounter.next();
        m_clientProxy.notifyEndOfSession(tapebridgeTransId);
      }
    } catch(std::exception &se) {
      TAPE_THROW_EX(castor::exception::Internal,
        ": Caught a std::exception"
        ": what=" << se.what());
    } catch(castor::exception::Exception &ex) {
      // Add some context information and rethrow exception
      TAPE_THROW_CODE(ex.code(), ": " << ex.getMessage().str());
    } catch(...) {
      TAPE_THROW_EX(castor::exception::Internal,
        ": Caught an unknown exception");
    }
  } catch(castor::exception::Exception &ex) {
    castor::dlf::Param params[] = {
      castor::dlf::Param("mountTransactionId", m_jobRequest.volReqId  ),
      castor::dlf::Param("volReqId"          , m_jobRequest.volReqId  ),
      castor::dlf::Param("TPVID"             , m_volume.vid()         ),
      castor::dlf::Param("driveUnit"         , m_jobRequest.driveUnit ),
      castor::dlf::Param("dgn"               , m_jobRequest.dgn       ),
      castor::dlf::Param("clientHost"        , m_jobRequest.clientHost),
      castor::dlf::Param("clientPort"        , m_jobRequest.clientPort),
      castor::dlf::Param("clientType",
        utils::volumeClientTypeToString(m_volume.clientType())),
      castor::dlf::Param("errorCode"         , ex.code()              ),
      castor::dlf::Param("errorMessage"      , ex.getMessage().str()  )};
    castor::dlf::dlf_writep(m_cuuid, DLF_LVL_ERROR,
      TAPEBRIDGE_FAILED_TO_NOTIFY_CLIENT_END_OF_SESSION, params);
  }
}


//-----------------------------------------------------------------------------
// shuttingDownRtcpdSession
//-----------------------------------------------------------------------------
bool castor::tape::tapebridge::BridgeProtocolEngine::shuttingDownRtcpdSession()
  const {
  // The session with the rtcpd daemon should be shut down if there has been at
  // least one error detected during the tape session
  return !m_sessionErrors.empty();
}


//-----------------------------------------------------------------------------
// sessionWithRtcpdIsFinished
//-----------------------------------------------------------------------------
bool
  castor::tape::tapebridge::BridgeProtocolEngine::sessionWithRtcpdIsFinished()
  const throw() {
  return m_sessionWithRtcpdIsFinished;
}


//-----------------------------------------------------------------------------
// getNbDiskTapeIOControlConnections
//-----------------------------------------------------------------------------
int castor::tape::tapebridge::BridgeProtocolEngine::
  getNbDiskTapeIOControlConns() const {

  return m_sockCatalogue.getNbDiskTapeIOControlConns();
}


//-----------------------------------------------------------------------------
// getNbReceivedENDOF_REQs
//-----------------------------------------------------------------------------
uint32_t castor::tape::tapebridge::BridgeProtocolEngine::
  getNbReceivedENDOF_REQs() const throw() {
  return m_nbReceivedENDOF_REQs;
}


//-----------------------------------------------------------------------------
// generateMigrationTapeFileId
//-----------------------------------------------------------------------------
void castor::tape::tapebridge::BridgeProtocolEngine::
  generateMigrationTapeFileId(
  const uint64_t i, 
  char           (&dst)[CA_MAXPATHLEN+1])
  const throw(castor::exception::Exception) {

  const char hexDigits[16] = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
    'A', 'B', 'C', 'D', 'E', 'F'};
  char backwardsHexDigits[16];
  utils::setBytes(backwardsHexDigits, '\0');
  uint64_t exponent = 0;
  uint64_t quotient = i;
  int nbDigits = 0;

  for(exponent=0; exponent<16; exponent++) {
    backwardsHexDigits[exponent] = hexDigits[quotient % 16];
    nbDigits++;

    quotient = quotient / 16;

    if(quotient== 0) {
      break;
    }
  }

  for(int d=0; d<nbDigits;d++) {
    dst[d] = backwardsHexDigits[nbDigits-1-d];
  }
  dst[nbDigits] = '\0';
}
