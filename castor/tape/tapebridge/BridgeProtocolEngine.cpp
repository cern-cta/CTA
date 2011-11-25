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
#include "castor/tape/tapebridge/Constants.hpp"
#include "castor/tape/tapebridge/ClientTxRx.hpp"
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
#include <sys/time.h>


//-----------------------------------------------------------------------------
// constructor
//-----------------------------------------------------------------------------
castor::tape::tapebridge::BridgeProtocolEngine::BridgeProtocolEngine(
  const TapeFlushConfigParams         &tapeFlushConfigParams,
  const Cuuid_t                       &cuuid,
  const int                           listenSock,
  const int                           initialRtcpdSock,
  const legacymsg::RtcpJobRqstMsgBody &jobRequest,
  const tapegateway::Volume           &volume,
  const uint32_t                      nbFilesOnDestinationTape,
  utils::BoolFunctor                  &stoppingGracefully,
  Counter<uint64_t>                   &tapebridgeTransactionCounter)
  throw(castor::exception::Exception) :
  m_tapeFlushConfigParams(tapeFlushConfigParams),
  m_cuuid(cuuid),
  m_jobRequest(jobRequest),
  m_volume(volume),
  m_nextDestinationTapeFSeq(nbFilesOnDestinationTape + 1),
  m_stoppingGracefully(stoppingGracefully),
  m_tapebridgeTransactionCounter(tapebridgeTransactionCounter),
  m_nbReceivedENDOF_REQs(0),
  m_pendingTransferIds(MAXPENDINGTRANSFERS),
  m_pendingMigrationsStore(
    tapeFlushConfigParams.getMaxBytesBeforeFlush().value,
    tapeFlushConfigParams.getMaxFilesBeforeFlush().value),
  m_sessionWithRtcpdIsFinished(false) {

  // Store the listen socket and initial rtcpd connection in the socket
  // catalogue
  m_sockCatalogue.addListenSock(listenSock);
  m_sockCatalogue.addInitialRtcpdConn(initialRtcpdSock);

  // Build the map of rtcpd message body handlers
  m_rtcpdHandlers[createRtcpdHandlerKey(RTCOPY_MAGIC, RTCP_FILE_REQ   )] =
    &BridgeProtocolEngine::rtcpFileReqRtcpdCallback;
  m_rtcpdHandlers[createRtcpdHandlerKey(RTCOPY_MAGIC, RTCP_FILEERR_REQ)] =
    &BridgeProtocolEngine::rtcpFileErrReqRtcpdCallback;
  m_rtcpdHandlers[createRtcpdHandlerKey(RTCOPY_MAGIC, RTCP_TAPE_REQ   )] =
    &BridgeProtocolEngine::rtcpTapeReqRtcpdCallback;
  m_rtcpdHandlers[createRtcpdHandlerKey(RTCOPY_MAGIC, RTCP_TAPEERR_REQ)] =
    &BridgeProtocolEngine::rtcpTapeErrReqRtcpdCallback;
  m_rtcpdHandlers[createRtcpdHandlerKey(RTCOPY_MAGIC, RTCP_ENDOF_REQ  )] =
    &BridgeProtocolEngine::rtcpEndOfReqRtcpdCallback;
  m_rtcpdHandlers[createRtcpdHandlerKey(RTCOPY_MAGIC,
    TAPEBRIDGE_FLUSHEDTOTAPE)] =
    &BridgeProtocolEngine::tapeBridgeFlushedToTapeCallback;
  m_rtcpdHandlers[createRtcpdHandlerKey(RTCOPY_MAGIC_SHIFT, GIVE_OUTP )] =
    &BridgeProtocolEngine::giveOutpRtcpdCallback;

  // Build the map of client message handlers
  m_clientHandlers[OBJ_FilesToMigrateList] =
    &BridgeProtocolEngine::filesToMigrateListClientCallback;
  m_clientHandlers[OBJ_FilesToRecallList]  =
    &BridgeProtocolEngine::filesToRecallListClientCallback;
  m_clientHandlers[OBJ_NoMoreFiles]  =
    &BridgeProtocolEngine::noMoreFilesClientCallback;
  m_clientHandlers[OBJ_EndNotificationErrorReport]  =
    &BridgeProtocolEngine::endNotificationErrorReportClientCallback;
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

  try {
    unsigned short port = 0; // Client port
    unsigned long  ip   = 0; // Client IP
    char           hostName[net::HOSTNAMEBUFLEN];

    net::getPeerIpPort(connectedSock.get(), ip, port);
    net::getPeerHostName(connectedSock.get(), hostName);

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
      castor::dlf::Param("IP"                , castor::dlf::IPAddress(ip)  ),
      castor::dlf::Param("Port"              , port                        ),
      castor::dlf::Param("HostName"          , hostName                    ),
      castor::dlf::Param("socketFd"          , connectedSock.get()         ),
      castor::dlf::Param("nbDiskTapeConns"   ,
        m_sockCatalogue.getNbDiskTapeIOControlConns())};
    castor::dlf::dlf_writep(m_cuuid, DLF_LVL_SYSTEM, TAPEBRIDGE_RTCPD_CALLBACK,
      params);
  } catch(castor::exception::Exception &ex) {
    TAPE_THROW_CODE(ECANCELED,
      ": Failed to get IP, port and host name"
      ": volReqId=" << m_jobRequest.volReqId <<
      ": nbDiskTapeConns=" << m_sockCatalogue.getNbDiskTapeIOControlConns());
  }

  return connectedSock.release();
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
      processSocks();
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

    // Gather the error information into an RtcpdError object
    RtcpdError rtcpdError;
    rtcpdError.setErrorCode(ex.code());
    rtcpdError.setErrorMessage(ex.getMessage().str());
    rtcpdError.setErrorScope(RtcpdError::SESSION_SCOPE);

    // Push the error onto the back of the list of errors generated during the
    // sesion with the rtcpd daemon
    m_rtcpdErrors.push_back(rtcpdError);
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
// processSocks
//-----------------------------------------------------------------------------
void castor::tape::tapebridge::BridgeProtocolEngine::processSocks()
  throw(castor::exception::Exception) {

  unsigned int nbOneSecondTimeouts = 0;
  int          maxFd               = 0;
  fd_set       readFdSet;
  timeval      timeout;

  // Select loop
  bool continueProcessingSocks = true;
  while(continueProcessingSocks) {
    // Throw an exception if the daemon is stopping gracefully
    if(m_stoppingGracefully()) {
      castor::exception::Exception ex(ECANCELED);

      ex.getMessage() << "Stopping gracefully";
      throw(ex);
    }

    m_sockCatalogue.buildReadFdSet(readFdSet, maxFd);

    timeout.tv_sec  = 1; // 1 second
    timeout.tv_usec = 0;

    // Wait for up to 1 second to see if any read file descriptors are ready
    const int selectRc = select(maxFd + 1, &readFdSet, NULL, NULL, &timeout);
    const int selectErrno = errno;
//  {
//    castor::dlf::Param params[] = {
//      castor::dlf::Param("mountTransactionId", m_jobRequest.volReqId  ),
//      castor::dlf::Param("volReqId"          , m_jobRequest.volReqId  ),
//      castor::dlf::Param("TPVID"             , m_volume.vid()         ),
//      castor::dlf::Param("driveUnit"         , m_jobRequest.driveUnit ),
//      castor::dlf::Param("dgn"               , m_jobRequest.dgn       ),
//      castor::dlf::Param("clientHost"        , m_jobRequest.clientHost),
//      castor::dlf::Param("clientPort"        , m_jobRequest.clientPort),
//      castor::dlf::Param("clientType",
//    utils::volumeClientTypeToString(m_volume.clientType())),
//      castor::dlf::Param("selectRc"          , selectRc               )};
//    castor::dlf::dlf_writep(m_cuuid, DLF_LVL_SYSTEM,
//      TAPEBRIDGE_RETURNED_FROM_MAIN_SELECT, params);
//  }

    switch(selectRc) {
    case 0: // Select timed out

      nbOneSecondTimeouts++;

      // Ping rtcpd if the session with rtcpd is not finished and the
      // RTCPDPINGTIMEOUT has been reached
      if(!m_sessionWithRtcpdIsFinished &&
        (nbOneSecondTimeouts % RTCPDPINGTIMEOUT == 0)) {
        RtcpTxRx::pingRtcpd(m_cuuid, m_jobRequest.volReqId,
        m_sockCatalogue.getInitialRtcpdConn(), RTCPDNETRWTIMEOUT);
      }

      // Ping the client if the ping timeout has been reached and the client is
      // readtp, writetp or dumptp
      if(
        (nbOneSecondTimeouts % CLIENTPINGTIMEOUT == 0) &&
        (m_volume.clientType() == tapegateway::READ_TP  ||
         m_volume.clientType() == tapegateway::WRITE_TP ||
         m_volume.clientType() == tapegateway::DUMP_TP)) {

        try {
          ClientTxRx::ping(m_cuuid, m_jobRequest.volReqId,
            m_tapebridgeTransactionCounter.next(), m_jobRequest.clientHost,
            m_jobRequest.clientPort);
        } catch(castor::exception::Exception &ex) {
          castor::exception::Exception ex2(ex.code());

          ex2.getMessage() <<
            "Failed to ping client"
            ": clientType=" <<
            utils::volumeClientTypeToString(m_volume.clientType()) <<
            ": " << ex.getMessage().str();

          throw(ex2);
        }
      }

      break;

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

    // Throw an exception if a timeout has occured
    m_sockCatalogue.checkForTimeout();

    // Determine whether or not continue processing sockets
    continueProcessingSocks = !(
      m_sessionWithRtcpdIsFinished &&
      m_flushedBatches.empty() &&
      !m_sockCatalogue.clientMigrationReportSockIsSet());

  } // while(continueProcessingSocks)
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
  case BridgeSocketCatalogue::CLIENT:
    processPendingClientSocket(pendingSock);
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

  // Throw an exception if connection is not from localhost
  checkPeerIsLocalhost(acceptedConnection);
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
    net::readBytesFromCloseable(rtcpdClosedConnection, pendingSock,
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
    bool peerClosed = false;
    LegacyTxRx::receiveMsgHeaderFromCloseable(m_cuuid, peerClosed,
      m_jobRequest.volReqId, pendingSock, RTCPDNETRWTIMEOUT, header);

    // If the peer closed its side of the connection, then close this side
    // of the connection and return in order to continue the RTCOPY session
    if(peerClosed) {
      close(m_sockCatalogue.releaseRtcpdDiskTapeIOControlConn(pendingSock));

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

  // Process the rtcpd request
  {
    bool receivedENDOF_REQ = false;
    processRtcpdRequest(header, pendingSock, receivedENDOF_REQ);

    // If the message processed was not an ENDOF_REQ, then return in order to
    // continue the RTCOPY session
    if(!receivedENDOF_REQ) {
      return; // Continue the RTCOPY session
    }
  }

  // If this line has been reached, then the processed rtcpd message was
  // an ENDOF_REQ
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
      castor::dlf::Param("socketFd"            , pendingSock            ),
      castor::dlf::Param("nbReceivedENDOF_REQs", m_nbReceivedENDOF_REQs )};
    castor::dlf::dlf_writep(m_cuuid, DLF_LVL_SYSTEM,
      TAPEBRIDGE_RECEIVED_RTCP_ENDOF_REQ, params);
  }

  const int closeRc = close(m_sockCatalogue.releaseRtcpdDiskTapeIOControlConn(
    pendingSock));
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
      castor::dlf::Param("socketFd"          , pendingSock            ),
      castor::dlf::Param("closeRc"           , closeRc                ),
      castor::dlf::Param("nbIOControlConns"  ,
        nbDiskTapeIOControlConnsAfterClose)};
    castor::dlf::dlf_writep(m_cuuid, DLF_LVL_SYSTEM,
      TAPEBRIDGE_CLOSED_RTCPD_DISK_TAPE_CONNECTION_DUE_TO_ENDOF_REQ, params);
  }

  // If only the initial callback connection is open, then record the fact that
  // the session with the rtcpd daemon is over and the rtcpd daemon is awaiting
  // the final RTCP_ENDOF_REQ message from the tapebridged daemon
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
      castor::dlf::Param("socketFd"            , pendingSock           ),
      castor::dlf::Param("nbReceivedENDOF_REQs", m_nbReceivedENDOF_REQs)};
    castor::dlf::dlf_writep(m_cuuid, DLF_LVL_SYSTEM,
      TAPEBRIDGE_ONLY_INITIAL_RTCPD_CONNECTION_OPEN, params);
  }
}


//------------------------------------------------------------------------------
// processPendingClientSocket
//------------------------------------------------------------------------------
void castor::tape::tapebridge::BridgeProtocolEngine::
  processPendingClientSocket(const int pendingSock)
  throw(castor::exception::Exception) {

  // Check function arguments
  if(pendingSock < 0) {
    TAPE_THROW_EX(castor::exception::InvalidArgument,
      ": Invalid method argument"
      ": pendingSock is an invalid socket descriptor"
      ": value=" << pendingSock);
  }

  // Get information about the rtcpd disk/tape IO control-connection that
  // is waiting for the reply from the pending client-connection
  int            rtcpdSock          = 0;
  uint32_t       rtcpdReqMagic      = 0;
  uint32_t       rtcpdReqType       = 0;
  const char     *rtcpdReqTapePath  = NULL;
  uint64_t       tapebridgeTransId  = 0;
  struct timeval clientReqTimeStamp = {0, 0};
  m_sockCatalogue.getRtcpdConn(pendingSock, rtcpdSock, rtcpdReqMagic,
    rtcpdReqType, rtcpdReqTapePath, tapebridgeTransId, clientReqTimeStamp);

  // Release the client-connection from the catalogue
  m_sockCatalogue.releaseClientConn(rtcpdSock, pendingSock);

  // Get the current time
  struct timeval now = {0, 0};
  gettimeofday(&now, NULL);

  // Calculate the number of remaining seconds before timing out the
  // client-connection
  int remainingClientTimeout = CLIENTNETRWTIMEOUT -
    (now.tv_sec - clientReqTimeStamp.tv_sec);
  if(remainingClientTimeout <= 0) {
    remainingClientTimeout = 1;
  }

  std::auto_ptr<IObject> obj(ClientTxRx::receiveReplyAndClose(pendingSock,
    remainingClientTimeout));

  // Drop the client reply if the associated rtcpd-connection has been closed
  if(-1 == rtcpdSock) {
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

  // Find the message type's corresponding handler
  ClientCallbackMap::iterator itor = m_clientHandlers.find(obj->type());
  if(itor == m_clientHandlers.end()) {
    TAPE_THROW_CODE(EBADMSG,
      ": Unknown client message object type"
      ": object type=0x"   << std::hex << obj->type() << std::dec);
  }
  const ClientMsgCallback handler = itor->second;

  // Invoke the handler
  try {
    (this->*handler)(pendingSock, obj.get(), rtcpdSock, rtcpdReqMagic,
      rtcpdReqType, rtcpdReqTapePath, tapebridgeTransId,
      clientReqTimeStamp);
  } catch(castor::exception::Exception &ex) {
    TAPE_THROW_CODE(ECANCELED,
      ": Client message handler failed"
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
    ClientTxRx::receiveNotificationReplyAndClose(m_jobRequest.volReqId,
      tapebridgeTransId, closedClientSock);
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

    rtcpdSessionStarted = false;
  }

  return rtcpdSessionStarted;
}


//-----------------------------------------------------------------------------
// startMigrationSession
//-----------------------------------------------------------------------------
bool castor::tape::tapebridge::BridgeProtocolEngine::startMigrationSession()
  throw(castor::exception::Exception) {

  // Send the request for the first file to migrate to the client
  timeval connectDuration = {0, 0};
  const uint64_t tapebridgeTransId =
    m_tapebridgeTransactionCounter.next();
  const uint64_t maxFiles = 1;
  const uint64_t maxBytes = 1;
  utils::SmartFd clientSock(ClientTxRx::sendFilesToMigrateListRequest(
    m_jobRequest.volReqId, tapebridgeTransId, m_jobRequest.clientHost,
    m_jobRequest.clientPort, maxFiles, maxBytes, connectDuration));
  {
    const double connectDurationDouble =
      utils::timevalToDouble(connectDuration);

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
      castor::dlf::Param("clientSock"        , clientSock.get()       ),
      castor::dlf::Param("connectDuration"   , connectDurationDouble  )};
    castor::dlf::dlf_writep(m_cuuid, DLF_LVL_SYSTEM,
      TAPEBRIDGE_SENT_FILESTOMIGRATELISTREQUEST, params);
  }

  // Receive the reply
  const int closedClientSock = clientSock.release();
  std::auto_ptr<tapegateway::FilesToMigrateList> filesFromClient(
    ClientTxRx::receiveFilesToMigrateListRequestReplyAndClose(
      m_jobRequest.volReqId, tapebridgeTransId, closedClientSock));

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
      ": The FilesToMigrateList message for the first file to migrate should"
      " only contain one file"
      ": nbFiles=" << filesFromClient->filesToMigrate().size());
  }
  const tapegateway::FileToMigrateStruct *const firstFileToMigrate =
    filesFromClient->filesToMigrate()[0];

  // If the client is the tape-gateway then cross-check and update the next
  // expected tape-file sequence-number
  if(tapegateway::TAPE_GATEWAY == m_volume.clientType()) {

    if(firstFileToMigrate->fseq() != m_nextDestinationTapeFSeq) {
      castor::exception::Exception ex(ECANCELED);

      ex.getMessage() <<
        "Invalid tape file sequence number from client"
        ": expected=" << (m_nextDestinationTapeFSeq) <<
        ": actual=" << firstFileToMigrate->fseq();

      throw(ex);
    }

    m_nextDestinationTapeFSeq++;
  }

  // Remember the file transaction ID and get its unique index to be passed
  // to rtcpd through the "rtcpFileRequest.disk_fseq" message field
  const uint32_t diskFSeq = m_pendingTransferIds.insert(
    firstFileToMigrate->fileTransactionId());

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
    utils::toHex((uint64_t)firstFileToMigrate->fileid(),
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

  char tapePath[CA_MAXPATHLEN+1];
  utils::setBytes(tapePath, '\0');

  // Ask rtcpd to request more work
  RtcpTxRx::askRtcpdToRequestMoreWork(m_cuuid, m_jobRequest.volReqId,
    tapePath, m_sockCatalogue.getInitialRtcpdConn(), RTCPDNETRWTIMEOUT,
    WRITE_ENABLE);

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

  char tapePath[CA_MAXPATHLEN+1];
  utils::setBytes(tapePath, '\0');

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
  RtcpTxRx::askRtcpdToRequestMoreWork(m_cuuid, m_jobRequest.volReqId,
    tapePath, m_sockCatalogue.getInitialRtcpdConn(), RTCPDNETRWTIMEOUT,
    WRITE_DISABLE);

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
    ClientTxRx::getDumpParameters(m_cuuid, m_jobRequest.volReqId,
    m_tapebridgeTransactionCounter.next(), m_jobRequest.clientHost,
    m_jobRequest.clientPort));

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
  }

  // Close the initial rtcpd-connection
  {
    const int closeRc = close(m_sockCatalogue.releaseInitialRtcpdConn());

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
  const legacymsg::MessageHeader &header, const int socketFd, 
  bool &receivedENDOF_REQ) throw(castor::exception::Exception) {

  {
    char magicHex[2 + 17]; // 0 + x + FFFFFFFFFFFFFFFF + '\0'
    magicHex[0] = '0';
    magicHex[1] = 'x';
    utils::toHex(header.magic, &(magicHex[2]), 17);

    const char *magicName = utils::magicToString(header.magic);

    char reqTypeHex[2 + 17]; // 0 + x + FFFFFFFFFFFFFFFF + '\0'
    reqTypeHex[0] = '0';
    reqTypeHex[1] = 'x';
    utils::toHex(header.reqType, &(reqTypeHex[2]), 17);

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
      castor::dlf::Param("magic"             , magicHex               ),
      castor::dlf::Param("magicName"         , magicName              ),
      castor::dlf::Param("reqType"           , reqTypeHex             ),
      castor::dlf::Param("reqTypeName"       , reqTypeName            ),
      castor::dlf::Param("len"               , header.lenOrStatus     )};
    castor::dlf::dlf_writep(m_cuuid, DLF_LVL_DEBUG,
      TAPEBRIDGE_PROCESSING_TAPE_DISK_RQST, params);
  }

  // Find the message type's corresponding handler
  RtcpdCallbackMap::iterator itor = m_rtcpdHandlers.find(
    createRtcpdHandlerKey(header.magic, header.reqType));
  if(itor == m_rtcpdHandlers.end()) {
    TAPE_THROW_CODE(EBADMSG,
      ": Unknown magic and request type combination"
      ": magic=0x"   << std::hex << header.magic <<
      ": reqType=0x" << header.reqType << std::dec);
  }
  const RtcpdMsgBodyCallback handler = itor->second;

  // Invoke the handler
  return (this->*handler)(header, socketFd, receivedENDOF_REQ);
}


//-----------------------------------------------------------------------------
// rtcpFileReqRtcpdCallback
//-----------------------------------------------------------------------------
void castor::tape::tapebridge::BridgeProtocolEngine::rtcpFileReqRtcpdCallback(
  const legacymsg::MessageHeader &header, const int socketFd, 
  bool &) throw(castor::exception::Exception) {

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

  processRtcpFileErrReq(header, bodyErr, socketFd);
}


//-----------------------------------------------------------------------------
// rtcpFileErrReqRtcpdCallback
//-----------------------------------------------------------------------------
void
  castor::tape::tapebridge::BridgeProtocolEngine::rtcpFileErrReqRtcpdCallback(
  const legacymsg::MessageHeader &header,
  const int                      socketFd, 
  bool                           &)
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

  processRtcpFileErrReq(header, body, socketFd);
}


//-----------------------------------------------------------------------------
// processRtcpFileErrReq
//-----------------------------------------------------------------------------
void castor::tape::tapebridge::BridgeProtocolEngine::processRtcpFileErrReq(
  const legacymsg::MessageHeader &header,
  legacymsg::RtcpFileRqstErrMsgBody &body,
  const int                         rtcpdSock)
  throw(castor::exception::Exception) {

  // Call the appropriate helper method to process the message
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
// processRtcpFileErrReqDump
//-----------------------------------------------------------------------------
void castor::tape::tapebridge::BridgeProtocolEngine::processRtcpFileErrReqDump(
  const legacymsg::MessageHeader &header, const int rtcpdSock)
  throw(castor::exception::Exception) {
  // Simply reply with a positive acknowledgement
  legacymsg::MessageHeader ackMsg;
  ackMsg.magic       = header.magic;
  ackMsg.reqType     = header.reqType;
  ackMsg.lenOrStatus = 0;
  LegacyTxRx::sendMsgHeader(m_cuuid, m_jobRequest.volReqId, rtcpdSock,
    RTCPDNETRWTIMEOUT, ackMsg);
}


//-----------------------------------------------------------------------------
// processRtcpWaiting
//-----------------------------------------------------------------------------
void castor::tape::tapebridge::BridgeProtocolEngine::processRtcpWaiting(
  const legacymsg::MessageHeader &header,
  legacymsg::RtcpFileRqstErrMsgBody &body, const int rtcpdSock)
  throw(castor::exception::Exception) {

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

  // Send an acknowledge to rtcpd
  legacymsg::MessageHeader ackMsg;
  ackMsg.magic       = header.magic;
  ackMsg.reqType     = header.reqType;
  ackMsg.lenOrStatus = 0;
  LegacyTxRx::sendMsgHeader(m_cuuid, m_jobRequest.volReqId, rtcpdSock,
    RTCPDNETRWTIMEOUT, ackMsg);

  // If the message contains an error
  if(0 != body.rqst.cprc) {
    // Gather the error information into an RtcpdError object
    RtcpdError rtcpdError;
    rtcpdError.setErrorCode(errorCode);
    rtcpdError.setErrorMessage(errorMessage);

    // If this is a file-specific error
    if(0 < body.rqst.tapeFseq) {
      rtcpdError.setErrorScope(RtcpdError::FILE_SCOPE);
      rtcpdError.setFileTransactionId(
        m_pendingTransferIds.get(body.rqst.diskFseq));
      rtcpdError.setNsHost(body.rqst.segAttr.nameServerHostName);
      rtcpdError.setNsFileId(body.rqst.segAttr.castorFileId);
      rtcpdError.setTapeFSeq(body.rqst.tapeFseq);

    // Else this is a session error - don't need to fill in file specifics
    } else {
      rtcpdError.setErrorScope(RtcpdError::SESSION_SCOPE);
    }

    // Push the error onto the back of the list of errors generated during the
    // session with the rtcpd daemon
    m_rtcpdErrors.push_back(rtcpdError);
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
    // Send a FilesToMigrateListRequest to the client
    timeval connectDuration = {0, 0};
    const uint64_t tapebridgeTransId = m_tapebridgeTransactionCounter.next();
    const uint64_t maxFiles = 1;
    const uint64_t maxBytes = 1;
    utils::SmartFd clientSock(ClientTxRx::sendFilesToMigrateListRequest(
      m_jobRequest.volReqId, tapebridgeTransId, m_jobRequest.clientHost,
      m_jobRequest.clientPort, maxFiles, maxBytes, connectDuration));
    {
      const double connectDurationDouble =
        utils::timevalToDouble(connectDuration);
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
        castor::dlf::Param("clientSock"        , clientSock.get()       ),
        castor::dlf::Param("connectDuration"   , connectDurationDouble  )};
      castor::dlf::dlf_writep(m_cuuid, DLF_LVL_SYSTEM,
        TAPEBRIDGE_SENT_FILESTOMIGRATELISTREQUEST, params);
    }

    // Add the client connection to the socket catalogue so that the
    // association with the rtcpd connection is made
    m_sockCatalogue.addClientConn(rtcpdSock, header.magic, header.reqType,
      body.rqst.tapePath, clientSock.release(), tapebridgeTransId);

  // Else recalling
  } else {

    // Send a FilesToRecallListRequest to the client
    timeval connectDuration = {0, 0};
    const uint64_t tapebridgeTransId = m_tapebridgeTransactionCounter.next();
    const uint64_t maxFiles = 1;
    const uint64_t maxBytes = 1;
    utils::SmartFd clientSock(ClientTxRx::sendFilesToRecallListRequest(
      m_jobRequest.volReqId, tapebridgeTransId, m_jobRequest.clientHost,
      m_jobRequest.clientPort, maxFiles, maxBytes, connectDuration));
    {
      const double connectDurationDouble =
        utils::timevalToDouble(connectDuration);
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
        castor::dlf::Param("clientSock"        , clientSock.get()       ),
        castor::dlf::Param("connectDuration"   , connectDurationDouble  )};
      castor::dlf::dlf_writep(m_cuuid, DLF_LVL_SYSTEM,
        TAPEBRIDGE_SENT_FILESTORECALLLISTREQUEST, params);
    }

    // Add the client connection to the socket catalogue so that the
    // association with the rtcpd connection is made
    m_sockCatalogue.addClientConn(rtcpdSock, header.magic, header.reqType,
      body.rqst.tapePath, clientSock.release(), tapebridgeTransId);
  }
}


//-----------------------------------------------------------------------------
// processRtcpFilePositionedRequest
//-----------------------------------------------------------------------------
void castor::tape::tapebridge::BridgeProtocolEngine::
  processRtcpFilePositionedRequest(const legacymsg::MessageHeader &header,
  legacymsg::RtcpFileRqstErrMsgBody &body, const int rtcpdSock)
  throw(castor::exception::Exception) {

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

  // Send an acknowledgment to rtcpd
  legacymsg::MessageHeader ackMsg;
  ackMsg.magic       = header.magic;
  ackMsg.reqType     = header.reqType;
  ackMsg.lenOrStatus = 0;
  LegacyTxRx::sendMsgHeader(m_cuuid, m_jobRequest.volReqId, rtcpdSock,
    RTCPDNETRWTIMEOUT, ackMsg);

  // If the message contains an error
  if(0 != body.rqst.cprc) {
    // Gather the error information into an RtcpdError object
    RtcpdError rtcpdError;
    rtcpdError.setErrorCode(errorCode);
    rtcpdError.setErrorMessage(errorMessage);

    // If this is a file-specific error
    if(0 < body.rqst.tapeFseq) {
      rtcpdError.setErrorScope(RtcpdError::FILE_SCOPE);
      rtcpdError.setFileTransactionId(
        m_pendingTransferIds.get(body.rqst.diskFseq));
      rtcpdError.setNsHost(body.rqst.segAttr.nameServerHostName);
      rtcpdError.setNsFileId(body.rqst.segAttr.castorFileId);
      rtcpdError.setTapeFSeq(body.rqst.tapeFseq);

    // Else this is a session error - don't need to fill in file specifics
    } else {
      rtcpdError.setErrorScope(RtcpdError::SESSION_SCOPE);
    }

    // Push the error onto the back of the list of errors received from the
    // rtcpd daemon
    m_rtcpdErrors.push_back(rtcpdError);
  }
}


//-----------------------------------------------------------------------------
// processRtcpFileFinishedRequest
//-----------------------------------------------------------------------------
void castor::tape::tapebridge::BridgeProtocolEngine::
  processRtcpFileFinishedRequest(const legacymsg::MessageHeader &header,
  legacymsg::RtcpFileRqstErrMsgBody &body, const int rtcpdSock)
  throw(castor::exception::Exception) {

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

  // Send an acknowledge to rtcpd
  legacymsg::MessageHeader ackMsg;
  ackMsg.magic       = header.magic;
  ackMsg.reqType     = header.reqType;
  ackMsg.lenOrStatus = 0;
  LegacyTxRx::sendMsgHeader(m_cuuid, m_jobRequest.volReqId, rtcpdSock,
    RTCPDNETRWTIMEOUT, ackMsg);

  // If the message contains an error
  if(0 != body.rqst.cprc) {
    // Gather the error information into an RtcpdError object
    RtcpdError rtcpdError;
    rtcpdError.setErrorCode(errorCode);
    rtcpdError.setErrorMessage(errorMessage);

    // If this is a file-specific error
    if(0 < body.rqst.tapeFseq) {
      rtcpdError.setErrorScope(RtcpdError::FILE_SCOPE);
      rtcpdError.setFileTransactionId(
        m_pendingTransferIds.get(body.rqst.diskFseq));
      rtcpdError.setNsHost(body.rqst.segAttr.nameServerHostName);
      rtcpdError.setNsFileId(body.rqst.segAttr.castorFileId);
      rtcpdError.setTapeFSeq(body.rqst.tapeFseq);

    // Else this is a session error - don't need to fill in file specifics
    } else {
      rtcpdError.setErrorScope(RtcpdError::SESSION_SCOPE);
    }

    // Push the error onto the back of the list of errors received from the
    // rtcpd daemon
    m_rtcpdErrors.push_back(rtcpdError);
  }

  // Get and remove from the pending transfer ids, the file transaction ID
  // that was sent by the client (the tapegatewayd daemon or the readtp or
  // writetp command-line tool)
  const uint64_t fileTransactonId =
    m_pendingTransferIds.remove(body.rqst.diskFseq);

  // If there was no error
  if(0 == body.rqst.cprc) {
    try {
      processSuccessfullRtcpFileFinishedRequest(fileTransactonId, body);
    } catch(castor::exception::Exception &ex) {
      // Gather the error information into an RtcpdError object
      RtcpdError rtcpdError;
      rtcpdError.setErrorCode(ex.code());
      rtcpdError.setErrorMessage(ex.getMessage().str());

      // If this is a file-specific error
      if(0 < body.rqst.tapeFseq) {
        rtcpdError.setErrorScope(RtcpdError::FILE_SCOPE);
        rtcpdError.setFileTransactionId(
          m_pendingTransferIds.get(body.rqst.diskFseq));
        rtcpdError.setNsHost(body.rqst.segAttr.nameServerHostName);
        rtcpdError.setNsFileId(body.rqst.segAttr.castorFileId);
        rtcpdError.setTapeFSeq(body.rqst.tapeFseq);

      // Else this is a session error - don't need to fill in file specifics
      } else {
        rtcpdError.setErrorScope(RtcpdError::SESSION_SCOPE);
      }

      // Push the error onto the back of the list of errors received from the
      // rtcpd daemon
      m_rtcpdErrors.push_back(rtcpdError);

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
  const uint64_t fileTransactonId,
  legacymsg::RtcpFileRqstErrMsgBody &body)
  throw(castor::exception::Exception) {

  // If migrating
  if(m_volume.mode() == tapegateway::WRITE) {

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

  // Else recall (READ or DUMP)
  } else {
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
    utils::SmartFd clientSock(ClientTxRx::sendMessage(m_jobRequest.clientHost,
      m_jobRequest.clientPort, CLIENTNETRWTIMEOUT, report, connectDuration));
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
      ClientTxRx::receiveNotificationReplyAndClose(m_jobRequest.volReqId,
        tapebridgeTransId, closedClientSock);
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
}


//-----------------------------------------------------------------------------
// rtcpTapeReqRtcpdCallback
//-----------------------------------------------------------------------------
void castor::tape::tapebridge::BridgeProtocolEngine::rtcpTapeReqRtcpdCallback(
  const legacymsg::MessageHeader &header, const int socketFd, 
  bool &receivedENDOF_REQ) throw(castor::exception::Exception) {

  legacymsg::RtcpTapeRqstMsgBody body;

  RtcpTxRx::receiveMsgBody(m_cuuid, m_jobRequest.volReqId, socketFd,
    RTCPDNETRWTIMEOUT, header, body);

  return(processRtcpTape(header, body, socketFd, receivedENDOF_REQ));
}


//-----------------------------------------------------------------------------
// rtcpTapeErrReqRtcpdCallback
//-----------------------------------------------------------------------------
void
  castor::tape::tapebridge::BridgeProtocolEngine::rtcpTapeErrReqRtcpdCallback(
  const legacymsg::MessageHeader &header,
  const int                      socketFd, 
  bool                           &receivedENDOF_REQ)
  throw(castor::exception::Exception) {

  legacymsg::RtcpTapeRqstErrMsgBody body;

  RtcpTxRx::receiveMsgBody(m_cuuid, m_jobRequest.volReqId, socketFd,
    RTCPDNETRWTIMEOUT, header, body);

  if(body.err.errorCode != 0) {
    TAPE_THROW_CODE(body.err.errorCode,
      ": Received an error from rtcpd"
      ": errorCode=" << body.err.errorCode <<
      ": errorMsg=" << body.err.errorMsg <<
      ": sstrerror=" << sstrerror(body.err.errorCode));
  }

  return(processRtcpTape(header, body, socketFd, receivedENDOF_REQ));
}


//-----------------------------------------------------------------------------
// processRtcpTape
//-----------------------------------------------------------------------------
void castor::tape::tapebridge::BridgeProtocolEngine::processRtcpTape(
  const legacymsg::MessageHeader&,
  legacymsg::RtcpTapeRqstMsgBody&,
  const int socketFd,
  bool&)
  throw(castor::exception::Exception) {

  // Acknowledge tape request
  legacymsg::MessageHeader ackMsg;
  ackMsg.magic       = RTCOPY_MAGIC;
  ackMsg.reqType     = RTCP_TAPE_REQ;
  ackMsg.lenOrStatus = 0;
  LegacyTxRx::sendMsgHeader(m_cuuid, m_jobRequest.volReqId, socketFd,
    RTCPDNETRWTIMEOUT, ackMsg);
}


//-----------------------------------------------------------------------------
// rtcpEndOfReqRtcpdCallback
//-----------------------------------------------------------------------------
void castor::tape::tapebridge::BridgeProtocolEngine::rtcpEndOfReqRtcpdCallback(
  const legacymsg::MessageHeader&,
  const int                      socketFd, 
  bool                           &receivedENDOF_REQ)
  throw(castor::exception::Exception) {

  receivedENDOF_REQ = true;

  // Acknowledge RTCP_ENDOF_REQ message
  legacymsg::MessageHeader ackMsg;
  ackMsg.magic       = RTCOPY_MAGIC;
  ackMsg.reqType     = RTCP_ENDOF_REQ;
  ackMsg.lenOrStatus = 0;
  LegacyTxRx::sendMsgHeader(m_cuuid, m_jobRequest.volReqId, socketFd,
    RTCPDNETRWTIMEOUT, ackMsg);
}


//-----------------------------------------------------------------------------
// tapeBridgeFlushedToTapeCallback
//-----------------------------------------------------------------------------
void castor::tape::tapebridge::BridgeProtocolEngine::
  tapeBridgeFlushedToTapeCallback(
  const legacymsg::MessageHeader &header,
  const int                      socketFd,
  bool                           &)
  throw(castor::exception::Exception) {

  const char *const task = "process TAPEBRIDGE_FLUSHEDTOTAPE message";

  tapeBridgeFlushedToTapeMsgBody_t body;

  RtcpTxRx::receiveMsgBody(m_cuuid, m_jobRequest.volReqId, socketFd,
    RTCPDNETRWTIMEOUT, header, body);

  // Acknowledge TAPEBRIDGE_FLUSHEDTOTAPE message
  legacymsg::MessageHeader ackMsg;
  ackMsg.magic       = RTCOPY_MAGIC;
  ackMsg.reqType     = TAPEBRIDGE_FLUSHEDTOTAPE;
  ackMsg.lenOrStatus = 0;
  LegacyTxRx::sendMsgHeader(m_cuuid, m_jobRequest.volReqId, socketFd,
    RTCPDNETRWTIMEOUT, ackMsg);

  {
    castor::dlf::Param params[] = {
      castor::dlf::Param("mountTransactionId", m_jobRequest.volReqId  ),
      castor::dlf::Param("volReqId"          , m_jobRequest.volReqId  ),
      castor::dlf::Param("TPVID"             , m_volume.vid()         ),
      castor::dlf::Param("driveUnit"         , m_jobRequest.driveUnit ),
      castor::dlf::Param("dgn"               , m_jobRequest.dgn       ),
      castor::dlf::Param("clientHost"        , m_jobRequest.clientHost),
      castor::dlf::Param("clientPort"        , m_jobRequest.clientPort),
      castor::dlf::Param("clientType"        ,
        utils::volumeClientTypeToString(m_volume.clientType())),
      castor::dlf::Param("volReqId"          , body.volReqId          ),
      castor::dlf::Param("tapeFseq"          , body.tapeFseq          )};
    castor::dlf::dlf_writep(m_cuuid, DLF_LVL_SYSTEM,
      TAPEBRIDGE_RECEIVED_TAPEBRIDGE_FLUSHEDTOTAPE, params);
  }

  // If there is a volume request ID mismatch
  if(m_jobRequest.volReqId != body.volReqId) {
    // Gather the error information into an RtcpdError object
    std::ostringstream oss;
    oss <<
      "Failed to " << task <<
      ": message contains an unexpected volReqId"
      ": expected=" << m_jobRequest.volReqId <<
      ": actual=" << body.volReqId;
    RtcpdError rtcpdError;
    rtcpdError.setErrorCode(EBADMSG);
    rtcpdError.setErrorMessage(oss.str());
    rtcpdError.setErrorScope(RtcpdError::SESSION_SCOPE);

    // Push the error onto the back of the list of errors generated during the
    // session with the rtcpd daemon
    m_rtcpdErrors.push_back(rtcpdError);

    // There is nothing else to do
    return;
  }

  // If the tape-file sequence-number is invalid
  if(0 == body.tapeFseq) {
    // Gather the error information into an RtcpdError object
    std::ostringstream oss;
    oss <<
      "Failed to " << task <<
      ": message contains an fseq with the illegal value of 0";
    RtcpdError rtcpdError;
    rtcpdError.setErrorCode(EBADMSG);
    rtcpdError.setErrorMessage(oss.str());
    rtcpdError.setErrorScope(RtcpdError::SESSION_SCOPE);

    // Push the error onto the back of the list of errors generated during the
    // session with the rtcpd daemon
    m_rtcpdErrors.push_back(rtcpdError);

    // There is nothing else to do
    return;
  }

  // It is an error to receive a TAPEBRIDGE_FLUSHEDTOTAPE message if the
  // current tape mount is anything other than WRITE
  if(tapegateway::WRITE != m_volume.mode()) {
    // Gather the error information into an RtcpdError object
    std::ostringstream oss;
    oss <<
      "Failed to " << task <<
      ": Tape is not mounted for write";
    RtcpdError rtcpdError;
    rtcpdError.setErrorCode(EINVAL);
    rtcpdError.setErrorMessage(oss.str());
    rtcpdError.setErrorScope(RtcpdError::SESSION_SCOPE);

    // Push the error onto the back of the list of errors generated during the
    // session with the rtcpd daemon
    m_rtcpdErrors.push_back(rtcpdError);

    // There is nothing else to do
    return;
  }

  try {
    // Get and remove the batch of now flushed file-migrations that have a
    // tape-file sequence-number less than or equal to that of the flush
    const FileWrittenNotificationList migrations =
      m_pendingMigrationsStore.dataFlushedToTape(body.tapeFseq);

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
    // Gather the error information into an RtcpdError object
    std::ostringstream oss;
    oss <<
      "Failed to " << task <<
      ": " << ex.getMessage().str();
    RtcpdError rtcpdError;
    rtcpdError.setErrorCode(ECANCELED);
    rtcpdError.setErrorMessage(oss.str());
    rtcpdError.setErrorScope(RtcpdError::SESSION_SCOPE);

    // Push the error onto the back of the list of errors generated during the
    // session with the rtcpd daemon
    m_rtcpdErrors.push_back(rtcpdError);

    // There is nothing else to do
    return;
  }
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
  utils::SmartFd clientSock(ClientTxRx::sendMessage(m_jobRequest.clientHost,
    m_jobRequest.clientPort, CLIENTNETRWTIMEOUT, report, connectDuration));
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
  const int                      socketFd,
  bool&)
  throw(castor::exception::Exception) {

  legacymsg::GiveOutpMsgBody body;

  RtcpTxRx::receiveMsgBody(m_cuuid, m_jobRequest.volReqId, socketFd,
    RTCPDNETRWTIMEOUT, header, body);

  ClientTxRx::notifyDumpMessage(m_cuuid, m_jobRequest.volReqId,
    m_tapebridgeTransactionCounter.next(), m_jobRequest.clientHost,
    m_jobRequest.clientPort, body.message);
}


//-----------------------------------------------------------------------------
// checkPeerIsLocalhost
//-----------------------------------------------------------------------------
void castor::tape::tapebridge::BridgeProtocolEngine::checkPeerIsLocalhost(
  const int socketFd) throw(castor::exception::Exception) {

  unsigned long  ip;
  unsigned short port;

  net::getPeerIpPort(socketFd, ip, port);

  // localhost = 127.0.0.1 = 0x7F000001
  if(ip != 0x7F000001) {
    castor::exception::PermissionDenied ex;
    std::ostream &os = ex.getMessage();
    os <<
      "Peer is not local host"
      ": expected=127.0.0.1"
      ": actual=";
    net::writeIp(os, ip);

    throw ex;
  }
}


//-----------------------------------------------------------------------------
// filesToMigrateListClientCallback
//-----------------------------------------------------------------------------
void castor::tape::tapebridge::BridgeProtocolEngine::
  filesToMigrateListClientCallback(
  const int            clientSock,
  IObject *const       obj,
  const int            rtcpdSock,
  const uint32_t       rtcpdReqMagic,
  const uint32_t       rtcpdReqType,
  const char *const,
  const uint64_t       tapebridgeTransId,
  const struct         timeval)
  throw(castor::exception::Exception) {

  const char *const task = "process FilesToMigrateList message from client";

  // Down cast the reply to its specific class
  tapegateway::FilesToMigrateList * reply =
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
      castor::dlf::Param("clientSock"        , clientSock                    ),
      castor::dlf::Param("nbFiles"           , reply->filesToMigrate().size())};
    castor::dlf::dlf_writep(m_cuuid, DLF_LVL_SYSTEM,
      TAPEBRIDGE_RECEIVED_FILESTOMIGRATELIST, params);
  }

  ClientTxRx::checkTransactionIds("FileToMigrateList",
    m_jobRequest.volReqId, reply->mountTransactionId(),
    tapebridgeTransId    , reply->aggregatorTransactionId());

  if(1 != reply->filesToMigrate().size()) {
    TAPE_THROW_CODE(EBADMSG,
      ": Failed to " << task <<
      ": This version of tapebridged only accepts FilesToMigrateList messages"
      " containing exactly one file to migrate"
      ": nbFiles=" << reply->filesToMigrate().size());
  }

  const tapegateway::FileToMigrateStruct *const fileToMigrate =
    reply->filesToMigrate()[0];

  // Remember the file transaction ID and get its unique index to be
  // passed to rtcpd through the "rtcpFileRequest.disk_fseq" message
  // field
  const int diskFSeq =
    m_pendingTransferIds.insert(fileToMigrate->fileTransactionId());

  // If the client is the tape gateway
  if(m_volume.clientType() == tapegateway::TAPE_GATEWAY) {

    // Throw an exception if the tape file sequence number from the client is
    // invalid
    if(fileToMigrate->fseq() != m_nextDestinationTapeFSeq) {
      TAPE_THROW_CODE(ECANCELED,
        ": Failed to " << task <<
        "Invalid tape-file sequence-number from client"
        ": expected=" << (m_nextDestinationTapeFSeq) <<
        ": actual=" << fileToMigrate->fseq());
    }

    // Update the next expected tape file sequence number
    m_nextDestinationTapeFSeq++;
  }

  // Add the file to the pending migrations store
  {
    RequestToMigrateFile request;

    request.fileTransactionId = fileToMigrate->fileTransactionId();
    request.nsHost = fileToMigrate->nshost();
    request.nsFileId = fileToMigrate->fileid();
    request.positionCommandCode = fileToMigrate->positionCommandCode();
    request.tapeFSeq = fileToMigrate->fseq();
    request.fileSize = fileToMigrate->fileSize();
    request.lastKnownFilename = fileToMigrate->lastKnownFilename();
    request.lastModificationTime = fileToMigrate->lastModificationTime();
    request.path = fileToMigrate->path();
    request.umask = fileToMigrate->umask();

    m_pendingMigrationsStore.receivedRequestToMigrateFile(request);
  }


  {
    castor::dlf::Param params[] = {
      castor::dlf::Param("tapebridgeTransId", reply->aggregatorTransactionId()),
      castor::dlf::Param("mountTransactionId", reply->mountTransactionId()),
      castor::dlf::Param("volReqId", m_jobRequest.volReqId),
      castor::dlf::Param("TPVID", m_volume.vid()),
      castor::dlf::Param("driveUnit", m_jobRequest.driveUnit),
      castor::dlf::Param("dgn", m_jobRequest.dgn),
      castor::dlf::Param("clientHost", m_jobRequest.clientHost),
      castor::dlf::Param("clientPort", m_jobRequest.clientPort),
      castor::dlf::Param("clientType",
        utils::volumeClientTypeToString(m_volume.clientType())),
      castor::dlf::Param("clientSock", clientSock),
      castor::dlf::Param("fileTransactionId",
        fileToMigrate->fileTransactionId()),
      castor::dlf::Param("NSHOSTNAME", fileToMigrate->nshost()),
      castor::dlf::Param("NSFILEID", fileToMigrate->fileid()),
      castor::dlf::Param("tapeFSeq", fileToMigrate->fseq())};
    castor::dlf::dlf_writep(m_cuuid, DLF_LVL_SYSTEM,
      TAPEBRIDGE_ADDED_PENDING_MIGRATION_TO_STORE, params);
  }

  // Give file to migrate to rtcpd
  {
    char migrationTapeFileId[CA_MAXPATHLEN+1];
    utils::toHex((uint64_t)fileToMigrate->fileid(), migrationTapeFileId);
    unsigned char blockId[4];
    utils::setBytes(blockId, '\0');
    char nshost[CA_MAXHOSTNAMELEN+1];
    utils::copyString(nshost, fileToMigrate->nshost().c_str());

    RtcpTxRx::giveFileToRtcpd(
      m_cuuid,
      m_jobRequest.volReqId,
      rtcpdSock,
      RTCPDNETRWTIMEOUT,
      WRITE_ENABLE,
      fileToMigrate->path().c_str(),
      fileToMigrate->fileSize(),
      "",
      RECORDFORMAT,
      migrationTapeFileId,
      RTCOPYCONSERVATIVEUMASK,
      (int32_t)fileToMigrate->positionCommandCode(),
      (int32_t)fileToMigrate->fseq(),
      diskFSeq,
      nshost,
      (uint64_t)fileToMigrate->fileid(),
      blockId);
  }

  char tapePath[CA_MAXPATHLEN+1];
  utils::setBytes(tapePath, '\0');

  // Ask rtcpd to request more work
  RtcpTxRx::askRtcpdToRequestMoreWork(m_cuuid, m_jobRequest.volReqId,
    tapePath, rtcpdSock, RTCPDNETRWTIMEOUT, WRITE_ENABLE);

  // Tell rtcpd end of file list
  RtcpTxRx::tellRtcpdEndOfFileList(m_cuuid, m_jobRequest.volReqId,
    rtcpdSock, RTCPDNETRWTIMEOUT);

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
  LegacyTxRx::sendMsgHeader(m_cuuid, m_jobRequest.volReqId, rtcpdSock,
    RTCPDNETRWTIMEOUT,ackMsg);

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
  const int            clientSock,
  IObject *const       obj,
  const int            rtcpdSock,
  const uint32_t       rtcpdReqMagic,
  const uint32_t       rtcpdReqType,
  const char *const    rtcpdReqTapePath,
  const uint64_t       tapebridgeTransId,
  const struct         timeval)
  throw(castor::exception::Exception) {

  const char *const task = "process FilesToRecallList message from client";

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
      castor::dlf::Param("clientSock"        , clientSock                   ),
      castor::dlf::Param("nbFiles"           , reply->filesToRecall().size())};
    castor::dlf::dlf_writep(m_cuuid, DLF_LVL_SYSTEM,
      TAPEBRIDGE_RECEIVED_FILESTORECALLLIST, params);
  }

  ClientTxRx::checkTransactionIds("FileToRecallList",
     m_jobRequest.volReqId, reply->mountTransactionId(),
     tapebridgeTransId    , reply->aggregatorTransactionId());

  // Throw an exception if there is no tape path associated with the
  // initiating rtcpd request
  if(NULL == rtcpdReqTapePath) {
    castor::exception::Exception ce(ECANCELED);

    ce.getMessage() <<
      "The initiating rtcpd request has no associated tape path";

    throw(ce);
  }

  if(1 != reply->filesToRecall().size()) {
    TAPE_THROW_CODE(EBADMSG,
      ": Failed to " << task <<
      ": This version of tapebridged only accepts FilesToRecallList messages"
      " containing exactly one file to recall"
      ": nbFiles=" << reply->filesToRecall().size());
  }

  const tapegateway::FileToRecallStruct *const fileToRecall =
    reply->filesToRecall()[0];

  // Remember the file transaction ID and get its unique index to be
  // passed to rtcpd through the "rtcpFileRequest.disk_fseq" message
  // field
  const int diskFSeq =
    m_pendingTransferIds.insert(fileToRecall->fileTransactionId());

  // Give file to recall to rtcpd
  {
    char tapeFileId[CA_MAXPATHLEN+1];
    utils::setBytes(tapeFileId, '\0');
    unsigned char blockId[4] = {
      fileToRecall->blockId0(),
      fileToRecall->blockId1(),
      fileToRecall->blockId2(),
      fileToRecall->blockId3()};
    char nshost[CA_MAXHOSTNAMELEN+1];
    utils::copyString(nshost, fileToRecall->nshost().c_str());

    // The file size is not specified when recalling
    const uint64_t fileSize = 0;

    RtcpTxRx::giveFileToRtcpd(
      m_cuuid,
      m_jobRequest.volReqId,
      rtcpdSock,
      RTCPDNETRWTIMEOUT,
      WRITE_DISABLE,
      fileToRecall->path().c_str(),
      fileSize,
      rtcpdReqTapePath,
      RECORDFORMAT,
      tapeFileId,
      (uint32_t)fileToRecall->umask(),
      (int32_t)fileToRecall->positionCommandCode(),
      (int32_t)fileToRecall->fseq(),
      diskFSeq,
      nshost,
      (uint64_t)fileToRecall->fileid(),
      blockId);
  }

  // Ask rtcpd to request more work
  RtcpTxRx::askRtcpdToRequestMoreWork(m_cuuid, m_jobRequest.volReqId,
    rtcpdReqTapePath, rtcpdSock, RTCPDNETRWTIMEOUT, WRITE_DISABLE);

  // Tell rtcpd end of file list
  RtcpTxRx::tellRtcpdEndOfFileList(m_cuuid, m_jobRequest.volReqId,
    rtcpdSock, RTCPDNETRWTIMEOUT);

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
  LegacyTxRx::sendMsgHeader(m_cuuid, m_jobRequest.volReqId, rtcpdSock,
    RTCPDNETRWTIMEOUT,ackMsg);

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
// noMoreFilesClientCallback
//-----------------------------------------------------------------------------
void
  castor::tape::tapebridge::BridgeProtocolEngine::noMoreFilesClientCallback(
  const int            clientSock,
  IObject *const       obj,
  const int            rtcpdSock,
  const uint32_t       rtcpdReqMagic,
  const uint32_t       rtcpdReqType,
  const char *const,
  const uint64_t       tapebridgeTransId,
  const struct         timeval)
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
    castor::dlf::Param("clientSock"        , clientSock                      )};
  castor::dlf::dlf_writep(m_cuuid, DLF_LVL_SYSTEM,
    TAPEBRIDGE_RECEIVED_NOMOREFILES, params);

  ClientTxRx::checkTransactionIds("NoMoreFiles",
    m_jobRequest.volReqId, reply->mountTransactionId(),
    tapebridgeTransId    , reply->aggregatorTransactionId());

  tellRtcpdThereAreNoMoreFiles(rtcpdSock, rtcpdReqMagic, rtcpdReqType);
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
  LegacyTxRx::sendMsgHeader(m_cuuid, m_jobRequest.volReqId, rtcpdSock,
    RTCPDNETRWTIMEOUT,ackMsg);

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
// endNotificationErrorReportClientCallback
//-----------------------------------------------------------------------------
void
  castor::tape::tapebridge::BridgeProtocolEngine::
  endNotificationErrorReportClientCallback(
  const int            clientSock,
  IObject *const       obj,
  const int,
  const uint32_t,
  const uint32_t,
  const char*,
  const uint64_t,
  const struct timeval)
  throw(castor::exception::Exception) {

  // Down cast the reply to its specific class
  const tapegateway::EndNotificationErrorReport *reply =
    dynamic_cast<const tapegateway::EndNotificationErrorReport*>(obj);

  if(reply == NULL) {
    TAPE_THROW_EX(castor::exception::Internal,
      ": Failed to down cast reply object to "
      "tapegateway::EndNotificationErrorReport");
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
    castor::dlf::Param("clientSock"        , clientSock                      ),
    castor::dlf::Param("errorCode"         , reply->errorCode()              ),
    castor::dlf::Param("errorMessage"      , reply->errorMessage()           )};
  castor::dlf::dlf_writep(m_cuuid, DLF_LVL_SYSTEM,
    TAPEBRIDGE_RECEIVED_ENDNOTIFCATIONERRORREPORT, params);

  // Translate the reception of the error report into a C++ exception
  TAPE_THROW_CODE(reply->errorCode(),
    ": Client error report "
    ": " << reply->errorMessage());
}


//-----------------------------------------------------------------------------
// notificationAcknowledge
//-----------------------------------------------------------------------------
void
  castor::tape::tapebridge::BridgeProtocolEngine::notificationAcknowledge(
  const int            clientSock,
  IObject *const       obj,
  const int,
  const uint32_t,
  const uint32_t,
  const char*,
  const uint64_t tapebridgeTransId,
  const struct timeval)
  throw(castor::exception::Exception) {

  // Down cast the reply to its specific class
  const tapegateway::NotificationAcknowledge *reply =
    dynamic_cast<const tapegateway::NotificationAcknowledge*>(obj);

  if(reply == NULL) {
    TAPE_THROW_EX(castor::exception::Internal,
      ": Failed to down cast reply object to "
      "tapegateway::NotificationAcknowledge");
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
    castor::dlf::Param("clientSock"        , clientSock                      )};
  castor::dlf::dlf_writep(m_cuuid, DLF_LVL_SYSTEM,
    TAPEBRIDGE_RECEIVED_NOTIFCATIONACKNOWLEDGE, params);

  ClientTxRx::checkTransactionIds("NotificationAcknowledge",
    m_jobRequest.volReqId, reply->mountTransactionId(),
    tapebridgeTransId    , reply->aggregatorTransactionId());
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
  LegacyTxRx::sendMsgHeader(m_cuuid, m_jobRequest.volReqId,
    m_sockCatalogue.getInitialRtcpdConn(), RTCPDNETRWTIMEOUT,
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
    LegacyTxRx::receiveMsgHeader(m_cuuid, m_jobRequest.volReqId,
      m_sockCatalogue.getInitialRtcpdConn(), RTCPDNETRWTIMEOUT, ackMsg);
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
      std::list<RtcpdError> fileSpecificRtcpdErrors;
      for(std::list<RtcpdError>::const_iterator itor = m_rtcpdErrors.begin();
        itor != m_rtcpdErrors.end(); itor++) {
        if(RtcpdError::FILE_SCOPE == itor->getErrorScope()) {
          fileSpecificRtcpdErrors.push_back(*itor);
        }
      }

      // If there are any file-specific errors, then notify the client
      if(!fileSpecificRtcpdErrors.empty()) {

        // How the client is notified of file-specific error is different
        // depending on the tape access-mode
        switch(m_volume.mode()) {
        case tapegateway::READ:
          notifyClientOfFailedRecalls(m_cuuid, fileSpecificRtcpdErrors);
          break;
        case tapegateway::WRITE:
          notifyClientOfFailedMigrations(m_cuuid, fileSpecificRtcpdErrors);
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
  const std::list<RtcpdError> &rtcpdErrors)
  throw(castor::exception::Exception) {

  // Return if there are no errors to report
  if(rtcpdErrors.empty()) {
    return;
  }

  // Create the list report
  const uint64_t tapebridgeTransId = m_tapebridgeTransactionCounter.next();
  tapegateway::FileMigrationReportList listReport;
  listReport.setMountTransactionId(m_jobRequest.volReqId);
  listReport.setAggregatorTransactionId(tapebridgeTransId);

  // Add the individual file error-reports
  for(std::list<RtcpdError>::const_iterator itor = rtcpdErrors.begin();
    itor != rtcpdErrors.end(); itor++) {
    if(RtcpdError::FILE_SCOPE != itor->getErrorScope()) {
      TAPE_THROW_EX(castor::exception::Internal,
        ": Failed to notifyClientOfFailedMigrations"
        ": Scope of RtcpdError is not FILE_SCOPE");
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
  utils::SmartFd clientSock(ClientTxRx::sendMessage(
    m_jobRequest.clientHost, m_jobRequest.clientPort, CLIENTNETRWTIMEOUT,
    listReport, connectDuration));
  try {
    ClientTxRx::receiveNotificationReplyAndClose(m_jobRequest.volReqId,
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
  const std::list<RtcpdError> &rtcpdErrors)
  throw(castor::exception::Exception) {

  // Return if there are no errors to report
  if(rtcpdErrors.empty()) {
    return;
  }

  // Create the list report
  const uint64_t tapebridgeTransId = m_tapebridgeTransactionCounter.next();
  tapegateway::FileRecallReportList listReport;
  listReport.setMountTransactionId(m_jobRequest.volReqId);
  listReport.setAggregatorTransactionId(tapebridgeTransId);

  // Add the individual file error-reports
  for(std::list<RtcpdError>::const_iterator itor = rtcpdErrors.begin();
    itor != rtcpdErrors.end(); itor++) {
    if(RtcpdError::FILE_SCOPE != itor->getErrorScope()) {
      TAPE_THROW_EX(castor::exception::Internal,
        ": Failed to notifyClientOfFailedMigrations"
        ": Scope of RtcpdError is not FILE_SCOPE");
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
  utils::SmartFd clientSock(ClientTxRx::sendMessage(
    m_jobRequest.clientHost, m_jobRequest.clientPort, CLIENTNETRWTIMEOUT,
    listReport, connectDuration));
  try {
    ClientTxRx::receiveNotificationReplyAndClose(m_jobRequest.volReqId,
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
      // thrown whilst running the session with the rtcpd daemon

      // If an error was generated during the session with the rtcpd daemon
      if(!m_rtcpdErrors.empty()) {

        // Get the oldest and first error to have occurred
        const RtcpdError rtcpdError = m_rtcpdErrors.front();

        // Notify the client the session ended due to the error
        const uint64_t tapebridgeTransId =
          m_tapebridgeTransactionCounter.next();
        ClientTxRx::notifyEndOfFailedSession(m_cuuid, m_jobRequest.volReqId,
          tapebridgeTransId, m_jobRequest.clientHost, m_jobRequest.clientPort,
          rtcpdError.getErrorCode(), rtcpdError.getErrorMessage());
      } else {
        // Notify the client the session has ended with success
        const uint64_t tapebridgeTransId =
          m_tapebridgeTransactionCounter.next();
        ClientTxRx::notifyEndOfSession(m_cuuid, m_jobRequest.volReqId,
          tapebridgeTransId, m_jobRequest.clientHost, m_jobRequest.clientPort);
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
  // The session with the rtcpd daemon should be shut down if the rtcpd daemon
  // has sent us any errors
  return !m_rtcpdErrors.empty();
}
