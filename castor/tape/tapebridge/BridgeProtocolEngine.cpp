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
#include "castor/tape/tapebridge/DlfMessageConstants.hpp"
#include "castor/tape/tapebridge/BridgeProtocolEngine.hpp"
#include "castor/tape/tapebridge/Constants.hpp"
#include "castor/tape/tapebridge/ClientTxRx.hpp"
#include "castor/tape/tapebridge/LegacyTxRx.hpp"
#include "castor/tape/tapebridge/RtcpTxRx.hpp"
#include "castor/tape/net/net.hpp"
#include "castor/tape/tapegateway/EndNotificationErrorReport.hpp"
#include "castor/tape/tapegateway/FileToMigrate.hpp"
#include "castor/tape/tapegateway/FileToRecall.hpp"
#include "castor/tape/tapegateway/NoMoreFiles.hpp"
#include "castor/tape/tapegateway/NotificationAcknowledge.hpp"
#include "castor/tape/utils/SmartFd.hpp"
#include "castor/tape/utils/SmartFdList.hpp"
#include "castor/tape/utils/utils.hpp"
#include "h/Ctape_constants.h"
#include "h/getconfent.h"
#include "h/rtcp_constants.h"
#include "h/rtcpd_constants.h"

#include <algorithm>
#include <list>
#include <memory>
#include <sys/time.h>


//-----------------------------------------------------------------------------
// constructor
//-----------------------------------------------------------------------------
castor::tape::tapebridge::BridgeProtocolEngine::BridgeProtocolEngine(
  const Cuuid_t                       &cuuid,
  const int                           listenSock,
  const int                           initialRtcpdSock,
  const legacymsg::RtcpJobRqstMsgBody &jobRequest,
  const tapegateway::Volume           &volume,
  const uint32_t                      nbFilesOnDestinationTape,
  BoolFunctor                         &stoppingGracefully,
  Counter<uint64_t>                   &tapebridgeTransactionCounter) throw() :
  m_cuuid(cuuid),
  m_jobRequest(jobRequest),
  m_volume(volume),
  m_nextDestinationFseq(nbFilesOnDestinationTape + 1),
  m_stoppingGracefully(stoppingGracefully),
  m_tapebridgeTransactionCounter(tapebridgeTransactionCounter),
  m_nbReceivedENDOF_REQs(0),
  m_pendingTransferIds(MAXPENDINGTRANSFERS) {

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
  m_rtcpdHandlers[createRtcpdHandlerKey(RTCOPY_MAGIC_SHIFT, GIVE_OUTP )] =
    &BridgeProtocolEngine::giveOutpRtcpdCallback;

  // Build the map of client message handlers
  m_clientHandlers[OBJ_FileToMigrate] =
    &BridgeProtocolEngine::fileToMigrateClientCallback;
  m_clientHandlers[OBJ_FileToRecall]  =
    &BridgeProtocolEngine::fileToRecallClientCallback;
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
// processSocks
//-----------------------------------------------------------------------------
void castor::tape::tapebridge::BridgeProtocolEngine::processSocks()
  throw(castor::exception::Exception) {

  int          selectRc            = 0;
  int          selectErrno         = 0;
  unsigned int nbOneSecondTimeouts = 0;
  int          maxFd               = 0;
  fd_set       readFdSet;
  timeval      timeout;

  // Select loop
  bool continueRtcopySession = true;
  while(continueRtcopySession) {
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
    selectRc = select(maxFd + 1, &readFdSet, NULL, NULL, &timeout);
    selectErrno = errno;

    switch(selectRc) {
    case 0: // Select timed out

      nbOneSecondTimeouts++;

      // Ping rtcpd if the RTCPDPINGTIMEOUT has been reached
      if(nbOneSecondTimeouts % RTCPDPINGTIMEOUT == 0) {
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

      continueRtcopySession = processAPendingSocket(readFdSet);

    } // switch(selectRc)

    // Throw an exception if a timeout has occured
    m_sockCatalogue.checkForTimeout();

  } // while(continueRtcopySession)
}


//------------------------------------------------------------------------------
// processAPendingSocket
//------------------------------------------------------------------------------
bool castor::tape::tapebridge::BridgeProtocolEngine::processAPendingSocket(
  fd_set &readFdSet) throw(castor::exception::Exception) {

  BridgeSocketCatalogue::SocketType sockType = BridgeSocketCatalogue::LISTEN;
  const int pendingSock = m_sockCatalogue.getAPendingSock(readFdSet, sockType);
  if(pendingSock == -1) {
    TAPE_THROW_EX(exception::Internal,
      ": Lost pending file descriptor");
  }

  switch(sockType) {
  case BridgeSocketCatalogue::LISTEN:
    {
      int acceptedConnection = 0;

      // Accept the connection
      m_sockCatalogue.addRtcpdDiskTapeIOControlConn(
        acceptedConnection = acceptRtcpdConnection());

      // Throw an exception if connection is not from localhost
      checkPeerIsLocalhost(acceptedConnection);

      // Determine the number of RTCPD disk IO threads
      int nbRtcpdDiskIOThreads = RTCPD_THREAD_POOL; // Compile-time default
      char *p = NULL;
      if((p = getenv("RTCPD_THREAD_POOL")) ||
         (p = getconfent("RTCPD", "THREAD_POOL", 0))) {
        if(!utils::isValidUInt(p)) {
          castor::exception::InvalidArgument ex;

          ex.getMessage() <<
            "RTCPD THREAD_POOL value is not a valid unsigned integer"
            ": value=" << p;

          throw(ex);
        }

        nbRtcpdDiskIOThreads = atoi(p);
      }

      // Determine maximum number of RTCPD disk/tape IO control connections
      // This is the number of disk IO threads plus 1 for the tape IO thread
      const int maxNbDiskTapeIOControlConns = nbRtcpdDiskIOThreads + 1;

      // Throw an exception if the expected maximum number of disk/tape IO
      // control connections has been exceeded
      if(m_sockCatalogue.getNbDiskTapeIOControlConns() >
        maxNbDiskTapeIOControlConns) {
        castor::exception::Exception ex(ECANCELED);

        ex.getMessage() <<
          "Maximum number of disk/tape IO control connections exceeded"
          ": maxNbDiskTapeIOControlConns=" << maxNbDiskTapeIOControlConns <<
          ": actual=" << m_sockCatalogue.getNbDiskTapeIOControlConns();

        throw(ex);
      }
    }
    return true; // Continue the RTCOPY session
  case BridgeSocketCatalogue::INITIAL_RTCPD:
    {
      exception::Exception ce(ECANCELED);

      ce.getMessage() <<
        "Received un-excpected data from the initial rtcpd connection";
      throw(ce);
    }
    break;
  case BridgeSocketCatalogue::RTCPD_DISK_TAPE_IO_CONTROL:
    {
      legacymsg::MessageHeader header;
      utils::setBytes(header, '\0');

      // Try to receive the message header which may not be possible; The file
      // descriptor may be ready because rtcpd has closed the connection
      {
        bool peerClosed = false;
        LegacyTxRx::receiveMsgHeaderFromCloseable(m_cuuid, peerClosed,
          m_jobRequest.volReqId, pendingSock, RTCPDNETRWTIMEOUT, header);

        // If the peer closed its side of the connection, then close this side
        // of the connection and return true in order to continue the RTCOPY
        // session
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

          return true; // Continue the RTCOPY session
        }
      }

      // Process the rtcpd request
      {
        bool receivedENDOF_REQ = false;
        processRtcpdRequest(header, pendingSock, receivedENDOF_REQ);

        // If the message processed was not an ENDOF_REQ, then return true in
        // order to continue the RTCOPY session
        if(!receivedENDOF_REQ) {
          return true; // Continue the RTCOPY session
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
          castor::dlf::Param("socketFd"            , pendingSock           ),
          castor::dlf::Param("nbReceivedENDOF_REQs", m_nbReceivedENDOF_REQs)};
        castor::dlf::dlf_writep(m_cuuid, DLF_LVL_SYSTEM,
          TAPEBRIDGE_RECEIVED_RTCP_ENDOF_REQ, params);
      }

      close(m_sockCatalogue.releaseRtcpdDiskTapeIOControlConn(pendingSock));

      // If only the initial callback connection is open, then send an
      // RTCP_ENDOF_REQ message to rtcpd and close the connection
      if(m_sockCatalogue.getNbDiskTapeIOControlConns() == 0) {

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

        // The initial callback connection will be closed shortly by the
        // VdqmRequestHandler

        return false; // End the RTCOPY session
      }
    }
    return true; // Continue the RTCOPY session
  case BridgeSocketCatalogue::CLIENT:
    {
      // Get information about the rtcpd disk/tape IO control-connection that
      // is waiting for the reply from the pending client-connection
      int            rtcpdSock          = 0;
      uint32_t       rtcpdReqMagic      = 0;
      uint32_t       rtcpdReqType       = 0;
      char           *rtcpdReqTapePath  = NULL;
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
    return true; // Continue the RTCOPY session
  default:
    TAPE_THROW_EX(exception::Internal,
      "Unknown socket type"
      ": socketType = " << sockType);
  }
}


//-----------------------------------------------------------------------------
// run
//-----------------------------------------------------------------------------
void castor::tape::tapebridge::BridgeProtocolEngine::run()
  throw(castor::exception::Exception) {

  switch(m_volume.mode()) {
  case tapegateway::READ:
    runRecallSession();
    break;
  case tapegateway::WRITE:
    runMigrationSession();
    break;
  case tapegateway::DUMP:
    runDumpSession();
    break;
  default:
    TAPE_THROW_EX(castor::exception::Internal,
      ": Unknown VolumeMode"
      ": Actual=" << m_volume.mode());
  }
}


//-----------------------------------------------------------------------------
// runMigrationSession
//-----------------------------------------------------------------------------
void castor::tape::tapebridge::BridgeProtocolEngine::runMigrationSession()
  throw(castor::exception::Exception) {

  // Send the request for the first file to migrate to the client
  time_t connectDuration = 0;
  const uint64_t tapebridgeTransId =
    m_tapebridgeTransactionCounter.next();
  utils::SmartFd clientSock(ClientTxRx::sendFileToMigrateRequest(
    m_jobRequest.volReqId, tapebridgeTransId, m_jobRequest.clientHost,
    m_jobRequest.clientPort, connectDuration));
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
      castor::dlf::Param("clientType"        ,
        utils::volumeClientTypeToString(m_volume.clientType())),
      castor::dlf::Param("clientSock"        , clientSock.get()       ),
      castor::dlf::Param("connectDuration"   , connectDuration        )};
    castor::dlf::dlf_writep(m_cuuid, DLF_LVL_SYSTEM,
      TAPEBRIDGE_SENT_FILETOMIGRATEREQUEST, params);
  }

  // Receive the reply
  int closedClientSock = 0;
  std::auto_ptr<tapegateway::FileToMigrate> fileFromClient(
    ClientTxRx::receiveFileToMigrateReplyAndClose(m_jobRequest.volReqId,
      tapebridgeTransId, closedClientSock = clientSock.release()));

  if(fileFromClient.get() != NULL) {
    castor::dlf::Param params[] = {
      castor::dlf::Param("tapebridgeTransId", 
        fileFromClient->aggregatorTransactionId()),
      castor::dlf::Param("mountTransactionId",
        fileFromClient->mountTransactionId()),
      castor::dlf::Param("volReqId"  , m_jobRequest.volReqId  ),
      castor::dlf::Param("TPVID"     , m_volume.vid()         ),
      castor::dlf::Param("driveUnit" , m_jobRequest.driveUnit ),
      castor::dlf::Param("dgn"       , m_jobRequest.dgn       ),
      castor::dlf::Param("clientHost", m_jobRequest.clientHost),
      castor::dlf::Param("clientPort", m_jobRequest.clientPort),
      castor::dlf::Param("clientType",
        utils::volumeClientTypeToString(m_volume.clientType())),
      castor::dlf::Param("clientSock", closedClientSock       ),
      castor::dlf::Param("fileTransactionId",
        fileFromClient->fileTransactionId()                   ),
      castor::dlf::Param("path"      , fileFromClient->path() )};
    castor::dlf::dlf_writep(m_cuuid, DLF_LVL_SYSTEM,
      TAPEBRIDGE_RECEIVED_FILETOMIGRATE, params);
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

  // If there is no file to migrate
  if(fileFromClient.get() == NULL) {
    const uint64_t tapebridgeTransId =
      m_tapebridgeTransactionCounter.next();

    // Notify client of end of session and return
    try {
      ClientTxRx::notifyEndOfSession(m_cuuid, m_jobRequest.volReqId,
        tapebridgeTransId, m_jobRequest.clientHost,
      m_jobRequest.clientPort);
    } catch(castor::exception::Exception &ex) {
      // Don't rethrow, just log the exception
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
        castor::dlf::Param("Message"           , ex.getMessage().str()  ),
        castor::dlf::Param("Code"              , ex.code()              )};
      castor::dlf::dlf_writep(m_cuuid, DLF_LVL_ERROR,
        TAPEBRIDGE_FAILED_TO_NOTIFY_CLIENT_END_OF_SESSION, params);
    }

    return;
  }

  // If the client is the tape gateway
  if(m_volume.clientType() == tapegateway::TAPE_GATEWAY) {

    // If the tape file sequence number from the client is invalid
    if((uint32_t)fileFromClient->fseq() != m_nextDestinationFseq) {
      castor::exception::Exception ex(ECANCELED);

      ex.getMessage() <<
        "Invalid tape file sequence number from client"
        ": expected=" << (m_nextDestinationFseq) <<
        ": actual=" << fileFromClient->fseq();

      throw(ex);
    }

    // Update the next expected tape file sequence number
    m_nextDestinationFseq++;
  }

  // Remember the file transaction ID and get its unique index to be passed
  // to rtcpd through the "rtcpFileRequest.disk_fseq" message field
  const uint32_t diskFseq =
    m_pendingTransferIds.insert(fileFromClient->fileTransactionId());

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
  rtcpVolume.err.severity   =  1;
  rtcpVolume.err.maxTpRetry = -1;
  rtcpVolume.err.maxCpRetry = -1;
  RtcpTxRx::giveVolumeToRtcpd(m_cuuid, m_jobRequest.volReqId,
    m_sockCatalogue.getInitialRtcpdConn(), RTCPDNETRWTIMEOUT, rtcpVolume);

  // Give file to migrate to rtcpd
  {
    char migrationTapeFileId[CA_MAXPATHLEN+1];
    utils::toHex((uint64_t)fileFromClient->fileid(), migrationTapeFileId);
    unsigned char blockId[4];
    utils::setBytes(blockId, '\0');
    char nshost[CA_MAXHOSTNAMELEN+1];
    utils::copyString(nshost, fileFromClient->nshost().c_str());

    RtcpTxRx::giveFileToRtcpd(
      m_cuuid,
      m_jobRequest.volReqId,
      m_sockCatalogue.getInitialRtcpdConn(),
      RTCPDNETRWTIMEOUT,
      rtcpVolume.mode,
      fileFromClient->path().c_str(),
      fileFromClient->fileSize(),
      "",
      RECORDFORMAT,
      migrationTapeFileId,
      RTCOPYCONSERVATIVEUMASK,
      (int32_t)fileFromClient->positionCommandCode(),
      (int32_t)fileFromClient->fseq(),
      diskFseq,
      nshost,
      (uint64_t)fileFromClient->fileid(),
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

  processSocks();

  {
    const uint64_t tapebridgeTransId =
      m_tapebridgeTransactionCounter.next();
    try {
      ClientTxRx::notifyEndOfSession(m_cuuid, m_jobRequest.volReqId,
        tapebridgeTransId, m_jobRequest.clientHost, m_jobRequest.clientPort);
    } catch(castor::exception::Exception &ex) {
      // Don't rethrow, just log the exception
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
        castor::dlf::Param("Message"           , ex.getMessage().str()  ),
        castor::dlf::Param("Code"              , ex.code()              )};
      castor::dlf::dlf_writep(m_cuuid, DLF_LVL_ERROR,
        TAPEBRIDGE_FAILED_TO_NOTIFY_CLIENT_END_OF_SESSION, params);
    }
  }
}


//-----------------------------------------------------------------------------
// runRecallSession
//-----------------------------------------------------------------------------
void castor::tape::tapebridge::BridgeProtocolEngine::runRecallSession()
  throw(castor::exception::Exception) {

  try {
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
    rtcpVolume.err.severity   =  1;
    rtcpVolume.err.maxTpRetry = -1;
    rtcpVolume.err.maxCpRetry = -1;
    RtcpTxRx::giveVolumeToRtcpd(m_cuuid, m_jobRequest.volReqId,
      m_sockCatalogue.getInitialRtcpdConn(), RTCPDNETRWTIMEOUT, rtcpVolume);

    // Ask rtcpd to request more work
    RtcpTxRx::askRtcpdToRequestMoreWork(m_cuuid, m_jobRequest.volReqId,
      tapePath, m_sockCatalogue.getInitialRtcpdConn(), RTCPDNETRWTIMEOUT,
      WRITE_DISABLE);

    // Tell rtcpd end of file list
    RtcpTxRx::tellRtcpdEndOfFileList(m_cuuid, m_jobRequest.volReqId,
      m_sockCatalogue.getInitialRtcpdConn(), RTCPDNETRWTIMEOUT);

    processSocks();

    {
      const uint64_t tapebridgeTransId = m_tapebridgeTransactionCounter.next();
      try {
        ClientTxRx::notifyEndOfSession(m_cuuid, m_jobRequest.volReqId,
          tapebridgeTransId, m_jobRequest.clientHost,
          m_jobRequest.clientPort);
      } catch(castor::exception::Exception &ex) {
        // Don't rethrow, just log the exception
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
          castor::dlf::Param("Message"           , ex.getMessage().str()  ),
          castor::dlf::Param("Code"              , ex.code()              )};
        castor::dlf::dlf_writep(m_cuuid, DLF_LVL_ERROR,
          TAPEBRIDGE_FAILED_TO_NOTIFY_CLIENT_END_OF_SESSION, params);
      }
    }
  } catch(castor::exception::Exception &ex) {
    castor::exception::Exception ex2(ex.code());

    ex2.getMessage() <<
      "Failed to process rtcpd sockets"
      ": " << ex.getMessage().str();

    throw(ex2);
  }
}


//-----------------------------------------------------------------------------
// runDumpSession
//-----------------------------------------------------------------------------
void castor::tape::tapebridge::BridgeProtocolEngine::runDumpSession()
  throw(castor::exception::Exception) {

  try {
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
    rtcpVolume.err.severity   =  1;
    rtcpVolume.err.maxTpRetry = -1;
    rtcpVolume.err.maxCpRetry = -1;
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

    // Process the rtcpd and tape-gateway sockets until the end of the rtcpd
    // session has been reached
    processSocks();

    {
      const uint64_t tapebridgeTransId = m_tapebridgeTransactionCounter.next();

      try {
        ClientTxRx::notifyEndOfSession(m_cuuid, m_jobRequest.volReqId,
          tapebridgeTransId, m_jobRequest.clientHost, m_jobRequest.clientPort);
      } catch(castor::exception::Exception &ex) {
        // Don't rethrow, just log the exception
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
          castor::dlf::Param("Message"           , ex.getMessage().str()  ),
          castor::dlf::Param("Code"              , ex.code()              )};
        castor::dlf::dlf_writep(m_cuuid, DLF_LVL_ERROR,
          TAPEBRIDGE_FAILED_TO_NOTIFY_CLIENT_END_OF_SESSION, params);
      }
    }
  } catch(castor::exception::Exception &ex) {
    castor::exception::Exception ex2(ex.code());

    ex2.getMessage() <<
      "Failed to process rtcpd sockets"
      ": " << ex.getMessage().str();

    throw(ex2);
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
  bool &receivedENDOF_REQ) throw(castor::exception::Exception) {

  legacymsg::RtcpFileRqstMsgBody body;

  RtcpTxRx::receiveMsgBody(m_cuuid, m_jobRequest.volReqId, socketFd,
    RTCPDNETRWTIMEOUT, header, body);

  processRtcpFileReq(header, body, socketFd, receivedENDOF_REQ);
}


//-----------------------------------------------------------------------------
// rtcpFileErrReqRtcpdCallback
//-----------------------------------------------------------------------------
void
  castor::tape::tapebridge::BridgeProtocolEngine::rtcpFileErrReqRtcpdCallback(
  const legacymsg::MessageHeader &header,
  const int                      socketFd, 
  bool                           &receivedENDOF_REQ)
  throw(castor::exception::Exception) {

  legacymsg::RtcpFileRqstErrMsgBody body;

  RtcpTxRx::receiveMsgBody(m_cuuid, m_jobRequest.volReqId, socketFd,
    RTCPDNETRWTIMEOUT, header, body);

  // If rtcpd has reported an error
  if(body.err.errorCode != 0) {

    TAPE_THROW_CODE(body.err.errorCode,
      ": Received an error from rtcpd: " << body.err.errorMsg);
  }
  
  processRtcpFileReq(header, body, socketFd, receivedENDOF_REQ);
}


//-----------------------------------------------------------------------------
// processRtcpFileReq
//-----------------------------------------------------------------------------
void castor::tape::tapebridge::BridgeProtocolEngine::processRtcpFileReq(
  const legacymsg::MessageHeader &header,
  legacymsg::RtcpFileRqstMsgBody &body,
  const int                      rtcpdSock,
  bool&)
  throw(castor::exception::Exception) {

  // If the tape is being dumped
  if(m_volume.mode() == tapegateway::DUMP) {
    // Send an acknowledge to rtcpd
    legacymsg::MessageHeader ackMsg;
    ackMsg.magic       = header.magic;
    ackMsg.reqType     = header.reqType;
    ackMsg.lenOrStatus = 0;
    LegacyTxRx::sendMsgHeader(m_cuuid, m_jobRequest.volReqId, rtcpdSock,
      RTCPDNETRWTIMEOUT, ackMsg);

    return;
  }

  switch(body.procStatus) {
  case RTCP_REQUEST_MORE_WORK:
    // If migrating
    if(m_volume.mode() == tapegateway::WRITE) {
      // Send a FileToMigrateRequest to the client
      time_t connectDuration = 0;
      const uint64_t tapebridgeTransId = m_tapebridgeTransactionCounter.next();
      utils::SmartFd clientSock(ClientTxRx::sendFileToMigrateRequest(
        m_jobRequest.volReqId, tapebridgeTransId, m_jobRequest.clientHost,
        m_jobRequest.clientPort, connectDuration));
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
          castor::dlf::Param("clientSock"        , clientSock.get()       ),
          castor::dlf::Param("connectDuration"   , connectDuration        )};
        castor::dlf::dlf_writep(m_cuuid, DLF_LVL_SYSTEM,
          TAPEBRIDGE_SENT_FILETOMIGRATEREQUEST, params);
      }

      // Add the client connection to the socket catalogue so that the
      // association with the rtcpd connection is made
      m_sockCatalogue.addClientConn(rtcpdSock, header.magic, header.reqType,
        body.tapePath, clientSock.release(), tapebridgeTransId);

    // Else recalling (READ or DUMP)
    } else {

      // Send a FileToRecallRequest to the client
      time_t connectDuration = 0;
      const uint64_t tapebridgeTransId = m_tapebridgeTransactionCounter.next();
      utils::SmartFd clientSock(ClientTxRx::sendFileToRecallRequest(
        m_jobRequest.volReqId, tapebridgeTransId, m_jobRequest.clientHost,
        m_jobRequest.clientPort, connectDuration));
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
          castor::dlf::Param("clientSock"        , clientSock.get()       ),
          castor::dlf::Param("connectDuration"   , connectDuration        )};
        castor::dlf::dlf_writep(m_cuuid, DLF_LVL_SYSTEM,
          TAPEBRIDGE_SENT_FILETORECALLREQUEST, params);
      }

      // Add the client connection to the socket catalogue so that the
      // association with the rtcpd connection is made
      m_sockCatalogue.addClientConn(rtcpdSock, header.magic, header.reqType,
        body.tapePath, clientSock.release(), tapebridgeTransId);
    }
    break;
  case RTCP_POSITIONED:
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
        castor::dlf::Param("filePath"          , body.filePath          ),
        castor::dlf::Param("tapePath"          , body.tapePath          )};
      castor::dlf::dlf_writep(m_cuuid, DLF_LVL_SYSTEM,
        TAPEBRIDGE_TAPE_POSITIONED, params);

      // Send an acknowledge to rtcpd
      legacymsg::MessageHeader ackMsg;
      ackMsg.magic       = header.magic;
      ackMsg.reqType     = header.reqType;
      ackMsg.lenOrStatus = 0;
      LegacyTxRx::sendMsgHeader(m_cuuid, m_jobRequest.volReqId, rtcpdSock,
        RTCPDNETRWTIMEOUT, ackMsg);
    }
    break;
  case RTCP_FINISHED:
    {
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
        castor::dlf::Param("filePath"          , body.filePath          ),
        castor::dlf::Param("tapePath"          , body.tapePath          ),
        castor::dlf::Param("tapeFseq"          , body.tapeFseq          ),
        castor::dlf::Param("diskFseq"          , body.diskFseq          ),
        castor::dlf::Param("bytesIn"           , body.bytesIn           ),
        castor::dlf::Param("bytesOut"          , body.bytesOut          )};
      castor::dlf::dlf_writep(m_cuuid, DLF_LVL_SYSTEM,
        TAPEBRIDGE_FILE_TRANSFERED, params);

      // Send an acknowledge to rtcpd
      legacymsg::MessageHeader ackMsg;
      ackMsg.magic       = header.magic;
      ackMsg.reqType     = header.reqType;
      ackMsg.lenOrStatus = 0;
      LegacyTxRx::sendMsgHeader(m_cuuid, m_jobRequest.volReqId, rtcpdSock,
        RTCPDNETRWTIMEOUT, ackMsg);

      // Get the file transaction ID that was sent by the tape gateway
      const uint64_t fileTransactonId =
        m_pendingTransferIds.remove(body.diskFseq);

      // Notify the tape gateway
      // If migrating
      if(m_volume.mode() == tapegateway::WRITE) {
        const uint64_t fileSize     = body.bytesIn;  //  "in" from the disk
        uint64_t compressedFileSize = fileSize;      // without compression
        if(0 < body.bytesOut && body.bytesOut < fileSize) {
          compressedFileSize        = body.bytesOut; //   "Out" to the tape
        } 

        time_t connectDuration = 0;
        const uint64_t tapebridgeTransId =
          m_tapebridgeTransactionCounter.next();
        utils::SmartFd clientSock(ClientTxRx::sendFileMigratedNotification(
          m_jobRequest.volReqId, tapebridgeTransId, m_jobRequest.clientHost,
          m_jobRequest.clientPort, connectDuration, fileTransactonId,
          body.segAttr.nameServerHostName, body.segAttr.castorFileId,
          body.tapeFseq, body.blockId, body.positionMethod,
          body.segAttr.segmCksumAlgorithm, body.segAttr.segmCksum, fileSize,
          compressedFileSize));
        {
          castor::dlf::Param params[] = {
            castor::dlf::Param("tapebridgeTransId" , tapebridgeTransId      ),
            castor::dlf::Param("mountTransActionId", m_jobRequest.volReqId  ),
            castor::dlf::Param("volReqId"          , m_jobRequest.volReqId  ),
            castor::dlf::Param("TPVID"             , m_volume.vid()         ),
            castor::dlf::Param("driveUnit"         , m_jobRequest.driveUnit ),
            castor::dlf::Param("dgn"               , m_jobRequest.dgn       ),
            castor::dlf::Param("clientHost"        , m_jobRequest.clientHost),
            castor::dlf::Param("clientPort"        , m_jobRequest.clientPort),
            castor::dlf::Param("clientType",
              utils::volumeClientTypeToString(m_volume.clientType())),
            castor::dlf::Param("clientSock"        , clientSock.get()       ),
            castor::dlf::Param("connectDuration"   , connectDuration        ),
            castor::dlf::Param("fileTransactionId" , fileTransactonId       )};
          castor::dlf::dlf_writep(m_cuuid, DLF_LVL_SYSTEM,
            TAPEBRIDGE_SENT_FILEMIGRATEDNOTIFICATION, params);
        }

        int closedClientSock = 0;
        ClientTxRx::receiveNotificationReplyAndClose(m_jobRequest.volReqId,
          tapebridgeTransId, closedClientSock = clientSock.release());
        {
          castor::dlf::Param params[] = {
            castor::dlf::Param("tapebridgeTransId" , tapebridgeTransId      ),
            castor::dlf::Param("mountTransActionId", m_jobRequest.volReqId  ),
            castor::dlf::Param("volReqId"          , m_jobRequest.volReqId  ),
            castor::dlf::Param("TPVID"             , m_volume.vid()         ),
            castor::dlf::Param("driveUnit"         , m_jobRequest.driveUnit ),
            castor::dlf::Param("dgn"               , m_jobRequest.dgn       ),
            castor::dlf::Param("clientHost"        , m_jobRequest.clientHost),
            castor::dlf::Param("clientPort"        , m_jobRequest.clientPort),
            castor::dlf::Param("clientType",
              utils::volumeClientTypeToString(m_volume.clientType())),
            castor::dlf::Param("clientSock"        , closedClientSock       ),
            castor::dlf::Param("connectDuration"   , connectDuration        ),
            castor::dlf::Param("fileTransactionId" , fileTransactonId       )};
          castor::dlf::dlf_writep(m_cuuid, DLF_LVL_SYSTEM,
            TAPEBRIDGE_RECEIVED_ACK_OF_NOTIFICATION, params);
        }

      // Else recall (READ or DUMP)
      } else {
        const uint64_t fileSize = body.bytesOut; // "out" from the tape

        time_t connectDuration = 0;
        const uint64_t tapebridgeTransId =
          m_tapebridgeTransactionCounter.next();
        utils::SmartFd clientSock(ClientTxRx::sendFileRecalledNotification(
          m_jobRequest.volReqId, tapebridgeTransId, m_jobRequest.clientHost,
          m_jobRequest.clientPort, connectDuration, fileTransactonId,
          body.segAttr.nameServerHostName, body.segAttr.castorFileId,
          body.tapeFseq, body.filePath, body.positionMethod,
          body.segAttr.segmCksumAlgorithm, body.segAttr.segmCksum, fileSize));
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
            castor::dlf::Param("clientSock"        , clientSock.get()       ),
            castor::dlf::Param("connectDuration"   , connectDuration        ),
            castor::dlf::Param("fileTransactionId" , fileTransactonId       )};
          castor::dlf::dlf_writep(m_cuuid, DLF_LVL_SYSTEM,
            TAPEBRIDGE_SENT_FILERECALLEDNOTIFICATION, params);
        }

        int closedClientSock = 0;
        ClientTxRx::receiveNotificationReplyAndClose(m_jobRequest.volReqId,
          tapebridgeTransId, closedClientSock = clientSock.release());
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
            castor::dlf::Param("clientSock"        , closedClientSock       ),
            castor::dlf::Param("connectDuration"   , connectDuration        ),
            castor::dlf::Param("fileTransactionId" , fileTransactonId       )};
          castor::dlf::dlf_writep(m_cuuid, DLF_LVL_SYSTEM,
            TAPEBRIDGE_RECEIVED_ACK_OF_NOTIFICATION, params);
        }
      }
    }
    break;
  default:
    {
      TAPE_THROW_CODE(EBADMSG,
           ": Received unexpected file request process status 0x"
        << std::hex << body.procStatus
        << "(" << utils::procStatusToString(body.procStatus) << ")");
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
      ": errorMsg=" << body.err.errorMsg  <<
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
// fileToMigrateClientCallback
//-----------------------------------------------------------------------------
void
  castor::tape::tapebridge::BridgeProtocolEngine::fileToMigrateClientCallback(
  const int            clientSock,
  const IObject *const obj,
  const int            rtcpdSock,
  const uint32_t       rtcpdReqMagic,
  const uint32_t       rtcpdReqType,
  const char *const,
  const uint64_t       tapebridgeTransId,
  const struct         timeval)
  throw(castor::exception::Exception) {

  // Down cast the reply to its specific class
  const tapegateway::FileToMigrate *reply =
    dynamic_cast<const tapegateway::FileToMigrate*>(obj);

  if(reply == NULL) {
    TAPE_THROW_EX(castor::exception::Internal,
      ": Failed to down cast reply object to tapegateway::FileToMigrate");
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
    castor::dlf::Param("fileTransactionId" , reply->fileTransactionId()      ),
    castor::dlf::Param("path"              , reply->path()                   )};
  castor::dlf::dlf_writep(m_cuuid, DLF_LVL_SYSTEM,
    TAPEBRIDGE_RECEIVED_FILETOMIGRATE, params);

  ClientTxRx::checkTransactionIds("FileToMigrate",
    m_jobRequest.volReqId, reply->mountTransactionId(),
    tapebridgeTransId    , reply->aggregatorTransactionId());

  // Remember the file transaction ID and get its unique index to be
  // passed to rtcpd through the "rtcpFileRequest.disk_fseq" message
  // field
  const int diskFseq =
    m_pendingTransferIds.insert(reply->fileTransactionId());

  // If the client is the tape gateway
  if(m_volume.clientType() == tapegateway::TAPE_GATEWAY) {

    // Throw an exception if the tape file sequence number from the client is
    // invalid
    if((uint32_t)reply->fseq() != m_nextDestinationFseq) {
      castor::exception::Exception ex(ECANCELED);

      ex.getMessage() <<
        "Invalid tape file sequence number from client"
        ": expected=" << (m_nextDestinationFseq) <<
        ": actual=" << reply->fseq();

      throw(ex);
    }

    // Update the next expected tape file sequence number
    m_nextDestinationFseq++;
  }

  // Give file to migrate to rtcpd
  {
    char migrationTapeFileId[CA_MAXPATHLEN+1];
    utils::toHex((uint64_t)reply->fileid(), migrationTapeFileId);
    unsigned char blockId[4];
    utils::setBytes(blockId, '\0');
    char nshost[CA_MAXHOSTNAMELEN+1];
    utils::copyString(nshost, reply->nshost().c_str());

    RtcpTxRx::giveFileToRtcpd(
      m_cuuid,
      m_jobRequest.volReqId,
      rtcpdSock,
      RTCPDNETRWTIMEOUT,
      WRITE_ENABLE,
      reply->path().c_str(),
      reply->fileSize(),
      "",
      RECORDFORMAT,
      migrationTapeFileId,
      RTCOPYCONSERVATIVEUMASK,
      (int32_t)reply->positionCommandCode(),
      (int32_t)reply->fseq(),
      diskFseq,
      nshost,
      (uint64_t)reply->fileid(),
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
// fileToRecallClientCallback
//-----------------------------------------------------------------------------
void
  castor::tape::tapebridge::BridgeProtocolEngine::fileToRecallClientCallback(
  const int            clientSock,
  const IObject *const obj,
  const int            rtcpdSock,
  const uint32_t       rtcpdReqMagic,
  const uint32_t       rtcpdReqType,
  const char *const    rtcpdReqTapePath,
  const uint64_t       tapebridgeTransId,
  const struct         timeval)
  throw(castor::exception::Exception) {

  // Down cast the reply to its specific class
  const tapegateway::FileToRecall *reply =
    dynamic_cast<const tapegateway::FileToRecall*>(obj);

  if(reply == NULL) {
    TAPE_THROW_EX(castor::exception::Internal,
      ": Failed to down cast reply object to tapegateway::FileToRecall");
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
    castor::dlf::Param("fileTransactionId" , reply->fileTransactionId()      ),
    castor::dlf::Param("path"              , reply->path()                   )};
  castor::dlf::dlf_writep(m_cuuid, DLF_LVL_SYSTEM,
    TAPEBRIDGE_RECEIVED_FILETORECALL, params);

  ClientTxRx::checkTransactionIds("FileToRecall",
     m_jobRequest.volReqId, reply->mountTransactionId(),
     tapebridgeTransId    , reply->aggregatorTransactionId());

  // Throw an exception if there is no tape path associated with the
  // initiating rtcpd request
  if(rtcpdReqTapePath == NULL) {
    castor::exception::Exception ce(ECANCELED);

    ce.getMessage() <<
      "The initiating rtcpd request has no associated tape path";

    throw(ce);
  }

  // Remember the file transaction ID and get its unique index to be
  // passed to rtcpd through the "rtcpFileRequest.disk_fseq" message
  // field
  const int diskFseq =
    m_pendingTransferIds.insert(reply->fileTransactionId());

  // Give file to recall to rtcpd
  {
    char tapeFileId[CA_MAXPATHLEN+1];
    utils::setBytes(tapeFileId, '\0');
    unsigned char blockId[4] = {
      reply->blockId0(),
      reply->blockId1(),
      reply->blockId2(),
      reply->blockId3()};
    char nshost[CA_MAXHOSTNAMELEN+1];
    utils::copyString(nshost, reply->nshost().c_str());

    // The file size is not specified when recalling
    const uint64_t fileSize = 0;

    RtcpTxRx::giveFileToRtcpd(
      m_cuuid,
      m_jobRequest.volReqId,
      rtcpdSock,
      RTCPDNETRWTIMEOUT,
      WRITE_DISABLE,
      reply->path().c_str(),
      fileSize,
      rtcpdReqTapePath,
      RECORDFORMAT,
      tapeFileId,
      (uint32_t)reply->umask(),
      (int32_t)reply->positionCommandCode(),
      (int32_t)reply->fseq(),
      diskFseq,
      nshost,
      (uint64_t)reply->fileid(),
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
  const IObject *const obj,
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
  const IObject *const obj,
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
  const IObject *const obj,
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
