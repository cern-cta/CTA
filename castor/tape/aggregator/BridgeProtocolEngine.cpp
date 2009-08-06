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

#include "castor/dlf/Dlf.hpp"
#include "castor/exception/Internal.hpp"
#include "castor/exception/TimeOut.hpp"
#include "castor/tape/Constants.hpp"
#include "castor/tape/aggregator/AggregatorDlfMessageConstants.hpp"
#include "castor/tape/aggregator/BridgeProtocolEngine.hpp"
#include "castor/tape/aggregator/Constants.hpp"
#include "castor/tape/aggregator/GatewayTxRx.hpp"
#include "castor/tape/aggregator/GiveOutpMsgBody.hpp"
#include "castor/tape/aggregator/MessageHeader.hpp"
#include "castor/tape/aggregator/RtcpDumpTapeRqstMsgBody.hpp"
#include "castor/tape/aggregator/RtcpTxRx.hpp"
#include "castor/tape/aggregator/SmartFd.hpp"
#include "castor/tape/aggregator/SmartFdList.hpp"
#include "castor/tape/net/net.hpp"
#include "castor/tape/utils/utils.hpp"
#include "h/Ctape_constants.h"
#include "h/rtcp_constants.h"

#include <list>


//-----------------------------------------------------------------------------
// constructor
//-----------------------------------------------------------------------------
castor::tape::aggregator::BridgeProtocolEngine::BridgeProtocolEngine(
  const Cuuid_t           &cuuid,
  const int               rtcpdCallbackSockFd,
  const int               rtcpdInitialSockFd,
  const RcpJobRqstMsgBody &jobRequest,
  tapegateway::Volume     &volume,
  char                    (&vsn)[CA_MAXVSNLEN+1],
  BoolFunctor             &stoppingGracefully) throw() :
  m_cuuid(cuuid),
  m_rtcpdCallbackSockFd(rtcpdCallbackSockFd),
  m_rtcpdInitialSockFd(rtcpdInitialSockFd),
  m_jobRequest(jobRequest),
  m_volume(volume),
  m_vsn(vsn),
  m_stoppingGracefully(stoppingGracefully),
  m_nbCallbackConnections(1), // Initial callback connection has already exists
  m_nbReceivedENDOF_REQs(0) {

  // Build the map of message body handlers
  m_handlers[createHandlerKey(RTCOPY_MAGIC,       RTCP_FILE_REQ   )] = 
    &BridgeProtocolEngine::rtcpFileReqCallback;

  m_handlers[createHandlerKey(RTCOPY_MAGIC,       RTCP_FILEERR_REQ)] = 
    &BridgeProtocolEngine::rtcpFileErrReqCallback;

  m_handlers[createHandlerKey(RTCOPY_MAGIC,       RTCP_TAPE_REQ   )] = 
    &BridgeProtocolEngine::rtcpTapeReqCallback;

  m_handlers[createHandlerKey(RTCOPY_MAGIC,       RTCP_TAPEERR_REQ)] = 
    &BridgeProtocolEngine::rtcpTapeErrReqCallback;

  m_handlers[createHandlerKey(RTCOPY_MAGIC,       RTCP_ENDOF_REQ  )] = 
    &BridgeProtocolEngine::rtcpEndOfReqCallback;

  m_handlers[createHandlerKey(RTCOPY_MAGIC_SHIFT, GIVE_OUTP       )] = 
    &BridgeProtocolEngine::giveOutpCallback;
}


//-----------------------------------------------------------------------------
// acceptRtcpdConnection
//-----------------------------------------------------------------------------
int castor::tape::aggregator::BridgeProtocolEngine::acceptRtcpdConnection()
  throw(castor::exception::Exception) {

  SmartFd connectedSockFd;
  const int timeout = 5; // Seconds

  bool connectionAccepted = false;
  for(int i=0; i<timeout && !connectionAccepted; i++) {
    try {
      connectedSockFd.reset(net::acceptConnection(m_rtcpdCallbackSockFd, 1));
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

  // Throw an exception if the RTCPD connection could not be accepted
  if(!connectionAccepted) {
    castor::exception::TimeOut ex;

    ex.getMessage() <<
      "Failed to accept RTCPD connection after " << timeout << " seconds";
    throw ex;
  }

  // Update the number of callback connections
  m_nbCallbackConnections++;

  try {
    unsigned short port = 0; // Client port
    unsigned long  ip   = 0; // Client IP
    char           hostName[net::HOSTNAMEBUFLEN];

    net::getPeerIpPort(connectedSockFd.get(), ip, port);
    net::getPeerHostName(connectedSockFd.get(), hostName);

    castor::dlf::Param params[] = {
      castor::dlf::Param("volReqId"       , m_jobRequest.volReqId     ),
      castor::dlf::Param("IP"             , castor::dlf::IPAddress(ip)),
      castor::dlf::Param("Port"           , port                      ),
      castor::dlf::Param("HostName"       , hostName                  ),
      castor::dlf::Param("socketFd"       , connectedSockFd.get()   ),
      castor::dlf::Param("nbCallbackConns", m_nbCallbackConnections   )};
    castor::dlf::dlf_writep(m_cuuid, DLF_LVL_SYSTEM,
      AGGREGATOR_RTCPD_CALLBACK_WITH_INFO, params);
  } catch(castor::exception::Exception &ex) {
    castor::dlf::Param params[] = {
      castor::dlf::Param("volReqId"       , m_jobRequest.volReqId  ),
      castor::dlf::Param("nbCallbackConns", m_nbCallbackConnections)};
    castor::dlf::dlf_writep(m_cuuid, DLF_LVL_SYSTEM,
      AGGREGATOR_RTCPD_CALLBACK_WITHOUT_INFO, params);
  }

  return connectedSockFd.release();
}


//-----------------------------------------------------------------------------
// processRtcpdSocks
//-----------------------------------------------------------------------------
void castor::tape::aggregator::BridgeProtocolEngine::processRtcpdSocks()
  throw(castor::exception::Exception) {

  int          selectRc            = 0;
  int          selectErrno         = 0;
  int          maxFd               = 0;
  unsigned int nbOneSecondTimeouts = 0;
  timeval      timeout;
  fd_set       readFdSet;

  // Append the socket descriptors of the RTCPD callback port and the initial
  // connection from RTCPD to the list of read file descriptors
  m_readFds.push_back(m_rtcpdCallbackSockFd);
  m_readFds.push_back(m_rtcpdInitialSockFd);
  if(m_rtcpdCallbackSockFd > m_rtcpdInitialSockFd) {
    maxFd = m_rtcpdCallbackSockFd;
  } else {
    maxFd = m_rtcpdInitialSockFd;
  }

  // Select loop
  bool continueRtcopySession = true;
  while(continueRtcopySession) {
    // Throw an exception if the daemon is stopping gracefully
    if(m_stoppingGracefully()) {
      castor::exception::Exception ex(ECANCELED);

      ex.getMessage() << "Stopping gracefully";
      throw(ex);
    }

    // Ping the client if it is readtp, writetp or dumptp
    if(m_volume.clientType() == tapegateway::READ_TP  ||
       m_volume.clientType() == tapegateway::WRITE_TP ||
       m_volume.clientType() == tapegateway::DUMP_TP) {

      try {
        GatewayTxRx::pingClient(m_cuuid, m_jobRequest.volReqId,
          m_jobRequest.clientHost, m_jobRequest.clientPort);
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

    // Build the file descriptor set ready for the select call
    FD_ZERO(&readFdSet);
    for(std::list<int>::iterator itor = m_readFds.begin();
      itor != m_readFds.end(); itor++) {

      FD_SET(*itor, &readFdSet);

      if(*itor > maxFd) {
        maxFd = *itor;
      }
    }

    timeout.tv_sec  = 1; // 1 second
    timeout.tv_usec = 0;

    // See if any of the read file descriptors are ready waiting up to 1 second
    selectRc = select(maxFd + 1, &readFdSet, NULL, NULL, &timeout);
    selectErrno = errno;

    switch(selectRc) {
    case 0: // Select timed out

      nbOneSecondTimeouts++;

      if(nbOneSecondTimeouts % RTCPDPINGTIMEOUT == 0) {
        RtcpTxRx::pingRtcpd(m_cuuid, m_jobRequest.volReqId,
        m_rtcpdInitialSockFd, RTCPDNETRWTIMEOUT);
      }
      break;

    case -1: // Select encountered an error

      // If the select was interrupted
      if(selectErrno == EINTR) {

        // Write a log message
        castor::dlf::Param params[] = {
          castor::dlf::Param("volReqId", m_jobRequest.volReqId)};
        castor::dlf::dlf_writep(m_cuuid, DLF_LVL_SYSTEM,
          AGGREGATOR_SELECT_INTR, params);

      // Else select encountered an error other than an interruption
      } else {

        // Convert the error into an exception
        char strerrorBuf[STRERRORBUFLEN];
        char *const errorStr = strerror_r(selectErrno, strerrorBuf,
          sizeof(strerrorBuf));

        TAPE_THROW_CODE(selectErrno,
          ": Select encountered an error other than an interruption"
          ": " << errorStr);
      }
      break;

    default: // One or more select file descriptors require attention

      int nbProcessedFds = 0;

      // For each read file descriptor or until all ready file descriptors
      // have been processed
      for(std::list<int>::iterator itor = m_readFds.begin();
        itor != m_readFds.end() && nbProcessedFds < selectRc; itor++) {

        // If the read file descriptor is ready
        if(FD_ISSET(*itor, &readFdSet)) {
          bool endOfSession = false;

          processRtcpdSock(*itor, endOfSession);
          nbProcessedFds++;

          if(endOfSession) {
            continueRtcopySession = false;
          }
        }
      }
    } // switch(selectRc)
  } // while(continueRtcopySession)
}


//-----------------------------------------------------------------------------
// processRtcpdSock
//-----------------------------------------------------------------------------
void castor::tape::aggregator::BridgeProtocolEngine::processRtcpdSock(
  const int socketFd, bool &endOfSession) throw(castor::exception::Exception) {

  // If the file descriptor is that of the callback port
  if(socketFd == m_rtcpdCallbackSockFd) {

    // Accept the connection and append its socket descriptor to
    // the list of read file descriptors
    //
    // acceptRtcpdConnection() increments m_nbCallbackConnections
    m_readFds.push_back(acceptRtcpdConnection());

  // Else the file descriptor is that of a tape/disk IO connection
  } else {

    // Try to receive the message header which may not be possible; The file
    // descriptor may be ready because RTCPD has closed the connection
    bool connectionClosed = false;
    MessageHeader header;
    utils::setBytes(header, '\0');
    RtcpTxRx::receiveMessageHeaderFromCloseable(m_cuuid, connectionClosed,
      m_jobRequest.volReqId, socketFd, RTCPDNETRWTIMEOUT, header);

    // If the connection has been closed by RTCPD, then remove the
    // file descriptor from the list of read file descriptors and
    // close it
    if(connectionClosed) {
      close(m_readFds.release(socketFd));

      m_nbCallbackConnections--;

      castor::dlf::Param params[] = {
        castor::dlf::Param("volReqId"       , m_jobRequest.volReqId  ),
        castor::dlf::Param("socketFd"       , socketFd               ),
        castor::dlf::Param("nbCallbackConns", m_nbCallbackConnections)};
      castor::dlf::dlf_writep(m_cuuid, DLF_LVL_SYSTEM,
        AGGREGATOR_CONNECTION_CLOSED_BY_RTCPD, params);

      // Finished processing socket as the connection is closed
      return;
    }

    bool receivedENDOF_REQ = false;
    processRtcpdRequest(header, socketFd, receivedENDOF_REQ);

    // If an RTCP_ENDOF_REQ message was received
    if(receivedENDOF_REQ) {
      m_nbReceivedENDOF_REQs++;

      {
        castor::dlf::Param params[] = {
          castor::dlf::Param("volReqId"            , m_jobRequest.volReqId ),
          castor::dlf::Param("socketFd"            , socketFd              ),
          castor::dlf::Param("nbReceivedENDOF_REQs", m_nbReceivedENDOF_REQs)};
        castor::dlf::dlf_writep(m_cuuid, DLF_LVL_SYSTEM,
          AGGREGATOR_RECEIVED_RTCP_ENDOF_REQ, params);
      }

      // Remove the file descriptor from the list of read file descriptors and
      // close it
      close(m_readFds.release(socketFd));

      m_nbCallbackConnections--;

      {
        castor::dlf::Param params[] = {
          castor::dlf::Param("volReqId"       , m_jobRequest.volReqId  ),
          castor::dlf::Param("socketFd"       , socketFd               ),
          castor::dlf::Param("nbCallbackConns", m_nbCallbackConnections)};
        castor::dlf::dlf_writep(m_cuuid, DLF_LVL_SYSTEM,
          AGGREGATOR_CLOSED_CONNECTION, params);
      }

      // If only the initial callback connection is open, then send an
      // RTCP_ENDOF_REQ message to RTCPD and close the connection
      if(m_nbCallbackConnections == 1) {

        // Try to find the socket file descriptor of the initial callback
        // connection is the list of open connections
        SmartFdList::iterator itor = std::find(m_readFds.begin(),
          m_readFds.end(), m_rtcpdInitialSockFd);

        // If the file descriptor cannot be found
        if(itor == m_readFds.end()) {
          TAPE_THROW_EX(castor::exception::Internal,
            ": The initial callback connection (fd=" << m_rtcpdInitialSockFd
            <<")is not the last one open. Found fd= " << *itor);
        }

        // Send an RTCP_ENDOF_REQ message to RTCPD
        MessageHeader endofReqMsg;
        endofReqMsg.magic       = RTCOPY_MAGIC;
        endofReqMsg.reqType     = RTCP_ENDOF_REQ;
        endofReqMsg.lenOrStatus = 0;
        RtcpTxRx::sendMessageHeader(m_cuuid, m_jobRequest.volReqId,
          m_rtcpdInitialSockFd, RTCPDNETRWTIMEOUT, endofReqMsg);

        // Receive the acknowledge of the RTCP_ENDOF_REQ message
        MessageHeader ackMsg;
        try {
          RtcpTxRx::receiveMessageHeader(m_cuuid, m_jobRequest.volReqId,
            m_rtcpdInitialSockFd, RTCPDNETRWTIMEOUT, ackMsg);
        } catch(castor::exception::Exception &ex) {
          TAPE_THROW_CODE(EPROTO,
            ": Failed to receive acknowledge of RTCP_ENDOF_REQ from RTCPD: "
            << ex.getMessage().str());
        }

        // Close the connection
        close(m_readFds.release(m_rtcpdInitialSockFd));

        // Mark the end of the recall/migration session
        endOfSession = true;
      }
    }
  }
}


//-----------------------------------------------------------------------------
// run
//-----------------------------------------------------------------------------
void castor::tape::aggregator::BridgeProtocolEngine::run()
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
void castor::tape::aggregator::BridgeProtocolEngine::runMigrationSession()
  throw(castor::exception::Exception) {

  char          filePath[CA_MAXPATHLEN+1];
  char          fileNsHost[CA_MAXHOSTNAMELEN+1];
  char          fileLastKnownFilename[CA_MAXPATHLEN+1];
  char          tapePath[CA_MAXPATHLEN+1];
  unsigned char (blockId)[4];

  utils::setBytes(filePath             , '\0');
  utils::setBytes(fileNsHost           , '\0');
  utils::setBytes(fileLastKnownFilename, '\0');
  utils::setBytes(tapePath             , '\0');
  utils::setBytes(blockId              , '\0');

  uint64_t fileTransactionId        = 0;
  uint64_t fileId                   = 0;
  int32_t  fileTapeFileSeq          = 0;
  uint64_t fileSize                 = 0;
  uint64_t fileLastModificationTime = 0;
  int32_t  positionCommandCode      = 0;

  // Get first file to migrate from tape gateway
  const bool thereIsAFileToMigrate =
    GatewayTxRx::getFileToMigrateFromGateway(m_cuuid, m_jobRequest.volReqId,
    m_jobRequest.clientHost, m_jobRequest.clientPort, fileTransactionId,
    filePath, fileNsHost, fileId, fileTapeFileSeq, fileSize,
    fileLastKnownFilename, fileLastModificationTime, positionCommandCode);

  // If there is no file to migrate, then notify tape gatway of end of session
  // and return
  if(!thereIsAFileToMigrate) {
    try {
      GatewayTxRx::notifyGatewayEndOfSession(m_cuuid, m_jobRequest.volReqId,
        m_jobRequest.clientHost, m_jobRequest.clientPort);
    } catch(castor::exception::Exception &ex) {
      // Don't rethrow, just log the exception
      castor::dlf::Param params[] = {
        castor::dlf::Param("volReqId", m_jobRequest.volReqId           ),
        castor::dlf::Param("Message" , ex.getMessage().str()),
        castor::dlf::Param("Code"    , ex.code()            )};
      castor::dlf::dlf_writep(m_cuuid, DLF_LVL_SYSTEM,
        AGGREGATOR_FAILED_TO_NOTIFY_CLIENT_END_OF_SESSION, params);
    }

    return;
  }

  // Remember the file transaction ID and get its unique index to be passed to
  // RTCPD through the "rtcpFileRequest.disk_fseq" message field
  const uint32_t diskFseq = m_pendingTransferIds.insert(fileTransactionId);

  // Give volume to RTCPD
  RtcpTapeRqstErrMsgBody rtcpVolume;
  utils::setBytes(rtcpVolume, '\0');
  utils::copyString(rtcpVolume.vid    , m_volume.vid().c_str()    );
  utils::copyString(rtcpVolume.vsn    , m_vsn                     );
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
     m_rtcpdInitialSockFd, RTCPDNETRWTIMEOUT, rtcpVolume);

  // Give file to migrate to RTCPD
  char migrationTapeFileId[CA_MAXPATHLEN+1];
  utils::toHex(fileId, migrationTapeFileId);
  RtcpTxRx::giveFileToRtcpd(m_cuuid, m_jobRequest.volReqId,
    m_rtcpdInitialSockFd, RTCPDNETRWTIMEOUT, rtcpVolume.mode, filePath,
    fileSize, "", RECORDFORMAT, migrationTapeFileId, MIGRATEUMASK,
    positionCommandCode, fileTapeFileSeq, diskFseq, fileNsHost, fileId,
    blockId);

  // Ask RTCPD to request more work
  RtcpTxRx::askRtcpdToRequestMoreWork(m_cuuid, m_jobRequest.volReqId, tapePath,
    m_rtcpdInitialSockFd, RTCPDNETRWTIMEOUT, WRITE_ENABLE);

  // Tell RTCPD end of file list
  RtcpTxRx::tellRtcpdEndOfFileList(m_cuuid, m_jobRequest.volReqId,
    m_rtcpdInitialSockFd, RTCPDNETRWTIMEOUT);

  try {
    processRtcpdSocks();

    try {
      GatewayTxRx::notifyGatewayEndOfSession(m_cuuid, m_jobRequest.volReqId,
        m_jobRequest.clientHost, m_jobRequest.clientPort);
    } catch(castor::exception::Exception &ex) {
      // Don't rethrow, just log the exception
      castor::dlf::Param params[] = {
        castor::dlf::Param("volReqId", m_jobRequest.volReqId),
        castor::dlf::Param("Message" , ex.getMessage().str()),
        castor::dlf::Param("Code"    , ex.code()            )};
      castor::dlf::dlf_writep(m_cuuid, DLF_LVL_SYSTEM,
        AGGREGATOR_FAILED_TO_NOTIFY_CLIENT_END_OF_SESSION, params);
    }
  } catch(castor::exception::Exception &ex) {
    castor::dlf::Param params[] = {
      castor::dlf::Param("volReqId", m_jobRequest.volReqId),
      castor::dlf::Param("Message" , ex.getMessage().str()),
      castor::dlf::Param("Code"    , ex.code()            )};
    castor::dlf::dlf_writep(m_cuuid, DLF_LVL_SYSTEM,
      AGGREGATOR_FAILED_TO_PROCESS_RTCPD_SOCKETS, params);

    try {
      GatewayTxRx::notifyGatewayEndOfFailedSession(m_cuuid,
         m_jobRequest.volReqId, m_jobRequest.clientHost,
         m_jobRequest.clientPort, ex);
    } catch(castor::exception::Exception &ex) {
      // Don't rethrow, just log the exception
      castor::dlf::Param params[] = {
        castor::dlf::Param("volReqId", m_jobRequest.volReqId),
        castor::dlf::Param("Message" , ex.getMessage().str()),
        castor::dlf::Param("Code"    , ex.code()            )};
      castor::dlf::dlf_writep(m_cuuid, DLF_LVL_SYSTEM,
        AGGREGATOR_FAILED_TO_NOTIFY_CLIENT_END_OF_FAILED_SESSION, params);
    }
  }
}


//-----------------------------------------------------------------------------
// runRecallSession
//-----------------------------------------------------------------------------
void castor::tape::aggregator::BridgeProtocolEngine::runRecallSession()
  throw(castor::exception::Exception) {

  char tapePath[CA_MAXPATHLEN+1];
  utils::setBytes(tapePath, '\0');

  // Give volume to RTCPD
  RtcpTapeRqstErrMsgBody rtcpVolume;
  utils::setBytes(rtcpVolume, '\0');
  utils::copyString(rtcpVolume.vid    , m_volume.vid().c_str()    );
  utils::copyString(rtcpVolume.vsn    , m_vsn                     );
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
    m_rtcpdInitialSockFd, RTCPDNETRWTIMEOUT, rtcpVolume);

  // Ask RTCPD to request more work
  RtcpTxRx::askRtcpdToRequestMoreWork(m_cuuid, m_jobRequest.volReqId, tapePath,
    m_rtcpdInitialSockFd, RTCPDNETRWTIMEOUT, WRITE_DISABLE);

  // Tell RTCPD end of file list
  RtcpTxRx::tellRtcpdEndOfFileList(m_cuuid, m_jobRequest.volReqId,
    m_rtcpdInitialSockFd, RTCPDNETRWTIMEOUT);

  try {
    processRtcpdSocks();

    try {
      GatewayTxRx::notifyGatewayEndOfSession(m_cuuid, m_jobRequest.volReqId,
        m_jobRequest.clientHost, m_jobRequest.clientPort);
    } catch(castor::exception::Exception &ex) {
      // Don't rethrow, just log the exception
      castor::dlf::Param params[] = {
        castor::dlf::Param("volReqId", m_jobRequest.volReqId),
        castor::dlf::Param("Message" , ex.getMessage().str()),
        castor::dlf::Param("Code"    , ex.code()            )};
      castor::dlf::dlf_writep(m_cuuid, DLF_LVL_SYSTEM,
        AGGREGATOR_FAILED_TO_NOTIFY_CLIENT_END_OF_SESSION, params);
    }
  } catch(castor::exception::Exception &ex) {
    castor::dlf::Param params[] = {
      castor::dlf::Param("volReqId", m_jobRequest.volReqId),
      castor::dlf::Param("Message" , ex.getMessage().str()),
      castor::dlf::Param("Code"    , ex.code()            )};
    castor::dlf::dlf_writep(m_cuuid, DLF_LVL_SYSTEM,
      AGGREGATOR_FAILED_TO_PROCESS_RTCPD_SOCKETS, params);

    try {
      GatewayTxRx::notifyGatewayEndOfFailedSession(m_cuuid,
        m_jobRequest.volReqId, m_jobRequest.clientHost,
        m_jobRequest.clientPort, ex);
    } catch(castor::exception::Exception &ex) {
      // Don't rethrow, just log the exception
      castor::dlf::Param params[] = {
        castor::dlf::Param("volReqId", m_jobRequest.volReqId),
        castor::dlf::Param("Message" , ex.getMessage().str()),
        castor::dlf::Param("Code"    , ex.code()            )};
      castor::dlf::dlf_writep(m_cuuid, DLF_LVL_SYSTEM,
        AGGREGATOR_FAILED_TO_NOTIFY_CLIENT_END_OF_FAILED_SESSION, params);
    }
  }
}


//-----------------------------------------------------------------------------
// runDumpSession
//-----------------------------------------------------------------------------
void castor::tape::aggregator::BridgeProtocolEngine::runDumpSession()
  throw(castor::exception::Exception) {

  // Give volume to RTCPD
  RtcpTapeRqstErrMsgBody rtcpVolume;
  utils::setBytes(rtcpVolume, '\0');
  utils::copyString(rtcpVolume.vid    , m_volume.vid().c_str()    );
  utils::copyString(rtcpVolume.vsn    , m_vsn                     );
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
    m_rtcpdInitialSockFd, RTCPDNETRWTIMEOUT, rtcpVolume);

  // Get dump parameters from client

  // Tell RTCPD to dump tape
  RtcpDumpTapeRqstMsgBody request;
  // request.maxBytes      = m_volume.dumpTapeMaxBytes();
  // request.blockSize     = m_volume.dumpTapeBlockSize();
  // request.convert       = m_volume.dumpTapeConverter();
  // request.tapeErrAction = m_volume.dumpTapeErrAction();
  // request.startFile     = m_volume.dumpTapeStartFile();
  // request.maxFiles      = m_volume.dumpTapeMaxFile();
  // request.fromBlock     = m_volume.dumpTapeFromBlock();
  // request.toBlock       = m_volume.dumpTapeToBlock();
  RtcpTxRx::tellRtcpdDumpTape(m_cuuid, m_jobRequest.volReqId,
    m_rtcpdInitialSockFd, RTCPDNETRWTIMEOUT, request);

  // Tell RTCPD end of file list
  RtcpTxRx::tellRtcpdEndOfFileList(m_cuuid, m_jobRequest.volReqId,
    m_rtcpdInitialSockFd, RTCPDNETRWTIMEOUT);

  try {
    processRtcpdSocks();

    try {
      GatewayTxRx::notifyGatewayEndOfSession(m_cuuid, m_jobRequest.volReqId,
        m_jobRequest.clientHost, m_jobRequest.clientPort);
    } catch(castor::exception::Exception &ex) {
      // Don't rethrow, just log the exception
      castor::dlf::Param params[] = {
        castor::dlf::Param("volReqId", m_jobRequest.volReqId),
        castor::dlf::Param("Message" , ex.getMessage().str()),
        castor::dlf::Param("Code"    , ex.code()            )};
      castor::dlf::dlf_writep(m_cuuid, DLF_LVL_SYSTEM,
        AGGREGATOR_FAILED_TO_NOTIFY_CLIENT_END_OF_SESSION, params);
    }
  } catch(castor::exception::Exception &ex) {
    castor::dlf::Param params[] = {
      castor::dlf::Param("volReqId", m_jobRequest.volReqId),
      castor::dlf::Param("Message" , ex.getMessage().str()),
      castor::dlf::Param("Code"    , ex.code()            )};                       castor::dlf::dlf_writep(m_cuuid, DLF_LVL_SYSTEM,
      AGGREGATOR_FAILED_TO_PROCESS_RTCPD_SOCKETS, params);

    try {
      GatewayTxRx::notifyGatewayEndOfFailedSession(m_cuuid,
        m_jobRequest.volReqId, m_jobRequest.clientHost,
        m_jobRequest.clientPort, ex);
    } catch(castor::exception::Exception &ex) {
      // Don't rethrow, just log the exception
      castor::dlf::Param params[] = {
        castor::dlf::Param("volReqId", m_jobRequest.volReqId),
        castor::dlf::Param("Message" , ex.getMessage().str()),
        castor::dlf::Param("Code"    , ex.code()            )};
      castor::dlf::dlf_writep(m_cuuid, DLF_LVL_SYSTEM,
        AGGREGATOR_FAILED_TO_NOTIFY_CLIENT_END_OF_FAILED_SESSION, params);
    }
  }
}


//-----------------------------------------------------------------------------
// processRtcpdRequest
//-----------------------------------------------------------------------------
void castor::tape::aggregator::BridgeProtocolEngine::processRtcpdRequest(
  const MessageHeader &header, const int socketFd, bool &receivedENDOF_REQ)
  throw(castor::exception::Exception) {

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
      castor::dlf::Param("volReqId"   , m_jobRequest.volReqId),
      castor::dlf::Param("socketFd"   , socketFd             ),
      castor::dlf::Param("magic"      , magicHex             ),
      castor::dlf::Param("magicName"  , magicName            ),
      castor::dlf::Param("reqType"    , reqTypeHex           ),
      castor::dlf::Param("reqTypeName", reqTypeName          ),
      castor::dlf::Param("len"        , header.lenOrStatus   )};
    castor::dlf::dlf_writep(m_cuuid, DLF_LVL_SYSTEM,
      AGGREGATOR_PROCESSING_TAPE_DISK_RQST, params);
  }

  // Find the message type's corresponding handler
  MsgBodyCallbackMap::iterator itor = m_handlers.find(
    createHandlerKey(header.magic, header.reqType));
  if(itor == m_handlers.end()) {
    TAPE_THROW_CODE(EBADMSG,
      ": Unknown magic and request type combination"
      ": magic=0x"   << std::hex << header.magic <<
      ": reqType=0x" << header.reqType << std::dec);
  }
  const MsgBodyCallback handler = itor->second;

  // Invoke the handler
  return (this->*handler)(header, socketFd, receivedENDOF_REQ);
}


//-----------------------------------------------------------------------------
// rtcpFileReqCallback
//-----------------------------------------------------------------------------
void castor::tape::aggregator::BridgeProtocolEngine::rtcpFileReqCallback(
  const MessageHeader &header, const int socketFd, bool &receivedENDOF_REQ)
  throw(castor::exception::Exception) {

  RtcpFileRqstMsgBody body;

  RtcpTxRx::receiveMsgBody(m_cuuid, m_jobRequest.volReqId, socketFd,
    RTCPDNETRWTIMEOUT, header, body);

  processRtcpFileReq(header, body, socketFd, receivedENDOF_REQ);
}


//-----------------------------------------------------------------------------
// rtcpFileErrReqCallback
//-----------------------------------------------------------------------------
void castor::tape::aggregator::BridgeProtocolEngine::rtcpFileErrReqCallback(
  const MessageHeader &header, const int socketFd, bool &receivedENDOF_REQ)
  throw(castor::exception::Exception) {

  RtcpFileRqstErrMsgBody body;

  RtcpTxRx::receiveMsgBody(m_cuuid, m_jobRequest.volReqId, socketFd,
    RTCPDNETRWTIMEOUT, header, body);

  // If RTCPD has reported an error
  if(body.err.errorCode != 0) {

    TAPE_THROW_CODE(body.err.errorCode,
      ": Received an error from RTCPD: " << body.err.errorMsg);
  }
  
  processRtcpFileReq(header, body, socketFd, receivedENDOF_REQ);
}


//-----------------------------------------------------------------------------
// processRtcpFileReq
//-----------------------------------------------------------------------------
void castor::tape::aggregator::BridgeProtocolEngine::processRtcpFileReq(
  const MessageHeader &header, RtcpFileRqstMsgBody &body, const int socketFd,
  bool &receivedENDOF_REQ) throw(castor::exception::Exception) {

  // If the tape is being dumped
  if(m_volume.mode() == tapegateway::DUMP) {
    // Send an acknowledge to RTCPD
    MessageHeader ackMsg;
    ackMsg.magic       = header.magic;
    ackMsg.reqType     = header.reqType;
    ackMsg.lenOrStatus = 0;
    RtcpTxRx::sendMessageHeader(m_cuuid, m_jobRequest.volReqId, socketFd,
      RTCPDNETRWTIMEOUT, ackMsg);

    return;
  }

  switch(body.procStatus) {
  case RTCP_REQUEST_MORE_WORK:
    // If migrating
    if(m_volume.mode() == tapegateway::WRITE) {

      char filePath[CA_MAXPATHLEN+1];
      char nsHost[CA_MAXHOSTNAMELEN+1];
      char tapePath[CA_MAXPATHLEN+1];
      char lastKnownFilename[CA_MAXPATHLEN+1];
      unsigned char blockId[4];

      utils::setBytes(filePath         , '\0');
      utils::setBytes(nsHost           , '\0');
      utils::setBytes(tapePath         , '\0');
      utils::setBytes(lastKnownFilename, '\0');
      utils::setBytes(blockId          , '\0');

      uint64_t fileTransactionId    = 0;
      uint64_t fileId               = 0;
      int32_t  tapeFseq             = 0;
      uint64_t fileSize             = 0;
      uint64_t lastModificationTime = 0;
      int32_t  positionMethod       = 0;

      // If there is a file to migrate
      if(GatewayTxRx::getFileToMigrateFromGateway(m_cuuid,
        m_jobRequest.volReqId, m_jobRequest.clientHost,
        m_jobRequest.clientPort, fileTransactionId, filePath, nsHost,
        fileId, tapeFseq, fileSize, lastKnownFilename, lastModificationTime,
        positionMethod)) {

        // Remember the file transaction ID and get its unique index to be
        // passed to RTCPD through the "rtcpFileRequest.disk_fseq" message
        // field
        const int diskFseq = m_pendingTransferIds.insert(fileTransactionId);

        char tapeFileId[CA_MAXPATHLEN+1];
        utils::toHex(fileId, tapeFileId);
        RtcpTxRx::giveFileToRtcpd(m_cuuid, m_jobRequest.volReqId, socketFd,
          RTCPDNETRWTIMEOUT, WRITE_ENABLE, filePath, fileSize, "", RECORDFORMAT,
          tapeFileId, MIGRATEUMASK, positionMethod, tapeFseq, diskFseq, nsHost,
          fileId, blockId);

        RtcpTxRx::askRtcpdToRequestMoreWork(m_cuuid, m_jobRequest.volReqId,
          tapePath, socketFd, RTCPDNETRWTIMEOUT, WRITE_ENABLE);

        RtcpTxRx::tellRtcpdEndOfFileList(m_cuuid, m_jobRequest.volReqId,
          socketFd, RTCPDNETRWTIMEOUT);

      // Else there is no file to migrate
      } else {

        // Tell RTCPD there is no file by sending an empty file list
        RtcpTxRx::tellRtcpdEndOfFileList(m_cuuid, m_jobRequest.volReqId,
          socketFd, RTCPDNETRWTIMEOUT);
      }

    // Else recalling (READ or DUMP)
    } else {

      char filePath[CA_MAXPATHLEN+1];
      char nsHost[CA_MAXHOSTNAMELEN+1];
      unsigned char blockId[4];

      utils::setBytes(filePath, '\0');
      utils::setBytes(nsHost  , '\0');
      utils::setBytes(blockId , '\0');

      uint64_t fileTransactionId   = 0;
      uint64_t fileId              = 0;
      int32_t  tapeFseq            = 0;
      int32_t  positionCommandCode = 0;

      // If there is a file to recall
      if(GatewayTxRx::getFileToRecallFromGateway(m_cuuid,
        m_jobRequest.volReqId, m_jobRequest.clientHost,
        m_jobRequest.clientPort, fileTransactionId, filePath, nsHost,
        fileId, tapeFseq, blockId, positionCommandCode)) {

        // Remember the file transaction ID and get its unique index to be
        // passed to RTCPD through the "rtcpFileRequest.disk_fseq" message
        // field
        const int diskFseq = m_pendingTransferIds.insert(fileTransactionId);

        // The file size is not specified when recalling
        const uint64_t fileSize = 0;

        char tapeFileId[CA_MAXPATHLEN+1];
        utils::setBytes(tapeFileId, '\0');
        RtcpTxRx::giveFileToRtcpd(m_cuuid, m_jobRequest.volReqId, socketFd,
          RTCPDNETRWTIMEOUT, WRITE_DISABLE, filePath, fileSize, body.tapePath,
          RECORDFORMAT, tapeFileId, RECALLUMASK, positionCommandCode, tapeFseq,
          diskFseq, nsHost, fileId, blockId);

        RtcpTxRx::askRtcpdToRequestMoreWork(m_cuuid, m_jobRequest.volReqId,
          body.tapePath, socketFd, RTCPDNETRWTIMEOUT, WRITE_DISABLE);

        RtcpTxRx::tellRtcpdEndOfFileList(m_cuuid, m_jobRequest.volReqId,
          socketFd, RTCPDNETRWTIMEOUT);

      // Else there is no file to recall
      } else {

        castor::dlf::Param params[] = {
          castor::dlf::Param("volReqId"   , m_jobRequest.volReqId  ),
          castor::dlf::Param("gatewayHost", m_jobRequest.clientHost),
          castor::dlf::Param("gatewayPort", m_jobRequest.clientPort)};
        castor::dlf::dlf_writep(m_cuuid, DLF_LVL_SYSTEM,
          AGGREGATOR_NO_MORE_FILES_TO_RECALL, params);

        // Tell RTCPD there is no file by sending an empty file list
        RtcpTxRx::tellRtcpdEndOfFileList(m_cuuid, m_jobRequest.volReqId,
          socketFd, RTCPDNETRWTIMEOUT);
      }
    } // Else recalling

    {
      castor::dlf::Param params[] = {
        castor::dlf::Param("volReqId", m_jobRequest.volReqId),
        castor::dlf::Param("socketFd", socketFd             )};
      castor::dlf::dlf_writep(m_cuuid, DLF_LVL_SYSTEM,
        AGGREGATOR_SEND_DELAYED_REQUEST_MORE_WORK_ACK_TO_RTCPD, params);
    }

    // Send delayed acknowledge of the request for more work
    MessageHeader ackMsg;
    ackMsg.magic       = header.magic;
    ackMsg.reqType     = header.reqType;
    ackMsg.lenOrStatus = 0;
    RtcpTxRx::sendMessageHeader(m_cuuid, m_jobRequest.volReqId, socketFd,
      RTCPDNETRWTIMEOUT,ackMsg);

    {
      castor::dlf::Param params[] = {
        castor::dlf::Param("volReqId", m_jobRequest.volReqId),
        castor::dlf::Param("socketFd", socketFd             )};
      castor::dlf::dlf_writep(m_cuuid, DLF_LVL_SYSTEM,
        AGGREGATOR_SENT_DELAYED_REQUEST_MORE_WORK_ACK_TO_RTCPD, params);
    }

    break;
  case RTCP_POSITIONED:
    {
      castor::dlf::Param params[] = {
        castor::dlf::Param("volReqId", m_jobRequest.volReqId),
        castor::dlf::Param("filePath", body.filePath        ),
        castor::dlf::Param("tapePath", body.tapePath        )};
      castor::dlf::dlf_writep(m_cuuid, DLF_LVL_SYSTEM,
        AGGREGATOR_TAPE_POSITIONED, params);

      // Send an acknowledge to RTCPD
      MessageHeader ackMsg;
      ackMsg.magic       = header.magic;
      ackMsg.reqType     = header.reqType;
      ackMsg.lenOrStatus = 0;
      RtcpTxRx::sendMessageHeader(m_cuuid, m_jobRequest.volReqId, socketFd,
        RTCPDNETRWTIMEOUT, ackMsg);
    }
    break;
  case RTCP_FINISHED:
    {
      castor::dlf::Param params[] = {
        castor::dlf::Param("volReqId", m_jobRequest.volReqId),
        castor::dlf::Param("filePath", body.filePath        ),
        castor::dlf::Param("tapePath", body.tapePath        ),
        castor::dlf::Param("tapeFseq", body.tapeFseq        ),
        castor::dlf::Param("diskFseq", body.diskFseq        ),
        castor::dlf::Param("bytesIn" , body.bytesIn         ),
        castor::dlf::Param("bytesOut", body.bytesOut        )};
      castor::dlf::dlf_writep(m_cuuid, DLF_LVL_SYSTEM,
        AGGREGATOR_FILE_TRANSFERED, params);

      // Send an acknowledge to RTCPD
      MessageHeader ackMsg;
      ackMsg.magic       = header.magic;
      ackMsg.reqType     = header.reqType;
      ackMsg.lenOrStatus = 0;
      RtcpTxRx::sendMessageHeader(m_cuuid, m_jobRequest.volReqId, socketFd,
        RTCPDNETRWTIMEOUT, ackMsg);

      // Get the file transaction ID that was sent by the tape gateway
      const uint64_t fileTransactonId =
        m_pendingTransferIds.remove(body.diskFseq);

      // Notify the tape gateway
      // If migrating
      if(m_volume.mode() == tapegateway::WRITE) {
        const uint64_t fileSize           = body.bytesIn; // "in" to the tape
        const uint64_t compressedFileSize = fileSize; // Ignore compression

        GatewayTxRx::notifyGatewayFileMigrated(m_cuuid, m_jobRequest.volReqId,
          m_jobRequest.clientHost, m_jobRequest.clientPort, fileTransactonId,
          body.segAttr.nameServerHostName, body.segAttr.castorFileId,
          body.tapeFseq, body.blockId, body.positionMethod,
          body.segAttr.segmCksumAlgorithm, body.segAttr.segmCksum, fileSize,
          compressedFileSize);

      // Else recall (READ or DUMP)
      } else {
        const uint64_t fileSize = body.bytesOut; // "out" from the tape

        GatewayTxRx::notifyGatewayFileRecalled(m_cuuid, m_jobRequest.volReqId,
          m_jobRequest.clientHost, m_jobRequest.clientPort, fileTransactonId,
          body.segAttr.nameServerHostName, body.segAttr.castorFileId,
          body.tapeFseq, body.filePath, body.positionMethod,
          body.segAttr.segmCksumAlgorithm, body.segAttr.segmCksum, fileSize);
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
// rtcpTapeReqCallback
//-----------------------------------------------------------------------------
void castor::tape::aggregator::BridgeProtocolEngine::rtcpTapeReqCallback(
  const MessageHeader &header, const int socketFd, bool &receivedENDOF_REQ)
  throw(castor::exception::Exception) {

  RtcpTapeRqstMsgBody body;

  RtcpTxRx::receiveMsgBody(m_cuuid, m_jobRequest.volReqId, socketFd,
    RTCPDNETRWTIMEOUT, header, body);

  return(processRtcpTape(header, body, socketFd, receivedENDOF_REQ));
}


//-----------------------------------------------------------------------------
// rtcpTapeErrReqCallback
//-----------------------------------------------------------------------------
void
  castor::tape::aggregator::BridgeProtocolEngine::rtcpTapeErrReqCallback(
  const MessageHeader &header, const int socketFd, bool &receivedENDOF_REQ)
  throw(castor::exception::Exception) {

  RtcpTapeRqstErrMsgBody body;

  RtcpTxRx::receiveMsgBody(m_cuuid, m_jobRequest.volReqId, socketFd,
    RTCPDNETRWTIMEOUT, header, body);

  if(body.err.errorCode != 0) {
    char codeStr[STRERRORBUFLEN];
    sstrerror_r(body.err.errorCode, codeStr, sizeof(codeStr));

    TAPE_THROW_CODE(body.err.errorCode,
      ": Received an error from RTCPD"
      ": errorCode=" << body.err.errorCode <<
      ": errorMsg=" << body.err.errorMsg  <<
      ": sstrerror_r=" << codeStr);
  }

  return(processRtcpTape(header, body, socketFd, receivedENDOF_REQ));
}


//-----------------------------------------------------------------------------
// processRtcpTape
//-----------------------------------------------------------------------------
void castor::tape::aggregator::BridgeProtocolEngine::processRtcpTape(
  const MessageHeader &header, RtcpTapeRqstMsgBody &body, const int socketFd,
  bool &receivedENDOF_REQ) throw(castor::exception::Exception) {

  // Acknowledge tape request
  MessageHeader ackMsg;
  ackMsg.magic       = RTCOPY_MAGIC;
  ackMsg.reqType     = RTCP_TAPE_REQ;
  ackMsg.lenOrStatus = 0;
  RtcpTxRx::sendMessageHeader(m_cuuid, m_jobRequest.volReqId, socketFd,
    RTCPDNETRWTIMEOUT, ackMsg);
}


//-----------------------------------------------------------------------------
// rtcpEndOfReqCallback
//-----------------------------------------------------------------------------
void castor::tape::aggregator::BridgeProtocolEngine::rtcpEndOfReqCallback(
  const MessageHeader &header, const int socketFd, bool &receivedENDOF_REQ)
  throw(castor::exception::Exception) {

  receivedENDOF_REQ = true;

  // Acknowledge RTCP_ENDOF_REQ message
  MessageHeader ackMsg;
  ackMsg.magic       = RTCOPY_MAGIC;
  ackMsg.reqType     = RTCP_ENDOF_REQ;
  ackMsg.lenOrStatus = 0;
  RtcpTxRx::sendMessageHeader(m_cuuid, m_jobRequest.volReqId, socketFd,
    RTCPDNETRWTIMEOUT, ackMsg);
}


//-----------------------------------------------------------------------------
// giveOutpCallback
//-----------------------------------------------------------------------------
void castor::tape::aggregator::BridgeProtocolEngine::giveOutpCallback(
  const MessageHeader &header, const int socketFd, bool &receivedENDOF_REQ)
  throw(castor::exception::Exception) {

  GiveOutpMsgBody body;

  RtcpTxRx::receiveMsgBody(m_cuuid, m_jobRequest.volReqId, socketFd,
    RTCPDNETRWTIMEOUT, header, body);

  GatewayTxRx::notifyGatewayDumpMessage(m_cuuid, m_jobRequest.volReqId,
    m_jobRequest.clientHost, m_jobRequest.clientPort, body.message);
}
