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
#include "castor/tape/aggregator/AggregatorDlfMessageConstants.hpp"
#include "castor/tape/aggregator/BridgeProtocolEngine.hpp"
#include "castor/tape/aggregator/Constants.hpp"
#include "castor/tape/aggregator/GatewayTxRx.hpp"
#include "castor/tape/aggregator/Net.hpp"
#include "castor/tape/aggregator/RtcpTxRx.hpp"
#include "castor/tape/aggregator/SmartFd.hpp"
#include "castor/tape/aggregator/SmartFdList.hpp"
#include "castor/tape/aggregator/TapeDiskRqstHandler.hpp"
#include "castor/tape/aggregator/Utils.hpp"
#include "h/Ctape_constants.h"
#include "h/rtcp_constants.h"

#include <list>


//-----------------------------------------------------------------------------
// processErrorOnInitialRtcpdConnection
//-----------------------------------------------------------------------------
void castor::tape::aggregator::BridgeProtocolEngine::
  processErrorOnInitialRtcpdConnection(const Cuuid_t &cuuid,
  const uint32_t volReqId, const int socketFd)
  throw(castor::exception::Exception) {

  {
    castor::dlf::Param params[] = {
      castor::dlf::Param("volReqId", volReqId),
      castor::dlf::Param("socketFd", socketFd)};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM,
      AGGREGATOR_DATA_ON_INITIAL_RTCPD_CONNECTION, params);
  }

  MessageHeader          header;
  RtcpTapeRqstErrMsgBody body;

  RtcpTxRx::receiveRtcpMsgHeader(cuuid, volReqId, socketFd,
    RTCPDNETRWTIMEOUT, header);
  RtcpTxRx::receiveRtcpTapeRqstErrBody(cuuid, volReqId, socketFd,
    RTCPDNETRWTIMEOUT, header, body);

  RtcpAcknowledgeMsg ackMsg;
  ackMsg.magic   = RTCOPY_MAGIC;
  ackMsg.reqType = RTCP_TAPEERR_REQ;
  ackMsg.status  = 0;
  RtcpTxRx::sendRtcpAcknowledge(cuuid, volReqId, socketFd, RTCPDNETRWTIMEOUT,
    ackMsg);

  TAPE_THROW_CODE(body.err.errorCode,
    ": Received an error from RTCPD:" << body.err.errorMsg);
}


//-----------------------------------------------------------------------------
// acceptRtcpdConnection
//-----------------------------------------------------------------------------
int castor::tape::aggregator::BridgeProtocolEngine::
  acceptRtcpdConnection(const Cuuid_t &cuuid, const uint32_t volReqId,
  const int rtcpdCallbackSocketFd) throw(castor::exception::Exception) {

  SmartFd connectedSocketFd(Net::acceptConnection(rtcpdCallbackSocketFd,
    RTCPDPINGTIMEOUT));

  try {
    unsigned short port = 0; // Client port
    unsigned long  ip   = 0; // Client IP
    char           hostName[HOSTNAMEBUFLEN];

    Net::getPeerIpAndPort(connectedSocketFd.get(), ip, port);
    Net::getPeerHostName(connectedSocketFd.get(), hostName);

    castor::dlf::Param params[] = {
      castor::dlf::Param("volReqId", volReqId                  ),
      castor::dlf::Param("IP"      , castor::dlf::IPAddress(ip)),
      castor::dlf::Param("Port"    , port                      ),
      castor::dlf::Param("HostName", hostName                  ),
      castor::dlf::Param("socketFd", connectedSocketFd.get()   )};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM,
      AGGREGATOR_RTCPD_CALLBACK_WITH_INFO, params);
  } catch(castor::exception::Exception &ex) {
    castor::dlf::Param params[] = {
      castor::dlf::Param("volReqId", volReqId)};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM,
      AGGREGATOR_RTCPD_CALLBACK_WITHOUT_INFO, params);
  }

  return connectedSocketFd.release();
}


//-----------------------------------------------------------------------------
// processRtcpdSockets
//-----------------------------------------------------------------------------
void castor::tape::aggregator::BridgeProtocolEngine::
  processRtcpdSockets(const Cuuid_t &cuuid, const uint32_t volReqId,
  const char (&gatewayHost)[CA_MAXHOSTNAMELEN+1],
  const unsigned short gatewayPort, const uint32_t mode,
  const int rtcpdCallbackSocketFd, const int rtcpdInitialSocketFd)
  throw(castor::exception::Exception) {

  SmartFdList connectedSocketFds;
  int selectRc = 0;
  int selectErrno = 0;
  fd_set readFdSet;
  int maxFd = 0;
  timeval timeout;
  TapeDiskRqstHandler tapeDiskRqstHandler;

  try {
    // Select loop
    bool continueMainSelectLoop = true;
    while(continueMainSelectLoop) {

      // Build the file descriptor set ready for the select call
      FD_ZERO(&readFdSet);
      FD_SET(rtcpdCallbackSocketFd, &readFdSet);
      FD_SET(rtcpdInitialSocketFd, &readFdSet);
      if(rtcpdCallbackSocketFd > rtcpdInitialSocketFd) {
        maxFd = rtcpdCallbackSocketFd;
      } else {
        maxFd = rtcpdInitialSocketFd;
      }
      for(std::list<int>::iterator itor = connectedSocketFds.begin();
        itor != connectedSocketFds.end(); itor++) {

        FD_SET(*itor, &readFdSet);

        if(*itor > maxFd) {
          maxFd = *itor;
        }
      }

      timeout.tv_sec  = RTCPDPINGTIMEOUT;
      timeout.tv_usec = 0;

      selectRc = select(maxFd + 1, &readFdSet, NULL, NULL, &timeout);
      selectErrno = errno;

      switch(selectRc) {
      case 0: // Select timed out

        RtcpTxRx::pingRtcpd(cuuid, volReqId, rtcpdInitialSocketFd,
          RTCPDNETRWTIMEOUT);
        break;

      case -1: // Select encountered an error

        // If select encountered an error other than an interruption
        if(selectErrno != EINTR) {
          char strerrorBuf[STRERRORBUFLEN];
          char *const errorStr = strerror_r(selectErrno, strerrorBuf,
            sizeof(strerrorBuf));

          TAPE_THROW_CODE(selectErrno,
            ": Select encountered an error other than an interruption"
            ": " << errorStr);
        }
        break;

      default: // One or more select file descriptors require attention

        int nbProcessed = 0;

        // While all waiting file descriptors have not been processed
        while(nbProcessed < selectRc) {

          // If there is an incoming message on the initial RTCPD connection
          if(FD_ISSET(rtcpdInitialSocketFd, &readFdSet)) {
            FD_CLR(rtcpdInitialSocketFd, &readFdSet);

            processErrorOnInitialRtcpdConnection(cuuid, volReqId,
              rtcpdInitialSocketFd);

            nbProcessed++;
          }

          // if there is a callback connection request from RTCPD
          if(FD_ISSET(rtcpdCallbackSocketFd, &readFdSet)) {
            FD_CLR(rtcpdCallbackSocketFd, &readFdSet);

            connectedSocketFds.push_back(acceptRtcpdConnection(cuuid,
              volReqId, rtcpdCallbackSocketFd));

            nbProcessed++;
          }

          // For each connected socket or until all waiting file descriptors
          // have been processed
          for(std::list<int>::iterator itor=connectedSocketFds.begin();
            itor != connectedSocketFds.end() && nbProcessed < selectRc;
            itor++) {

            if(FD_ISSET(*itor, &readFdSet)) {
              FD_CLR(*itor, &readFdSet);

              continueMainSelectLoop = tapeDiskRqstHandler.processRequest(
                cuuid, volReqId, gatewayHost, gatewayPort, mode, *itor);

              nbProcessed++;
            }
          }
        } // While all waiting file descriptors have not been processed
      }
    }
  } catch(castor::exception::Exception &ex) {
    castor::dlf::Param params[] = {
      castor::dlf::Param("volReqId", volReqId             ),
      castor::dlf::Param("Message" , ex.getMessage().str()),
      castor::dlf::Param("Code"    , ex.code()            )};
    CASTOR_DLF_WRITEPC(cuuid, DLF_LVL_ERROR, AGGREGATOR_MAIN_SELECT_FAILED,
      params);
  }
}


//-----------------------------------------------------------------------------
// run
//-----------------------------------------------------------------------------
void castor::tape::aggregator::BridgeProtocolEngine::run(const Cuuid_t &cuuid,
  const uint32_t volReqId, const char (&gatewayHost)[CA_MAXHOSTNAMELEN+1],
  const unsigned short gatewayPort, const int rtcpdCallbackSocketFd,
  const int rtcpdInitialSocketFd, const uint32_t mode,
  char (&unit)[CA_MAXUNMLEN+1], const char (&vid)[CA_MAXVIDLEN+1],
  char (&vsn)[CA_MAXVSNLEN+1], const char (&label)[CA_MAXLBLTYPLEN+1],
  const char (&density)[CA_MAXDENLEN+1]) throw(castor::exception::Exception) {

  RtcpFileRqstErrMsgBody rtcpFileReply;
  Utils::setBytes(rtcpFileReply, '\0');

  // Allocate some variables on the stack for information about a possible file
  // to migrate.  These variables will not be used in the case of a recall
  char migrationFilePath[CA_MAXPATHLEN+1];
  Utils::setBytes(migrationFilePath, '\0');
  char migrationFileNsHost[CA_MAXHOSTNAMELEN+1];
  Utils::setBytes(migrationFileNsHost, '\0');
  uint64_t migrationFileId = 0;
  uint32_t migrationFileTapeFseq = 0;
  uint64_t migrationFileSize = 0;
  char migrationFileLastKnownFileName[CA_MAXPATHLEN+1];
  Utils::setBytes(migrationFileLastKnownFileName, '\0');
  uint64_t migrationFileLastModificationTime = 0;
  int32_t positionCommandCode = 0;

  // If migrating
  if(mode == WRITE_ENABLE) {

    // Get first file to migrate from tape gateway
    const bool thereIsAFileToMigrate =
      GatewayTxRx::getFileToMigrateFromGateway(cuuid, volReqId, gatewayHost,
        gatewayPort, migrationFilePath, migrationFileNsHost, migrationFileId,
        migrationFileTapeFseq, migrationFileSize,
        migrationFileLastKnownFileName, migrationFileLastModificationTime,
        positionCommandCode);

    // Return if there is no file to migrate
    if(!thereIsAFileToMigrate) {
      return;
    }
  }

  // Give volume to RTCPD
  RtcpTapeRqstErrMsgBody rtcpVolume;
  Utils::setBytes(rtcpVolume, '\0');
  Utils::copyString(rtcpVolume.vid    , vid    );
  Utils::copyString(rtcpVolume.vsn    , vsn    );
  Utils::copyString(rtcpVolume.label  , label  );
  Utils::copyString(rtcpVolume.density, density);
  Utils::copyString(rtcpVolume.unit   , unit   );
  rtcpVolume.volReqId       = volReqId;
  rtcpVolume.mode           = mode;
  rtcpVolume.tStartRequest  = time(NULL);
  rtcpVolume.err.severity   =  1;
  rtcpVolume.err.maxTpRetry = -1;
  rtcpVolume.err.maxCpRetry = -1;
  RtcpTxRx::giveVolumeToRtcpd(cuuid, volReqId, rtcpdInitialSocketFd,
    RTCPDNETRWTIMEOUT, rtcpVolume);

  // If migrating
  if(mode == WRITE_ENABLE) {

    // Give file to migrate to RTCPD
    char migrationTapeFileId[CA_MAXPATHLEN+1];
    Utils::toHex(migrationFileId, migrationTapeFileId);
    RtcpTxRx::giveFileToRtcpd(cuuid, volReqId, rtcpdInitialSocketFd,
      RTCPDNETRWTIMEOUT, rtcpVolume.mode, migrationFilePath, "", RECORDFORMAT,
      migrationTapeFileId, MIGRATEUMASK, positionCommandCode,
      migrationFileNsHost, migrationFileId);
  }

  // Ask RTCPD to request more work
  RtcpTxRx::askRtcpdToRequestMoreWork(cuuid, volReqId, rtcpdInitialSocketFd,
    RTCPDNETRWTIMEOUT, mode);

  // Tell RTCPD end of file list
  RtcpTxRx::tellRtcpdEndOfFileList(cuuid, volReqId, rtcpdInitialSocketFd,
    RTCPDNETRWTIMEOUT);

  // Spin a select loop processing the RTCPD sockets
  processRtcpdSockets(cuuid, volReqId, gatewayHost, gatewayPort, mode,
    rtcpdCallbackSocketFd, rtcpdInitialSocketFd);
}
