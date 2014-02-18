/******************************************************************************
 *                castor/tape/tapebridge/VdqmRequestHandler.cpp
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

#include "castor/PortNumbers.hpp"
#include "castor/dlf/Dlf.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/exception/Internal.hpp"
#include "castor/tape/tapebridge/DlfMessageConstants.hpp"
#include "castor/tape/tapebridge/BridgeClientInfo2Sender.hpp"
#include "castor/tape/tapebridge/BridgeProtocolEngine.hpp"
#include "castor/tape/tapebridge/ClientAddressTcpIp.hpp"
#include "castor/tape/tapebridge/ClientProxy.hpp"
#include "castor/tape/tapebridge/Constants.hpp"
#include "castor/tape/tapebridge/LegacyTxRx.hpp"
#include "castor/tape/tapebridge/RtcpJobSubmitter.hpp"
#include "castor/tape/tapebridge/RtcpTxRx.hpp"
#include "castor/tape/tapebridge/SystemFileCloser.hpp"
#include "castor/tape/tapebridge/VdqmRequestHandler.hpp"
#include "castor/tape/tapebridge/VmgrTxRx.hpp"
#include "castor/tape/legacymsg/RtcpMarshal.hpp"
#include "castor/tape/legacymsg/VmgrMarshal.hpp"
#include "castor/tape/net/net.hpp"
#include "castor/tape/tapegateway/FileMigrationReportList.hpp"
#include "castor/tape/tapegateway/FileRecallReportList.hpp"
#include "castor/tape/tapegateway/FileErrorReportStruct.hpp"
#include "castor/tape/tapegateway/VolumeRequest.hpp"
#include "castor/tape/tapegateway/Volume.hpp"
#include "castor/tape/utils/SmartFdList.hpp"
#include "castor/tape/utils/utils.hpp"
#include "castor/utils/SmartFd.hpp"
#include "castor/utils/utils.hpp"
#include "h/common.h"
#include "h/Ctape_constants.h"
#include "h/rtcp_constants.h"
#include "h/tapebridge_constants.h"
#include "h/tapeBridgeClientInfo2MsgBody.h"
#include "h/u64subr.h"
#include "h/vdqm_constants.h"
#include "h/vmgr_constants.h"

#include <algorithm>
#include <memory>
#include <string.h>


//-----------------------------------------------------------------------------
// s_stoppingGracefully
//-----------------------------------------------------------------------------
bool castor::tape::tapebridge::VdqmRequestHandler::s_stoppingGracefully = false;


//-----------------------------------------------------------------------------
// s_stoppingGracefullyFunctor
//-----------------------------------------------------------------------------
castor::tape::tapebridge::VdqmRequestHandler::StoppingGracefullyFunctor
  castor::tape::tapebridge::VdqmRequestHandler::s_stoppingGracefullyFunctor;


//-----------------------------------------------------------------------------
// constructor
//-----------------------------------------------------------------------------
castor::tape::tapebridge::VdqmRequestHandler::VdqmRequestHandler(
  const BulkRequestConfigParams &bulkRequestConfigParams,
  const TapeFlushConfigParams   &tapeFlushConfigParams,
  const uint32_t                nbDrives)
  throw() :
  m_bulkRequestConfigParams(bulkRequestConfigParams),
  m_tapeFlushConfigParams(tapeFlushConfigParams),
  m_nbDrives(nbDrives) {
}


//-----------------------------------------------------------------------------
// destructor
//-----------------------------------------------------------------------------
castor::tape::tapebridge::VdqmRequestHandler::~VdqmRequestHandler()
  throw() {
}


//-----------------------------------------------------------------------------
// init
//-----------------------------------------------------------------------------
void castor::tape::tapebridge::VdqmRequestHandler::init() throw() {
}


//-----------------------------------------------------------------------------
// run
//-----------------------------------------------------------------------------
void castor::tape::tapebridge::VdqmRequestHandler::run(void *param)
  throw() {

  // Give a Cuuid to the request
  Cuuid_t cuuid = nullCuuid;
  Cuuid_create(&cuuid);

  // Check function-parameters
  if(param == NULL) {
    castor::dlf::Param params[] = {
      castor::dlf::Param("Message", "VDQM request handler socket is NULL"),
      castor::dlf::Param("Code"   , EINVAL                               )};
    CASTOR_DLF_WRITEPC(cuuid, DLF_LVL_ERROR,
      TAPEBRIDGE_TRANSFER_FAILED, params);
    return;
  }

  // Wrap the VDQM connection socket within a smart file-descriptor.  When the
  // smart file-descriptor goes out of scope it will close file-descriptor of
  // the socket.
  castor::utils::SmartFd vdqmSock;
  {
    castor::io::ServerSocket *tmpServerSocket =
      (castor::io::ServerSocket*)param;

    vdqmSock.reset(tmpServerSocket->socket());
    tmpServerSocket->resetSocket();
    delete(tmpServerSocket);
    param = NULL; /* Will cause a segementation fault if used by accident */
  }

  // Job request to be received from VDQM
  legacymsg::RtcpJobRqstMsgBody jobRequest;
  utils::setBytes(jobRequest, '\0');

  // Receive the job request from the VDQM
  try {

    // Log the connection from the VDQM
    {
      char hostName[net::HOSTNAMEBUFLEN];
      const net::IpAndPort peerIpAndPort = net::getPeerIpPort(vdqmSock.get());
      net::getPeerHostName(vdqmSock.get(), hostName);

      castor::dlf::Param params[] = {
        castor::dlf::Param("IP"      ,
          castor::dlf::IPAddress(peerIpAndPort.getIp())),
        castor::dlf::Param("Port"    , peerIpAndPort.getPort()),
        castor::dlf::Param("HostName", hostName               ),
        castor::dlf::Param("socketFd", vdqmSock.get()         )};
      castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM,
        TAPEBRIDGE_RECEIVED_VDQM_CONNECTION, params);
    }

    // Check the VDQM (an RTCP job submitter) is authorised
    checkRtcpJobSubmitterIsAuthorised(vdqmSock.get());

    // Receive the RTCOPY job-request from the VDQM
    RtcpTxRx::receiveRtcpJobRqst(cuuid, vdqmSock.get(), RTCPDNETRWTIMEOUT,
      jobRequest);

  } catch(castor::exception::Exception &ex) {

    castor::dlf::Param params[] = {
      castor::dlf::Param("Message", ex.getMessage().str()),
      castor::dlf::Param("Code"   , ex.code()            )};
    CASTOR_DLF_WRITEPC(cuuid, DLF_LVL_ERROR,
      TAPEBRIDGE_TRANSFER_FAILED, params);

    // Return
    return;
  }

  // Open a new try block where the catch statements can now log the
  // job-request details in case an exception is thrown
  try {
    // Parse the TPCONFIG file
    utils::TpconfigLines tpconfigLines;
    utils::parseTpconfigFile(TPCONFIGPATH, tpconfigLines);

    // Extract the drive-unit names
    std::list<std::string> driveNames;
    utils::extractTpconfigDriveNames(tpconfigLines, driveNames);
    std::stringstream driveNamesStream;
    for(std::list<std::string>::const_iterator itor = driveNames.begin();
      itor != driveNames.end(); itor++) {

      if(itor != driveNames.begin()) {
        driveNamesStream << ",";
      }

      driveNamesStream << *itor;
    }

    // Log the result of successfully parsing the TPCONFIG file
    castor::dlf::Param params[] = {
      castor::dlf::Param("filename" , TPCONFIGPATH          ),
      castor::dlf::Param("nbDrives" , driveNames.size()     ),
      castor::dlf::Param("unitNames", driveNamesStream.str())};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM,
      TAPEBRIDGE_PARSED_TPCONFIG, params);

    // Check the drive-unit name given by the VDQM against TPCONFIG
    if(std::find(driveNames.begin(), driveNames.end(), jobRequest.driveUnit)
      == driveNames.end()) {

      castor::exception::Exception ex(ECANCELED);
      ex.getMessage() <<
        "Drive unit name from VDQM does not exist in the TPCONFIG file"
        ": tpconfigFilename="  << TPCONFIGPATH <<
        ": vdqmDriveUnitName=" << jobRequest.driveUnit <<
        ": tpconfigUnitNames=" << driveNamesStream.str();;

      throw(ex);
    }

    // Log an error if there are more drives attached to the tape-server than
    // there were just before the VdqmRequestHandlerPool thread-pool was
    // created.
    if(driveNames.size() > m_nbDrives) {
      castor::dlf::Param params[] = {
        castor::dlf::Param("filename"         , TPCONFIGPATH     ),
        castor::dlf::Param("expectedNbDrives" , m_nbDrives       ),
        castor::dlf::Param("actualNbDrives"   , driveNames.size())};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR,
        TAPEBRIDGE_TOO_MANY_DRIVES_IN_TPCONFIG, params);
    }

    // Create, bind and mark a listen socket for RTCPD callback connections.
    //
    // Wrap the socket file-descriptor in a smart file-descriptor so that it is
    // guaranteed to be closed when it goes out of scope.
    const unsigned short lowPort = utils::getPortFromConfig(
      "TAPEBRIDGE", "RTCPDLOWPORT", TAPEBRIDGE_RTCPDLOWPORT);
    const unsigned short highPort = utils::getPortFromConfig(
      "TAPEBRIDGE", "RTCPDHIGHPORT", TAPEBRIDGE_RTCPDHIGHPORT);
    unsigned short chosenPort = 0;
    castor::utils::SmartFd listenSock(net::createListenerSock("127.0.0.1",
      lowPort, highPort, chosenPort));

    // Get and log the IP, host name, port and socket file-descriptor of the
    // callback socket
    unsigned long bridgeCallbackIp = 0;
    char bridgeCallbackHost[net::HOSTNAMEBUFLEN];
    utils::setBytes(bridgeCallbackHost, '\0');
    unsigned short bridgeCallbackPort = 0;
    net::getSockIpHostnamePort(listenSock.get(),
      bridgeCallbackIp, bridgeCallbackHost, bridgeCallbackPort);
    {
      castor::dlf::Param params[] = {
        castor::dlf::Param("mountTransactionId", jobRequest.volReqId),
        castor::dlf::Param("IP"                ,
          dlf::IPAddress(bridgeCallbackIp)),
        castor::dlf::Param("Port"              , bridgeCallbackPort ),
        castor::dlf::Param("HostName"          , bridgeCallbackHost ),
        castor::dlf::Param("socketFd"          , listenSock.get()   )};
      castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM,
        TAPEBRIDGE_CREATED_RTCPD_CALLBACK_PORT, params);
    }

    // Pass a modified version of the job request through to RTCPD, setting the
    // clientHost and clientPort parameters to identify the tape tapebridge as
    // being a proxy for RTCPClientD
    {
      castor::dlf::Param params[] = {
        castor::dlf::Param("volReqId"       , jobRequest.volReqId      ),
        castor::dlf::Param("Port"           , bridgeCallbackPort       ),
        castor::dlf::Param("HostName"       , bridgeCallbackHost       ),
        castor::dlf::Param("clientHost"     , jobRequest.clientHost    ),
        castor::dlf::Param("clientPort"     , jobRequest.clientPort    ),
        castor::dlf::Param("clientUserName" , jobRequest.clientUserName),
        castor::dlf::Param("clientEuid"     , jobRequest.clientEuid    ),
        castor::dlf::Param("clientEgid"     , jobRequest.clientEgid    ),
        castor::dlf::Param("dgn"            , jobRequest.dgn           ),
        castor::dlf::Param("driveUnit"      , jobRequest.driveUnit     )};
      castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM,
        TAPEBRIDGE_SUBMITTING_RTCOPY_JOB_TO_RTCPD, params);
    }

    const legacymsg::RtcpJobReplyMsgBody rtcpdReply = sendClientInfoToRtcpd(
      "localhost", RTCOPY_PORT, RTCPDNETRWTIMEOUT, jobRequest,
      bridgeCallbackHost, bridgeCallbackPort);

    // Throw an exception if RTCPD returned an error message.
    //
    // Note that an error from RTCPD is indicated by checking the size of the
    // error message because the status maybe non-zero even if there is no
    // error.
    if(strlen(rtcpdReply.errorMessage) > 0) {
      castor::exception::Exception ex(ECANCELED);

      ex.getMessage() <<
        "Received an error message from RTCPD"
        ": " << rtcpdReply.errorMessage;

      throw(ex);
    }

    // Send a positive acknowledge to the VDQM
    {
      legacymsg::RtcpJobReplyMsgBody vdqmReply;
      utils::setBytes(vdqmReply, '\0');
      char vdqmReplyBuf[RTCPMSGBUFSIZE];
      const size_t vdqmReplyLen = legacymsg::marshal(vdqmReplyBuf, vdqmReply);
      net::writeBytes(vdqmSock.get(), RTCPDNETRWTIMEOUT, vdqmReplyLen,
        vdqmReplyBuf);
    }

    // Close the connection to the VDQM
    close(vdqmSock.release());

    // Create the tapebridge transaction ID
    Counter<uint64_t> tapebridgeTransactionCounter(0);

    // Perform the rest of the thread's work
    exceptionThrowingRun(cuuid, jobRequest, tapebridgeTransactionCounter,
      listenSock.get());

  } catch(castor::exception::Exception &ex) {
    castor::dlf::Param params[] = {
      castor::dlf::Param("mountTransactionId", jobRequest.volReqId  ),
      castor::dlf::Param("Message"           , ex.getMessage().str()),
      castor::dlf::Param("Code"              , ex.code()            )};
    CASTOR_DLF_WRITEPC(cuuid, DLF_LVL_ERROR,
      TAPEBRIDGE_TRANSFER_FAILED, params);
  } catch(std::exception &stdEx) {
    castor::dlf::Param params[] = {
      castor::dlf::Param("mountTransactionId", jobRequest.volReqId),
      castor::dlf::Param("Message"           , stdEx.what()       )};
    CASTOR_DLF_WRITEPC(cuuid, DLF_LVL_ERROR,
      TAPEBRIDGE_TRANSFER_FAILED, params);
  } catch(...) {
    castor::dlf::Param params[] = {
      castor::dlf::Param("mountTransactionId", jobRequest.volReqId),
      castor::dlf::Param("Message"           , "Unknown exception")};
    CASTOR_DLF_WRITEPC(cuuid, DLF_LVL_ERROR,
      TAPEBRIDGE_TRANSFER_FAILED, params);
  }
}


//-----------------------------------------------------------------------------
// sendClientInfoToRtcpd
//-----------------------------------------------------------------------------
castor::tape::legacymsg::RtcpJobReplyMsgBody
  castor::tape::tapebridge::VdqmRequestHandler::sendClientInfoToRtcpd(
  const std::string                   rtcpdHost,
  const unsigned int                  rtcpdPort,
  const int                           netReadWriteTimeout,
  const legacymsg::RtcpJobRqstMsgBody &jobRequest,
  const char                         (&bridgeCallbackHost)[net::HOSTNAMEBUFLEN],
  const unsigned short                bridgeCallbackPort)
  const throw(castor::exception::Exception) {
  tapeBridgeClientInfo2MsgBody_t clientInfoMsgBody;
  legacymsg::RtcpJobReplyMsgBody rtcpdReply;

  utils::setBytes(clientInfoMsgBody, '\0');
  utils::setBytes(rtcpdReply, '\0');

  clientInfoMsgBody.volReqId = jobRequest.volReqId;
  clientInfoMsgBody.bridgeCallbackPort = bridgeCallbackPort;
  clientInfoMsgBody.bridgeClientCallbackPort = jobRequest.clientPort;
  clientInfoMsgBody.clientUID = jobRequest.clientEuid;
  clientInfoMsgBody.clientGID = jobRequest.clientEgid;

  try {
    clientInfoMsgBody.tapeFlushMode =
      m_tapeFlushConfigParams.getTapeFlushMode().getValue();
  } catch(castor::exception::Exception &ex) {
    // Add context and rethrow
    TAPE_THROW_CODE(ex.code(),
      ex.getMessage().str());
  }

  // Max files and bytes before flush is only applicable if the tape flush
  // mode is TAPEBRIDGE_ONE_FLUSH_PER_N_FILES
  if(TAPEBRIDGE_ONE_FLUSH_PER_N_FILES == clientInfoMsgBody.tapeFlushMode) {
    try {
      clientInfoMsgBody.maxBytesBeforeFlush =
        m_tapeFlushConfigParams.getMaxBytesBeforeFlush().getValue();
      clientInfoMsgBody.maxFilesBeforeFlush =
        m_tapeFlushConfigParams.getMaxFilesBeforeFlush().getValue();
    } catch(castor::exception::Exception &ex) {
      // Add context and rethrow
      TAPE_THROW_CODE(ex.code(),
        ex.getMessage().str());
    }
  } else {
    clientInfoMsgBody.maxBytesBeforeFlush = 1;
    clientInfoMsgBody.maxFilesBeforeFlush = 1;
  }

  castor::utils::copyString(clientInfoMsgBody.bridgeHost, bridgeCallbackHost);
  castor::utils::copyString(clientInfoMsgBody.bridgeClientHost,
    jobRequest.clientHost);
  castor::utils::copyString(clientInfoMsgBody.dgn, jobRequest.dgn);
  castor::utils::copyString(clientInfoMsgBody.drive, jobRequest.driveUnit);
  castor::utils::copyString(clientInfoMsgBody.clientName,
    jobRequest.clientUserName);

  try {
    castor::tape::tapebridge::BridgeClientInfo2Sender::send(
      rtcpdHost,
      rtcpdPort,
      netReadWriteTimeout,
      clientInfoMsgBody,
      rtcpdReply);
  } catch(castor::exception::Exception &ex) {
    // Add context and rethrow
    TAPE_THROW_CODE(ex.code(),
      ex.getMessage().str());
  }

  return rtcpdReply;
}


//-----------------------------------------------------------------------------
// exceptionThrowingRun
//-----------------------------------------------------------------------------
void castor::tape::tapebridge::VdqmRequestHandler::exceptionThrowingRun(
  const Cuuid_t                       &cuuid,
  const legacymsg::RtcpJobRqstMsgBody &jobRequest,
  Counter<uint64_t>                   &tapebridgeTransactionCounter,
  const int                           bridgeCallbackSockFd)
  throw(castor::exception::Exception) {

  // Accept the initial incoming RTCPD callback connection.
  // Wrap the socket file descriptor in a smart file descriptor so that it is
  // guaranteed to be closed if it goes out of scope.
  castor::utils::SmartFd rtcpdInitialSock(
    net::acceptConnection(bridgeCallbackSockFd, RTCPDCALLBACKTIMEOUT));

  // Log the initial callback connection from RTCPD
  try {
    char hostName[net::HOSTNAMEBUFLEN];

    const net::IpAndPort peerIpAndPort =
      net::getPeerIpPort(rtcpdInitialSock.get());
    net::getPeerHostName(rtcpdInitialSock.get(), hostName);

    castor::dlf::Param params[] = {
      castor::dlf::Param("volReqId", jobRequest.volReqId       ),
      castor::dlf::Param("IP"      ,
        castor::dlf::IPAddress(peerIpAndPort.getIp())),
      castor::dlf::Param("Port"    , peerIpAndPort.getPort()   ),
      castor::dlf::Param("HostName", hostName                  ),
      castor::dlf::Param("socketFd", rtcpdInitialSock.get()    )};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM,
      TAPEBRIDGE_INITIAL_RTCPD_CALLBACK, params);
  } catch(castor::exception::Exception &ex) {
    TAPE_THROW_CODE(ECANCELED,
      ": Failed to get IP, port and host name"
      ": volReqId=" << jobRequest.volReqId);
  }

  // Get the request information and the drive unit from RTCPD
  legacymsg::RtcpTapeRqstErrMsgBody rtcpdRequestInfoReply;
  RtcpTxRx::getRequestInfoFromRtcpd(cuuid, jobRequest.volReqId,
    rtcpdInitialSock.get(), RTCPDNETRWTIMEOUT, rtcpdRequestInfoReply);

  // If the VDQM and RTCPD drive units do not match
  if(0 != strcmp(jobRequest.driveUnit, rtcpdRequestInfoReply.unit)) {
    TAPE_THROW_CODE(EBADMSG,
      ": VDQM and RTCPD drive units do not match"
      ": VDQM drive unit='" << jobRequest.driveUnit       << "'"
      " RTCPD drive unit='" << rtcpdRequestInfoReply.unit << "'");
  }

  // If the VDQM and RTCPD volume request IDs do not match
  if(jobRequest.volReqId != rtcpdRequestInfoReply.volReqId) {
    TAPE_THROW_CODE(EBADMSG,
      ": VDQM and RTCPD volume request Ids do not match"
      ": VDQM volume request ID=" << jobRequest.volReqId <<
      " RTCPD volume request ID=" << rtcpdRequestInfoReply.volReqId);
  }

  // Create the client proxy
  ClientAddressTcpIp
    clientAddress(jobRequest.clientHost, jobRequest.clientPort);
  ClientProxy clientProxy(
    cuuid,
    jobRequest.volReqId,
    CLIENTNETRWTIMEOUT,
    clientAddress,
    jobRequest.dgn,
    jobRequest.driveUnit);

  // Get the volume from the client of the tape-bridge
  std::auto_ptr<tapegateway::Volume> volume(clientProxy.getVolume(
    tapebridgeTransactionCounter.next()));

  // If there is no volume to mount, then notify the client end of session
  // and return
  if(NULL == volume.get()) {
    const uint64_t tapebridgeTransId =
      tapebridgeTransactionCounter.next();
    {
      castor::dlf::Param params[] = {
        castor::dlf::Param("mountTransactionId", jobRequest.volReqId ),
        castor::dlf::Param("volReqId"          , jobRequest.volReqId ),
        castor::dlf::Param("tapebridgeTransId" , tapebridgeTransId   ),
        castor::dlf::Param("driveUnit"         , jobRequest.driveUnit),
        castor::dlf::Param("clientAddress"     ,
          clientAddress.getDescription())};
      castor::dlf::dlf_writep(cuuid, DLF_LVL_WARNING,
        TAPEBRIDGE_NO_MOUNT_DUE_TO_NO_TAPE, params);
    }
    try {
      clientProxy.notifyEndOfSession(tapebridgeTransId);
    } catch(castor::exception::Exception &ex) {
      castor::exception::Exception ex2(ex.code());

      ex2.getMessage() <<
        "Failed to notify client end of session"
        ": mountTransactionId=" << jobRequest.volReqId <<
        ": tapebridgeTransId=" << tapebridgeTransId <<
        ": " << ex.getMessage().str();

      throw(ex2);
    }

    return;
  }

  // Create the object reponsible for sending and receiving the headers of
  // messages belonging to the legacy RTCOPY protocol
  LegacyTxRx legacyTxRx(RTCPDNETRWTIMEOUT);

  // If migrating
  if(volume->mode() == tapegateway::WRITE) {

    // Get volume information from the VMGR
    legacymsg::VmgrTapeInfoMsgBody tapeInfo;
    utils::setBytes(tapeInfo, '\0');
    {
      const uint32_t uid = getuid();
      const uint32_t gid = getgid();

      try {
        VmgrTxRx::getTapeInfoFromVmgr(cuuid, jobRequest.volReqId,
          VMGRNETRWTIMEOUT, uid, gid, volume->vid().c_str(), tapeInfo);
      } catch(castor::exception::Exception &ex) {
        castor::exception::Exception ex2(ex.code());

        ex2.getMessage() <<
          "Failed to get tape information"
          ": volReqId=" << jobRequest.volReqId <<
          ": vid=" << volume->vid() <<
          ": " << ex.getMessage().str();

        throw(ex2);
      }
    }

    // If the client is the tape gateway and the volume is not marked as BUSY
    if(volume->clientType() == tapegateway::TAPE_GATEWAY &&
      !(tapeInfo.status & TAPE_BUSY)) {
      castor::exception::Exception ex(ECANCELED);

      ex.getMessage() <<
        "The tape gateway is the client and the tape to be mounted is not BUSY"
        ": volReqId=" << jobRequest.volReqId <<
        ": vid=" << volume->vid();

      throw ex;
    }

    SystemFileCloser systemFileCloser;
    const bool logPeerOfCallbackConnectionsFromRtcpd = true;
    const bool checkRtcpdIsConnectingFromLocalHost = true;
    BridgeProtocolEngine bridgeProtocolEngine(
      systemFileCloser,
      m_bulkRequestConfigParams,
      m_tapeFlushConfigParams,
      cuuid,
      bridgeCallbackSockFd,
      rtcpdInitialSock.release(),
      jobRequest,
      *volume,
      tapeInfo.nbFiles,
      s_stoppingGracefullyFunctor,
      tapebridgeTransactionCounter,
      logPeerOfCallbackConnectionsFromRtcpd,
      checkRtcpdIsConnectingFromLocalHost,
      clientProxy,
      legacyTxRx);
    bridgeProtocolEngine.run();

  // Else recalling
  } else {
    SystemFileCloser systemFileCloser;
    const uint32_t nbFilesOnDestinationTape = 0;
    const bool logPeerOfCallbackConnectionsFromRtcpd = true;
    const bool checkRtcpdIsConnectingFromLocalHost = true;

    BridgeProtocolEngine bridgeProtocolEngine(
      systemFileCloser,
      m_bulkRequestConfigParams,
      m_tapeFlushConfigParams,
      cuuid,
      bridgeCallbackSockFd,
      rtcpdInitialSock.release(),
      jobRequest,
      *volume,
      nbFilesOnDestinationTape,
      s_stoppingGracefullyFunctor,
      tapebridgeTransactionCounter,
      logPeerOfCallbackConnectionsFromRtcpd,
      checkRtcpdIsConnectingFromLocalHost,
      clientProxy,
      legacyTxRx);
    bridgeProtocolEngine.run();
  }
}


//-----------------------------------------------------------------------------
// stop
//-----------------------------------------------------------------------------
void castor::tape::tapebridge::VdqmRequestHandler::stop()
  throw() {
  s_stoppingGracefully = true;
}


//-----------------------------------------------------------------------------
// checkRtcpJobSubmitterIsAuthorised
//-----------------------------------------------------------------------------
void castor::tape::tapebridge::VdqmRequestHandler::
  checkRtcpJobSubmitterIsAuthorised(const int socketFd)
  throw(castor::exception::Exception) {

  char peerHost[CA_MAXHOSTNAMELEN+1];

  // isadminhost fills in peerHost
  const int rc = isadminhost(socketFd, peerHost);

  if(rc == -1 && serrno != SENOTADMIN) {
    TAPE_THROW_EX(castor::exception::Internal,
         ": Failed to lookup connection"
      << ": Peer Host: " << peerHost);
  }

  if(*peerHost == '\0' ) {
    TAPE_THROW_CODE(EINVAL,
      ": Peer host name is an empty string");
  }

  if(rc != 0) {
    TAPE_THROW_CODE(SENOTADMIN,
         ": Unauthorized host"
      << ": Peer Host: " << peerHost);
  }
}
