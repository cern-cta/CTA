/******************************************************************************
 *                castor/tape/tapeserver/VdqmRequestHandler.cpp
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
#include "castor/tape/tapeserver/DlfMessageConstants.hpp"
#include "castor/tape/tapeserver/BridgeProtocolEngine.hpp"
#include "castor/tape/tapeserver/Constants.hpp"
#include "castor/tape/tapeserver/DriveAllocationProtocolEngine.hpp"
#include "castor/tape/tapeserver/ClientTxRx.hpp"
#include "castor/tape/tapeserver/Packer.hpp"
#include "castor/tape/tapeserver/RtcpJobSubmitter.hpp"
#include "castor/tape/tapeserver/RtcpTxRx.hpp"
#include "castor/tape/tapeserver/Unpacker.hpp"
#include "castor/tape/tapeserver/VdqmRequestHandler.hpp"
#include "castor/tape/tapeserver/VmgrTxRx.hpp"
#include "castor/tape/legacymsg/RtcpMarshal.hpp"
#include "castor/tape/legacymsg/VmgrMarshal.hpp"
#include "castor/tape/net/net.hpp"
#include "castor/tape/tapegateway/VolumeRequest.hpp"
#include "castor/tape/tapegateway/Volume.hpp"
#include "castor/tape/utils/SmartFd.hpp"
#include "castor/tape/utils/SmartFdList.hpp"
#include "castor/tape/utils/utils.hpp"
#include "h/common.h"
#include "h/Ctape_constants.h"
#include "h/rtcp_constants.h"
#include "h/vdqm_constants.h"
#include "h/vmgr_constants.h"

#include <algorithm>
#include <memory>


//-----------------------------------------------------------------------------
// s_stoppingGracefully
//-----------------------------------------------------------------------------
bool castor::tape::tapeserver::VdqmRequestHandler::s_stoppingGracefully = false;


//-----------------------------------------------------------------------------
// s_stoppingGracefullyFunctor
//-----------------------------------------------------------------------------
castor::tape::tapeserver::VdqmRequestHandler::StoppingGracefullyFunctor
  castor::tape::tapeserver::VdqmRequestHandler::s_stoppingGracefullyFunctor;


//-----------------------------------------------------------------------------
// constructor
//-----------------------------------------------------------------------------
castor::tape::tapeserver::VdqmRequestHandler::VdqmRequestHandler(
  const uint32_t nbDrives) throw() : m_nbDrives(nbDrives) {
}


//-----------------------------------------------------------------------------
// destructor
//-----------------------------------------------------------------------------
castor::tape::tapeserver::VdqmRequestHandler::~VdqmRequestHandler()
  throw() {
}


//-----------------------------------------------------------------------------
// init
//-----------------------------------------------------------------------------
void castor::tape::tapeserver::VdqmRequestHandler::init() throw() {
}


//-----------------------------------------------------------------------------
// run
//-----------------------------------------------------------------------------
void castor::tape::tapeserver::VdqmRequestHandler::run(void *param)
  throw() {

  // Give a Cuuid to the request
  Cuuid_t cuuid = nullCuuid;
  Cuuid_create(&cuuid);

  // The remote-copy job request to be read from the VDQM
  legacymsg::RtcpJobRqstMsgBody jobRequest;

  try {
    // Check the parameter to the run function has been set
    if(param == NULL) {
      TAPE_THROW_EX(castor::exception::InvalidArgument,
        "VDQM request handler socket is NULL");
    }

    getRtcpJobAndCloseConn(cuuid, (castor::io::ServerSocket*)param, jobRequest);
  } catch(castor::exception::Exception &ex) {

    castor::dlf::Param params[] = {
      castor::dlf::Param("Message", ex.getMessage().str()),
      castor::dlf::Param("Code"   , ex.code()            )};
    CASTOR_DLF_WRITEPC(cuuid, DLF_LVL_ERROR,
      TAPESERVER_TRANSFER_FAILED, params);

    // Return
    return;
  }

  try {
    // Check the drive unit name given by the VDQM exists in the TPCONFIG file
    // and that there are not more drives attached to the tape server than
    // there were just before the VdqmRequestHandlerPool thread-pool was
    // created.
    {
      utils::TpconfigLines tpconfigLines;
      try {
        utils::parseTpconfig(TPCONFIGPATH, tpconfigLines);
      } catch (castor::exception::Exception &ex) {
        castor::dlf::Param params[] = {
          castor::dlf::Param("filename"     , TPCONFIGPATH        ),
          castor::dlf::Param("errorCode"    , ex.code()           ),
          castor::dlf::Param("errorMessage", ex.getMessage().str())};
        castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR,
          TAPESERVER_FAILED_TO_PARSE_TPCONFIG, params);

        castor::exception::Exception ex2(ex.code());
        ex2.getMessage() <<
          "Failed to parse TPCONFIG file"
          ": " << ex.getMessage().str();
        throw(ex2);
      }

      // Extract the drive units names
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
        TAPESERVER_PARSED_TPCONFIG, params);

      // Throw an exception if the drive-unit name given by the VDQM is not
      // in the list of names extracted from the TPCONFIG file
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

      // Log an error if there are more drives attached to the tape server than
      // there were just before the VdqmRequestHandlerPool thread-pool was
      // created.
      if(driveNames.size() > m_nbDrives) {
        castor::dlf::Param params[] = {
          castor::dlf::Param("filename"         , TPCONFIGPATH     ),
          castor::dlf::Param("expectedNbDrives" , m_nbDrives       ),
          castor::dlf::Param("actualNbDrives"   , driveNames.size())};
        castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR,
          TAPESERVER_TOO_MANY_DRIVES_IN_TPCONFIG, params);
      }
    }

    // Create the tapeserver transaction ID
    Counter<uint64_t> tapeserverTransactionCounter(0);

    // Perform the rest of the thread's work, notifying the client if any
    // exception is thrown
    try {
      exceptionThrowingRun(cuuid, jobRequest, tapeserverTransactionCounter);
    } catch(castor::exception::Exception &ex) {

      const uint64_t aggregatorTransactionId =
        tapeserverTransactionCounter.next();
      try {
        castor::dlf::Param params[] = {
          castor::dlf::Param("mountTransactionId", jobRequest.volReqId    ),
          castor::dlf::Param("aggregatorTransId" , aggregatorTransactionId),
          castor::dlf::Param("Message"           , ex.getMessage().str()  ),
          castor::dlf::Param("Code"              , ex.code()              )};
        castor::dlf::dlf_writep(cuuid, DLF_LVL_DEBUG,
          TAPESERVER_NOTIFY_CLIENT_END_OF_FAILED_SESSION, params);

        ClientTxRx::notifyEndOfFailedSession(cuuid, jobRequest.volReqId,
          aggregatorTransactionId, jobRequest.clientHost,
          jobRequest.clientPort, ex);
      } catch(castor::exception::Exception &ex2) {
        // Don't rethrow, just log the exception
        castor::dlf::Param params[] = {
          castor::dlf::Param("mountTransactionId", jobRequest.volReqId    ),
          castor::dlf::Param("aggregatorTransId" , aggregatorTransactionId),
          castor::dlf::Param("Message"           , ex2.getMessage().str() ),
          castor::dlf::Param("Code"              , ex2.code()             )};
        castor::dlf::dlf_writep(cuuid, DLF_LVL_ERROR,
          TAPESERVER_FAILED_TO_NOTIFY_CLIENT_END_OF_FAILED_SESSION, params);
      }

      // Rethrow the exception thrown by exceptionThrowingRun() so that the
      // outer try and catch block always logs TAPESERVER_TRANSFER_FAILED when
      // a failure has occured
      throw(ex);
    }
  } catch(castor::exception::Exception &ex) {

    castor::dlf::Param params[] = {
      castor::dlf::Param("mountTransactionId", jobRequest.volReqId  ),
      castor::dlf::Param("Message"           , ex.getMessage().str()),
      castor::dlf::Param("Code"              , ex.code()            )};
    CASTOR_DLF_WRITEPC(cuuid, DLF_LVL_ERROR,
      TAPESERVER_TRANSFER_FAILED, params);
  } catch(std::exception &stdEx) {
    castor::dlf::Param params[] = {
      castor::dlf::Param("mountTransactionId", jobRequest.volReqId),
      castor::dlf::Param("Message"           , stdEx.what()       )};
    CASTOR_DLF_WRITEPC(cuuid, DLF_LVL_ERROR,
      TAPESERVER_TRANSFER_FAILED, params);
  } catch(...) {
    castor::dlf::Param params[] = {
      castor::dlf::Param("mountTransactionId", jobRequest.volReqId),
      castor::dlf::Param("Message"           , "Unknown exception")};
    CASTOR_DLF_WRITEPC(cuuid, DLF_LVL_ERROR,
      TAPESERVER_TRANSFER_FAILED, params);
  }
}


//-----------------------------------------------------------------------------
// getRtcpJobFromVdqmAndCloseConn
//-----------------------------------------------------------------------------
void castor::tape::tapeserver::VdqmRequestHandler::getRtcpJobAndCloseConn(
  const Cuuid_t                 &cuuid,
  io::ServerSocket*             sock,
  legacymsg::RtcpJobRqstMsgBody &jobRequest)
  throw(castor::exception::Exception) {

  // Wrap the VDQM connection socket within an auto pointer.  When the auto
  // pointer goes out of scope it will delete the socket.  The destructor of
  // the socket will in turn close the connection.
  std::auto_ptr<castor::io::ServerSocket> vdqmSock(sock);

  // Log the new connection
  try {
    unsigned short port = 0; // Client port
    unsigned long  ip   = 0; // Client IP
    char           hostName[net::HOSTNAMEBUFLEN];

    net::getPeerIpPort(vdqmSock->socket(), ip, port);
    net::getPeerHostName(vdqmSock->socket(), hostName);

    castor::dlf::Param params[] = {
      castor::dlf::Param("IP"      , castor::dlf::IPAddress(ip)),
      castor::dlf::Param("Port"    , port                      ),
      castor::dlf::Param("HostName", hostName                  ),
      castor::dlf::Param("socketFd", vdqmSock->socket()        )};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM,
      TAPESERVER_RECEIVED_VDQM_CONNECTION, params);

  } catch(castor::exception::Exception &ex) {
    castor::exception::Exception ex2(ex.code());

    ex2.getMessage() <<
      "Failed to log new connection"
      ": " << ex.getMessage().str();

    throw(ex2);
  }

  // Check the VDQM (an RTCP job submitter) is authorised
  checkRtcpJobSubmitterIsAuthorised(vdqmSock->socket());

  // Receive the RTCOPY job request from the VDQM
  utils::setBytes(jobRequest, '\0');
  RtcpTxRx::receiveRtcpJobRqst(cuuid, vdqmSock->socket(), RTCPDNETRWTIMEOUT,
    jobRequest);

  // Send a positive acknowledge to the VDQM
  {
    legacymsg::RtcpJobReplyMsgBody rtcpdReply;
    utils::setBytes(rtcpdReply, '\0');
    rtcpdReply.status = VDQM_CLIENTINFO; // Strange status code
    char vdqmReplyBuf[RTCPMSGBUFSIZE];
    size_t vdqmReplyLen = 0;
    vdqmReplyLen = legacymsg::marshal(vdqmReplyBuf, rtcpdReply);
    net::writeBytes(vdqmSock->socket(), RTCPDNETRWTIMEOUT, vdqmReplyLen,
      vdqmReplyBuf);
  }
}


//-----------------------------------------------------------------------------
// exceptionThrowingRun
//-----------------------------------------------------------------------------
void castor::tape::tapeserver::VdqmRequestHandler::exceptionThrowingRun(
  const Cuuid_t                       &cuuid,
  const legacymsg::RtcpJobRqstMsgBody &jobRequest,
  Counter<uint64_t>                   &tapeserverTransactionCounter)
  throw(castor::exception::Exception) {

  // Create, bind and mark a listen socket for RTCPD callback connections
  // Wrap the socket file descriptor in a smart file descriptor so that it is
  // guaranteed to be closed when it goes out of scope.
  const unsigned short lowPort = utils::getPortFromConfig(
    "TAPESERVER", "RTCPDLOWPORT", TAPEBRIDGE_RTCPDLOWPORT);
  const unsigned short highPort = utils::getPortFromConfig(
    "TAPESERVER", "RTCPDHIGHPORT", TAPEBRIDGE_RTCPDHIGHPORT);
  unsigned short chosenPort = 0;
  utils::SmartFd listenSock(net::createListenerSock("127.0.0.1", lowPort,
    highPort,  chosenPort));

  // Get and log the IP, host name, port and socket file-descriptor of the
  // callback socket
  unsigned long rtcpdCallbackIp = 0;
  char rtcpdCallbackHost[net::HOSTNAMEBUFLEN];
  utils::setBytes(rtcpdCallbackHost, '\0');
  unsigned short rtcpdCallbackPort = 0;
  net::getSockIpHostnamePort(listenSock.get(),
    rtcpdCallbackIp, rtcpdCallbackHost, rtcpdCallbackPort);
  castor::dlf::Param params[] = {
    castor::dlf::Param("mountTransactionId", jobRequest.volReqId            ),
    castor::dlf::Param("IP"                , dlf::IPAddress(rtcpdCallbackIp)),
    castor::dlf::Param("Port"              , rtcpdCallbackPort              ),
    castor::dlf::Param("HostName"          , rtcpdCallbackHost              ),
    castor::dlf::Param("socketFd"          , listenSock.get()               )};
  castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM,
    TAPESERVER_CREATED_RTCPD_CALLBACK_PORT, params);

  // Execute the drive allocation part of the RTCOPY protocol
  utils::SmartFd initialRtcpdSock;
  DriveAllocationProtocolEngine
    driveAllocationProtocolEngine(tapeserverTransactionCounter);
  std::auto_ptr<tapegateway::Volume>
    volume(driveAllocationProtocolEngine.run(cuuid, listenSock.get(),
    rtcpdCallbackHost, rtcpdCallbackPort, initialRtcpdSock, jobRequest));

  // If there is no volume to mount, then notify the client end of session
  // and return
  if(volume.get() == NULL) {
    const uint64_t aggregatorTransactionId =
      tapeserverTransactionCounter.next();
    try {
      ClientTxRx::notifyEndOfSession(cuuid, jobRequest.volReqId,
        aggregatorTransactionId, jobRequest.clientHost, jobRequest.clientPort);
    } catch(castor::exception::Exception &ex) {
      castor::exception::Exception ex2(ex.code());

      ex2.getMessage() <<
        "Failed to notify client end of session"
        ": mountTransactionId=" << jobRequest.volReqId <<
        ": aggregatorTransactionId=" << aggregatorTransactionId <<
        ": " << ex.getMessage().str();

      throw(ex2);
    }

    return;
  }

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

    enterBridgeOrTapeServerMode(cuuid, listenSock.get(),
      initialRtcpdSock.get(), jobRequest, *volume, tapeInfo.nbFiles,
      s_stoppingGracefullyFunctor, tapeserverTransactionCounter);

  // Else recalling
  } else {
    const uint32_t nbFilesOnDestinationTape = 0;

    enterBridgeOrTapeServerMode(cuuid, listenSock.get(),
      initialRtcpdSock.get(), jobRequest, *volume,
      nbFilesOnDestinationTape, s_stoppingGracefullyFunctor,
      tapeserverTransactionCounter);
  }
}


//-----------------------------------------------------------------------------
// enterBridgeOrTapeServerMode
//-----------------------------------------------------------------------------
void castor::tape::tapeserver::VdqmRequestHandler::enterBridgeOrTapeServerMode(
  const Cuuid_t                       &cuuid,
  const int                           listenSock,
  const int                           initialRtcpdSock,
  const legacymsg::RtcpJobRqstMsgBody &jobRequest,
  tapegateway::Volume                 &volume,
  const uint32_t                      nbFilesOnDestinationTape,
  BoolFunctor                         &stoppingGracefully,
  Counter<uint64_t>                   &tapeserverTransactionCounter)
  throw(castor::exception::Exception) {

  // If the volume has the aggregation format
  if(volume.label() == "ALB") {

    // Enter tapeserver mode
    if(volume.mode() == tapegateway::WRITE) {
      Packer packer;
      packer.run();
    } else {
      Unpacker unpacker;
      unpacker.run();
    }

  // Else the volume does not have the aggregation format
  } else {

    // Enter bridge mode
    castor::dlf::Param params[] = {
      castor::dlf::Param("volReqId", jobRequest.volReqId)};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM,
      TAPESERVER_ENTERING_BRIDGE_MODE, params);
    BridgeProtocolEngine bridgeProtocolEngine(cuuid, listenSock,
      initialRtcpdSock, jobRequest, volume, nbFilesOnDestinationTape,
      s_stoppingGracefullyFunctor, tapeserverTransactionCounter);
    bridgeProtocolEngine.run();
  }
}


//-----------------------------------------------------------------------------
// stop
//-----------------------------------------------------------------------------
void castor::tape::tapeserver::VdqmRequestHandler::stop()
  throw() {
  s_stoppingGracefully = true;
}


//-----------------------------------------------------------------------------
// checkRtcpJobSubmitterIsAuthorised
//-----------------------------------------------------------------------------
void castor::tape::tapeserver::VdqmRequestHandler::
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
