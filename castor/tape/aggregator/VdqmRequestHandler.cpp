/******************************************************************************
 *                castor/tape/aggregator/VdqmRequestHandler.cpp
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

#include "castor/dlf/Dlf.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/exception/Internal.hpp"
#include "castor/tape/aggregator/AggregatorDlfMessageConstants.hpp"
#include "castor/tape/aggregator/BridgeProtocolEngine.hpp"
#include "castor/tape/aggregator/Constants.hpp"
#include "castor/tape/aggregator/DriveAllocationProtocolEngine.hpp"
#include "castor/tape/aggregator/GatewayTxRx.hpp"
#include "castor/tape/aggregator/Marshaller.hpp"
#include "castor/tape/aggregator/Packer.hpp"
#include "castor/tape/aggregator/RcpJobSubmitter.hpp"
#include "castor/tape/aggregator/Unpacker.hpp"
#include "castor/tape/aggregator/RtcpTapeRqstErrMsgBody.hpp"
#include "castor/tape/aggregator/RtcpFileRqstErrMsgBody.hpp"
#include "castor/tape/aggregator/SmartFd.hpp"
#include "castor/tape/aggregator/SmartFdList.hpp"
#include "castor/tape/aggregator/RtcpTxRx.hpp"
#include "castor/tape/aggregator/VdqmRequestHandler.hpp"
#include "castor/tape/net/net.hpp"
#include "castor/tape/tapegateway/VolumeRequest.hpp"
#include "castor/tape/tapegateway/Volume.hpp"
#include "castor/tape/utils/utils.hpp"
#include "h/common.h"
#include "h/Ctape_constants.h"
#include "h/rtcp_constants.h"
#include "h/vdqm_constants.h"

#include <memory>
#include <sys/select.h>


//-----------------------------------------------------------------------------
// destructor
//-----------------------------------------------------------------------------
castor::tape::aggregator::VdqmRequestHandler::~VdqmRequestHandler()
  throw() {
}


//-----------------------------------------------------------------------------
// init
//-----------------------------------------------------------------------------
void castor::tape::aggregator::VdqmRequestHandler::init() throw() {
}


//-----------------------------------------------------------------------------
// run
//-----------------------------------------------------------------------------
void castor::tape::aggregator::VdqmRequestHandler::run(void *param)
  throw() {
  Cuuid_t cuuid = nullCuuid;

  // Give a Cuuid to the request
  Cuuid_create(&cuuid);

  // Check the parameter to the run function has been set
  if(param == NULL) {
    CASTOR_DLF_WRITEC(cuuid, DLF_LVL_ERROR,
      AGGREGATOR_VDQM_REQUEST_HANDLER_SOCKET_IS_NULL);
    return;
  }

  // Wrap the VDQM connection socket within an auto pointer.  When the auto
  // pointer goes out of scope it will delete the socket.  The destructor of
  // the socket will in turn close the connection.
  std::auto_ptr<castor::io::AbstractTCPSocket>
    vdqmSocket((castor::io::AbstractTCPSocket*)param);

  // Log the new connection
  try {
    unsigned short port = 0; // Client port
    unsigned long  ip   = 0; // Client IP
    char           hostName[net::HOSTNAMEBUFLEN];

    net::getPeerIpPort(vdqmSocket->socket(), ip, port);
    net::getPeerHostName(vdqmSocket->socket(), hostName);

    castor::dlf::Param params[] = {
      castor::dlf::Param("IP"      , castor::dlf::IPAddress(ip)),
      castor::dlf::Param("Port"    , port),
      castor::dlf::Param("HostName", hostName)};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM,
      AGGREGATOR_VDQM_CONNECTION_WITH_INFO, params);

  } catch(castor::exception::Exception &ex) {
    castor::dlf::Param params[] = {
      castor::dlf::Param("Message" , ex.getMessage().str()),
      castor::dlf::Param("Code"    , ex.code())};
    CASTOR_DLF_WRITEPC(cuuid, DLF_LVL_ERROR,
      AGGREGATOR_VDQM_CONNECTION_WITHOUT_INFO, params);
  }

  try {
    // Create, bind and mark a listen socket for RTCPD callback connections
    // Wrap the socket file descriptor in a smart file descriptor so that it is
    // guaranteed to be closed when it goes out of scope.
    SmartFd rtcpdCallbackSocketFd(net::createListenerSocket("127.0.0.1",0));

    // Get the IP, host name and port of the callback port
    unsigned long rtcpdCallbackIp = 0;
    char rtcpdCallbackHost[net::HOSTNAMEBUFLEN];
    utils::setBytes(rtcpdCallbackHost, '\0');
    unsigned short rtcpdCallbackPort = 0;
    net::getSocketIpHostnamePort(rtcpdCallbackSocketFd.get(),
    rtcpdCallbackIp, rtcpdCallbackHost, rtcpdCallbackPort);

    castor::dlf::Param params[] = {
      castor::dlf::Param("IP"      , castor::dlf::IPAddress(rtcpdCallbackIp)),
      castor::dlf::Param("Port"    , rtcpdCallbackPort),
      castor::dlf::Param("HostName", rtcpdCallbackHost)};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM,
      AGGREGATOR_CREATED_RTCPD_CALLBACK_PORT, params);

    uint32_t volReqId = 0;
    char gatewayHost[CA_MAXHOSTNAMELEN+1];
    utils::setBytes(gatewayHost, '\0');
    unsigned short gatewayPort = 0;
    SmartFd rtcpdInitialSocketFd;
    uint32_t mode = 0;
    char unit[CA_MAXUNMLEN+1];
    utils::setBytes(unit, '\0');
    char vid[CA_MAXVIDLEN+1];
    utils::setBytes(vid, '\0');
    char label[CA_MAXLBLTYPLEN+1];
    utils::setBytes(label, '\0');
    char density[CA_MAXDENLEN+1];
    utils::setBytes(density, '\0');

    DriveAllocationProtocolEngine driveAllocationProtocolEngine;

    driveAllocationProtocolEngine.run(cuuid, *(vdqmSocket.get()),
      rtcpdCallbackSocketFd.get(), rtcpdCallbackHost, rtcpdCallbackPort,
      volReqId, gatewayHost, gatewayPort, rtcpdInitialSocketFd, mode, unit,
      vid, label, density);

    // If the volume has the aggregation format
    if(strcmp(label, "ALB") == 0) {

      // If migrating
      if(mode == WRITE_ENABLE) {

        Packer packer;
        packer.run();

      // Else recalling
      } else {

        Unpacker unpacker;
        unpacker.run();
      }

    // Else the volume does not have the aggregation format
    } else {

      // Note the call to run() sets the vsn to ""
      char vsn[CA_MAXVSNLEN+1];
      utils::setBytes(vsn, '\0');
      BridgeProtocolEngine bridgeProtocolEngine(cuuid, volReqId, gatewayHost,
        gatewayPort, rtcpdCallbackSocketFd.get(), rtcpdInitialSocketFd.get(),
        mode, unit, vid, vsn, label, density);
      bridgeProtocolEngine.run();
    }
  } catch(castor::exception::Exception &ex) {

    char codeStr[1024];
    sstrerror_r(ex.code(), codeStr, sizeof(codeStr));

    castor::dlf::Param params[] = {
      castor::dlf::Param("Message"   , ex.getMessage().str()),
      castor::dlf::Param("Code"      , ex.code()),
      castor::dlf::Param("CodeString", codeStr)};
    CASTOR_DLF_WRITEPC(cuuid, DLF_LVL_ERROR,
      AGGREGATOR_TRANSFER_FAILED, params);
  }
}


//-----------------------------------------------------------------------------
// stop
//-----------------------------------------------------------------------------
void castor::tape::aggregator::VdqmRequestHandler::stop()
  throw() {
}
