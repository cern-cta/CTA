/******************************************************************************
 *                      DriveAllocationProtocolEngine.cpp
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

#include "castor/exception/Internal.hpp"
#include "castor/tape/aggregator/AggregatorDlfMessageConstants.hpp"
#include "castor/tape/aggregator/Constants.hpp"
#include "castor/tape/aggregator/DriveAllocationProtocolEngine.hpp"
#include "castor/tape/aggregator/GatewayTxRx.hpp"
#include "castor/tape/aggregator/Marshaller.hpp"
#include "castor/tape/aggregator/RcpJobReplyMsgBody.hpp"
#include "castor/tape/aggregator/RcpJobRqstMsgBody.hpp"
#include "castor/tape/aggregator/RcpJobSubmitter.hpp"
#include "castor/tape/aggregator/RtcpTxRx.hpp"
#include "castor/tape/fsm/Callback.hpp"
#include "castor/tape/net/net.hpp"
#include "castor/tape/utils/utils.hpp"
#include "h/Ctape_constants.h"
#include "h/rtcp_constants.h"
#include "h/vdqm_constants.h"

#include <iostream>


//-----------------------------------------------------------------------------
// checkRcpJobSubmitterIsAuthorised
//-----------------------------------------------------------------------------
void castor::tape::aggregator::DriveAllocationProtocolEngine::
  checkRcpJobSubmitterIsAuthorised(const int socketFd)
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


//-----------------------------------------------------------------------------
// run
//-----------------------------------------------------------------------------
bool castor::tape::aggregator::DriveAllocationProtocolEngine::run(
  const Cuuid_t &cuuid, castor::io::AbstractTCPSocket &vdqmSocket,
  const int rtcpdCallbackSocketFd, const char *rtcpdCallbackHost,
  const unsigned short rtcpdCallbackPort, uint32_t &volReqId,
  char (&gatewayHost)[CA_MAXHOSTNAMELEN+1], unsigned short &gatewayPort,
  SmartFd &rtcpdInitialSocketFd, uint32_t &mode, char (&unit)[CA_MAXUNMLEN+1],
  char (&vid)[CA_MAXVIDLEN+1], char (&label)[CA_MAXLBLTYPLEN+1],
  char (&density)[CA_MAXDENLEN+1]) throw(castor::exception::Exception) {
  
  RcpJobRqstMsgBody jobRequest;
  
  utils::setBytes(jobRequest, '\0');
  
  checkRcpJobSubmitterIsAuthorised(vdqmSocket.socket());
  
  RtcpTxRx::receiveRcpJobRqst(cuuid, vdqmSocket.socket(), RTCPDNETRWTIMEOUT,
    jobRequest);
    
  // Extract the volume request ID, gateway host and gateway port
  volReqId = jobRequest.tapeRequestId;
  utils::copyString(gatewayHost, jobRequest.clientHost);
  gatewayPort = jobRequest.clientPort;
  
  // Pass a modified version of the job request through to RTCPD, setting the
  // clientHost and clientPort parameters to identify the tape aggregator as
  // being a proxy for RTCPClientD
  {
    castor::dlf::Param params[] = {
      castor::dlf::Param("volReqId"       , jobRequest.tapeRequestId  ),
      castor::dlf::Param("Port"           , rtcpdCallbackPort         ),
      castor::dlf::Param("HostName"       , rtcpdCallbackHost         ),
      castor::dlf::Param("clientHost"     , jobRequest.clientHost     ),
      castor::dlf::Param("clientPort"     , jobRequest.clientPort     ),
      castor::dlf::Param("clientUserName" , jobRequest.clientUserName ),
      castor::dlf::Param("clientEuid"     , jobRequest.clientEuid     ),
      castor::dlf::Param("clientEgid"     , jobRequest.clientEgid     ),
      castor::dlf::Param("deviceGroupName", jobRequest.deviceGroupName),
      castor::dlf::Param("driveName"      , jobRequest.driveName      )};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM,
      AGGREGATOR_SUBMITTING_JOB_TO_RTCPD, params);
  }

  RcpJobReplyMsgBody rtcpdReply;
  RcpJobSubmitter::submit(
    "localhost",               // host
    RTCOPY_PORT,               // port
    RTCPDNETRWTIMEOUT,         // netReadWriteTimeout
    "RTCPD",                   // remoteCopyType
    jobRequest.tapeRequestId,
    jobRequest.clientUserName,
    rtcpdCallbackHost,
    rtcpdCallbackPort,
    jobRequest.clientEuid,
    jobRequest.clientEgid,
    jobRequest.deviceGroupName,
    jobRequest.driveName,
    rtcpdReply);

  // Prepare a positive response for the VDQM which will be overwritten if
  // RTCPD replied to the tape aggregator with an error message
  uint32_t    errorStatusForVdqm  = VDQM_CLIENTINFO; // Strange status code
  std::string errorMessageForVdqm = "";

  // If RTCPD returned an error message
  // Checking the size of the error message because the status maybe non-zero
  // even if there is no error
  if(strlen(rtcpdReply.errorMessage) > 0) {
    castor::dlf::Param params[] = {
      castor::dlf::Param("volReqId", jobRequest.tapeRequestId),
      castor::dlf::Param("Message" , rtcpdReply.errorMessage ),
      castor::dlf::Param("Code"    , rtcpdReply.status       )};
    CASTOR_DLF_WRITEPC(cuuid, DLF_LVL_ERROR,
      AGGREGATOR_RECEIVED_RTCPD_ERROR_MESSAGE, params);

    // Override positive response with the error message from RTCPD
    errorStatusForVdqm  = rtcpdReply.status;
    errorMessageForVdqm = rtcpdReply.errorMessage;
  }

  // Acknowledge the VDQM - maybe positive or negative depending on reply
  // from RTCPD
  char vdqmReplyBuf[MSGBUFSIZ];
  size_t vdqmReplyLen = 0;
  vdqmReplyLen = Marshaller::marshallRcpJobReplyMsgBody(vdqmReplyBuf,
    rtcpdReply);
  net::writeBytes(vdqmSocket.socket(), RTCPDNETRWTIMEOUT, vdqmReplyLen,
    vdqmReplyBuf);

  // Close the connection to the VDQM
  // Please note that the destructor of AbstractTCPSocket will not close the
  // socket a second time
  vdqmSocket.close();

  // If RTCPD returned an error message then it will not make a callback
  // connection and the aggregator should not continue any further
  if(strlen(rtcpdReply.errorMessage) > 0) {
    // A volume was not received from the tape gateway
    return false;
  }

  // Accept the initial incoming RTCPD callback connection.
  // Wrap the socket file descriptor in a smart file descriptor so that it is
  // guaranteed to be closed when it goes out of scope.
  rtcpdInitialSocketFd.reset(net::acceptConnection(rtcpdCallbackSocketFd,
    RTCPDCALLBACKTIMEOUT));

  // Log the connection
  try {
    unsigned short port = 0; // Client port
    unsigned long  ip   = 0; // Client IP
    char           hostName[net::HOSTNAMEBUFLEN];

    net::getPeerIpPort(rtcpdInitialSocketFd.get(), ip, port);
    net::getPeerHostName(rtcpdInitialSocketFd.get(), hostName);

    castor::dlf::Param params[] = {
      castor::dlf::Param("volReqId", volReqId),
      castor::dlf::Param("IP"      , castor::dlf::IPAddress(ip)  ),
      castor::dlf::Param("Port"    , port                        ),
      castor::dlf::Param("HostName", hostName                    ),
      castor::dlf::Param("socketFd", rtcpdInitialSocketFd.get()  )};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM,
      AGGREGATOR_INITIAL_RTCPD_CALLBACK_WITH_INFO, params);
  } catch(castor::exception::Exception &ex) {
    CASTOR_DLF_WRITEC(cuuid, DLF_LVL_ERROR,
      AGGREGATOR_INITIAL_RTCPD_CALLBACK_WITHOUT_INFO);
  }

  // Get the request informatiom and the drive unit from RTCPD
  RtcpTapeRqstErrMsgBody rtcpdRequestInfoReply;
  RtcpTxRx::getRequestInfoFromRtcpd(cuuid, volReqId, rtcpdInitialSocketFd.get(),
    RTCPDNETRWTIMEOUT, rtcpdRequestInfoReply);
  utils::copyString(unit, rtcpdRequestInfoReply.unit);

  // If the VDQM and RTCPD volume request IDs do not match
  if(rtcpdRequestInfoReply.volReqId != volReqId) {

    TAPE_THROW_CODE(EBADMSG,
         ": VDQM and RTCPD volume request Ids do not match"
         ": VDQM volume request ID: " << volReqId
      << ": RTCPD volume request ID: " << rtcpdRequestInfoReply.volReqId);
  }

  // Get the volume from the tape gateway
  const bool thereIsAVolume = GatewayTxRx::getVolumeFromGateway(cuuid,
    volReqId, gatewayHost, gatewayPort, vid, mode, label, density);

  return thereIsAVolume;
}


//-----------------------------------------------------------------------------
// testFsm
//-----------------------------------------------------------------------------
void castor::tape::aggregator::DriveAllocationProtocolEngine::testFsm() {
  std::cout << std::endl;                         
  std::cout << "=========================================" << std::endl;
  std::cout << "castor::tape::aggregator::FsmTest::doit()" << std::endl;
  std::cout << "=========================================" << std::endl;
  std::cout << std::endl;                                               


  //---------------------------------------------
  // Define and set the state machine transitions
  //---------------------------------------------

  typedef fsm::Callback<DriveAllocationProtocolEngine> Callback;
  Callback getReqFromRtcpd(*this,
    &DriveAllocationProtocolEngine::getReqFromRtcpd);
  Callback getVolFromTGate(*this,
    &DriveAllocationProtocolEngine::getVolFromTGate);
  Callback error(*this, &DriveAllocationProtocolEngine::error);

  fsm::Transition transitions[] = {
  // from state       , to state        , event , action
    {"INIT"           , "WAIT_RTCPD_REQ", "INIT", &getReqFromRtcpd},
    {"WAIT_RTCPD_REQ" , "WAIT_TGATE_VOL", "REQ" , &getVolFromTGate},
    {"WAIT_TGATE_VOL" , "SUCCEEDED"     , "VOL" , NULL            },
    {NULL             , NULL            , NULL  , NULL            }};

  m_fsm.setTransitions(transitions);


  //------------------------
  // Set the state hierarchy
  //------------------------

  fsm::ChildToParentState hierarchy[] = {
  // child           , parent            
    {"WAIT_RTCPD_REQ", "RTCPD_CONNECTED"},
    {"WAIT_TGATE_VOL", "RTCPD_CONNECTED"},
    {NULL            , NULL             }
  };

  m_fsm.setStateHierarchy(hierarchy);


  //-------------------------------------------
  // Set the initial state of the state machine
  //-------------------------------------------

  m_fsm.setState("INIT");


  //----------------------------------------------------------
  // Execute the state machine with debugging print statements
  //----------------------------------------------------------

  const char *event = "INIT";

  // While no more automatic state transitions
  while(event != NULL) {

    std::cout << "From state        = " << m_fsm.getState() << std::endl;
    std::cout << "Dispatching event = " << event            << std::endl;

    event = m_fsm.dispatch(event);

    if(event != NULL) {
      std::cout << "Internally generated event for an automatic state "
        "transition" << std::endl;
    }
  }

  std::cout << std::endl;
}


const char *castor::tape::aggregator::DriveAllocationProtocolEngine::
  getVolFromTGate() {
  std::cout << "getVolFromTGate()" << std::endl;
  sleep(1);                                     
  return "VOL";                                 
}                                               


const char *castor::tape::aggregator::DriveAllocationProtocolEngine::
  getReqFromRtcpd() {
  std::cout << "getReqFromRtcpd()" << std::endl;
  sleep(1);                                     
  return "REQ";                                 
}                                               


const char *castor::tape::aggregator::DriveAllocationProtocolEngine::
  error() {
  std::cout << "error()" << std::endl;
  sleep(1);                           
  return NULL;                        
}                                     
