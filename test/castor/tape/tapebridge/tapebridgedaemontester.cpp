/******************************************************************************
 *                test/castor/tape/tapebridge/tapebridgedaemontester.cpp
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
 * @author Steven.Murray@cern.ch
 *****************************************************************************/

#include "test_exception.hpp"
#include "castor/PortNumbers.hpp"
#include "castor/exception/Internal.hpp"
#include "castor/tape/legacymsg/MessageHeader.hpp"
#include "castor/tape/legacymsg/CommonMarshal.hpp"
#include "castor/tape/legacymsg/RtcpMarshal.hpp"
#include "castor/tape/net/net.hpp"
#include "castor/tape/tapebridge/LegacyTxRx.hpp"
#include "castor/tape/tapegateway/VolumeRequest.hpp"
#include "castor/tape/utils/SmartFd.hpp"
#include "castor/tape/utils/utils.hpp"
#include "castor/vdqm/RemoteCopyConnection.hpp"
#include "h/Cuuid.h"
#include "h/rtcp_constants.h"
#include "h/rtcpd_GetClientInfo.h"
#include "h/tapebridge_constants.h"
#include "h/tapebridge_marshall.h"

#include <arpa/inet.h>
#include <errno.h>
#include <exception>
#include <iostream>
#include <netinet/in.h>
#include <pthread.h>
#include <sstream>
#include <stdlib.h>
#include <string>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

extern "C" int rtcp_InitLog (char *, FILE *, FILE *, SOCKET *);

/**
 * C++ wrapper around the connect() C function.  This wrapper converts errors
 * reported by a return value of -1 from connect() in exceptions.
 */
void connect_stdException(
  const int                    sockFd,
  const struct sockaddr *const servAddr,
  const socklen_t              addrLen) {

  const int rc = connect(sockFd, servAddr, addrLen);
  const int saved_errno = errno;
  if(0 != rc) {
    char buf[1024];
    std::ostringstream oss;

    oss <<
      "Failed to connect"
      ": Function=" << __FUNCTION__ <<
      " Line=" << __LINE__ <<
      ": " << strerror_r(saved_errno, buf, sizeof(buf));

    test_exception te(oss.str());

    throw te;
  }
}

int createListenerSock_stdException(const char *addr,
  const unsigned short lowPort, const unsigned short highPort,
  unsigned short &chosenPort) {
  try {
    return castor::tape::net::createListenerSock(addr, lowPort, highPort,
      chosenPort);
  } catch(castor::exception::Exception &ex) {
    test_exception te(ex.getMessage().str());

    throw te;
  }
}

typedef struct {
  int                           inListenSocketFd;
  int                           outGetClientInfoSuccess;
  tapeBridgeClientInfoMsgBody_t outMsgBody;
} rtcpdThread_params;

void *exceptionThrowingRtcpdThread(void *arg) {
  rtcpdThread_params *threadParams =
    (rtcpdThread_params*)arg;
  castor::tape::utils::SmartFd rtcpdListenSock(threadParams->inListenSocketFd);

  const time_t acceptTimeout = 10; // Timeout is in seconds
  castor::tape::utils::SmartFd connectionFromBridge(
    castor::tape::net::acceptConnection(rtcpdListenSock.get(), acceptTimeout));

  const int netReadWriteTimeout = 10; // Timeout is in seconds
  rtcpClientInfo_t client;
  rtcpTapeRequest_t tapeReq;
  rtcpFileRequest_t fileReq;
  int clientIsTapeBridge = 0;
  char errBuf[1024];
  rtcpd_GetClientInfo(
    connectionFromBridge.get(),
    netReadWriteTimeout,
    &tapeReq,
    &fileReq,
    &client,
    &clientIsTapeBridge,
    &(threadParams->outMsgBody),
    errBuf,
    sizeof(errBuf));
  threadParams->outGetClientInfoSuccess = 1;

  close(connectionFromBridge.release());

  castor::tape::utils::SmartFd
    connectionToBridge(socket(PF_INET, SOCK_STREAM, IPPROTO_TCP));

  if(0 > connectionToBridge.get()) {
    test_exception ex("Failed to create socket");
    throw ex;
  }


  struct sockaddr_in bridgeAddr;
  memset(&bridgeAddr, '\0', sizeof(bridgeAddr));
  bridgeAddr.sin_family      = AF_INET;
  bridgeAddr.sin_addr.s_addr = inet_addr("127.0.0.1");
  bridgeAddr.sin_port        = htons(client.clientport);

  std::cout << "Connecting to 127.0.0.1:" << client.clientport << std::endl;

  try {
    connect_stdException(connectionToBridge.get(),
      (struct sockaddr *)&bridgeAddr, sizeof(bridgeAddr));
  } catch(std::exception &se) {
    std::ostringstream oss;

    oss <<
      "Failed to connect to bridge"
      ": Function=" << __FUNCTION__ <<
      " Line=" << __LINE__ <<
      ": host=127.0.0.1" <<
      " port=" << client.clientport <<
      ": " << se.what();

    test_exception te(oss.str());
    throw te;
  }

  std::cout << "Connected" << std::endl;

  // Read in the message header
  char headerBuf[3 * sizeof(uint32_t)]; // magic + request type + len
  try {
    castor::tape::net::readBytes(connectionToBridge.get(), netReadWriteTimeout,
      sizeof(headerBuf), headerBuf);
  } catch (castor::exception::Exception &ex) {
    TAPE_THROW_CODE(SECOMERR,
         ": Failed to read message header from remote-copy job submitter"
      << ": " << ex.getMessage().str());
  }

  std::cout << "Read in message header" << std::endl;

  // Unmarshal the messager header
  castor::tape::legacymsg::MessageHeader header;
  try {
    const char *p           = headerBuf;
    size_t     remainingLen = sizeof(headerBuf);
    castor::tape::legacymsg::unmarshal(p, remainingLen, header);
  } catch(castor::exception::Exception &ex) {
    TAPE_THROW_CODE(EBADMSG,
      ": Failed to unmarshal message header from remote-copy job submitter"
      ": " << ex.getMessage().str());
  }

  // Length of body buffer = Length of message buffer - length of header
  char bodyBuf[1024];

  // If the message body is too large
  if(header.lenOrStatus > sizeof(bodyBuf)) {
    TAPE_THROW_CODE(EMSGSIZE,
         ": Message body from remote-copy job submitter is too large"
         ": Maximum: " << sizeof(bodyBuf)
      << ": Received: " << header.lenOrStatus);
  }
     
  // Read the message body
  try {
    castor::tape::net::readBytes(connectionToBridge.get(), netReadWriteTimeout,
      header.lenOrStatus, bodyBuf);
  } catch (castor::exception::Exception &ex) {
    TAPE_THROW_CODE(EIO,
         ": Failed to read message body from remote-copy job submitter"
      << ": "<< ex.getMessage().str());
  }

  std::cout << "Read in message body" << std::endl;

  castor::tape::legacymsg::RtcpTapeRqstErrMsgBody request;
  // Unmarshal the message body
  try {
    const char *p           = bodyBuf;
    size_t     remainingLen = header.lenOrStatus;
    castor::tape::legacymsg::unmarshal(p, remainingLen, request);
  } catch(castor::exception::Exception &ex) {
    TAPE_THROW_EX(castor::exception::Internal,
         ": Failed to unmarshal message body from remote-copy job submitter"
      << ": "<< ex.getMessage().str());
  }

  // Marshal the acknowledgement
  castor::tape::legacymsg::MessageHeader ackMsg;
  memset(&ackMsg, '\0', sizeof(ackMsg));
  ackMsg.magic = RTCOPY_MAGIC;
  ackMsg.reqType = RTCP_TAPEERR_REQ;
  ackMsg.lenOrStatus = 0; // Success
  char ackBuf[3 * sizeof(uint32_t)];
  memset(ackBuf, '\0', sizeof(ackBuf));
  size_t totalLen = 0;
  try {
    totalLen = castor::tape::legacymsg::marshal(ackBuf, ackMsg);
  } catch(castor::exception::Exception &ex) {
    TAPE_THROW_EX(castor::exception::Internal,
         ": Failed to marshal acknowledgement: "
      << ex.getMessage().str());
  }

  // Send the acknowledgement
  try {
    castor::tape::net::writeBytes(connectionToBridge.get(), netReadWriteTimeout,
      totalLen, ackBuf);
  } catch(castor::exception::Exception &ex) {
    TAPE_THROW_CODE(SECOMERR,
         ": Failed to acknowledgement to tapebridged: "
      << ex.getMessage().str());
  }

  // Marshall the request info
  castor::tape::legacymsg::RtcpTapeRqstErrMsgBody requestInfoMsgBody;
  memset(&requestInfoMsgBody, '\0', sizeof(requestInfoMsgBody));
  castor::tape::utils::copyString(requestInfoMsgBody.unit, "drive");
  requestInfoMsgBody.volReqId = 1111;
  char requestInfoMsgBuf[1024];
  memset(requestInfoMsgBuf, '\0', sizeof(requestInfoMsgBuf));
  try {
    totalLen = castor::tape::legacymsg::marshal(requestInfoMsgBuf,
      requestInfoMsgBody);
  } catch(castor::exception::Exception &ex) {
    TAPE_THROW_EX(castor::exception::Internal,
         ": Failed to marshal acknowledgement: "
      << ex.getMessage().str());
  }

  // Send the request info to tapebridged
  try {
    castor::tape::net::writeBytes(connectionToBridge.get(), netReadWriteTimeout,
      totalLen, requestInfoMsgBuf);
  } catch(castor::exception::Exception &ex) {
    TAPE_THROW_CODE(SECOMERR,
         ": Failed to send request info to tapebridged: "
      << ex.getMessage().str());
  }

  // Receive acknowledgement from tapebridged
  memset(&ackMsg, '\0', sizeof(ackMsg));
  try {
    castor::tape::tapebridge::LegacyTxRx::receiveMsgHeader(nullCuuid, 1111,
      connectionToBridge.get(), netReadWriteTimeout, ackMsg);
  } catch(castor::exception::Exception &ex) {
    TAPE_THROW_CODE(EPROTO,
         ": Failed to receive acknowledge from RTCPD: "
      << ex.getMessage().str());
  }

  close(connectionToBridge.release());

  return arg;
}


void *rtcpdThread(void *arg) {
  try {
    return exceptionThrowingRtcpdThread(arg);
  } catch(castor::exception::Exception &ce) {
    std::cerr <<
      "ERROR"
      ": " << __FUNCTION__ <<
      ": Caught a castor::exception::Exception"
      ": " << ce.getMessage().str() << std::endl;
  } catch(std::exception &se) {
    std::cerr <<
      "ERROR"
      ": " << __FUNCTION__ <<
      ": Caught an std::exception"
      ": " << se.what() << std::endl;
  } catch(...) {
    std::cerr <<
      "ERROR"
      ": " << __FUNCTION__ <<
      ": Caught an unknown exception";
  }

  return arg;
}

void *exceptionThrowingClientThread(void *arg) {
  return arg;
}

void *clientThread(void *arg) {
  try {
    return exceptionThrowingClientThread(arg);
  } catch(castor::exception::Exception &ce) {
    std::cerr <<
      "ERROR"
      ": " << __FUNCTION__ <<
      ": Caught a castor::exception::Exception"
      ": " << ce.getMessage().str() << std::endl;
  } catch(std::exception &se) {
    std::cerr <<
      "ERROR"
      ": " << __FUNCTION__ <<
      ": Caught an std::exception"
      ": " << se.what() << std::endl;
  } catch(...) {
    std::cerr <<
      "ERROR"
      ": " << __FUNCTION__ <<
      ": Caught an unknown exception";
  }

  return arg;
}


bool pathExists(const char *const path) {
   struct stat fileStat;

   memset(&fileStat, '\0', sizeof(fileStat));

   return 0 == stat(path, &fileStat);
}

/**
 * Forks and execs a tapebridged process and returns its process id.
 */
pid_t forkAndExecTapebridged() throw(std::exception) {
   char *const executable  = "tapebridged";
   pid_t       forkRc      = 0;
   int         saved_errno = 0;

   if(!pathExists(executable)) {
     std::ostringstream oss;

     oss <<
       "File does not exist"
       ": path=" << executable;

     test_exception te(oss.str());

     throw te;
   }

   forkRc = fork();
   saved_errno = errno;

   switch(forkRc) {
   case -1:
     {
       char buf[1024];
       std::ostringstream oss;

       strerror_r(saved_errno, buf, sizeof(buf));
       oss <<
       "Failed to fork child process"
         ": Function=" << __FUNCTION__ <<
         " Line=" << __LINE__ <<
         ": " << buf;

       test_exception te(oss.str());

       throw te;
     }
     break;
   case 0: // Child
     {
       char *const argv[3] = {executable, "-f", NULL};
       execv(argv[0], argv);
     }
     exit(1); // Should never get here so exit with an error
   default: // Parent
     return forkRc;
   }
}

/*
  std::cout << "Killing tapebridged" << std::endl;
  system("killall tapebridged");

  // Give tapebridged a chance to die
  std::cout << "Sleeping 2 seconds" << std::endl;
  sleep(2);

  pid_t bridgePid = 0;
  try {
    bridgePid = forkAndExecTapebridged();
    std::cout << "tapebridged running with pid=" << bridgePid << std::endl;
  } catch(std::exception &ex) {
    std::cerr <<
      "Aborting"
      ": " << ex.what() << std::endl;
    return(1);
  }

  // Give tapebridged a chance to start listening on TAPEBRIDGE_VDQMPORT
  std::cout << "Sleeping 1 second" << std::endl;
  sleep(1);
*/
void executeProtocol(const int clientPort, const int clientListenSockFd) {
  const unsigned short bridgePort = castor::TAPEBRIDGE_VDQMPORT;
  const std::string bridgeHost("127.0.0.1");
  castor::tape::utils::SmartFd clientListenSock(clientListenSockFd);
  bool acknSucc = false;

  castor::vdqm::RemoteCopyConnection connection(bridgePort, bridgeHost);

  std::cout <<
    "Connecting to tapebridged"
    ": bridgeHost=" << bridgeHost <<
    " bridgePort=" << bridgePort << std::endl;
  connection.connect();

  std::cout << "Sending job" << std::endl;
  const Cuuid_t        cuuid          = nullCuuid;
  const char    *const remoteCopyType  = "RTCPD";
  const u_signed64     tapeRequestID   = 1111;
  const std::string    clientUserName  = "pippo";
  const std::string    clientMachine   = "localhost";
  const int            clientEuid      = 3333;
  const int            clientEgid      = 4444;
  const std::string    deviceGroupName = "DGN";
  const std::string    tapeDriveName   = "drive";
  acknSucc = connection.sendJob(
    cuuid,
    remoteCopyType,
    tapeRequestID,
    clientUserName,
    clientMachine,
    clientPort,
    clientEuid,
    clientEgid,
    deviceGroupName,
    tapeDriveName);

  if(!acknSucc) {
    test_exception te("Received a negative acknowledgement from tapebridged");

    throw te;
  }

  const time_t acceptTimeout = 10; // Timeout is in seconds
  castor::tape::utils::SmartFd connectionFromBridge(
    castor::tape::net::acceptConnection(clientListenSock.get(), acceptTimeout));

  // Wrap the connection socket descriptor in a CASTOR framework socket in
  // order to get access to the framework marshalling and un-marshalling
  // methods
  castor::io::AbstractTCPSocket callbackConnectionSock(
    connectionFromBridge.release());

  // Read in the object sent by the tapebridge
  std::auto_ptr<castor::IObject> firstObj(callbackConnectionSock.readObject());

  // Cast the object to its type
  castor::tape::tapegateway::VolumeRequest &volumeRequest =
    dynamic_cast<castor::tape::tapegateway::VolumeRequest&>(*(firstObj.get()));
  std::cout << "Selected drive unit is \"" << volumeRequest.unit() << "\"" <<
    std::endl;

  // Create the volume message for the tapebridge
  castor::tape::tapegateway::Volume volumeMsg;
  volumeMsg.setVid("D12345");
  volumeMsg.setClientType(castor::tape::tapegateway::READ_TP);
  volumeMsg.setMode(castor::tape::tapegateway::READ);
  volumeMsg.setLabel("AUL");
  volumeMsg.setMountTransactionId(1111);
  volumeMsg.setAggregatorTransactionId(1);
  volumeMsg.setDensity("density");

  // Send the volume message to the tapebridge
  callbackConnectionSock.sendObject(volumeMsg);

  // Close the connection to the tapebridge
  callbackConnectionSock.close();
}

int main() {
  int rc = 0;
  char rtcpLogErrTxt[1024];

  rtcp_InitLog(rtcpLogErrTxt,NULL,stderr,NULL);

  // Create a fake client callback socket
  const unsigned short clientPort = 65432;
  castor::tape::utils::SmartFd clientListenSock;
  try {
    const unsigned short lowPort    = clientPort;
    const unsigned short highPort   = clientPort;
    unsigned short       chosenPort = 0;

    clientListenSock.reset(createListenerSock_stdException("127.0.0.1", lowPort,
      highPort, chosenPort));
  } catch(std::exception &ex) {
    std::cerr << "Failed to create fake client callback socket"
      ": port=" << clientPort << std::endl;
    return(1);
  }
  std::cout << "Created a fake client callback socket"
    ": port=" << clientPort << std::endl;

  // Create a fake rtcpd server socket
  castor::tape::utils::SmartFd rtcpdListenSock;
  try {
    const unsigned short lowPort    = RTCOPY_PORT;
    const unsigned short highPort   = RTCOPY_PORT;
    unsigned short       chosenPort = 0;

    rtcpdListenSock.reset(createListenerSock_stdException("127.0.0.1", lowPort,
      highPort, chosenPort));
  } catch(std::exception &ex) {
    std::cerr << "Failed to create fake rtcpd server socket"
      ": port=" << RTCOPY_PORT << std::endl;
    return(1);
  }
  std::cout << "Created a fake rtcpd server socket"
    ": port=" << RTCOPY_PORT << std::endl;

  // Create the rtcpd server-thread
  rtcpdThread_params threadParams;
  memset(&threadParams, '\0', sizeof(threadParams));
  threadParams.inListenSocketFd = rtcpdListenSock.release();
  pthread_t rtcpdThreadId;
  rc = pthread_create(&rtcpdThreadId, NULL, rtcpdThread, &threadParams);
  if(0 != rc) {
    std::cerr << "Failed to create rtcpd server-thread" << std::endl;
    return(1);
  }

  try {
    executeProtocol(clientPort, clientListenSock.release());
  } catch(std::exception &se) {
    std::cerr <<
      "Failed to execute protocol"
      ": Caught an std::exception"
      ": " << se.what() << std::endl;
  } catch(castor::exception::Exception &ce) {
    std::cerr << 
      "Failed to execute protocol"
      ": Caught a castor::exception::Exception"
      ": " << ce.getMessage().str() << std::endl;
  } catch(...) {
    std::cerr <<
      "Failed to execute protocol"
      ": Caught an unknown exception" << std::endl;
  }

  rc = pthread_join(rtcpdThreadId, NULL);
  if(0 != rc) {
    std::cerr << "Failed to join with rtcpd server-thread" << std::endl;
    return(1);
  }

  return 0;
}

