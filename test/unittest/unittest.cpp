/******************************************************************************
 *                test/unittest/unittest.cpp
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

#include "castor/tape/Constants.hpp"
#include "castor/tape/legacymsg/CommonMarshal.hpp"
#include "castor/tape/legacymsg/RtcpMarshal.hpp"
#include "castor/tape/net/net.hpp"
#include "castor/tape/tapebridge/Constants.hpp"
#include "castor/tape/utils/SmartFd.hpp"
#include "h/rtcp_constants.h"
#include "test/unittest/unittest.hpp"
#include "test/unittest/test_exception.hpp"

#include <errno.h>
#include <memory>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>
#include <utility>


//-----------------------------------------------------------------------------
// createLocalListenSocket
//-----------------------------------------------------------------------------
int unittest::createLocalListenSocket(const char *const listenSockPath)
  throw (std::exception) {
  using namespace castor::tape;

  // Delete the file to be used for the socket if the file already exists
  unlink(listenSockPath);

  // Create the socket
  utils::SmartFd smartListenSock(socket(PF_LOCAL, SOCK_STREAM, 0));
  if(-1 == smartListenSock.get()) {
    char strErrBuf[256];
    if(0 != strerror_r(errno, strErrBuf, sizeof(strErrBuf))) {
      memset(strErrBuf, '\0', sizeof(strErrBuf));
      strncpy(strErrBuf, "Unknown", sizeof(strErrBuf) - 1);
    }

    std::string errorMessage("Call to socket() failed: ");
    errorMessage += strErrBuf;

    test_exception te(errorMessage);
    throw te;
  }

  // Bind the socket
  {
    struct sockaddr_un listenAddr;
    memset(&listenAddr, '\0', sizeof(listenAddr));
    listenAddr.sun_family = PF_LOCAL;
    strncpy(listenAddr.sun_path, listenSockPath,
      sizeof(listenAddr.sun_path) - 1);

    if(0 != bind(smartListenSock.get(), (const struct sockaddr *)&listenAddr,
      sizeof(listenAddr))) {
      char strErrBuf[256];
      if(0 != strerror_r(errno, strErrBuf, sizeof(strErrBuf))) {
        memset(strErrBuf, '\0', sizeof(strErrBuf));
        strncpy(strErrBuf, "Unknown", sizeof(strErrBuf) - 1);
      }

      std::string errorMessage("Call to bind() failed: ");
      errorMessage += strErrBuf;

      test_exception te(errorMessage);
      throw te;
    }
  }

  // Make the socket listen
  {
    const int backlog = 128;
    if(0 != listen(smartListenSock.get(), backlog)) {
      char strErrBuf[256];
      if(0 != strerror_r(errno, strErrBuf, sizeof(strErrBuf))) {
        memset(strErrBuf, '\0', sizeof(strErrBuf));
        strncpy(strErrBuf, "Unknown", sizeof(strErrBuf) - 1);
      }

      std::string errorMessage("Call to listen() failed: ");
      errorMessage += strErrBuf;

      test_exception te(errorMessage);
      throw te;
    }
  }

  return smartListenSock.release();
}


//-----------------------------------------------------------------------------
// readInAckAndDropNRtcopyMsgs
//-----------------------------------------------------------------------------
void *unittest::readInAckAndDropNRtcopyMsgs(void *sockFdAndNMsgsPtr) {
  std::pair<int, int> sockFdAndNMsgs =
    *((std::pair<int, int> *)sockFdAndNMsgsPtr);

  int sockFd = sockFdAndNMsgs.first;
  int nMsgs  = sockFdAndNMsgs.second;

  for(int i=0; i < nMsgs; i++) {
    delete[] (char *)readInAndAckRtcopyMsgHeaderAndBody(&sockFd);
  }

  return NULL;
}


//-----------------------------------------------------------------------------
// readInAndAckRtcopyMsgHeaderAndBody
//-----------------------------------------------------------------------------
void *unittest::readInAndAckRtcopyMsgHeaderAndBody(void *sockFdPtr) {
  using namespace castor::tape;

  const int sockFd  = *((int *)sockFdPtr);
  const int timeout = 5; // Timeout in seconds

  // Read out the RTCOPY message header
  char headerBuf[12]; // uint32_t magic + uint32_t reqType + uint32_t len
  memset(headerBuf, '\0', sizeof(headerBuf));
  try {
    net::readBytes(sockFd, timeout, sizeof(headerBuf), headerBuf);
  } catch(...) {
    return NULL; // There was an error reading
  }

  // Unmarshall the RTCOPY message header
  uint32_t magic   = 0;
  uint32_t reqType = 0;
  uint32_t bodyLen = 0;
  try {
    const char *srcPtr = headerBuf;
    size_t     srcLen  = sizeof(headerBuf);
    legacymsg::unmarshalUint32(srcPtr, srcLen, magic  );
    legacymsg::unmarshalUint32(srcPtr, srcLen, reqType);
    legacymsg::unmarshalUint32(srcPtr, srcLen, bodyLen);
  } catch(...) {
    return NULL;  // There as an error un-marshalling
  }

  // Allocate the raw data-block for both the message header and body
  char *const headerAndBody = new char[sizeof(headerBuf) + bodyLen];

  // Copy the header into the raw data-block
  memcpy(headerAndBody, headerBuf, sizeof(headerBuf));

  // If there is a message body to be read in
  if(bodyLen > 0) {
    // Read the RTCOPY message body into the raw data-block immediately after
    // the header
    if(bodyLen !=
      read(sockFd, headerAndBody + sizeof(headerBuf), bodyLen)) {
      delete[] headerAndBody;
      return NULL; // There was an error reading
    }
  }

  // Write back a positive acknowlegement
  try {
    writeRtcopyAck(sockFd, magic, reqType);
  } catch(std::exception &se) {
    delete[] headerAndBody;
    return NULL; // There was an error when sending the acknowledgement
  }

  return headerAndBody;
}


//-----------------------------------------------------------------------------
// writeRtcopyAck
//-----------------------------------------------------------------------------
void unittest::writeRtcopyAck(
  const int      sockFd,
  const uint32_t magic,
  const uint32_t reqType)
  throw(std::exception) {
  using namespace castor::tape;

  char ackBuf[12];

  memset(ackBuf, '\0', sizeof(ackBuf));

  char *p = ackBuf;
  legacymsg::marshalUint32(magic  , p); // Magic number
  legacymsg::marshalUint32(reqType, p); // Request type
  legacymsg::marshalUint32(0      , p); // Postive acknowledgement

  const int timeout = 1;
  net::writeBytes(sockFd, timeout, sizeof(ackBuf), ackBuf);
}


//-----------------------------------------------------------------------------
// connectionHasBeenClosedByPeer
//-----------------------------------------------------------------------------
bool unittest::connectionHasBeenClosedByPeer(const int sockFd)
  throw(std::exception) {
  fd_set readFds;
  FD_ZERO(&readFds);
  FD_SET(sockFd, &readFds);
  int selectRc = -1;

  {
    const int nfds = sockFd + 1;
    struct timeval selectTimeout = {0, 0};
    selectRc = select(nfds, &readFds, NULL, NULL, &selectTimeout);
    const int saved_serrno = 0;

    if(-1 == selectRc) {
      std::string errorMessage =
        "checkConnectionHasBeenClosedByPeer() failed"
        ": select() failed"
        ": ";
      char strerrbuf[256];
      memset(strerrbuf, '\0', sizeof(strerrbuf));
      if(0 != strerror_r(saved_serrno, strerrbuf, sizeof(strerrbuf) - 1)) {
        strncpy(strerrbuf, "Unknown", sizeof(strerrbuf) - 1);
      }

      test_exception te(errorMessage + strerrbuf);
      throw(te);
    }
  }

  if(1 != selectRc) {
    return false;
  }

  if(!FD_ISSET(sockFd, &readFds)) {
    return false;
  }

  char buf[1];

  ssize_t readRc = -1;
  {
    readRc = read(sockFd, buf, sizeof(buf));
    const int saved_serrno = 0;

    if(-1 == selectRc) {
      std::string errorMessage =
        "checkConnectionHasBeenClosedByPeer() failed"
        ": read() failed"
        ": ";
      char strerrbuf[256];
      memset(strerrbuf, '\0', sizeof(strerrbuf));
      if(0 != strerror_r(saved_serrno, strerrbuf, sizeof(strerrbuf) - 1)) {
        strncpy(strerrbuf, "Unknown", sizeof(strerrbuf) - 1);
      }

      test_exception te(errorMessage + strerrbuf);
      throw(te);
    }
  }

  if(0 != readRc) {
    return false;
  }

  return true;
}


//-----------------------------------------------------------------------------
// readAck
//-----------------------------------------------------------------------------
void unittest::readAck(
  const int      sockFd,
  const uint32_t magic,
  const uint32_t reqType,
  const uint32_t status)
  throw(std::exception) {
  using namespace castor::tape;

  const int netReadWriteTimeout = 1;
  char ackBuf[3 * sizeof(uint32_t)];
  memset(ackBuf, '\0', sizeof(ackBuf));

  try {
    net::readBytes(sockFd, netReadWriteTimeout, sizeof(ackBuf), ackBuf);
  } catch (castor::exception::Exception &ex) {
    std::ostringstream errorMessage;
    errorMessage <<
      __FUNCTION__ << " failed"
      ": net::readBytes() failed"
      ": " << ex.getMessage().str();
    test_exception te(errorMessage.str());
    throw te;
  }

  legacymsg::MessageHeader header;
  memset(&header, '\0', sizeof(header));
  {
    const char *src    = ackBuf;
    size_t      srcLen = sizeof(ackBuf);

    legacymsg::unmarshal(src, srcLen, header);
  }

  if(magic != header.magic) {
    std::ostringstream errorMessage;
    errorMessage <<
      __FUNCTION__ << " failed"
      ": Invalid magic number"
      ": expected=0x" << std::hex << magic <<
      " actual=0x" << header.magic;
    test_exception te(errorMessage.str());
    throw(te);
  }

  if(reqType != header.reqType) {
    std::ostringstream errorMessage;
    errorMessage <<
      __FUNCTION__ << " failed"
      ": Invalid request-type"
      ": expected=0x" << std::hex << reqType <<
      " actual=0x" << header.reqType;
    test_exception te(errorMessage.str());
    throw(te);
  }

  if(status != header.lenOrStatus) {
    std::ostringstream errorMessage;
    errorMessage <<
      __FUNCTION__ << " failed"
      ": Invalid status"
      ": expected=" << status <<
      " actual=" << header.lenOrStatus;
    test_exception te(errorMessage.str());
    throw(te);
  }
}


//-----------------------------------------------------------------------------
// writeRTCP_REQUEST_MORE_WORK
//-----------------------------------------------------------------------------
void unittest::writeRTCP_REQUEST_MORE_WORK(
  const int         sockFd,
  const uint32_t    volReqId,
  const char *const tapePath)
  throw(std::exception) {
  using namespace castor::tape;

  legacymsg::RtcpFileRqstErrMsgBody msgBody;
  memset(&msgBody, '\0', sizeof(msgBody));
  strncpy(msgBody.rqst.tapePath, tapePath, sizeof(msgBody.rqst.tapePath) - 1);
  msgBody.rqst.volReqId       =  volReqId;
  msgBody.rqst.jobId          = -1;
  msgBody.rqst.stageSubReqId  = -1;
  msgBody.rqst.umask          = RTCOPYCONSERVATIVEUMASK;
  msgBody.rqst.positionMethod = -1;
  msgBody.rqst.tapeFseq       = -1;
  msgBody.rqst.diskFseq       = -1;
  msgBody.rqst.blockSize      = -1;
  msgBody.rqst.recordLength   = -1;
  msgBody.rqst.retention      = -1;
  msgBody.rqst.defAlloc       = -1;
  msgBody.rqst.rtcpErrAction  = -1;
  msgBody.rqst.tpErrAction    = -1;
  msgBody.rqst.convert        = ASCCONV;
  msgBody.rqst.checkFid       = -1;
  msgBody.rqst.concat         =  1;
  msgBody.rqst.procStatus     = RTCP_REQUEST_MORE_WORK;
  msgBody.err.severity        = RTCP_OK;
  msgBody.err.maxTpRetry      = 1;
  msgBody.err.maxCpRetry      = 1;

  char buf[tapebridge::RTCPMSGBUFSIZE];
  size_t totalLen = 0;
  try {
    totalLen = legacymsg::marshal(buf, msgBody);
  } catch(castor::exception::Exception &ex) {
    std::string errorMessage =
      "writeRTCP_REQUEST_MORE_WORK() failed"
      ": legacymsg::marshal() failed";
    test_exception te(errorMessage + ex.getMessage().str());
    throw te;
  }

  try {
    const int netReadWriteTimeout = 1;
    net::writeBytes(sockFd, netReadWriteTimeout, totalLen, buf);
  } catch(castor::exception::Exception &ex) {
    std::string errorMessage =
      "writeRTCP_REQUEST_MORE_WORK() failed"
      ": net::writeBytes() failed";
    test_exception te(errorMessage + ex.getMessage().str());
    throw te;
  }
}


//-----------------------------------------------------------------------------
// writeRTCP_ENDOF_REQ
//-----------------------------------------------------------------------------
void unittest::writeRTCP_ENDOF_REQ(const int sockFd)
  throw(std::exception) {
  using namespace castor::tape;

  castor::tape::legacymsg::MessageHeader endofReqMsg;
  endofReqMsg.magic       = RTCOPY_MAGIC;
  endofReqMsg.reqType     = RTCP_ENDOF_REQ;
  endofReqMsg.lenOrStatus = 0;
  char endofReqMsgBuf[3 * sizeof(uint32_t)];
  memset(endofReqMsgBuf, '\0', sizeof(endofReqMsgBuf));
  size_t totalLen = 0;
  const int netReadWriteTimeout = 1;

  try {
    totalLen = legacymsg::marshal(endofReqMsgBuf, endofReqMsg);
  } catch(castor::exception::Exception &ex) {
    test_exception te(
      "Failed to marshall RTCP_ENDOF_REQ"
      ": " + ex.getMessage().str());
    throw te;
  }

  if(12 != totalLen) {
    test_exception te(
      "Failed to marshall RTCP_ENDOF_REQ"
      ": totalLen is not 12");
    throw te;
  }

  try {
    net::writeBytes(sockFd, netReadWriteTimeout, totalLen, endofReqMsgBuf);
  } catch(castor::exception::Exception &ex) {
    test_exception te(
      "writeBytes of RTCP_ENDOF_REQ failed"
      ": " + ex.getMessage().str());
    throw te;
  }
}


//-----------------------------------------------------------------------------
// writeRTCP_FINISHED
//-----------------------------------------------------------------------------
void unittest::writeRTCP_FINISHED(
  const int         sockFd,
  const uint32_t    volReqId,
  const char *const tapePath,
  const int32_t     positionMethod,
  const int32_t     tapeFseq,
  const int32_t     diskFseq,
  const uint64_t    bytesIn,
  const uint64_t    bytesOut,
  const struct castor::tape::legacymsg::RtcpSegmentAttributes &segAttr)
  throw(std::exception) {
  using namespace castor::tape;

  legacymsg::RtcpFileRqstErrMsgBody msgBody;
  memset(&msgBody, '\0', sizeof(msgBody));
  strncpy(msgBody.rqst.tapePath, tapePath, sizeof(msgBody.rqst.tapePath) - 1);
  msgBody.rqst.volReqId       =  volReqId;
  msgBody.rqst.jobId          = -1;
  msgBody.rqst.stageSubReqId  = -1;
  msgBody.rqst.umask          = RTCOPYCONSERVATIVEUMASK;
  msgBody.rqst.positionMethod = positionMethod;
  msgBody.rqst.tapeFseq       = tapeFseq;
  msgBody.rqst.diskFseq       = diskFseq;
  msgBody.rqst.blockSize      = -1;
  msgBody.rqst.recordLength   = -1;
  msgBody.rqst.retention      = -1;
  msgBody.rqst.defAlloc       = -1;
  msgBody.rqst.rtcpErrAction  = -1;
  msgBody.rqst.tpErrAction    = -1;
  msgBody.rqst.convert        = ASCCONV;
  msgBody.rqst.checkFid       = -1;
  msgBody.rqst.concat         =  1;
  msgBody.rqst.procStatus     =  RTCP_FINISHED;
  msgBody.rqst.bytesIn        = bytesIn;
  msgBody.rqst.bytesOut       = bytesOut;
  msgBody.rqst.segAttr        = segAttr;
  msgBody.err.severity        =  RTCP_OK;
  msgBody.err.maxTpRetry      = 1;
  msgBody.err.maxCpRetry      = 1;

  char buf[tapebridge::RTCPMSGBUFSIZE];
  size_t totalLen = 0;
  try {
    totalLen = legacymsg::marshal(buf, msgBody);
  } catch(castor::exception::Exception &ex) {
    std::string errorMessage =
      "writeRTCP_FINISHED() failed"
      ": legacymsg::marshal() failed";
    test_exception te(errorMessage + ex.getMessage().str());
    throw te;
  }

  try {
    const int netReadWriteTimeout = 1;
    net::writeBytes(sockFd, netReadWriteTimeout, totalLen, buf);
  } catch(castor::exception::Exception &ex) {
    std::string errorMessage =
      "writeRTCP_FINISHED failed"
      ": net::writeBytes() failed";
    test_exception te(errorMessage + ex.getMessage().str());
    throw te;
  }
}


//-----------------------------------------------------------------------------
// netAcceptConnection
//-----------------------------------------------------------------------------
int unittest::netAcceptConnection(
    const int    listenSockFd,
    const time_t &timeout) 
    throw(std::exception) {
  using namespace castor::tape;

  try {
    return net::acceptConnection(listenSockFd, timeout);
  } catch(castor::exception::Exception &ex) {
    test_exception te(ex.getMessage().str());
    throw te;
  } catch(std::exception &se) {
    throw se;
  } catch(...) {
    test_exception te("Caught an unknown exception");
    throw te;
  }
}
