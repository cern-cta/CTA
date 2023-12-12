/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
 */

#include "common/utils/utils.hpp"
#include "mediachanger/SmartFd.hpp"
#include "mediachanger/CommonMarshal.hpp"
#include "mediachanger/io.hpp"
#include "mediachanger/RmcMarshal.hpp"
#include "mediachanger/RmcProxy.hpp"
#include "mediachanger/ScsiLibrarySlot.hpp"

namespace cta::mediachanger {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
RmcProxy::RmcProxy(
  const uint16_t rmcPort,
  const uint32_t netTimeout,
  const uint32_t maxRqstAttempts):
  m_rmcPort(rmcPort),
  m_netTimeout(netTimeout),
  m_maxRqstAttempts(maxRqstAttempts) {
} 

//------------------------------------------------------------------------------
// mountTapeReadOnly
//------------------------------------------------------------------------------
void RmcProxy::mountTapeReadOnly(const std::string &vid, const LibrarySlot &librarySlot) {
  // SCSI libraries do not support read-only mounts
  mountTapeReadWrite(vid, librarySlot);
}

//------------------------------------------------------------------------------
// mountTapeReadWrite
//------------------------------------------------------------------------------
void RmcProxy::mountTapeReadWrite(const std::string &vid, const LibrarySlot &librarySlot) {
  try {
    RmcMountMsgBody rqstBody;
    rqstBody.uid = geteuid();
    rqstBody.gid = getegid();
    utils::copyString(rqstBody.vid, vid);
    const ScsiLibrarySlot &scsiLibrarySlot = dynamic_cast<const ScsiLibrarySlot&>(librarySlot);
    rqstBody.drvOrd = scsiLibrarySlot.getDrvOrd();

    rmcSendRecvNbAttempts(m_maxRqstAttempts, rqstBody);
  } catch(cta::exception::Exception &ne) {
    cta::exception::Exception ex;
    ex.getMessage() <<
      "Failed to mount tape in SCSI tape-library for read/write access"
      ": vid=" << vid << " librarySlot=" << librarySlot.str() << ": " <<
      ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// dismountTape
//------------------------------------------------------------------------------
void RmcProxy::dismountTape(const std::string &vid, const LibrarySlot &librarySlot) {
  try {
    RmcUnmountMsgBody rqstBody;
    rqstBody.uid = geteuid();
    rqstBody.gid = getegid();
    utils::copyString(rqstBody.vid, vid);
    const ScsiLibrarySlot &scsiLibrarySlot = dynamic_cast<const ScsiLibrarySlot&>(librarySlot);
    rqstBody.drvOrd = scsiLibrarySlot.getDrvOrd();
    rqstBody.force = 0;

    rmcSendRecvNbAttempts(m_maxRqstAttempts, rqstBody);
  } catch(cta::exception::Exception &ne) {
    cta::exception::Exception ex;
    ex.getMessage() <<
      "Failed to dismount tape in SCSI tape-library"
      ": vid=" << vid << " librarySlot=" << librarySlot.str() << ": " <<
      ne.getMessage().str();
    throw ex;
  }
}

//-----------------------------------------------------------------------------
// connectToRmc
//-----------------------------------------------------------------------------
int RmcProxy::connectToRmc()
  const {
  const std::string rmcHost = "localhost";
  cta::SmartFd smartConnectSock;
  try {
    smartConnectSock.reset(connectWithTimeout(rmcHost, m_rmcPort,
      m_netTimeout));
  } catch(cta::exception::Exception &ne) {
    cta::exception::Exception ex;
    ex.getMessage() << "Failed to connect to rmcd: rmcHost=" << rmcHost
      << " rmcPort=" << RMC_PORT << ": " << ne.getMessage().str();
    throw ex;
  }

  return smartConnectSock.release();
}

//-----------------------------------------------------------------------------
// readRmcMsgHeader
//-----------------------------------------------------------------------------
MessageHeader RmcProxy::readRmcMsgHeader(const int fd) {
  char buf[12]; // Magic + type + len
  MessageHeader header;

  try {
    readBytes(fd, m_netTimeout, sizeof(buf), buf);
  } catch(cta::exception::Exception &ne) {
    cta::exception::Exception ex;
    ex.getMessage() << "Failed to read message header: "
      << ne.getMessage().str();
    throw ex;
  }

  const char *bufPtr = buf;
  size_t bufLen = sizeof(buf);
  unmarshal(bufPtr, bufLen, header);

  if(RMC_MAGIC != header.magic) {
    cta::exception::Exception ex;
    ex.getMessage() << "Failed to read message header: "
      " Header contains an invalid magic number: expected=0x" << std::hex <<
      RMC_MAGIC << " actual=0x" << header.magic;
    throw ex;
  }

  return header;
}

//-----------------------------------------------------------------------------
// rmcReplyTypeToStr
//-----------------------------------------------------------------------------
std::string RmcProxy::rmcReplyTypeToStr(const int replyType) {
  std::ostringstream oss;
  switch(replyType) {
  case RMC_RC:
    oss << "RMC_RC";
    break;
  case MSG_ERR:
    oss << "MSG_ERR";
    break;
  default:
    oss << "UNKNOWN(0x" << std::hex << replyType << ")";
  }
  return oss.str();
}

//-----------------------------------------------------------------------------
// handleMSG_ERR
//-----------------------------------------------------------------------------
std::string RmcProxy::handleMSG_ERR(const MessageHeader &header, const int fd) {
  char errorBuf[1024];
  const int nbBytesToRead = header.lenOrStatus > sizeof(errorBuf) ?
    sizeof(errorBuf) : header.lenOrStatus;
  readBytes(fd, m_netTimeout, nbBytesToRead, errorBuf);
  errorBuf[sizeof(errorBuf) - 1] = '\0';
  return errorBuf;
}

} // namespace cta::mediachanger
