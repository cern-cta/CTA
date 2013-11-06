/******************************************************************************
 *                      castor/tape/tapebridge/BridgeClientInfo2Sender.cpp
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
 * @author Steven.Murray@cern.ch
 *****************************************************************************/

#include "castor/exception/Internal.hpp"
#include "castor/exception/InvalidArgument.hpp"
#include "castor/io/ClientSocket.hpp"
#include "castor/tape/legacymsg/CommonMarshal.hpp"
#include "castor/tape/legacymsg/MessageHeader.hpp"
#include "castor/tape/tapebridge/Constants.hpp"
#include "castor/tape/tapebridge/BridgeClientInfo2Sender.hpp"
#include "castor/tape/net/net.hpp"
#include "castor/tape/utils/utils.hpp"
#include "h/net.h"
#include "h/osdep.h"
#include "h/rtcp_constants.h"
#include "h/serrno.h"
#include "h/tapeBridgeClientInfo2MsgBody.h"
#include "h/tapebridge_constants.h"
#include "h/tapebridge_marshall.h"
#include "h/vdqm_constants.h"

#include <errno.h>
#include <string.h>
#include <unistd.h>


//------------------------------------------------------------------------------
// send
//------------------------------------------------------------------------------
void castor::tape::tapebridge::BridgeClientInfo2Sender::send(
  const std::string              &rtcpdHost,
  const unsigned int             rtcpdPort,
  const int                      netReadWriteTimeout,
  tapeBridgeClientInfo2MsgBody_t &msgBody,
  legacymsg::RtcpJobReplyMsgBody &reply)
  throw(castor::exception::Exception) {

  if(rtcpdHost.length() == 0) {
    TAPE_THROW_EX(castor::exception::InvalidArgument,
      ": Length of rtcpdHost string is 0");
  }

  char buf[RTCPMSGBUFSIZE];
  const int hdrPlusBodyLen = tapebridge_marshallTapeBridgeClientInfo2Msg(buf,
    sizeof(buf), &msgBody);
  int save_serrno = serrno;

  if(hdrPlusBodyLen < 0) {
    TAPE_THROW_CODE(save_serrno,
      ": Failed to marhsall tapeBridgeClientInfo2MsgBody_t"
      ": " << sstrerror(save_serrno));
  }

  // Connect to the rtcpd daemon
  castor::io::ClientSocket sock(rtcpdPort, rtcpdHost);
  sock.setTimeout(netReadWriteTimeout);
  try {
    sock.connect();
  } catch(castor::exception::Exception &ex) {
    TAPE_THROW_CODE(ECONNABORTED,
      ": Failed to connect to rtcpd"
      ": rtcpdHost: " << rtcpdHost <<
      ": rtcpdPort: " << rtcpdPort <<
      ": " << ex.getMessage().str());
  }

  // Send the job submission request message to the rtcpd or tape tapebridge
  // daemon
  try {
    net::writeBytes(sock.socket(), netReadWriteTimeout, hdrPlusBodyLen, buf);
  } catch(castor::exception::Exception &ex) {
    TAPE_THROW_CODE(SECOMERR,
      ": Failed to send the TAPEBRIDGE_CLIENTINFO2 message to rtcpd" <<
      ": " << ex.getMessage().str());
  }

  // Read the reply from rtcpd daemon
  readReply(sock, netReadWriteTimeout, reply);
}


//------------------------------------------------------------------------------
// readReply
//------------------------------------------------------------------------------
void castor::tape::tapebridge::BridgeClientInfo2Sender::readReply(
  castor::io::AbstractTCPSocket  &sock,
  const int                      netReadWriteTimeout,
  legacymsg::RtcpJobReplyMsgBody &reply)
  throw(castor::exception::Exception) {

  // Read in the message header
  char headerBuf[3 * sizeof(uint32_t)]; // magic + request type + len
  try {
    net::readBytes(sock.socket(), netReadWriteTimeout, sizeof(headerBuf),
      headerBuf);
  } catch (castor::exception::Exception &ex) {
    TAPE_THROW_CODE(SECOMERR,
      ": Failed to read message header from rtcpd"
      ": " << ex.getMessage().str());
  }

  // Unmarshal the messager header
  legacymsg::MessageHeader header;
  try {
    const char *p           = headerBuf;
    size_t     remainingLen = sizeof(headerBuf);
    legacymsg::unmarshal(p, remainingLen, header);
  } catch(castor::exception::Exception &ex) {
    TAPE_THROW_CODE(EBADMSG,
      ": Failed to unmarshal message header from rtcpd"
      ": " << ex.getMessage().str());
  }

  // If the magic number is invalid
  if(header.magic != RTCOPY_MAGIC && header.magic != RTCOPY_MAGIC_OLD0) {
    TAPE_THROW_CODE(EBADMSG,
       std::hex <<
       ": Invalid magic number from rtcpd"
       ": Expected: 0x" << RTCOPY_MAGIC << " or 0x" << RTCOPY_MAGIC_OLD0 <<
       ": Received: 0x" << header.magic);
  }

  // If the request type is invalid
  if(header.reqType != TAPEBRIDGE_CLIENTINFO2 &&
    header.reqType != VDQM_CLIENTINFO) {
    TAPE_THROW_CODE(EBADMSG,
      std::hex <<
      ": Invalid request type from rtpcd"
      ": Expected: either TAPEBRIDGE_CLIENTINFO2=0x" <<
      TAPEBRIDGE_CLIENTINFO2 <<
      " or VDQM_CLIENTINFO=0x" << VDQM_CLIENTINFO <<
      ": Received: 0x" << header.reqType);
  }

  // If the request type is VDQM_CLIENTINFO then raise an exception because
  // this means the rtcpd daemon was unable to determine that its client is the
  // tapebridged daemon.  It has therefore replied assuming the client is the
  // vdqmd daemon.
  if(header.reqType == VDQM_CLIENTINFO) {
    TAPE_THROW_CODE(EBADMSG,
      ": rtcpd could not determine its client is in fact tapebridged.");
  }

  // If the message body is not large enough for a status number and an empty
  // error string
  {
    const size_t minimumLen = sizeof(uint32_t) + 1;
    if(header.lenOrStatus < minimumLen) {
      TAPE_THROW_CODE(EMSGSIZE,
        ": Invalid header from rtcpd"
        ": Message body not large enough for a status number and an empty "
        "string: Minimum size expected: " << minimumLen <<
        ": Received: " << header.lenOrStatus);
    }
  }

  // Length of message header = 3 * sizeof(uint32_t)
  char bodyBuf[RTCPMSGBUFSIZE - 3 * sizeof(uint32_t)];

  // If the message body is larger than the message body buffer
  if(header.lenOrStatus > sizeof(bodyBuf)) {
    TAPE_THROW_CODE(EMSGSIZE,
      ": Message body from rtcpd is too large" <<
      ": Maximum: " << sizeof(bodyBuf) <<
      ": Received: " << header.lenOrStatus);
  }

  // Read the message body
  try {
    net::readBytes(sock.socket(), netReadWriteTimeout, header.lenOrStatus,
      bodyBuf);
  } catch (castor::exception::Exception &ex) {
    TAPE_THROW_CODE(SECOMERR,
      ": Failed to read message body from rtpcd"
      ": " << ex.getMessage().str());
  }

  // Unmarshal the job submission reply
  try {
    const char *p           = bodyBuf;
    size_t     remainingLen = header.lenOrStatus;
    legacymsg::unmarshal(p, remainingLen, reply);
  } catch(castor::exception::Exception &ex) {
    TAPE_THROW_EX(castor::exception::Internal,
      ": Failed to unmarshal job submission reply"
      ": " << ex.getMessage().str());
  }
}
