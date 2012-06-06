/******************************************************************************
 *                castor/tape/tapebridge/LegacyTxRx.cpp
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

#include "castor/Constants.hpp"
#include "castor/dlf/Dlf.hpp"
#include "castor/exception/Internal.hpp"
#include "castor/tape/legacymsg/CommonMarshal.hpp"
#include "castor/tape/net/net.hpp"
#include "castor/tape/tapebridge/DlfMessageConstants.hpp"
#include "castor/tape/tapebridge/Constants.hpp"
#include "castor/tape/tapebridge/LegacyTxRx.hpp"
#include "castor/tape/utils/utils.hpp"
#include "h/common.h"

#include <sstream>
#include <string.h>


//-----------------------------------------------------------------------------
// constructor
//-----------------------------------------------------------------------------
castor::tape::tapebridge::LegacyTxRx::LegacyTxRx(const int netReadWriteTimeout)
  throw(): m_netReadWriteTimeout(netReadWriteTimeout) {
  // Do nothing
}


//-----------------------------------------------------------------------------
// destructor
//-----------------------------------------------------------------------------
castor::tape::tapebridge::LegacyTxRx::~LegacyTxRx() throw() {
  // Do nothing
}


//-----------------------------------------------------------------------------
// getNetReadWriteTimeout
//-----------------------------------------------------------------------------
int castor::tape::tapebridge::LegacyTxRx::getNetReadWriteTimeout() const
  throw() {
  return m_netReadWriteTimeout;
}


//-----------------------------------------------------------------------------
// sendMsgHeader
//-----------------------------------------------------------------------------
void castor::tape::tapebridge::LegacyTxRx::sendMsgHeader(
  const int                      socketFd,
  const legacymsg::MessageHeader &header)
  throw(castor::exception::Exception) {

  char buf[RTCPMSGBUFSIZE];
  size_t totalLen = 0;

  try {
    totalLen = legacymsg::marshal(buf, header);
  } catch(castor::exception::Exception &ex) {
    TAPE_THROW_EX(castor::exception::Internal,
         ": Failed to marshal RCP acknowledge message: "
      << ex.getMessage().str());
  }

  try {
    net::writeBytes(socketFd, m_netReadWriteTimeout, totalLen, buf);
  } catch(castor::exception::Exception &ex) {
    TAPE_THROW_CODE(SECOMERR,
      ": Failed to send message header to RTCPD"
      ": " << ex.getMessage().str());
  }
}


//-----------------------------------------------------------------------------
// receiveMsgHeader
//-----------------------------------------------------------------------------
void castor::tape::tapebridge::LegacyTxRx::receiveMsgHeader(
  const int                socketFd,
  legacymsg::MessageHeader &header)
  throw(castor::exception::Exception) {

  // Read in the message header
  char headerBuf[3 * sizeof(uint32_t)]; // magic + request type + len
  try {
    net::readBytes(socketFd, m_netReadWriteTimeout, sizeof(headerBuf),
      headerBuf);
  } catch (castor::exception::Exception &ex) {
    TAPE_THROW_CODE(SECOMERR,
         ": Failed to read message header from RTCPD"
      << ": " << ex.getMessage().str());
  }

  // Unmarshal the messager header
  try {
    const char *p           = headerBuf;
    size_t     remainingLen = sizeof(headerBuf);
    legacymsg::unmarshal(p, remainingLen, header);
  } catch(castor::exception::Exception &ex) {
    TAPE_THROW_CODE(EBADMSG,
       ": Failed to unmarshal message header from RTCPD"
       ": " << ex.getMessage().str());
  }
}


//-----------------------------------------------------------------------------
// receiveMsgHeaderFromCloseableConn
//-----------------------------------------------------------------------------
bool castor::tape::tapebridge::LegacyTxRx::receiveMsgHeaderFromCloseable(
  const int                socketFd,
  legacymsg::MessageHeader &header) throw(castor::exception::Exception) {

  bool connClosed = false;

  // Read in the message header
  char headerBuf[3 * sizeof(uint32_t)]; // magic + request type + len
  try {
    connClosed = net::readBytesFromCloseable(socketFd, m_netReadWriteTimeout, 
      sizeof(headerBuf), headerBuf);
  } catch (castor::exception::Exception &ex) {
    TAPE_THROW_CODE(SECOMERR,
         ": Failed to read message header from RTCPD"
      << ": " << ex.getMessage().str());
  }

  // Unmarshal the messager header
  try {
    const char *p           = headerBuf;
    size_t     remainingLen = sizeof(headerBuf);
    legacymsg::unmarshal(p, remainingLen, header);
  } catch(castor::exception::Exception &ex) {
    TAPE_THROW_CODE(EBADMSG,
       ": Failed to unmarshal message header from RTCPD"
       ": " << ex.getMessage().str());
  }

  return connClosed;
}
