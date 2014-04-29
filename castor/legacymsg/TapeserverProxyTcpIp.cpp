/******************************************************************************
 *                castor/legacymsg/TapeserverProxyTcpIp.cpp
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
 * @author dkruse@cern.ch
 *****************************************************************************/

#include "castor/exception/Internal.hpp"
#include "castor/io/io.hpp"
#include "castor/legacymsg/CommonMarshal.hpp"
#include "castor/legacymsg/TapeMarshal.hpp"
#include "castor/legacymsg/TapeserverProxyTcpIp.hpp"
#include "castor/utils/SmartFd.hpp"
#include "castor/utils/utils.hpp"
#include "h/rtcp_constants.h"
#include "h/vdqm_constants.h"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::legacymsg::TapeserverProxyTcpIp::TapeserverProxyTcpIp(log::Logger &log, const unsigned short tapeserverPort, const int netTimeout) throw():
  m_log(log),
  m_tapeserverHostName("localhost"),
  m_tapeserverPort(tapeserverPort),
  m_netTimeout(netTimeout) {
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
castor::legacymsg::TapeserverProxyTcpIp::~TapeserverProxyTcpIp() throw() {
}

//------------------------------------------------------------------------------
// setVidInDriveCatalogue
//------------------------------------------------------------------------------
void castor::legacymsg::TapeserverProxyTcpIp::setVidInDriveCatalogue(const std::string &vid, const std::string &unitName) throw(castor::exception::Exception) {
  try {
    legacymsg::SetVidRequestMsgBody body;
    castor::utils::copyString(body.vid, vid.c_str());
    castor::utils::copyString(body.drive, unitName.c_str()); 
    castor::utils::SmartFd fd(connectToTapeserver());
    writeSetVidRequestMsg(fd.get(), body);
    readCommitAck(fd.get());  
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to set vid of tape drive " << unitName <<
      " to: " << vid << ". Error msg: " << ne.getMessage().str();
    throw ex;
  }
}

//-----------------------------------------------------------------------------
// connectToTapeserver
//-----------------------------------------------------------------------------
int castor::legacymsg::TapeserverProxyTcpIp::connectToTapeserver() const throw(castor::exception::Exception) {
  castor::utils::SmartFd smartConnectSock;
  try {
    smartConnectSock.reset(io::connectWithTimeout(m_tapeserverHostName, m_tapeserverPort,
      m_netTimeout));
  } catch(castor::exception::Exception &ne) {
    castor::exception::Internal ex;
    ex.getMessage() << "Failed to connect to tapeserver on host " << m_tapeserverHostName
      << " port " << m_tapeserverPort << ": " << ne.getMessage().str();
    throw ex;
  }

  return smartConnectSock.release();
}

//-----------------------------------------------------------------------------
// writeSetVidRequestMsg
//-----------------------------------------------------------------------------
void castor::legacymsg::TapeserverProxyTcpIp::writeSetVidRequestMsg(const int fd, const legacymsg::SetVidRequestMsgBody &body) throw(castor::exception::Exception) {
  char buf[CA_MAXVIDLEN+1+CA_MAXUNMLEN+1];
  const size_t len = legacymsg::marshal(buf, sizeof(buf), body);

  try {
    io::writeBytes(fd, m_netTimeout, len, buf);
  } catch(castor::exception::Exception &ne) {
    castor::exception::Internal ex;
    ex.getMessage() << "Failed to write drive status message: "
      << ne.getMessage().str();
    throw ex;
  }
}

//-----------------------------------------------------------------------------
// readCommitAck
//-----------------------------------------------------------------------------
void castor::legacymsg::TapeserverProxyTcpIp::readCommitAck(const int fd) throw(castor::exception::Exception) {
  legacymsg::MessageHeader ack;

  try {
    ack = readAck(fd);
  } catch(castor::exception::Exception &ne) {
    castor::exception::Internal ex;
    ex.getMessage() << "Failed to read VDQM_COMMIT ack: " <<
      ne.getMessage().str();
    throw ex;
  }

  if(VDQM_MAGIC != ack.magic) {
    castor::exception::Internal ex;
    ex.getMessage() << "Failed to read VDQM_COMMIT ack: Invalid magic"
      ": expected=0x" << std::hex << VDQM_MAGIC << " actual=" << ack.magic;
    throw ex;
  }

  if(VDQM_COMMIT == ack.reqType) {
    // Read a successful VDQM_COMMIT ack
    return;
  } else if(0 < ack.reqType) {
    // VDQM_COMMIT ack is reporting an error
    char errBuf[80];
    sstrerror_r(ack.reqType, errBuf, sizeof(errBuf));
    castor::exception::Internal ex;
    ex.getMessage() << "VDQM_COMMIT ack reported an error: " << errBuf;
    throw ex;
  } else {
    // VDQM_COMMIT ack contains an invalid request type
    castor::exception::Internal ex;
    ex.getMessage() << "VDQM_COMMIT ack contains an invalid request type"
      ": reqType=" << ack.reqType;
    throw ex;
  }
}

//-----------------------------------------------------------------------------
// readAck
//-----------------------------------------------------------------------------
castor::legacymsg::MessageHeader castor::legacymsg::TapeserverProxyTcpIp::readAck(const int fd) throw(castor::exception::Exception) {
  char buf[12]; // Magic + type + len
  legacymsg::MessageHeader ack;

  try {
    io::readBytes(fd, m_netTimeout, sizeof(buf), buf);
  } catch(castor::exception::Exception &ne) {
    castor::exception::Internal ex;
    ex.getMessage() << "Failed to read ack: "
      << ne.getMessage().str();
    throw ex;
  }

  const char *bufPtr = buf;
  size_t bufLen = sizeof(buf);
  legacymsg::unmarshal(bufPtr, bufLen, ack);

  return ack;
}
