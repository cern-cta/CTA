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

#include "castor/io/io.hpp"
#include "castor/legacymsg/CommonMarshal.hpp"
#include "castor/legacymsg/TapeMarshal.hpp"
#include "castor/legacymsg/TapeserverProxyTcpIp.hpp"
#include "castor/utils/SmartFd.hpp"
#include "castor/utils/utils.hpp"
#include "h/rtcp_constants.h"
#include "h/vdqm_constants.h"
#include "h/Ctape.h"

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
// gotReadMountDetailsFromClient
//------------------------------------------------------------------------------
void castor::legacymsg::TapeserverProxyTcpIp::gotReadMountDetailsFromClient(
  const std::string &unitName,
  const std::string &vid)
  throw(castor::exception::Exception) {
  try {
    legacymsg::TapeUpdateDriveRqstMsgBody body;
    castor::utils::copyString(body.vid, vid.c_str());
    castor::utils::copyString(body.drive, unitName.c_str()); 
    castor::utils::SmartFd fd(connectToTapeserver());
    writeTapeUpdateDriveRqstMsg(fd.get(), body);
    readReplyMsg(fd.get());  
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to notify tapeserverd of read mount: unitName="
      << unitName << " vid=" << vid << ": " << ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// gotWriteMountDetailsFromClient
//------------------------------------------------------------------------------
void castor::legacymsg::TapeserverProxyTcpIp::gotWriteMountDetailsFromClient(
  const std::string &unitName,
  const std::string &vid)
  throw(castor::exception::Exception) {
  try {
    legacymsg::TapeUpdateDriveRqstMsgBody body;
    castor::utils::copyString(body.vid, vid.c_str());
    castor::utils::copyString(body.drive, unitName.c_str());
    castor::utils::SmartFd fd(connectToTapeserver());
    writeTapeUpdateDriveRqstMsg(fd.get(), body);
    readReplyMsg(fd.get());
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to notify tapeserverd of read mount: unitName=" 
      << unitName << " vid=" << vid << ": " << ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// gotDumpMountDetailsFromClient
//------------------------------------------------------------------------------
void castor::legacymsg::TapeserverProxyTcpIp::gotDumpMountDetailsFromClient(
  const std::string &unitName,
  const std::string &vid)
  throw(castor::exception::Exception) {
  try {
    legacymsg::TapeUpdateDriveRqstMsgBody body;
    castor::utils::copyString(body.vid, vid.c_str());
    castor::utils::copyString(body.drive, unitName.c_str());
    castor::utils::SmartFd fd(connectToTapeserver());
    writeTapeUpdateDriveRqstMsg(fd.get(), body);
    readReplyMsg(fd.get());
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to notify tapeserverd of read mount: unitName=" 
      << unitName << " vid=" << vid << ": " << ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// tapeMountedForRead
//------------------------------------------------------------------------------
void castor::legacymsg::TapeserverProxyTcpIp::tapeMountedForRead(
  const std::string &unitName,
  const std::string &vid)
  throw(castor::exception::Exception) {
}

//------------------------------------------------------------------------------
// tapeMountedForWrite
//------------------------------------------------------------------------------
void castor::legacymsg::TapeserverProxyTcpIp::tapeMountedForWrite(
  const std::string &unitName,
  const std::string &vid)
  throw(castor::exception::Exception) {
} 

//------------------------------------------------------------------------------
// tapeUnmounted
//------------------------------------------------------------------------------
void castor::legacymsg::TapeserverProxyTcpIp::tapeUnmounted(
  const std::string &unitName,
  const std::string &vid)
  throw(castor::exception::Exception) {
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
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to connect to tapeserver on host " << m_tapeserverHostName
      << " port " << m_tapeserverPort << ": " << ne.getMessage().str();
    throw ex;
  }

  return smartConnectSock.release();
}

//-----------------------------------------------------------------------------
// writeTapeUpdateDriveRqstMsg
//-----------------------------------------------------------------------------
void castor::legacymsg::TapeserverProxyTcpIp::writeTapeUpdateDriveRqstMsg(
  const int fd, const legacymsg::TapeUpdateDriveRqstMsgBody &body)
  throw(castor::exception::Exception) {
  char buf[3 * sizeof(uint32_t) + sizeof(body)]; // header + body
  const size_t len = legacymsg::marshal(buf, sizeof(buf), body);

  try {
    io::writeBytes(fd, m_netTimeout, len, buf);
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to write drive status message: "
      << ne.getMessage().str();
    throw ex;
  }
}

//-----------------------------------------------------------------------------
// readReplyMsg
//-----------------------------------------------------------------------------
void castor::legacymsg::TapeserverProxyTcpIp::readReplyMsg(const int fd) throw(castor::exception::Exception) {
  
  char buf[3 * sizeof(uint32_t)]; // magic + request type + len
  io::readBytes(fd, m_netTimeout, sizeof(buf), buf);

  const char *bufPtr = buf;
  size_t bufLen = sizeof(buf);
  legacymsg::MessageHeader header;
  memset(&header, '\0', sizeof(header));
  legacymsg::unmarshal(bufPtr, bufLen, header);

  if(TPMAGIC != header.magic) {
    castor::exception::Exception ex;
    ex.getMessage() << "Invalid mount session message: Invalid magic"
      ": expected=0x" << std::hex << TPMAGIC << " actual=0x" <<
      header.magic;
    throw ex;
  }
  
  if(0 != header.lenOrStatus) {
    castor::exception::Exception ex;
    ex.getMessage() << "\"Set Vid\" failed. Return code: "
      << header.lenOrStatus;
    throw ex;
  }
}
