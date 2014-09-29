/******************************************************************************
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
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#include "castor/io/io.hpp"
#include "castor/legacymsg/CommonMarshal.hpp"
#include "castor/legacymsg/CupvMarshal.hpp"
#include "castor/legacymsg/CupvProxyTcpIp.hpp"
#include "castor/utils/SmartFd.hpp"
#include "castor/utils/utils.hpp"

#include "h/Cupv.h"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::legacymsg::CupvProxyTcpIp::CupvProxyTcpIp(
  log::Logger &log,
  const std::string &cupvHostName,
  const unsigned short cupvPort,
  const int netTimeout) throw():
  m_log(log),
  m_cupvHostName(cupvHostName),
  m_cupvPort(cupvPort),
  m_netTimeout(netTimeout) {
}

//-----------------------------------------------------------------------------
// destructor
//-----------------------------------------------------------------------------
castor::legacymsg::CupvProxyTcpIp::~CupvProxyTcpIp() throw() {
}

//-----------------------------------------------------------------------------
// isGranted
//-----------------------------------------------------------------------------
bool castor::legacymsg::CupvProxyTcpIp::isGranted(
  const uid_t privUid,
  const gid_t privGid,
  const std::string &srcHost,
  const std::string &tgtHost,
  const int priv)  {
  std::ostringstream task;
  task << "check privilege privUid=" << privUid << " privGid=" << privGid <<
    " srcHost=" << srcHost << " tgtHost=" << tgtHost << " priv=" << priv;

  try {
    utils::SmartFd fd(connectToCupv());

    CupvCheckMsgBody body;
    body.uid = geteuid();
    body.gid = getegid();
    body.privUid = privUid;
    body.privGid = privGid;
    utils::copyString(body.srcHost, srcHost);
    utils::copyString(body.tgtHost, tgtHost);
    body.priv = priv;
    writeCupvCheckMsg(fd.get(), body);

    const MessageHeader header = readCupvMsgHeader(fd.get());
    switch(header.reqType) {
    case CUPV_RC:
      switch(header.lenOrStatus) {
      case 0:
        return true;
      case EACCES:
        return false;
      default:
        castor::exception::Exception ex;
        ex.getMessage() << "Received error code from cupv running on " <<
          m_cupvHostName << ": code=" << header.lenOrStatus;
        throw ex;
      }
    case MSG_ERR:
      {
        char errorBuf[1024];
        const int nbBytesToRead = header.lenOrStatus > sizeof(errorBuf) ?
          sizeof(errorBuf) : header.lenOrStatus;
        castor::io::readBytes(fd.get(), m_netTimeout, nbBytesToRead, errorBuf);
        errorBuf[sizeof(errorBuf) - 1] = '\0';
        castor::exception::Exception ex;
        ex.getMessage() << "Received error message from cupv running on " <<
          m_cupvHostName << ": " << errorBuf;
        throw ex;
      }
    default:
      {
        castor::exception::Exception ex;
        ex.getMessage() <<
          "Reply message from cupv running on " << m_cupvHostName <<
          " has an unexpected request type"
          ": reqType=0x" << header.reqType;
        throw ex;
      }
    }

  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to " << task.str() << ": " << ne.getMessage().str();
    throw ex;
  }
}

//-----------------------------------------------------------------------------
// connectToCupv
//-----------------------------------------------------------------------------
int castor::legacymsg::CupvProxyTcpIp::connectToCupv() const  {
  castor::utils::SmartFd smartConnectSock;
  try {
    smartConnectSock.reset(io::connectWithTimeout(m_cupvHostName, m_cupvPort,
      m_netTimeout));
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to connect to cupv on host " << m_cupvHostName
      << " port " << m_cupvPort << ": " << ne.getMessage().str();
    throw ex;
  }

  return smartConnectSock.release();
}

//-----------------------------------------------------------------------------
// writeCupvCheckMsg
//-----------------------------------------------------------------------------
void castor::legacymsg::CupvProxyTcpIp::writeCupvCheckMsg(const int fd, const legacymsg::CupvCheckMsgBody &body)  {
  char buf[REQBUFSZ];
  const size_t len = legacymsg::marshal(buf, body);

  try {
    io::writeBytes(fd, m_netTimeout, len, buf);
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to write CUPV_CHECK message: "
      << ne.getMessage().str();
    throw ex;
  }
}

//-----------------------------------------------------------------------------
// readCupvMsgHeader
//-----------------------------------------------------------------------------
castor::legacymsg::MessageHeader castor::legacymsg::CupvProxyTcpIp::readCupvMsgHeader(const int fd)  {
  char buf[12]; // Magic + type + len
  MessageHeader header;

  try {
    io::readBytes(fd, m_netTimeout, sizeof(buf), buf);
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to read message header: "
      << ne.getMessage().str();
    throw ex;
  }

  const char *bufPtr = buf;
  size_t bufLen = sizeof(buf);
  unmarshal(bufPtr, bufLen, header);

  if(CUPV_MAGIC != header.magic) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to read message header: "
      " Header contains an invalid magic number: expected=0x" << std::hex <<
      CUPV_MAGIC << " actual=0x" << header.magic;
    throw ex;
  }

  return header;
}
