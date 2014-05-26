/******************************************************************************
 *                castor/legacymsg/VmgrProxyTcpIp.cpp
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
 * @author Steven.Murray@cern.ch
 *****************************************************************************/

#include "castor/io/io.hpp"
#include "castor/legacymsg/CommonMarshal.hpp"
#include "castor/legacymsg/VmgrMarshal.hpp"
#include "castor/legacymsg/VmgrProxyTcpIp.hpp"
#include "castor/legacymsg/VmgrTapeMountedMsgBody.hpp"
#include "castor/utils/SmartFd.hpp"
#include "castor/utils/utils.hpp"
#include "h/Ctape_constants.h"
#include "h/vmgr.h"

#include <unistd.h>
#include <sys/types.h>

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::legacymsg::VmgrProxyTcpIp::VmgrProxyTcpIp(
  log::Logger &log,
  const std::string &vmgrHostName,
  const unsigned short vmgrPort,
  const int netTimeout) throw():
    m_log(log),
    m_vmgrHostName(vmgrHostName),
    m_vmgrPort(vmgrPort),
    m_netTimeout(netTimeout) {
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
castor::legacymsg::VmgrProxyTcpIp::~VmgrProxyTcpIp() throw() {
}

//-----------------------------------------------------------------------------
// readVmgrReply
//-----------------------------------------------------------------------------
void castor::legacymsg::VmgrProxyTcpIp::readVmgrReply(const int fd)  {
  legacymsg::MessageHeader reply;

  try {
    reply = readRcReplyMsg(fd);
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to read VMGR reply: " <<
      ne.getMessage().str();
    throw ex;
  }

  if(VMGR_MAGIC != reply.magic) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to read VMGR reply: Invalid magic"
      ": expected=0x" << std::hex << VMGR_MAGIC << " actual=" << reply.magic;
    throw ex;
  }
  
  if(VMGR_RC != reply.reqType) {
    castor::exception::Exception ex;
    ex.getMessage() << "Wrong VMGR reply type"
      ": expected=0x" << std::hex << VMGR_RC << " actual=" << reply.reqType;
    throw ex;
  } 
  
  if(0 != reply.lenOrStatus) {
    // VMGR is reporting an error
    char errBuf[80];
    sstrerror_r(reply.reqType, errBuf, sizeof(errBuf));
    castor::exception::Exception ex;
    ex.getMessage() << "VMGR reported an error: " << errBuf;
    throw ex;
  }
}

//-----------------------------------------------------------------------------
// readRcReplyMsg
//-----------------------------------------------------------------------------
castor::legacymsg::MessageHeader castor::legacymsg::VmgrProxyTcpIp::readRcReplyMsg(const int fd)  {
  char buf[12]; // Magic + type + len
  legacymsg::MessageHeader reply;

  try {
    io::readBytes(fd, m_netTimeout, sizeof(buf), buf);
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to read ack: "
      << ne.getMessage().str();
    throw ex;
  }

  const char *bufPtr = buf;
  size_t bufLen = sizeof(buf);
  legacymsg::unmarshal(bufPtr, bufLen, reply);

  return reply;
}

//-----------------------------------------------------------------------------
// writeDriveStatusMsg
//-----------------------------------------------------------------------------
void castor::legacymsg::VmgrProxyTcpIp::writeTapeMountNotificationMsg(const int fd, const legacymsg::VmgrTapeMountedMsgBody &body)  {
  char buf[REQBUFSZ];
  const size_t len = legacymsg::marshal(buf, body);

  try {
    io::writeBytes(fd, m_netTimeout, len, buf);
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to write mount notification message: "
      << ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// setDriveStatus
//------------------------------------------------------------------------------
void castor::legacymsg::VmgrProxyTcpIp::sendNotificationAndReceiveReply(const legacymsg::VmgrTapeMountedMsgBody &body)  {
  castor::utils::SmartFd fd(connectToVmgr());
  writeTapeMountNotificationMsg(fd.get(), body);
  readVmgrReply(fd.get());
}

//------------------------------------------------------------------------------
// tapeMountedForRead
//------------------------------------------------------------------------------
void castor::legacymsg::VmgrProxyTcpIp::tapeMountedForRead(const std::string &vid) 
{
  try {
    castor::legacymsg::VmgrTapeMountedMsgBody msg;
    msg.uid = getuid();
    msg.gid = getgid();
    msg.jid = getpid();
    msg.mode = WRITE_DISABLE;
    castor::utils::copyString(msg.vid, vid.c_str());

    sendNotificationAndReceiveReply(msg);
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to notify the VMGR that the tape " << vid <<
      " was mounted: " << ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// tapeMountedForWrite
//------------------------------------------------------------------------------
void castor::legacymsg::VmgrProxyTcpIp::tapeMountedForWrite(const std::string &vid)
{
  try {
    castor::legacymsg::VmgrTapeMountedMsgBody msg;
    msg.uid = getuid();
    msg.gid = getgid();
    msg.jid = getpid();
    msg.mode = WRITE_ENABLE;
    castor::utils::copyString(msg.vid, vid.c_str());

    sendNotificationAndReceiveReply(msg);
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to notify the VMGR that the tape " << vid <<
      " was mounted: " << ne.getMessage().str();
    throw ex;
  }
}

//-----------------------------------------------------------------------------
// connectToVmgr
//-----------------------------------------------------------------------------
int castor::legacymsg::VmgrProxyTcpIp::connectToVmgr() const  {
  castor::utils::SmartFd smartConnectSock;
  try {
    smartConnectSock.reset(io::connectWithTimeout(m_vmgrHostName, m_vmgrPort,
      m_netTimeout));
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to connect to vmgr on host " << m_vmgrHostName
      << " port " << m_vmgrPort << ": " << ne.getMessage().str();
    throw ex;
  }

  return smartConnectSock.release();
}
