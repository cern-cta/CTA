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
#include "castor/legacymsg/VmgrProxyTcpIp.hpp"
#include "castor/utils/SmartFd.hpp"

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

//------------------------------------------------------------------------------
// tapeMountedForRead
//------------------------------------------------------------------------------
void castor::legacymsg::VmgrProxyTcpIp::tapeMountedForRead(const std::string &vid) 
  throw (castor::exception::Exception) {
}

//------------------------------------------------------------------------------
// tapeMountedForWrite
//------------------------------------------------------------------------------
void castor::legacymsg::VmgrProxyTcpIp::tapeMountedForWrite(const std::string &vid)
  throw (castor::exception::Exception) {
}

//-----------------------------------------------------------------------------
// connectToVmgr
//-----------------------------------------------------------------------------
int castor::legacymsg::VmgrProxyTcpIp::connectToVmgr() const throw(castor::exception::Exception) {
  castor::utils::SmartFd smartConnectSock;
  try {
    smartConnectSock.reset(io::connectWithTimeout(m_vmgrHostName, m_vmgrPort,
      m_netTimeout));
  } catch(castor::exception::Exception &ne) {
    castor::exception::Internal ex;
    ex.getMessage() << "Failed to connect to vmgr on host " << m_vmgrHostName
      << " port " << m_vmgrPort << ": " << ne.getMessage().str();
    throw ex;
  }

  return smartConnectSock.release();
}
