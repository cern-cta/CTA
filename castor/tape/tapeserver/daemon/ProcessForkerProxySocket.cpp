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
 * @author Steven.Murray@cern.ch
 *****************************************************************************/

#include "castor/tape/tapeserver/daemon/ProcessForkerProxySocket.hpp"
#include "h/serrno.h"

#include <errno.h>

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::ProcessForkerProxySocket::
  ProcessForkerProxySocket(log::Logger &log, const int socketFd) throw():
  m_log(log), m_socketFd(socketFd) {
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::ProcessForkerProxySocket::
  ~ProcessForkerProxySocket() throw() {
  if(-1 == close(m_socketFd)) {
    char message[100];
    sstrerror_r(errno, message, sizeof(message));
    log::Param params[] = {log::Param("socketFd", m_socketFd), 
      log::Param("message", message)};
    m_log(LOG_ERR,
      "Failed to close file-descriptor of ProcessForkerProxySocket", params);
  }
}

//------------------------------------------------------------------------------
// forkDataTransferSession
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::ProcessForkerProxySocket::
  forkDataTransferSession() {
}

//------------------------------------------------------------------------------
// writeMsg
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::ProcessForkerProxySocket::writeMsg() {
  writeMsgHeader();
  writeMsgBody();
}

//------------------------------------------------------------------------------
// writeMsgHeader
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::ProcessForkerProxySocket::
  writeMsgHeader() {
}

//------------------------------------------------------------------------------
// writeMsgBody
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::ProcessForkerProxySocket::
  writeMsgBody() {
}

//------------------------------------------------------------------------------
// forkLabelSession
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::ProcessForkerProxySocket::
  forkLabelSession() {
}

//------------------------------------------------------------------------------
// forkCleanupSession
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::ProcessForkerProxySocket::
  forkCleanupSession() {
}
