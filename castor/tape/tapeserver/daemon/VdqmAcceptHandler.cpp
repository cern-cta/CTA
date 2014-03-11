/******************************************************************************
 *         castor/tape/tapeserver/daemon/VdqmAcceptHandler.cpp
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

#include "castor/exception/BadAlloc.hpp"
#include "castor/exception/Errnum.hpp"
#include "castor/exception/Internal.hpp"
#include "castor/io/io.hpp"
#include "castor/tape/tapeserver/daemon/VdqmAcceptHandler.hpp"
#include "castor/tape/tapeserver/daemon/VdqmConnectionHandler.hpp"
#include "castor/utils/SmartFd.hpp"
#include "h/common.h"
#include "h/serrno.h"

#include <errno.h>
#include <memory>
#include <string.h>

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::VdqmAcceptHandler::VdqmAcceptHandler(
  const int listenSock,
  io::PollReactor &reactor,
  log::Logger &log,
  Vdqm &vdqm,
  DriveCatalogue &driveCatalogue)
  throw():
    m_listenSock(listenSock),
    m_reactor(reactor),
    m_log(log),
    m_vdqm(vdqm),
    m_driveCatalogue(driveCatalogue) {
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::VdqmAcceptHandler::~VdqmAcceptHandler()
  throw() {
}

//------------------------------------------------------------------------------
// getFd
//------------------------------------------------------------------------------
int castor::tape::tapeserver::daemon::VdqmAcceptHandler::getFd() throw() {
  return m_listenSock;
}

//------------------------------------------------------------------------------
// fillPollFd
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::VdqmAcceptHandler::fillPollFd(
  struct pollfd &fd) throw() {
  fd.fd = m_listenSock;
  fd.events = POLLRDNORM;
  fd.revents = 0;
}

//------------------------------------------------------------------------------
// handleEvent
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::VdqmAcceptHandler::handleEvent(
  const struct pollfd &fd) throw(castor::exception::Exception) {
  checkHandleEventFd(fd.fd);

  // Do nothing if there is no data to read
  if(0 == (fd.revents & POLLIN)) {
    return;
  }

  // Accept the connection from the vdqmd daemon
  castor::utils::SmartFd connection;
  try {
    connection.reset(io::acceptConnection(fd.fd, 1));
  } catch(castor::exception::Exception &ne) {
    castor::exception::Internal ex;
    ex.getMessage() << "Failed to accept a connection from the vdqm daemon"
      ": " << ne.getMessage().str();
  }

  // Close connection and return if connection is not from an admin host
  {
    // isadminhost fills in peerHost
    char peerHost[CA_MAXHOSTNAMELEN+1];
    memset(peerHost, '\0', sizeof(peerHost));

    // Unfortunately isadminhost can either set errno or serrno
    serrno = 0;
    errno = 0;
    const int rc = isadminhost(connection.get(), peerHost);
    if(0 == rc) {
      log::Param params[] = {
        log::Param("host", peerHost[0] != '\0' ? peerHost : "UNKNOWN")};
      m_log(LOG_INFO, "Received connection from authorized admin host", params);
    } else {
      // Give preference to serrno over errno when it comes to isadminhost()
      const int savedErrno = errno;
      const int savedSerrno = serrno;
      std::string errorMessage;
      if(0 != serrno) {
        errorMessage = sstrerror(savedSerrno);
      } else if(0 != errno) {
        errorMessage = sstrerror(savedErrno);
      } else {
        errorMessage = "UKNOWN";
      }
      log::Param params[] = {
        log::Param("host", peerHost[0] != '\0' ? peerHost : "UNKNOWN"),
        log::Param("message", errorMessage)};
      m_log(LOG_ERR, "Failed to authorize vdqm host", params);
      exception::Errnum::throwOnNegative(close(connection.release()),
        "Failed to close unauthorized vdqm connection");
      return;
    }
  }

  // Create a new vdqm connection handler
  std::auto_ptr<VdqmConnectionHandler> connectionHandler;
  try {
    connectionHandler.reset(
      new VdqmConnectionHandler(connection.get(), m_reactor, m_log, m_vdqm,
        m_driveCatalogue));
    connection.release();
  } catch(std::bad_alloc &ba) {
    castor::exception::BadAlloc ex;
    ex.getMessage() << "Failed to allocate a new vdqm connection handler"
      ": " << ba.what();
    throw ex;
  }

  // Register the new vdqm connection handler with the reactor
  try {
    m_reactor.registerHandler(connectionHandler.get());
    connectionHandler.release();
  } catch(castor::exception::Exception &ne) {
    castor::exception::Internal ex;
    ex.getMessage() << "Failed to register a new vdqm connection handler"
      ": " << ne.getMessage().str();
  }
}

//------------------------------------------------------------------------------
// checkHandleEventFd
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::VdqmAcceptHandler::checkHandleEventFd(
  const int fd) throw (castor::exception::Exception) {
  if(m_listenSock != fd) {
    castor::exception::Internal ex;
    ex.getMessage() << "Failed to accept connection from vdqmd daemon"
      ": Event handler passed wrong file descriptor"
      ": expected=" << m_listenSock << " actual=" << fd;
    throw ex;
  }
}
