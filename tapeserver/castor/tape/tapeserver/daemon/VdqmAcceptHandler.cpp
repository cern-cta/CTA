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
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#include "castor/exception/BadAlloc.hpp"
#include "castor/exception/Errnum.hpp"
#include "castor/io/io.hpp"
#include "castor/tape/tapeserver/daemon/VdqmAcceptHandler.hpp"
#include "castor/tape/tapeserver/daemon/VdqmConnectionHandler.hpp"
#include "castor/utils/SmartFd.hpp"

#include <errno.h>
#include <memory>
#include <string.h>

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::VdqmAcceptHandler::VdqmAcceptHandler(
  const int fd,
  reactor::ZMQReactor &reactor,
  log::Logger &log,
  Catalogue &driveCatalogue,
  const TapeDaemonConfig &tapeDaemonConfig)
  throw():
    m_fd(fd),
    m_reactor(reactor),
    m_log(log),
    m_driveCatalogue(driveCatalogue),
    m_tapeDaemonConfig(tapeDaemonConfig) {
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::VdqmAcceptHandler::~VdqmAcceptHandler()
  throw() {
  log::Param params[] = {log::Param("fd", m_fd)};
  m_log(LOG_DEBUG, "Closing vdqm listen socket", params);
  close(m_fd);
}

//------------------------------------------------------------------------------
// getName
//------------------------------------------------------------------------------
std::string castor::tape::tapeserver::daemon::VdqmAcceptHandler::getName() 
  const throw() {
  return "VdqmAcceptHandler";
}

//------------------------------------------------------------------------------
// fillPollFd
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::VdqmAcceptHandler::fillPollFd(
  zmq_pollitem_t &fd) throw() {
  fd.fd = m_fd;
  fd.events = ZMQ_POLLIN;
  fd.revents = 0;
  fd.socket = NULL;
}

//------------------------------------------------------------------------------
// handleEvent
//------------------------------------------------------------------------------
bool castor::tape::tapeserver::daemon::VdqmAcceptHandler::handleEvent(
  const zmq_pollitem_t &fd)  {
  logVdqmAcceptEvent(fd);

  checkHandleEventFd(fd.fd);

  // Accept the connection
  castor::utils::SmartFd connection;
  try {
    const time_t acceptTimeout  = 1; // Timeout in seconds
    connection.reset(io::acceptConnection(fd.fd, acceptTimeout));
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to accept a connection from the vdqm daemon"
      ": " << ne.getMessage().str();
    throw ex;
  }

  log::Param params[] = {log::Param("fd", connection.get())};
  m_log(LOG_DEBUG, "Accepted a possible vdqm connection", params);

  // Create a new vdqm connection handler
  std::unique_ptr<VdqmConnectionHandler> connectionHandler;
  try {
    connectionHandler.reset(new VdqmConnectionHandler(connection.get(),
      m_reactor, m_log, m_driveCatalogue, m_tapeDaemonConfig));
    connection.release();
  } catch(std::bad_alloc &ba) {
    castor::exception::BadAlloc ex;
    ex.getMessage() << "Failed to allocate a new vdqm connection handler"
      ": " << ba.what();
    throw ex;
  }

  m_log(LOG_DEBUG, "Created a new vdqm connection handler", params);

  // Register the new vdqm connection handler with the reactor
  try {
    m_reactor.registerHandler(connectionHandler.get());
    connectionHandler.release();
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to register a new vdqm connection handler"
      ": " << ne.getMessage().str();
    throw ex;
  }

  m_log(LOG_DEBUG, "Registered the new vdqm connection handler", params);

  return false; // Stay registered with the reactor
}

//------------------------------------------------------------------------------
// logVdqmAcceptConnectionEvent
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::VdqmAcceptHandler::logVdqmAcceptEvent(
  const zmq_pollitem_t &fd)  {
  log::Param params[] = {
  log::Param("fd", fd.fd),
  log::Param("ZMQ_POLLIN", fd.revents & ZMQ_POLLIN ? "true" : "false"),
  log::Param("ZMQ_POLLOUT", fd.revents & ZMQ_POLLOUT ? "true" : "false"),
  log::Param("ZMQ_POLLERR", fd.revents & ZMQ_POLLERR ? "true" : "false")};
  m_log(LOG_DEBUG, "I/O event on vdqm listen socket", params);
}

//------------------------------------------------------------------------------
// checkHandleEventFd
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::VdqmAcceptHandler::checkHandleEventFd(
  const int fd)  {
  if(m_fd != fd) {
    castor::exception::Exception ex;
    ex.getMessage() <<
      "VdqmAcceptHandler passed wrong file descriptor"
      ": expected=" << m_fd << " actual=" << fd;
    throw ex;
  }
}
