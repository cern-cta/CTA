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
  close(m_listenSock);
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
bool castor::tape::tapeserver::daemon::VdqmAcceptHandler::handleEvent(
  const struct pollfd &fd) throw(castor::exception::Exception) {
  {
    log::Param params[] = {
      log::Param("fd"        , fd.fd                                     ),
      log::Param("POLLIN"    , fd.revents & POLLIN     ? "true" : "false"),
      log::Param("POLLRDNORM", fd.revents & POLLRDNORM ? "true" : "false"),
      log::Param("POLLRDBAND", fd.revents & POLLRDBAND ? "true" : "false"),
      log::Param("POLLPRI"   , fd.revents & POLLPRI    ? "true" : "false"),
      log::Param("POLLOUT"   , fd.revents & POLLOUT    ? "true" : "false"),
      log::Param("POLLWRNORM", fd.revents & POLLWRNORM ? "true" : "false"),
      log::Param("POLLWRBAND", fd.revents & POLLWRBAND ? "true" : "false"),
      log::Param("POLLERR"   , fd.revents & POLLERR    ? "true" : "false"),
      log::Param("POLLHUP"   , fd.revents & POLLHUP    ? "true" : "false"),
      log::Param("POLLNVAL"  , fd.revents & POLLNVAL   ? "true" : "false")};
    m_log(LOG_DEBUG, "VdqmAcceptHandler::handleEvent()", params);
  }

  checkHandleEventFd(fd.fd);

  // Do nothing if there is no data to read
  //
  // POLLIN is unfortuntaley not the logical or of POLLRDNORM and POLLRDBAND
  // on SLC 5.  I therefore replaced POLLIN with the logical or.  I also
  // added POLLPRI into the mix to cover all possible types of read event.
  if(0 == (fd.revents & POLLRDNORM) && 0 == (fd.revents & POLLRDBAND) &&
    0 == (fd.revents & POLLPRI)) {
    return false; // Stay registeed with the reactor
  }

  // Accept the connection from the vdqmd daemon
  castor::utils::SmartFd connection;
  try {
    connection.reset(io::acceptConnection(fd.fd, 1));
  } catch(castor::exception::Exception &ne) {
    castor::exception::Internal ex;
    ex.getMessage() << "Failed to accept a connection from the vdqm daemon"
      ": " << ne.getMessage().str();
    throw ex;
  }

  m_log(LOG_DEBUG, "Accepted a possible vdqm connection");

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

  m_log(LOG_DEBUG, "Created a new vdqm connection handler");

  // Register the new vdqm connection handler with the reactor
  try {
    m_reactor.registerHandler(connectionHandler.get());
    connectionHandler.release();
  } catch(castor::exception::Exception &ne) {
    castor::exception::Internal ex;
    ex.getMessage() << "Failed to register a new vdqm connection handler"
      ": " << ne.getMessage().str();
  }

  m_log(LOG_DEBUG, "Registered the new vdqm connection handler");

  return false; // Stay registered with the reactor
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
