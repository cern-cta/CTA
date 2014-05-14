/******************************************************************************
 *         castor/tape/rmc/AcceptHandler.cpp
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
#include "castor/io/io.hpp"
#include "castor/tape/rmc/AcceptHandler.hpp"
#include "castor/tape/rmc/ConnectionHandler.hpp"
#include "castor/utils/SmartFd.hpp"

#include <errno.h>
#include <memory>
#include <string.h>

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::tape::rmc::AcceptHandler::AcceptHandler(
  const int fd, io::PollReactor &reactor, log::Logger &log)
  throw(): m_fd(fd), m_reactor(reactor), m_log(log) {
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
castor::tape::rmc::AcceptHandler::~AcceptHandler()
  throw() {
  {
    log::Param params[] = {
      log::Param("fd", m_fd)};
    m_log(LOG_DEBUG, "Closing listen socket", params);
  }
  close(m_fd);
}

//------------------------------------------------------------------------------
// getFd
//------------------------------------------------------------------------------
int castor::tape::rmc::AcceptHandler::getFd() throw() {
  return m_fd;
}

//------------------------------------------------------------------------------
// fillPollFd
//------------------------------------------------------------------------------
void castor::tape::rmc::AcceptHandler::fillPollFd(
  struct pollfd &fd) throw() {
  fd.fd = m_fd;
  fd.events = POLLRDNORM;
  fd.revents = 0;
}

//------------------------------------------------------------------------------
// handleEvent
//------------------------------------------------------------------------------
bool castor::tape::rmc::AcceptHandler::handleEvent(
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
    m_log(LOG_DEBUG, "AcceptHandler::handleEvent()", params);
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

  // Accept the connection
  castor::utils::SmartFd connection;
  try {
    connection.reset(io::acceptConnection(fd.fd, 1));
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to accept a connection: " << ne.getMessage().str();
    throw ex;
  }

  m_log(LOG_DEBUG, "Accepted a possible client connection");

  // Create a new connection handler
  std::auto_ptr<ConnectionHandler> connectionHandler;
  try {
    connectionHandler.reset(new ConnectionHandler(connection.get(),
      m_reactor, m_log));
    connection.release();
  } catch(std::bad_alloc &ba) {
    castor::exception::BadAlloc ex;
    ex.getMessage() << "Failed to allocate a new connection handler"
      ": " << ba.what();
    throw ex;
  }

  m_log(LOG_DEBUG, "Created a new connection handler");

  // Register the new connection handler with the reactor
  try {
    m_reactor.registerHandler(connectionHandler.get());
    connectionHandler.release();
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to register a new connection handler"
      ": " << ne.getMessage().str();
  }

  m_log(LOG_DEBUG, "Registered the new connection handler");

  return false; // Stay registered with the reactor
}

//------------------------------------------------------------------------------
// checkHandleEventFd
//------------------------------------------------------------------------------
void castor::tape::rmc::AcceptHandler::checkHandleEventFd(
  const int fd) throw (castor::exception::Exception) {
  if(m_fd != fd) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to accept connection from client"
      ": Event handler passed wrong file descriptor"
      ": expected=" << m_fd << " actual=" << fd;
    throw ex;
  }
}
