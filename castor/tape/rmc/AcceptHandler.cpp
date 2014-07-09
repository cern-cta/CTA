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
#include "castor/tape/rmc/AcceptHandler.hpp"
#include "castor/tape/rmc/ConnectionHandler.hpp"
#include "castor/utils/SmartFd.hpp"

#include <errno.h>
#include <memory>
#include <string.h>

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::tape::rmc::AcceptHandler::AcceptHandler(const int fd,
  reactor::ZMQReactor &reactor, log::Logger &log) throw(): m_fd(fd),
  m_reactor(reactor), m_log(log) {
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
castor::tape::rmc::AcceptHandler::~AcceptHandler() throw() {
  {
    log::Param params[] = {
      log::Param("fd", m_fd)};
    m_log(LOG_DEBUG, "Closing listen socket", params);
  }
  close(m_fd);
}

//------------------------------------------------------------------------------
// getName
//------------------------------------------------------------------------------
std::string castor::tape::rmc::AcceptHandler::getName() const throw() {
  return "rmc::AcceptHandler";
}

//------------------------------------------------------------------------------
// fillPollFd
//------------------------------------------------------------------------------
void castor::tape::rmc::AcceptHandler::fillPollFd(zmq_pollitem_t &fd) throw() {
  fd.fd = m_fd;
  fd.events = ZMQ_POLLIN;
  fd.revents = 0;
  fd.socket = NULL;
}

//------------------------------------------------------------------------------
// handleEvent
//------------------------------------------------------------------------------
bool castor::tape::rmc::AcceptHandler::handleEvent(
  const zmq_pollitem_t &fd)  {
  log::Param params[] = {
    log::Param("fd", fd.fd),
    log::Param("ZMQ_POLLIN", fd.revents & ZMQ_POLLIN ? "true" : "false"),
    log::Param("ZMQ_POLLOUT", fd.revents & ZMQ_POLLOUT ? "true" : "false"),
    log::Param("ZMQ_POLLERR", fd.revents & ZMQ_POLLERR ? "true" : "false")};
  m_log(LOG_DEBUG, "I/O event on rmc listen socket", params);

  checkHandleEventFd(fd.fd);

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
void castor::tape::rmc::AcceptHandler::checkHandleEventFd(const int fd)  {
  if(m_fd != fd) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to accept connection from client"
      ": Event handler passed wrong file descriptor"
      ": expected=" << m_fd << " actual=" << fd;
    throw ex;
  }
}
