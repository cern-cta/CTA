/******************************************************************************
 *         castor/tape/tapeserver/daemon/AdminAcceptHandler.cpp
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

#include "castor/exception/BadAlloc.hpp"
#include "castor/exception/Errnum.hpp"
#include "castor/io/io.hpp"
#include "castor/tape/tapeserver/daemon/AdminAcceptHandler.hpp"
#include "castor/tape/tapeserver/daemon/AdminConnectionHandler.hpp"
#include "castor/utils/SmartFd.hpp"
#include "h/common.h"
#include "h/serrno.h"
#include "h/Ctape.h"
#include "h/vdqm_api.h"
#include "castor/legacymsg/CommonMarshal.hpp"
#include "castor/legacymsg/TapeMarshal.hpp"
#include "castor/tape/utils/utils.hpp"
#include "castor/utils/utils.hpp"

#include <errno.h>
#include <list>
#include <memory>
#include <string.h>
#include <stdio.h>
#include <sys/types.h>
#include <unistd.h>

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::AdminAcceptHandler::AdminAcceptHandler(
  const int fd,
  reactor::ZMQReactor &reactor,
  log::Logger &log,
  legacymsg::VdqmProxy &vdqm,
  DriveCatalogue &driveCatalogue,
  const std::string &hostName)
  throw():
    m_fd(fd),
    m_reactor(reactor),
    m_log(log),
    m_vdqm(vdqm),
    m_driveCatalogue(driveCatalogue),
    m_hostName(hostName),
    m_netTimeout(10) { // Timeout in seconds
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::AdminAcceptHandler::~AdminAcceptHandler()
  throw() {
  log::Param params[] = {log::Param("fd", m_fd)};
  m_log(LOG_DEBUG, "Closing admin listen socket", params);
  close(m_fd);
}

//------------------------------------------------------------------------------
// getFd
//------------------------------------------------------------------------------
int castor::tape::tapeserver::daemon::AdminAcceptHandler::getFd() throw() {
  return m_fd;
}

//------------------------------------------------------------------------------
// fillPollFd
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::AdminAcceptHandler::fillPollFd(zmq::pollitem_t &fd) throw() {
  fd.fd = m_fd;
  fd.revents = 0;
  fd.socket = NULL;
}

//------------------------------------------------------------------------------
// handleEvent
//------------------------------------------------------------------------------
bool castor::tape::tapeserver::daemon::AdminAcceptHandler::handleEvent(
  const zmq::pollitem_t &fd)  {
  logAdminAcceptEvent(fd);

  checkHandleEventFd(fd.fd);

  // Accept the connection from the admin command
  castor::utils::SmartFd connection;
  try {
    connection.reset(io::acceptConnection(fd.fd, 1));
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to accept an admin connection"
      ": " << ne.getMessage().str();
    throw ex;
  }

  log::Param params[] = {log::Param("fd", connection.get())};
  m_log(LOG_DEBUG, "Accepted a possible admin connection", params);

  // Create a new admin connection handler
  std::auto_ptr<AdminConnectionHandler> connectionHandler;
  try {
    connectionHandler.reset(new AdminConnectionHandler(connection.get(),
      m_reactor, m_log, m_vdqm, m_driveCatalogue, m_hostName));
    connection.release();
  } catch(std::bad_alloc &ba) {
    castor::exception::BadAlloc ex;
    ex.getMessage() << "Failed to allocate a new admin connection handler"
      ": " << ba.what();
    throw ex;
  }

  m_log(LOG_DEBUG, "Created a new admin connection handler", params);

  // Register the new admin connection handler with the reactor
  try {
    m_reactor.registerHandler(connectionHandler.get());
    connectionHandler.release();
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to register a new admin connection handler"
      ": " << ne.getMessage().str();
    throw ex;
  }

  m_log(LOG_DEBUG, "Registered the new admin connection handler", params);

  return false; // Stay registered with the reactor
}

//------------------------------------------------------------------------------
// logAdminAcceptConnectionEvent
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::AdminAcceptHandler::
  logAdminAcceptEvent(const zmq::pollitem_t &fd)  {
  std::list<log::Param> params;
  params.push_back(log::Param("fd", fd.fd));
  params.push_back(log::Param("POLLIN",
    fd.revents & POLLIN ? "true" : "false"));
  params.push_back(log::Param("POLLRDNORM",
    fd.revents & POLLRDNORM ? "true" : "false"));
  params.push_back(log::Param("POLLRDBAND",
    fd.revents & POLLRDBAND ? "true" : "false"));
  params.push_back(log::Param("POLLPRI",
    fd.revents & POLLPRI ? "true" : "false"));
  params.push_back(log::Param("POLLOUT",
    fd.revents & POLLOUT ? "true" : "false"));
  params.push_back(log::Param("POLLWRNORM",
    fd.revents & POLLWRNORM ? "true" : "false"));
  params.push_back(log::Param("POLLWRBAND",
    fd.revents & POLLWRBAND ? "true" : "false"));
  params.push_back(log::Param("POLLERR",
    fd.revents & POLLERR ? "true" : "false"));
  params.push_back(log::Param("POLLHUP",
    fd.revents & POLLHUP ? "true" : "false"));
  params.push_back(log::Param("POLLNVAL",
    fd.revents & POLLNVAL ? "true" : "false"));
  m_log(LOG_DEBUG, "I/O event on admin listen socket", params);
}

//------------------------------------------------------------------------------
// checkHandleEventFd
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::AdminAcceptHandler::checkHandleEventFd(
  const int fd)  {
  if(m_fd != fd) {
    castor::exception::Exception ex;
    ex.getMessage() <<
      "AdminAcceptHandler passed wrong file descriptor"
      ": expected=" << m_fd << " actual=" << fd;
    throw ex;
  }
}
