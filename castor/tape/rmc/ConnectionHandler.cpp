/******************************************************************************
 *         castor/tape/rmc/ConnectionHandler.cpp
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

#include "castor/tape/rmc/ConnectionHandler.hpp"
#include "h/common.h"
#include "h/serrno.h"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::tape::rmc::ConnectionHandler::ConnectionHandler(const int fd,
  reactor::ZMQReactor &reactor, log::Logger &log) throw():
    m_fd(fd),
    m_reactor(reactor),
    m_log(log),
    m_netTimeout(1) // Timneout in seconds
{
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
castor::tape::rmc::ConnectionHandler::~ConnectionHandler() throw() {
  log::Param params[] = {log::Param("fd", m_fd)};
  m_log(LOG_DEBUG, "Closing client connection", params);
  close(m_fd);
}

//------------------------------------------------------------------------------
// getFd
//------------------------------------------------------------------------------
int castor::tape::rmc::ConnectionHandler::getFd() throw() {
  return m_fd;
}

//------------------------------------------------------------------------------
// fillPollFd
//------------------------------------------------------------------------------
void castor::tape::rmc::ConnectionHandler::fillPollFd(zmq::Pollitem &fd) throw() {
  fd.fd = m_fd;
  fd.revents = 0;
  fd.socket = NULL;
}

//------------------------------------------------------------------------------
// handleEvent
//------------------------------------------------------------------------------
bool castor::tape::rmc::ConnectionHandler::handleEvent(const zmq::Pollitem &fd)  {
  std::list<log::Param> params;
  params.push_back(log::Param("fd"        , fd.fd                                     ));
  params.push_back(log::Param("POLLIN"    , fd.revents & POLLIN     ? "true" : "false"));
  params.push_back(log::Param("POLLRDNORM", fd.revents & POLLRDNORM ? "true" : "false"));
  params.push_back(log::Param("POLLRDBAND", fd.revents & POLLRDBAND ? "true" : "false"));
  params.push_back(log::Param("POLLPRI"   , fd.revents & POLLPRI    ? "true" : "false"));
  params.push_back(log::Param("POLLOUT"   , fd.revents & POLLOUT    ? "true" : "false"));
  params.push_back(log::Param("POLLWRNORM", fd.revents & POLLWRNORM ? "true" : "false"));
  params.push_back(log::Param("POLLWRBAND", fd.revents & POLLWRBAND ? "true" : "false"));
  params.push_back(log::Param("POLLERR"   , fd.revents & POLLERR    ? "true" : "false"));
  params.push_back(log::Param("POLLHUP"   , fd.revents & POLLHUP    ? "true" : "false"));
  params.push_back(log::Param("POLLNVAL"  , fd.revents & POLLNVAL   ? "true" : "false"));
  m_log(LOG_DEBUG, "ConnectionHandler::handleEvent()", params);

  checkHandleEventFd(fd.fd);


  if(!connectionIsAuthorized()) {
    return true; // Ask reactor to remove and delete this handler
  }

  return true; // Ask reactor to remove and delete this handler
}

//------------------------------------------------------------------------------
// checkHandleEventFd
//------------------------------------------------------------------------------
void castor::tape::rmc::ConnectionHandler::checkHandleEventFd(const int fd)  {
  if(m_fd != fd) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to handle client connection"
      ": Event handler passed wrong file descriptor"
      ": expected=" << m_fd << " actual=" << fd;
    throw ex;
  }
}

//------------------------------------------------------------------------------
// connectionIsAuthorized
//------------------------------------------------------------------------------
bool castor::tape::rmc::ConnectionHandler::connectionIsAuthorized() throw() {
/*
	if (Cupv_check (uid, gid, rqst_context->clienthost,
		rqst_context->localhost, P_TAPE_SYSTEM)) {
		rmc_sendrep (rqst_context->rpfd, MSG_ERR, "%s\n",
			sstrerror(serrno));
		rmc_logit (func, "returns %d\n", ERMCUNREC);
		return ERMCUNREC;
	}
*/
  return true; // Client is authorized
}
