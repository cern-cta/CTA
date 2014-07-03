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
// getName
//------------------------------------------------------------------------------
std::string castor::tape::rmc::ConnectionHandler::getName() const throw() {
  return "rmc::ConnectionHandler";
}

//------------------------------------------------------------------------------
// fillPollFd
//------------------------------------------------------------------------------
void castor::tape::rmc::ConnectionHandler::fillPollFd(zmq_pollitem_t &fd) throw() {
  fd.fd = m_fd;
  fd.events = ZMQ_POLLIN;
  fd.revents = 0;
  fd.socket = NULL;
}

//------------------------------------------------------------------------------
// handleEvent
//------------------------------------------------------------------------------
bool castor::tape::rmc::ConnectionHandler::handleEvent(const zmq_pollitem_t &fd)  {
  log::Param params[] = {
  log::Param("fd", fd.fd),
  log::Param("ZMQ_POLLIN", fd.revents & ZMQ_POLLIN ? "true" : "false"),
  log::Param("ZMQ_POLLOUT", fd.revents & ZMQ_POLLOUT ? "true" : "false"),
  log::Param("ZMQ_POLLERR", fd.revents & ZMQ_POLLERR ? "true" : "false")};
  m_log(LOG_DEBUG, "I/O event on rmc connection", params);

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
