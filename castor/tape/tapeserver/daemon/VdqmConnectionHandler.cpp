/******************************************************************************
 *         castor/tape/tapeserver/daemon/VdqmConnectionHandler.cpp
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

#include "castor/tape/tapeserver/daemon/VdqmConnectionHandler.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::VdqmConnectionHandler::VdqmConnectionHandler(
  const int connection, io::PollReactor &reactor, log::Logger &log, Vdqm &vdqm)
  throw(): m_connection(connection), m_reactor(reactor), m_log(log),
    m_vdqm(vdqm)  {
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::VdqmConnectionHandler::
  ~VdqmConnectionHandler() throw() {
}

//------------------------------------------------------------------------------
// getFd
//------------------------------------------------------------------------------
int castor::tape::tapeserver::daemon::VdqmConnectionHandler::getFd() throw() {
  return m_connection;
}

//------------------------------------------------------------------------------
// fillPollFd
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::VdqmConnectionHandler::fillPollFd(
  struct pollfd &fd) throw() {
  fd.fd = m_connection;
  fd.events = POLLRDNORM;
  fd.revents = 0;
}

//------------------------------------------------------------------------------
// handleEvent
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::VdqmConnectionHandler::handleEvent(
  const struct pollfd &fd) throw(castor::exception::Exception) {

  // Do nothing if there is not data to be read
  if(0 == fd.revents & POLLIN) {
    return;
  }

  const int timeout = 1; // 1 second
  const legacymsg::RtcpJobRqstMsgBody job = m_vdqm.receiveJob(fd.fd, timeout);
  logVdqmJobReception(job);
}

//------------------------------------------------------------------------------
// logVdqmJobReception
//------------------------------------------------------------------------------
void
  castor::tape::tapeserver::daemon::VdqmConnectionHandler::logVdqmJobReception(
  const legacymsg::RtcpJobRqstMsgBody &job) const throw() {
  log::Param params[] = {
    log::Param("volReqId", job.volReqId),
    log::Param("clientPort", job.clientPort),
    log::Param("clientEuid", job.clientEuid),
    log::Param("clientEgid", job.clientEgid),
    log::Param("clientHost", job.clientHost),
    log::Param("dgn", job.dgn),
    log::Param("driveUnit", job.driveUnit),
    log::Param("clientUserName", job.clientUserName)};
  m_log(LOG_INFO, "Received job from the vdqmd daemon", params);
}
