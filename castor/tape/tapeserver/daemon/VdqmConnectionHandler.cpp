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
#include "h/common.h"
#include "h/serrno.h"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::VdqmConnectionHandler::VdqmConnectionHandler(
  const int fd,
  io::PollReactor &reactor,
  log::Logger &log,
  DriveCatalogue &driveCatalogue) throw():
    m_fd(fd),
    m_reactor(reactor),
    m_log(log),
    m_driveCatalogue(driveCatalogue),
    m_netTimeout(1) // Timeout in seconds
{
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::VdqmConnectionHandler::~VdqmConnectionHandler() throw() {
  log::Param params[] = {log::Param("fd", m_fd)};
  m_log(LOG_DEBUG, "Closing vdqm connection", params);
  close(m_fd);
}

//------------------------------------------------------------------------------
// getFd
//------------------------------------------------------------------------------
int castor::tape::tapeserver::daemon::VdqmConnectionHandler::getFd() throw() {
  return m_fd;
}

//------------------------------------------------------------------------------
// fillPollFd
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::VdqmConnectionHandler::fillPollFd(struct pollfd &fd) throw() {
  fd.fd = m_fd;
  fd.events = POLLRDNORM;
  fd.revents = 0;
}

//------------------------------------------------------------------------------
// handleEvent
//------------------------------------------------------------------------------
bool castor::tape::tapeserver::daemon::VdqmConnectionHandler::handleEvent(
  const struct pollfd &fd)  {
  logVdqmConnectionEvent(fd);

  checkHandleEventFd(fd.fd);

  std::list<log::Param> params;
  params.push_back(log::Param("fd", m_fd));

  // Do nothing if there is no data to read
  //
  // POLLIN is unfortuntaley not the logical or of POLLRDNORM and POLLRDBAND
  // on SLC 5.  I therefore replaced POLLIN with the logical or.  I also
  // added POLLPRI into the mix to cover all possible types of read event.
  if(0 == (fd.revents & POLLRDNORM) && 0 == (fd.revents & POLLRDBAND) &&
    0 == (fd.revents & POLLPRI)) {
    return false; // Stay registered with the reactor
  }

  if(!connectionIsAuthorized()) {
    return true; // Ask reactor to remove and delete this handler
  }

  try {
    const legacymsg::RtcpJobRqstMsgBody job = readJobMsg();
    logVdqmJobReception(job);
    writeJobReplyMsg();
    DriveCatalogueEntry &drive = m_driveCatalogue.findDrive(job.driveUnit);
    drive.receivedVdqmJob(job);
  } catch(castor::exception::Exception &ex) {
    params.push_back(log::Param("message", ex.getMessage().str()));
    m_log(LOG_ERR, "Failed to handle vdqm-connection event", params);
  }

  m_log(LOG_DEBUG, "Asking reactor to remove and delete"
    " VdqmConnectionHandler", params);
  return true; // Ask reactor to remove and delete this handler
}

//------------------------------------------------------------------------------
// logVdqmConnectionEvent 
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::VdqmConnectionHandler::
  logVdqmConnectionEvent(const struct pollfd &fd)  {
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
  m_log(LOG_DEBUG, "I/O event on vdqm connection", params);
}

//------------------------------------------------------------------------------
// checkHandleEventFd
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::VdqmConnectionHandler::
  checkHandleEventFd(const int fd)  {
  if(m_fd != fd) {
    castor::exception::Exception ex;
    ex.getMessage() <<
      "VdqmConnectionHandler passed wrong file descriptor"
      ": expected=" << m_fd << " actual=" << fd;
    throw ex;
  }
}

//------------------------------------------------------------------------------
// connectionIsAuthorized
//------------------------------------------------------------------------------
bool castor::tape::tapeserver::daemon::VdqmConnectionHandler::connectionIsAuthorized() throw() {
  // isadminhost fills in peerHost
  char peerHost[CA_MAXHOSTNAMELEN+1];
  memset(peerHost, '\0', sizeof(peerHost));

  // Unfortunately isadminhost can either set errno or serrno
  serrno = 0;
  errno = 0;
  const int rc = isadminhost(m_fd, peerHost);
  if(0 == rc) {
    log::Param params[] = {
      log::Param("host", peerHost[0] != '\0' ? peerHost : "UNKNOWN")};
    m_log(LOG_INFO, "Received connection from authorized admin host", params);
    return true; // Connection is authorized
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
      errorMessage = "UNKNOWN";
    }
    log::Param params[] = {
      log::Param("host", peerHost[0] != '\0' ? peerHost : "UNKNOWN"),
      log::Param("message", errorMessage)};
    m_log(LOG_ERR, "Failed to authorize vdqm host", params);

    return false; // Connection is not authorized
  }
}

//------------------------------------------------------------------------------
// logVdqmJobReception
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::VdqmConnectionHandler::logVdqmJobReception(const legacymsg::RtcpJobRqstMsgBody &job) const throw() {
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

//------------------------------------------------------------------------------
// readJobMsg
//------------------------------------------------------------------------------
castor::legacymsg::RtcpJobRqstMsgBody
  castor::tape::tapeserver::daemon::VdqmConnectionHandler::readJobMsg()  {
  const legacymsg::MessageHeader header = readMsgHeader();
  const legacymsg::RtcpJobRqstMsgBody body = readJobMsgBody(header.lenOrStatus);

  return body;
}

//------------------------------------------------------------------------------
// readMsgHeader
//------------------------------------------------------------------------------
castor::legacymsg::MessageHeader
  castor::tape::tapeserver::daemon::VdqmConnectionHandler::readMsgHeader()  {
  // Read in the message header
  char buf[3 * sizeof(uint32_t)]; // magic + request type + len
  io::readBytes(m_fd, m_netTimeout, sizeof(buf), buf);

  const char *bufPtr = buf;
  size_t bufLen = sizeof(buf);
  legacymsg::MessageHeader header;
  memset(&header, '\0', sizeof(header));
  legacymsg::unmarshal(bufPtr, bufLen, header);

  if(RTCOPY_MAGIC_OLD0 != header.magic) {
    castor::exception::Exception ex;
    ex.getMessage() << "Invalid vdqm job message: Invalid magic"
      ": expected=0x" << std::hex << RTCOPY_MAGIC_OLD0 << " actual=0x" <<
      header.magic;
    throw ex;
  }

  if(VDQM_CLIENTINFO != header.reqType) {
    castor::exception::Exception ex;
    ex.getMessage() << "Invalid vdqm job message: Invalid request type"
       ": expected=0x" << std::hex << VDQM_CLIENTINFO << " actual=0x" <<
       header.reqType;
    throw ex;
  }

  // The length of the message body is checked later, just before it is read in
  // to memory

  return header;
}

//------------------------------------------------------------------------------
// readJobMsgBody
//------------------------------------------------------------------------------
castor::legacymsg::RtcpJobRqstMsgBody castor::tape::tapeserver::daemon::
  VdqmConnectionHandler::readJobMsgBody(const uint32_t len)  {
  char buf[1024];

  if(sizeof(buf) < len) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to read body of job message"
       ": Maximum body length exceeded"
       ": max=" << sizeof(buf) << " actual=" << len;
    throw ex;
  }

  try {
    io::readBytes(m_fd, m_netTimeout, len, buf);
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to read body of job message"
      ": " << ne.getMessage().str();
    throw ex;
  }

  legacymsg::RtcpJobRqstMsgBody body;
  const char *bufPtr = buf;
  size_t bufLen = sizeof(buf);
  legacymsg::unmarshal(bufPtr, bufLen, body);
  return body;
}

//------------------------------------------------------------------------------
// writeJobReplyMsg
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::VdqmConnectionHandler::
  writeJobReplyMsg()  {
  legacymsg::RtcpJobReplyMsgBody body;
  char buf[1024];
  const size_t len = legacymsg::marshal(buf, body);
  try {
    io::writeBytes(m_fd, m_netTimeout, len, buf);
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to write job reply message: " <<
      ne.getMessage().str();
    throw ex;
  }
}
