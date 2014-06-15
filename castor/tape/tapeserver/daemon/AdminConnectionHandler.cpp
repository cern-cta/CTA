/******************************************************************************
 *         castor/tape/tapeserver/daemon/AdminConnectionHandler.cpp
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
#include <memory>
#include <string.h>
#include <list>
#include <unistd.h>
#include <sys/types.h>
#include <stdio.h>

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::AdminConnectionHandler::AdminConnectionHandler(
  const int fd,
  io::PollReactor &reactor,
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
    m_netTimeout(1) { // Timeout in seconds
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::AdminConnectionHandler::~AdminConnectionHandler() throw() {
  log::Param params[] = {log::Param("fd", m_fd)};
  m_log(LOG_DEBUG, "Closing admin connection", params);
  close(m_fd);
}

//------------------------------------------------------------------------------
// getFd
//------------------------------------------------------------------------------
int castor::tape::tapeserver::daemon::AdminConnectionHandler::getFd() throw() {
  return m_fd;
}

//------------------------------------------------------------------------------
// fillPollFd
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::AdminConnectionHandler::fillPollFd(
  struct pollfd &fd) throw() {
  fd.fd = m_fd;
  fd.events = POLLRDNORM;
  fd.revents = 0;
}

//------------------------------------------------------------------------------
// handleEvent
//------------------------------------------------------------------------------
bool castor::tape::tapeserver::daemon::AdminConnectionHandler::handleEvent(
  const struct pollfd &fd) {
  logAdminConnectionEvent(fd);

  checkHandleEventFd(fd.fd);

  // Do nothing if there is no data to read
  //
  // POLLIN is unfortunately not the logical or of POLLRDNORM and POLLRDBAND
  // on SLC 5.  I therefore replaced POLLIN with the logical or.  I also
  // added POLLPRI into the mix to cover all possible types of read event.
  if(0 == (fd.revents & POLLRDNORM) && 0 == (fd.revents & POLLRDBAND) &&
    0 == (fd.revents & POLLPRI)) {
    return false; // Stay registered with the reactor
  }

  try {
    const legacymsg::MessageHeader header = readJobMsgHeader(fd.fd);
    handleRequest(header);
  } catch(castor::exception::Exception &ex) {
    log::Param params[] = {log::Param("message", ex.getMessage().str())};
    m_log(LOG_ERR, "Failed to handle IO event on admin connection", params);
  }

  return true; // Ask reactor to remove and delete this handler
}

//------------------------------------------------------------------------------
// logAdminConnectionEvent
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::AdminConnectionHandler::
  logAdminConnectionEvent(const struct pollfd &fd)  {
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
  m_log(LOG_DEBUG, "I/O event on admin connection", params);
}

//------------------------------------------------------------------------------
// checkHandleEventFd
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::AdminConnectionHandler::
  checkHandleEventFd(const int fd)  {
  if(m_fd != fd) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to handle admin connection"
      ": Event handler passed wrong file descriptor"
      ": expected=" << m_fd << " actual=" << fd;
    throw ex;
  }
}

//------------------------------------------------------------------------------
// handleRequest
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::AdminConnectionHandler::handleRequest(
  const legacymsg::MessageHeader &header) {
  switch(header.reqType) {
  case TPCONF:
    handleConfigRequest(header);
    break;
  case TPSTAT:
    handleStatRequest(header);
    break;
  default:
    {
      castor::exception::Exception ex;
      ex.getMessage() << "Failed to handle request: Unknown request type"
        ": reqType=" << header.reqType;
      throw ex;
    }
  }
}

//------------------------------------------------------------------------------
// handleConfigRequest
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::AdminConnectionHandler::
  handleConfigRequest(const legacymsg::MessageHeader &header) {
  const uint32_t totalLen = header.lenOrStatus;
  const uint32_t headerLen = 3 * sizeof(uint32_t); // magic, type and len
  const uint32_t bodyLen = totalLen - headerLen;
  const legacymsg::TapeConfigRequestMsgBody body =
    readTapeConfigMsgBody(bodyLen);
  logTapeConfigJobReception(body);

  const std::string unitName(body.drive);
  DriveCatalogueEntry &drive = m_driveCatalogue.findDrive(unitName);
  const utils::DriveConfig &driveConfig = drive.getConfig();

  log::Param params[] = {
    log::Param("unitName", unitName),
    log::Param("dgn", driveConfig.dgn)};

  switch(body.status) {
  case CONF_UP:
    m_vdqm.setDriveUp(m_hostName, unitName, driveConfig.dgn);
    drive.configureUp();
    m_log(LOG_INFO, "Drive configured up", params);
    break;
  case CONF_DOWN:
    m_vdqm.setDriveDown(m_hostName, unitName, driveConfig.dgn);
    drive.configureDown();
    m_log(LOG_INFO, "Drive configured down", params);
    break;
  default:
    {
      castor::exception::Exception ex;
      ex.getMessage() << "Wrong drive status requested: " << body.status;
      throw ex;
    }
  }
  writeTapeRcReplyMsg(0); // Return code of 0 means success
}

//------------------------------------------------------------------------------
// readTapeConfigMsgBody
//------------------------------------------------------------------------------
castor::legacymsg::TapeConfigRequestMsgBody castor::tape::tapeserver::daemon::
  AdminConnectionHandler::readTapeConfigMsgBody(uint32_t bodyLen) {
  char buf[REQBUFSZ];

  if(sizeof(buf) < bodyLen) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to read body of config message"
       ": Maximum body length exceeded"
       ": max=" << sizeof(buf) << " actual=" << bodyLen;
    throw ex;
  }

  try {
    io::readBytes(m_fd, m_netTimeout, bodyLen, buf);
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to read body of config message"
      ": " << ne.getMessage().str();
    throw ex;
  }

  legacymsg::TapeConfigRequestMsgBody body;
  const char *bufPtr = buf;
  size_t bufLen = sizeof(buf);
  legacymsg::unmarshal(bufPtr, bufLen, body);
  return body;
}

//------------------------------------------------------------------------------
// logTapeConfigJobReception
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::AdminConnectionHandler::
  logTapeConfigJobReception(
  const legacymsg::TapeConfigRequestMsgBody &job) const throw() {
  log::Param params[] = {
    log::Param("drive", job.drive),
    log::Param("gid", job.gid),
    log::Param("uid", job.uid),
    log::Param("status", job.status)};
  m_log(LOG_INFO, "Received message from tpconfig command", params);
}

//------------------------------------------------------------------------------
// writeTapeConfigReplyMsg
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::AdminConnectionHandler::
  writeTapeRcReplyMsg(const int rc)  {
  char buf[REPBUFSZ];
  const size_t len = marshalTapeRcReplyMsg(buf, sizeof(buf), rc);
  try {
    io::writeBytes(m_fd, m_netTimeout, len, buf);
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to write job reply message: " <<
      ne.getMessage().str();
    throw ex;
  }
}

//-----------------------------------------------------------------------------
// marshalTapeConfigReplyMsg
//-----------------------------------------------------------------------------
size_t castor::tape::tapeserver::daemon::AdminConnectionHandler::
  marshalTapeRcReplyMsg(char *const dst, const size_t dstLen, const int rc) {
  legacymsg::MessageHeader src;
  src.magic = TPMAGIC;
  src.reqType = TAPERC;
  src.lenOrStatus = rc;
  return legacymsg::marshal(dst, dstLen, src);
}

//------------------------------------------------------------------------------
// writeTapeStatReplyMsg
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::AdminConnectionHandler::
  writeTapeStatReplyMsg()  {
  legacymsg::TapeStatReplyMsgBody body;
  
  const std::list<std::string> unitNames = m_driveCatalogue.getUnitNames();
  if(unitNames.size() > CA_MAXNBDRIVES) {
    castor::exception::Exception ex;
    ex.getMessage() << "Too many drives in drive catalogue: " <<
      unitNames.size() << ": max=" << CA_MAXNBDRIVES << " actual=" <<
      unitNames.size();
    throw ex;
  }
  body.number_of_drives = unitNames.size();
  int i=0;
  for(std::list<std::string>::const_iterator itor = unitNames.begin();
    itor!=unitNames.end() and i<CA_MAXNBDRIVES; itor++) {
    const std::string &unitName = *itor;
    const DriveCatalogueEntry &drive =
       m_driveCatalogue.findConstDrive(unitName);
    body.drives[i] = drive.getTapeStatDriveEntry();
    i++;
  }
  
  char buf[REPBUFSZ];
  const size_t len = legacymsg::marshal(buf, sizeof(buf), body);
  try {
    io::writeBytes(m_fd, m_netTimeout, len, buf);
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to write job reply message: " <<
      ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// handleStatRequest
//------------------------------------------------------------------------------
void
  castor::tape::tapeserver::daemon::AdminConnectionHandler::handleStatRequest(
  const legacymsg::MessageHeader &header) {
  const uint32_t totalLen = header.lenOrStatus;
  const uint32_t headerLen = 3 * sizeof(uint32_t); // magic, type and len
  const uint32_t bodyLen = totalLen - headerLen;
  const legacymsg::TapeStatRequestMsgBody body = readTapeStatMsgBody(bodyLen);
  logTapeStatJobReception(body);
  writeTapeStatReplyMsg();
  writeTapeRcReplyMsg(0); // Return value of 0 means success
}

//------------------------------------------------------------------------------
// logTapeStatJobReception
//------------------------------------------------------------------------------
void
  castor::tape::tapeserver::daemon::AdminConnectionHandler::logTapeStatJobReception(
  const legacymsg::TapeStatRequestMsgBody &job) const throw() {
  log::Param params[] = {
    log::Param("gid", job.gid),
    log::Param("uid", job.uid)};
  m_log(LOG_INFO, "Received message from tpstat command", params);
}

//------------------------------------------------------------------------------
// readJobMsgHeader
//------------------------------------------------------------------------------
castor::legacymsg::MessageHeader
  castor::tape::tapeserver::daemon::AdminConnectionHandler::readJobMsgHeader(
    const int connection)  {
  // Read in the message header
  char buf[3 * sizeof(uint32_t)]; // magic + request type + len
  io::readBytes(connection, m_netTimeout, sizeof(buf), buf);

  const char *bufPtr = buf;
  size_t bufLen = sizeof(buf);
  legacymsg::MessageHeader header;
  memset(&header, '\0', sizeof(header));
  legacymsg::unmarshal(bufPtr, bufLen, header);

  if(TPMAGIC != header.magic) {
    castor::exception::Exception ex;
    ex.getMessage() << "Invalid admin job message: Invalid magic"
      ": expected=0x" << std::hex << TPMAGIC << " actual=0x" <<
      header.magic;
    throw ex;
  }

  if(TPCONF != header.reqType and TPSTAT != header.reqType) {
    castor::exception::Exception ex;
    ex.getMessage() << "Invalid admin job message: Invalid request type"
       ": expected= 0x" << std::hex << TPCONF << " or 0x" << TPSTAT << " actual=0x" <<
       header.reqType;
    throw ex;
  }

  // The length of the message body is checked later, just before it is read in
  // to memory

  return header;
}

//------------------------------------------------------------------------------
// readTapeStatMsgBody
//------------------------------------------------------------------------------
castor::legacymsg::TapeStatRequestMsgBody castor::tape::tapeserver::daemon::
  AdminConnectionHandler::readTapeStatMsgBody(const uint32_t bodyLen) {
  char buf[REQBUFSZ];

  if(sizeof(buf) < bodyLen) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to read body of stat message"
       ": Maximum body length exceeded"
       ": max=" << sizeof(buf) << " actual=" << bodyLen;
    throw ex;
  }

  try {
    io::readBytes(m_fd, m_netTimeout, bodyLen, buf);
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to read body of stat message"
      ": " << ne.getMessage().str();
    throw ex;
  }

  legacymsg::TapeStatRequestMsgBody body;
  const char *bufPtr = buf;
  size_t bufLen = sizeof(buf);
  legacymsg::unmarshal(bufPtr, bufLen, body);
  return body;
}
