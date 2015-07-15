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
#include "castor/legacymsg/CommonMarshal.hpp"
#include "castor/legacymsg/TapeMarshal.hpp"
#include "castor/legacymsg/legacymsg.hpp"
#include "castor/tape/tapeserver/daemon/AdminConnectionHandler.hpp"
#include "castor/utils/SmartFd.hpp"
#include "castor/utils/utils.hpp"
#include "common.h"
#include "serrno.h"
#include "Ctape.h"
#include "vdqm_api.h"

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
  reactor::ZMQReactor &reactor,
  log::Logger &log,
  Catalogue &driveCatalogue)
  throw():
    m_fd(fd),
    m_reactor(reactor),
    m_log(log),
    m_driveCatalogue(driveCatalogue),
    m_netTimeout(1) { // Timeout in seconds
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::AdminConnectionHandler::
  ~AdminConnectionHandler() throw() {
  log::Param params[] = {log::Param("fd", m_fd)};
  m_log(LOG_DEBUG, "Closing admin connection", params);
  close(m_fd);
}

//------------------------------------------------------------------------------
// getName
//------------------------------------------------------------------------------
std::string castor::tape::tapeserver::daemon::AdminConnectionHandler::getName() 
  const throw() {
  return "AdminConnectionHandler";
}

//------------------------------------------------------------------------------
// fillPollFd
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::AdminConnectionHandler::fillPollFd(
  zmq_pollitem_t &fd) throw() {
  fd.fd = m_fd;
  fd.events = ZMQ_POLLIN;
  fd.revents = 0;
  fd.socket = NULL;
}

//------------------------------------------------------------------------------
// handleEvent
//------------------------------------------------------------------------------
bool castor::tape::tapeserver::daemon::AdminConnectionHandler::handleEvent(
  const zmq_pollitem_t &fd) {
  logAdminConnectionEvent(fd);

  checkHandleEventFd(fd.fd);

  std::list<log::Param> params;
  params.push_back(log::Param("fd", m_fd));


  try {
    const legacymsg::MessageHeader header = readMsgHeader();
    handleRequest(header);
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to handle IO event on admin connection. Reason: " << ne.getMessage().str() << std::endl;    
    m_log(LOG_ERR, ex.getMessage().str());
    try {
      castor::legacymsg::writeTapeReplyErrorMsg(m_netTimeout, m_fd, ex.getMessage().str());
    } catch(castor::exception::Exception &ne2) {
      castor::exception::Exception ex2;
      ex2.getMessage() << "Failed to notify the client of the failure of the handling of the IO event on admin connection. Reason: " << ne2.getMessage().str();
      m_log(LOG_ERR, ex2.getMessage().str());
    }
  }

  m_log(LOG_DEBUG, "Asking reactor to remove and delete"
    " AdminConnectionHandler", params);
  return true; // Ask reactor to remove and delete this handler
}

//------------------------------------------------------------------------------
// logAdminConnectionEvent
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::AdminConnectionHandler::
  logAdminConnectionEvent(const zmq_pollitem_t &fd)  {
  log::Param params[] = {
  log::Param("fd", fd.fd),
  log::Param("ZMQ_POLLIN", fd.revents & ZMQ_POLLIN ? "true" : "false"),
  log::Param("ZMQ_POLLOUT", fd.revents & ZMQ_POLLOUT ? "true" : "false"),
  log::Param("ZMQ_POLLERR", fd.revents & ZMQ_POLLERR ? "true" : "false")};
  m_log(LOG_DEBUG, "I/O event on admin connection", params);
}

//------------------------------------------------------------------------------
// checkHandleEventFd
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::AdminConnectionHandler::
  checkHandleEventFd(const int fd)  {
  if(m_fd != fd) {
    castor::exception::Exception ex;
    ex.getMessage() <<
      "AdminConnectionHandler passed wrong file descriptor"
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
    handleConfigRequestAndReply(header);
    break;
  case TPSTAT:
    handleStatRequestAndReply(header);
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
// handleConfigRequestAndReply
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::AdminConnectionHandler::
  handleConfigRequestAndReply(const legacymsg::MessageHeader &header) {
  try {
    handleConfigRequest(header);
    writeTapeRcReplyMsg(0); // Return code of 0 means success
  } catch(castor::exception::Exception &ex) {
    log::Param params[] = {log::Param("message", ex.getMessage().str())};
    m_log(LOG_ERR, "Failed to handle config request", params);
    writeTapeRcReplyMsg(ex.code());
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

  CatalogueDrive &drive = m_driveCatalogue.findDrive(unitName);
  const DriveConfig &driveConfig = drive.getConfig();

  log::Param params[] = {
    log::Param("unitName", unitName),
    log::Param("dgn", driveConfig.getDgn())};

  switch(body.status) {
  case CONF_UP:
    drive.configureUp();
    m_log(LOG_INFO, "Drive configured up", params);
    break;
  case CONF_DOWN:
    drive.configureDown();
    m_log(LOG_INFO, "Drive configured down", params);
    break;
  default:
    {
      castor::exception::Exception ex;
      ex.getMessage() << "Failed to configure drive"
        ": Unexpected drive status requested: status=" << body.status;
      throw ex;
    }
  }
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
    const CatalogueDrive &drive = m_driveCatalogue.findDrive(unitName);
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
// handleStatRequestAndReply
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::AdminConnectionHandler::
  handleStatRequestAndReply(const legacymsg::MessageHeader &header) {
  try {
    handleStatRequest(header);
    writeTapeStatReplyMsg();
    writeTapeRcReplyMsg(0); // Return value of 0 means success
  } catch(castor::exception::Exception &ex) {
    log::Param params[] = {log::Param("message", ex.getMessage().str())};
    m_log(LOG_ERR, "Failed to handle stat request", params);
    writeTapeRcReplyMsg(ex.code());
  }
}

//------------------------------------------------------------------------------
// handleStatRequest
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::AdminConnectionHandler::
  handleStatRequest(const legacymsg::MessageHeader &header) {
  const uint32_t totalLen = header.lenOrStatus;
  const uint32_t headerLen = 3 * sizeof(uint32_t); // magic, type and len
  const uint32_t bodyLen = totalLen - headerLen;
  const legacymsg::TapeStatRequestMsgBody body = readTapeStatMsgBody(bodyLen);
  logTapeStatJobReception(body);
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
  castor::tape::tapeserver::daemon::AdminConnectionHandler::readMsgHeader() {
  // Read in the message header
  char buf[3 * sizeof(uint32_t)]; // magic + request type + len
  io::readBytes(m_fd, m_netTimeout, sizeof(buf), buf);

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
