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
#include "castor/exception/Internal.hpp"
#include "castor/io/io.hpp"
#include "castor/tape/tapeserver/daemon/AdminAcceptHandler.hpp"
#include "castor/tape/tapeserver/daemon/VdqmConnectionHandler.hpp"
#include "castor/utils/SmartFd.hpp"
#include "h/common.h"
#include "h/serrno.h"
#include "h/Ctape.h"
#include "castor/tape/legacymsg/CommonMarshal.hpp"
#include "castor/tape/legacymsg/AdminMarshal.hpp"
#include "castor/tape/utils/utils.hpp"

#include <vdqm_api.h>

#include <errno.h>
#include <memory>
#include <string.h>

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::AdminAcceptHandler::AdminAcceptHandler(
  const int fd,
  io::PollReactor &reactor,
  log::Logger &log,
  Vdqm &vdqm,
  DriveCatalogue &driveCatalogue,
  const std::string &hostName)
  throw():
    m_fd(fd),
    m_reactor(reactor),
    m_log(log),
    m_vdqm(vdqm),
    m_driveCatalogue(driveCatalogue),
    m_hostName(hostName),
    m_netTimeout(10) {
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::AdminAcceptHandler::~AdminAcceptHandler()
  throw() {
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
void castor::tape::tapeserver::daemon::AdminAcceptHandler::fillPollFd(
  struct pollfd &fd) throw() {
  fd.fd = m_fd;
  fd.events = POLLRDNORM;
  fd.revents = 0;
}

//-----------------------------------------------------------------------------
// marshalReplyMsg
//-----------------------------------------------------------------------------
size_t castor::tape::tapeserver::daemon::AdminAcceptHandler::marshalReplyMsg(char *const dst,
  const size_t dstLen, const int rc)
  throw(castor::exception::Exception) {

  if(dst == NULL) {
    TAPE_THROW_CODE(EINVAL,
      ": Pointer to destination buffer is NULL");
  }

  // Calculate the length of the message header
  const uint32_t totalLen = (2 * sizeof(uint32_t)) + sizeof(int32_t);  // magic + reqType + returnCode

  // Check that the message header buffer is big enough
  if(totalLen > dstLen) {
    TAPE_THROW_CODE(EMSGSIZE,
      ": Buffer too small for reply message"
      ": Required size: " << totalLen <<
      ": Actual size: " << dstLen);
  }

  // Marshal the message header
  char *p = dst;
  io::marshalUint32(TPMAGIC, p);
  io::marshalUint32(TAPERC, p);
  io::marshalInt32(rc, p);

  // Calculate the number of bytes actually marshalled
  const size_t nbBytesMarshalled = p - dst;

  // Check that the number of bytes marshalled was what was expected
  if(totalLen != nbBytesMarshalled) {
    TAPE_THROW_EX(castor::exception::Internal,
      ": Mismatch between the expected total length of the "
      "reply message and the actual number of bytes marshalled"
      ": Expected: " << totalLen <<
      ": Marshalled: " << nbBytesMarshalled);
  }

  return totalLen;
}

//------------------------------------------------------------------------------
// writeReplyMsg
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::AdminAcceptHandler::writeReplyMsg(const int fd, const int rc) throw(castor::exception::Exception) {
  legacymsg::RtcpJobReplyMsgBody body;
  char buf[1024];
  const size_t len = marshalReplyMsg(buf, sizeof(buf), rc);
  try {
    io::writeBytes(fd, m_netTimeout, len, buf);
  } catch(castor::exception::Exception &ne) {
    castor::exception::Internal ex;
    ex.getMessage() << "Failed to write job reply message: " <<
      ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// handleEvent
//------------------------------------------------------------------------------
bool castor::tape::tapeserver::daemon::AdminAcceptHandler::handleEvent(
  const struct pollfd &fd) throw(castor::exception::Exception) {
  checkHandleEventFd(fd.fd);

  // Do nothing if there is no data to read
  //
  // POLLIN is unfortunately not the logical or of POLLRDNORM and POLLRDBAND
  // on SLC 5.  I therefore replaced POLLIN with the logical or.  I also
  // added POLLPRI into the mix to cover all possible types of read event.
  if(0 == (fd.revents & POLLRDNORM) && 0 == (fd.revents & POLLRDBAND) &&
    0 == (fd.revents & POLLPRI)) {
    return false; // Stayt registered with the reactor
  }

  // Accept the connection from the admin command
  castor::utils::SmartFd connection;
  try {
    connection.reset(io::acceptConnection(fd.fd, 1));
  } catch(castor::exception::Exception &ne) {
    castor::exception::Internal ex;
    ex.getMessage() << "Failed to accept a connection from the admin command"
      ": " << ne.getMessage().str();
    throw ex;
  }
  
  const legacymsg::TapeMsgBody job = readJobMsg(connection.get());
  logAdminJobReception(job);
  const std::string unitName(job.drive);
  const std::string dgn = m_driveCatalogue.getDgn(unitName);

  if(CONF_UP==job.status) {
    m_vdqm.setDriveStatusUp(m_hostName, unitName, dgn);
    m_driveCatalogue.configureUp(unitName);
    m_log(LOG_INFO, "Drive is up now");
  }
  else if(CONF_DOWN==job.status) {
    m_vdqm.setDriveStatusDown(m_hostName, unitName, dgn);
    m_driveCatalogue.configureDown(unitName);
    m_log(LOG_INFO, "Drive is down now");
  }
  else {
    castor::exception::Internal ex;
    ex.getMessage() << "Wrong drive status requested:" << job.status;
    throw ex;
  }
  writeReplyMsg(connection.get(), 0);  
  return false; // Stay registered with the reactor
}

//------------------------------------------------------------------------------
// checkHandleEventFd
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::AdminAcceptHandler::checkHandleEventFd(
  const int fd) throw (castor::exception::Exception) {
  if(m_fd != fd) {
    castor::exception::Internal ex;
    ex.getMessage() << "Failed to accept connection from the admin command"
      ": Event handler passed wrong file descriptor"
      ": expected=" << m_fd << " actual=" << fd;
    throw ex;
  }
}

//------------------------------------------------------------------------------
// logAdminJobReception
//------------------------------------------------------------------------------
void
  castor::tape::tapeserver::daemon::AdminAcceptHandler::logAdminJobReception(
  const legacymsg::TapeMsgBody &job) const throw() {
  log::Param params[] = {
    log::Param("drive", job.drive),
    log::Param("gid", job.gid),
    log::Param("uid", job.uid),
    log::Param("status", job.status)};
  m_log(LOG_INFO, "Received message from admin command", params);
}

//------------------------------------------------------------------------------
// readJob
//------------------------------------------------------------------------------
castor::tape::legacymsg::TapeMsgBody
  castor::tape::tapeserver::daemon::AdminAcceptHandler::readJobMsg(
    const int connection) throw(castor::exception::Exception) {
  
  const legacymsg::MessageHeader header = readJobMsgHeader(connection);
  const legacymsg::TapeMsgBody body = readJobMsgBody(connection, header.lenOrStatus-sizeof(header));

  return body;
}

//------------------------------------------------------------------------------
// readJobMsgHeader
//------------------------------------------------------------------------------
castor::tape::legacymsg::MessageHeader
  castor::tape::tapeserver::daemon::AdminAcceptHandler::readJobMsgHeader(
    const int connection) throw(castor::exception::Exception) {
  // Read in the message header
  char buf[3 * sizeof(uint32_t)]; // magic + request type + len
  io::readBytes(connection, m_netTimeout, sizeof(buf), buf);

  const char *bufPtr = buf;
  size_t bufLen = sizeof(buf);
  legacymsg::MessageHeader header;
  memset(&header, '\0', sizeof(header));
  legacymsg::unmarshal(bufPtr, bufLen, header);

  if(TPMAGIC != header.magic) {
    castor::exception::Internal ex;
    ex.getMessage() << "Invalid admin job message: Invalid magic"
      ": expected=0x" << std::hex << TPMAGIC << " actual=0x" <<
      header.magic;
    throw ex;
  }

  if(TPCONF != header.reqType) {
    castor::exception::Internal ex;
    ex.getMessage() << "Invalid admin job message: Invalid request type"
       ": expected=0x" << std::hex << TPCONF << " actual=0x" <<
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
castor::tape::legacymsg::TapeMsgBody
  castor::tape::tapeserver::daemon::AdminAcceptHandler::readJobMsgBody(
    const int connection, const uint32_t len)
    throw(castor::exception::Exception) {
  char buf[REQBUFSZ];

  if(sizeof(buf) < len) {
    castor::exception::Internal ex;
    ex.getMessage() << "Failed to read body of job message"
       ": Maximum body length exceeded"
       ": max=" << sizeof(buf) << " actual=" << len;
    throw ex;
  }

  try {
    io::readBytes(connection, m_netTimeout, len, buf);
  } catch(castor::exception::Exception &ne) {
    castor::exception::Internal ex;
    ex.getMessage() << "Failed to read body of job message"
      ": " << ne.getMessage().str();
    throw ex;
  }

  legacymsg::TapeMsgBody body;
  const char *bufPtr = buf;
  size_t bufLen = sizeof(buf);
  legacymsg::unmarshal(bufPtr, bufLen, body);
  return body;
}
