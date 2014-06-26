/******************************************************************************
 *         castor/tape/tapeserver/daemon/LabelCmdConnectionHandler.cpp
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
#include "castor/legacymsg/CommonMarshal.hpp"
#include "castor/legacymsg/legacymsg.hpp"
#include "castor/legacymsg/TapeMarshal.hpp"
#include "castor/tape/utils/utils.hpp"
#include "castor/tape/tapeserver/daemon/DriveCatalogue.hpp"
#include "castor/tape/tapeserver/daemon/LabelCmdConnectionHandler.hpp"
#include "castor/utils/SmartFd.hpp"
#include "h/common.h"
#include "h/serrno.h"
#include "h/Ctape.h"
#include "h/vdqm_api.h"
#include "h/vmgr_constants.h"

#include <errno.h>
#include <fcntl.h>
#include <memory>
#include <list>
#include <unistd.h>
#include <sys/types.h>
#include <stdio.h>
#include <unistd.h>

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::LabelCmdConnectionHandler::LabelCmdConnectionHandler(
  const int fd, reactor::ZMQReactor &reactor, log::Logger &log,
  DriveCatalogue &driveCatalogue, const std::string &hostName,
  castor::legacymsg::VdqmProxy & vdqm,
  castor::legacymsg::VmgrProxy & vmgr) throw():
  m_fd(fd),
  m_thisEventHandlerOwnsFd(true),
  m_reactor(reactor),
  m_log(log),
  m_driveCatalogue(driveCatalogue),
  m_hostName(hostName),
  m_vdqm(vdqm),
  m_vmgr(vmgr),
  m_netTimeout(10) { // Timeout in seconds
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::LabelCmdConnectionHandler::
  ~LabelCmdConnectionHandler() throw() {
  if(m_thisEventHandlerOwnsFd) {
    log::Param params[] = {log::Param("fd", m_fd)};
    m_log(LOG_DEBUG, "Closing label-command connection", params);
    close(m_fd);
  }
}

//------------------------------------------------------------------------------
// getFd
//------------------------------------------------------------------------------
int castor::tape::tapeserver::daemon::LabelCmdConnectionHandler::getFd() throw() {
  return m_fd;
}

//------------------------------------------------------------------------------
// fillPollFd
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::LabelCmdConnectionHandler::fillPollFd(zmq::Pollitem &fd) throw() {
  fd.fd = m_fd;
  fd.revents = 0;
  fd.socket = NULL;
}

//------------------------------------------------------------------------------
// handleEvent
//------------------------------------------------------------------------------
bool castor::tape::tapeserver::daemon::LabelCmdConnectionHandler::handleEvent(
  const zmq::Pollitem &fd)  {
  logLabelCmdConnectionEvent(fd);

  checkHandleEventFd(fd.fd);

  std::list<log::Param> params;
  params.push_back(log::Param("fd", m_fd));

  try {
    const legacymsg::MessageHeader header = readMsgHeader();
    handleRequest(header);
  } catch(castor::exception::Exception &ex) {
    params.push_back(log::Param("message", ex.getMessage().str()));
    m_log(LOG_ERR, "Failed to handle IO event on label-command connection",
      params);
  }

  m_log(LOG_DEBUG, "Asking reactor to remove and delete"
    " LabelCmdConnectionHandler", params);
  return true; // Ask reactor to remove and delete this handler
}

//------------------------------------------------------------------------------
// logLabelCmdConnectionEvent
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::LabelCmdConnectionHandler::
  logLabelCmdConnectionEvent(const zmq::Pollitem &fd) {
  log::Param params[] = {
  log::Param("fd", fd.fd),
  log::Param("ZMQ_POLLIN", fd.revents & ZMQ_POLLIN ? "true" : "false"),
  log::Param("ZMQ_POLLOUT", fd.revents & ZMQ_POLLOUT ? "true" : "false"),
  log::Param("ZMQ_POLLERR", fd.revents & ZMQ_POLLERR ? "true" : "false")};
  m_log(LOG_DEBUG, "I/O event on label-command connection", params);
}

//------------------------------------------------------------------------------
// checkHandleEventFd
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::LabelCmdConnectionHandler::
  checkHandleEventFd(const int fd)  {
  if(m_fd != fd) {
    castor::exception::Exception ex;
    ex.getMessage() <<
      "LabelCmdConnectionHandler passed wrong file descriptor"
      ": expected=" << m_fd << " actual=" << fd;
    throw ex;
  }
}

//------------------------------------------------------------------------------
// readMsgHeader
//------------------------------------------------------------------------------
castor::legacymsg::MessageHeader castor::tape::tapeserver::daemon::
  LabelCmdConnectionHandler::readMsgHeader()  {
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

  // The length of the message body is checked later, just before it is read in
  // to memory

  return header;
}

//------------------------------------------------------------------------------
// handleRequest
//------------------------------------------------------------------------------
void
castor::tape::tapeserver::daemon::LabelCmdConnectionHandler::handleRequest(
  const legacymsg::MessageHeader &header) {

  switch(header.reqType) {
  case TPLABEL:
    handleLabelRequest(header);
    break;
  default:
    {
      castor::exception::Exception ex;
      ex.getMessage() << "Failed to handle label-command request"
        ": Unknown request type: reqType=" << header.reqType;
      throw ex;
    }
  }
}

//------------------------------------------------------------------------------
// handleLabelRequest
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::LabelCmdConnectionHandler::
  handleLabelRequest(const legacymsg::MessageHeader &header) {
  const char *const task = "handle incoming label job";

  try {
    // Read message body
    const uint32_t totalLen = header.lenOrStatus;
    const uint32_t headerLen = 3 * sizeof(uint32_t); // magic, type and length
    const uint32_t bodyLen = totalLen - headerLen;
    const legacymsg::TapeLabelRqstMsgBody body = readLabelRqstMsgBody(bodyLen);
    logLabelRequest(body);

    // Try to inform the drive catalogue of the reception of the label job
    DriveCatalogueEntry *const drive = m_driveCatalogue.findDrive(body.drive);
    drive->receivedLabelJob(body, m_fd);

    // The drive catalogue will now remember and own the client connection,
    // therefore it is now safe and necessary for this connection handler to
    // relinquish ownership of the connection
    m_thisEventHandlerOwnsFd = false;
    {
      log::Param params[] = {log::Param("fd", m_fd)};
      m_log(LOG_DEBUG, "Mount-session handler released label connection",
        params);
    }
  } catch(castor::exception::Exception &ex) {
    log::Param params[] = {log::Param("message", ex.getMessage().str())};
    m_log(LOG_ERR, "Informing client label-command of error", params);

    // Inform the client there was an error
    try {
      legacymsg::writeTapeReplyMsg(m_netTimeout, m_fd, 1,
        ex.getMessage().str());
    } catch(castor::exception::Exception &ne) {
      castor::exception::Exception ex;
      ex.getMessage() << "Failed to " << task <<
        ": Failed to inform the client there was an error: " <<
        ne.getMessage().str();
      throw ex;
    }
  }
}

//------------------------------------------------------------------------------
// logLabelRequest
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::LabelCmdConnectionHandler::
  logLabelRequest(const legacymsg::TapeLabelRqstMsgBody &job)
  const throw() {
  log::Param params[] = {
    log::Param("drive", job.drive),
    log::Param("vid", job.vid),
    log::Param("dgn", job.dgn),
    log::Param("uid", job.uid),
    log::Param("gid", job.gid)};
  m_log(LOG_INFO, "Received request to label a tape", params);
}

//------------------------------------------------------------------------------
// readLabelRqstMsgBody
//------------------------------------------------------------------------------
castor::legacymsg::TapeLabelRqstMsgBody castor::tape::tapeserver::daemon::
  LabelCmdConnectionHandler::readLabelRqstMsgBody(const uint32_t bodyLen) {
  char buf[REQBUFSZ];

  if(sizeof(buf) < bodyLen) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to read body of label request"
       ": Maximum body length exceeded"
       ": max=" << sizeof(buf) << " actual=" << bodyLen;
    throw ex;
  }

  try {
    io::readBytes(m_fd, m_netTimeout, bodyLen, buf);
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to read body of label request"
      ": " << ne.getMessage().str();
    throw ex;
  }

  legacymsg::TapeLabelRqstMsgBody body;
  const char *bufPtr = buf;
  size_t bufLen = sizeof(buf);
  legacymsg::unmarshal(bufPtr, bufLen, body);
  return body;
}
