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

#include "castor/legacymsg/legacymsg.hpp"
#include "castor/messages/Constants.hpp"
#include "castor/tape/tapeserver/daemon/ProcessForkerConnectionHandler.hpp"
#include "castor/tape/tapeserver/daemon/ProcessForkerUtils.hpp"
#include "castor/utils/SmartFd.hpp"
#include "h/common.h"
#include "h/serrno.h"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::ProcessForkerConnectionHandler::
  ProcessForkerConnectionHandler(
  const int fd,
  reactor::ZMQReactor &reactor,
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
castor::tape::tapeserver::daemon::ProcessForkerConnectionHandler::
  ~ProcessForkerConnectionHandler() throw() {
  log::Param params[] = {log::Param("fd", m_fd)};
  m_log(LOG_DEBUG, "Closing incoming connection from the ProcessForker",
    params);
  close(m_fd);
}

//------------------------------------------------------------------------------
// getName
//------------------------------------------------------------------------------
std::string castor::tape::tapeserver::daemon::ProcessForkerConnectionHandler::
  getName() const throw() {
  return "ProcessForkerConnectionHandler";
}

//------------------------------------------------------------------------------
// fillPollFd
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::ProcessForkerConnectionHandler::
  fillPollFd(zmq_pollitem_t &fd) throw() {
  fd.fd = m_fd;
  fd.events = ZMQ_POLLIN;
  fd.revents = 0;
  fd.socket = NULL;
}

//------------------------------------------------------------------------------
// handleEvent
//------------------------------------------------------------------------------
bool castor::tape::tapeserver::daemon::ProcessForkerConnectionHandler::
  handleEvent(const zmq_pollitem_t &fd)  {
  logConnectionEvent(fd);

  checkHandleEventFd(fd.fd);

  handleMsg();

  return false; // Ask reactor to keep this handler registered
}

//------------------------------------------------------------------------------
// logConnectionEvent 
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::ProcessForkerConnectionHandler::
  logConnectionEvent(const zmq_pollitem_t &fd)  {
  log::Param params[] = {
  log::Param("fd", fd.fd),
  log::Param("ZMQ_POLLIN", fd.revents & ZMQ_POLLIN ? "true" : "false"),
  log::Param("ZMQ_POLLOUT", fd.revents & ZMQ_POLLOUT ? "true" : "false"),
  log::Param("ZMQ_POLLERR", fd.revents & ZMQ_POLLERR ? "true" : "false")};
  m_log(LOG_DEBUG, "I/O event on incoming ProcessForker connection", params);
}

//------------------------------------------------------------------------------
// checkHandleEventFd
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::ProcessForkerConnectionHandler::
  checkHandleEventFd(const int fd)  {
  if(m_fd != fd) {
    castor::exception::Exception ex;
    ex.getMessage() <<
      "ProcessForkerConnectionHandler passed wrong file descriptor"
      ": expected=" << m_fd << " actual=" << fd;
    throw ex;
  }
}

//------------------------------------------------------------------------------
// handleMsg
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::ProcessForkerConnectionHandler::
  handleMsg() {
  ProcessForkerFrame frame;
  try {
    const int timeout = 10; // Timeout in seconds
    frame = ProcessForkerUtils::readFrame(m_fd, timeout);
  } catch(castor::exception::Exception &ne) {
    log::Param params[] = {log::Param("message", ne.getMessage().str())};
    m_log(LOG_ERR, "ProcessForkerConnectionHandler failed to handle message",
      params);
    sleep(1); // Sleep a moment to avoid going into a tight error loop
  }

  log::Param params[] = {
    log::Param("type", messages::msgTypeToString(frame.type)),
    log::Param("payloadLen", frame.payload.length())};
  m_log(LOG_INFO, "ProcessForkerConnectionHandler handling a message", params);

  dispatchMsgHandler(frame);
}

//------------------------------------------------------------------------------
// dispatchMsgHandler
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::ProcessForkerConnectionHandler::
  dispatchMsgHandler(const ProcessForkerFrame &frame) {
  switch(frame.type) {
  case messages::MSG_TYPE_PROCESSCRASHED:
    return handleProcessCrashedMsg(frame);
  case messages::MSG_TYPE_PROCESSEXITED:
    return handleProcessExitedMsg(frame);
  default:
    {
      castor::exception::Exception ex;
      ex.getMessage() << "ProcessForkerConnectionHandler failed to dispatch"
        " message handler: Unexpected message type"
        ": type=" << messages::msgTypeToString(frame.type);
      throw ex;
    }
  }
}

//------------------------------------------------------------------------------
// handleProcessCrashedMsg
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::ProcessForkerConnectionHandler::
  handleProcessCrashedMsg(const ProcessForkerFrame &frame) {
  try {
    // Parse the message
    messages::ProcessCrashed msg;
    ProcessForkerUtils::parsePayload(frame, msg);

    // Log the contents of the message
    std::list<log::Param> params;
    params.push_back(log::Param("pid", msg.pid()));
    params.push_back(log::Param("signal", msg.signal()));
    m_log(LOG_INFO,
      "ProcessForkerConnectionHandler handling ProcessCrashed message", params);

    // Notify the catalogue session
    DriveCatalogueEntry &drive = m_driveCatalogue.findDrive(msg.pid());
    drive.sessionFailed();

    // Create a cleaner session if the session that crashed is itself not a
    // cleaner
    if(DriveCatalogueEntry::SESSION_TYPE_CLEANER != drive.getSessionType()) {
      const std::string vid = drive.getVidForCleaner();
      const time_t assignmentTime = drive.getAssignmentTimeForCleaner();
      drive.createCleaner(vid, assignmentTime);
    }

  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to handle ProcessCrashed message: " <<
      ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// handleProcessExitedMsg
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::ProcessForkerConnectionHandler::
  handleProcessExitedMsg(const ProcessForkerFrame &frame) {
  try {
    // Parse the message
    messages::ProcessExited msg;
    ProcessForkerUtils::parsePayload(frame, msg);

    // Log the contents of the message
    std::list<log::Param> params;
    params.push_back(log::Param("pid", msg.pid()));
    params.push_back(log::Param("exitCode", msg.exitcode()));
    m_log(LOG_INFO, 
      "ProcessForkerConnectionHandler handling ProcessExited message", params);

    DriveCatalogueEntry &drive = m_driveCatalogue.findDrive(msg.pid());
    if(0 == msg.exitcode()) {
      drive.sessionSucceeded();
    } else {
      drive.sessionFailed();
    }

  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to handle ProcessExited message: " <<
      ne.getMessage().str();
    throw ex;
  }
}
