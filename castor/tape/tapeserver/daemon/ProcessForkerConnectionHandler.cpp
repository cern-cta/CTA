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
  DriveCatalogue &driveCatalogue,
  const std::string &hostName,
  legacymsg::VdqmProxy &vdqm) throw():
  m_fd(fd),
  m_reactor(reactor),
  m_log(log),
  m_driveCatalogue(driveCatalogue),
  m_hostName(hostName),
  m_vdqm(vdqm),
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
      ex.getMessage() << "Failed to dispatch message handler"
        ": Unexpected message type"
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

    DriveCatalogueEntry &drive = m_driveCatalogue.findDrive(msg.pid());
    dispatchCrashedProcessHandler(drive, msg);
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to handle ProcessCrashed message: " <<
      ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// dispatchCrashedProcessHandler
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::ProcessForkerConnectionHandler::
  dispatchCrashedProcessHandler(DriveCatalogueEntry &drive,
  const messages::ProcessCrashed &msg) {

  switch(drive.getSessionType()) {
  case DriveCatalogueEntry::SESSION_TYPE_DATATRANSFER:
    return handleCrashedDataTransferSession(drive, msg);
  case DriveCatalogueEntry::SESSION_TYPE_LABEL:
    return handleCrashedLabelSession(drive, msg);
  case DriveCatalogueEntry::SESSION_TYPE_CLEANER:
    return handleCrashedCleanerSession(drive, msg);
  default:
    {
      castor::exception::Exception ex;
      ex.getMessage() << "Failed to dispatch handler for crashed process"
        ": Unexpected session type: sessionType=" << drive.getSessionType();
      throw ex;
    }
  }
}

//------------------------------------------------------------------------------
// handleCrashedDataTransferSession
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::ProcessForkerConnectionHandler::
  handleCrashedDataTransferSession(DriveCatalogueEntry &drive,
  const messages::ProcessCrashed &msg) {
  std::list<log::Param> params;
  params.push_back(log::Param("pid", msg.pid()));
  params.push_back(log::Param("signal", msg.signal()));

  try {
    drive.sessionFailed();
    m_log(LOG_WARNING, "Data-transfer session failed", params);
    drive.createCleaner();
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to handle crashed data-transfer session: " << 
    ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// handleCrashedCleanerSession
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::ProcessForkerConnectionHandler::
  handleCrashedCleanerSession(DriveCatalogueEntry &drive,
  const messages::ProcessCrashed &msg) {
  std::list<log::Param> params;
  params.push_back(log::Param("pid", msg.pid()));
  params.push_back(log::Param("signal", msg.signal()));

  try {
    drive.sessionFailed();
    m_log(LOG_WARNING, "Cleaner session failed", params);
    setDriveDownInVdqm(msg.pid(), drive.getConfig());
  }  catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to handle crashed cleaner session: " << 
    ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// handleCrashedLabelSession 
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::ProcessForkerConnectionHandler::
  handleCrashedLabelSession(DriveCatalogueEntry &drive,
  const messages::ProcessCrashed &msg) {
  std::list<log::Param> params;
  params.push_back(log::Param("pid", msg.pid()));
  params.push_back(log::Param("signal", msg.signal()));

  try {
    drive.sessionFailed();
    m_log(LOG_WARNING, "Label session failed", params);
    setDriveDownInVdqm(msg.pid(), drive.getConfig());
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to handle crashed label session: " <<
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
    dispatchExitedProcessHandler(drive, msg);
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to handle ProcessExited message: " <<
      ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// dispatchExitedProcessHandler
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::ProcessForkerConnectionHandler::
  dispatchExitedProcessHandler(DriveCatalogueEntry &drive,
  const messages::ProcessExited &msg) {

  switch(drive.getSessionType()) {
  case DriveCatalogueEntry::SESSION_TYPE_DATATRANSFER:
    return handleExitedDataTransferSession(drive, msg);
  case DriveCatalogueEntry::SESSION_TYPE_LABEL:
    return handleExitedLabelSession(drive, msg);
  case DriveCatalogueEntry::SESSION_TYPE_CLEANER:
    return handleExitedCleanerSession(drive, msg);
  default:
    {
      castor::exception::Exception ex;
      ex.getMessage() << "Failed to dispatch handler for exited process"
        ": Unexpected session type: sessionType=" << drive.getSessionType();
      throw ex;
    }
  }
}

//------------------------------------------------------------------------------
// handleExitedDataTransferSession
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::ProcessForkerConnectionHandler::
  handleExitedDataTransferSession(DriveCatalogueEntry &drive,
  const messages::ProcessExited &msg) {
  std::list<log::Param> params;
  params.push_back(log::Param("pid", msg.pid()));
  params.push_back(log::Param("exitCode", msg.exitcode()));

  try {
    if(0 == msg.exitcode()) {
      const std::string vid = drive.getTransferSession().getVid();
      drive.sessionSucceeded();
      m_log(LOG_INFO, "Data-transfer session succeeded", params);
      requestVdqmToReleaseDrive(drive.getConfig(), msg.pid());
      notifyVdqmTapeUnmounted(drive.getConfig(), vid, msg.pid());
    } else {
      drive.sessionFailed();
      m_log(LOG_WARNING, "Data-transfer session failed", params);
      setDriveDownInVdqm(msg.pid(), drive.getConfig());
    }
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to handle exited data-transfer session: " << 
    ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// handleExitedCleanerSession
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::ProcessForkerConnectionHandler::
  handleExitedCleanerSession(DriveCatalogueEntry &drive,
  const messages::ProcessExited &msg) {
  std::list<log::Param> params;
  params.push_back(log::Param("pid", msg.pid()));
  params.push_back(log::Param("exitCode", msg.exitcode()));

  try {
    if(0 == msg.exitcode()) {
      const std::string &vid = drive.getCleanerSession().getVid();
      drive.sessionSucceeded();
      m_log(LOG_INFO, "Cleaner session succeeded", params);
      requestVdqmToReleaseDrive(drive.getConfig(), msg.pid());
      notifyVdqmTapeUnmounted(drive.getConfig(), vid, msg.pid());
    } else {
      drive.sessionFailed();
      m_log(LOG_WARNING, "Cleaner session failed", params);
      setDriveDownInVdqm(msg.pid(), drive.getConfig());
    }
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to handle exited cleaner session: " << 
    ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// handleExitedLabelSession 
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::ProcessForkerConnectionHandler::
  handleExitedLabelSession(DriveCatalogueEntry &drive,
  const messages::ProcessExited &msg) {
  std::list<log::Param> params;
  params.push_back(log::Param("pid", msg.pid()));
  params.push_back(log::Param("exitCode", msg.exitcode()));

  try {
    if(0 == msg.exitcode()) {
      drive.sessionSucceeded();
      m_log(LOG_INFO, "Label session succeeded", params);
    } else {
      drive.sessionFailed();
      m_log(LOG_WARNING, "Label session failed", params);
      setDriveDownInVdqm(msg.pid(), drive.getConfig());

      castor::exception::Exception ex;
      ex.getMessage() << "Label session exited with failure code " <<
        msg.exitcode();
      throw ex;
    }
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to handle exited label session: " <<
    ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// requestVdqmToReleaseDrive
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::ProcessForkerConnectionHandler::
  requestVdqmToReleaseDrive(const utils::DriveConfig &driveConfig,
  const pid_t pid) {
  std::list<log::Param> params;
  try {
    const bool forceUnmount = true;

    params.push_back(log::Param("pid", pid));
    params.push_back(log::Param("unitName", driveConfig.unitName));
    params.push_back(log::Param("dgn", driveConfig.dgn));
    params.push_back(log::Param("forceUnmount", forceUnmount));

    m_vdqm.releaseDrive(m_hostName, driveConfig.unitName, driveConfig.dgn,
      forceUnmount, pid);
    m_log(LOG_INFO, "Requested vdqm to release drive", params);
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to request vdqm to release drive: " <<
      ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// notifyVdqmTapeUnmounted
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::ProcessForkerConnectionHandler::
  notifyVdqmTapeUnmounted(const utils::DriveConfig &driveConfig,
  const std::string &vid, const pid_t pid) {
  try {
    std::list<log::Param> params;
    params.push_back(log::Param("pid", pid));
    params.push_back(log::Param("unitName", driveConfig.unitName));
    params.push_back(log::Param("vid", vid));
    params.push_back(log::Param("dgn", driveConfig.dgn));

    m_vdqm.tapeUnmounted(m_hostName, driveConfig.unitName, driveConfig.dgn,
      vid);
    m_log(LOG_INFO, "Notified vdqm that a tape was unmounted", params);
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed notify vdqm that a tape was unmounted: " <<
      ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// setDriveDownInVdqm
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::ProcessForkerConnectionHandler::
  setDriveDownInVdqm(const pid_t pid, const utils::DriveConfig &driveConfig) {
  std::list<log::Param> params;
  params.push_back(log::Param("pid", pid));

  try {
    params.push_back(log::Param("unitName", driveConfig.unitName));
    params.push_back(log::Param("dgn", driveConfig.dgn));

    m_vdqm.setDriveDown(m_hostName, driveConfig.unitName, driveConfig.dgn);
    m_log(LOG_INFO, "Set tape-drive down in vdqm", params);
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to set tape-drive down in vdqm: " <<
      ne.getMessage().str();
    throw ex;
  }
}
