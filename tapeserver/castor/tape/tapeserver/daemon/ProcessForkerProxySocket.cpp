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
 *
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#include "castor/messages/ForkCleaner.pb.h"
#include "castor/messages/ForkSucceeded.pb.h"
#include "castor/messages/StopProcessForker.pb.h"
#include "castor/tape/tapeserver/daemon/ProcessForkerProxySocket.hpp"
#include "castor/tape/tapeserver/daemon/ProcessForkerUtils.hpp"
#include "common/utils/utils.hpp"

#include <errno.h>
#include <unistd.h>

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::ProcessForkerProxySocket::
  ProcessForkerProxySocket(cta::log::Logger &log, const int socketFd) throw():
  m_log(log), m_socketFd(socketFd) {
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::ProcessForkerProxySocket::
  ~ProcessForkerProxySocket() throw() {
  if(-1 == close(m_socketFd)) {
    const std::string message = cta::utils::errnoToString(errno);
    std::list<cta::log::Param> params = {cta::log::Param("socketFd", m_socketFd), 
      cta::log::Param("message", message)};
    m_log(cta::log::ERR,
      "Failed to close proxy side of ProcessForker socket pair", params);
  } else {
    m_log(cta::log::INFO, "Closed proxy side of ProcessForker socket pair");
  }
}

//------------------------------------------------------------------------------
// stopProcessForker
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::ProcessForkerProxySocket::
  stopProcessForker(const std::string &reason) {

  // Request the process forker to stop gracefully
  const messages::StopProcessForker rqst = createStopProcessForkerMsg(reason);
  ProcessForkerUtils::writeFrame(m_socketFd, rqst);

  // Read back the reply
  const int timeout = 10; // Timeout in seconds
  messages::ReturnValue reply;
  ProcessForkerUtils::readReplyOrEx(m_socketFd, timeout, reply);
  if(0 != reply.value()) {
    // Should never get here
    cta::exception::Exception ex;
    ex.getMessage() << "Failed to request ProcessForker to stop gracefully: "
      "Received a non-zero return value: value=" << reply.value();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// createStopProcessForkerMsg
//------------------------------------------------------------------------------
castor::messages::StopProcessForker castor::tape::tapeserver::daemon::
  ProcessForkerProxySocket::createStopProcessForkerMsg(
  const std::string &reason) {
  messages::StopProcessForker msg;

  msg.set_reason(reason);

  return msg;
}

//------------------------------------------------------------------------------
// forkDataTransfer
//------------------------------------------------------------------------------
pid_t castor::tape::tapeserver::daemon::ProcessForkerProxySocket::
  forkDataTransfer(const DriveConfig &driveConfig) {

  // Request the process forker to fork a data-transfer session
  const messages::ForkDataTransfer rqst =
    createForkDataTransferMsg(driveConfig);
  ProcessForkerUtils::writeFrame(m_socketFd, rqst);

  // Read back the reply
  const int timeout = 10; // Timeout in seconds
  messages::ForkSucceeded reply;
  ProcessForkerUtils::readReplyOrEx(m_socketFd, timeout, reply);
  std::list<cta::log::Param> params = {cta::log::Param("pid", reply.pid())};
  m_log(cta::log::INFO,
    "Got process ID of the data-transfer session from the ProcessForker",
    params);

  return reply.pid();
}

//------------------------------------------------------------------------------
// createForkDataTransferMsg
//------------------------------------------------------------------------------
castor::messages::ForkDataTransfer
  castor::tape::tapeserver::daemon::ProcessForkerProxySocket::
  createForkDataTransferMsg(const DriveConfig &driveConfig) {
  messages::ForkDataTransfer msg;

  // Description of the tape drive
  fillMsgWithDriveConfig(msg, driveConfig);

  return msg;
}

//------------------------------------------------------------------------------
// forkLabel
//------------------------------------------------------------------------------
pid_t castor::tape::tapeserver::daemon::ProcessForkerProxySocket::
  forkLabel(const DriveConfig &driveConfig,
  const legacymsg::TapeLabelRqstMsgBody &labelJob) {

  // Request the process forker to fork a label session
  const messages::ForkLabel rqst = createForkLabelMsg(driveConfig, labelJob);
  ProcessForkerUtils::writeFrame(m_socketFd, rqst);

  // Read back the reply
  const int timeout = 10; // Timeout in seconds
  messages::ForkSucceeded reply;
  ProcessForkerUtils::readReplyOrEx(m_socketFd, timeout, reply);
  std::list<cta::log::Param> params = {cta::log::Param("pid", reply.pid())};
  m_log(cta::log::INFO, "Got process ID of the label session from the ProcessForker",
    params);

  return reply.pid();
}

//------------------------------------------------------------------------------
// createForkLabelMsg
//------------------------------------------------------------------------------
castor::messages::ForkLabel castor::tape::tapeserver::daemon::
  ProcessForkerProxySocket::createForkLabelMsg(
  const DriveConfig &driveConfig,
  const legacymsg::TapeLabelRqstMsgBody &labelJob) {
  messages::ForkLabel msg;

  // Description of the tape drive
  fillMsgWithDriveConfig(msg, driveConfig);

  // Description of the label job
  fillMsgWithLabelJob(msg, labelJob);

  return msg;
}

//------------------------------------------------------------------------------
// forkCleaner
//------------------------------------------------------------------------------
pid_t castor::tape::tapeserver::daemon::ProcessForkerProxySocket::
  forkCleaner(const DriveConfig &driveConfig, const std::string &vid,
  const bool waitMediaInDrive,
  const uint32_t waitMediaInDriveTimeout) {

  // Request the process forker to fork a label session
  const messages::ForkCleaner rqst = createForkCleanerMsg(driveConfig, vid, waitMediaInDrive,
    waitMediaInDriveTimeout);
  ProcessForkerUtils::writeFrame(m_socketFd, rqst);

  // Read back the reply
  const int timeout = 10; // Timeout in seconds
  messages::ForkSucceeded reply;
  ProcessForkerUtils::readReplyOrEx(m_socketFd, timeout, reply);
  std::list<cta::log::Param> params = {cta::log::Param("pid", reply.pid())};
  m_log(cta::log::INFO,
    "Got process ID of the cleaner session from the ProcessForker", params);

  return reply.pid();
}

//------------------------------------------------------------------------------
// createForkCleanerMsg
//------------------------------------------------------------------------------
castor::messages::ForkCleaner castor::tape::tapeserver::daemon::
  ProcessForkerProxySocket::createForkCleanerMsg(
  const DriveConfig &driveConfig, const std::string &vid,
  const bool waitMediaInDrive,
  const uint32_t waitMediaInDriveTimeout) {
  messages::ForkCleaner msg;

  // Description of the tape drive
  fillMsgWithDriveConfig(msg, driveConfig);

  // Description of the tape
  msg.set_vid(vid);

  // Description of the cleaner job
  msg.set_waitmediaindrive(waitMediaInDrive);
  msg.set_waitmediaindrivetimeout(waitMediaInDriveTimeout);

  return msg;
}
