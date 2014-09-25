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

#include "castor/legacymsg/legacymsg.hpp"
#include "castor/tape/tapeserver/daemon/DriveCatalogueLabelSession.hpp"
#include "h/Ctape_constants.h"

//------------------------------------------------------------------------------
// create
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::DriveCatalogueLabelSession *
  castor::tape::tapeserver::daemon::DriveCatalogueLabelSession::create(
    log::Logger &log,
    const int netTimeout,
    const tape::utils::DriveConfig &driveConfig,
    ProcessForkerProxy &processForker,
    const castor::legacymsg::TapeLabelRqstMsgBody &labelJob,
    const unsigned short rmcPort,
    const int labelCmdConnection) {

  const pid_t pid = processForker.forkLabel(driveConfig, labelJob, rmcPort);

  return new DriveCatalogueLabelSession(
    log,
    netTimeout,
    pid,
    driveConfig,
    labelJob,
    labelCmdConnection);
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::DriveCatalogueLabelSession::
  DriveCatalogueLabelSession(
  log::Logger &log,
  const int netTimeout,
  const pid_t pid,
  const tape::utils::DriveConfig &driveConfig,
  const castor::legacymsg::TapeLabelRqstMsgBody &labelJob,
  const int labelCmdConnection) throw():
  DriveCatalogueSession(log, netTimeout, pid, driveConfig),
  m_assignmentTime(time(0)),
  m_labelJob(labelJob),
  m_labelCmdConnection(labelCmdConnection) {
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::DriveCatalogueLabelSession::
  ~DriveCatalogueLabelSession() throw() {
  if(m_labelCmdConnection >= 0) {
    close(m_labelCmdConnection);
  }
}

//------------------------------------------------------------------------------
// sessionSucceeded
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::DriveCatalogueLabelSession::
  sessionSucceeded() {
  try {
    legacymsg::writeTapeReplyMsg(m_netTimeout, m_labelCmdConnection, 0, "");
  } catch(castor::exception::Exception &we) {
    log::Param params[] = {log::Param("message", we.getMessage().str())};
    m_log(LOG_ERR, "Failed to send success reply-message to label command",
      params);
  }
}

//------------------------------------------------------------------------------
// sessionFailed
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::DriveCatalogueLabelSession::
  sessionFailed() {
  try {
    if(!m_labelErrors.empty()) {
      const castor::exception::Exception &labelEx = m_labelErrors.front();
      legacymsg::writeTapeReplyMsg(m_netTimeout, m_labelCmdConnection,
        labelEx.code(), labelEx.getMessage().str());
    } else {
      legacymsg::writeTapeReplyMsg(m_netTimeout, m_labelCmdConnection,
        SEINTERNAL, "Unknown error");
    }
  } catch(castor::exception::Exception &we) {
    log::Param params[] = {log::Param("message", we.getMessage().str())};
    m_log(LOG_ERR, "Failed to send failure reply-message to label command",
      params);
  }
}

//------------------------------------------------------------------------------
// getAssignmentTime
//------------------------------------------------------------------------------
time_t castor::tape::tapeserver::daemon::DriveCatalogueLabelSession::
  getAssignmentTime() const throw() {
  return m_assignmentTime;
}

//------------------------------------------------------------------------------
// getLabelJob
//------------------------------------------------------------------------------
castor::legacymsg::TapeLabelRqstMsgBody castor::tape::tapeserver::daemon::
  DriveCatalogueLabelSession::getLabelJob() const throw() {
  return m_labelJob;
}

//------------------------------------------------------------------------------
// getVid
//------------------------------------------------------------------------------
std::string castor::tape::tapeserver::daemon::DriveCatalogueLabelSession::
  getVid() const throw() {
  return m_labelJob.vid;
}

//------------------------------------------------------------------------------
// getMode
//------------------------------------------------------------------------------
int castor::tape::tapeserver::daemon::DriveCatalogueLabelSession::getMode()
  const throw() {
  return WRITE_ENABLE;
}

//-----------------------------------------------------------------------------
// getPid
//-----------------------------------------------------------------------------
pid_t castor::tape::tapeserver::daemon::DriveCatalogueLabelSession::
  getPid() const throw() {
  return m_pid;
}

//-----------------------------------------------------------------------------
// tapeIsBeingMounted
//-----------------------------------------------------------------------------
bool castor::tape::tapeserver::daemon::DriveCatalogueLabelSession::
  tapeIsBeingMounted() const throw() {
  return false;
}

//-----------------------------------------------------------------------------
// receivedLabelError
//-----------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::DriveCatalogueLabelSession::
  receivedLabelError(const castor::exception::Exception &labelEx) {
  m_labelErrors.push_back(labelEx);
}
