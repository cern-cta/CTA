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

#include "castor/io/io.hpp"
#include "castor/legacymsg/legacymsg.hpp"
#include "castor/tape/tapeserver/daemon/CatalogueLabelSession.hpp"
#include "h/Ctape_constants.h"
#include "h/Cupv_constants.h"

//------------------------------------------------------------------------------
// create
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::CatalogueLabelSession *
  castor::tape::tapeserver::daemon::CatalogueLabelSession::create(
    log::Logger &log,
    const int netTimeout,
    const tape::utils::DriveConfig &driveConfig,
    const legacymsg::TapeLabelRqstMsgBody &labelJob,
    const int labelCmdConnection,
    legacymsg::CupvProxy &cupv,
    ProcessForkerProxy &processForker) {

  checkUserCanLabelTape(log, cupv, labelJob, labelCmdConnection);

  const pid_t pid = processForker.forkLabel(driveConfig, labelJob);

  return new CatalogueLabelSession(
    log,
    netTimeout,
    pid,
    driveConfig,
    labelJob,
    labelCmdConnection);
}

//------------------------------------------------------------------------------
// checkUserCanLabelTape
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::CatalogueLabelSession::
  checkUserCanLabelTape(log::Logger &log, legacymsg::CupvProxy &cupv,
  const legacymsg::TapeLabelRqstMsgBody &labelJob,
  const int labelCmdConnection) {
  const std::string sourceHost = io::getPeerHostName(labelCmdConnection);
  const std::string targetHost = io::getSockHostName(labelCmdConnection);

  const bool userIsAdmin = cupv.isGranted(
    labelJob.uid,
    labelJob.gid,
    sourceHost,
    targetHost,
    P_ADMIN);
  log::Param params[] = {
    log::Param("uid", labelJob.uid),
    log::Param("gid", labelJob.gid),
    log::Param("sourceHost", sourceHost),
    log::Param("targetHost", targetHost),
    log::Param("privilegeCode", P_ADMIN),
    log::Param("privilegeStr", "ADMIN"),
    log::Param("userIsAdmin", userIsAdmin ?  "true" : "false")};
  log(LOG_INFO, "Queried cupvd for tape to be labelled", params);

  if(!userIsAdmin) {
    castor::exception::Exception ex;
    ex.getMessage() << "Only an administrator can label a tape: vid=" <<
      labelJob.vid;
    throw ex;
  }
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::CatalogueLabelSession::
  CatalogueLabelSession(
  log::Logger &log,
  const int netTimeout,
  const pid_t pid,
  const tape::utils::DriveConfig &driveConfig,
  const castor::legacymsg::TapeLabelRqstMsgBody &labelJob,
  const int labelCmdConnection) throw():
  CatalogueSession(SESSION_TYPE_LABEL, log, netTimeout, pid, driveConfig),
  m_assignmentTime(time(0)),
  m_labelJob(labelJob),
  m_labelCmdConnection(labelCmdConnection) {
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::CatalogueLabelSession::
  ~CatalogueLabelSession() throw() {
  if(m_labelCmdConnection >= 0) {
    close(m_labelCmdConnection);
  }
}

//------------------------------------------------------------------------------
// handleTick
//------------------------------------------------------------------------------
bool castor::tape::tapeserver::daemon::CatalogueLabelSession::handleTick() {
  return true; // Continue the main event loop
}

//------------------------------------------------------------------------------
// sessionSucceeded
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::CatalogueLabelSession::
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
void castor::tape::tapeserver::daemon::CatalogueLabelSession::
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
time_t castor::tape::tapeserver::daemon::CatalogueLabelSession::
  getAssignmentTime() const throw() {
  return m_assignmentTime;
}

//------------------------------------------------------------------------------
// getLabelJob
//------------------------------------------------------------------------------
castor::legacymsg::TapeLabelRqstMsgBody castor::tape::tapeserver::daemon::
  CatalogueLabelSession::getLabelJob() const throw() {
  return m_labelJob;
}

//------------------------------------------------------------------------------
// getVid
//------------------------------------------------------------------------------
std::string castor::tape::tapeserver::daemon::CatalogueLabelSession::
  getVid() const throw() {
  return m_labelJob.vid;
}

//------------------------------------------------------------------------------
// getMode
//------------------------------------------------------------------------------
int castor::tape::tapeserver::daemon::CatalogueLabelSession::getMode()
  const throw() {
  return WRITE_ENABLE;
}

//-----------------------------------------------------------------------------
// getPid
//-----------------------------------------------------------------------------
pid_t castor::tape::tapeserver::daemon::CatalogueLabelSession::
  getPid() const throw() {
  return m_pid;
}

//-----------------------------------------------------------------------------
// tapeIsBeingMounted
//-----------------------------------------------------------------------------
bool castor::tape::tapeserver::daemon::CatalogueLabelSession::
  tapeIsBeingMounted() const throw() {
  return false;
}

//-----------------------------------------------------------------------------
// receivedLabelError
//-----------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::CatalogueLabelSession::
  receivedLabelError(const castor::exception::Exception &labelEx) {
  m_labelErrors.push_back(labelEx);
}
