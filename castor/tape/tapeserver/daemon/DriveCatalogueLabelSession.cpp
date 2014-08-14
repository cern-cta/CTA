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

#include "castor/common/CastorConfiguration.hpp"
#include "castor/tape/tapeserver/daemon/DriveCatalogueLabelSession.hpp"
#include "h/rmc_constants.h"
#include "h/Ctape_constants.h"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::DriveCatalogueLabelSession::
  DriveCatalogueLabelSession(
  log::Logger &log,
  ProcessForkerProxy &processForker,
  const tape::utils::DriveConfig &driveConfig,
  const castor::legacymsg::TapeLabelRqstMsgBody labelJob,
  const int labelCmdConnection):
  m_assignmentTime(time(0)),
  m_log(log),
  m_processForker(processForker),
  m_driveConfig(driveConfig),
  m_labelJob(labelJob),
  m_labelCmdConnection(labelCmdConnection) {
}

//------------------------------------------------------------------------------
// getAssignmentTime
//------------------------------------------------------------------------------
time_t castor::tape::tapeserver::daemon::DriveCatalogueLabelSession::
  getAssignmentTime() const throw() {
  return m_assignmentTime;
}

//------------------------------------------------------------------------------
// forkLabelSession
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::DriveCatalogueLabelSession::
  forkLabelSession() {
  try {
    const unsigned short rmcPort =
      common::CastorConfiguration::getConfig().getConfEntInt(
        "RMC", "PORT", (unsigned short)RMC_PORT, &m_log);
    m_pid = m_processForker.forkLabel(m_driveConfig, m_labelJob, rmcPort);
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to fork label session: unitName=" <<
      m_driveConfig.unitName << ": " << ne.getMessage().str();
    throw ex;
  }
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
// tapeIsBeingMounted
//-----------------------------------------------------------------------------
bool castor::tape::tapeserver::daemon::DriveCatalogueLabelSession::
  tapeIsBeingMounted() const throw() {
  return false;
}
    
//------------------------------------------------------------------------------
// getLabelCmdConnection
//------------------------------------------------------------------------------
int castor::tape::tapeserver::daemon::DriveCatalogueLabelSession::
  getLabelCmdConnection() const {
  return m_labelCmdConnection;
}
