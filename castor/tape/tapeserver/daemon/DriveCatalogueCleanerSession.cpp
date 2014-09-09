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

#include "castor/exception/Exception.hpp"
#include "castor/tape/tapeserver/daemon/DriveCatalogueCleanerSession.hpp"
#include "h/Ctape_constants.h"

//------------------------------------------------------------------------------
// create
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::DriveCatalogueCleanerSession *
  castor::tape::tapeserver::daemon::DriveCatalogueCleanerSession::create(
  log::Logger &log,
  ProcessForkerProxy &processForker,
  const tape::utils::DriveConfig &driveConfig,
  const std::string &vid,
  const unsigned short rmcPort,
  const time_t assignmentTime) {

  const pid_t pid = forkCleanerSession(processForker, driveConfig, vid,
    rmcPort);

  return new DriveCatalogueCleanerSession(
    pid,
    log,
    driveConfig,
    vid,
    assignmentTime);
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::DriveCatalogueCleanerSession::
  DriveCatalogueCleanerSession(
  const pid_t pid,
  log::Logger &log,
  const tape::utils::DriveConfig &driveConfig,
  const std::string &vid,
  const time_t assignmentTime) throw():
  m_pid(pid),
  m_log(log),
  m_vid(vid),
  m_assignmentTime(assignmentTime) {
}

//------------------------------------------------------------------------------
// forkCleanerSession
//------------------------------------------------------------------------------
pid_t castor::tape::tapeserver::daemon::DriveCatalogueCleanerSession::
  forkCleanerSession(ProcessForkerProxy &processForker,
  const tape::utils::DriveConfig &driveConfig,
  const std::string &vid,
  const unsigned short rmcPort) {

  // TO BE DONE
  // processForker->forkCleaner(driveConfig, vid, rmcPort);
  return 0;
}

//------------------------------------------------------------------------------
// sessionSucceeded
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::DriveCatalogueCleanerSession::
  sessionSucceeded() {
}

//------------------------------------------------------------------------------
// sessionFailed
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::DriveCatalogueCleanerSession::
  sessionFailed() {
}

//------------------------------------------------------------------------------
// getVid
//------------------------------------------------------------------------------
std::string castor::tape::tapeserver::daemon::DriveCatalogueCleanerSession::
  getVid() const {
  // If the volume identifier of the tape drive is not known
  if(m_vid.empty()) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to get VID from cleaner session"
      ": VID not known";
    throw ex;
  }

  return m_vid;
}

//------------------------------------------------------------------------------
// getMode
//------------------------------------------------------------------------------
int castor::tape::tapeserver::daemon::DriveCatalogueCleanerSession::getMode()
  const throw() {
  return WRITE_DISABLE;
}

//-----------------------------------------------------------------------------
// getPid
//-----------------------------------------------------------------------------
pid_t castor::tape::tapeserver::daemon::DriveCatalogueCleanerSession::
  getPid() const throw() {
  return m_pid;
}

//------------------------------------------------------------------------------
// getAssignmentTime
//------------------------------------------------------------------------------
time_t castor::tape::tapeserver::daemon::DriveCatalogueCleanerSession::
  getAssignmentTime() const throw() {
  return m_assignmentTime;
}

//-----------------------------------------------------------------------------
// tapeIsBeingMounted
//-----------------------------------------------------------------------------
bool castor::tape::tapeserver::daemon::DriveCatalogueCleanerSession::
  tapeIsBeingMounted() const throw() {
  return false;
}
