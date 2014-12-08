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
#include "castor/tape/tapeserver/daemon/VdqmDriveSynchronizer.hpp"
#include "h/vdqm_constants.h"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::VdqmDriveSynchronizer::VdqmDriveSynchronizer(
  log::Logger &log,
  legacymsg::VdqmProxy &vdqm,
  const std::string &hostName,
  const DriveConfig &config,
  const time_t syncIntervalSecs) throw():
  m_log(log),
  m_vdqm(vdqm),
  m_hostName(hostName),
  m_config(config),
  m_syncIntervalSecs(syncIntervalSecs) {
}

//------------------------------------------------------------------------------
// sync
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::VdqmDriveSynchronizer::sync(
  const CatalogueDriveState catalogueDriveState) {
  std::string errMsg;

  try {
    return exceptionThrowingSync(catalogueDriveState);
  } catch(castor::exception::Exception &ex) {
    errMsg = ex.getMessage().str();
  } catch(std::exception &se) {
    errMsg = se.what();
  } catch(...) {
    errMsg = "Caught an unknown exception";
  }

  // Reaching here means an exception was thrown
  // Failing to synchronise the vdqm with the drive state is not fatal
  std::list<log::Param> params;
  params.push_back(log::Param("unitName", m_config.getUnitName()));
  params.push_back(log::Param("dgn", m_config.getDgn()));
  params.push_back(log::Param("message", errMsg));
  m_log(LOG_WARNING, "Failed to synchronise vdqm with drive state", params);
}

//------------------------------------------------------------------------------
// exceptionThrowingSync
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::VdqmDriveSynchronizer::
  exceptionThrowingSync(const CatalogueDriveState catalogueDriveState) {
  // If the synchronization interval has expired
  if(m_syncIntervalSecs < m_syncTimer.secs()) {

    m_syncTimer.reset();

    // If the tape drive is UP and ready for work
    if(DRIVE_STATE_UP == catalogueDriveState) {
      syncIfDriveNotUpInVdqm();
    }
  }
}

//------------------------------------------------------------------------------
// syncIfDriveNotUpInVdqm
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::VdqmDriveSynchronizer::
  syncIfDriveNotUpInVdqm() {
  // Query the vdqmd daemon for the state of the drive
  const int vdqmDriveStatus = m_vdqm.getDriveStatus(m_hostName,
    m_config.getUnitName(), m_config.getDgn());
  const bool vdqmDriveRunning = vdqmDriveStatus ==
    (VDQM_UNIT_UP|VDQM_UNIT_BUSY|VDQM_UNIT_ASSIGN);
  const bool vdqmDriveRelease = vdqmDriveStatus ==
    (VDQM_UNIT_UP|VDQM_UNIT_BUSY|VDQM_UNIT_RELEASE|VDQM_UNIT_UNKNOWN);
  const bool vdqmDriveBadUnknown = vdqmDriveStatus ==
    (VDQM_UNIT_UP|VDQM_UNIT_BUSY|VDQM_UNIT_UNKNOWN);

  std::list<log::Param> params;
  params.push_back(log::Param("unitName", m_config.getUnitName()));
  params.push_back(log::Param("dgn", m_config.getDgn()));
  params.push_back(log::Param("vdqmDriveStatus", vdqmDriveStatus));
  params.push_back(log::Param("vdqmDriveStatusStr",
    vdqmDriveStatusToString(vdqmDriveStatus)));
  m_log(LOG_DEBUG, "Got drive status from vdqm", params);

  // If the vdqmd daemon thinks the drive is not read for new work
  //
  // We explicitely do not check (vdqmstate == UP|FREE) here, since
  // VDQM changes the state to UP|BUSY before contacting tape daemon.
  // The latter should not reset the state to UP|FREE in that case. 
  if(vdqmDriveRunning || vdqmDriveRelease || vdqmDriveBadUnknown) {
    m_log(LOG_WARNING, "Vdqm is out of sync with drive status", params);
    unconditionallySync();
  }
}

//------------------------------------------------------------------------------
// unconditionallySync
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::VdqmDriveSynchronizer::
  unconditionallySync() {
  m_vdqm.setDriveUp(m_hostName, m_config.getUnitName(), m_config.getDgn());

  const int vdqmDriveStatus = m_vdqm.getDriveStatus(m_hostName,
      m_config.getUnitName(), m_config.getDgn());

  std::list<log::Param> params;
  params.push_back(log::Param("unitName", m_config.getUnitName()));
  params.push_back(log::Param("dgn", m_config.getDgn()));
  params.push_back(log::Param("vdqmDriveStatus", vdqmDriveStatus));
  params.push_back(log::Param("vdqmDriveStatusStr",
    vdqmDriveStatusToString(vdqmDriveStatus)));

  m_log(LOG_WARNING, "Forced drive UP in vdqm", params);
}

//------------------------------------------------------------------------------
// vdqmDriveStatusToString
//------------------------------------------------------------------------------
std::string castor::tape::tapeserver::daemon::VdqmDriveSynchronizer::
  vdqmDriveStatusToString(int status) {
  std::ostringstream oss;

  appendBitIfSet(VDQM_UNIT_UP,             "UP", status, oss);
  appendBitIfSet(VDQM_UNIT_DOWN,         "DOWN", status, oss);
  appendBitIfSet(VDQM_UNIT_WAITDOWN, "WAITDOWN", status, oss);
  appendBitIfSet(VDQM_UNIT_ASSIGN  ,   "ASSIGN", status, oss);
  appendBitIfSet(VDQM_UNIT_RELEASE,   "RELEASE", status, oss);
  appendBitIfSet(VDQM_UNIT_BUSY,         "BUSY", status, oss);
  appendBitIfSet(VDQM_UNIT_FREE,         "FREE", status, oss);
  appendBitIfSet(VDQM_UNIT_UNKNOWN,   "UNKNOWN", status, oss);
  appendBitIfSet(VDQM_UNIT_ERROR,       "ERROR", status, oss);

  if(status) {
    if(!oss.str().empty()) {
      oss << "|";
    }
    oss << "leftovers=0x" << std::hex << status;
  }

  return oss.str();
}

//------------------------------------------------------------------------------
// appendBitIfSet
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::VdqmDriveSynchronizer::appendBitIfSet(
  const int bitMask, const std::string bitMaskName, int &bitSet,
  std::ostringstream &bitSetStringStream) {
  if(bitSet & bitMask) {
    if(!bitSetStringStream.str().empty()) {
      bitSetStringStream << "|";
    }
    bitSetStringStream << bitMaskName;
    bitSet -= bitMask;
  }
}
