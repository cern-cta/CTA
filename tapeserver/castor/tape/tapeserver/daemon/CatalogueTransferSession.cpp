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
#include "castor/exception/Exception.hpp"
#include "castor/tape/tapeserver/daemon/CatalogueTransferSession.hpp"
#include "Ctape_constants.h"
#include "rmc_constants.h"
#include "vmgr_constants.h"

#include <sys/types.h>
#include <signal.h>

//------------------------------------------------------------------------------
// create
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::CatalogueTransferSession*
  castor::tape::tapeserver::daemon::CatalogueTransferSession::create(
    log::Logger &log,
    const int netTimeout,
    const DriveConfig &driveConfig,
    const legacymsg::RtcpJobRqstMsgBody &vdqmJob,
    legacymsg::VmgrProxy &vmgr,
    const std::string &hostName,
    const time_t waitJobTimeoutInSecs,
    const time_t mountTimeoutInSecs,
    const time_t blockMoveTimeoutInSecs,
    ProcessForkerProxy &processForker) {

  const pid_t pid = processForker.forkDataTransfer(driveConfig, vdqmJob);

  return new CatalogueTransferSession(
    log,
    netTimeout,
    pid,
    driveConfig,
    vdqmJob,
    vmgr,
    hostName,
    waitJobTimeoutInSecs,
    mountTimeoutInSecs,
    blockMoveTimeoutInSecs);
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::CatalogueTransferSession::
  CatalogueTransferSession(
  log::Logger &log,
  const int netTimeout,
  const pid_t pid,
  const DriveConfig &driveConfig,
  const legacymsg::RtcpJobRqstMsgBody &vdqmJob,
  legacymsg::VmgrProxy &vmgr,
  const std::string &hostName,
  const time_t waitJobTimeoutInSecs,
  const time_t mountTimeoutInSecs,
  const time_t blockMoveTimeoutInSecs) throw():
  CatalogueSession(SESSION_TYPE_TRANSFER, log, netTimeout, pid, driveConfig),
  m_state(WAIT_JOB),
  m_mode(WRITE_DISABLE),
  m_assignmentTime(time(0)),
  m_mountStartTime(0),
  m_lastTimeSomeBlocksWereMoved(0),
  m_vdqmJob(vdqmJob),
  m_vmgr(vmgr),
  m_hostName(hostName),
  m_waitJobTimeoutInSecs(waitJobTimeoutInSecs),
  m_mountTimeoutInSecs(mountTimeoutInSecs),
  m_blockMoveTimeoutInSecs(blockMoveTimeoutInSecs),
  m_sessionLogContext(log)
{
  // Record immediately the parameters from the vdqm request for the end of
  // session logs.
  typedef castor::log::Param Param;
  m_sessionLogContext.pushOrReplace(Param("volReqId", m_vdqmJob.volReqId));
  m_sessionLogContext.pushOrReplace(Param("dgn", m_vdqmJob.dgn));
  m_sessionLogContext.pushOrReplace(Param("driveUnit", m_vdqmJob.driveUnit));
  m_sessionLogContext.pushOrReplace(Param("clientHost", m_vdqmJob.clientHost));
  m_sessionLogContext.pushOrReplace(Param("clientPort", m_vdqmJob.clientPort));
  m_sessionLogContext.pushOrReplace(Param("clientEuid", m_vdqmJob.clientEuid));
  m_sessionLogContext.pushOrReplace(Param("clientEgid", m_vdqmJob.clientEgid));
}

//------------------------------------------------------------------------------
// handleTick
//------------------------------------------------------------------------------
bool castor::tape::tapeserver::daemon::CatalogueTransferSession::handleTick() {
  switch(m_state) {
  case WAIT_JOB         : return handleTickWhilstWaitJob();
  case WAIT_MOUNTED     : return handleTickWhilstWaitMounted();
  case RUNNING          : return handleTickWhilstRunning();
  case WAIT_TIMEOUT_KILL: return handleTickWhilstWaitTimeoutKill();
  default: return true; // Continue the main event loop
  }
}

//------------------------------------------------------------------------------
// handleTickWhilstWaitJob
//------------------------------------------------------------------------------
bool castor::tape::tapeserver::daemon::CatalogueTransferSession::
  handleTickWhilstWaitJob() {
  const time_t now = time(0);
  const time_t secsWaiting = now - m_assignmentTime;
  const bool timeOutExceeded = secsWaiting > m_waitJobTimeoutInSecs;

  if(timeOutExceeded) {
    std::list<log::Param> params;
    params.push_back(log::Param("transferSessionPid", m_pid));
    params.push_back(log::Param("secsWaiting", secsWaiting));
    params.push_back(log::Param("waitJobTimeoutInSecs",
      m_waitJobTimeoutInSecs));
    m_log(LOG_ERR,
      "Killing data-transfer session because transfer job is too late",
      params);
    // We will also attach the error to final log for the session
    addLogParam(castor::log::Param("Error_timeoutGettingJobInfo","1"));

    try {
      idempotentKill(m_pid, SIGKILL);
      m_state = WAIT_TIMEOUT_KILL;
    } catch(castor::exception::Exception &ex) {
      params.push_back(log::Param("message", ex.getMessage()));
      m_log(LOG_ERR, "Failed to kill data-transfer session", params);
    }
  }

  return true; // Continue the main event loop
}

//------------------------------------------------------------------------------
// idempotentKill
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::CatalogueTransferSession::idempotentKill(
  const pid_t pid, const int signal) {
  // Try to kill the process
  const int killRc = kill(m_pid, signal);

  // If the kill failed for a reason other than the fact the process was already
  // dead
  if(killRc && ESRCH != errno) {
    const std::string errnoStr = castor::utils::errnoToString(errno);
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to kill process"
      ": pid=" << pid << " signal=" << signal << ": " << errnoStr;
    throw ex;
  }
}

//------------------------------------------------------------------------------
// handleTickWhilstWaitMounted
//------------------------------------------------------------------------------
bool castor::tape::tapeserver::daemon::CatalogueTransferSession::
  handleTickWhilstWaitMounted() {
  const time_t now = time(0);
  const time_t secsWaiting = now - m_mountStartTime;
  const bool timeOutExceeded = secsWaiting > m_mountTimeoutInSecs;

  if(timeOutExceeded) {
    std::list<log::Param> params;
    params.push_back(log::Param("transferSessionPid", m_pid));
    params.push_back(log::Param("secsWaiting", secsWaiting));
    params.push_back(log::Param("mountTimeoutInSecs",
      m_mountTimeoutInSecs));
    m_log(LOG_ERR,
      "Killing data-transfer session because tape mount is taking too long",
      params);
    // We will also attach the error to final log for the session
    addLogParam(castor::log::Param("Error_timeoutMountingTape","1"));
    
    try {
      idempotentKill(m_pid, SIGKILL);
      m_state = WAIT_TIMEOUT_KILL;
    } catch(castor::exception::Exception &ex) {
      params.push_back(log::Param("message", ex.getMessage()));
      m_log(LOG_ERR, "Failed to kill data-transfer session", params);
    }
  }

  return true; // Continue the main event loop
}

//------------------------------------------------------------------------------
// handleTickWhilstRunning
//------------------------------------------------------------------------------
bool castor::tape::tapeserver::daemon::CatalogueTransferSession::
  handleTickWhilstRunning() {
  const time_t now = time(0);
  const time_t secsWaiting = now - m_lastTimeSomeBlocksWereMoved;
  const bool timeOutExceeded = secsWaiting > m_blockMoveTimeoutInSecs;

  if(timeOutExceeded) {
    std::list<log::Param> params;
    params.push_back(log::Param("transferSessionPid", m_pid));
    params.push_back(log::Param("secsWaiting", secsWaiting));
    params.push_back(log::Param("blockMoveTimeoutInSecs",
      m_blockMoveTimeoutInSecs));
    m_log(LOG_ERR,
      "Killing data-transfer session because data blocks are not being moved",
      params);
    // We will also attach the error to final log for the session
    addLogParam(castor::log::Param("Error_sessionHung","1"));

    try {
      idempotentKill(m_pid, SIGKILL);
      m_state = WAIT_TIMEOUT_KILL;
    } catch(castor::exception::Exception &ex) {
      params.push_back(log::Param("message", ex.getMessage()));
      m_log(LOG_ERR, "Failed to kill data-transfer session", params);
    }
  }

  return true; // Continue the main event loop
}

//------------------------------------------------------------------------------
// handleTickWhilstWaitTimeoutKill
//------------------------------------------------------------------------------
bool castor::tape::tapeserver::daemon::CatalogueTransferSession::
  handleTickWhilstWaitTimeoutKill() {
  return true; // Continue the main event loop
}

//------------------------------------------------------------------------------
// sessionSucceeded
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::CatalogueTransferSession::
  sessionSucceeded() {
  // If the session is happy, it could determine the operational status
  // (was there a problem or not?) by itself. status should be set by now.
  m_sessionLogContext.log(LOG_INFO, "Tape session finished");
}

//------------------------------------------------------------------------------
// sessionFailed
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::CatalogueTransferSession::
  sessionFailed() {
  // In case of problem, we mark the session failed ourselves
  m_sessionLogContext.pushOrReplace(log::Param("status","failure"));
  m_sessionLogContext.log(LOG_INFO, "Tape session finished");
}

//------------------------------------------------------------------------------
// sessionFailed
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::CatalogueTransferSession::
  sessionKilled(uint32_t signal) {
  // In case of problem, we mark the session failed ourselves
  m_sessionLogContext.pushOrReplace(log::Param("Error_sessionKilled","1"));
  m_sessionLogContext.pushOrReplace(log::Param("killSignal",signal));
  m_sessionLogContext.pushOrReplace(log::Param("status","failure"));
  m_sessionLogContext.log(LOG_INFO, "Tape session finished");
}

//------------------------------------------------------------------------------
// getAssignmentTime
//------------------------------------------------------------------------------
time_t castor::tape::tapeserver::daemon::CatalogueTransferSession::
  getAssignmentTime() const throw() {
  return m_assignmentTime;
}

//------------------------------------------------------------------------------
// getVdqmJob
//------------------------------------------------------------------------------
castor::legacymsg::RtcpJobRqstMsgBody castor::tape::tapeserver::daemon::
  CatalogueTransferSession::getVdqmJob() const{
  return m_vdqmJob;
}

//-----------------------------------------------------------------------------
// receivedRecallJob
//-----------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::CatalogueTransferSession::
  receivedRecallJob(const std::string &vid) {
  const char *const task = "accept reception of recall job";

  if(WAIT_JOB != m_state) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to " << task <<
      ": Catalogue transfer-session state-mismatch: "
      "expected=" << transferStateToStr(WAIT_JOB) <<
      " actual=" << transferStateToStr(m_state);
    throw ex;
  }

  // Store the VID before checking whether or not the user can actually recall
  // files from the tape.  This is needed when the user is not permitted, the
  // the corresponding exception is thrown and the exception logging logic
  // comes back to the CatalogueTransferSession to ask for the
  // the VID tape.
  m_vid = vid;

  m_mountStartTime = time(0);
  m_state = WAIT_MOUNTED;

  m_mode = WRITE_DISABLE;
}

//-----------------------------------------------------------------------------
// receivedMigrationJob
//-----------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::CatalogueTransferSession::
  receivedMigrationJob(const std::string &vid) {
  if(WAIT_JOB != m_state) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to accept reception of recall job"
      ": Catalogue transfer-session state-mismatch: "
      "expected=" << transferStateToStr(WAIT_JOB) <<
      " actual=" << transferStateToStr(m_state);
    throw ex;
  }

  // Store the VID before checking whether or not the user can actually migrate
  // files to the tape.  This is needed when the user is not permitted, the
  // the corresponding exception is thrown and the exception logging logic
  // comes back to the CatalogueTransferSession to ask for the
  // the VID tape.
  m_vid = vid;

  m_mountStartTime = time(0);
  m_state = WAIT_MOUNTED;

  m_mode = WRITE_ENABLE;
}

//------------------------------------------------------------------------------
// getVid
//------------------------------------------------------------------------------
std::string castor::tape::tapeserver::daemon::CatalogueTransferSession::
  getVid() const {
  return m_vid;
}

//------------------------------------------------------------------------------
// getMode
//------------------------------------------------------------------------------
int castor::tape::tapeserver::daemon::CatalogueTransferSession::
  getMode() const {
  switch(m_state) {
  case WAIT_MOUNTED:
  case RUNNING:
    return m_mode;
  default:
    {
      castor::exception::Exception ex;
      ex.getMessage() << "Failed to get access mode from catalogue"
        " transfer-session"
        ": Catalogue transfer-session is in an incompatible state: "
        " state=" << transferStateToStr(m_state);
      throw ex;
    }
  }
}

//-----------------------------------------------------------------------------
// getPid
//-----------------------------------------------------------------------------
pid_t castor::tape::tapeserver::daemon::CatalogueTransferSession::
  getPid() const throw() {
  return m_pid;
}

//-----------------------------------------------------------------------------
// tapeMountedForMigration
//-----------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::CatalogueTransferSession::
  tapeMountedForMigration(const std::string &vid) {
  if(WAIT_MOUNTED != m_state) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to accept tape mounted for migration"
      ": Catalogue transfer-session state-mismatch: "
      "expected=" << transferStateToStr(WAIT_MOUNTED) <<
      " actual=" << transferStateToStr(m_state);
    throw ex;
  }

  // If the volume identifier of the data transfer job does not match the
  // mounted tape
  if(m_vid != vid) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to accept tape mounted for migration"
      ": VID mismatch: expected=" << m_vid << " actual=" << vid;
    throw ex;
  }

  // If the mount is not for migration
  if(WRITE_ENABLE != m_mode) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to accept tape mounted for migration"
      ": Data transfer job is not for migration";
    throw ex;
  }

  m_lastTimeSomeBlocksWereMoved = time(0); // Start watchdog timer
  m_state = RUNNING;
}

//-----------------------------------------------------------------------------
// tapeMountedForRecall
//-----------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::CatalogueTransferSession::
  tapeMountedForRecall(const std::string &vid) {

  if(WAIT_MOUNTED != m_state) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to accept tape mounted for recall"
      ": Catalogue transfer-session state-mismatch: "
      "expected=" << transferStateToStr(WAIT_MOUNTED) <<
      " actual=" << transferStateToStr(m_state);
    throw ex;
  }

  // If the volume identifier of the data transfer job does not match the
  // mounted tape
  if(m_vid != vid) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to accept tape mounted for recall"
      ": VID mismatch: expected=" << m_vid << " actual=" << vid;
    throw ex;
  }

  // If the mount is not for recall
  if(WRITE_DISABLE != m_mode) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to accept tape mounted for recall"
      ": Data transfer job is not for recall";
    throw ex;
  }

  m_lastTimeSomeBlocksWereMoved = time(0); // Start watchdog timer
  m_state = RUNNING;
}

//-----------------------------------------------------------------------------
// transferStateToStr
//-----------------------------------------------------------------------------
const char *castor::tape::tapeserver::daemon::CatalogueTransferSession::
  transferStateToStr(const TransferState state) const throw() {
  switch(state) {
  case WAIT_JOB         : return "WAIT_JOB";
  case WAIT_MOUNTED     : return "WAIT_MOUNTED";
  case RUNNING          : return "RUNNING";
  case WAIT_TIMEOUT_KILL: return "WAIT_TIMEOUT_KILL";
  default               : return "UNKNOWN";
  }
}

//-----------------------------------------------------------------------------
// tapeIsBeingMounted
//-----------------------------------------------------------------------------
bool castor::tape::tapeserver::daemon::CatalogueTransferSession::
  tapeIsBeingMounted() const throw() {
  return WAIT_MOUNTED == m_state;
}

//-----------------------------------------------------------------------------
// receivedHeartbeat
//-----------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::CatalogueTransferSession::
  receivedHeartbeat(const uint64_t nbBlocksMoved) {
  if(nbBlocksMoved > 0) {
    m_lastTimeSomeBlocksWereMoved = time(0);
  }
}

//-----------------------------------------------------------------------------
// addLogParam
//-----------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::CatalogueTransferSession::
  addLogParam(const log::Param & param) {
  m_sessionLogContext.pushOrReplace(param);
}

//-----------------------------------------------------------------------------
// deleteLogParam
//-----------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::CatalogueTransferSession::
  deleteLogParam(const std::string & paramName) {
  m_sessionLogContext.erase(paramName);
}
