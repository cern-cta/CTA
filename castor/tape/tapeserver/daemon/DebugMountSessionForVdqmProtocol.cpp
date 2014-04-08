/******************************************************************************
 *         castor/tape/tapeserver/daemon/DebugMountSessionForVdqmProtocol.cpp
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
 * @author Steven.Murray@cern.ch
 *****************************************************************************/

#include "castor/tape/tapeserver/daemon/DebugMountSessionForVdqmProtocol.hpp"

#include <unistd.h>

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::DebugMountSessionForVdqmProtocol::DebugMountSessionForVdqmProtocol(
  const std::string &hostName,
  const legacymsg::RtcpJobRqstMsgBody &job,
  castor::log::Logger &logger,
  const utils::TpconfigLines &tpConfig,
  Vdqm &vdqm,
  Vmgr &vmgr) throw():
  m_sessionPid(getpid()),
  m_hostName(hostName),
  m_job(job),
  m_log(logger),
  m_tpConfig(tpConfig),
  m_vdqm(vdqm),
  m_vmgr(vmgr) {
}

//------------------------------------------------------------------------------
// execute
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::DebugMountSessionForVdqmProtocol::execute() throw (castor::exception::Exception) {
  const std::string vid = "V12345";
  m_vdqm.assignDrive(m_hostName,  m_job.driveUnit, m_job.dgn, m_job.volReqId, m_sessionPid);
  {
    log::Param params[] = {
      log::Param("unitName", m_job.driveUnit),
      log::Param("TPVID", vid)};
    m_log(LOG_INFO, "Mounting tape", params);
  }
  ::sleep(1);
  {
    log::Param params[] = {
      log::Param("unitName", m_job.driveUnit),
      log::Param("TPVID", vid)};
    m_log(LOG_INFO, "Tape mounted", params);
  }
  m_vdqm.tapeMounted(m_hostName, m_job.driveUnit, m_job.dgn, vid, m_sessionPid);
  {
    log::Param params[] = {
      log::Param("unitName", m_job.driveUnit),
      log::Param("TPVID", vid)};
    m_log(LOG_INFO, "Pretending to transfer files to/from tape", params);
  }
  ::sleep(1);
  const bool forceUnmount = true;
  {
    log::Param params[] = {
      log::Param("unitName", m_job.driveUnit),
      log::Param("TPVID", vid),
      log::Param("forceUnmount", forceUnmount)};
    m_log(LOG_INFO, "Releasing tape drive", params);
  }
  m_vdqm.releaseDrive(m_hostName, m_job.driveUnit, m_job.dgn, forceUnmount, m_sessionPid);
}
