/******************************************************************************
 *         castor/tape/tapeserver/daemon/DebugMountSessionForVdqmProtocol.hpp
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

#pragma once

#include "castor/exception/Exception.hpp"
#include "castor/log/Logger.hpp"
#include "castor/tape/legacymsg/RtcpJobRqstMsgBody.hpp"
#include "castor/tape/tapeserver/daemon/Vdqm.hpp"
#include "castor/tape/tapeserver/daemon/Vmgr.hpp"
#include "castor/tape/utils/utils.hpp"

#include <sys/types.h>

namespace castor     {
namespace tape       {
namespace tapeserver {
namespace daemon     {

/**
 * A temporary class implementing a mount session in order to test the vdqm
 * protocol.
 */
class DebugMountSessionForVdqmProtocol {

public:

  /**
   * Constructor.
   *
   * @param hostName The name of the host on which the tapeserverd daemon is running.
   * @param job The job received from the vdwqmd daemon.
   * @param logger The interface to the CASTOR logging system.
   * @param tpConfig The contents of /etc/castor/TPCONFIG.
   * @param vdqm The object representing the vdqmd daemon.
   * @param vmgr The object representing the vmgrd daemon.
   */
  DebugMountSessionForVdqmProtocol(
    const std::string &hostName,
    const legacymsg::RtcpJobRqstMsgBody &job,
    castor::log::Logger &logger,
    const utils::TpconfigLines &tpConfig,
    Vdqm &vdqm,
    Vmgr &vmgr) throw();

  /**
   * The only method. It will execute (like a task, that it is)
   */
  void execute() throw (castor::exception::Exception);

private:

  /**
   * The ID of the mount-session process.
   */
  const pid_t m_sessionPid;

  /**
   * The name of the host on which the tapeserverd daemon is running.
   */
  const std::string m_hostName;

  /**
   * The job received from the vdwqmd daemon.
   */
  const legacymsg::RtcpJobRqstMsgBody m_job;

  /**
   *
   */
  castor::log::Logger &m_log;

  /**
   * The contents of /etc/castor/TPCONFIG.
   */
  const utils::TpconfigLines &m_tpConfig;

  /**
   * The object representing the vdqmd daemon.
   */
  Vdqm &m_vdqm;

  /**
   * The object representing the vmgrd daemon.
   */
  Vmgr &m_vmgr;

}; // class DebugMountSessionForVdqmProtocol

} // namespace daemon
} // namespace tapeserver
} // namespace tape
} // namespace castor

