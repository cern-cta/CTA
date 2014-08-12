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

#pragma once

#include "castor/legacymsg/RtcpJobRqstMsgBody.hpp"
#include "castor/legacymsg/TapeLabelRqstMsgBody.hpp"
#include "castor/log/Logger.hpp"
#include "castor/tape/tapeserver/daemon/DataTransferSession.hpp"
#include "castor/tape/utils/DriveConfig.hpp"

namespace castor     {
namespace tape       {
namespace tapeserver {
namespace daemon     {

/**
 * Proxy class representing the process forker.
 */
class ProcessForkerProxy {
public:

  /**
   * Destructor.
   */
  virtual ~ProcessForkerProxy() throw() = 0;

  /**
   * Tells the ProcessForker to stop executing.
   *
   * @param reason Human readable string for logging purposes that describes
   * the reason for stopping.
   */
  virtual void stopProcessForker(const std::string &reason) = 0;

  /**
   * Forks a data-transfer session for the specified tape drive.
   *
   * @param driveConfig The configuration of the tape drive.
   * @param vdqmJob The job received from the vdqmd daemon.
   * @param conf The configuration of the data-transfer session.
   * @param rmcPort The TCP/IP port on which the rmcd daemon is listening.
   * @return The process identifier of the newly forked session.
   */
  virtual pid_t forkDataTransfer(const utils::DriveConfig &driveConfig,
    const legacymsg::RtcpJobRqstMsgBody vdqmJob,
    const DataTransferSession::CastorConf &conf,
    const unsigned short rmcPort) = 0;

  /** 
   * Forks a label-session process for the specified tape drive.
   *  
   * @param driveConfig The configuration of the tape drive.
   * @param labelJob The job received from the tape-labeling command-line tool.
   * @param rmcPort The TCP/IP port on which the rmcd daemon is listening.
   * @return The process identifier of the newly forked session.
   */
  virtual pid_t forkLabel(const utils::DriveConfig &driveConfig,
    const legacymsg::TapeLabelRqstMsgBody &labelJob,
    const unsigned short rmcPort) = 0;

  /**
   * Forks a cleaner session for the specified tape drive.
   *
   * @param driveConfig The configuration of the tape drive.
   * @param vid If known then this string specifies the volume identifier of the
   * tape in the drive if there is in fact a tape in the drive and its volume
   * identifier is known.  If the volume identifier is not known then this
   * parameter should be set to an empty string.
   * @param rmcPort The TCP/IP port on which the rmcd daemon is listening.
   * @return The process identifier of the newly forked session.
   */
  virtual pid_t forkCleaner(const utils::DriveConfig &driveConfig,
    const std::string &vid, const unsigned short rmcPort) = 0;

}; // class ProcessForkerProxy

} // namespace daemon
} // namespace tapeserver
} // namespace tape
} // namespace castor
