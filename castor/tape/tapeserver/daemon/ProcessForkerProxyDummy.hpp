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

#include "castor/tape/tapeserver/daemon/ProcessForkerProxy.hpp"

namespace castor     {
namespace tape       {
namespace tapeserver {
namespace daemon     {

/**
 * A concrete dummy proxy class representing the process forker.
 *
 * The methods of this class intentionally do nothing.  The primary goal of
 * this class is to facilitate unit testing.
 */
class ProcessForkerProxyDummy: public ProcessForkerProxy {
public:

  /**
   * Tells the ProcessForker to stop executing.
   *
   * PLEASE NOTE that this method is a dummy method and intentionally does
   * nothing.
   *
   * @param reason Human readable string for logging purposes that describes
   * the reason for stopping.
   */
  void stopProcessForker(const std::string &reason);

  /**
   * Forks a data-transfer session for the specified tape drive.
   *
   * PLEASE NOTE that this method is a dummy method and intentionally does
   * nothing.
   *
   * @param driveConfig The configuration of the tape drive.
   * @param vdqmJob The job received from the vdqmd daemon.
   * @return The process identifier of the newly forked session which will
   * always be 0 because this is a dummy method.
   */ 
  pid_t forkDataTransfer(const utils::DriveConfig &driveConfig,
    const legacymsg::RtcpJobRqstMsgBody vdqmJob);

  /** 
   * Forks a label-session process for the specified tape drive.
   *
   * PLEASE NOTE that this method is a dummy method and intentionally does
   * nothing.
   *  
   * @param driveConfig The configuration of the tape drive.
   * @param labelJob The job received from the tape-labeling command-line tool.
   * @return The process identifier of the newly forked session which will
   * always be 0 because this is a dummy method.

   */
  pid_t forkLabel(const utils::DriveConfig &driveConfig,
    const legacymsg::TapeLabelRqstMsgBody &labelJob);

  /**
   * Forks a cleaner session for the specified tape drive.
   *
   * @param driveConfig The configuration of the tape drive.
   * @param vid If known then this string specifies the volume identifier of the
   * tape in the drive if there is in fact a tape in the drive and its volume
   * identifier is known.  If the volume identifier is not known then this
   * parameter should be set to an empty string.
   * @param driveReadyDelayInSeconds The maximum number of seconds to wait for
   * the drive to be raedy with a tape inside of it.
   * @return The process identifier of the newly forked session.
   */
  pid_t forkCleaner(const utils::DriveConfig &driveConfig,
    const std::string &vid, const uint32_t driveReadyDelayInSeconds);

}; // class ProcessForkerProxySocket

} // namespace daemon
} // namespace tapeserver
} // namespace tape
} // namespace castor
