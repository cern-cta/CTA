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

#include "castor/legacymsg/VdqmProxy.hpp"
#include "castor/log/Logger.hpp"
#include "castor/tape/tapeserver/daemon/CatalogueDriveState.hpp"
#include "castor/tape/tapeserver/daemon/VdqmDriveSynchronizer.hpp"
#include "castor/tape/tapeserver/daemon/DriveConfig.hpp"
#include "castor/utils/Timer.hpp"

#include <string>

namespace castor     {
namespace tape       {
namespace tapeserver {
namespace daemon     {

/**
 * Class responsible for sychronizing the vdqmd daemon with the state of a
 * tape drive within the catalogue of the tapeserverd daemon.
 */
class VdqmDriveSynchronizer {
public:
  
  /**
   * Constructor.
   *
   * @param log Object representing the API of the CASTOR logging system.
   * @param vdqm Proxy object representing the vdqmd daemon.
   * @param hostName The name of the host on which the daemon is running.  This
   * name is needed to fill in messages to be sent to the vdqmd daemon.
   * @param config The configuration of the tape drive.
   * @param syncIntervalSecs The interval in seconds between attempts to
   * synchronize the vdqmd daemon.
   */
  VdqmDriveSynchronizer(
    log::Logger &log,
    legacymsg::VdqmProxy &vdqm,
    const std::string &hostName,
    const DriveConfig &config,
    const time_t syncIntervalSecs) throw();

  /**
   * If the synchronization interval has elapsed and the vdqmd daemon is out of
   * sync with the specified drive state then this method will sychronize the
   * vdqmd daemon.
   *
   * @param catalogueDriveState The state of the tape drive within the
   * catalogue of the tapeserverd daemon.
   */
  void sync(const CatalogueDriveState catalogueDriveState);

private:

  /**
   * The object representing the API of the CASTOR logging system.
   */
  log::Logger &m_log;

  /**
   * Proxy object representing the vdqmd daemon.
   */
  legacymsg::VdqmProxy &m_vdqm;

  /**
   * The name of the host on which the daemon is running.  This name is
   * needed to fill in messages to be sent to the vdqmd daemon.
   */
  const std::string m_hostName;

  /**
   * The configuration of the tape drive.
   */
  DriveConfig m_config;

  /**
   * The interval in seconds between attempts to synchronize the vdqmd daemon.
   */
  const time_t m_syncIntervalSecs;

  /**
   * Timer used to decide when to synchronize the vdqmd daemon.
   */
  castor::utils::Timer m_syncTimer;

  /**
   * If the synchronization interval has elapsed and the vdqmd daemon is out of
   * sync with the specified drive state then this method will sychronize the
   * vdqmd daemon.
   *
   * @param catalogueDriveState The state of the tape drive within the
   * catalogue of the tapeserverd daemon.
   */
  void exceptionThrowingSync(const CatalogueDriveState catalogueDriveState);

  /**
   * If the drive is not UP and ready for work in the vdqmd daemon then this
   * method synchronizes the vdqmd daemon.
   */
  void syncIfDriveNotUpInVdqm();

  /**
   * Unconditionally synchronises the vdqmd daemon with the specified tape drive
   * state.
   */
  void unconditionallySync();

  /**
   * Returns the string representation of the specified drive status from the
   * vdqmd daemon.
   *
   * @param The drive status.
   * @return The string representation.
   */
  std::string vdqmDriveStatusToString(int status);

  /**
   * This is a helper method for vdqmDriveStatusToString().
   *
   * If the specified bit mask is set within the specified bit set then this
   * method appends the specified name of the bit mask to the end of the
   * specified string stream representation of the bit set.  This method also
   * modifies the bit set by unsetting the bit mask if it was set.
   *
   * @param bitMask The bit mask to be tested against the bit set.
   * @param bitMaskName The name of the bit mask.
   * @param bitSet In/out parameter: The bit set.
   * @param bitSetStringStream The string stream representation of the bit set.
   */
  void appendBitIfSet(const int bitMask, const std::string bitName, int &bitSet,
    std::ostringstream &bitSetStringStream);

}; // class VdqmDriveSynchronizer

} // namespace daemon
} // namespace tapeserver
} // namespace tape
} // namespace castor
