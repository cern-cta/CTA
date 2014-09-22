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

#include "castor/legacymsg/RmcProxy.hpp"
#include "castor/log/LogContext.hpp"
#include "castor/log/Logger.hpp"
#include "castor/tape/tapeserver/drive/DriveInterface.hpp"
#include "castor/tape/tapeserver/file/Structures.hpp"
#include "castor/tape/tapeserver/SCSI/Device.hpp"
#include "castor/tape/utils/DriveConfig.hpp"

#include <memory>

namespace castor {
namespace tape {
namespace tapeserver {
namespace daemon {
  /**
   * Class responsible for cleaning up a tape drive left in a (possibly) dirty state.
   */
  class CleanerSession {
    
  public:
    /**
     * Constructor
     * 
     * @param rmc          Proxy object representing the rmcd daemon.
     * @param log          Object representing the API to the CASTOR logging system.
     * @param driveConfig  Configuration of the tape drive to be cleaned.
     * @param sysWrapper   Object representing the operating system.
     */
    CleanerSession(
      legacymsg::RmcProxy &rmc,
      castor::log::Logger &log,
      const utils::DriveConfig &driveConfig,
      System::virtualWrapper &sysWrapper);
    
    /**
     * Cleans the drive by unloading any tape present and unmounting it
     * 
     * @param vid The identifier of the mounted volume (one can pass the empty string 
     * in case it is unknown, as it not used except for logging purposes).
     * 
     * @return 0 in case of success (drive can stay UP) or 1 in case of failure (drive needs to be put down by the caller)
     */
    int execute(const std::string &vid);
    
  private:
    /**
     * The object representing the rmcd daemon.
     */
    legacymsg::RmcProxy &m_rmc;
    
    /**
     * The logging object     
     */
    castor::log::Logger & m_log;
    
    /**
     * The configuration of the tape drive to be cleaned.
     */
    const utils::DriveConfig m_driveConfig;
    
    /**
     * The system wrapper used to find the device and instantiate the drive object
     */
    System::virtualWrapper & m_sysWrapper;
    
  }; // class CleanerSession

} // namespace daemon
} // namespace tapeserver
} // namespace tape
} // namespace castor
