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

#include "castor/log/LogContext.hpp"
#include "castor/log/Logger.hpp"
#include "castor/mediachanger/MediaChangerFacade.hpp"
#include "castor/tape/tapeserver/daemon/Session.hpp"
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
  class CleanerSession : public Session {
    
  public:
    /**
     * Constructor
     * 
     * @param mc           Object representing the media changer.
     * @param log          Object representing the API to the CASTOR logging system.
     * @param driveConfig  Configuration of the tape drive to be cleaned.
     * @param sysWrapper   Object representing the operating system.
     * @param vid          The volume identifier of the mounted tape if known,
     *                     else the empty string.
     */
    CleanerSession(
      mediachanger::MediaChangerFacade &mc,
      castor::log::Logger &log,
      const utils::DriveConfig &driveConfig,
      System::virtualWrapper &sysWrapper,
      const std::string &vid);
    
    /** 
     * Execute the session and return the type of action to be performed
     * immediately after the session has completed.
     *  
     * The session is responsible for mounting a tape into the tape drive,
     * working with that tape, unloading the tape from the drive and then
     * dismounting the tape from the drive and storing it back in its home slot
     * within the tape library.
     *
     * If this method throws an exception and the session is not a cleaner
     * session then it assumed that the post session action is
     * EndOfSessionAction::CLEAN_DRIVE.
     *
     * If this method throws an exception and the session is a cleaner
     * session then it assumed that the post session action is
     * EndOfSessionAction::MARK_DRIVE_AS_DOWN.
     *
     * @return Returns the type of action to be performed after the session has
     * completed.
     */
    EndOfSessionAction execute();
    
  private:

    /**
     * The object representing the media changer.
     */
    mediachanger::MediaChangerFacade &m_mc;
    
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

    /**
     * The volume identifier of the mounted tape if known, else the empty
     * string.
     */
    const std::string m_vid;
    
  }; // class CleanerSession

} // namespace daemon
} // namespace tapeserver
} // namespace tape
} // namespace castor
