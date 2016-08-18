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
#include "tapeserver/daemon/TapedProxy.hpp"
#include "castor/server/ProcessCap.hpp"
#include "castor/tape/tapeserver/daemon/DataTransferConfig.hpp"
#include "castor/tape/tapeserver/daemon/DriveConfig.hpp"
#include "castor/tape/tapeserver/daemon/Session.hpp"
#include "castor/tape/tapeserver/daemon/TapeSingleThreadInterface.hpp"
#include "castor/tape/tapeserver/system/Wrapper.hpp"
#include "scheduler/ArchiveMount.hpp"
#include "scheduler/RetrieveMount.hpp"
#include "scheduler/Scheduler.hpp"
#include "scheduler/TapeMount.hpp"

namespace castor {
namespace legacymsg {
  class VdqmProxy;
  class VmgrProxy;
}
namespace tape {
namespace tapeserver {
namespace daemon {
  /**
   * The main class handling a tape session. This is the main container started
   * by the master process. It will drive a separate process. Only the sub
   * process interface is not included here to allow testability.
   */
  class DataTransferSession: public Session {
  public:
    /**
     * Constructor.
     *
     * @param log Object representing the API of the CASTOR logging system.
     */
    DataTransferSession(
      const std::string & hostname,
      castor::log::Logger & log,
      System::virtualWrapper & sysWrapper,
      const DriveConfig & driveConfig,
      castor::mediachanger::MediaChangerFacade & mc,
      cta::daemon::TapedProxy & initialProcess,
      castor::server::ProcessCap &capUtils,
      const DataTransferConfig & castorConf,
      cta::Scheduler &scheduler);

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

    /** Temporary method used for debugging while building the session class */
    std::string getVid() { return m_volInfo.vid; }
    
    /**
     * Destructor.
     */
    ~DataTransferSession() throw();

  private:
    
    /**
     * Object representing the API of the CASTOR logging system.
     */
    castor::log::Logger & m_log;
    VolumeInfo m_volInfo;
    System::virtualWrapper & m_sysWrapper;
    /**
     * The configuration of the tape drive to be used by this session.
     */
    const DriveConfig m_driveConfig;
    const DataTransferConfig & m_castorConf;
    /** utility to find the drive on the system. This function logs 
     * all errors and hence does not throw exceptions. It returns NULL
     * in case of failure. */
    castor::tape::tapeserver::drive::DriveInterface * findDrive(
     const DriveConfig &driveConfig,log::LogContext & lc, cta::TapeMount *mount);
        
    /** sub-part of execute for the read sessions */
    EndOfSessionAction executeRead(log::LogContext & lc, cta::RetrieveMount *retrieveMount);
    /** sub-part of execute for a write session */
    EndOfSessionAction executeWrite(log::LogContext & lc, cta::ArchiveMount *archiveMount);
    /** Reference to the MediaChangerFacade, allowing the mounting of the tape
     * by the library. It will be used exclusively by the tape thread. */
    castor::mediachanger::MediaChangerFacade & m_mc;
    /** Reference to the tape server's parent process to report detailed status */
    cta::daemon::TapedProxy & m_intialProcess;
    /** Object providing utilities for working UNIX capabilities. */
    castor::server::ProcessCap &m_capUtils;
    /** hostname, used to report status of the drive */
    const std::string m_hostname;
    /**
     * The scheduler, i.e. the local interface to the Objectstore DB
     */
    cta::Scheduler &m_scheduler;

    /**
     * Returns the string representation of the specified mount type
     */
    const char *mountTypeToString(const cta::MountType::Enum mountType) const
      throw();
  };
}
}
}
}
