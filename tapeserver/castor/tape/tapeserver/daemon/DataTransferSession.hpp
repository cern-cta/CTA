/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2003-2021 CERN
 * @license        This program is free software: you can redistribute it and/or modify
 *                 it under the terms of the GNU General Public License as published by
 *                 the Free Software Foundation, either version 3 of the License, or
 *                 (at your option) any later version.
 *
 *                 This program is distributed in the hope that it will be useful,
 *                 but WITHOUT ANY WARRANTY; without even the implied warranty of
 *                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *                 GNU General Public License for more details.
 *
 *                 You should have received a copy of the GNU General Public License
 *                 along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include "common/log/LogContext.hpp"
#include "common/log/Logger.hpp"
#include "common/processCap/ProcessCap.hpp"
#include "mediachanger/MediaChangerFacade.hpp"
#include "tapeserver/daemon/TapedProxy.hpp"
#include "DataTransferConfig.hpp"
#include "tapeserver/daemon/TpconfigLine.hpp"
#include "Session.hpp"
#include "TapeSingleThreadInterface.hpp"
#include "tapeserver/castor/tape/tapeserver/system/Wrapper.hpp"
#include "scheduler/ArchiveMount.hpp"
#include "scheduler/RetrieveMount.hpp"
#include "scheduler/LabelMount.hpp"
#include "scheduler/Scheduler.hpp"
#include "scheduler/TapeMount.hpp"

namespace castor {
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
      cta::log::Logger & log,
      System::virtualWrapper & sysWrapper,
      const cta::tape::daemon::TpconfigLine & driveConfig,
      cta::mediachanger::MediaChangerFacade & mc,
      cta::tape::daemon::TapedProxy & initialProcess,
      cta::server::ProcessCap &capUtils,
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

    /**
     * Sets the capabilities of the process and logs the result.
     *
     * @param capabilities The string representation of the capabilities.
     */
    void setProcessCapabilities(const std::string &capabilities);

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
    cta::log::Logger & m_log;
    VolumeInfo m_volInfo;
    System::virtualWrapper & m_sysWrapper;
    /**
     * The configuration of the tape drive to be used by this session.
     */
    const cta::tape::daemon::TpconfigLine m_driveConfig;
    const DataTransferConfig & m_castorConf;
    /**
     * The drive information bundle allowing drive register update.
     * Filled up at construction time.
     */
    const cta::common::dataStructures::DriveInfo m_driveInfo;
    /** utility to find the drive on the system. This function logs 
     * all errors and hence does not throw exceptions. It returns NULL
     * in case of failure. */
    castor::tape::tapeserver::drive::DriveInterface * findDrive(
     const cta::tape::daemon::TpconfigLine &driveConfig, cta::log::LogContext & lc, cta::TapeMount *mount);
        
    /** sub-part of execute for the read sessions */
    EndOfSessionAction executeRead(cta::log::LogContext & lc, cta::RetrieveMount *retrieveMount);
    /** sub-part of execute for a write session */
    EndOfSessionAction executeWrite(cta::log::LogContext & lc, cta::ArchiveMount *archiveMount);
    /** sub-part of execute for a label session */
    EndOfSessionAction executeLabel(cta::log::LogContext & lc, cta::LabelMount *labelMount);
    /** Reference to the MediaChangerFacade, allowing the mounting of the tape
     * by the library. It will be used exclusively by the tape thread. */
    cta::mediachanger::MediaChangerFacade & m_mc;
    /** Reference to the tape server's parent process to report detailed status */
    cta::tape::daemon::TapedProxy & m_intialProcess;
    /** Object providing utilities for working UNIX capabilities. */
    cta::server::ProcessCap &m_capUtils;
    /** hostname, used to report status of the drive */
    const std::string m_hostname;
    /**
     * The scheduler, i.e. the local interface to the Objectstore DB
     */
    cta::Scheduler &m_scheduler;

    /**
     * Returns the string representation of the specified mount type
     */
    const char *mountTypeToString(const cta::common::dataStructures::MountType mountType) const
      throw();
  };
}
}
}
}
