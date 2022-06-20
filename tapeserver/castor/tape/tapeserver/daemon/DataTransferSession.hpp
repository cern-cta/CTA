/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
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
class DataTransferSession : public Session {
public:
  /**
   * Constructor.
   *
   * @param log Object representing the API of the CASTOR logging system.
   */
  DataTransferSession(const std::string& hostname, cta::log::Logger& log,
                      System::virtualWrapper& sysWrapper,
                      const cta::tape::daemon::TpconfigLine& driveConfig,
                      cta::mediachanger::MediaChangerFacade& mc,
                      cta::tape::daemon::TapedProxy& initialProcess,
                      cta::server::ProcessCap& capUtils, const DataTransferConfig& castorConf,
                      cta::Scheduler& scheduler);

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
  EndOfSessionAction execute() override;

  /**
   * Sets the capabilities of the process and logs the result.
   *
   * @param capabilities The string representation of the capabilities.
   */
  void setProcessCapabilities(const std::string& capabilities);

  /** Temporary method used for debugging while building the session class */
  std::string getVid() const { return m_volInfo.vid; }

  /**
   * Destructor.
   */
  ~DataTransferSession() noexcept override;

private:

  /**
   * Object representing the API of the CASTOR logging system.
   */
  cta::log::Logger& m_log;
  VolumeInfo m_volInfo;
  System::virtualWrapper& m_sysWrapper;
  /**
   * The configuration of the tape drive to be used by this session.
   */
  const cta::tape::daemon::TpconfigLine m_driveConfig;
  const DataTransferConfig& m_castorConf;
  /**
   * The drive information bundle allowing drive register update.
   * Filled up at construction time.
   */
  const cta::common::dataStructures::DriveInfo m_driveInfo;

  /** utility to find the drive on the system. This function logs
   * all errors and hence does not throw exceptions. It returns nullptr
   * in case of failure. */
  castor::tape::tapeserver::drive::DriveInterface *findDrive(cta::log::LogContext& logContext, cta::TapeMount *mount);

  /**
   * Put drive down with reason with [cta-taped] prefix, update the desired state (which is also down).
   * If mount is passed, it will be marked as complete.
   * Log the error with drive and mount details
   */
  void putDriveDown(const std::string &headerErrMsg, cta::TapeMount *mount, cta::log::LogContext &logContext);

  /** sub-part of execute for the read sessions */
  EndOfSessionAction executeRead(cta::log::LogContext& logContext, cta::RetrieveMount *retrieveMount,
                                 TapeSessionReporter& reporter);

  /** sub-part of execute for a write session */
  EndOfSessionAction executeWrite(cta::log::LogContext& logContext, cta::ArchiveMount *archiveMount,
                                  TapeSessionReporter& reporter);

  /** sub-part of execute for a label session */
  EndOfSessionAction executeLabel(cta::log::LogContext& logContext, cta::LabelMount *labelMount);

  /** Reference to the MediaChangerFacade, allowing the mounting of the tape
   * by the library. It will be used exclusively by the tape thread. */
  cta::mediachanger::MediaChangerFacade& m_mediaChanger;
  /** Reference to the tape server's parent process to report detailed status */
  cta::tape::daemon::TapedProxy& m_initialProcess;
  /** Object providing utilities for working UNIX capabilities. */
  cta::server::ProcessCap& m_capUtils;
  /** hostname, used to report status of the drive */
  const std::string m_hostname;
  /**
   * The scheduler, i.e. the local interface to the Objectstore DB
   */
  cta::Scheduler& m_scheduler;
};
}
}
}
}
