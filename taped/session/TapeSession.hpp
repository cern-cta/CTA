/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "TapeSessionResult.hpp"
#include "TapeSessionTracker.hpp"
#include "TapeSingleThreadInterface.hpp"
#include "common/dataStructures/DriveDownReason.hpp"
#include "common/log/LogContext.hpp"
#include "common/log/Logger.hpp"
#include "mediachanger/MediaChangerFacade.hpp"
#include "scheduler/ArchiveMount.hpp"
#include "scheduler/LabelMount.hpp"
#include "scheduler/RetrieveMount.hpp"
#include "scheduler/Scheduler.hpp"
#include "scheduler/TapeMount.hpp"
#include "taped/TapedConfig.hpp"
#include "taped/system/Wrapper.hpp"

#include <memory>

namespace cta::tape::daemon {

/**
 * The main class handling a tape session.
 */
class TapeSession {
public:
  /**
   * Constructor.
   *
   * @param log Object representing the API of the CTA logging system.
   */
  TapeSession(cta::log::Logger& log,
              System::virtualWrapper& sysWrapper,
              const cta::common::dataStructures::DriveInfo& driveInfo,
              cta::mediachanger::MediaChangerFacade& mc,
              cta::TapeMount& tapeMount,
              const TransfersConfig& transfersConfig,
              uint32_t tapeLoadTimeoutSecs,
              cta::Scheduler& scheduler);

  /**
   * Mount, transfer, unload and dismount the tape, returning the session outcomes.
   * Exceptions propagate unchanged; the result describes sessions that return normally.
   */
  TapeSessionResult execute();

  // Read-only inspection remains valid for the lifetime of this session.
  const TapeSessionTracker& tracker() const { return m_tapeSessionTracker; }

  /** Temporary method used for debugging while building the session class */
  const std::string& getVid() const { return m_volInfo.vid; }

  /**
   * Destructor.
   */
  ~TapeSession() noexcept = default;

private:
  // Owned tracking state outlives the workers and reporter created by execute().
  TapeSessionTracker m_tapeSessionTracker;

  /**
   * Object representing the API of the CTA logging system.
   */
  cta::log::Logger& m_log;
  cta::TapeMount& m_tapeMount;
  VolumeInfo m_volInfo {};
  System::virtualWrapper& m_sysWrapper;
  const TransfersConfig m_transfersConfig;
  const uint32_t m_tapeLoadTimeoutSecs;
  /**
   * The drive information bundle allowing drive register update.
   * Filled up at construction time.
   */
  const cta::common::dataStructures::DriveInfo m_driveInfo;

  /** utility to find the drive on the system. This function logs
   * all errors and hence does not throw exceptions. It returns nullptr
   * in case of failure. */
  std::unique_ptr<cta::tape::drive::DriveInterface> findDrive(cta::log::LogContext& logContext, cta::TapeMount& mount);

  /**
   * Put drive down with reason with [cta-taped] prefix, update the desired state (which is also down).
   * If mount is passed, it will be marked as complete.
   * Log the error with drive and mount details
   */
  void putDriveDown(common::dataStructures::DriveDownReason reason,
                    cta::TapeMount* mount,
                    cta::log::LogContext& logContext,
                    std::string_view detail = {});

  /** sub-part of execute for the read sessions */
  TapeSessionResult executeRead(cta::log::LogContext& logContext, cta::RetrieveMount& retrieveMount);

  /** sub-part of execute for a write session */
  TapeSessionResult executeWrite(cta::log::LogContext& logContext, cta::ArchiveMount& archiveMount);

  /** sub-part of execute for a label session */
  TapeSessionResult executeLabel(cta::log::LogContext& logContext, cta::LabelMount* labelMount) const;

  /** Reference to the MediaChangerFacade, allowing the mounting of the tape
   * by the library. It will be used exclusively by the tape thread. */
  cta::mediachanger::MediaChangerFacade& m_mediaChanger;
  /**
   * The scheduler, i.e. the local interface to the Objectstore DB
   */
  cta::Scheduler& m_scheduler;
};

}  // namespace cta::tape::daemon
