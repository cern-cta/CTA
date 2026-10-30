/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "TapeSessionResult.hpp"
#include "TapeSessionTracker.hpp"
#include "TapeSingleThreadInterface.hpp"
#include "catalogue/Catalogue.hpp"
#include "common/dataStructures/DriveDownReason.hpp"
#include "common/log/LogContext.hpp"
#include "common/log/Logger.hpp"
#include "mediachanger/MediaChangerFacade.hpp"
#include "scheduler/ArchiveMount.hpp"
#include "scheduler/LabelMount.hpp"
#include "scheduler/RetrieveMount.hpp"
#include "scheduler/TapeMount.hpp"
#include "taped/TapedConfig.hpp"
#include "taped/system/Wrapper.hpp"

#include <memory>

namespace cta::tape::daemon {

class RecallTaskInjector;

/// @brief Run one archive or retrieval assignment: mount the tape, transfer files, then unmount it.
///
/// DriveSession schedules the assignment; MountedTape ensures cartridge cleanup, including after failures.
class TapeSession {
public:
  /// @brief Create a session for a scheduler assignment; execute() starts the work.
  ///
  /// Logger, system wrapper, media changer, mount and catalogue must outlive the session.
  /// Drive identity and transfer settings are copied; tapeLoadTimeoutSecs is the readiness timeout in seconds.
  TapeSession(cta::log::Logger& log,
              System::virtualWrapper& sysWrapper,
              const cta::common::dataStructures::DriveInfo& driveInfo,
              cta::mediachanger::MediaChangerFacade& mc,
              cta::TapeMount& tapeMount,
              const TransfersConfig& transfersConfig,
              uint32_t tapeLoadTimeoutSecs,
              cta::catalogue::Catalogue& catalogue);

  /// @brief Run one assignment through transfer, cleanup and final reporting.
  ///
  /// Assignments with no work may finish without mounting a tape. The caller can attempt recovery if this throws.
  /// Exit the process immediately if workers cannot be confirmed stopped, avoiding unsafe cleanup.
  /// @return Whether the assignment succeeded and, separately, whether the drive can accept more work.
  TapeSessionResult execute();

  /// Return read-only tracking state, valid for this session's lifetime.
  const TapeSessionTracker& tracker() const { return *m_tapeSessionTracker; }

  /// Share read-only tracking state that can outlive the session for concurrent health queries.
  std::shared_ptr<const TapeSessionTracker> sharedTracker() const { return m_tapeSessionTracker; }

  /// Return the VID captured by execute(), or an empty string before metadata is loaded.
  const std::string& getVid() const { return m_volInfo.vid; }

  /// Release session-owned data; execute() is responsible for worker teardown.
  ~TapeSession() noexcept = default;

private:
  struct ExecutionState;

  /// Owned tracking state outlives the workers and reporter created by execute().
  std::shared_ptr<TapeSessionTracker> m_tapeSessionTracker;
  cta::log::Logger& m_log;
  cta::TapeMount& m_tapeMount;
  VolumeInfo m_volInfo {};
  System::virtualWrapper& m_sysWrapper;
  const TransfersConfig m_transfersConfig;
  const uint32_t m_tapeLoadTimeoutSecs;
  const cta::common::dataStructures::DriveInfo m_driveInfo;

  /// @brief Discover and open the drive, recording a down decision in state before propagating failures.
  ///
  /// Return an owned drive initialized with the session drive identity.
  std::unique_ptr<cta::tape::drive::DriveInterface> findDrive(cta::log::LogContext& logContext, ExecutionState& state);

  /// @brief Run the tape-to-disk pipeline if jobs and disk space are available.
  ///
  /// Record whether the pipeline completes the scheduler mount and whether the drive can be reused.
  /// execute() completes empty or rejected mounts.
  void executeRead(cta::log::LogContext& logContext, cta::RetrieveMount& retrieveMount, ExecutionState& state);

  /// Configure requested RAO unless the tape label format is incompatible.
  void configureRAO(cta::log::LogContext& logContext, RecallTaskInjector& taskInjector);

  /// @brief Run the disk-to-tape pipeline if archive jobs are available.
  ///
  /// Record whether the pipeline completes the scheduler mount and whether the drive can be reused.
  /// execute() completes empty mounts.
  void executeWrite(cta::log::LogContext& logContext, cta::ArchiveMount& archiveMount, ExecutionState& state);

  cta::mediachanger::MediaChangerFacade& m_mediaChanger;

  cta::catalogue::Catalogue& m_catalogue;
};

}  // namespace cta::tape::daemon
