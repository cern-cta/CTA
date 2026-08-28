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
 * @brief Encapsulates a single tape session (mount -> transfer -> unmount).
 */
class TapeSession {
public:
  /**
   * @brief Create a session for a given tape mount.
   *
   * @param log Logger that must outlive the session.
   * @param sysWrapper System-call wrapper that must outlive the session.
   * @param driveInfo Drive identity copied into the session.
   * @param mc Borrowed media changer used by tape operations.
   * @param tapeMount Mount kept alive by the caller until session execution and reporting finish.
   * @param transfersConfig Transfer settings copied into the session.
   * @param tapeLoadTimeoutSecs Maximum time in seconds allowed for tape loading.
   * @param scheduler Borrowed scheduler used for catalogue access and drive-state publication.
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
   * @brief Mount, transfer, unload and dismount the tape, returning the session outcomes.
   *
   * Recoverable operational failures return recovery decisions after local cleanup.
   * Unrecoverable failures propagate; partial worker startup/termination recovery is not yet supported.
   * May produce an empty mount, in which case hardware remains untouched.
   *
   * @return Drive usability and backend-recovery or retry-delay decisions for the controller.
   */
  TapeSessionResult execute();

  /**
   * @brief Return read-only tracking state valid for the lifetime of this session.
   *
   * @return Read-only reference to the tracker, valid for this session lifetime.
   */
  const TapeSessionTracker& tracker() const { return m_tapeSessionTracker; }

  /**
   * @brief Return the volume identifier captured during execute(), or an empty string before it is known.
   *
   * @return Volume identifier, empty until the session has obtained it from its mount.
   */
  const std::string& getVid() const { return m_volInfo.vid; }

  /**
   * @brief Destructor.
   */
  ~TapeSession() noexcept = default;

private:
  struct ExecutionState;

  // Owned tracking state outlives the workers and reporter created by execute().
  TapeSessionTracker m_tapeSessionTracker;
  cta::log::Logger& m_log;
  cta::TapeMount& m_tapeMount;
  VolumeInfo m_volInfo {};
  System::virtualWrapper& m_sysWrapper;
  const TransfersConfig m_transfersConfig;
  const uint32_t m_tapeLoadTimeoutSecs;
  const cta::common::dataStructures::DriveInfo m_driveInfo;

  /**
   * @brief Discover and open the configured drive, recording a down decision before propagating failures.
   *
   * @param logContext Log context for session diagnostics.
   * @param state Execution state updated with drive-open and failure decisions.
   * @return Owned, opened drive interface initialized with the session drive identity.
   */
  std::unique_ptr<cta::tape::drive::DriveInterface> findDrive(cta::log::LogContext& logContext, ExecutionState& state);

  /**
   * @brief Build and run the tape-to-disk pipeline, or finalize an empty or rejected retrieval mount.
   *
   * @param logContext Session logging context.
   * @param retrieveMount Borrowed mount supplying retrieval jobs.
   * @param state Track worker startup, completion ownership and recovery decisions.
   */
  void executeRead(cta::log::LogContext& logContext, cta::RetrieveMount& retrieveMount, ExecutionState& state);

  /**
   * @brief Build and run the disk-to-tape pipeline, or finalize a mount with no archive work.
   *
   * @param logContext Session logging context.
   * @param archiveMount Borrowed mount supplying archive jobs.
   * @param state Track worker startup, completion ownership and recovery decisions.
   */
  void executeWrite(cta::log::LogContext& logContext, cta::ArchiveMount& archiveMount, ExecutionState& state);

  /**
   * @brief Reference to the MediaChangerFacade, allowing the mounting of the tape
   * by the library. It will be used exclusively by the tape thread.
   */
  cta::mediachanger::MediaChangerFacade& m_mediaChanger;

  /**
   * @brief The scheduler, i.e. the local interface to the Objectstore DB
   */
  cta::Scheduler& m_scheduler;
};

}  // namespace cta::tape::daemon
