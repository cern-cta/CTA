/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "catalogue/TapeDrivesCatalogueState.hpp"
#include "common/dataStructures/DriveDownReason.hpp"
#include "common/dataStructures/DriveInfo.hpp"
#include "common/dataStructures/DriveReport.hpp"
#include "common/log/LogContext.hpp"
#include "mediachanger/MediaChangerFacade.hpp"
#include "taped/TapedConfig.hpp"
#include "taped/drive/DriveReservation.hpp"
#include "taped/session/TapeSessionResult.hpp"
#include "taped/session/TapeSessionTracker.hpp"
#include "taped/system/Wrapper.hpp"

#include <atomic>
#include <memory>
#include <optional>
#include <stop_token>
#include <string>
#include <string_view>

namespace cta {
class TapeMount;
}  // namespace cta

namespace cta::tape::daemon {
class SchedulerContext;

/// @brief Run tape sessions and idle polling during one period of drive ownership.
///
/// Prepare hardware before scheduling; stop or operator Down ends the session after active tape work finishes.
class DriveSession final {
public:
  /// @brief Initialize a session without accessing hardware; run() performs preparation.
  ///
  /// Configuration, logger, scheduler context and any supplied wrapper must outlive the session.
  /// A null sysWrapper selects the session-owned real wrapper.
  DriveSession(const TapedConfig& config,
               log::Logger& log,
               SchedulerContext& schedulerContext,
               System::virtualWrapper* sysWrapper = nullptr);

  /// @brief Prepare the drive and schedule tape sessions until stop or reservation release.
  ///
  /// Call once.
  /// Preparation failure or operator Down ends the session; unexpected failures propagate.
  void run(std::stop_token stopToken);

  /// Attempt to release drive ownership and report Down in the catalogue.
  ~DriveSession() noexcept;

  /// @brief Return whether the current operation is still within its configured timeout.
  ///
  /// Safe for concurrent health queries; returns true when no tracker is active.
  bool isLive() const;

  /// Prevent copying a drive reservation.
  DriveSession(const DriveSession&) = delete;

  /// Prevent replacing a drive reservation by copying.
  DriveSession& operator=(const DriveSession&) = delete;

  /// Keep the session at a stable address for health readers.
  DriveSession(DriveSession&&) = delete;

  /// Prevent moving a session observed by health readers.
  DriveSession& operator=(DriveSession&&) = delete;

private:
  friend class DriveSessionTest;

  /// Choose immediate scheduling, a retry delay, or release of drive ownership.
  enum class IterationAction { Continue, RetryAfterDelay, EndOwnership };

  /// Scheduling decision with a diagnostic when ending the reservation.
  struct IterationResult {
    IterationAction action;
    std::string endReason;
  };

  /// @brief Check the desired drive state, obtain work and decide whether to continue, wait or stop.
  ///
  /// Continue and RetryAfterDelay leave the drive empty and reusable.
  /// EndOwnership may leave a cartridge after failed cleanup.
  /// @pre The drive is empty and reserved by this session.
  IterationResult runIteration(std::stop_token stopToken);

  /// @brief Execute an assignment and attempt drive cleanup if the session throws.
  ///
  /// Request Down if the drive cannot be reused.
  /// If worker termination cannot be established, the process exits without recovery or unwinding.
  TapeSessionResult runTapeSession(TapeMount& tapeMount);

  /// @brief Probe or clean the drive for preparation, returning whether scheduling may continue.
  ///
  /// Initial preparation follows clean_on_up; active-session recovery always cleans.
  /// Only recovery supplies a known tape volume identifier (VID). A Down request prevents further scheduling.
  bool prepareDrive(const std::optional<std::string>& vid = std::nullopt);

  /// @brief Release drive ownership, then report Down; retry a failed catalogue update on the next call.
  ///
  /// Call only after all drive users have stopped.
  void releaseAndReportDown();

  /// @brief Request Down without overwriting an existing, more specific reason.
  ///
  /// Catalogue update failures throw; this does not release drive ownership.
  void requestDown(common::dataStructures::DriveDownReason reason, std::string_view detail = "");

  /// Request Down, logging catalogue update failures without throwing.
  void requestDownNoThrow(common::dataStructures::DriveDownReason reason, std::string_view detail) noexcept;

  /// Publish drive readiness and optional tape activity using this session's identity and log context.
  void reportDriveState(const common::dataStructures::DriveReport& report);

  const TapedConfig& m_config;
  const common::dataStructures::DriveInfo m_driveInfo;
  log::LogContext m_lc;
  SchedulerContext& m_schedulerContext;
  TapeDrivesCatalogueState m_driveState;
  /// Hardware access is scoped to this ownership period, including preparation and recovery.
  mediachanger::MediaChangerFacade m_mediaChanger;
  System::realWrapper m_realSysWrapper;
  System::virtualWrapper& m_sysWrapper;
  /// Active preparation or tape-session tracker; health readers retain their own shared reference.
  std::atomic<std::shared_ptr<const TapeSessionTracker>> m_activeTracker {nullptr};
  /// Prevents duplicate Down reports after a successful catalogue update.
  bool m_downPublicationComplete = false;
  /// Destroy first, while its borrowed dependencies and hardware access objects are still alive.
  DriveReservation m_driveReservation;
};

}  // namespace cta::tape::daemon
