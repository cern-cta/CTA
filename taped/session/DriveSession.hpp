/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "TapeSessionResult.hpp"
#include "TapeSessionTracker.hpp"
#include "common/dataStructures/DriveDownReason.hpp"
#include "common/dataStructures/DriveInfo.hpp"
#include "common/log/LogContext.hpp"
#include "mediachanger/MediaChangerFacade.hpp"
#include "taped/TapedConfig.hpp"
#include "taped/drive/DriveReservation.hpp"
#include "taped/system/Wrapper.hpp"

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

/// Manage one drive reservation across preparation, tape sessions and recovery.
class DriveSession final {
public:
  /// @brief Create a session using the real system-call wrapper, without accessing hardware.
  ///
  /// Configuration, logger and scheduler context must outlive the session; run() performs preparation.
  static std::unique_ptr<DriveSession>
  create(const TapedConfig& config, log::Logger& log, SchedulerContext& schedulerContext);

  /// @brief Create a session with a borrowed system-call wrapper, without accessing hardware.
  ///
  /// All supplied dependencies must outlive the session; run() performs preparation.
  static std::unique_ptr<DriveSession> create(const TapedConfig& config,
                                              log::Logger& log,
                                              SchedulerContext& schedulerContext,
                                              System::virtualWrapper& sysWrapper);

  /// @brief Prepare the drive and schedule tape sessions until stop or reservation release.
  ///
  /// Call once. Active transfers are not interrupted by the stop token.
  /// Preparation failure or withdrawn up intent ends the session; unexpected failures propagate.
  void run(std::stop_token stopToken);

  /// Attempt reservation release and Down publication, logging failures without throwing.
  ~DriveSession() noexcept;

  /// @brief Return whether the active preparation, recovery or transfer is within its liveness deadline.
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

  /// @brief Initialize borrowed dependencies and owned hardware helpers without preparing the drive.
  ///
  /// A null sysWrapper selects the session-owned real wrapper.
  DriveSession(const TapedConfig& config,
               log::Logger& log,
               SchedulerContext& schedulerContext,
               System::virtualWrapper* sysWrapper = nullptr);

  /// Scheduling decision after one iteration.
  enum class IterationAction { Continue, RetryAfterDelay, EndOwnership };

  /// Scheduling decision with a diagnostic when ending the reservation.
  struct IterationResult {
    IterationAction action;
    std::string endReason;
  };

  /// @brief Poll operator intent, obtain work and translate a tape-session outcome into a loop decision.
  ///
  /// Idle polls and recoverable scheduling failures request a retry delay.
  /// @pre The drive is empty and reserved by this session.
  IterationResult runIteration(std::stop_token stopToken);

  /// @brief Execute an assignment and attempt drive recovery after ordinary escaping failures.
  ///
  /// Unsafe worker teardown propagates without recovery; unusable results request desired Down.
  TapeSessionResult runTapeSession(TapeMount& tapeMount);

  /// @brief Clean the drive for initial preparation or recovery, returning whether scheduling may continue.
  ///
  /// Only recovery supplies a known VID. Operator Down intent prevents further scheduling.
  bool cleanDrive(const std::optional<std::string>& vid = std::nullopt);

  /// @brief Release the reservation before reporting Down; retain failed publication for a later retry.
  ///
  /// Unsafe worker teardown prevents release and Down publication.
  void releaseAndReportDown();

  /// @brief Request desired Down while preserving an observed specific reason.
  ///
  /// Publication failures propagate; this does not release the reservation.
  void requestDown(common::dataStructures::DriveDownReason reason, std::string_view detail = "");

  /// Request desired Down, logging publication failures without throwing.
  void requestDownNoThrow(common::dataStructures::DriveDownReason reason, std::string_view detail) noexcept;

  const TapedConfig& m_config;
  const common::dataStructures::DriveInfo m_driveInfo;
  log::LogContext m_lc;
  SchedulerContext& m_schedulerContext;
  /// Hardware access is scoped to this ownership period, including preparation and recovery.
  mediachanger::MediaChangerFacade m_mediaChanger;
  System::realWrapper m_realSysWrapper;
  System::virtualWrapper& m_sysWrapper;
  /// Active cleanup or transfer tracker; health readers retain their own atomically loaded shared reference.
  std::shared_ptr<const TapeSessionTracker> m_activeTracker;
  /// Tracks terminal publication only; hardware ownership is managed independently.
  bool m_downPublicationComplete = false;
  /// Destroy first, while its borrowed dependencies and hardware access objects are still alive.
  DriveReservation m_driveReservation;
};

}  // namespace cta::tape::daemon
