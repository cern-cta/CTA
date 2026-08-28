/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "MountType.hpp"
#include "TapeSessionState.hpp"

#include <cstdint>
#include <ctime>
#include <optional>
#include <string>

namespace cta::common::dataStructures {

/// Hardware ownership and readiness, independently of desired state and tape activity.
enum class ReportedDriveState {
  Down,       ///< Hardware ownership has ended, or has not started.
  Preparing,  ///< Published before acquisition; preparation or recovery is in progress.
  Up          ///< Preparation succeeded; hardware is owned and available for scheduled work.
};

/// Activity and scheduler metadata belonging to one tape session.
/// Cleanup has no scheduler mount and leaves mount metadata at its defaults.
struct TapeSessionReport {
  TapeSessionState state;
  MountType mountType = MountType::NoMount;
  uint64_t mountSessionId = 0;
  uint64_t bytesTransferred = 0;
  uint64_t filesTransferred = 0;
  std::string vid {};
  std::string tapePool {};
  std::string vo {};
  uint64_t priority = 0;
  std::optional<std::string> activity = std::nullopt;
};

/// One observation; desired state is never part of a daemon report.
/// Preparing may carry cleanup activity; probing needs no tape session.
/// Down confirms that no hardware users remain and must not carry tape activity.
struct DriveReport {
  ReportedDriveState state;
  std::optional<TapeSessionReport> tapeSession = std::nullopt;
  time_t reportTime = std::time(nullptr);
};

}  // namespace cta::common::dataStructures
