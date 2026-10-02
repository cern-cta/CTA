/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

namespace cta::tape::daemon {

class TapeSessionTracker;

/// @brief Indicates that TapeSessionTracker::recordFailure() has already counted a failure.
///
/// Prevents recounting failures.
class RecordedFailure {
public:
  /// Return whether this receipt was issued by the supplied tracker.
  bool belongsTo(const TapeSessionTracker& tracker) const { return m_tracker == &tracker; }

private:
  friend class TapeSessionTracker;

  /// Create a receipt for a failure already counted by the issuing tracker.
  explicit RecordedFailure(const TapeSessionTracker& tracker) : m_tracker(&tracker) {}

  /// Non-owning identity used to validate receipts; never dereferenced.
  const TapeSessionTracker* m_tracker;
};

}  // namespace cta::tape::daemon
