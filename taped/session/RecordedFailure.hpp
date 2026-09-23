/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

namespace cta::tape::daemon {

class TapeSessionTracker;
enum class TapeSessionFailure;

/** Receipt for a failure already counted by a tracker; producers pass it with the failed file. */
class RecordedFailure {
public:
  bool belongsTo(const TapeSessionTracker& tracker) const { return m_tracker == &tracker; }

  TapeSessionFailure reason() const { return m_reason; }

private:
  friend class TapeSessionTracker;

  RecordedFailure(const TapeSessionTracker& tracker, TapeSessionFailure reason)
      : m_tracker(&tracker),
        m_reason(reason) {}

  const TapeSessionTracker* m_tracker;
  TapeSessionFailure m_reason;
};

}  // namespace cta::tape::daemon
