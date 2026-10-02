/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

namespace cta::tape::daemon {

class TapeSessionTracker;

/** Receipt for a failure already counted by a tracker; producers pass it with the failed file. */
class RecordedFailure {
public:
  bool belongsTo(const TapeSessionTracker& tracker) const { return m_tracker == &tracker; }

private:
  friend class TapeSessionTracker;

  explicit RecordedFailure(const TapeSessionTracker& tracker) : m_tracker(&tracker) {}

  const TapeSessionTracker* m_tracker;
};

}  // namespace cta::tape::daemon
