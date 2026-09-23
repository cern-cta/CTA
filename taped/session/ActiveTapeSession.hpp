/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "TapeSessionTracker.hpp"

#include <atomic>
#include <memory>
#include <optional>
#include <utility>

namespace cta::tape::daemon {

/** Publish one active session to concurrent health readers without borrowing its lifetime. */
class ActiveTapeSession {
public:
  /** The single session owner keeps this guard until execute() returns or throws; scopes must not overlap. */
  class Scope {
  public:
    Scope(ActiveTapeSession& active, std::shared_ptr<const TapeSessionTracker> tracker) : m_active(active) {
      m_active.m_tracker.store(std::move(tracker));
    }

    ~Scope() { m_active.m_tracker.store(nullptr); }

    Scope(const Scope&) = delete;
    Scope& operator=(const Scope&) = delete;

  private:
    ActiveTapeSession& m_active;
  };

  std::optional<TapeSessionLivenessSnapshot> snapshot() const {
    // Retain the tracker until the snapshot is copied, even if execution ends concurrently.
    const auto tracker = m_tracker.load();
    if (!tracker) {
      return std::nullopt;
    }
    return tracker->livenessSnapshot();
  }

private:
  std::atomic<std::shared_ptr<const TapeSessionTracker>> m_tracker {nullptr};
};

}  // namespace cta::tape::daemon
