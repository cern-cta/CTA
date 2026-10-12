/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "TapeSessionTracker.hpp"

#include <algorithm>
#include <utility>

namespace cta::tape::daemon {

TapeSessionTracker::TapeSessionTracker(StateReporter reporter) : m_stateReporter(std::move(reporter)) {}

void TapeSessionTracker::beginTapeSession(Clock::time_point now) {
  std::lock_guard transitionLock(m_transitionMutex);
  {
    std::lock_guard lock(m_mutex);
    m_stats = {};
    m_failureCounts.fill(0);
    m_eventCounts.fill(0);
    m_tapeAlertStats.clear();
    m_activeDiskFiles.clear();
    m_mountAttempted = true;
    m_fileId = 0;
    m_fSeq = 0;
    m_fileBeingMoved = false;
    m_fileStartTime = {};
    m_bytesMoved = 0;
    m_lastBlockMovement = {};
    m_tapeDone = false;
    m_diskDone = false;
    m_sessionStartTime = now;
    m_stateEnteredAt = now;
    m_state = cta::tape::session::TapeSessionState::Preparing;
    m_type = cta::tape::session::SessionType::Undetermined;
  }
  publishState(cta::tape::session::TapeSessionState::Preparing);
}

void TapeSessionTracker::reportState(cta::tape::session::TapeSessionState state, Clock::time_point now) {
  std::lock_guard transitionLock(m_transitionMutex);
  {
    std::lock_guard lock(m_mutex);
    if (!setStateLocked(state, now)) {
      return;
    }
  }
  publishState(state);
}

std::optional<cta::tape::session::TapeSessionState> TapeSessionTracker::state() const {
  std::lock_guard lock(m_mutex);
  return m_state;
}

void TapeSessionTracker::setType(cta::tape::session::SessionType type) {
  std::lock_guard lock(m_mutex);
  m_type = type;
}

void TapeSessionTracker::notifyTapeDone(Clock::time_point now) {
  std::lock_guard transitionLock(m_transitionMutex);
  std::optional<cta::tape::session::TapeSessionState> changedState;
  {
    std::lock_guard lock(m_mutex);
    m_tapeDone = true;
    changedState = updateRetrievalCompletionState(now);
  }
  if (changedState) {
    publishState(*changedState);
  }
}

void TapeSessionTracker::notifyDiskDone(Clock::time_point now) {
  std::lock_guard transitionLock(m_transitionMutex);
  std::optional<cta::tape::session::TapeSessionState> changedState;
  {
    std::lock_guard lock(m_mutex);
    m_diskDone = true;
    changedState = updateRetrievalCompletionState(now);
  }
  if (changedState) {
    publishState(*changedState);
  }
}

cta::tape::session::SessionType TapeSessionTracker::type() const {
  std::lock_guard lock(m_mutex);
  return m_type;
}

TapeSessionOutcomeSnapshot TapeSessionTracker::outcomeSnapshot() const {
  std::lock_guard lock(m_mutex);
  return {m_state == cta::tape::session::TapeSessionState::Finished,
          hasFailuresLocked(),
          m_failureCounts,
          m_eventCounts,
          m_tapeAlertStats};
}

bool TapeSessionTracker::hasFailures() const {
  std::lock_guard lock(m_mutex);
  return hasFailuresLocked();
}

bool TapeSessionTracker::recallCompletionHasDiagnostics() const {
  std::lock_guard lock(m_mutex);
  if (!m_tapeAlertStats.empty()) {
    return true;
  }
  for (const auto count : m_eventCounts) {
    if (count != 0) {
      return true;
    }
  }
  for (size_t i = 0; i < m_failureCounts.size(); ++i) {
    if (m_failureCounts[i] == 0) {
      continue;
    }
    // Propagation-only failures do not change the legacy recall end-report protocol.
    switch (static_cast<TapeSessionFailure>(i)) {
      case TapeSessionFailure::UnexpectedSession:
      case TapeSessionFailure::TaskInjection:
      case TapeSessionFailure::WorkerSignalling:
      case TapeSessionFailure::UnexpectedCleanup:
      case TapeSessionFailure::UnclassifiedFile:
        break;
      default:
        return true;
    }
  }
  return false;
}

void TapeSessionTracker::setMountAttempted(bool attempted) {
  std::lock_guard lock(m_mutex);
  m_mountAttempted = attempted;
}

bool TapeSessionTracker::mountAttempted() const {
  std::lock_guard lock(m_mutex);
  return m_mountAttempted;
}

RecordedFailure TapeSessionTracker::recordFailure(TapeSessionFailure failure) {
  std::lock_guard lock(m_mutex);
  recordFailureLocked(failure);
  return RecordedFailure(*this);
}

void TapeSessionTracker::recordFailureIfNone(TapeSessionFailure failure) {
  std::lock_guard lock(m_mutex);
  if (!hasFailuresLocked()) {
    recordFailureLocked(failure);
  }
}

void TapeSessionTracker::recordEvent(TapeSessionEvent event) {
  std::lock_guard lock(m_mutex);
  auto& count = m_eventCounts.at(static_cast<size_t>(event));
  if (event == TapeSessionEvent::TapeFilledUp) {
    count = 1;
  } else {
    ++count;
  }
}

void TapeSessionTracker::incrementTapeAlert(uint16_t tapeAlertCode) {
  std::lock_guard lock(m_mutex);
  ++m_tapeAlertStats[tapeAlertCode];
}

void TapeSessionTracker::updateTapeSetupStats(const TapeSetupStats& stats) {
  std::lock_guard lock(m_mutex);
  m_stats.setup = stats;
}

void TapeSessionTracker::addTapeSetupStats(const TapeSetupStats& stats) {
  std::lock_guard lock(m_mutex);
  m_stats.setup.add(stats);
}

void TapeSessionTracker::updateTapeTransferStats(const TapeTransferStats& stats) {
  std::lock_guard lock(m_mutex);
  m_stats.tape = stats;
}

void TapeSessionTracker::addTapeTransferStats(const TapeTransferStats& stats) {
  std::lock_guard lock(m_mutex);
  m_stats.tape.add(stats);
}

void TapeSessionTracker::updateDiskTransferStats(const DiskTransferStats& stats) {
  std::lock_guard lock(m_mutex);
  m_stats.disk = stats;
}

void TapeSessionTracker::addDiskTransferStats(const DiskTransferStats& stats) {
  std::lock_guard lock(m_mutex);
  m_stats.disk.add(stats);
}

void TapeSessionTracker::updateTapeCleanupStats(const TapeCleanupStats& stats) {
  std::lock_guard lock(m_mutex);
  m_stats.cleanup = stats;
}

void TapeSessionTracker::addTapeCleanupStats(const TapeCleanupStats& stats) {
  std::lock_guard lock(m_mutex);
  m_stats.cleanup.add(stats);
}

void TapeSessionTracker::setTotalTime(double totalTime) {
  std::lock_guard lock(m_mutex);
  m_stats.totalTime = totalTime;
}

void TapeSessionTracker::setDiskDeliveryTime(double deliveryTime) {
  std::lock_guard lock(m_mutex);
  m_stats.disk.deliveryTime = deliveryTime;
}

void TapeSessionTracker::notifyDiskFileOpened(uint32_t threadId, uint64_t fileId, std::string path) {
  std::lock_guard lock(m_mutex);
  m_activeDiskFiles.insert_or_assign(threadId,
                                     DiskFileProgress {fileId, std::move(path), std::chrono::steady_clock::now()});
}

void TapeSessionTracker::notifyDiskFileClosed(uint32_t threadId) {
  std::lock_guard lock(m_mutex);
  m_activeDiskFiles.erase(threadId);
}

ActiveDiskFiles TapeSessionTracker::activeDiskFiles() const {
  std::lock_guard lock(m_mutex);
  return m_activeDiskFiles;
}

TapeSessionFailureStats TapeSessionTracker::failureStats() const {
  std::lock_guard lock(m_mutex);
  TapeSessionFailureStats result;
  for (size_t i = 0; i < m_failureCounts.size(); ++i) {
    if (m_failureCounts[i]) {
      result.emplace(static_cast<TapeSessionFailure>(i), m_failureCounts[i]);
    }
  }
  return result;
}

TapeAlertStats TapeSessionTracker::tapeAlertStats() const {
  std::lock_guard lock(m_mutex);
  return m_tapeAlertStats;
}

TapeSessionStats TapeSessionTracker::stats() const {
  std::lock_guard lock(m_mutex);
  return m_stats;
}

void TapeSessionTracker::notifyBlockMovement(uint64_t bytes, Clock::time_point now) {
  std::lock_guard lock(m_mutex);
  m_bytesMoved += bytes;
  m_lastBlockMovement = now;
}

uint64_t TapeSessionTracker::bytesMoved() const {
  std::lock_guard lock(m_mutex);
  return m_bytesMoved;
}

std::chrono::steady_clock::time_point TapeSessionTracker::lastBlockMovement() const {
  std::lock_guard lock(m_mutex);
  return m_lastBlockMovement;
}

TapeSessionProgress TapeSessionTracker::progress() const {
  std::lock_guard lock(m_mutex);
  return {m_fileId, m_fSeq, m_fileBeingMoved, m_fileStartTime, m_bytesMoved, m_lastBlockMovement};
}

TapeSessionLivenessSnapshot TapeSessionTracker::livenessSnapshot() const {
  std::lock_guard lock(m_mutex);
  return {m_state, m_stateEnteredAt, m_lastBlockMovement};
}

std::chrono::steady_clock::time_point TapeSessionTracker::sessionStartTime() const {
  std::lock_guard lock(m_mutex);
  return m_sessionStartTime;
}

std::chrono::steady_clock::duration TapeSessionTracker::sessionElapsedTime() const {
  std::lock_guard lock(m_mutex);
  if (m_sessionStartTime == std::chrono::steady_clock::time_point {}) {
    return {};
  }
  return std::chrono::steady_clock::now() - m_sessionStartTime;
}

void TapeSessionTracker::notifyBeginNewJob(uint64_t fileId, uint64_t fSeq) {
  std::lock_guard lock(m_mutex);
  m_fileId = fileId;
  m_fSeq = fSeq;
  m_fileBeingMoved = true;
  m_fileStartTime = std::chrono::steady_clock::now();
}

void TapeSessionTracker::fileFinished() {
  std::lock_guard lock(m_mutex);
  m_fileBeingMoved = false;
  m_fileId = 0;
  m_fSeq = 0;
  m_fileStartTime = {};
}

bool TapeSessionTracker::hasFailuresLocked() const {
  return std::any_of(m_failureCounts.begin(), m_failureCounts.end(), [](auto count) { return count != 0; });
}

void TapeSessionTracker::recordFailureLocked(TapeSessionFailure failure) { ++m_failureCounts.at(static_cast<size_t>(failure)); }

bool TapeSessionTracker::setStateLocked(cta::tape::session::TapeSessionState state, Clock::time_point now) {
  if (m_state != state) {
    m_state = state;
    m_stateEnteredAt = now;
    return true;
  }
  return false;
}

std::optional<cta::tape::session::TapeSessionState> TapeSessionTracker::updateRetrievalCompletionState(Clock::time_point now) {
  using cta::tape::session::TapeSessionState;
  if (m_type == cta::tape::session::SessionType::Retrieve && m_tapeDone && m_state
      && m_state != TapeSessionState::Finished) {
    const auto state = m_diskDone ? TapeSessionState::Finalizing : TapeSessionState::DrainingToDisk;
    if (setStateLocked(state, now)) {
      return state;
    }
  }
  return std::nullopt;
}

void TapeSessionTracker::publishState(cta::tape::session::TapeSessionState state) {
  try {
    if (m_stateReporter) {
      m_stateReporter(state);
    }
  } catch (...) {
    recordFailure(TapeSessionFailure::Reporting);
  }
}

}  // namespace cta::tape::daemon
