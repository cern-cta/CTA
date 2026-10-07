/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "RecordedFailure.hpp"
#include "taped/session/SessionType.hpp"
#include "taped/session/TapeSessionState.hpp"
#include "taped/session/TapeSessionStats.hpp"

#include <algorithm>
#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <map>
#include <mutex>
#include <optional>
#include <string>
#include <utility>

namespace cta::tape::daemon {

/// Counted failures that affect session success.
enum class TapeSessionFailure {
  DiskOpenForWrite,
  DiskWrite,
  DiskCloseAfterWrite,
  DiskOpenForRead,
  DiskFileToReadSizeMismatch,
  DiskRead,
  DiskUnexpectedSizeWhenReading,
  TapeFSeqOutOfSequenceForWrite,
  TapeWriteHeader,
  TapeWriteData,
  TapeWriteTrailer,
  TapePositionForRead,
  TapeReadData,
  TapeUnload,
  TapeDismount,
  TapeMountForWrite,
  TapeMountForRead,
  TapeLoad,
  CheckingTapeAlert,
  TapeNotWriteable,
  TapeEncryptionEnable,
  TapeEncryptionDisable,
  TapeLbpDisable,
  TapePositionForWrite,
  TapeFlush,
  TapesCheckLabelBeforeReading,
  Reporting,
  FileNotArchived,
  UnexpectedSession,
  TaskInjection,
  WorkerSignalling,
  UnexpectedCleanup,
  UnclassifiedFile,
  Count
};

/// Informational stopping conditions independent of session success.
enum class TapeSessionEvent {
  DiskSpaceReservationTestFailure,
  DiskSpaceReservationFailure,
  NoFilesToRecall,
  NoFilesToMigrate,
  EmptyMount,
  TapeFilledUp,
  Count
};

using TapeSessionFailureStats = std::map<TapeSessionFailure, uint32_t>;
using TapeSessionFailureCounts = std::array<uint32_t, static_cast<size_t>(TapeSessionFailure::Count)>;
using TapeSessionEventCounts = std::array<uint32_t, static_cast<size_t>(TapeSessionEvent::Count)>;

using TapeAlertStats = std::map<uint16_t, uint32_t>;

/// Consistent snapshot of completion and diagnostics.
struct TapeSessionOutcomeSnapshot {
  // Derived from session state; failure presence is meaningful before completion too.
  bool finished;
  bool hasFailures;
  TapeSessionFailureCounts failures;
  TapeSessionEventCounts events;
  TapeAlertStats tapeAlerts;
};

/// Identity and opening time of an active disk file.
struct DiskFileProgress {
  uint64_t fileId = 0;
  std::string path;
  std::chrono::steady_clock::time_point openedAt;
};

using ActiveDiskFiles = std::map<uint32_t, DiskFileProgress>;

/// Snapshot of active tape-file and block-transfer progress.
struct TapeSessionProgress {
  uint64_t fileId = 0;
  uint64_t fSeq = 0;
  bool fileBeingMoved = false;
  std::chrono::steady_clock::time_point fileStartTime;
  uint64_t bytesMoved = 0;
  std::chrono::steady_clock::time_point lastBlockMovement;
};

/// Consistent phase and activity timestamps for health checks.
struct TapeSessionLivenessSnapshot {
  std::optional<cta::tape::session::TapeSessionState> state;
  std::chrono::steady_clock::time_point stateEnteredAt;
  std::chrono::steady_clock::time_point lastBlockMovement;
};

/// @brief Synchronize session progress, counters and statistics shared by workers and the reporter.
///
/// Each accessor returns a snapshot; separate accessor calls need not describe the same instant.
class TapeSessionTracker {
public:
  using Clock = std::chrono::steady_clock;
  using StateReporter = std::function<void(cta::tape::session::TapeSessionState)>;

  /// The synchronous reporter must not re-enter transitions; its dependencies must outlive all transitions.
  explicit TapeSessionTracker(StateReporter reporter = {}) : m_stateReporter(std::move(reporter)) {}

  /// @brief Reset tracking data and enter Preparing at now.
  ///
  /// Only the session owner may reset the tracker. The mount-attempt flag resets to true;
  /// call setMountAttempted(false) for an assignment that has not attempted mounting.
  void beginTapeSession(Clock::time_point now = Clock::now()) {
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

  /// @brief Set the phase without resetting progress; repeated reports preserve its entry time.
  /// @param state Phase to record.
  /// @param now Entry time used only when the phase changes.
  void reportState(cta::tape::session::TapeSessionState state, Clock::time_point now = Clock::now()) {
    std::lock_guard transitionLock(m_transitionMutex);
    {
      std::lock_guard lock(m_mutex);
      if (!setStateLocked(state, now)) {
        return;
      }
    }
    publishState(state);
  }

  /// Return the current phase, or std::nullopt before any phase is recorded.
  std::optional<cta::tape::session::TapeSessionState> state() const {
    std::lock_guard lock(m_mutex);
    return m_state;
  }

  /// Set the operation type used for reporting and retrieval completion.
  void setType(cta::tape::session::SessionType type) {
    std::lock_guard lock(m_mutex);
    m_type = type;
  }

  /// Mark tape work complete and update the retrieval phase using now.
  void notifyTapeDone(Clock::time_point now = Clock::now()) {
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

  /// Mark disk work complete and update the retrieval phase using now if tape work is done.
  void notifyDiskDone(Clock::time_point now = Clock::now()) {
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

  /// Return the recorded operation type, initially Undetermined.
  cta::tape::session::SessionType type() const {
    std::lock_guard lock(m_mutex);
    return m_type;
  }

  /// Snapshot completion, failures, events and tape alerts together.
  TapeSessionOutcomeSnapshot outcomeSnapshot() const {
    std::lock_guard lock(m_mutex);
    return {m_state == cta::tape::session::TapeSessionState::Finished,
            hasFailuresLocked(),
            m_failureCounts,
            m_eventCounts,
            m_tapeAlertStats};
  }

  /// Return whether any session failure has been recorded since reset.
  bool hasFailures() const {
    std::lock_guard lock(m_mutex);
    return hasFailuresLocked();
  }

  /// @brief Return whether recall should use its diagnostic end-report protocol.
  ///
  /// Includes events and tape alerts; this is distinct from session failure.
  bool recallCompletionHasDiagnostics() const {
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

  /// Record whether the assignment is attempting a physical tape mount.
  void setMountAttempted(bool attempted) {
    std::lock_guard lock(m_mutex);
    m_mountAttempted = attempted;
  }

  /// Return the recorded mount-attempt flag; true by default and after reset.
  bool mountAttempted() const {
    std::lock_guard lock(m_mutex);
    return m_mountAttempted;
  }

  /// Count a classified failure and return a receipt for propagation without recounting it.
  RecordedFailure recordFailure(TapeSessionFailure failure) {
    std::lock_guard lock(m_mutex);
    recordFailureLocked(failure);
    return RecordedFailure(*this);
  }

  /// Count the supplied fallback failure only if no session failure has been recorded.
  void recordFailureIfNone(TapeSessionFailure failure) {
    std::lock_guard lock(m_mutex);
    if (!hasFailuresLocked()) {
      recordFailureLocked(failure);
    }
  }

  /// @brief Count an informational event and mark recall diagnostics.
  ///
  /// TapeFilledUp is recorded at most once per session.
  void recordEvent(TapeSessionEvent event) {
    std::lock_guard lock(m_mutex);
    auto& count = m_eventCounts.at(static_cast<size_t>(event));
    if (event == TapeSessionEvent::TapeFilledUp) {
      count = 1;
    } else {
      ++count;
    }
  }

  /// Count a tape-alert code and mark recall diagnostics.
  void incrementTapeAlert(uint16_t tapeAlertCode) {
    std::lock_guard lock(m_mutex);
    ++m_tapeAlertStats[tapeAlertCode];
  }

  /// Replace setup statistics with the supplied snapshot.
  void updateTapeSetupStats(const TapeSetupStats& stats) {
    std::lock_guard lock(m_mutex);
    m_stats.setup = stats;
  }

  /// Accumulate setup statistics.
  void addTapeSetupStats(const TapeSetupStats& stats) {
    std::lock_guard lock(m_mutex);
    m_stats.setup.add(stats);
  }

  /// Replace tape-transfer statistics with the supplied snapshot.
  void updateTapeTransferStats(const TapeTransferStats& stats) {
    std::lock_guard lock(m_mutex);
    m_stats.tape = stats;
  }

  /// Accumulate tape-transfer statistics.
  void addTapeTransferStats(const TapeTransferStats& stats) {
    std::lock_guard lock(m_mutex);
    m_stats.tape.add(stats);
  }

  /// Replace disk statistics, including delivery time, with the supplied snapshot.
  void updateDiskTransferStats(const DiskTransferStats& stats) {
    std::lock_guard lock(m_mutex);
    m_stats.disk = stats;
  }

  /// Accumulate disk reporting wait time without changing delivery time.
  void addDiskTransferStats(const DiskTransferStats& stats) {
    std::lock_guard lock(m_mutex);
    m_stats.disk.add(stats);
  }

  /// Replace cleanup statistics with the supplied snapshot.
  void updateTapeCleanupStats(const TapeCleanupStats& stats) {
    std::lock_guard lock(m_mutex);
    m_stats.cleanup = stats;
  }

  /// Accumulate cleanup statistics, including retry durations.
  void addTapeCleanupStats(const TapeCleanupStats& stats) {
    std::lock_guard lock(m_mutex);
    m_stats.cleanup.add(stats);
  }

  /// Set the reported tape-worker elapsed time in seconds.
  void setTotalTime(double totalTime) {
    std::lock_guard lock(m_mutex);
    m_stats.totalTime = totalTime;
  }

  /// Set the elapsed time through disk delivery in seconds.
  void setDiskDeliveryTime(double deliveryTime) {
    std::lock_guard lock(m_mutex);
    m_stats.disk.deliveryTime = deliveryTime;
  }

  /// @brief Record an active file and its opening time for a disk worker, replacing its previous entry.
  /// @param threadId Disk worker identifier.
  /// @param fileId Archive-file identifier.
  /// @param path Disk path, moved into the tracker.
  void notifyDiskFileOpened(uint32_t threadId, uint64_t fileId, std::string path) {
    std::lock_guard lock(m_mutex);
    m_activeDiskFiles.insert_or_assign(threadId,
                                       DiskFileProgress {fileId, std::move(path), std::chrono::steady_clock::now()});
  }

  /// Remove the active-file entry for the specified disk worker.
  void notifyDiskFileClosed(uint32_t threadId) {
    std::lock_guard lock(m_mutex);
    m_activeDiskFiles.erase(threadId);
  }

  /// Return a snapshot of active disk files indexed by worker identifier.
  ActiveDiskFiles activeDiskFiles() const {
    std::lock_guard lock(m_mutex);
    return m_activeDiskFiles;
  }

  /// Return nonzero failure counters indexed by failure category.
  TapeSessionFailureStats failureStats() const {
    std::lock_guard lock(m_mutex);
    TapeSessionFailureStats result;
    for (size_t i = 0; i < m_failureCounts.size(); ++i) {
      if (m_failureCounts[i]) {
        result.emplace(static_cast<TapeSessionFailure>(i), m_failureCounts[i]);
      }
    }
    return result;
  }

  /// Return a snapshot of tape-alert counts.
  TapeAlertStats tapeAlertStats() const {
    std::lock_guard lock(m_mutex);
    return m_tapeAlertStats;
  }

  /// Return one consistent snapshot of all session statistics.
  TapeSessionStats stats() const {
    std::lock_guard lock(m_mutex);
    return m_stats;
  }

  /// Accumulate moved bytes and record now as the latest tape-block activity time.
  void notifyBlockMovement(uint64_t bytes, Clock::time_point now = Clock::now()) {
    std::lock_guard lock(m_mutex);
    m_bytesMoved += bytes;
    m_lastBlockMovement = now;
  }

  /// Return bytes accumulated through block-movement notifications since reset.
  uint64_t bytesMoved() const {
    std::lock_guard lock(m_mutex);
    return m_bytesMoved;
  }

  /// Return the last block-movement time, or a default time point if none was recorded.
  std::chrono::steady_clock::time_point lastBlockMovement() const {
    std::lock_guard lock(m_mutex);
    return m_lastBlockMovement;
  }

  /// Snapshot the active tape file and block-movement progress together.
  TapeSessionProgress progress() const {
    std::lock_guard lock(m_mutex);
    return {m_fileId, m_fSeq, m_fileBeingMoved, m_fileStartTime, m_bytesMoved, m_lastBlockMovement};
  }

  /// Snapshot the phase, its entry time and last block-movement time together.
  TapeSessionLivenessSnapshot livenessSnapshot() const {
    std::lock_guard lock(m_mutex);
    return {m_state, m_stateEnteredAt, m_lastBlockMovement};
  }

  /// Return the time recorded by beginTapeSession(), or a default time point before it is called.
  std::chrono::steady_clock::time_point sessionStartTime() const {
    std::lock_guard lock(m_mutex);
    return m_sessionStartTime;
  }

  /// Return elapsed time since beginTapeSession(), or zero before it is called.
  std::chrono::steady_clock::duration sessionElapsedTime() const {
    std::lock_guard lock(m_mutex);
    if (m_sessionStartTime == std::chrono::steady_clock::time_point {}) {
      return {};
    }
    return std::chrono::steady_clock::now() - m_sessionStartTime;
  }

  /// Record the active archive-file ID, tape sequence number and current start time.
  void notifyBeginNewJob(uint64_t fileId, uint64_t fSeq) {
    std::lock_guard lock(m_mutex);
    m_fileId = fileId;
    m_fSeq = fSeq;
    m_fileBeingMoved = true;
    m_fileStartTime = std::chrono::steady_clock::now();
  }

  /// Clear the active tape-file identity and start time without resetting block progress.
  void fileFinished() {
    std::lock_guard lock(m_mutex);
    m_fileBeingMoved = false;
    m_fileId = 0;
    m_fSeq = 0;
    m_fileStartTime = {};
  }

private:
  /// @brief Return whether any failure counter is nonzero.
  /// @pre The caller holds m_mutex.
  bool hasFailuresLocked() const {
    return std::any_of(m_failureCounts.begin(), m_failureCounts.end(), [](auto count) { return count != 0; });
  }

  /// @brief Increment the supplied failure counter.
  /// @pre The caller holds m_mutex.
  void recordFailureLocked(TapeSessionFailure failure) { ++m_failureCounts.at(static_cast<size_t>(failure)); }

  /// @brief Change the phase and entry time only when the phase differs.
  /// @pre The caller holds m_mutex.
  bool setStateLocked(cta::tape::session::TapeSessionState state, Clock::time_point now) {
    if (m_state != state) {
      m_state = state;
      m_stateEnteredAt = now;
      return true;
    }
    return false;
  }

  /// @brief Advance retrieval to draining or finalizing once tape work is complete.
  ///
  /// Preserves Finished, which is established by the session owner.
  /// @pre The caller holds m_mutex.
  std::optional<cta::tape::session::TapeSessionState> updateRetrievalCompletionState(Clock::time_point now) {
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

  /// Publish after committing local progress; catalogue failures must not interrupt hardware cleanup.
  void publishState(cta::tape::session::TapeSessionState state) {
    try {
      if (m_stateReporter) {
        m_stateReporter(state);
      }
    } catch (...) {
      recordFailure(TapeSessionFailure::Reporting);
    }
  }

  const StateReporter m_stateReporter;
  std::mutex m_transitionMutex;
  mutable std::mutex m_mutex;

  std::optional<cta::tape::session::TapeSessionState> m_state;
  Clock::time_point m_stateEnteredAt;
  bool m_tapeDone = false;
  bool m_diskDone = false;
  cta::tape::session::SessionType m_type = cta::tape::session::SessionType::Undetermined;
  bool m_mountAttempted = true;

  uint64_t m_fileId = 0;
  uint64_t m_fSeq = 0;
  bool m_fileBeingMoved = false;
  std::chrono::steady_clock::time_point m_fileStartTime;

  TapeSessionStats m_stats;
  TapeSessionFailureCounts m_failureCounts {};
  TapeSessionEventCounts m_eventCounts {};
  TapeAlertStats m_tapeAlertStats;
  ActiveDiskFiles m_activeDiskFiles;

  uint64_t m_bytesMoved = 0;
  std::chrono::steady_clock::time_point m_lastBlockMovement;
  std::chrono::steady_clock::time_point m_sessionStartTime;
};

}  // namespace cta::tape::daemon
