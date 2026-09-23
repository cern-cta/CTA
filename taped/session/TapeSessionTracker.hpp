/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "RecordedFailure.hpp"
#include "scheduler/TapeMount.hpp"
#include "taped/session/SessionType.hpp"
#include "taped/session/TapeSessionState.hpp"
#include "taped/session/TapeSessionStats.hpp"

#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <map>
#include <mutex>
#include <optional>
#include <string>
#include <utility>

namespace cta::tape::daemon {

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

// Normal stopping conditions are diagnostics, not session failures.
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

struct TapeSessionOutcomeSnapshot {
  // Derived from session state; failure presence is meaningful before completion too.
  bool finished;
  bool hasFailures;
  TapeSessionFailureCounts failures;
  TapeSessionEventCounts events;
  TapeAlertStats tapeAlerts;
};

struct DiskFileProgress {
  uint64_t fileId = 0;
  std::string path;
  std::chrono::steady_clock::time_point openedAt;
};

using ActiveDiskFiles = std::map<uint32_t, DiskFileProgress>;

struct TapeSessionProgress {
  uint64_t fileId = 0;
  uint64_t fSeq = 0;
  bool fileBeingMoved = false;
  std::chrono::steady_clock::time_point fileStartTime;
  uint64_t bytesMoved = 0;
  std::chrono::steady_clock::time_point lastBlockMovement;
};

// A consistent, mount-independent view for health checks.
struct TapeSessionLivenessSnapshot {
  std::optional<cta::tape::session::TapeSessionState> state;
  std::chrono::steady_clock::time_point stateEnteredAt;
  std::chrono::steady_clock::time_point lastBlockMovement;
};

/**
 * @brief Track session state, statistics and progress shared by workers and the reporter.
 *
 * This class is only responsible for keeping track of the state; the TapeSessionReporter is responsible for
 * reporting said state.
 */
class TapeSessionTracker {
public:
  using Clock = std::chrono::steady_clock;

  /**
   * @brief Attach a borrowed mount; the owner must keep it alive while workers or the reporter use it.
   *
   * @param mount Borrowed scheduler mount; the caller must keep it alive while it is used.
   */
  void setMount(cta::TapeMount* mount) {
    std::lock_guard lock(m_mutex);
    m_mount = mount;
  }

  /**
   * @brief Return the borrowed mount pointer; the caller must ensure its lifetime.
   *
   * @return Borrowed mount pointer currently attached to the tracker, possibly nullptr.
   */
  cta::TapeMount* mount() const {
    std::lock_guard lock(m_mutex);
    return m_mount;
  }

  /**
   * @brief Reset progress, statistics and completion flags for a new session, retaining the attached mount.
   *
   * Only the session owner may begin a session; subsequent state changes do not reset its data.
   */
  void beginTapeSession(Clock::time_point now = Clock::now()) {
    std::lock_guard lock(m_mutex);
    m_stats = {};
    m_failureCounts.fill(0);
    m_eventCounts.fill(0);
    m_hasFailures = false;
    m_recallCompletionHasDiagnostics = false;
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

  /**
   * @brief Set the session phase without resetting progress or statistics.
   *
   * @param state Session phase to record without resetting other tracking data.
   */
  void reportState(cta::tape::session::TapeSessionState state, Clock::time_point now = Clock::now()) {
    std::lock_guard lock(m_mutex);
    setStateLocked(state, now);
  }

  /**
   * @brief Return the current phase, or std::nullopt before any phase has been established.
   *
   * @return Current phase, or std::nullopt if no phase has been established.
   */
  std::optional<cta::tape::session::TapeSessionState> state() const {
    std::lock_guard lock(m_mutex);
    return m_state;
  }

  /**
   * @brief Set the session operation type used for reporting and retrieval completion.
   *
   * @param type Session operation type used for reporting and retrieval completion.
   */
  void setType(cta::tape::session::SessionType type) {
    std::lock_guard lock(m_mutex);
    m_type = type;
  }

  /**
   * @brief Mark tape work complete and atomically update the retrieval draining or finalizing phase.
   */
  void notifyTapeDone(Clock::time_point now = Clock::now()) {
    std::lock_guard lock(m_mutex);
    m_tapeDone = true;
    updateRetrievalCompletionState(now);
  }

  /**
   * @brief Mark disk work complete and atomically update the retrieval phase when tape work is done.
   */
  void notifyDiskDone(Clock::time_point now = Clock::now()) {
    std::lock_guard lock(m_mutex);
    m_diskDone = true;
    updateRetrievalCompletionState(now);
  }

  /**
   * @brief Return the current session operation type.
   *
   * @return Current session operation type.
   */
  cta::tape::session::SessionType type() const {
    std::lock_guard lock(m_mutex);
    return m_type;
  }

  /** Snapshot completion and persistent failures together; only finished sessions have a final log status. */
  TapeSessionOutcomeSnapshot outcomeSnapshot() const {
    std::lock_guard lock(m_mutex);
    return {m_state == cta::tape::session::TapeSessionState::Finished,
            m_hasFailures,
            m_failureCounts,
            m_eventCounts,
            m_tapeAlertStats};
  }

  bool hasFailures() const {
    std::lock_guard lock(m_mutex);
    return m_hasFailures;
  }

  /** Preserve recall's existing end-report selection, including informational diagnostics and alerts. */
  bool recallCompletionHasDiagnostics() const {
    std::lock_guard lock(m_mutex);
    return m_recallCompletionHasDiagnostics;
  }

  /**
   * @brief Record whether the session attempted a physical tape mount.
   *
   * @param attempted Whether a physical mount was attempted.
   */
  void setMountAttempted(bool attempted) {
    std::lock_guard lock(m_mutex);
    m_mountAttempted = attempted;
  }

  /**
   * @brief Return whether the session attempted a physical tape mount.
   *
   * @return True if a physical mount attempt is recorded.
   */
  bool mountAttempted() const {
    std::lock_guard lock(m_mutex);
    return m_mountAttempted;
  }

  /** Count the owning operation's failure and return a receipt for propagation, without choosing recovery. */
  RecordedFailure recordFailure(TapeSessionFailure failure) {
    std::lock_guard lock(m_mutex);
    recordFailureLocked(failure);
    return RecordedFailure(*this, failure);
  }

  /** Propagated errors need a fallback reason only when no operation has classified a failure. */
  void recordFailureIfNone(TapeSessionFailure failure) {
    std::lock_guard lock(m_mutex);
    if (!m_hasFailures) {
      recordFailureLocked(failure);
    }
  }

  void recordEvent(TapeSessionEvent event) {
    std::lock_guard lock(m_mutex);
    auto& count = m_eventCounts.at(static_cast<size_t>(event));
    if (event == TapeSessionEvent::TapeFilledUp) {
      count = 1;
    } else {
      ++count;
    }
    m_recallCompletionHasDiagnostics = true;
  }

  /**
   * @brief Increment the occurrence count for a tape alert code.
   *
   * @param tapeAlertCode Tape alert code whose occurrence count is incremented.
   */
  void incrementTapeAlert(uint16_t tapeAlertCode) {
    std::lock_guard lock(m_mutex);
    ++m_tapeAlertStats[tapeAlertCode];
    m_recallCompletionHasDiagnostics = true;
  }

  /**
   * @brief Replace the tape setup statistics with the supplied snapshot.
   *
   * @param stats Statistics to store or accumulate.
   */
  void updateTapeSetupStats(const TapeSetupStats& stats) {
    std::lock_guard lock(m_mutex);
    m_stats.setup = stats;
  }

  /**
   * @brief Accumulate the supplied tape setup statistics.
   *
   * @param stats Statistics to store or accumulate.
   */
  void addTapeSetupStats(const TapeSetupStats& stats) {
    std::lock_guard lock(m_mutex);
    m_stats.setup.add(stats);
  }

  /**
   * @brief Replace the tape transfer statistics with the supplied snapshot.
   *
   * @param stats Statistics to store or accumulate.
   */
  void updateTapeTransferStats(const TapeTransferStats& stats) {
    std::lock_guard lock(m_mutex);
    m_stats.tape = stats;
  }

  /**
   * @brief Accumulate the supplied tape transfer statistics.
   *
   * @param stats Statistics to store or accumulate.
   */
  void addTapeTransferStats(const TapeTransferStats& stats) {
    std::lock_guard lock(m_mutex);
    m_stats.tape.add(stats);
  }

  /**
   * @brief Replace the disk transfer statistics with the supplied snapshot.
   *
   * @param stats Statistics to store or accumulate.
   */
  void updateDiskTransferStats(const DiskTransferStats& stats) {
    std::lock_guard lock(m_mutex);
    m_stats.disk = stats;
  }

  /**
   * @brief Accumulate the supplied disk transfer statistics.
   *
   * @param stats Statistics to store or accumulate.
   */
  void addDiskTransferStats(const DiskTransferStats& stats) {
    std::lock_guard lock(m_mutex);
    m_stats.disk.add(stats);
  }

  /**
   * @brief Replace the cleanup statistics with the supplied snapshot.
   *
   * @param stats Statistics to store or accumulate.
   */
  void updateTapeCleanupStats(const TapeCleanupStats& stats) {
    std::lock_guard lock(m_mutex);
    m_stats.cleanup = stats;
  }

  /**
   * @brief Accumulate the supplied cleanup statistics.
   *
   * @param stats Statistics to store or accumulate.
   */
  void addTapeCleanupStats(const TapeCleanupStats& stats) {
    std::lock_guard lock(m_mutex);
    m_stats.cleanup.add(stats);
  }

  /**
   * @brief Set the total session time in seconds.
   *
   * @param totalTime Total session duration in seconds.
   */
  void setTotalTime(double totalTime) {
    std::lock_guard lock(m_mutex);
    m_stats.totalTime = totalTime;
  }

  /**
   * @brief Set the disk delivery time in seconds.
   *
   * @param deliveryTime Disk delivery duration in seconds.
   */
  void setDiskDeliveryTime(double deliveryTime) {
    std::lock_guard lock(m_mutex);
    m_stats.disk.deliveryTime = deliveryTime;
  }

  /**
   * @brief Record the active file and opening time for a disk worker, replacing its previous entry.
   *
   * @param threadId Identifier of the disk worker owning the active-file entry.
   * @param fileId Archive-file identifier of the file being processed.
   * @param path Disk path of the active file, moved into the tracker.
   */
  void notifyDiskFileOpened(uint32_t threadId, uint64_t fileId, std::string path) {
    std::lock_guard lock(m_mutex);
    m_activeDiskFiles.insert_or_assign(threadId,
                                       DiskFileProgress {fileId, std::move(path), std::chrono::steady_clock::now()});
  }

  /**
   * @brief Remove the active-file entry for the specified disk worker.
   *
   * @param threadId Identifier of the disk worker owning the active-file entry.
   */
  void notifyDiskFileClosed(uint32_t threadId) {
    std::lock_guard lock(m_mutex);
    m_activeDiskFiles.erase(threadId);
  }

  /**
   * @brief Return a snapshot of files currently open in disk workers.
   *
   * @return Snapshot of active disk files indexed by worker ID.
   */
  ActiveDiskFiles activeDiskFiles() const {
    std::lock_guard lock(m_mutex);
    return m_activeDiskFiles;
  }

  /**
   * @brief Return a snapshot of the session failure counters.
   *
   * @return Snapshot of session failure counts.
   */
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

  /**
   * @brief Return a snapshot of the tape alert counters.
   *
   * @return Snapshot of tape alert counts.
   */
  TapeAlertStats tapeAlertStats() const {
    std::lock_guard lock(m_mutex);
    return m_tapeAlertStats;
  }

  /**
   * @brief Return a snapshot of the accumulated session statistics.
   *
   * @return Snapshot of the session statistics.
   */
  TapeSessionStats stats() const {
    std::lock_guard lock(m_mutex);
    return m_stats;
  }

  /**
   * @brief Add transferred bytes and refresh the last tape-block activity timestamp.
   *
   * @param bytes Additional transferred bytes to accumulate.
   */
  void notifyBlockMovement(uint64_t bytes, Clock::time_point now = Clock::now()) {
    std::lock_guard lock(m_mutex);
    m_bytesMoved += bytes;
    m_lastBlockMovement = now;
  }

  /**
   * @brief Return the cumulative bytes recorded through block-movement notifications.
   *
   * @return Cumulative transferred bytes recorded since the session began.
   */
  uint64_t bytesMoved() const {
    std::lock_guard lock(m_mutex);
    return m_bytesMoved;
  }

  /**
   * @brief Return the last block-movement timestamp, or a default timestamp when none was recorded.
   *
   * @return Most recent block-movement timestamp, or a default timestamp if none was recorded.
   */
  std::chrono::steady_clock::time_point lastBlockMovement() const {
    std::lock_guard lock(m_mutex);
    return m_lastBlockMovement;
  }

  /**
   * @brief Return one consistent snapshot of the current tape file and block-movement progress.
   *
   * @return Consistent snapshot of the active tape file and block-movement progress.
   */
  TapeSessionProgress progress() const {
    std::lock_guard lock(m_mutex);
    return {m_fileId, m_fSeq, m_fileBeingMoved, m_fileStartTime, m_bytesMoved, m_lastBlockMovement};
  }

  /** Return phase and progress timestamps together without accessing the borrowed mount. */
  TapeSessionLivenessSnapshot livenessSnapshot() const {
    std::lock_guard lock(m_mutex);
    return {m_state, m_stateEnteredAt, m_lastBlockMovement};
  }

  /**
   * @brief Return the time recorded when the session began.
   *
   * @return Session start timestamp, or a default timestamp before a session begins.
   */
  std::chrono::steady_clock::time_point sessionStartTime() const {
    std::lock_guard lock(m_mutex);
    return m_sessionStartTime;
  }

  /**
   * @brief Return time since the session began, or zero before beginTapeSession().
   *
   * @return Elapsed session duration, or zero before beginTapeSession().
   */
  std::chrono::steady_clock::duration sessionElapsedTime() const {
    std::lock_guard lock(m_mutex);
    if (m_sessionStartTime == std::chrono::steady_clock::time_point {}) {
      return {};
    }
    return std::chrono::steady_clock::now() - m_sessionStartTime;
  }

  /**
   * @brief Record the active archive-file ID, tape sequence number and file start time.
   *
   * @param fileId Archive-file identifier of the file being processed.
   * @param fSeq Tape file sequence number of the active job.
   */
  void notifyBeginNewJob(uint64_t fileId, uint64_t fSeq) {
    std::lock_guard lock(m_mutex);
    m_fileId = fileId;
    m_fSeq = fSeq;
    m_fileBeingMoved = true;
    m_fileStartTime = std::chrono::steady_clock::now();
  }

  /**
   * @brief Notify the tracker we have finished operating on the current file.
   */
  void fileFinished() {
    std::lock_guard lock(m_mutex);
    m_fileBeingMoved = false;
    m_fileId = 0;
    m_fSeq = 0;
    m_fileStartTime = {};
  }

private:
  void recordFailureLocked(TapeSessionFailure failure) {
    ++m_failureCounts.at(static_cast<size_t>(failure));
    m_hasFailures = true;
    // Newly classified propagation failures must not change the legacy recall end-report protocol.
    switch (failure) {
      case TapeSessionFailure::UnexpectedSession:
      case TapeSessionFailure::TaskInjection:
      case TapeSessionFailure::WorkerSignalling:
      case TapeSessionFailure::UnexpectedCleanup:
      case TapeSessionFailure::UnclassifiedFile:
        break;
      default:
        m_recallCompletionHasDiagnostics = true;
    }
  }

  // The caller holds m_mutex. Repeated reports must not postpone the phase deadline.
  void setStateLocked(cta::tape::session::TapeSessionState state, Clock::time_point now) {
    if (m_state != state) {
      m_state = state;
      m_stateEnteredAt = now;
    }
  }

  /**
   * @brief Advance retrieval to draining or finalizing once tape work has completed.
   *
   * @pre The caller holds m_mutex.
   * Only the session owner may establish Finished; this method preserves that state.
   */
  void updateRetrievalCompletionState(Clock::time_point now) {
    using cta::tape::session::TapeSessionState;
    if (m_type == cta::tape::session::SessionType::Retrieve && m_tapeDone && m_state
        && m_state != TapeSessionState::Finished) {
      setStateLocked(m_diskDone ? TapeSessionState::Finalizing : TapeSessionState::DrainingToDisk, now);
    }
  }

  mutable std::mutex m_mutex;
  // TODO: can we have something that is not a raw pointer here?
  cta::TapeMount* m_mount = nullptr;

  std::optional<cta::tape::session::TapeSessionState> m_state;
  Clock::time_point m_stateEnteredAt;
  bool m_tapeDone = false;
  bool m_diskDone = false;
  cta::tape::session::SessionType m_type = cta::tape::session::SessionType::Undetermined;
  bool m_hasFailures = false;
  bool m_recallCompletionHasDiagnostics = false;
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
