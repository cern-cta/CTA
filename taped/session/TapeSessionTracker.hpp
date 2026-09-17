/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "scheduler/TapeMount.hpp"
#include "taped/session/SessionType.hpp"
#include "taped/session/TapeSessionState.hpp"
#include "taped/session/TapeSessionStats.hpp"

#include <chrono>
#include <cstdint>
#include <map>
#include <mutex>
#include <optional>
#include <string>
#include <utility>

namespace cta::tape::daemon {

enum class TapeSessionError {
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
  DiskSpaceReservationTestFailure,
  DiskSpaceReservationFailure,
  NoFilesToRecall,
  NoFilesToMigrate,
  EmptyMount,
  FileSkipped,
  TapeFilledUp
};

enum class TapeSessionOutcome { Automatic, Success, Failure };

using TapeSessionErrorStats = std::map<TapeSessionError, uint32_t>;
using TapeAlertStats = std::map<uint16_t, uint32_t>;

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

/**
 * @brief Track session state, statistics and progress shared by workers and the reporter.
 *
 * This class is only responsible for keeping track of the state; the TapeSessionReporter is responsible for
 * reporting said state.
 */
class TapeSessionTracker {
public:
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
  void beginTapeSession() {
    std::lock_guard lock(m_mutex);
    m_stats = {};
    m_errorStats.clear();
    m_tapeAlertStats.clear();
    m_activeDiskFiles.clear();
    m_outcome = TapeSessionOutcome::Automatic;
    m_mountAttempted = true;
    m_fileId = 0;
    m_fSeq = 0;
    m_fileBeingMoved = false;
    m_fileStartTime = {};
    m_bytesMoved = 0;
    m_lastBlockMovement = {};
    m_tapeDone = false;
    m_diskDone = false;
    m_sessionStartTime = std::chrono::steady_clock::now();
    m_state = cta::tape::session::TapeSessionState::Preparing;
    m_type = cta::tape::session::SessionType::Undetermined;
  }

  /**
   * @brief Set the session phase without resetting progress or statistics.
   *
   * @param state Session phase to record without resetting other tracking data.
   */
  void reportState(cta::tape::session::TapeSessionState state) {
    std::lock_guard lock(m_mutex);
    m_state = state;
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
  void notifyTapeDone() {
    std::lock_guard lock(m_mutex);
    m_tapeDone = true;
    updateRetrievalCompletionState();
  }

  /**
   * @brief Mark disk work complete and atomically update the retrieval phase when tape work is done.
   */
  void notifyDiskDone() {
    std::lock_guard lock(m_mutex);
    m_diskDone = true;
    updateRetrievalCompletionState();
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

  /**
   * @brief Set the explicit outcome without replacing an already recorded failure.
   *
   * @param outcome Requested outcome; an existing explicit failure is preserved.
   */
  void setOutcome(TapeSessionOutcome outcome) {
    std::lock_guard lock(m_mutex);
    // A later successful operation cannot hide an earlier explicit failure.
    if (m_outcome != TapeSessionOutcome::Failure) {
      m_outcome = outcome;
    }
  }

  /**
   * @brief Return the explicit outcome policy used by the reporter.
   *
   * @return Current explicit outcome or Automatic for error-based outcome reporting.
   */
  TapeSessionOutcome outcome() const {
    std::lock_guard lock(m_mutex);
    return m_outcome;
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

  /**
   * @brief Increment an error counter; reporting errors also force a failure outcome.
   *
   * @param error Session error category to count or name.
   */
  void incrementError(TapeSessionError error) {
    std::lock_guard lock(m_mutex);
    ++m_errorStats[error];
    if (error == TapeSessionError::Reporting) {
      m_outcome = TapeSessionOutcome::Failure;
    }
  }

  /**
   * @brief Replace an error count, removing zero counts.
   *
   * A nonzero reporting-error count forces failure; clearing it does not reset the outcome.
   *
   * @param error Session error category to count or name.
   * @param count Replacement error count; zero removes the counter without resetting a failure outcome.
   */
  void setErrorCount(TapeSessionError error, uint32_t count) {
    std::lock_guard lock(m_mutex);
    if (count == 0) {
      m_errorStats.erase(error);
    } else {
      m_errorStats[error] = count;
      if (error == TapeSessionError::Reporting) {
        m_outcome = TapeSessionOutcome::Failure;
      }
    }
  }

  /**
   * @brief Increment the occurrence count for a tape alert code.
   *
   * @param tapeAlertCode Tape alert code whose occurrence count is incremented.
   */
  void incrementTapeAlert(uint16_t tapeAlertCode) {
    std::lock_guard lock(m_mutex);
    ++m_tapeAlertStats[tapeAlertCode];
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
   * @brief Return a snapshot of the session error counters.
   *
   * @return Snapshot of session error counts.
   */
  TapeSessionErrorStats errorStats() const {
    std::lock_guard lock(m_mutex);
    return m_errorStats;
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
  void notifyBlockMovement(uint64_t bytes) {
    std::lock_guard lock(m_mutex);
    m_bytesMoved += bytes;
    m_lastBlockMovement = std::chrono::steady_clock::now();
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

  /**
   * @brief Return whether any error or tape alert counters remain recorded.
   *
   * @return True if any error or tape alert counters are present.
   */
  bool errorHappened() const {
    std::lock_guard lock(m_mutex);
    return !m_errorStats.empty() || !m_tapeAlertStats.empty();
  }

private:
  /**
   * @brief Advance retrieval to draining or finalizing once tape work has completed.
   *
   * @pre The caller holds m_mutex.
   * Only the session owner may establish Finished; this method preserves that state.
   */
  void updateRetrievalCompletionState() {
    using cta::tape::session::TapeSessionState;
    if (m_type == cta::tape::session::SessionType::Retrieve && m_tapeDone && m_state
        && m_state != TapeSessionState::Finished) {
      m_state = m_diskDone ? TapeSessionState::Finalizing : TapeSessionState::DrainingToDisk;
    }
  }

  mutable std::mutex m_mutex;
  // TODO: can we have something that is not a raw pointer here?
  cta::TapeMount* m_mount = nullptr;

  std::optional<cta::tape::session::TapeSessionState> m_state;
  bool m_tapeDone = false;
  bool m_diskDone = false;
  cta::tape::session::SessionType m_type = cta::tape::session::SessionType::Undetermined;
  TapeSessionOutcome m_outcome = TapeSessionOutcome::Automatic;
  bool m_mountAttempted = true;

  uint64_t m_fileId = 0;
  uint64_t m_fSeq = 0;
  bool m_fileBeingMoved = false;
  std::chrono::steady_clock::time_point m_fileStartTime;

  TapeSessionStats m_stats;
  TapeSessionErrorStats m_errorStats;
  TapeAlertStats m_tapeAlertStats;
  ActiveDiskFiles m_activeDiskFiles;

  uint64_t m_bytesMoved = 0;
  std::chrono::steady_clock::time_point m_lastBlockMovement;
  std::chrono::steady_clock::time_point m_sessionStartTime;
};

}  // namespace cta::tape::daemon
