/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "RecordedFailure.hpp"
#include "taped/session/SessionType.hpp"
#include "taped/session/TapeSessionState.hpp"
#include "taped/session/TapeSessionStats.hpp"

#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <map>
#include <mutex>
#include <optional>
#include <string>

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
/// Store each phase change before calling the optional notification callback on the calling thread.
/// Serialize phase changes so callbacks run in transition order.
/// Each accessor returns a snapshot; separate accessor calls need not describe the same instant.
class TapeSessionTracker {
public:
  using Clock = std::chrono::steady_clock;
  using StatePublisher = std::function<void(cta::tape::session::TapeSessionState)>;

  /// @brief Create an empty tracker with an optional callback for phase changes.
  ///
  /// The callback may read snapshots but must not call methods that change or reset the phase.
  /// Objects used by the callback must outlive all phase changes; callback exceptions count as Reporting failures.
  explicit TapeSessionTracker(StatePublisher publisher = {});

  /// @brief Reset tracking data and enter Preparing at now.
  ///
  /// Only the session owner may reset the tracker. The mount-attempt flag resets to true;
  /// call setMountAttempted(false) for an assignment that has not attempted mounting.
  void beginTapeSession(Clock::time_point now = Clock::now());

  /// @brief Set the phase without resetting progress; repeated reports preserve its entry time.
  /// Repeated states neither republish nor retry failed publication.
  /// @param state Phase to record.
  /// @param now Entry time used only when the phase changes.
  void reportState(cta::tape::session::TapeSessionState state, Clock::time_point now = Clock::now());

  /// Return the current phase, or std::nullopt before any phase is recorded.
  std::optional<cta::tape::session::TapeSessionState> state() const;

  /// Set the operation type used for reporting and retrieval completion.
  void setType(cta::tape::session::SessionType type);

  /// Mark tape work complete and update the retrieval phase using now.
  void notifyTapeDone(Clock::time_point now = Clock::now());

  /// Mark disk work complete and update the retrieval phase using now if tape work is done.
  void notifyDiskDone(Clock::time_point now = Clock::now());

  /// Return the recorded operation type, initially Undetermined.
  cta::tape::session::SessionType type() const;

  /// Snapshot completion, failures, events and tape alerts together.
  TapeSessionOutcomeSnapshot outcomeSnapshot() const;

  /// Return whether any session failure has been recorded since reset.
  bool hasFailures() const;

  /// Record whether the assignment is attempting a physical tape mount.
  void setMountAttempted(bool attempted);

  /// Return the recorded mount-attempt flag; true by default and after reset.
  bool mountAttempted() const;

  /// Count a classified failure and return a receipt for propagation without recounting it.
  RecordedFailure recordFailure(TapeSessionFailure failure);

  /// Count the supplied fallback failure only if no session failure has been recorded.
  void recordFailureIfNone(TapeSessionFailure failure);

  /// @brief Count an informational event.
  ///
  /// TapeFilledUp is recorded at most once per session.
  void recordEvent(TapeSessionEvent event);

  /// Count a tape-alert code.
  void incrementTapeAlert(uint16_t tapeAlertCode);

  /// Replace setup statistics with the supplied snapshot.
  void updateTapeSetupStats(const TapeSetupStats& stats);

  /// Accumulate setup statistics.
  void addTapeSetupStats(const TapeSetupStats& stats);

  /// Replace tape-transfer statistics with the supplied snapshot.
  void updateTapeTransferStats(const TapeTransferStats& stats);

  /// Accumulate tape-transfer statistics.
  void addTapeTransferStats(const TapeTransferStats& stats);

  /// Replace disk statistics, including delivery time, with the supplied snapshot.
  void updateDiskTransferStats(const DiskTransferStats& stats);

  /// Accumulate disk reporting wait time without changing delivery time.
  void addDiskTransferStats(const DiskTransferStats& stats);

  /// Replace cleanup statistics with the supplied snapshot.
  void updateTapeCleanupStats(const TapeCleanupStats& stats);

  /// Accumulate cleanup statistics, including retry durations.
  void addTapeCleanupStats(const TapeCleanupStats& stats);

  /// Set the reported tape-worker elapsed time in seconds.
  void setTotalTime(double totalTime);

  /// Set the elapsed time through disk delivery in seconds.
  void setDiskDeliveryTime(double deliveryTime);

  /// @brief Record a disk worker's active file and opening time, replacing its previous entry.
  ///
  /// The path is moved into the tracker.
  void notifyDiskFileOpened(uint32_t threadId, uint64_t fileId, std::string path);

  /// Remove the active-file entry for the specified disk worker.
  void notifyDiskFileClosed(uint32_t threadId);

  /// Return a snapshot of active disk files indexed by worker identifier.
  ActiveDiskFiles activeDiskFiles() const;

  /// Return one consistent snapshot of all session statistics.
  TapeSessionStats stats() const;

  /// Accumulate moved bytes and record now as the latest tape-block activity time.
  void notifyBlockMovement(uint64_t bytes, Clock::time_point now = Clock::now());

  /// Return bytes accumulated through block-movement notifications since reset.
  uint64_t bytesMoved() const;

  /// Snapshot the active tape file and block-movement progress together.
  TapeSessionProgress progress() const;

  /// Snapshot the phase, its entry time and last block-movement time together.
  TapeSessionLivenessSnapshot livenessSnapshot() const;

  /// Return the time recorded by beginTapeSession(), or a default time point before it is called.
  std::chrono::steady_clock::time_point sessionStartTime() const;

  /// Return elapsed time since beginTapeSession(), or zero before it is called.
  std::chrono::steady_clock::duration sessionElapsedTime() const;

  /// Record the active archive-file ID, tape sequence number and current start time.
  void notifyBeginNewJob(uint64_t fileId, uint64_t fSeq);

  /// Clear the active tape-file identity and start time without resetting block progress.
  void fileFinished();

private:
  /// @brief Return whether any failure counter is nonzero.
  /// @pre The caller holds m_mutex.
  bool hasFailuresLocked() const;

  /// @brief Increment the supplied failure counter.
  /// @pre The caller holds m_mutex.
  void recordFailureLocked(TapeSessionFailure failure);

  /// @brief Change the phase and entry time, returning true only when the phase differs.
  /// @pre The caller holds m_mutex.
  bool setStateLocked(cta::tape::session::TapeSessionState state, Clock::time_point now);

  /// @brief Advance retrieval to draining or finalizing once tape work is complete.
  ///
  /// Preserves Finished, which is established by the session owner.
  /// @pre The caller holds m_mutex.
  std::optional<cta::tape::session::TapeSessionState> updateRetrievalCompletionState(Clock::time_point now);

  /// @brief Publish committed state, counting callback exceptions as Reporting failures.
  /// @pre The caller holds m_transitionMutex and has released m_mutex.
  void publishState(cta::tape::session::TapeSessionState state);

  const StatePublisher m_statePublisher;
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
