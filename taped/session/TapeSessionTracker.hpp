/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "taped/session/SessionState.hpp"
#include "taped/session/SessionType.hpp"
#include "taped/session/TapeSessionStats.hpp"

#include <chrono>
#include <cstdint>
#include <map>
#include <mutex>

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

// TODO: unordered map?
using TapeSessionErrorStats = std::map<TapeSessionError, uint32_t>;
using TapeAlertStats = std::map<uint16_t, uint32_t>;

struct TapeSessionProgress {
  uint64_t fileId = 0;
  uint64_t fSeq = 0;
  bool fileBeingMoved = false;
  std::chrono::steady_clock::time_point fileStartTime;
  uint64_t bytesMoved = 0;
  std::chrono::steady_clock::time_point lastBlockMovement;
};

class TapeSessionTracker {
public:
  void reportState(cta::tape::session::SessionState state, cta::tape::session::SessionType type) {
    std::lock_guard lock(m_mutex);

    // TODO: for now the transition to scheduler clears the stats, but we may want to update the state
    if (state == cta::tape::session::SessionState::Scheduling && m_state != state) {
      m_tapeStats = {};
      m_diskStats = {};
      m_errorStats.clear();
      m_tapeAlertStats.clear();
      m_fileId = 0;
      m_fSeq = 0;
      m_fileBeingMoved = false;
      m_fileStartTime = {};
      m_bytesMoved = 0;
      m_lastBlockMovement = {};
      m_sessionStartTime = std::chrono::steady_clock::now();
    }

    m_state = state;
    m_type = type;
  }

  cta::tape::session::SessionState state() const {
    std::lock_guard lock(m_mutex);
    return m_state;
  }

  cta::tape::session::SessionType type() const {
    std::lock_guard lock(m_mutex);
    return m_type;
  }

  void incrementError(TapeSessionError error) {
    std::lock_guard lock(m_mutex);
    ++m_errorStats[error];
  }

  void setErrorCount(TapeSessionError error, uint32_t count) {
    std::lock_guard lock(m_mutex);
    if (count == 0) {
      m_errorStats.erase(error);
    } else {
      m_errorStats[error] = count;
    }
  }

  void incrementTapeAlert(uint16_t tapeAlertCode) {
    std::lock_guard lock(m_mutex);
    ++m_tapeAlertStats[tapeAlertCode];
  }

  void updateTapeStats(const TapeSideStats& stats) {
    std::lock_guard lock(m_mutex);
    m_tapeStats = stats;
  }

  void addTapeStats(const TapeSideStats& stats) {
    std::lock_guard lock(m_mutex);
    m_tapeStats.add(stats);
  }

  void updateDiskStats(const DiskSideStats& stats) {
    std::lock_guard lock(m_mutex);
    m_diskStats = stats;
  }

  void addDiskStats(const DiskSideStats& stats) {
    std::lock_guard lock(m_mutex);
    m_diskStats.add(stats);
  }

  TapeSessionErrorStats errorStats() const {
    std::lock_guard lock(m_mutex);
    return m_errorStats;
  }

  TapeAlertStats tapeAlertStats() const {
    std::lock_guard lock(m_mutex);
    return m_tapeAlertStats;
  }

  TapeSideStats tapeStats() const {
    std::lock_guard lock(m_mutex);
    return m_tapeStats;
  }

  DiskSideStats diskStats() const {
    std::lock_guard lock(m_mutex);
    return m_diskStats;
  }

  void notifyBlockMovement(uint64_t bytes) {
    std::lock_guard lock(m_mutex);
    m_bytesMoved += bytes;
    m_lastBlockMovement = std::chrono::steady_clock::now();
  }

  uint64_t bytesMoved() const {
    std::lock_guard lock(m_mutex);
    return m_bytesMoved;
  }

  std::chrono::steady_clock::time_point lastBlockMovement() const {
    std::lock_guard lock(m_mutex);
    return m_lastBlockMovement;
  }

  TapeSessionProgress progress() const {
    std::lock_guard lock(m_mutex);
    return {m_fileId, m_fSeq, m_fileBeingMoved, m_fileStartTime, m_bytesMoved, m_lastBlockMovement};
  }

  std::chrono::steady_clock::time_point sessionStartTime() const {
    std::lock_guard lock(m_mutex);
    return m_sessionStartTime;
  }

  std::chrono::steady_clock::duration sessionElapsedTime() const {
    std::lock_guard lock(m_mutex);
    if (m_sessionStartTime == std::chrono::steady_clock::time_point {}) {
      return {};
    }
    return std::chrono::steady_clock::now() - m_sessionStartTime;
  }

  void notifyBeginNewJob(uint64_t fileId, uint64_t fSeq) {
    std::lock_guard lock(m_mutex);
    m_fileId = fileId;
    m_fSeq = fSeq;
    m_fileBeingMoved = true;
    m_fileStartTime = std::chrono::steady_clock::now();
  }

  /**
   * Notify the tracker we have finished operating on the current file.
   */
  void fileFinished() {
    std::lock_guard lock(m_mutex);
    m_fileBeingMoved = false;
    m_fileId = 0;
    m_fSeq = 0;
    m_fileStartTime = {};
  }

  bool errorHappened() const {
    std::lock_guard lock(m_mutex);
    return !m_errorStats.empty() || !m_tapeAlertStats.empty();
  }

private:
  mutable std::mutex m_mutex;

  cta::tape::session::SessionState m_state = cta::tape::session::SessionState::StartingUp;
  cta::tape::session::SessionType m_type = cta::tape::session::SessionType::Undetermined;

  uint64_t m_fileId = 0;
  uint64_t m_fSeq = 0;
  bool m_fileBeingMoved = false;
  std::chrono::steady_clock::time_point m_fileStartTime;

  TapeSideStats m_tapeStats;
  DiskSideStats m_diskStats;
  TapeSessionErrorStats m_errorStats;
  TapeAlertStats m_tapeAlertStats;

  uint64_t m_bytesMoved = 0;
  std::chrono::steady_clock::time_point m_lastBlockMovement;
  std::chrono::steady_clock::time_point m_sessionStartTime;
};

}  // namespace cta::tape::daemon
