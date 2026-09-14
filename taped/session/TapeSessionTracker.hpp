/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "taped/session/SessionState.hpp"
#include "taped/session/SessionType.hpp"
#include "taped/session/TapeSessionStats.hpp"

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

class TapeSessionTracker {
public:
  void reportState(cta::tape::session::SessionState state, cta::tape::session::SessionType type) {
    std::lock_guard lock(m_mutex);

    // TODO: for now the transition to scheduler clears the stats
    if (state == cta::tape::session::SessionState::Scheduling && m_state != state) {
      m_sessionStats = {};
      m_errorStats.clear();
      m_fileId = 0;
      m_fSeq = 0;
      m_fileBeingMoved = false;
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

  void updateStats(const TapeSessionStats& stats) {
    std::lock_guard lock(m_mutex);
    m_sessionStats = stats;
  }

  void addStats(const TapeSessionStats& stats) {
    std::lock_guard lock(m_mutex);
    m_sessionStats.add(stats);
  }

  TapeSessionErrorStats errorStats() const {
    std::lock_guard lock(m_mutex);
    return m_errorStats;
  }

  TapeSessionStats sessionStats() const {
    std::lock_guard lock(m_mutex);
    return m_sessionStats;
  }

  void notifyBeginNewJob(uint64_t fileId, uint64_t fSeq) {
    std::lock_guard lock(m_mutex);
    m_fileId = fileId;
    m_fSeq = fSeq;
    m_fileBeingMoved = true;
  }

  /**
   * Notify the tracker we have finished operating on the current file.
   */
  void fileFinished() {
    std::lock_guard lock(m_mutex);
    m_fileBeingMoved = false;
    m_fileId = 0;
    m_fSeq = 0;
  }

  bool errorHappened() const {
    std::lock_guard lock(m_mutex);
    return !m_errorStats.empty();
  }

private:
  mutable std::mutex m_mutex;

  cta::tape::session::SessionState m_state = cta::tape::session::SessionState::StartingUp;
  cta::tape::session::SessionType m_type = cta::tape::session::SessionType::Undetermined;

  uint64_t m_fileId = 0;
  uint64_t m_fSeq = 0;
  bool m_fileBeingMoved = false;

  TapeSessionStats m_sessionStats;
  // TODO: unordered map?
  std::map<TapeSessionError, uint32_t> m_errorStats;
};

}  // namespace cta::tape::daemon
