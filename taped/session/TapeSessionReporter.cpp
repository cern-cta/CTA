/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "TapeSessionReporter.hpp"

#include "common/log/Logger.hpp"
#include "common/semconv/Attributes.hpp"
#include "taped/scsi/Constants.hpp"

#include <algorithm>
#include <optional>
#include <string>

namespace cta::tape::daemon {

namespace {

/**
 * @brief Map a session error to its log-field name, with a fallback for unknown values.
 *
 * @param error Session error category to count or name.
 * @return Log-field name for the error, or Error_unknown for an unrecognized value.
 */
const char* errorName(TapeSessionError error) {
  switch (error) {
    case TapeSessionError::DiskOpenForWrite:
      return "Error_diskOpenForWrite";
    case TapeSessionError::DiskWrite:
      return "Error_diskWrite";
    case TapeSessionError::DiskCloseAfterWrite:
      return "Error_diskCloseAfterWrite";
    case TapeSessionError::DiskOpenForRead:
      return "Error_diskOpenForRead";
    case TapeSessionError::DiskFileToReadSizeMismatch:
      return "Error_diskFileToReadSizeMismatch";
    case TapeSessionError::DiskRead:
      return "Error_diskRead";
    case TapeSessionError::DiskUnexpectedSizeWhenReading:
      return "Error_diskUnexpectedSizeWhenReading";
    case TapeSessionError::TapeFSeqOutOfSequenceForWrite:
      return "Error_tapeFSeqOutOfSequenceForWrite";
    case TapeSessionError::TapeWriteHeader:
      return "Error_tapeWriteHeader";
    case TapeSessionError::TapeWriteData:
      return "Error_tapeWriteData";
    case TapeSessionError::TapeWriteTrailer:
      return "Error_tapeWriteTrailer";
    case TapeSessionError::TapePositionForRead:
      return "Error_tapePositionForRead";
    case TapeSessionError::TapeReadData:
      return "Error_tapeReadData";
    case TapeSessionError::TapeUnload:
      return "Error_tapeUnload";
    case TapeSessionError::TapeDismount:
      return "Error_tapeDismount";
    case TapeSessionError::TapeMountForWrite:
      return "Error_tapeMountForWrite";
    case TapeSessionError::TapeMountForRead:
      return "Error_tapeMountForRead";
    case TapeSessionError::TapeLoad:
      return "Error_tapeLoad";
    case TapeSessionError::CheckingTapeAlert:
      return "Error_checkingTapeAlert";
    case TapeSessionError::TapeNotWriteable:
      return "Error_tapeNotWriteable";
    case TapeSessionError::TapeEncryptionEnable:
      return "Error_tapeEncryptionEnable";
    case TapeSessionError::TapeEncryptionDisable:
      return "Error_tapeEncryptionDisable";
    case TapeSessionError::TapeLbpDisable:
      return "Error_tapeLbpDisable";
    case TapeSessionError::TapePositionForWrite:
      return "Error_tapePositionForWrite";
    case TapeSessionError::TapeFlush:
      return "Error_tapeFlush";
    case TapeSessionError::TapesCheckLabelBeforeReading:
      return "Error_tapesCheckLabelBeforeReading";
    case TapeSessionError::Reporting:
      return "Error_reporting";
    case TapeSessionError::DiskSpaceReservationTestFailure:
      return "Info_diskSpaceReservationTestFailure";
    case TapeSessionError::DiskSpaceReservationFailure:
      return "Info_diskSpaceReservationFailure";
    case TapeSessionError::NoFilesToRecall:
      return "Info_noFilesToRecall";
    case TapeSessionError::NoFilesToMigrate:
      return "Info_noFilesToMigrate";
    case TapeSessionError::EmptyMount:
      return "Info_emptyMount";
    case TapeSessionError::FileSkipped:
      return "Info_fileSkipped";
    case TapeSessionError::TapeFilledUp:
      return "Info_tapeFilledUp";
  }
  return "Error_unknown";
}

}  // namespace

TapeSessionReporter::TapeSessionReporter(TapeSessionTracker& tracker,
                                         const cta::log::LogContext& lc,
                                         std::chrono::milliseconds reportPeriod,
                                         std::chrono::milliseconds stuckPeriod)
    : m_tracker(tracker),
      m_lc(lc),
      m_reportPeriod(std::max(reportPeriod, std::chrono::milliseconds(1))),
      m_stuckPeriod(std::max(stuckPeriod, std::chrono::milliseconds(1))) {
  m_lc.push(cta::log::Param("thread", "TapeSessionReporter"));
}

void TapeSessionReporter::startThreads() {
  start();
}

void TapeSessionReporter::finish() {
  {
    std::lock_guard lock(m_mutex);
    m_finishRequested = true;
  }
  m_condition.notify_one();
}

void TapeSessionReporter::waitThreads() {
  wait();
}

void TapeSessionReporter::run() {
  while (true) {
    {
      std::unique_lock lock(m_mutex);
      if (m_condition.wait_for(lock, m_reportPeriod, [this] { return m_finishRequested; })) {
        break;
      }
    }

    reportStuckFileIfNeeded();
    try {
      reportNow();
    } catch (const std::exception& ex) {
      m_tracker.incrementError(TapeSessionError::Reporting);
      cta::log::ScopedParamContainer params(m_lc);
      params.add("what", ex.what());
      m_lc.log(cta::log::WARNING, "Failed to report tape session statistics");
    }
  }

  try {
    reportSessionFinished();
  } catch (const std::exception& ex) {
    m_tracker.incrementError(TapeSessionError::Reporting);
    cta::log::ScopedParamContainer params(m_lc);
    params.add("what", ex.what());
    m_lc.log(cta::log::WARNING, "Failed to send final tape session statistics");
  }
}

void TapeSessionReporter::reportStuckFileIfNeeded() {
  const auto progress = m_tracker.progress();
  if (!progress.fileBeingMoved) {
    m_lastStuckReport = {};
    return;
  }

  const auto now = std::chrono::steady_clock::now();
  const auto lastActivity = progress.lastBlockMovement == std::chrono::steady_clock::time_point {} ?
                              progress.fileStartTime :
                              progress.lastBlockMovement;
  if (now - lastActivity <= m_stuckPeriod
      || (m_lastStuckReport != std::chrono::steady_clock::time_point {} && now - m_lastStuckReport <= m_stuckPeriod)) {
    return;
  }

  cta::log::ScopedParamContainer params(m_lc);
  params.add("TimeSinceLastBlockMove", std::chrono::duration<double>(now - lastActivity).count())
    .add("NoBlockMoveMaxSecs", std::chrono::duration<double>(m_stuckPeriod).count())
    .add("fileId", progress.fileId)
    .add("fSeq", progress.fSeq);
  m_lc.log(cta::log::WARNING, "No tape block movement for too long");
  m_lastStuckReport = now;
}

void TapeSessionReporter::reportNow() {
  const auto stats = m_tracker.stats();
  logStats(false, stats);
  m_tracker.mount()->setTapeSessionStats(stats.tape);
}

void TapeSessionReporter::reportSessionFinished() {
  // Stopping the reporter alone does not establish that transfer workers have stopped.
  if (m_tracker.state() != cta::tape::session::TapeSessionState::Finished) {
    return;
  }

  const auto stats = m_tracker.stats();
  try {
    m_tracker.mount()->setTapeSessionStats(stats.tape);
  } catch (...) {
    m_tracker.incrementError(TapeSessionError::Reporting);
    m_lc.log(cta::log::WARNING, "Failed to publish final tape session statistics");
  }
  // Publication failures must be reflected in the single final outcome log.
  logStats(true, stats);
}

void TapeSessionReporter::logStats(bool sessionFinished, const TapeSessionStats& stats) {
  const auto& mount = *m_tracker.mount();
  const auto& setupStats = stats.setup;
  const auto& tapeStats = stats.tape;
  const auto& diskStats = stats.disk;
  const auto& cleanupStats = stats.cleanup;
  const auto elapsedTime = std::chrono::duration<double>(m_tracker.sessionElapsedTime()).count();
  const double totalTime = stats.totalTime ? stats.totalTime : elapsedTime;
  const double deliveryTime = diskStats.deliveryTime ? diskStats.deliveryTime : elapsedTime;

  cta::log::ScopedParamContainer params(m_lc);

  params.add("wasTapeMounted", setupStats.mountTime != 0.0);
  params.add("mountTime", setupStats.mountTime);
  params.add("initialMountTime", setupStats.initialMountTime);
  params.add("tapeLoadTime", setupStats.tapeLoadTime);
  params.add("positionTime", setupStats.positionTime + tapeStats.positionTime);
  params.add("waitInstructionsTime", tapeStats.waitInstructionsTime);
  params.add("waitFreeMemoryTime", tapeStats.waitFreeMemoryTime);
  params.add("waitDataTime", tapeStats.waitDataTime);
  params.add("waitReportingTime", diskStats.waitReportingTime);
  params.add("checksumingTime", tapeStats.checksumingTime);
  params.add("readWriteTime", tapeStats.readWriteTime);
  params.add("flushTime", tapeStats.flushTime);
  params.add("unloadTime", cleanupStats.unloadTime);
  params.add("unmountTime", cleanupStats.unmountTime);
  params.add("encryptionControlTime", setupStats.encryptionControlTime + cleanupStats.encryptionControlTime);
  params.add("cleanupTime", cleanupStats.cleanupTime);
  params.add("lbpResetTime", cleanupStats.lbpResetTime);
  params.add("readinessWaitTime", cleanupStats.readinessWaitTime);
  params.add("rewindTime", cleanupStats.rewindTime);
  params.add("labelReadTime", cleanupStats.labelReadTime);
  params.add("transferTime", tapeStats.transferTime(diskStats.waitReportingTime));
  params.add("totalTime", totalTime);
  params.add("deliveryTime", deliveryTime);
  params.add("drainingTime", std::max(deliveryTime - totalTime, 0.0));
  params.add("dataVolume", tapeStats.dataVolume);
  params.add("filesCount", tapeStats.filesCount);
  params.add("headerVolume", tapeStats.headerVolume);
  params.add("repackFilesCount", tapeStats.repackFilesCount);
  params.add("userFilesCount", tapeStats.userFilesCount);
  params.add("verifiedFilesCount", tapeStats.verifiedFilesCount);
  params.add("repackBytesCount", tapeStats.repackBytesCount);
  params.add("userBytesCount", tapeStats.userBytesCount);
  params.add("verifiedBytesCount", tapeStats.verifiedBytesCount);
  params.add("payloadTransferSpeedMBps", totalTime ? tapeStats.dataVolume / 1000.0 / 1000.0 / totalTime : 0.0);
  params.add("driveTransferSpeedMBps",
             totalTime ? (tapeStats.dataVolume + tapeStats.headerVolume) / 1000.0 / 1000.0 / totalTime : 0.0);
  const auto state = m_tracker.state();
  if (state) {
    params.add("sessionState", cta::tape::session::toString(*state));
  } else {
    params.add("sessionState", std::nullopt);
  }
  params.add("sessionType", cta::tape::session::toString(m_tracker.type()));
  if (!sessionFinished) {
    params.add("status", "in_progress");
  } else {
    switch (m_tracker.outcome()) {
      case TapeSessionOutcome::Automatic:
        params.add("status", m_tracker.errorHappened() ? "failure" : "success");
        break;
      case TapeSessionOutcome::Success:
        params.add("status", "success");
        break;
      case TapeSessionOutcome::Failure:
        params.add("status", "failure");
        break;
    }
  }
  params.add("mountAttempted", m_tracker.mountAttempted() ? 1 : 0);
  params.add("tapeVid", mount.getVid());
  params.add("mountType", cta::common::dataStructures::toCamelCaseString(mount.getMountType()));
  params.add("mountId", mount.getMountTransactionId());
  params.add("volReqId", mount.getMountTransactionId());
  params.add("vendor", mount.getVendor());
  params.add("vo", mount.getVo());
  params.add("mediaType", mount.getMediaType());
  params.add("tapePool", mount.getPoolName());
  params.add("capacityInBytes", mount.getCapacityInBytes());

  for (const auto& [error, count] : m_tracker.errorStats()) {
    params.add(errorName(error), count);
  }
  for (const auto& [tapeAlertCode, count] : m_tracker.tapeAlertStats()) {
    params.add("Error_" + cta::tape::SCSI::tapeAlertToCompactString(tapeAlertCode), count);
  }
  for (const auto& [threadId, file] : m_tracker.activeDiskFiles()) {
    params.add("stillOpenFileForThread" + std::to_string(threadId), file.path);
  }

  if (sessionFinished) {
    m_lc.logEvent(cta::log::INFO, "Tape session finished", cta::semconv::log::EventNameValues::kTapeSessionFinished);
  } else {
    params.log(cta::log::INFO, "Tape session statistics");
  }
}

}  // namespace cta::tape::daemon
