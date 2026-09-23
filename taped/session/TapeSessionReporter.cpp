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
const char* errorName(TapeSessionFailure error) {
  switch (error) {
    case TapeSessionFailure::DiskOpenForWrite:
      return "Error_diskOpenForWrite";
    case TapeSessionFailure::DiskWrite:
      return "Error_diskWrite";
    case TapeSessionFailure::DiskCloseAfterWrite:
      return "Error_diskCloseAfterWrite";
    case TapeSessionFailure::DiskOpenForRead:
      return "Error_diskOpenForRead";
    case TapeSessionFailure::DiskFileToReadSizeMismatch:
      return "Error_diskFileToReadSizeMismatch";
    case TapeSessionFailure::DiskRead:
      return "Error_diskRead";
    case TapeSessionFailure::DiskUnexpectedSizeWhenReading:
      return "Error_diskUnexpectedSizeWhenReading";
    case TapeSessionFailure::TapeFSeqOutOfSequenceForWrite:
      return "Error_tapeFSeqOutOfSequenceForWrite";
    case TapeSessionFailure::TapeWriteHeader:
      return "Error_tapeWriteHeader";
    case TapeSessionFailure::TapeWriteData:
      return "Error_tapeWriteData";
    case TapeSessionFailure::TapeWriteTrailer:
      return "Error_tapeWriteTrailer";
    case TapeSessionFailure::TapePositionForRead:
      return "Error_tapePositionForRead";
    case TapeSessionFailure::TapeReadData:
      return "Error_tapeReadData";
    case TapeSessionFailure::TapeUnload:
      return "Error_tapeUnload";
    case TapeSessionFailure::TapeDismount:
      return "Error_tapeDismount";
    case TapeSessionFailure::TapeMountForWrite:
      return "Error_tapeMountForWrite";
    case TapeSessionFailure::TapeMountForRead:
      return "Error_tapeMountForRead";
    case TapeSessionFailure::TapeLoad:
      return "Error_tapeLoad";
    case TapeSessionFailure::CheckingTapeAlert:
      return "Error_checkingTapeAlert";
    case TapeSessionFailure::TapeNotWriteable:
      return "Error_tapeNotWriteable";
    case TapeSessionFailure::TapeEncryptionEnable:
      return "Error_tapeEncryptionEnable";
    case TapeSessionFailure::TapeEncryptionDisable:
      return "Error_tapeEncryptionDisable";
    case TapeSessionFailure::TapeLbpDisable:
      return "Error_tapeLbpDisable";
    case TapeSessionFailure::TapePositionForWrite:
      return "Error_tapePositionForWrite";
    case TapeSessionFailure::TapeFlush:
      return "Error_tapeFlush";
    case TapeSessionFailure::TapesCheckLabelBeforeReading:
      return "Error_tapesCheckLabelBeforeReading";
    case TapeSessionFailure::Reporting:
      return "Error_reporting";
    case TapeSessionFailure::FileNotArchived:
      return "Info_fileSkipped";
    case TapeSessionFailure::UnexpectedSession:
      return "Error_unexpectedSession";
    case TapeSessionFailure::TaskInjection:
      return "Error_taskInjection";
    case TapeSessionFailure::WorkerSignalling:
      return "Error_workerSignalling";
    case TapeSessionFailure::UnexpectedCleanup:
      return "Error_unexpectedCleanup";
    case TapeSessionFailure::UnclassifiedFile:
      return "Error_unclassifiedFile";
    case TapeSessionFailure::Count:
      break;
  }
  return "Error_unknown";
}

const char* eventName(TapeSessionEvent event) {
  switch (event) {
    case TapeSessionEvent::DiskSpaceReservationTestFailure:
      return "Info_diskSpaceReservationTestFailure";
    case TapeSessionEvent::DiskSpaceReservationFailure:
      return "Info_diskSpaceReservationFailure";
    case TapeSessionEvent::NoFilesToRecall:
      return "Info_noFilesToRecall";
    case TapeSessionEvent::NoFilesToMigrate:
      return "Info_noFilesToMigrate";
    case TapeSessionEvent::EmptyMount:
      return "Info_emptyMount";
    case TapeSessionEvent::TapeFilledUp:
      return "Info_tapeFilledUp";
    case TapeSessionEvent::Count:
      break;
  }
  return "Info_unknown";
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
      m_tracker.recordFailure(TapeSessionFailure::Reporting);
      cta::log::ScopedParamContainer params(m_lc);
      params.add("what", ex.what());
      m_lc.log(cta::log::WARNING, "Failed to report tape session statistics");
    }
  }

  try {
    reportSessionFinished();
  } catch (const std::exception& ex) {
    m_tracker.recordFailure(TapeSessionFailure::Reporting);
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
    m_tracker.recordFailure(TapeSessionFailure::Reporting);
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
  const auto assessment = m_tracker.outcomeSnapshot();
  params.add("status",
             !sessionFinished || !assessment.finished ? "in_progress" :
                                                        (assessment.hasFailures ? "failure" : "success"));
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

  for (size_t i = 0; i < assessment.failures.size(); ++i) {
    if (assessment.failures[i]) {
      params.add(errorName(static_cast<TapeSessionFailure>(i)), assessment.failures[i]);
    }
  }
  for (size_t i = 0; i < assessment.events.size(); ++i) {
    if (assessment.events[i]) {
      params.add(eventName(static_cast<TapeSessionEvent>(i)), assessment.events[i]);
    }
  }
  for (const auto& [tapeAlertCode, count] : assessment.tapeAlerts) {
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
