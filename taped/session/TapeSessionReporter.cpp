/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "TapeSessionReporter.hpp"

#include "common/log/Logger.hpp"
#include "common/semconv/Attributes.hpp"
#include "taped/scsi/Constants.hpp"

#include <algorithm>
#include <map>
#include <string>

namespace cta::tape::daemon {

namespace {

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

void pushParameter(cta::log::ScopedParamContainer& container, const cta::log::Param& parameter) {
  if (!parameter.getValueVariant()) {
    container.add(parameter.getName(), std::nullopt);
    return;
  }
  std::visit([&](const auto& value) { container.add(parameter.getName(), value); }, *parameter.getValueVariant());
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
  logStats(false);
  m_tracker.mount()->setTapeSessionStats(m_tracker.tapeStats());
}

void TapeSessionReporter::reportSessionFinished() {
  logStats(true);
  m_tracker.mount()->setTapeSessionStats(m_tracker.tapeStats());
}

void TapeSessionReporter::logStats(bool sessionFinished) {
  const auto& mount = *m_tracker.mount();
  const auto tapeStats = m_tracker.tapeStats();
  const auto diskStats = m_tracker.diskStats();
  const auto elapsedTime = std::chrono::duration<double>(m_tracker.sessionElapsedTime()).count();
  const double totalTime = tapeStats.totalTime ? tapeStats.totalTime : elapsedTime;
  const double deliveryTime = diskStats.deliveryTime ? diskStats.deliveryTime : elapsedTime;

  std::map<std::string, cta::log::Param> reportParameters;
  const auto set = [&reportParameters](const std::string& name, const auto& value) {
    reportParameters.insert_or_assign(name, cta::log::Param(name, value));
  };

  set("wasTapeMounted", tapeStats.mountTime != 0.0);
  set("mountTime", tapeStats.mountTime);
  set("positionTime", tapeStats.positionTime);
  set("waitInstructionsTime", tapeStats.waitInstructionsTime);
  set("waitFreeMemoryTime", tapeStats.waitFreeMemoryTime);
  set("waitDataTime", tapeStats.waitDataTime);
  set("waitReportingTime", diskStats.waitReportingTime);
  set("checksumingTime", tapeStats.checksumingTime);
  set("readWriteTime", tapeStats.readWriteTime);
  set("flushTime", tapeStats.flushTime);
  set("unloadTime", tapeStats.unloadTime);
  set("unmountTime", tapeStats.unmountTime);
  set("encryptionControlTime", tapeStats.encryptionControlTime);
  set("transferTime", tapeStats.transferTime());
  set("totalTime", totalTime);
  set("deliveryTime", deliveryTime);
  set("drainingTime", std::max(deliveryTime - totalTime, 0.0));
  set("dataVolume", tapeStats.dataVolume);
  set("filesCount", tapeStats.filesCount);
  set("headerVolume", tapeStats.headerVolume);
  set("repackFilesCount", tapeStats.repackFilesCount);
  set("userFilesCount", tapeStats.userFilesCount);
  set("verifiedFilesCount", tapeStats.verifiedFilesCount);
  set("repackBytesCount", tapeStats.repackBytesCount);
  set("userBytesCount", tapeStats.userBytesCount);
  set("verifiedBytesCount", tapeStats.verifiedBytesCount);
  set("payloadTransferSpeedMBps", totalTime ? tapeStats.dataVolume / 1000.0 / 1000.0 / totalTime : 0.0);
  set("driveTransferSpeedMBps",
      totalTime ? (tapeStats.dataVolume + tapeStats.headerVolume) / 1000.0 / 1000.0 / totalTime : 0.0);
  set("sessionState", cta::tape::session::toString(m_tracker.state()));
  set("sessionType", cta::tape::session::toString(m_tracker.type()));
  switch (m_tracker.outcome()) {
    case TapeSessionOutcome::Automatic:
      set("status", m_tracker.errorHappened() ? "failure" : "success");
      break;
    case TapeSessionOutcome::Success:
      set("status", "success");
      break;
    case TapeSessionOutcome::Failure:
      set("status", "failure");
      break;
  }
  set("mountAttempted", m_tracker.mountAttempted() ? 1 : 0);
  set("tapeVid", mount.getVid());
  set("mountType", cta::common::dataStructures::toCamelCaseString(mount.getMountType()));
  set("mountId", mount.getMountTransactionId());
  set("volReqId", mount.getMountTransactionId());
  set("vendor", mount.getVendor());
  set("vo", mount.getVo());
  set("mediaType", mount.getMediaType());
  set("tapePool", mount.getPoolName());
  set("capacityInBytes", mount.getCapacityInBytes());

  for (const auto& [error, count] : m_tracker.errorStats()) {
    set(errorName(error), count);
  }
  for (const auto& [tapeAlertCode, count] : m_tracker.tapeAlertStats()) {
    set("Error_" + cta::tape::SCSI::tapeAlertToCompactString(tapeAlertCode), count);
  }
  for (const auto& [threadId, file] : m_tracker.activeDiskFiles()) {
    set("stillOpenFileForThread" + std::to_string(threadId), file.path);
  }
  cta::log::ScopedParamContainer params(m_lc);
  for (const auto& [name, parameter] : reportParameters) {
    pushParameter(params, parameter);
  }

  if (sessionFinished) {
    m_lc.logEvent(cta::log::INFO, "Tape session finished", cta::semconv::log::EventNameValues::kTapeSessionFinished);
  } else {
    params.log(cta::log::INFO, "Tape session statistics");
  }
}

}  // namespace cta::tape::daemon
