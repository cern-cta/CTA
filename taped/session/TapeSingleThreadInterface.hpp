/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
/*
 * Author: dcome
 *
 * Created on March 18, 2014, 4:28 PM
 */

#pragma once

#include "TapeSessionStats.hpp"
#include "TapeSessionTracker.hpp"
#include "VolumeInfo.hpp"
#include "common/log/LogContext.hpp"
#include "common/process/threading/BlockingQueue.hpp"
#include "common/process/threading/Thread.hpp"
#include "common/semconv/Attributes.hpp"
#include "common/utils/Timer.hpp"
#include "mediachanger/LibrarySlotParser.hpp"
#include "mediachanger/MediaChangerFacade.hpp"
#include "taped/drive/DriveInterface.hpp"
#include "taped/drive/EncryptionControl.hpp"
#include "telemetry/metrics/TapedMetrics.hpp"

#include <opentelemetry/context/runtime_context.h>

namespace cta::tape::daemon {

/// @brief Shared queue, drive helpers and statistics for a single tape-transfer worker.
///
/// Borrowed drive, media changer and tracker must outlive the worker. Join the worker before destruction.
/// @tparam Task TapeReadTask or TapeWriteTask.
template<class Task>
class TapeSingleThreadInterface : private cta::threading::Thread {
private:
protected:
  cta::threading::BlockingQueue<Task*> m_tasks;

  cta::tape::drive::DriveInterface& m_drive;

  cta::mediachanger::MediaChangerFacade& m_mediaChanger;

  TapeSessionTracker& m_tracker;

  const std::string m_vid;

  /// Private copy isolates worker log parameters from the caller.
  cta::log::LogContext m_logContext;

  VolumeInfo m_volInfo;

  /// Whether the tape thread permits the drive to be reused.
  bool m_driveReusable = true;
  std::string m_cleanupError;

  TapeTransferStats m_stats;
  double m_totalTime = 0;

  /// @brief Accumulate setup duration in the tracker, including failed operations.
  /// @param field Setup timing field to update, in seconds.
  /// @param operation Callable to execute; its exception propagates after timing is recorded.
  template<class Operation>
  void measureSetupTime(double TapeSetupStats::* field, Operation operation) {
    cta::utils::Timer timer;
    const auto record = [&] {
      TapeSetupStats stats;
      stats.*field = timer.secs();
      m_tracker.addTapeSetupStats(stats);
    };
    try {
      operation();
    } catch (...) {
      record();
      throw;
    }
    record();
  }

  EncryptionControl m_encryptionControl;

  /// Tape load timeout after which the mount is considered failed.
  uint32_t m_tapeLoadTimeout;

  /// @brief Wait for drive readiness within the configured load timeout.
  ///
  /// Log and propagate drive errors and timeouts.
  void waitForDrive() {
    cta::utils::Timer tapeLoadTime;
    try {
      // Mounting is synchronous; this wait covers drive readiness after loading.
      m_drive.waitUntilReady(m_tapeLoadTimeout);
    } catch (const cta::exception::Exception& e) {
      cta::log::ScopedParamContainer spc(m_logContext);
      spc.add(cta::semconv::log::exceptionMessage, e.getMessageValue())
        .add("configuredTapeLoadTimeout", m_tapeLoadTimeout)
        .add("tapeLoadTime", tapeLoadTime.secs());
      m_logContext.log(cta::log::ERR, "Got timeout or error while waiting for drive to be ready.");
      throw;
    }
  }

  /// @brief Read, log and count the drive's tape alerts.
  /// @return True if any alert was returned.
  bool logTapeAlerts() {
    std::vector<uint16_t> tapeAlertCodes = m_drive.getTapeAlertCodes();
    if (tapeAlertCodes.empty()) {
      return false;
    }
    size_t alertNumber = 0;
    // Log tape alerts in the logs.
    std::vector<std::string> tapeAlerts = m_drive.getTapeAlerts(tapeAlertCodes);
    for (const auto& ta : tapeAlerts) {
      cta::log::ScopedParamContainer params(m_logContext);
      params.add("tapeAlert", ta).add("tapeAlertNumber", alertNumber++).add("tapeAlertCount", tapeAlerts.size());
      m_logContext.log(cta::log::WARNING, "Tape alert detected");
    }
    // Add tape alerts in the tape log parameters
    for (const auto tapeAlertCode : tapeAlertCodes) {
      countTapeAlert(tapeAlertCode);
    }
    return true;
  }

  /// Log direction-specific SCSI metrics for the session.
  virtual void logSCSIMetrics() = 0;

  /// Log a metrics heading, or an unavailable message when the metric count is zero.
  void logSCSIStats(const std::string& logTitle, size_t metricsHashLength) {
    if (metricsHashLength == 0) {  // skip logging entirely if hash is empty.
      m_logContext.log(cta::log::INFO, "SCSI Statistics could not be acquired from drive");
      return;
    }
    m_logContext.log(cta::log::INFO, logTitle);
  }

  /// Add drive manufacturer, model, firmware and serial number to the supplied log parameters.
  void appendDriveAndTapeInfoToScopedParams(cta::log::ScopedParamContainer& scopedContainer) {
    drive::deviceInfo di = m_drive.getDeviceInfo();
    scopedContainer.add("driveManufacturer", di.vendor);
    scopedContainer.add("driveType", di.product);
    scopedContainer.add("firmwareVersion", m_drive.getDriveFirmwareVersion());
    scopedContainer.add("serialNumber", m_drive.getDeviceInfo().serialNumber);
  }

  /// Append each named metric to the supplied log parameters.
  template<class N>
  static void appendMetricsToScopedParams(cta::log::ScopedParamContainer& scopedContainer,
                                          const std::map<std::string, N>& metricsHash) {
    for (auto it = metricsHash.cbegin(); it != metricsHash.end(); it++) {
      scopedContainer.add(it->first, it->second);
    }
  }

  /// Count the supplied tape-alert code in the session tracker.
  virtual void countTapeAlert(uint16_t tapeAlertCode) = 0;

public:
  /// @brief Return whether cleanup permits reuse of the drive.
  /// @pre The tape worker has been joined.
  bool isDriveReusable() const { return m_driveReusable; }

  /// @brief Return cleanup failure details, or an empty string when none were recorded.
  /// @pre The tape worker has been joined.
  const std::string& cleanupError() const { return m_cleanupError; }

  /// Queue the end-of-work sentinel after all submitted tasks.
  void finish() { m_tasks.push(nullptr); }

  /// Transfer a task to the worker queue; nullptr marks the end of work.
  void push(Task* t) { m_tasks.push(t); }

  /// Start the tape worker.
  virtual void startThreads() { start(); }

  /// Join the tape worker after it finishes processing tasks.
  virtual void waitThreads() { wait(); }

  /// @brief Set the initial instruction-wait duration in seconds.
  /// @pre The tape worker has not started.
  virtual void setWaitForInstructionsTime(double secs) { m_stats.waitInstructionsTime = secs; }

  /// Return a non-owning pointer to the supplied drive.
  virtual cta::tape::drive::DriveInterface* getDriveReference() { return &m_drive; }

  /// @brief Initialize a tape worker without starting it.
  /// @param drive Borrowed tape drive.
  /// @param mc Borrowed media changer.
  /// @param tracker Borrowed session tracker.
  /// @param volInfo Volume metadata copied into the worker.
  /// @param lc Logging context copied for worker use.
  /// @param useEncryption Whether tape encryption is enabled.
  /// @param externalEncryptionKeyScript Path to the encryption-key script.
  /// @param tapeLoadTimeout Drive-readiness timeout in seconds.
  TapeSingleThreadInterface(cta::tape::drive::DriveInterface& drive,
                            cta::mediachanger::MediaChangerFacade& mc,
                            TapeSessionTracker& tracker,
                            const VolumeInfo& volInfo,
                            const cta::log::LogContext& lc,
                            const bool useEncryption,
                            const std::string& externalEncryptionKeyScript,
                            const uint32_t tapeLoadTimeout)
      : m_drive(drive),
        m_mediaChanger(mc),
        m_tracker(tracker),
        m_vid(volInfo.vid),
        m_logContext(lc),
        m_volInfo(volInfo),
        m_encryptionControl(useEncryption, externalEncryptionKeyScript),
        m_tapeLoadTimeout(tapeLoadTimeout) {}
};  // class TapeSingleThreadInterface

}  // namespace cta::tape::daemon
