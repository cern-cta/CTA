/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2003-2021 CERN
 * @license        This program is free software: you can redistribute it and/or modify
 *                 it under the terms of the GNU General Public License as published by
 *                 the Free Software Foundation, either version 3 of the License, or
 *                 (at your option) any later version.
 *
 *                 This program is distributed in the hope that it will be useful,
 *                 but WITHOUT ANY WARRANTY; without even the implied warranty of
 *                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *                 GNU General Public License for more details.
 *
 *                 You should have received a copy of the GNU General Public License
 *                 along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
/* 
 * Author: dcome
 *
 * Created on March 18, 2014, 4:28 PM
 */

#pragma once

#include "mediachanger/MediaChangerFacade.hpp"
#include "common/log/LogContext.hpp"
#include "common/threading/BlockingQueue.hpp"
#include "common/processCap/ProcessCap.hpp"
#include "common/threading/Thread.hpp"
#include "Session.hpp"
#include "TapeSessionStats.hpp"
#include "VolumeInfo.hpp"
#include "tapeserver/castor/tape/tapeserver/drive/DriveInterface.hpp"
#include "tapeserver/castor/tape/tapeserver/daemon/EncryptionControl.hpp"
#include "common/Timer.hpp"

namespace castor {
namespace tape {
namespace tapeserver {
namespace daemon {

// Forward declaration
class TapeServerReporter;

/**
 * This class is the base class for the 2 classes that will be executing
 * all tape-{read|write} tasks. The template parameter Task is the type of
 * task we are expecting : TapeReadTask or TapeWriteTask
 */
template<class Task>
class TapeSingleThreadInterface : private cta::threading::Thread {
private :
  /**
   * Utility to change the capabilities of the current tape thread
   */
  cta::server::ProcessCap& m_capUtils;
protected:
  ///the queue of tasks 
  cta::threading::BlockingQueue<Task *> m_tasks;

  /**
   * An interface to manipulate the drive to manipulate the tape
   * with the requested vid 
   */
  castor::tape::tapeserver::drive::DriveInterface& m_drive;

  /** Reference to the mount interface */
  cta::mediachanger::MediaChangerFacade& m_mediaChanger;

  /** Reference to the Global reporting interface */
  TapeServerReporter& m_reporter;

  ///The volumeID of the tape on which we want to operate  
  const std::string m_vid;

  ///log context, for ... logging purpose, copied du to thread mechanism 
  cta::log::LogContext m_logContext;

  VolumeInfo m_volInfo;

  /**
   * Integer to notify the tapeserver if the drive has to be put down or not.
   */
  Session::EndOfSessionAction m_hardwareStatus;

  /** Session statistics */
  TapeSessionStats m_stats;

  /** Encryption helper object */
  EncryptionControl m_encryptionControl;

  /** Tape load timeout after which the mount is considered failed. */
  uint32_t m_tapeLoadTimeout;

  /**
   * Try to mount the tape for read-only access, get an exception if it fails 
   */
  void mountTapeReadOnly() {
    cta::log::ScopedParamContainer scoped(m_logContext);
    scoped.add("drive_Slot", m_drive.config.librarySlot().str());
    try {
      cta::utils::Timer timer;
      m_mediaChanger.mountTapeReadOnly(m_volInfo.vid, m_drive.config.librarySlot());
      const std::string modeAsString = "R";
      scoped.add("MCMountTime", timer.secs()).add("mode", modeAsString);
      m_logContext.log(cta::log::INFO, "Tape mounted for read-only access");
    }
    catch (cta::exception::Exception& ex) {
      scoped.add("exceptionMessage", ex.getMessageValue());
      m_logContext.log(cta::log::ERR, "Failed to mount the tape for read-only access");
      throw;
    }
  }

  /**
   * Try to mount the tape for read/write access, get an exception if it fails 
   */
  void mountTapeReadWrite() {
    cta::log::ScopedParamContainer scoped(m_logContext);
    scoped.add("drive_Slot", m_drive.config.librarySlot().str());
    try {
      cta::utils::Timer timer;
      m_mediaChanger.mountTapeReadWrite(m_volInfo.vid, m_drive.config.librarySlot());
      const std::string modeAsString = "RW";
      scoped.add("MCMountTime", timer.secs()).add("mode", modeAsString);
      m_logContext.log(cta::log::INFO, "Tape mounted for read/write access");
    }
    catch (cta::exception::Exception& ex) {
      scoped.add("exceptionMessage", ex.getMessageValue());
      m_logContext.log(cta::log::ERR,
                       "Failed to mount the tape for read/write access");
      throw;
    }
  }

  /**
   * After mounting the tape, the drive will say it has no tape inside,
   * because there was no tape the first time it was opened... 
   * That function will wait a certain amount of time for the drive 
   * to tell us he acknowledge it has indeed a tape (get an ex exception in 
   * case of timeout)
   */
  void waitForDrive() {
    cta::utils::Timer tapeLoadTime;
    try {
      // wait tapeLoadTimeout seconds for drive to be ready (the mount call is synchronous, so this just the load operation).
      m_drive.waitUntilReady(m_tapeLoadTimeout);
    } catch (const cta::exception::Exception& e) {
      cta::log::ScopedParamContainer spc(m_logContext);
      spc.add("exceptionMessage", e.getMessageValue())
         .add("configuredTapeLoadTimeout", m_tapeLoadTimeout)
         .add("tapeLoadTime", tapeLoadTime.secs());
      m_logContext.log(cta::log::ERR, "Got timeout or error while waiting for drive to be ready.");
      throw;
    }
  }

  /**
   * After waiting for the drive, we will dump the tape alert log content, if
   * not empty 
   * @return true if any alert was detected
   */
  bool logTapeAlerts() {
    std::vector<uint16_t> tapeAlertCodes = m_drive.getTapeAlertCodes();
    if (tapeAlertCodes.empty()) return false;
    size_t alertNumber = 0;
    // Log tape alerts in the logs.
    std::vector<std::string> tapeAlerts = m_drive.getTapeAlerts(tapeAlertCodes);
    for (const auto& ta: tapeAlerts) {
      cta::log::ScopedParamContainer params(m_logContext);
      params.add("tapeAlert", ta)
            .add("tapeAlertNumber", alertNumber++)
            .add("tapeAlertCount", tapeAlerts.size());
      m_logContext.log(cta::log::WARNING, "Tape alert detected");
    }
    // Add tape alerts in the tape log parameters
    std::vector<std::string> tapeAlertsCompact = m_drive.getTapeAlertsCompact(tapeAlertCodes);
    for (const auto& tac: tapeAlertsCompact) {
      countTapeLogError(std::string("Error_") + tac);
    }
    return true;
  }

  /**
   * Log SCSI metrics for session.
   */
  virtual void logSCSIMetrics() = 0;

  /**
   * Function iterating through the map of available SCSI metrics and logging them.
   */
  void logSCSIStats(const std::string& logTitle, size_t metricsHashLength) {
    if (metricsHashLength == 0) { // skip logging entirely if hash is empty.
      m_logContext.log(cta::log::INFO, "SCSI Statistics could not be acquired from drive");
      return;
    }
    m_logContext.log(cta::log::INFO, logTitle);
  }

  /**
   * Function appending Tape VID, drive manufacturer and model and firmware version to the Scoped Container passed.
   */
  void appendDriveAndTapeInfoToScopedParams(cta::log::ScopedParamContainer& scopedContainer) {
    drive::deviceInfo di = m_drive.getDeviceInfo();
    scopedContainer.add("driveManufacturer", di.vendor);
    scopedContainer.add("driveType", di.product);
    scopedContainer.add("firmwareVersion", m_drive.getDriveFirmwareVersion());
    scopedContainer.add("serialNumber", m_drive.getDeviceInfo().serialNumber);
  }

  /**
   * Function appending SCSI Metrics to the Scoped Container passed.
   */
  template<class N>
  static void appendMetricsToScopedParams(cta::log::ScopedParamContainer& scopedContainer, const std::map<std::string, N>& metricsHash) {
    for (auto it = metricsHash.cbegin(); it != metricsHash.end(); it++) {
      scopedContainer.add(it->first, it->second);
    }
  }

  /**
   * Helper virtual function allowing the access to the m_watchdog member
   * in the inherited classes (TapeReadSingleThread and TapeWriteSingleThread)
   * @param error
   */
  virtual void countTapeLogError(const std::string& error) = 0;

public:

  Session::EndOfSessionAction getHardwareStatus() const {
    return m_hardwareStatus;
  }

  /**
   * Push into the class a sentinel value to trigger to end the the thread.
   */
  void finish() { m_tasks.push(nullptr); }

  /**
   * Push a new task into the internal queue
   * @param t the task to push
   */
  void push(Task *t) { m_tasks.push(t); }

  /**
   * Start the threads
   */
  virtual void startThreads() { start(); }

  /**
   *  Wait for the thread to finish
   */
  virtual void waitThreads() { wait(); }

  /**
   * Allows to pre-set the time spent waiting for instructions, spent before
   * the tape thread is started. This is for timing the synchronous task 
   * injection done before session startup.
   * This function MUST be called before starting the thread.
   * @param secs time in seconds (double)
   */
  virtual void setWaitForInstructionsTime(double secs) {
    m_stats.waitInstructionsTime = secs;
  }

  virtual castor::tape::tapeserver::drive::DriveInterface *getDriveReference() {
    return &m_drive;
  }

  /**
   * Constructor
   * @param drive An interface to manipulate the drive to manipulate the tape
   * with the requested vid
   * @param mc The media changer (=robot) that will (un)load/(un)mount the tape
   * @param gsr
   * @param volInfo All we need to know about the tape we are manipulating 
   * @param capUtils
   * @param lc lc The log context, later on copied
   * @param tapeLoadTimeout the timeout after which the mount of the tape is considered failed
   */
  TapeSingleThreadInterface(castor::tape::tapeserver::drive::DriveInterface& drive,
                            cta::mediachanger::MediaChangerFacade& mc,
                            TapeServerReporter& tsr,
                            const VolumeInfo& volInfo,
                            cta::server::ProcessCap& capUtils, cta::log::LogContext& lc,
                            const bool useEncryption,
                            const std::string& externalEncryptionKeyScript, const uint32_t tapeLoadTimeout)
    : m_capUtils(capUtils),
      m_drive(drive), m_mediaChanger(mc), m_reporter(tsr), m_vid(volInfo.vid), m_logContext(lc),
      m_volInfo(volInfo), m_hardwareStatus(Session::MARK_DRIVE_AS_UP),
      m_encryptionControl(useEncryption, externalEncryptionKeyScript), m_tapeLoadTimeout(tapeLoadTimeout) {}
}; // class TapeSingleThreadInterface

} // namespace daemon
} // namespace tapeserver
} // namespace tape
} // namespace castor

