/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "taped/drive/DriveCleaner.hpp"

#include "catalogue/Catalogue.hpp"
#include "common/dataStructures/SecurityIdentity.hpp"
#include "common/exception/Exception.hpp"
#include "common/semconv/Logging.hpp"
#include "common/utils/Timer.hpp"
#include "common/utils/utils.hpp"
#include "mediachanger/LibrarySlotParser.hpp"
#include "taped/file/HeaderChecker.hpp"
#include "taped/session/TapeSessionTracker.hpp"

#include <ctime>
#include <exception>
#include <optional>
#include <vector>

namespace cta::tape::daemon {

namespace {

/// Record elapsed time on every exit, including early returns and failed operations.
class CleanupTiming {
public:
  /// @brief Begin measuring a cleanup operation for the selected statistics field.
  ///
  /// @param tracker Borrowed session tracker that must outlive this object.
  /// @param field Cleanup-statistics field to which elapsed seconds are added on scope exit.
  CleanupTiming(TapeSessionTracker& tracker, double TapeCleanupStats::* field) : m_tracker(tracker), m_field(field) {}

  /// Accumulate elapsed seconds in the selected cleanup field on scope exit.
  ~CleanupTiming() {
    TapeCleanupStats stats;
    stats.*m_field = m_timer.secs();
    m_tracker.addTapeCleanupStats(stats);
  }

private:
  TapeSessionTracker& m_tracker;
  double TapeCleanupStats::* const m_field;
  utils::Timer m_timer;
};

}  // namespace

DriveCleaner::DriveCleaner(mediachanger::MediaChangerFacade& mc,
                           log::Logger& log,
                           const common::dataStructures::DriveInfo& driveInfo,
                           const std::string& vid,
                           const bool waitMediaInDrive,
                           const uint32_t waitMediaInDriveTimeout,
                           catalogue::Catalogue& catalogue,
                           TapeSessionTracker& tracker)
    : m_tracker(tracker),
      m_mediachanger(mc),
      m_lc(log),
      m_driveInfo(driveInfo),
      m_vid(vid),
      m_waitMediaInDrive(waitMediaInDrive),
      m_tapeLoadTimeout(waitMediaInDriveTimeout),
      m_catalogue(catalogue) {
  m_lc.push(log::Param("tapeVid", m_vid));
}

auto DriveCleaner::cleanDrive(System::virtualWrapper& sysWrapper) -> CleanupResult {
  // Cleanup gets its own deadline before drive discovery or acquisition can block.
  m_tracker.reportState(session::TapeSessionState::Unloading);
  std::unique_ptr<drive::DriveInterface> drive;
  std::optional<std::string> driveError;
  {
    // Account for acquisition separately so delegated cleanup is timed only once.
    CleanupTiming timing(m_tracker, &TapeCleanupStats::cleanupTime);
    try {
      drive = createDrive(sysWrapper);
    } catch (...) {
      driveError = exception::currentExceptionMessage();
    }
  }

  if (!driveError) {
    auto result = cleanDrive(*drive);
    CleanupTiming timing(m_tracker, &TapeCleanupStats::cleanupTime);
    logAndClearTapeAlerts(*drive);
    if (result.driveReusable()) {
      m_lc.log(log::INFO, "Cleaner completed successfully");
    }
    return result;
  }

  // The robot can return the cartridge even when the drive cannot be opened.
  CleanupTiming timing(m_tracker, &TapeCleanupStats::cleanupTime);
  CleanupResult result;
  result.driveOpenFailed = true;
  try {
    m_tracker.reportState(session::TapeSessionState::Unmounting);
    dismountTape("");
    result.errorMessage = "Tape was dismounted, but the drive could not be opened and must remain down: " + *driveError;
  } catch (...) {
    result.errorMessage = "Failed to create the drive (" + *driveError + ") and failed to dismount the tape ("
                          + exception::currentExceptionMessage() + ")";
    result.ejectFailed = true;
  }
  finishCleanup(result);
  return result;
}

void DriveCleaner::finishCleanup(const CleanupResult& result) {
  if (result.ejectFailed) {
    disableTapeAfterFailedEject(result.errorMessage);
  }
}

void DriveCleaner::disableTapeAfterFailedEject(const std::string& errorMsg) noexcept {
  if (m_vid.empty()) {
    m_lc.log(log::WARNING, "Cleaner cannot disable tape after failed eject because its VID is unknown");
    return;
  }

  log::ScopedParamContainer params(m_lc);
  try {
    using Tape = common::dataStructures::Tape;
    const auto tapes = m_catalogue.Tape()->getTapesByVid(m_vid);
    const auto tape = tapes.at(m_vid);

    std::optional<Tape::State> disabledState;
    if (tape.state == Tape::ACTIVE) {
      disabledState = Tape::DISABLED;
    } else if (tape.state == Tape::REPACKING) {
      disabledState = Tape::REPACKING_DISABLED;
    }

    if (!disabledState) {
      params.add("tapeState", Tape::stateToString(tape.state));
      m_lc.log(
        log::WARNING,
        "Cleaner did not change tape state after failed eject. Current tape state cannot be disabled automatically.");
      return;
    }

    common::dataStructures::SecurityIdentity admin;
    admin.username = "cta-taped " + m_driveInfo.driveName;
    admin.host = utils::getShortHostname();
    const std::string reason = utils::getCurrentLocalTime("%F %T") + ": " + errorMsg;
    m_catalogue.Tape()->modifyTapeState(admin, m_vid, *disabledState, tape.state, reason);
    params.add("newTapeState", Tape::stateToString(*disabledState));
    m_lc.log(log::WARNING, "Cleaner disabled tape after failed eject");
  } catch (...) {
    params.add(semconv::log::exceptionMessage, exception::currentExceptionMessage());
    m_lc.log(log::ERR, "Cleaner failed to disable tape after failed eject");
  }
}

auto DriveCleaner::cleanDrive(drive::DriveInterface& drive) -> CleanupResult {
  // Include readiness, configuration reset and rewind in the unloading budget.
  m_tracker.reportState(session::TapeSessionState::Unloading);
  CleanupTiming timing(m_tracker, &TapeCleanupStats::cleanupTime);
  auto result = cleanDriveImpl(drive);
  finishCleanup(result);
  return result;
}

auto DriveCleaner::cleanDriveImpl(drive::DriveInterface& drive) -> CleanupResult {
  CleanupResult result;
  auto recordResetFailure = [&](const std::string& operation, TapeSessionFailure error) {
    m_tracker.recordFailure(error);
    result.configurationResetFailed = true;
    if (!result.errorMessage.empty()) {
      result.errorMessage += "; ";
    }
    result.errorMessage += operation + ": " + exception::currentExceptionMessage();
    log::ScopedParamContainer params(m_lc);
    params.add(semconv::log::exceptionMessage, result.errorMessage);
    m_lc.log(log::WARNING, "Cleaner failed to reset drive configuration; continuing with tape eject");
  };

  if (m_waitMediaInDrive) {
    // Presence may be cached as empty after a partially completed mount.
    // Wait for readiness to refresh device status before checking for media.
    waitForMediaToBeReady(drive);
  }

  // Reset persistent configuration even when the drive is empty.
  utils::Timer encryptionTimer;
  try {
    drive.clearEncryptionKey();
  } catch (...) {
    recordResetFailure("Failed to clear encryption key", TapeSessionFailure::TapeEncryptionDisable);
  }
  m_tracker.addTapeCleanupStats({.encryptionControlTime = encryptionTimer.secs()});
  {
    CleanupTiming timing(m_tracker, &TapeCleanupStats::lbpResetTime);
    try {
      drive.disableLogicalBlockProtection();
    } catch (...) {
      recordResetFailure("Failed to disable logical block protection", TapeSessionFailure::TapeLbpDisable);
    }
  }

  try {
    if (!drive.hasTapeInPlace()) {
      m_lc.log(log::INFO, "Cleaner found no tape in the drive; nothing to do");
      return result;
    }
  } catch (...) {
    log::ScopedParamContainer params(m_lc);
    params.add(semconv::log::exceptionMessage, exception::currentExceptionMessage());
    m_lc.log(log::WARNING, "Cleaner could not determine whether the drive contains a tape");
  }

  std::optional<std::string> volumeLabel;
  try {
    rewindDrive(drive);
    volumeLabel = readVolumeLabel(drive);
  } catch (...) {
    log::ScopedParamContainer params(m_lc);
    params.add(semconv::log::exceptionMessage, exception::currentExceptionMessage());
    m_lc.log(log::WARNING,
             "Cleaner failed to prepare the drive or read the volume label; continuing with the provided VID");
  }

  try {
    unloadTape(drive);
  } catch (...) {
    log::ScopedParamContainer params(m_lc);
    params.add(semconv::log::exceptionMessage, exception::currentExceptionMessage());
    m_lc.log(log::WARNING, "Cleaner unload command failed; attempting robotic dismount");
  }
  std::string dismountVid = m_vid;
  if (volumeLabel) {
    if (!m_vid.empty() && *volumeLabel != m_vid) {
      log::ScopedParamContainer params(m_lc);
      params.add("volumeLabelVSN", *volumeLabel);
      m_lc.log(log::ERR, "Cleaner detected volume label does not match provided VID");
    }
    dismountVid = *volumeLabel;
  }

  if (!dismountVid.empty()) {
    try {
      m_tracker.reportState(session::TapeSessionState::Unmounting);
      dismountTape(dismountVid);
      return result;
    } catch (...) {
      log::ScopedParamContainer params(m_lc);
      params.add("dismountVid", dismountVid);
      params.add(semconv::log::exceptionMessage, exception::currentExceptionMessage());
      m_lc.log(log::WARNING,
               "Cleaner failed to dismount tape with VID; retrying with an empty VID to bypass the robot's "
               "cartridge-name consistency check");
    }
  }

  // An empty VID asks the robot to move the cartridge without checking its name.
  try {
    m_tracker.reportState(session::TapeSessionState::Unmounting);
    dismountTape("");
  } catch (...) {
    result.ejectFailed = true;
    if (!result.errorMessage.empty()) {
      result.errorMessage += "; ";
    }
    result.errorMessage += "Failed to dismount tape: " + exception::currentExceptionMessage();
  }
  return result;
}

void DriveCleaner::logAndClearTapeAlerts(drive::DriveInterface& drive) noexcept {
  std::string errorMessage;
  try {
    if (std::vector<uint16_t> tapeAlertCodes = drive.getTapeAlertCodes(); !tapeAlertCodes.empty()) {
      for (const auto code : tapeAlertCodes) {
        m_tracker.incrementTapeAlert(code);
      }
      size_t alertNumber = 0;
      // Log tape alerts in the logs.
      std::vector<std::string> tapeAlerts = drive.getTapeAlerts(tapeAlertCodes);
      for (std::vector<std::string>::iterator ta = tapeAlerts.begin(); ta != tapeAlerts.end(); ++ta) {
        log::ScopedParamContainer params(m_lc);
        params.add("tapeAlert", *ta).add("tapeAlertNumber", alertNumber++).add("tapeAlertCount", tapeAlerts.size());
        m_lc.log(log::WARNING, "Tape alert detected");
      }
    }
    return;
  } catch (exception::Exception& ex) {
    errorMessage = ex.getMessage().str();
  } catch (std::exception& se) {
    errorMessage = se.what();
  } catch (...) {
    errorMessage = "Caught an unknown exception";
  }

  // Reaching this point means it failed and an exception was thrown (because of the "return" above)
  log::ScopedParamContainer params(m_lc);
  params.add(semconv::log::exceptionMessage, errorMessage);
  m_lc.log(log::WARNING, "Cleaner failed to get tape alerts from the drive");
}

std::unique_ptr<drive::DriveInterface> DriveCleaner::createDrive(System::virtualWrapper& sysWrapper) {
  SCSI::DeviceVector dv(sysWrapper);
  SCSI::DeviceInfo driveInfo = dv.findBySymlink(m_driveInfo.devFilename);

  // Instantiate the drive object
  std::unique_ptr<drive::DriveInterface> drive(drive::createDrive(driveInfo, sysWrapper));

  if (nullptr == drive.get()) {
    exception::Exception ex;
    ex.getMessage() << "Failed to instantiate drive object";
    throw ex;
  }

  return drive;
}

void DriveCleaner::waitForMediaToBeReady(drive::DriveInterface& drive) {
  CleanupTiming timing(m_tracker, &TapeCleanupStats::readinessWaitTime);
  log::ScopedParamContainer params(m_lc);
  params.add("waitMediaInDriveTimeout", m_tapeLoadTimeout);

  try {
    m_lc.log(log::DEBUG, "Cleaner waiting for drive to become ready");
    drive.waitUntilReady(m_tapeLoadTimeout);
    m_lc.log(log::DEBUG, "Cleaner detected that the drive is ready");
  } catch (...) {
    params.add(semconv::log::exceptionMessage, exception::currentExceptionMessage());
    m_lc.log(log::DEBUG,
             "Cleaner caught a non-fatal exception while waiting for"
             " drive to become ready");
  }
}

void DriveCleaner::rewindDrive(drive::DriveInterface& drive) {
  CleanupTiming timing(m_tracker, &TapeCleanupStats::rewindTime);
  m_lc.log(log::DEBUG, "Cleaner rewinding tape");
  drive.rewind();
  m_lc.log(log::DEBUG, "Cleaner rewound tape");
}

void DriveCleaner::checkTapeContainsData(drive::DriveInterface& drive) {
  m_lc.log(log::DEBUG, "Cleaner checking whether the tape contains data");
  if (drive.isTapeBlank()) {
    exception::Exception ex;
    ex.getMessage() << "Tape is completely blank when it should be labeled";
    throw ex;
  }
  m_lc.log(log::DEBUG, "Cleaner detected that the tape contains data");
}

std::optional<std::string> DriveCleaner::readVolumeLabel(drive::DriveInterface& drive) {
  CleanupTiming timing(m_tracker, &TapeCleanupStats::labelReadTime);
  log::ScopedParamContainer params(m_lc);

  checkTapeContainsData(drive);
  if (m_vid.empty()) {
    m_lc.log(log::DEBUG,
             "Cleaner cannot determine the volume label format without a VID; the robotic dismount will use an empty "
             "VID and bypass the cartridge-name consistency check");
    return std::nullopt;
  }

  using LabelFormat = common::dataStructures::Label::Format;
  const LabelFormat labelFormat = m_catalogue.Tape()->getTapeLabelFormat(m_vid);
  const std::string volumeLabelVSN = tapeFile::HeaderChecker::checkVolumeLabel(drive, labelFormat);
  params.add("volumeLabelVSN", volumeLabelVSN);
  m_lc.log(log::DEBUG, "Cleaner read the VSN from the volume label");
  return volumeLabelVSN;
}

void DriveCleaner::unloadTape(drive::DriveInterface& drive) {
  m_lc.log(log::DEBUG, "Cleaner unloading tape");
  utils::Timer timer;
  try {
    drive.unloadTape();
  } catch (...) {
    m_tracker.addTapeCleanupStats({.unloadTime = timer.secs()});
    m_tracker.recordFailure(TapeSessionFailure::TapeUnload);
    throw;
  }
  m_tracker.addTapeCleanupStats({.unloadTime = timer.secs()});
  m_lc.log(log::DEBUG, "Cleaner unloaded tape");
}

void DriveCleaner::dismountTape(const std::string& vid) {
  const auto librarySlot = mediachanger::LibrarySlotParser::parse(m_driveInfo.rawLibrarySlot);
  log::ScopedParamContainer params(m_lc);
  params.add("dismountVid", vid).add("librarySlot", librarySlot.str());

  if (vid.empty()) {
    m_lc.log(log::DEBUG,
             "Cleaner requesting robotic tape dismount with an empty VID, bypassing the cartridge-name consistency "
             "check");
  } else {
    m_lc.log(log::DEBUG, "Cleaner requesting robotic tape dismount");
  }
  utils::Timer timer;
  try {
    m_mediachanger.dismountTape(vid, librarySlot);
  } catch (...) {
    m_tracker.addTapeCleanupStats({.unmountTime = timer.secs()});
    m_tracker.recordFailure(TapeSessionFailure::TapeDismount);
    throw;
  }
  m_tracker.addTapeCleanupStats({.unmountTime = timer.secs()});
  m_lc.log(log::DEBUG, "Cleaner dismounted tape");
}

}  // namespace cta::tape::daemon
