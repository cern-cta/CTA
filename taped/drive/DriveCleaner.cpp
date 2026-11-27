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
                           const uint32_t tapeUnloadTimeout,
                           catalogue::Catalogue& catalogue,
                           TapeSessionTracker& tracker)
    : m_tracker(tracker),
      m_mediachanger(mc),
      m_lc(log),
      m_driveInfo(driveInfo),
      m_vid(vid),
      m_tapeUnloadTimeout(tapeUnloadTimeout),
      m_catalogue(catalogue) {
  m_lc.push(log::Param("tapeVid", m_vid));
}

auto DriveCleaner::cleanDrive(System::virtualWrapper& sysWrapper) -> CleanupResult {
  const auto readinessDeadline = std::chrono::steady_clock::now() + m_tapeUnloadTimeout;
  // Include drive discovery and acquisition in the readiness budget.
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
    auto result = cleanDrive(*drive, readinessDeadline);
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
  if (result.ejectFailed) {
    disableTapeAfterFailedEject(result.errorMessage);
  }
  return result;
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
  return cleanDrive(drive, std::chrono::steady_clock::now() + m_tapeUnloadTimeout);
}

auto DriveCleaner::cleanDrive(drive::DriveInterface& drive, std::chrono::steady_clock::time_point readinessDeadline)
  -> CleanupResult {
  m_tracker.reportState(session::TapeSessionState::Unloading);
  CleanupTiming timing(m_tracker, &TapeCleanupStats::cleanupTime);
  CleanupResult result;
  // Refresh readiness before consulting the driver's media status.
  const bool ready = checkReadiness(drive, readinessDeadline, result);
  resetConfiguration(drive, result);

  // Empty drives still need their persistent configuration reset, but no tape movement.
  try {
    if (!drive.hasTapeInPlace()) {
      m_lc.log(log::INFO, "Cleaner found no tape in the drive; nothing to eject");
      return result;
    }
  } catch (...) {
    log::ScopedParamContainer params(m_lc);
    params.add(semconv::log::exceptionMessage, exception::currentExceptionMessage());
    m_lc.log(log::WARNING, "Cleaner could not determine whether the drive contains a tape; attempting tape eject");
  }

  const auto volumeLabel = ready ? readVolumeLabel(drive) : std::nullopt;
  unloadTape(drive);
  dismountCartridge(volumeLabel, result);
  if (result.ejectFailed) {
    disableTapeAfterFailedEject(result.errorMessage);
  }
  return result;
}

bool DriveCleaner::checkReadiness(drive::DriveInterface& drive,
                                  std::chrono::steady_clock::time_point readinessDeadline,
                                  CleanupResult& result) {
  try {
    CleanupTiming timing(m_tracker, &TapeCleanupStats::readinessWaitTime);
    m_lc.log(log::DEBUG, "Cleaner checking media readiness within the remaining budget");
    drive.waitUntilReady(readinessDeadline, drive::ReadinessWaitMode::ExistingMediaOnly);
    return true;
  } catch (const drive::MediaNotReady& ex) {
    log::ScopedParamContainer params(m_lc);
    params.add("readinessReason", ex.getMessageValue());
    m_lc.log(log::DEBUG, "Cleaner media is not ready; skipping label inspection and continuing with tape eject");
  } catch (...) {
    result.driveStateFailed = true;
    m_tracker.recordFailure(TapeSessionFailure::UnexpectedCleanup);
    if (!result.errorMessage.empty()) {
      result.errorMessage += "; ";
    }
    result.errorMessage += exception::currentExceptionMessage();
    log::ScopedParamContainer params(m_lc);
    params.add(semconv::log::exceptionMessage, result.errorMessage);
    m_lc.log(log::WARNING, "Cleaner could not establish drive state; continuing with tape eject");
  }

  return false;
}

void DriveCleaner::resetConfiguration(drive::DriveInterface& drive, CleanupResult& result) {
  // Reset persistent configuration even when the drive is empty.
  {
    CleanupTiming timing(m_tracker, &TapeCleanupStats::encryptionControlTime);
    try {
      drive.clearEncryptionKey();
    } catch (...) {
      const auto error = exception::currentExceptionMessage();
      m_tracker.recordFailure(TapeSessionFailure::TapeEncryptionDisable);
      result.configurationResetFailed = true;
      if (!result.errorMessage.empty()) {
        result.errorMessage += "; ";
      }
      result.errorMessage += "Failed to clear encryption key: " + error;
      log::ScopedParamContainer params(m_lc);
      params.add(semconv::log::exceptionMessage, error);
      m_lc.log(log::WARNING, "Failed to clear the tape encryption key; continuing cleanup.");
    }
  }
  {
    CleanupTiming timing(m_tracker, &TapeCleanupStats::lbpResetTime);
    try {
      drive.disableLogicalBlockProtection();
    } catch (...) {
      const auto error = exception::currentExceptionMessage();
      m_tracker.recordFailure(TapeSessionFailure::TapeLbpDisable);
      result.configurationResetFailed = true;
      if (!result.errorMessage.empty()) {
        result.errorMessage += "; ";
      }
      result.errorMessage += "Failed to disable logical block protection: " + error;
      log::ScopedParamContainer params(m_lc);
      params.add(semconv::log::exceptionMessage, error);
      m_lc.log(log::WARNING, "Failed to disable logical block protection; continuing with tape eject.");
    }
  }
}

void DriveCleaner::dismountCartridge(const std::optional<std::string>& volumeLabel, CleanupResult& result) {
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
      return;
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

std::optional<std::string> DriveCleaner::readVolumeLabel(drive::DriveInterface& drive) {
  try {
    {
      CleanupTiming timing(m_tracker, &TapeCleanupStats::rewindTime);
      m_lc.log(log::DEBUG, "Cleaner rewinding tape");
      drive.rewind();
      m_lc.log(log::DEBUG, "Cleaner rewound tape");
    }
    CleanupTiming timing(m_tracker, &TapeCleanupStats::labelReadTime);
    log::ScopedParamContainer params(m_lc);

    m_lc.log(log::DEBUG, "Cleaner checking whether the tape contains data");
    if (drive.isTapeBlank()) {
      throw exception::Exception("Tape is completely blank when it should be labeled");
    }
    m_lc.log(log::DEBUG, "Cleaner detected that the tape contains data");
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
  } catch (...) {
    log::ScopedParamContainer params(m_lc);
    params.add(semconv::log::exceptionMessage, exception::currentExceptionMessage());
    m_lc.log(log::WARNING,
             "Cleaner failed to prepare the drive or read the volume label; continuing with the provided VID");
    return std::nullopt;
  }
}

void DriveCleaner::unloadTape(drive::DriveInterface& drive) {
  m_lc.log(log::DEBUG, "Cleaner unloading tape");
  try {
    {
      CleanupTiming timing(m_tracker, &TapeCleanupStats::unloadTime);
      drive.unloadTape();
    }
    m_lc.log(log::DEBUG, "Cleaner unloaded tape");
  } catch (...) {
    m_tracker.recordFailure(TapeSessionFailure::TapeUnload);
    log::ScopedParamContainer params(m_lc);
    params.add(semconv::log::exceptionMessage, exception::currentExceptionMessage());
    m_lc.log(log::WARNING, "Cleaner unload command failed; attempting robotic dismount");
  }
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
  try {
    CleanupTiming timing(m_tracker, &TapeCleanupStats::unmountTime);
    m_mediachanger.dismountTape(vid, librarySlot);
  } catch (...) {
    m_tracker.recordFailure(TapeSessionFailure::TapeDismount);
    throw;
  }
  m_lc.log(log::DEBUG, "Cleaner dismounted tape");
}

}  // namespace cta::tape::daemon
