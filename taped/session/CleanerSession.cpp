/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "CleanerSession.hpp"

#include "catalogue/Catalogue.hpp"
#include "common/process/ProcessCap.hpp"
#include "common/utils/utils.hpp"
#include "mediachanger/LibrarySlotParser.hpp"
#include "taped/file/HeaderChecker.hpp"

#include <exception>
#include <optional>
#include <vector>

namespace {

std::string currentExceptionMessage() {
  try {
    throw;
  } catch (const cta::exception::Exception& ex) {
    return ex.getMessageValue();
  } catch (const std::exception& ex) {
    return ex.what();
  } catch (...) {
    return "Caught an unknown exception";
  }
}

CTA_GENERATE_EXCEPTION_CLASS(CleanerEjectFailed);

}  // namespace

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::tape::daemon::CleanerSession::CleanerSession(cta::mediachanger::MediaChangerFacade& mc,
                                                  cta::log::Logger& log,
                                                  const cta::common::dataStructures::DriveInfo& driveInfo,
                                                  System::virtualWrapper& sysWrapper,
                                                  const std::string& vid,
                                                  const bool waitMediaInDrive,
                                                  const uint32_t waitMediaInDriveTimeout,
                                                  cta::catalogue::Catalogue& catalogue,
                                                  cta::Scheduler& scheduler)
    : m_mediachanger(mc),
      m_lc(log),
      m_driveInfo(driveInfo),
      m_sysWrapper(sysWrapper),
      m_vid(vid),
      m_waitMediaInDrive(waitMediaInDrive),
      m_tapeLoadTimeout(waitMediaInDriveTimeout),
      m_catalogue(catalogue),
      m_scheduler(scheduler) {
  m_lc.push(cta::log::Param("tapeVid", m_vid));
  m_lc.push(cta::log::Param("tapeDrive", m_driveInfo.driveName));
}

//------------------------------------------------------------------------------
// execute
//------------------------------------------------------------------------------
cta::tape::daemon::Session::EndOfSessionAction cta::tape::daemon::CleanerSession::execute() {
  std::string errorMessage;
  bool ejectFailed = false;

  if (!server::ProcessCap::hasRawIoCap()) {
    m_lc.log(cta::log::ERR, "Missing CAP_SYS_RAWIO capability. Unable to use raw tape drive I/O.");
  }

  // First open the drive. If that is impossible, the robot can still return the cartridge
  // because we don't need the drive for that
  std::unique_ptr<drive::DriveInterface> drivePtr;
  std::optional<std::string> driveError;
  try {
    drivePtr = createDrive();
  } catch (...) {
    driveError = currentExceptionMessage();
  }

  if (driveError) {
    try {
      dismountTape("");
      // The tape was safely dismounted, but a drive that could not be opened must not be marked up
      errorMessage = "Tape was dismounted, but the drive could not be opened and must remain down: " + *driveError;
    } catch (...) {
      errorMessage = "Failed to create the drive (" + *driveError + ") and failed to dismount the tape ("
                     + currentExceptionMessage() + ")";
      ejectFailed = true;
    }
  } else {
    drive::DriveInterface& drive = *drivePtr;
    try {
      cleanDrive(drive);
    } catch (...) {
      errorMessage = currentExceptionMessage();
      ejectFailed = true;
    }

    // Read and clear tape alerts whether cleaning succeeded or failed
    logAndClearTapeAlerts(drive);
    if (errorMessage.empty()) {
      m_lc.log(cta::log::INFO, "Cleaner completed successfully");
      return MARK_DRIVE_AS_UP;
    }
  }

  // Reaching this point means the cleaner failed

  if (ejectFailed) {
    // As we failed to eject, we set the tape as disabled so that it will not be mounted for future retrieves
    // otherwise, we will go in an infinite loop of mounting with errors.
    // Gitlab ticket reference : https://gitlab.cern.ch/cta/CTA/issues/224
    disableTapeAfterFailedEject(errorMessage);
  }
  setDriveDownAfterCleanerFailed(errorMessage);

  return MARK_DRIVE_AS_DOWN;
}

void cta::tape::daemon::CleanerSession::setDriveDownAfterCleanerFailed(const std::string& errorMsg) noexcept {
  {
    cta::log::ScopedParamContainer params(m_lc);
    params.add(cta::semconv::log::exceptionMessage, errorMsg);
    m_lc.log(cta::log::ERR, "Cleaner failed; the drive is going down");
  }

  try {
    m_scheduler.reportDriveStatus(m_driveInfo,
                                  cta::common::dataStructures::MountType::NoMount,
                                  cta::common::dataStructures::DriveStatus::Down,
                                  m_lc);
    cta::common::dataStructures::DesiredDriveState driveState;
    driveState.up = false;
    driveState.forceDown = false;
    driveState.setReasonFromLogMsg(cta::log::ERR, "Cleaner failed: " + errorMsg);
    m_scheduler.setDesiredDriveState(m_driveInfo.driveName, driveState, m_lc);
  } catch (...) {
    cta::log::ScopedParamContainer params(m_lc);
    params.add(cta::semconv::log::exceptionMessage, currentExceptionMessage());
    m_lc.log(cta::log::ERR, "Cleaner failed to put the drive down");
  }
}

void cta::tape::daemon::CleanerSession::disableTapeAfterFailedEject(const std::string& errorMsg) noexcept {
  if (m_vid.empty()) {
    m_lc.log(cta::log::WARNING, "Cleaner cannot disable tape after failed eject because its VID is unknown");
    return;
  }

  cta::log::ScopedParamContainer params(m_lc);
  try {
    using Tape = cta::common::dataStructures::Tape;
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
        cta::log::WARNING,
        "Cleaner did not change tape state after failed eject. Current tape state cannot be disabled automatically.");
      return;
    }

    cta::common::dataStructures::SecurityIdentity admin;
    admin.username = "cta-taped " + m_driveInfo.driveName;
    admin.host = cta::utils::getShortHostname();
    const std::string reason = cta::utils::getCurrentLocalTime("%F %T") + ": " + errorMsg;
    m_catalogue.Tape()->modifyTapeState(admin, m_vid, *disabledState, tape.state, reason);
    params.add("newTapeState", Tape::stateToString(*disabledState));
    m_lc.log(cta::log::WARNING, "Cleaner disabled tape after failed eject");
  } catch (...) {
    params.add(cta::semconv::log::exceptionMessage, currentExceptionMessage());
    m_lc.log(cta::log::ERR, "Cleaner failed to disable tape after failed eject");
  }
}

//------------------------------------------------------------------------------
// cleanDrive
//------------------------------------------------------------------------------
void cta::tape::daemon::CleanerSession::cleanDrive(drive::DriveInterface& drive) {
  // Cleaning has two independent stages: unload from the tape mechanism, then
  // return the cartridge from the library drive element to its storage slot.
  if (m_waitMediaInDrive) {
    waitForMediaToBeReady(drive);
  }

  try {
    if (!drive.hasTapeInPlace()) {
      m_lc.log(cta::log::INFO, "Cleaner found no tape in the drive; nothing to do");
      return;
    }
  } catch (...) {
    cta::log::ScopedParamContainer params(m_lc);
    params.add(cta::semconv::log::exceptionMessage, currentExceptionMessage());
    m_lc.log(cta::log::WARNING, "Cleaner could not determine whether the drive contains a tape");
  }

  std::optional<std::string> volumeLabel;
  try {
    // Encryption keys and LBP mode can survive a failed session and affect later tapes
    drive.clearEncryptionKey();
    rewindDrive(drive);
    drive.disableLogicalBlockProtection();
    volumeLabel = readVolumeLabel(drive);
  } catch (...) {
    cta::log::ScopedParamContainer params(m_lc);
    params.add(cta::semconv::log::exceptionMessage, currentExceptionMessage());
    m_lc.log(cta::log::WARNING,
             "Cleaner failed to prepare the drive or read the volume label; continuing with the provided VID");
  }

  try {
    unloadTape(drive);
  } catch (...) {
    cta::log::ScopedParamContainer params(m_lc);
    params.add(cta::semconv::log::exceptionMessage, currentExceptionMessage());
    m_lc.log(cta::log::WARNING, "Cleaner unload command failed; attempting robotic dismount");
  }
  std::string dismountVid = m_vid;
  if (volumeLabel) {
    if (!m_vid.empty() && *volumeLabel != m_vid) {
      cta::log::ScopedParamContainer params(m_lc);
      params.add("volumeLabelVSN", *volumeLabel);
      m_lc.log(cta::log::ERR, "Cleaner detected volume label does not match provided VID");
    }
    dismountVid = *volumeLabel;
  }

  if (!dismountVid.empty()) {
    try {
      dismountTape(dismountVid);
      return;
    } catch (...) {
      cta::log::ScopedParamContainer params(m_lc);
      params.add("dismountVid", dismountVid);
      params.add(cta::semconv::log::exceptionMessage, currentExceptionMessage());
      m_lc.log(cta::log::WARNING,
               "Cleaner failed to dismount tape with VID; retrying with an empty VID to bypass the robot's "
               "cartridge-name consistency check");
    }
  }

  // An empty VID asks the robot to move the cartridge without checking its name.
  try {
    dismountTape("");
  } catch (...) {
    throw CleanerEjectFailed("Failed to dismount tape: " + currentExceptionMessage());
  }
}

//------------------------------------------------------------------------------
// logAndClearTapeAlerts
//------------------------------------------------------------------------------
void cta::tape::daemon::CleanerSession::logAndClearTapeAlerts(drive::DriveInterface& drive) noexcept {
  std::string errorMessage;
  try {
    if (std::vector<uint16_t> tapeAlertCodes = drive.getTapeAlertCodes(); !tapeAlertCodes.empty()) {
      size_t alertNumber = 0;
      // Log tape alerts in the logs.
      std::vector<std::string> tapeAlerts = drive.getTapeAlerts(tapeAlertCodes);
      for (std::vector<std::string>::iterator ta = tapeAlerts.begin(); ta != tapeAlerts.end(); ++ta) {
        cta::log::ScopedParamContainer params(m_lc);
        params.add("tapeAlert", *ta).add("tapeAlertNumber", alertNumber++).add("tapeAlertCount", tapeAlerts.size());
        m_lc.log(cta::log::WARNING, "Tape alert detected");
      }
    }
    return;
  } catch (cta::exception::Exception& ex) {
    errorMessage = ex.getMessage().str();
  } catch (std::exception& se) {
    errorMessage = se.what();
  } catch (...) {
    errorMessage = "Caught an unknown exception";
  }

  // Reaching this point means it failed and an exception was thrown (because of the "return" above)
  cta::log::ScopedParamContainer params(m_lc);
  params.add(cta::semconv::log::exceptionMessage, errorMessage);
  m_lc.log(cta::log::WARNING, "Cleaner failed to get tape alerts from the drive");
}

//------------------------------------------------------------------------------
// createDrive
//------------------------------------------------------------------------------
std::unique_ptr<cta::tape::drive::DriveInterface> cta::tape::daemon::CleanerSession::createDrive() {
  SCSI::DeviceVector dv(m_sysWrapper);
  SCSI::DeviceInfo driveInfo = dv.findBySymlink(m_driveInfo.devFilename);

  // Instantiate the drive object
  std::unique_ptr<cta::tape::drive::DriveInterface> drive(drive::createDrive(driveInfo, m_sysWrapper));

  if (nullptr == drive.get()) {
    cta::exception::Exception ex;
    ex.getMessage() << "Failed to instantiate drive object";
    throw ex;
  }

  return drive;
}

//------------------------------------------------------------------------------
// waitUntilDriveIsReady
//------------------------------------------------------------------------------
void cta::tape::daemon::CleanerSession::waitForMediaToBeReady(drive::DriveInterface& drive) {
  cta::log::ScopedParamContainer params(m_lc);
  params.add("waitMediaInDriveTimeout", m_tapeLoadTimeout);

  try {
    m_lc.log(cta::log::DEBUG, "Cleaner waiting for drive to become ready");
    drive.waitUntilReady(m_tapeLoadTimeout);
    m_lc.log(cta::log::DEBUG, "Cleaner detected that the drive is ready");
  } catch (...) {
    params.add(cta::semconv::log::exceptionMessage, currentExceptionMessage());
    m_lc.log(cta::log::DEBUG,
             "Cleaner caught a non-fatal exception while waiting for"
             " drive to become ready");
  }
}

//------------------------------------------------------------------------------
// rewindDrive
//------------------------------------------------------------------------------
void cta::tape::daemon::CleanerSession::rewindDrive(drive::DriveInterface& drive) {
  m_lc.log(cta::log::DEBUG, "Cleaner rewinding tape");
  drive.rewind();
  m_lc.log(cta::log::DEBUG, "Cleaner rewound tape");
}

//------------------------------------------------------------------------------
// checkTapeContainsData
//------------------------------------------------------------------------------
void cta::tape::daemon::CleanerSession::checkTapeContainsData(drive::DriveInterface& drive) {
  m_lc.log(cta::log::DEBUG, "Cleaner checking whether the tape contains data");
  if (drive.isTapeBlank()) {
    cta::exception::Exception ex;
    ex.getMessage() << "Tape is completely blank when it should be labeled";
    throw ex;
  }
  m_lc.log(cta::log::DEBUG, "Cleaner detected that the tape contains data");
}

//------------------------------------------------------------------------------
// readVolumeLabel
//------------------------------------------------------------------------------
std::optional<std::string> cta::tape::daemon::CleanerSession::readVolumeLabel(drive::DriveInterface& drive) {
  cta::log::ScopedParamContainer params(m_lc);

  checkTapeContainsData(drive);
  if (m_vid.empty()) {
    m_lc.log(cta::log::DEBUG,
             "Cleaner cannot determine the volume label format without a VID; the robotic dismount will use an empty "
             "VID and bypass the cartridge-name consistency check");
    return std::nullopt;
  }

  using LabelFormat = cta::common::dataStructures::Label::Format;
  const LabelFormat labelFormat = m_catalogue.Tape()->getTapeLabelFormat(m_vid);
  const std::string volumeLabelVSN = tapeFile::HeaderChecker::checkVolumeLabel(drive, labelFormat);
  params.add("volumeLabelVSN", volumeLabelVSN);
  m_lc.log(cta::log::DEBUG, "Cleaner read the VSN from the volume label");
  return volumeLabelVSN;
}

//------------------------------------------------------------------------------
// unloadTape
//------------------------------------------------------------------------------
void cta::tape::daemon::CleanerSession::unloadTape(drive::DriveInterface& drive) {
  m_lc.log(cta::log::DEBUG, "Cleaner unloading tape");
  drive.unloadTape();
  m_lc.log(cta::log::DEBUG, "Cleaner unloaded tape");
}

//------------------------------------------------------------------------------
// dismountTape
//------------------------------------------------------------------------------
void cta::tape::daemon::CleanerSession::dismountTape(const std::string& vid) {
  const auto librarySlot = cta::mediachanger::LibrarySlotParser::parse(m_driveInfo.rawLibrarySlot);
  cta::log::ScopedParamContainer params(m_lc);
  params.add("dismountVid", vid).add("librarySlot", librarySlot.str());

  if (vid.empty()) {
    m_lc.log(cta::log::DEBUG,
             "Cleaner requesting robotic tape dismount with an empty VID, bypassing the cartridge-name consistency "
             "check");
  } else {
    m_lc.log(cta::log::DEBUG, "Cleaner requesting robotic tape dismount");
  }
  m_mediachanger.dismountTape(vid, librarySlot);
  m_lc.log(cta::log::DEBUG, "Cleaner dismounted tape");
}
