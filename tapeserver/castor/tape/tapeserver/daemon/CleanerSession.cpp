/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
 */

#include <exception>
#include <list>
#include <vector>

#include "castor/tape/tapeserver/daemon/CleanerSession.hpp"
#include "castor/tape/tapeserver/file/HeaderChecker.hpp"
#include "catalogue/Catalogue.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::CleanerSession::CleanerSession(
  cta::server::ProcessCap &capUtils,
  cta::mediachanger::MediaChangerFacade &mc,
  cta::log::Logger &log,
  const cta::tape::daemon::TpconfigLine &driveConfig,
  System::virtualWrapper &sysWrapper,
  const std::string &vid,
  const bool waitMediaInDrive,
  const uint32_t waitMediaInDriveTimeout,
  const std::string & externalEncryptionKeyScript,
  cta::catalogue::Catalogue & catalogue,
  cta::Scheduler & scheduler):
  m_capUtils(capUtils),
  m_mc(mc),
  m_log(log),
  m_driveConfig(driveConfig),
  m_sysWrapper(sysWrapper),
  m_vid(vid),
  m_waitMediaInDrive(waitMediaInDrive),
  m_tapeLoadTimeout(waitMediaInDriveTimeout),
  m_encryptionControl(true, externalEncryptionKeyScript),
  m_catalogue(catalogue),
  m_scheduler(scheduler)
  {}

//------------------------------------------------------------------------------
// execute
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::Session::EndOfSessionAction
  castor::tape::tapeserver::daemon::CleanerSession::execute() noexcept {
  std::string errorMessage;

  try {
    return exceptionThrowingExecute();
  } catch(cta::exception::Exception &ex) {
    errorMessage = ex.getMessage().str();
  } catch(std::exception &se) {
    errorMessage = se.what();
  } catch(...) {
    errorMessage = "Caught an unknown exception";
  }

  // Reaching this point means the cleaner failed and an exception was thrown
  std::list<cta::log::Param> params = {
    cta::log::Param("tapeVid", m_vid),
    cta::log::Param("tapeDrive", m_driveConfig.unitName),
    cta::log::Param("message", errorMessage)};
  m_log(cta::log::ERR, "Cleaner failed, the drive is going down.", params);

  // Putting the drive down
  try {
    setDriveDownAfterCleanerFailed(std::string("Cleaner failed. ") + errorMessage);
  } catch(const cta::exception::Exception &ex) {
    std::list<cta::log::Param> params = {
    cta::log::Param("tapeVid", m_vid),
    cta::log::Param("tapeDrive", m_driveConfig.unitName),
    cta::log::Param("message", ex.getMessageValue())};
    m_log(cta::log::ERR, "Cleaner failed. Failed to put the drive down", params);
  }

  return MARK_DRIVE_AS_DOWN;
}

void castor::tape::tapeserver::daemon::CleanerSession::setDriveDownAfterCleanerFailed(const std::string & errorMsg) {

  std::string logicalLibrary =  m_driveConfig.logicalLibrary;
  std::string hostname = cta::utils::getShortHostname();
  std::string driveName = m_driveConfig.unitName;

  cta::common::dataStructures::DriveInfo driveInfo;
  driveInfo.driveName = driveName;
  driveInfo.logicalLibrary = logicalLibrary;
  driveInfo.host = hostname;

  cta::log::LogContext lc(m_log);

  m_scheduler.reportDriveStatus(driveInfo, cta::common::dataStructures::MountType::NoMount,
    cta::common::dataStructures::DriveStatus::Down, lc);
  cta::common::dataStructures::SecurityIdentity cliId;
  cta::common::dataStructures::DesiredDriveState driveState;
  driveState.up = false;
  driveState.forceDown = false;
  driveState.setReasonFromLogMsg(cta::log::ERR, errorMsg);
  m_scheduler.setDesiredDriveState(cliId, m_driveConfig.unitName, driveState, lc);
}

//------------------------------------------------------------------------------
// exceptionThrowingExecute
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::Session::EndOfSessionAction
  castor::tape::tapeserver::daemon::CleanerSession::exceptionThrowingExecute() {
  setProcessCapabilities("cap_sys_rawio+ep");

  std::unique_ptr<drive::DriveInterface> drivePtr = createDrive();
  drive::DriveInterface &drive = *drivePtr.get();

  try {
    cleanDrive(drive);
  } catch(...) {
    logAndClearTapeAlerts(drive);
    // As we failed to clean the drive (unmount the tape or rewinding impossible),
    // we set the tape as disabled so that it will not be mounted for future retrieves
    // otherwise, we will go in an infinite loop of mounting with errors.
    // Gitlab ticket reference : https://gitlab.cern.ch/cta/CTA/issues/224
    if (!m_vid.empty()) {
      // Get the message exception to log it into the comment section of the tape table
      auto currException = std::current_exception();
      std::string currentExceptionMsg = "";
      try {
        std::rethrow_exception(currException);
      } catch (cta::exception::Exception &ex) {
        currentExceptionMsg = ex.getMessageValue();
      } catch (std::exception & ex) {
        currentExceptionMsg = ex.what();
      } catch (...) {
        currentExceptionMsg = "Unknown exception";
      }

      std::string hostname = cta::utils::getShortHostname();
      std::string tapeDrive = m_driveConfig.unitName;
      std::list<cta::log::Param> params = {
        cta::log::Param("tapeVid", m_vid),
        cta::log::Param("tapeDrive", tapeDrive),
        cta::log::Param("logicalLibrary", m_driveConfig.logicalLibrary),
        cta::log::Param("host", hostname),
        cta::log::Param("exceptionMsg", currentExceptionMsg)};
      m_log(cta::log::ERR, "In CleanerSession::exceptionThrowingExecute(), failed to clean the Drive with a tape mounted. Disabling the tape.", params);
      cta::common::dataStructures::SecurityIdentity admin;
      admin.username = c_defaultUserNameUpdate + " " + tapeDrive;
      admin.host = hostname;

      try {
        using cta::common::dataStructures::Tape;
        std::string disabledReason = cta::utils::getCurrentLocalTime("%F %T") + ":" + currentExceptionMsg;
        auto curr_state = m_catalogue.Tape()->getTapesByVid(m_vid).at(m_vid).state;
        if (curr_state == Tape::REPACKING) {
          m_catalogue.Tape()->modifyTapeState(admin, m_vid, Tape::REPACKING_DISABLED, curr_state, disabledReason);
        } else if (curr_state == Tape::ACTIVE){
          m_catalogue.Tape()->modifyTapeState(admin, m_vid, Tape::DISABLED, curr_state, disabledReason);
        } else {
          cta::log::Param param("currState", curr_state);
          params.push_back(param);
          m_log(cta::log::ERR, "In CleanerSession::exceptionThrowingExecute(), failed to disable the tape. Current tape state can't be disabled automatically.", params);
        }
      } catch(cta::exception::Exception &ex) {
        cta::log::Param param("exceptionMsg", ex.getMessageValue());
        params.push_back(param);
        m_log(cta::log::ERR, "In CleanerSession::exceptionThrowingExecute(), failed to disable the tape.", params);
      }
    }
    throw;
  }

  logAndClearTapeAlerts(drive);
  return MARK_DRIVE_AS_UP;
}

//------------------------------------------------------------------------------
// cleanDrive
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::CleanerSession::cleanDrive(drive::DriveInterface &drive) {
  if (m_waitMediaInDrive) {
    waitUntilMediaIsReady(drive);
  }

  if (!drive.hasTapeInPlace()) {
    std::list<cta::log::Param> params;
    params.push_back(cta::log::Param("tapeVid", m_vid));
    params.push_back(cta::log::Param("tapeDrive", m_driveConfig.unitName));
    m_log(cta::log::INFO, "Cleaner found tape drive empty", params);
    return;  // return immediately if there is no tape
  }

  m_encryptionControl.disable(drive);

  rewindDrive(drive);
  drive.disableLogicalBlockProtection();

  checkTapeContainsData(drive);

  const std::string volumeLabelVSN = checkVolumeLabel(drive);

  unloadTape(volumeLabelVSN, drive);

  dismountTape(volumeLabelVSN);
}

//------------------------------------------------------------------------------
// logAndClearTapeAlerts
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::CleanerSession::logAndClearTapeAlerts(drive::DriveInterface &drive) noexcept {
  std::string errorMessage;
  try {
    std::vector<uint16_t> tapeAlertCodes = drive.getTapeAlertCodes();
    if (!tapeAlertCodes.empty()) {
      size_t alertNumber = 0;
      // Log tape alerts in the logs.
      std::vector<std::string> tapeAlerts = drive.getTapeAlerts(tapeAlertCodes);
      for (std::vector<std::string>::iterator ta = tapeAlerts.begin(); ta != tapeAlerts.end(); ++ta) {
        std::list<cta::log::Param> params = {
          cta::log::Param("tapeAlert", *ta),
          cta::log::Param("tapeAlertNumber", alertNumber++),
          cta::log::Param("tapeAlertCount", tapeAlerts.size())};
        m_log(cta::log::WARNING, "Tape alert detected", params);
      }
    }
    return;
  } catch(cta::exception::Exception &ex) {
    errorMessage = ex.getMessage().str();
  } catch(std::exception &se) {
    errorMessage = se.what();
  } catch(...) {
    errorMessage = "Caught an unknown exception";
  }

  // Reaching this point means it failed and an exception was thrown (because of the "return" above)
  std::list<cta::log::Param> params = {
    cta::log::Param("tapeVid", m_vid),
    cta::log::Param("tapeDrive", m_driveConfig.unitName),
    cta::log::Param("message", errorMessage)};
  m_log(cta::log::ERR, "Cleaner failed getting tape alerts from the drive", params);
}

//------------------------------------------------------------------------------
// setProcessCapabilities
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::CleanerSession::setProcessCapabilities(
  const std::string &capabilities) {
  m_capUtils.setProcText(capabilities);
  {
    std::list<cta::log::Param> params = {
      cta::log::Param("capabilities", m_capUtils.getProcText())};
    m_log(cta::log::INFO, "Cleaner set process capabilities for using tape",
      params);
  }
}

//------------------------------------------------------------------------------
// createDrive
//------------------------------------------------------------------------------
std::unique_ptr<castor::tape::tapeserver::drive::DriveInterface>
  castor::tape::tapeserver::daemon::CleanerSession::createDrive() {
  SCSI::DeviceVector dv(m_sysWrapper);
  SCSI::DeviceInfo driveInfo = dv.findBySymlink(m_driveConfig.devFilename);

  // Instantiate the drive object
  std::unique_ptr<castor::tape::tapeserver::drive::DriveInterface>
    drive(drive::createDrive(driveInfo, m_sysWrapper));

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
void castor::tape::tapeserver::daemon::CleanerSession::waitUntilMediaIsReady(drive::DriveInterface &drive) {
  std::list<cta::log::Param> params;
  params.push_back(cta::log::Param("tapeVid", m_vid));
  params.push_back(cta::log::Param("tapeDrive", m_driveConfig.unitName));
  params.push_back(cta::log::Param("waitMediaInDriveTimeout",
    m_tapeLoadTimeout));

  try {
    m_log(cta::log::INFO, "Cleaner waiting for drive to be ready", params);
    drive.waitUntilReady(m_tapeLoadTimeout);
    m_log(cta::log::INFO, "Cleaner detected drive is ready", params);
  } catch (cta::exception::Exception &ex) {
    params.push_back(cta::log::Param("message", ex.getMessage().str()));
    m_log(cta::log::INFO, "Cleaner caught non-fatal exception whilst waiting for"
      " drive to become ready", params);
  }
}

//------------------------------------------------------------------------------
// rewindDrive
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::CleanerSession::rewindDrive(drive::DriveInterface &drive) {
  std::list<cta::log::Param> params;
  params.push_back(cta::log::Param("tapeVid", m_vid));
  params.push_back(cta::log::Param("tapeDrive", m_driveConfig.unitName));

  m_log(cta::log::INFO, "Cleaner rewinding tape", params);
  drive.rewind();
  m_log(cta::log::INFO, "Cleaner successfully rewound tape", params);
}

//------------------------------------------------------------------------------
// checkTapeContainsData
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::CleanerSession::checkTapeContainsData(drive::DriveInterface &drive) {
  std::list<cta::log::Param> params;
  params.push_back(cta::log::Param("tapeVid", m_vid));
  params.push_back(cta::log::Param("tapeDrive", m_driveConfig.unitName));

  m_log(cta::log::INFO, "Cleaner checking tape contains data", params);
  if (drive.isTapeBlank()) {
    cta::exception::Exception ex;
    ex.getMessage() << "Tape is completely blank when it should be labeled";
    throw ex;
  }
  m_log(cta::log::INFO, "Cleaner successfully detected tape contains data", params);
}

//------------------------------------------------------------------------------
// checkVolumeLabel
//------------------------------------------------------------------------------
std::string castor::tape::tapeserver::daemon::CleanerSession::checkVolumeLabel(
  drive::DriveInterface &drive) {
  std::list<cta::log::Param> params;
  params.push_back(cta::log::Param("tapeVid", m_vid));
  params.push_back(cta::log::Param("tapeDrive", m_driveConfig.unitName));

  using LabelFormat = cta::common::dataStructures::Label::Format;
  const LabelFormat labelFormat = m_catalogue.Tape()->getTapeLabelFormat(m_vid);

  const std::string volumeLabelVSN = tapeFile::HeaderChecker::checkVolumeLabel(drive, labelFormat);

  params.push_back(cta::log::Param("volumeLabelVSN", volumeLabelVSN));

  m_log(cta::log::INFO, "Cleaner read VSN from volume label", params);

  // If the cleaner was given a VID
  if (!m_vid.empty()) {
    if (m_vid == volumeLabelVSN) {
      m_log(cta::log::INFO, "Cleaner detected volume label contains expected VSN", params);
    } else {
      m_log(cta::log::WARNING, "Cleaner detected volume label does not contain expected VSN", params);
    }
  }
  return volumeLabelVSN;
}

//------------------------------------------------------------------------------
// unloadTape
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::CleanerSession::unloadTape(const std::string &vid,
  drive::DriveInterface &drive) {
  const cta::mediachanger::LibrarySlot &librarySlot = m_driveConfig.librarySlot();
  std::list<cta::log::Param> params;
  params.push_back(cta::log::Param("tapeVid", vid));
  params.push_back(cta::log::Param("tapeDrive", m_driveConfig.unitName));
  params.push_back(cta::log::Param("librarySlot", librarySlot.str()));

  try {
    m_log(cta::log::INFO, "Cleaner unloading tape", params);
    drive.unloadTape();
    m_log(cta::log::INFO, "Cleaner unloaded tape", params);
  } catch (cta::exception::Exception &ne) {
    cta::exception::Exception ex;
    ex.getMessage() << "Cleaner failed to unload tape: " <<
      ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// dismountTape
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::CleanerSession::dismountTape(const std::string &vid) {
  const cta::mediachanger::LibrarySlot &librarySlot = m_driveConfig.librarySlot();
  std::list<cta::log::Param> params;
  params.push_back(cta::log::Param("tapeVid", vid));
  params.push_back(cta::log::Param("tapeDrive", m_driveConfig.unitName));
  params.push_back(cta::log::Param("librarySlot", librarySlot.str()));

  try {
    m_mc.dismountTape(vid, librarySlot);
    m_log(cta::log::INFO, "Cleaner dismounted tape", params);
  } catch(cta::exception::Exception &ne) {
    cta::exception::Exception ex;
    ex.getMessage() << "Cleaner failed to dismount tape: " <<
      ne.getMessage().str();
    throw ex;
  }
}
