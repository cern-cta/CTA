/******************************************************************************
 *
 * This file is part of the Castor project.
 * See http://castor.web.cern.ch/castor
 *
 * Copyright (C) 2003  CERN
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
 *
 * 
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#include "castor/tape/tapeserver/daemon/CleanerSession.hpp"

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
  const std::string & externalEncryptionKeyScript):
  m_capUtils(capUtils),
  m_mc(mc),
  m_log(log),
  m_driveConfig(driveConfig),
  m_sysWrapper(sysWrapper),
  m_vid(vid),
  m_waitMediaInDrive(waitMediaInDrive),
  m_waitMediaInDriveTimeout(waitMediaInDriveTimeout),
  m_encryptionControl(externalEncryptionKeyScript) {}

//------------------------------------------------------------------------------
// execute
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::Session::EndOfSessionAction
  castor::tape::tapeserver::daemon::CleanerSession::execute() throw() {
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
  m_log(cta::log::ERR, "Cleaner failed", params);
  return MARK_DRIVE_AS_DOWN;
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
    throw;
  }
  
  logAndClearTapeAlerts(drive);
  return MARK_DRIVE_AS_UP;
}

//------------------------------------------------------------------------------
// cleanDrive
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::CleanerSession::cleanDrive(drive::DriveInterface &drive) {
  if(m_waitMediaInDrive) {
    waitUntilMediaIsReady(drive);
  }

  if(!drive.hasTapeInPlace()) {
    std::list<cta::log::Param> params;
    params.push_back(cta::log::Param("tapeVid", m_vid));
    params.push_back(cta::log::Param("tapeDrive", m_driveConfig.unitName));
    m_log(cta::log::INFO, "Cleaner found tape drive empty", params);
    return; //return immediately if there is no tape
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
void castor::tape::tapeserver::daemon::CleanerSession::logAndClearTapeAlerts(drive::DriveInterface &drive) throw() {
  std::string errorMessage;
  try{
    std::vector<uint16_t> tapeAlertCodes = drive.getTapeAlertCodes();
    if (!tapeAlertCodes.empty()) {
      size_t alertNumber = 0;
      // Log tape alerts in the logs.
      std::vector<std::string> tapeAlerts = drive.getTapeAlerts(tapeAlertCodes);
      for (std::vector<std::string>::iterator ta=tapeAlerts.begin(); ta!=tapeAlerts.end();ta++)
      {
        std::list<cta::log::Param> params = {
          cta::log::Param("tapeAlert",*ta),
          cta::log::Param("tapeAlertNumber", alertNumber++),
          cta::log::Param("tapeAlertCount", tapeAlerts.size())};
        m_log(cta::log::WARNING, "Tape alert detected",params);
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

  if(NULL == drive.get()) {
    cta::exception::Exception ex;
    ex.getMessage() << "Failed to instantiate drive object";
    throw ex;
  } 
    
  return drive;
}

//------------------------------------------------------------------------------
// waitUntilDriveIsReady
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::CleanerSession::waitUntilMediaIsReady(
  drive::DriveInterface &drive) {  
  std::list<cta::log::Param> params;
  params.push_back(cta::log::Param("tapeVid", m_vid));
  params.push_back(cta::log::Param("tapeDrive", m_driveConfig.unitName));
  params.push_back(cta::log::Param("waitMediaInDriveTimeout",
    m_waitMediaInDriveTimeout));

  try {
    m_log(cta::log::INFO, "Cleaner waiting for drive to be ready", params);
    drive.waitUntilReady(m_waitMediaInDriveTimeout);
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
void castor::tape::tapeserver::daemon::CleanerSession::rewindDrive(
  drive::DriveInterface &drive) {
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
void castor::tape::tapeserver::daemon::CleanerSession::checkTapeContainsData(
  drive::DriveInterface &drive) {
  std::list<cta::log::Param> params;
  params.push_back(cta::log::Param("tapeVid", m_vid));
  params.push_back(cta::log::Param("tapeDrive", m_driveConfig.unitName));

  m_log(cta::log::INFO, "Cleaner checking tape contains data", params);
  if(drive.isTapeBlank()) {
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
  tapeFile::VOL1 vol1;
  std::list<cta::log::Param> params;
  params.push_back(cta::log::Param("tapeVid", m_vid));
  params.push_back(cta::log::Param("tapeDrive", m_driveConfig.unitName));
  
  try {
    drive.readExactBlock((void * )&vol1, sizeof(vol1),
      "[CleanerSession::clean()] - Reading header VOL1");
    vol1.verify();

    const std::string &volumeLabelVSN = vol1.getVSN();
    params.push_back(cta::log::Param("volumeLabelVSN", volumeLabelVSN));

    m_log(cta::log::INFO, "Cleaner read VSN from volume label", params);

    // If the cleaner was given a VID
    if(!m_vid.empty()) {
      if(m_vid == volumeLabelVSN) {
        m_log(cta::log::INFO, "Cleaner detected volume label contains expected VSN",
          params);
      } else {
        m_log(cta::log::WARNING,
          "Cleaner detected volume label does not contain expected VSN", params);
      }
    }

    return volumeLabelVSN;
  } catch(cta::exception::Exception &ne) {
    cta::exception::Exception ex;
    ex.getMessage() << "Failed to check volume label: " << ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// unloadTape
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::CleanerSession::unloadTape(
  const std::string &vid, drive::DriveInterface &drive) {
  const cta::mediachanger::LibrarySlot &librarySlot = m_driveConfig.librarySlot();
  std::list<cta::log::Param> params;
  params.push_back(cta::log::Param("tapeVid", vid));
  params.push_back(cta::log::Param("tapeDrive", m_driveConfig.unitName));
  params.push_back(cta::log::Param("librarySlot", librarySlot.str()));

  // We implement the same policy as with the tape sessions: 
  // if the librarySlot parameter is "manual", do nothing.
  if(cta::mediachanger::TAPE_LIBRARY_TYPE_MANUAL == librarySlot.getLibraryType()) {
    m_log(cta::log::INFO, "Cleaner not unloading tape because media changer is"
      " manual", params);
    return;
  }

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
void castor::tape::tapeserver::daemon::CleanerSession::dismountTape(
  const std::string &vid) {
  const cta::mediachanger::LibrarySlot &librarySlot = m_driveConfig.librarySlot();
  std::list<cta::log::Param> params;
  params.push_back(cta::log::Param("tapeVid", vid));
  params.push_back(cta::log::Param("tapeDrive", m_driveConfig.unitName));
  params.push_back(cta::log::Param("librarySlot", librarySlot.str()));

  try {
    m_mc.dismountTape(vid, librarySlot);
    const bool dismountWasManual = cta::mediachanger::TAPE_LIBRARY_TYPE_MANUAL ==
      librarySlot.getLibraryType();
    if(dismountWasManual) {
      m_log(cta::log::INFO, "Cleaner did not dismount tape because media changer is"
        " manual", params);
    } else {
      m_log(cta::log::INFO, "Cleaner dismounted tape", params);
    }
  } catch(cta::exception::Exception &ne) {
    cta::exception::Exception ex;
    ex.getMessage() << "Cleaner failed to dismount tape: " <<
      ne.getMessage().str();
    throw ex;
  }
}
