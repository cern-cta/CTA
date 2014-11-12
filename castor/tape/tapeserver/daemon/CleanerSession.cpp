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
  server::ProcessCap &capUtils,
  mediachanger::MediaChangerFacade &mc,
  castor::log::Logger &log,
  const utils::DriveConfig &driveConfig,
  System::virtualWrapper &sysWrapper,
  const std::string &vid,
  const uint32_t driveReadyDelayInSeconds):
  m_capUtils(capUtils),
  m_mc(mc),
  m_log(log),
  m_driveConfig(driveConfig),
  m_sysWrapper(sysWrapper),
  m_vid(vid),
  m_driveReadyDelayInSeconds(driveReadyDelayInSeconds) {
}

//------------------------------------------------------------------------------
// execute
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::Session::EndOfSessionAction
  castor::tape::tapeserver::daemon::CleanerSession::execute() {
  std::string errorMessage;

  try {
    return exceptionThrowingExecute();
  } catch(castor::exception::Exception &ex) {
    errorMessage = ex.getMessage().str();
  } catch(std::exception &se) {
    errorMessage = se.what();
  } catch(...) {
    errorMessage = "Caught an unknown exception";
  }

  // Reaching this point means the cleaner failed and an exception was thrown
  log::Param params[] = {
    log::Param("TPVID", m_vid),
    log::Param("unitName", m_driveConfig.unitName),
    log::Param("message", errorMessage)};
  m_log(LOG_ERR, "Cleaner failed", params);
  return MARK_DRIVE_AS_DOWN;
}

//------------------------------------------------------------------------------
// exceptionThrowingExecute
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::Session::EndOfSessionAction
  castor::tape::tapeserver::daemon::CleanerSession::exceptionThrowingExecute() {
  std::list<log::Param> params;
  params.push_back(log::Param("TPVID", m_vid));
  params.push_back(log::Param("unitName", m_driveConfig.unitName));

  setProcessCapabilities("cap_sys_rawio+ep");

  std::auto_ptr<tapeserver::drive::DriveInterface> drive(createDrive());

  waitUntilDriveIsReady(drive.get());

  if(!drive->hasTapeInPlace()) {
    m_log(LOG_INFO, "Cleaner found tape drive empty", params);
    return MARK_DRIVE_AS_UP;
  }

  rewindDrive(drive.get());

  checkTapeContainsData(drive.get());

  const std::string volumeLabelVSN = checkVolumeLabel(drive.get());

  unloadTape(volumeLabelVSN, drive.get());

  dismountTape(volumeLabelVSN);

  return MARK_DRIVE_AS_UP;
}

//------------------------------------------------------------------------------
// setProcessCapabilities
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::CleanerSession::setProcessCapabilities(
  const std::string &capabilities) {
  m_capUtils.setProcText(capabilities);
  {
    log::Param params[] = {
      log::Param("capabilities", m_capUtils.getProcText())};
    m_log(LOG_INFO, "Cleaner set process capabilities for using tape",
      params);
  }
}

//------------------------------------------------------------------------------
// createDrive
//------------------------------------------------------------------------------
castor::tape::tapeserver::drive::DriveInterface *
  castor::tape::tapeserver::daemon::CleanerSession::createDrive() {
  SCSI::DeviceVector dv(m_sysWrapper);    
  SCSI::DeviceInfo driveInfo = dv.findBySymlink(m_driveConfig.devFilename);
  drive::DriveInterface *const drive = drive::createDrive(driveInfo,
    m_sysWrapper);

  if(NULL == drive) {
    castor::exception::Exception ex;
    ex.getMessage() <<
      "Failed to instantiate drive object";
    throw ex;
  }

  return drive;
}

//------------------------------------------------------------------------------
// waitUntilDriveIsReady
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::CleanerSession::waitUntilDriveIsReady(
  drive::DriveInterface *const drive) {
  if(0 != m_driveReadyDelayInSeconds) {
    std::list<log::Param> params;
    params.push_back(log::Param("TPVID", m_vid));
    params.push_back(log::Param("unitName", m_driveConfig.unitName));
    params.push_back(log::Param("driveReadyDelayInSeconds",
      m_driveReadyDelayInSeconds));

    try {
      m_log(LOG_INFO, "Cleaner waiting for drive to be ready", params);
      drive->waitUntilReady(m_driveReadyDelayInSeconds);
      m_log(LOG_INFO, "Cleaner detected drive is ready", params);
    } catch (castor::exception::Exception &ex) {
      params.push_back(log::Param("message", ex.getMessage().str()));
      m_log(LOG_INFO, "Cleaner caught non-fatal exception whilst waiting for"
        " drive to become ready", params);
    }
  }
}

//------------------------------------------------------------------------------
// rewindDrive
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::CleanerSession::rewindDrive(
  drive::DriveInterface *const drive) {
  std::list<log::Param> params;
  params.push_back(log::Param("TPVID", m_vid));
  params.push_back(log::Param("unitName", m_driveConfig.unitName));

  m_log(LOG_INFO, "Cleaner rewinding drive", params);
  drive->rewind();
  m_log(LOG_INFO, "Cleaner successfully rewound drive", params);
}

//------------------------------------------------------------------------------
// checkTapeContainsData
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::CleanerSession::checkTapeContainsData(
  drive::DriveInterface *const drive) {
  std::list<log::Param> params;
  params.push_back(log::Param("TPVID", m_vid));
  params.push_back(log::Param("unitName", m_driveConfig.unitName));

  m_log(LOG_INFO, "Cleaner checking tape contains data", params);
  if(drive->isTapeBlank()) {
    castor::exception::Exception ex;
    ex.getMessage() << "Tape is completely blank when it should be labeled";
    throw ex;
  }
  m_log(LOG_INFO, "Cleaner successfully detected tape contains data", params);
}

//------------------------------------------------------------------------------
// checkVolumeLabel
//------------------------------------------------------------------------------
std::string castor::tape::tapeserver::daemon::CleanerSession::checkVolumeLabel(
  drive::DriveInterface *const drive) {
  tapeFile::VOL1 vol1;
  std::list<log::Param> params;
  params.push_back(log::Param("TPVID", m_vid));
  params.push_back(log::Param("unitName", m_driveConfig.unitName));
  
  try {
    drive->readExactBlock((void * )&vol1, sizeof(vol1),
      "[CleanerSession::clean()] - Reading header VOL1");
    vol1.verify();

    const std::string &volumeLabelVSN = vol1.getVSN();
    params.push_back(log::Param("volumeLabelVSN", volumeLabelVSN));

    m_log(LOG_INFO, "Cleaner read VSN from volume label", params);

    // If the cleaner was given a VID
    if(!m_vid.empty()) {
      if(m_vid == volumeLabelVSN) {
        m_log(LOG_INFO, "Cleaner detected volume label contains expected VSN",
          params);
      } else {
        m_log(LOG_WARNING,
          "Cleaner detected volume label does not contain expected VSN", params);
      }
    }

    return volumeLabelVSN;
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to check volume label: " << ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// unloadTape
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::CleanerSession::unloadTape(
  const std::string &vid, drive::DriveInterface *const drive) {
  std::list<log::Param> params;
  params.push_back(log::Param("TPVID", vid));
  params.push_back(log::Param("unitName", m_driveConfig.unitName));

  // We implement the same policy as with the tape sessions: 
  // if the librarySlot parameter is "manual", do nothing.
  if(mediachanger::TAPE_LIBRARY_TYPE_MANUAL ==
    m_driveConfig.librarySlot.getLibraryType()) {
    m_log(LOG_INFO, "Cleaner not unloading tape because media changer is"
      " manual", params);
    return;
  }

  try {
    m_log(LOG_INFO, "Cleaner unloading tape", params);
    drive->unloadTape();
    m_log(LOG_INFO, "Cleaner unloaded tape", params);
  } catch (castor::exception::Exception &ne) {
    castor::exception::Exception ex;
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
  std::list<log::Param> params;
  params.push_back(log::Param("TPVID", vid));
  params.push_back(log::Param("unitName", m_driveConfig.unitName));

  try {
    m_mc.dismountTape(vid, m_driveConfig.librarySlot.str());
    const bool dismountWasManual = mediachanger::TAPE_LIBRARY_TYPE_MANUAL ==
      m_driveConfig.librarySlot.getLibraryType();
    if(dismountWasManual) {
      m_log(LOG_INFO, "Cleaner did not dismount tape because media changer is"
        " manual", params);
    } else {
      m_log(LOG_INFO, "Cleaner dismounted tape", params);
    }
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Cleaner failed to dismount tape: " <<
      ne.getMessage().str();
    throw ex;
  }
}
