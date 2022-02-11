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

#include "common/log/LogContext.hpp"
#include "common/threading/System.hpp"
#include "castor/tape/tapeserver/daemon/LabelSession.hpp"
#include "castor/tape/tapeserver/daemon/LabelSessionConfig.hpp"
#include "castor/tape/tapeserver/drive/DriveInterface.hpp"
#include "castor/tape/tapeserver/file/File.hpp"
#include "castor/tape/tapeserver/file/Structures.hpp"
#include "castor/tape/tapeserver/SCSI/Device.hpp"
#include "common/exception/Exception.hpp"

#include <memory>

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::LabelSession::LabelSession(
  cta::server::ProcessCap &capUtils,
  cta::tape::daemon::TapedProxy &tapeserver,
  cta::mediachanger::MediaChangerFacade &mc, 
  const legacymsg::TapeLabelRqstMsgBody &clientRequest,
  cta::log::Logger &log,
  System::virtualWrapper &sysWrapper,
  const cta::tape::daemon::TpconfigLine &driveConfig,
  const bool force,
  const bool lbp,
  const LabelSessionConfig &labelSessionConfig,
  const std::string & externalEncryptionKeyScript):
  m_capUtils(capUtils),
  m_tapeserver(tapeserver),
  m_mc(mc),
  m_request(clientRequest),
  m_log(log),
  m_sysWrapper(sysWrapper),
  m_driveConfig(driveConfig),
  m_labelSessionConfig (labelSessionConfig),
  m_force(force),
  m_lbp(lbp),
  m_encryptionControl(false, externalEncryptionKeyScript) {}

//------------------------------------------------------------------------------
// execute
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::Session::EndOfSessionAction
  castor::tape::tapeserver::daemon::LabelSession::execute() throw() {
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

  // Reaching this point means the label session failed and an exception was
  // thrown
  std::list<cta::log::Param> params;
  params.push_back(cta::log::Param("uid", m_request.uid));
  params.push_back(cta::log::Param("gid", m_request.gid));
  params.push_back(cta::log::Param("tapeVid", m_request.vid));
  params.push_back(cta::log::Param("tapeDrive", m_request.drive));
  params.push_back(cta::log::Param("logicalLibrary", m_request.logicalLibrary));
  params.push_back(cta::log::Param("force", boolToStr(m_force)));
  params.push_back(cta::log::Param("lbp", boolToStr(m_lbp)));
  params.push_back(cta::log::Param("message", errorMessage));
  m_log(cta::log::ERR, "Label session failed", params);

  // Send details of exception to tapeserverd and then re-throw
  m_tapeserver.labelError(m_request.drive, errorMessage);

  return MARK_DRIVE_AS_DOWN;
}

//------------------------------------------------------------------------------
// exceptionThrowingExecute
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::Session::EndOfSessionAction
  castor::tape::tapeserver::daemon::LabelSession::exceptionThrowingExecute() { 
  if (!m_labelSessionConfig.useLbp && m_lbp) {
    const std::string message = "Tapeserver configuration does not allow label "
    "a tape with logical block protection.";
    notifyTapeserverOfUserError(message);
    return MARK_DRIVE_AS_UP;
  }
  if (!m_lbp && m_labelSessionConfig.useLbp) {
    std::list<cta::log::Param> params;
    params.push_back(cta::log::Param("uid", m_request.uid));
    params.push_back(cta::log::Param("gid", m_request.gid));
    params.push_back(cta::log::Param("tapeVid", m_request.vid));
    params.push_back(cta::log::Param("tapeDrive", m_request.drive));
    params.push_back(cta::log::Param("logicalLibrary", m_request.logicalLibrary));
    params.push_back(cta::log::Param("force", boolToStr(m_force)));
    params.push_back(cta::log::Param("lbp", boolToStr(m_lbp)));
    m_log(cta::log::WARNING, "Label session configured to use LBP but lbp parameter "
      "is not set", params);
  }
  setProcessCapabilities("cap_sys_rawio+ep");
   
  std::unique_ptr<drive::DriveInterface> drivePtr = createDrive();
  drive::DriveInterface &drive = *drivePtr.get();

  if(m_lbp) {
    // only crc32c lbp mode is supported
    drive.enableCRC32CLogicalBlockProtectionReadWrite();
  } else {
    drive.disableLogicalBlockProtection();
  }

  // The label to be written without encryption
  m_encryptionControl.disable(drive);

  mountTape();
  waitUntilTapeLoaded(drive, 60); // 60 = 60 seconds
  
  if(drive.isWriteProtected()) {
    const std::string message = "Cannot label the tape because it is write-protected";
    notifyTapeserverOfUserError(message);
  }
  else {
    rewindDrive(drive);

    // If the user is trying to label a non-empty tape without the force option
    if(!m_force && !drive.isTapeBlank()) {
      const std::string message = "Cannot label a non-empty tape without the"
        " force option";
      notifyTapeserverOfUserError(message);

    // Else the labeling can go ahead
    } else {
        if (m_lbp) {
          writeLabelWithLbpToTape(drive);
        } else {
          writeLabelToTape(drive);
        }
    }
  }
  unloadTape(m_request.vid, drive);
  dismountTape(m_request.vid);
  drive.disableLogicalBlockProtection();

  return MARK_DRIVE_AS_UP;
}

//------------------------------------------------------------------------------
// setProcessCapabilities
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::LabelSession::setProcessCapabilities(
  const std::string &capabilities) {
  std::list<cta::log::Param> params;
  params.push_back(cta::log::Param("uid", m_request.uid));
  params.push_back(cta::log::Param("gid", m_request.gid));
  params.push_back(cta::log::Param("tapeVid", m_request.vid));
  params.push_back(cta::log::Param("tapeDrive", m_request.drive));
  params.push_back(cta::log::Param("logicalLibrary", m_request.logicalLibrary));
  params.push_back(cta::log::Param("force", boolToStr(m_force)));
  params.push_back(cta::log::Param("lbp", boolToStr(m_lbp)));

  m_capUtils.setProcText(capabilities);
  params.push_back(cta::log::Param("capabilities", m_capUtils.getProcText()));
  m_log(cta::log::INFO, "Label session set process capabilities", params);
}

//------------------------------------------------------------------------------
// createDrive
//------------------------------------------------------------------------------
std::unique_ptr<castor::tape::tapeserver::drive::DriveInterface>
  castor::tape::tapeserver::daemon::LabelSession::createDrive() {
  SCSI::DeviceVector dv(m_sysWrapper);    
  SCSI::DeviceInfo driveInfo = dv.findBySymlink(m_driveConfig.devFilename);
  
  // Instantiate the drive object
  std::unique_ptr<drive::DriveInterface>
    drive(drive::createDrive(driveInfo, m_sysWrapper));

  if(NULL == drive.get()) {
    cta::exception::Exception ex;
    ex.getMessage() << "Failed to instantiate drive object";
    throw ex;
  }
  
  return drive;
}

//------------------------------------------------------------------------------
// mountTape
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::LabelSession::mountTape() {
  const cta::mediachanger::LibrarySlot &librarySlot = m_driveConfig.librarySlot();

  std::list<cta::log::Param> params;
  params.push_back(cta::log::Param("uid", m_request.uid));
  params.push_back(cta::log::Param("gid", m_request.gid));
  params.push_back(cta::log::Param("tapeVid", m_request.vid));
  params.push_back(cta::log::Param("tapeDrive", m_request.drive));
  params.push_back(cta::log::Param("logicalLibrary", m_request.logicalLibrary));
  params.push_back(cta::log::Param("force", boolToStr(m_force)));
  params.push_back(cta::log::Param("lbp", boolToStr(m_lbp)));
  params.push_back(cta::log::Param("librarySlot", librarySlot.str()));

  m_log(cta::log::INFO, "Label session mounting tape", params);
  m_mc.mountTapeReadWrite(m_request.vid, librarySlot);
  m_log(cta::log::INFO, "Label session mounted tape", params);
}

//------------------------------------------------------------------------------
// waitUntilTapeLoaded
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::LabelSession::waitUntilTapeLoaded(
  drive::DriveInterface &drive, const int timeoutSecond) { 
  std::list<cta::log::Param> params;
  params.push_back(cta::log::Param("uid", m_request.uid));
  params.push_back(cta::log::Param("gid", m_request.gid));
  params.push_back(cta::log::Param("tapeVid", m_request.vid));
  params.push_back(cta::log::Param("tapeDrive", m_request.drive));
  params.push_back(cta::log::Param("logicalLibrary", m_request.logicalLibrary));
  params.push_back(cta::log::Param("force", boolToStr(m_force)));
  params.push_back(cta::log::Param("lbp", boolToStr(m_lbp)));

  try {
    drive.waitUntilReady(timeoutSecond);
    m_log(cta::log::INFO, "Label session loaded tape", params);
  } catch(cta::exception::Exception &ne) {
    cta::exception::Exception ex;
    ex.getMessage() << "Failed to wait for tape to be loaded: " <<
      ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// rewindDrive
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::LabelSession::rewindDrive(
  drive::DriveInterface &drive) {
  std::list<cta::log::Param> params;
  params.push_back(cta::log::Param("uid", m_request.uid));
  params.push_back(cta::log::Param("gid", m_request.gid));
  params.push_back(cta::log::Param("tapeVid", m_request.vid));
  params.push_back(cta::log::Param("tapeDrive", m_request.drive));
  params.push_back(cta::log::Param("logicalLibrary", m_request.logicalLibrary));
  params.push_back(cta::log::Param("force", boolToStr(m_force)));
  params.push_back(cta::log::Param("lbp", boolToStr(m_lbp)));

  m_log(cta::log::INFO, "Label session rewinding tape", params);
  drive.rewind();
  m_log(cta::log::INFO, "Label session successfully rewound tape", params);
}

//------------------------------------------------------------------------------
// notifyTapeserverOfUserError
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::LabelSession::
  notifyTapeserverOfUserError(const std::string message) {
  std::list<cta::log::Param> params;
  params.push_back(cta::log::Param("uid", m_request.uid));
  params.push_back(cta::log::Param("gid", m_request.gid));
  params.push_back(cta::log::Param("tapeVid", m_request.vid));
  params.push_back(cta::log::Param("tapeDrive", m_request.drive));
  params.push_back(cta::log::Param("logicalLibrary", m_request.logicalLibrary));
  params.push_back(cta::log::Param("force", boolToStr(m_force)));
  params.push_back(cta::log::Param("lbp", boolToStr(m_lbp)));
  params.push_back(cta::log::Param("message", message));

  m_log(cta::log::ERR, "Label session encountered user error", params);
  m_tapeserver.labelError(m_request.drive, message);
}

//------------------------------------------------------------------------------
// writeLabelToTape
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::LabelSession::writeLabelToTape(
  drive::DriveInterface &drive) {
  std::list<cta::log::Param> params;
  params.push_back(cta::log::Param("uid", m_request.uid));
  params.push_back(cta::log::Param("gid", m_request.gid));
  params.push_back(cta::log::Param("tapeVid", m_request.vid));
  params.push_back(cta::log::Param("tapeDrive", m_request.drive));
  params.push_back(cta::log::Param("logicalLibrary", m_request.logicalLibrary));
  params.push_back(cta::log::Param("force", boolToStr(m_force)));
  params.push_back(cta::log::Param("lbp", boolToStr(m_lbp)));

  if(m_lbp) {
    m_log(cta::log::WARNING, "LBP mode mismatch. Force labeling without lbp.", params);
  }
  m_log(cta::log::INFO, "Label session is writing label to tape", params);
  tapeFile::LabelSession ls(drive, m_request.vid, false);
  m_log(cta::log::INFO, "Label session has written label to tape", params);
}

//------------------------------------------------------------------------------
// writeLabelWithLbpToTape
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::LabelSession::writeLabelWithLbpToTape(
  drive::DriveInterface &drive) {
  std::list<cta::log::Param> params;
  params.push_back(cta::log::Param("uid", m_request.uid));
  params.push_back(cta::log::Param("gid", m_request.gid));
  params.push_back(cta::log::Param("tapeVid", m_request.vid));
  params.push_back(cta::log::Param("tapeDrive", m_request.drive));
  params.push_back(cta::log::Param("logicalLibrary", m_request.logicalLibrary));
  params.push_back(cta::log::Param("force", boolToStr(m_force)));
  params.push_back(cta::log::Param("lbp", boolToStr(m_lbp)));

  if(!m_lbp) {
    m_log(cta::log::WARNING, "LBP mode mismatch. Force labeling with lbp.", params);
  }
  m_log(cta::log::INFO, "Label session is writing label with LBP to tape", params);
  tapeFile::LabelSession ls(drive, m_request.vid, true);
  m_log(cta::log::INFO, "Label session has written label with LBP to tape", params);
}

//------------------------------------------------------------------------------
// unloadTape
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::LabelSession::unloadTape(
  const std::string &vid, drive::DriveInterface &drive) {
  std::list<cta::log::Param> params;
  params.push_back(cta::log::Param("uid", m_request.uid));
  params.push_back(cta::log::Param("gid", m_request.gid));
  params.push_back(cta::log::Param("tapeVid", m_request.vid));
  params.push_back(cta::log::Param("tapeDrive", m_request.drive));
  params.push_back(cta::log::Param("logicalLibrary", m_request.logicalLibrary));
  params.push_back(cta::log::Param("force", boolToStr(m_force)));
  params.push_back(cta::log::Param("lbp", boolToStr(m_lbp)));

  try {
    m_log(cta::log::INFO, "Label session unloading tape", params);
    drive.unloadTape();
    m_log(cta::log::INFO, "Label session unloaded tape", params);
  } catch (cta::exception::Exception &ne) {
    cta::exception::Exception ex;
    ex.getMessage() << "Label session failed to unload tape: " <<
      ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// dismountTape
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::LabelSession::dismountTape(
  const std::string &vid) {
  const cta::mediachanger::LibrarySlot &librarySlot = m_driveConfig.librarySlot();
  std::list<cta::log::Param> params;
  params.push_back(cta::log::Param("uid", m_request.uid));
  params.push_back(cta::log::Param("gid", m_request.gid));
  params.push_back(cta::log::Param("tapeVid", m_request.vid));
  params.push_back(cta::log::Param("tapeDrive", m_request.drive));
  params.push_back(cta::log::Param("logicalLibrary", m_request.logicalLibrary));
  params.push_back(cta::log::Param("force", boolToStr(m_force)));
  params.push_back(cta::log::Param("lbp", boolToStr(m_lbp)));
  params.push_back(cta::log::Param("librarySlot", librarySlot.str()));

  try {
    m_log(cta::log::INFO, "Label session dismounting tape", params);
    m_mc.dismountTape(vid, librarySlot);
    m_log(cta::log::INFO, "Label session dismounted tape", params);
  } catch(cta::exception::Exception &ne) {
    cta::exception::Exception ex;
    ex.getMessage() << "Label session failed to dismount tape: " <<
      ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// boolToStr
//------------------------------------------------------------------------------
const char *castor::tape::tapeserver::daemon::LabelSession::boolToStr(
  const bool value) {
  return value ? "true" : "false";
}
