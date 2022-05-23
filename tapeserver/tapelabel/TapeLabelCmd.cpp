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

#include "common/Constants.hpp"
#include "tapeserver/castor/tape/Constants.hpp"
#include "tapeserver/castor/tape/tapeserver/file/File.hpp"
#include "tapeserver/castor/tape/tapeserver/file/Structures.hpp"
#include "tapeserver/tapelabel/TapeLabelCmd.hpp"
#include "tapeserver/tapelabel/TapeLabelCmdLineArgs.hpp"
#include "mediachanger/LibrarySlotParser.hpp"

namespace cta {
namespace tapeserver {
namespace tapelabel {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
TapeLabelCmd::TapeLabelCmd(std::istream &inStream, std::ostream &outStream,
  std::ostream &errStream, cta::log::StdoutLogger &log,
  cta::mediachanger::MediaChangerFacade &mc):
  CmdLineTool(inStream, outStream, errStream),
  m_log(log),
  m_encryptionControl(false, ""),
  m_mc(mc),
  m_useLbp(true),
  m_driveSupportLbp(true),
  m_force(false){
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
TapeLabelCmd::~TapeLabelCmd() noexcept {
}

//------------------------------------------------------------------------------
// exceptionThrowingMain
//------------------------------------------------------------------------------
int TapeLabelCmd::exceptionThrowingMain(const int argc, char *const *const argv) {
  const TapeLabelCmdLineArgs cmdLineArgs(argc, argv);

  if(cmdLineArgs.help) {
    printUsage(m_out);
    return 0;
  }

  if (!cmdLineArgs.m_debug) {
    m_log.setLogMask("WARNING");
  }
  
  if (cmdLineArgs.m_force) {
    m_force = true;
  } else {
    m_force = false;
  }
  std::list<cta::log::Param> params;
  params.push_back(cta::log::Param("userName", getUsername()));
  params.push_back(cta::log::Param("tapeVid", cmdLineArgs.m_vid));
  params.push_back(cta::log::Param("tapeOldLabel",cmdLineArgs.m_oldLabel));
  params.push_back(cta::log::Param("force", boolToStr(m_force)));
  params.push_back(cta::log::Param("tapeLoadTimeout", cmdLineArgs.m_tapeLoadTimeout));
  m_log(cta::log::INFO, "Label session started", params);
  
  readAndSetConfiguration(getUsername(), cmdLineArgs.m_vid, cmdLineArgs.m_oldLabel);
   
  const std::string capabilities("cap_sys_rawio+ep");
  setProcessCapabilities(capabilities);
  
  m_catalogue->checkTapeForLabel(m_vid);
  
  std::unique_ptr<castor::tape::tapeserver::drive::DriveInterface> drivePtr = createDrive();
  castor::tape::tapeserver::drive::DriveInterface &drive = *drivePtr.get();
  
  // The label to be written without encryption
  m_encryptionControl.disable(drive);
  
  if (!isDriveSupportLbp(drive)) {
    m_log(cta::log::WARNING, "Drive does not support LBP", params);
    m_driveSupportLbp = false;
  } else {
    m_driveSupportLbp = true;
  };
  
  mountTape(m_vid);
  waitUntilTapeLoaded(drive, cmdLineArgs.m_tapeLoadTimeout);
  
  int returnCode = 0;
  if(drive.isWriteProtected()) {
    m_log(cta::log::ERR, "Cannot label the tape because it is write-protected", params);
    returnCode = 1;
  } else {
    try {
      rewindDrive(drive);
      // If the user is trying to label a non-empty tape
      if(!drive.isTapeBlank()) {
        if (m_force) {
          m_log(cta::log::WARNING, "Label a non-empty tape with force option", params);
          setLbpMode(drive, m_useLbp, m_driveSupportLbp);
          writeTapeLabel(drive, m_useLbp, m_driveSupportLbp);
        } else {     
          if (m_oldLabel.empty()) {
            m_log(cta::log::WARNING, "Label a non-empty tape without the oldLabel option", params);
            checkTapeLabel(drive, m_vid); // oldLabel is not set assume it is the same as VID
            setLbpMode(drive, m_useLbp, m_driveSupportLbp);
            writeTapeLabel(drive, m_useLbp, m_driveSupportLbp);
          } else {
            checkTapeLabel(drive, m_oldLabel);
            setLbpMode(drive, m_useLbp, m_driveSupportLbp);
            writeTapeLabel(drive, m_useLbp, m_driveSupportLbp);
          }
        }
      // Else the labeling can go ahead
      } else {
        setLbpMode(drive, m_useLbp, m_driveSupportLbp);
        writeTapeLabel(drive, m_useLbp, m_driveSupportLbp);
      }
    } catch(cta::exception::Exception &ne) {
      params.push_back(cta::log::Param("tapeLabelError", ne.getMessage().str()));
      m_log(cta::log::ERR, "Label session failed to label the tape", params);
      returnCode = 1; 
    }
  }
  unloadTape(m_vid, drive);
  dismountTape(m_vid);
  drive.disableLogicalBlockProtection();
  if(!returnCode) {
    m_catalogue->tapeLabelled(m_vid, m_unitName);
  }
  return returnCode;
}


//------------------------------------------------------------------------------
// isDriveSupportLbp
//------------------------------------------------------------------------------
bool TapeLabelCmd::isDriveSupportLbp(
  castor::tape::tapeserver::drive::DriveInterface &drive) const {
  castor::tape::tapeserver::drive::deviceInfo devInfo = drive.getDeviceInfo();
  if (devInfo.isPIsupported) { //drive supports LBP
    return true;
  } else {
    return false;
  }
}

//------------------------------------------------------------------------------
// setLbpMode
//------------------------------------------------------------------------------
void TapeLabelCmd::setLbpMode(
  castor::tape::tapeserver::drive::DriveInterface &drive, const bool useLbp,
  const bool driveSupportLbp) {
  std::list<cta::log::Param> params;
  params.push_back(cta::log::Param("userName", m_userName));
  params.push_back(cta::log::Param("tapeVid", m_vid));
  params.push_back(cta::log::Param("tapeOldLabel", m_oldLabel));
  params.push_back(cta::log::Param("tapeDrive", m_unitName));
  params.push_back(cta::log::Param("logicalLibrary", m_logicalLibrary));
  params.push_back(cta::log::Param("useLbp",boolToStr(m_useLbp)));
  params.push_back(cta::log::Param("driveSupportLbp",boolToStr(m_driveSupportLbp)));
  params.push_back(cta::log::Param("force", boolToStr(m_force)));

  if(useLbp) {
    if (driveSupportLbp) {
      // only crc32c lbp mode is supported
      drive.enableCRC32CLogicalBlockProtectionReadWrite();
      m_log(cta::log::INFO, "Label session enabling LBP on drive", params);
    } else {
      drive.disableLogicalBlockProtection();
      m_log(cta::log::WARNING, "Label session disabling LBP on not supported drive", params);
    }
  } else {
    drive.disableLogicalBlockProtection();
    m_log(cta::log::INFO, "Label session disabling LBP on drive", params);
  }
}

//------------------------------------------------------------------------------
// writeTapeLabel
//------------------------------------------------------------------------------
void TapeLabelCmd::writeTapeLabel(
  castor::tape::tapeserver::drive::DriveInterface &drive, const bool useLbp,
  const bool driveSupportLbp) {
  if (useLbp && driveSupportLbp) {
    writeLabelWithLbpToTape(drive);
  } else {
    writeLabelToTape(drive);
  }
}

//------------------------------------------------------------------------------
// checkTapeLabel
//------------------------------------------------------------------------------
void TapeLabelCmd::checkTapeLabel(
  castor::tape::tapeserver::drive::DriveInterface &drive, const std::string &labelToCheck) {
  std::list<cta::log::Param> params;
  params.push_back(cta::log::Param("userName", m_userName));
  params.push_back(cta::log::Param("tapeVid", m_vid));
  params.push_back(cta::log::Param("tapeOldLabel", m_oldLabel));
  params.push_back(cta::log::Param("tapeDrive", m_unitName));
  params.push_back(cta::log::Param("logicalLibrary", m_logicalLibrary));
  params.push_back(cta::log::Param("useLbp",boolToStr(m_useLbp)));
  params.push_back(cta::log::Param("driveSupportLbp",boolToStr(m_driveSupportLbp)));
  params.push_back(cta::log::Param("force", boolToStr(m_force)));
  m_log(cta::log::INFO, "Label session checking non empty tape", params);
  
  if(drive.isTapeBlank()) {
    cta::exception::Exception ex;
    ex.getMessage() << "[TapeLabelCmd::checkTapeLabel()] - Tape is blank, "
                       "cannot proceed with checking the tape";
    throw ex;
  }
  
  drive.disableLogicalBlockProtection();
  {
    castor::tape::tapeFile::VOL1 vol1;
    drive.readExactBlock((void * )&vol1, sizeof(vol1), "[TapeLabelCmd::checkTapeLabel] - Reading VOL1");
    switch(vol1.getLBPMethod()) {
      case castor::tape::SCSI::logicBlockProtectionMethod::CRC32C:
        if (m_useLbp) {
          setLbpMode(drive, m_useLbp, m_driveSupportLbp);
        } else {
          cta::exception::Exception ex;
          ex.getMessage() << "[TapeLabelCmd::checkTapeLabel()] - Tape "
            "labeled with crc32c logical block protection but cta-tape-label "
            "started without LBP support";
          throw ex;
        }
        break;
      case castor::tape::SCSI::logicBlockProtectionMethod::ReedSolomon:
        throw cta::exception::Exception("In TapeLabelCmd::checkTapeLabel(): "
            "ReedSolomon LBP method not supported");
      case castor::tape::SCSI::logicBlockProtectionMethod::DoNotUse:
        drive.disableLogicalBlockProtection();
        break;
      default:
        throw cta::exception::Exception("In TapeLabelCmd::checkTapeLabel(): unknown LBP method");
    }
  }
  // from this point the right LBP mode should be set or not set
  drive.rewind();
  {
    castor::tape::tapeFile::VOL1 vol1;
    drive.readExactBlock((void *) &vol1, sizeof(vol1), "[TapeLabelCmd::checkTapeLabel()] - Reading VOL1");
    try {
      vol1.verify();
    } catch (std::exception &e) {
      throw castor::tape::tapeFile::TapeFormatError(e.what());
    }
    castor::tape::tapeFile::HeaderChecker::checkVOL1(vol1, labelToCheck); // now we know that we are going to check the correct tape
  }
  drive.rewind();
  params.push_back(cta::log::Param("tapeLabel", labelToCheck));
  m_log(cta::log::INFO, "Label session successfully checked non empty tape", params);
}

//------------------------------------------------------------------------------
// dismountTape
//------------------------------------------------------------------------------
void TapeLabelCmd::dismountTape(
  const std::string &vid) {
  std::unique_ptr<cta::mediachanger::LibrarySlot> librarySlotPtr;
  librarySlotPtr.reset(
    cta::mediachanger::LibrarySlotParser::parse(m_rawLibrarySlot));
  const cta::mediachanger::LibrarySlot &librarySlot = *librarySlotPtr.get();
  
  std::list<cta::log::Param> params;
  params.push_back(cta::log::Param("userName", m_userName));
  params.push_back(cta::log::Param("tapeVid", m_vid));
  params.push_back(cta::log::Param("tapeOldLabel", m_oldLabel));
  params.push_back(cta::log::Param("tapeDrive", m_unitName));
  params.push_back(cta::log::Param("logicalLibrary", m_logicalLibrary));
  params.push_back(cta::log::Param("useLbp",boolToStr(m_useLbp)));
  params.push_back(cta::log::Param("driveSupportLbp",boolToStr(m_driveSupportLbp)));
  params.push_back(cta::log::Param("librarySlot", librarySlot.str()));
  params.push_back(cta::log::Param("force", boolToStr(m_force)));

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
// writeLabelWithLbpToTape
//------------------------------------------------------------------------------
void TapeLabelCmd::writeLabelWithLbpToTape(
  castor::tape::tapeserver::drive::DriveInterface &drive) {
  std::list<cta::log::Param> params;
  params.push_back(cta::log::Param("userName", m_userName));
  params.push_back(cta::log::Param("tapeVid", m_vid));
  params.push_back(cta::log::Param("tapeOldLabel", m_oldLabel));
  params.push_back(cta::log::Param("tapeDrive", m_unitName));
  params.push_back(cta::log::Param("logicalLibrary", m_logicalLibrary));
  params.push_back(cta::log::Param("useLbp",boolToStr(m_useLbp)));
  params.push_back(cta::log::Param("driveSupportLbp",boolToStr(m_driveSupportLbp)));
  params.push_back(cta::log::Param("force", boolToStr(m_force)));

  if(!m_useLbp) {
    m_log(cta::log::WARNING, "LBP mode mismatch. Force labeling with LBP.", params);
  }
  m_log(cta::log::INFO, "Label session is writing label with LBP to tape", params);
  castor::tape::tapeFile::LabelSession ls(drive, m_vid, true);
  m_log(cta::log::INFO, "Label session has written label with LBP to tape", params);
}

//------------------------------------------------------------------------------
// writeLabelToTape
//------------------------------------------------------------------------------
void TapeLabelCmd::writeLabelToTape(
  castor::tape::tapeserver::drive::DriveInterface &drive) {
  std::list<cta::log::Param> params;
  params.push_back(cta::log::Param("userName", m_userName));
  params.push_back(cta::log::Param("tapeVid", m_vid));
  params.push_back(cta::log::Param("tapeOldLabel", m_oldLabel));
  params.push_back(cta::log::Param("tapeDrive", m_unitName));
  params.push_back(cta::log::Param("logicalLibrary", m_logicalLibrary));
  params.push_back(cta::log::Param("useLbp",boolToStr(m_useLbp)));
  params.push_back(cta::log::Param("driveSupportLbp",boolToStr(m_driveSupportLbp)));
  params.push_back(cta::log::Param("force", boolToStr(m_force)));

  if(m_useLbp) {
    m_log(cta::log::WARNING, "LBP mode mismatch. Force labeling without LBP.", params);
  }
  m_log(cta::log::INFO, "Label session is writing label to tape", params);
  castor::tape::tapeFile::LabelSession ls(drive, m_vid, false);
  m_log(cta::log::INFO, "Label session has written label to tape", params);
}

//------------------------------------------------------------------------------
// unloadTape
//------------------------------------------------------------------------------
void TapeLabelCmd::unloadTape(const std::string &vid, castor::tape::tapeserver::drive::DriveInterface &drive) {
  std::unique_ptr<cta::mediachanger::LibrarySlot> librarySlotPtr;
  librarySlotPtr.reset(
    cta::mediachanger::LibrarySlotParser::parse(m_rawLibrarySlot));
  const cta::mediachanger::LibrarySlot &librarySlot = *librarySlotPtr;

  std::list<cta::log::Param> params;
  params.push_back(cta::log::Param("userName", m_userName));
  params.push_back(cta::log::Param("tapeVid", m_vid));
  params.push_back(cta::log::Param("tapeOldLabel", m_oldLabel));
  params.push_back(cta::log::Param("tapeDrive", m_unitName));
  params.push_back(cta::log::Param("logicalLibrary", m_logicalLibrary));
  params.push_back(cta::log::Param("useLbp",boolToStr(m_useLbp)));
  params.push_back(cta::log::Param("driveSupportLbp",boolToStr(m_driveSupportLbp)));
  params.push_back(cta::log::Param("librarySlot", librarySlot.str()));
  params.push_back(cta::log::Param("force", boolToStr(m_force)));

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
// rewindDrive
//------------------------------------------------------------------------------
void TapeLabelCmd::rewindDrive(castor::tape::tapeserver::drive::DriveInterface &drive) {
  std::list<cta::log::Param> params;
  params.push_back(cta::log::Param("userName", m_userName));
  params.push_back(cta::log::Param("tapeVid", m_vid));
  params.push_back(cta::log::Param("tapeOldLabel", m_oldLabel));
  params.push_back(cta::log::Param("tapeDrive", m_unitName));
  params.push_back(cta::log::Param("logicalLibrary", m_logicalLibrary));
  params.push_back(cta::log::Param("useLbp",boolToStr(m_useLbp)));
  params.push_back(cta::log::Param("driveSupportLbp",boolToStr(m_driveSupportLbp)));
  params.push_back(cta::log::Param("force", boolToStr(m_force)));
  
  m_log(cta::log::INFO, "Label session rewinding tape", params);
  drive.rewind();
  m_log(cta::log::INFO, "Label session successfully rewound tape", params);
}

//------------------------------------------------------------------------------
// setProcessCapabilities
//------------------------------------------------------------------------------
void TapeLabelCmd::setProcessCapabilities(const std::string &capabilities) {
  m_capUtils.setProcText(capabilities);
  std::list<cta::log::Param> params;
  params.push_back(cta::log::Param("capabilities", capabilities));
  m_log(cta::log::INFO, "Label session set process capabilities", params);
}

//------------------------------------------------------------------------------
// readConfiguration
//------------------------------------------------------------------------------
void TapeLabelCmd::readAndSetConfiguration(const std::string &userName,
  const std::string &vid, const std::string &oldLabel) {
  m_vid = vid;
  m_oldLabel = oldLabel;
  m_userName = userName;
  cta::tape::daemon::Tpconfig tpConfig;
  tpConfig  = cta::tape::daemon::Tpconfig::parseFile(castor::tape::TPCONFIGPATH);
  const int configuredDrives =  tpConfig.size();
    if( 1 == configuredDrives)
     for(auto & driveConfig: tpConfig) {
      m_devFilename = driveConfig.second.value().devFilename;
      m_rawLibrarySlot = driveConfig.second.value().rawLibrarySlot;
      m_logicalLibrary = driveConfig.second.value().logicalLibrary;
      m_unitName = driveConfig.second.value().unitName;
    } else {
      cta::exception::Exception ex;
      ex.getMessage() << "Failed to read configuration: " 
                      << configuredDrives << " drives configured";
      throw ex;
    }

  const cta::rdbms::Login catalogueLogin = cta::rdbms::Login::parseFile(CATALOGUE_CONFIG_PATH);
  const uint64_t nbConns = 1;
  const uint64_t nbArchiveFileListingConns = 0;
  auto catalogueFactory = cta::catalogue::CatalogueFactoryFactory::create(m_log,
    catalogueLogin, nbConns, nbArchiveFileListingConns);
    m_catalogue = catalogueFactory->create();
    
  std::list<cta::log::Param> params;
  params.push_back(cta::log::Param("catalogueDbType", catalogueLogin.dbTypeToString(catalogueLogin.dbType)));
  params.push_back(cta::log::Param("catalogueDatabase", catalogueLogin.database));
  params.push_back(cta::log::Param("catalogueUsername", catalogueLogin.username));
  params.push_back(cta::log::Param("devFilename", m_devFilename));
  params.push_back(cta::log::Param("rawLibrarySlot", m_rawLibrarySlot));
  params.push_back(cta::log::Param("logicalLibrary", m_logicalLibrary));
  params.push_back(cta::log::Param("unitName", m_unitName));
  m_log(cta::log::INFO, "Label session read configuration", params);
}

//------------------------------------------------------------------------------
// mountTape
//------------------------------------------------------------------------------
void TapeLabelCmd::mountTape(const std::string &vid) {
  std::unique_ptr<cta::mediachanger::LibrarySlot> librarySlotPtr;
  librarySlotPtr.reset(
    cta::mediachanger::LibrarySlotParser::parse(m_rawLibrarySlot));
  const cta::mediachanger::LibrarySlot &librarySlot = *librarySlotPtr.get();
    
  std::list<cta::log::Param> params;
  params.push_back(cta::log::Param("userName", m_userName));
  params.push_back(cta::log::Param("tapeVid", vid));
  params.push_back(cta::log::Param("tapeOldLabel", m_oldLabel));
  params.push_back(cta::log::Param("tapeDrive", m_unitName));
  params.push_back(cta::log::Param("logicalLibrary", m_logicalLibrary));
  params.push_back(cta::log::Param("useLbp", boolToStr(m_useLbp)));
  params.push_back(cta::log::Param("driveSupportLbp",boolToStr(m_driveSupportLbp)));
  params.push_back(cta::log::Param("librarySlot", librarySlot.str()));
  params.push_back(cta::log::Param("force", boolToStr(m_force)));

  m_log(cta::log::INFO, "Label session mounting tape", params);
  m_mc.mountTapeReadWrite(vid, librarySlot);
  m_log(cta::log::INFO, "Label session mounted tape", params);
}

//------------------------------------------------------------------------------
// createDrive
//------------------------------------------------------------------------------
std::unique_ptr<castor::tape::tapeserver::drive::DriveInterface>
  TapeLabelCmd::createDrive() {
  castor::tape::SCSI::DeviceVector dv(m_sysWrapper);    
  castor::tape::SCSI::DeviceInfo driveInfo = dv.findBySymlink(m_devFilename);
  
  // Instantiate the drive object
  std::unique_ptr<castor::tape::tapeserver::drive::DriveInterface>
    drive(castor::tape::tapeserver::drive::createDrive(driveInfo, m_sysWrapper));

  if(nullptr == drive.get()) {
    cta::exception::Exception ex;
    ex.getMessage() << "Failed to instantiate drive object";
    throw ex;
  }
  
  return drive;
}


//------------------------------------------------------------------------------
// waitUntilTapeLoaded
//------------------------------------------------------------------------------
void TapeLabelCmd::waitUntilTapeLoaded(
  castor::tape::tapeserver::drive::DriveInterface &drive, const int timeoutSecond) { 
  std::list<cta::log::Param> params;
  params.push_back(cta::log::Param("userName", m_userName));
  params.push_back(cta::log::Param("tapeVid", m_vid));
  params.push_back(cta::log::Param("tapeOldLabel", m_oldLabel));
  params.push_back(cta::log::Param("tapeDrive", m_unitName));
  params.push_back(cta::log::Param("logicalLibrary", m_logicalLibrary));
  params.push_back(cta::log::Param("useLbp",boolToStr(m_useLbp)));
  params.push_back(cta::log::Param("driveSupportLbp",boolToStr(m_driveSupportLbp)));
  params.push_back(cta::log::Param("force", boolToStr(m_force)));

  try {
    m_log(cta::log::INFO, "Label session loading tape", params);
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
// boolToStr
//------------------------------------------------------------------------------
const char *TapeLabelCmd::boolToStr(
  const bool value) {
  return value ? "true" : "false";
}

//------------------------------------------------------------------------------
// printUsage
//------------------------------------------------------------------------------
void TapeLabelCmd::printUsage(std::ostream &os) {
  TapeLabelCmdLineArgs::printUsage(os);
}

} // namespace tapelabel
} // namespace tapeserver
} // namespace cta
