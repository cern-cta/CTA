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

#include "castor/tape/tapeserver/file/FileReaderFactory.hpp"
#include "catalogue/Catalogue.hpp"
#include "catalogue/CatalogueFactory.hpp"
#include "catalogue/CatalogueFactoryFactory.hpp"
#include "catalogue/CatalogueItor.hpp"
#include "catalogue/TapeSearchCriteria.hpp"
#include "common/Constants.hpp"
#include "common/exception/EncryptionException.hpp"
#include "common/log/DummyLogger.hpp"
#include "disk/DiskFile.hpp"
#include "disk/RadosStriperPool.hpp"
#include "mediachanger/LibrarySlotParser.hpp"
#include "rdbms/Login.hpp"
#include "scheduler/RetrieveJob.hpp"
#include "tapeserver/castor/tape/Constants.hpp"
#include "tapeserver/castor/tape/tapeserver/daemon/EncryptionControl.hpp"
#include "tapeserver/castor/tape/tapeserver/daemon/Payload.hpp"
#include "tapeserver/castor/tape/tapeserver/file/ReadSession.hpp"
#include "tapeserver/castor/tape/tapeserver/file/ReadSessionFactory.hpp"
#include "tapeserver/castor/tape/tapeserver/file/Structures.hpp"
#include "tapeserver/daemon/Tpconfig.hpp"
#include "tapeserver/readtp/ReadtpCmd.hpp"
#include "tapeserver/readtp/ReadtpCmdLineArgs.hpp"
#include "tapeserver/readtp/TapeFseqRange.hpp"
#include "tapeserver/readtp/TapeFseqRangeListSequence.hpp"

namespace cta {
namespace tapeserver {
namespace readtp {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
ReadtpCmd::ReadtpCmd(std::istream &inStream, std::ostream &outStream,
  std::ostream &errStream, cta::log::StdoutLogger &log, cta::log::DummyLogger &dummyLog,
  cta::mediachanger::MediaChangerFacade &mc, const bool useEncryption, 
  const std::string& externalEncryptionKeyScript):
  CmdLineTool(inStream, outStream, errStream),
  m_log(log),
  m_dummyLog(dummyLog),
  m_mc(mc),
  m_useLbp(true),
  m_nbSuccessReads(0),
  m_nbFailedReads(0) {
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
ReadtpCmd::~ReadtpCmd() noexcept {
}

//------------------------------------------------------------------------------
// exceptionThrowingMain
//------------------------------------------------------------------------------
int ReadtpCmd::exceptionThrowingMain(const int argc, char *const *const argv) {
  const ReadtpCmdLineArgs cmdLineArgs(argc, argv);
  if (cmdLineArgs.help) {
    printUsage(m_out);
    return 0;
  }

  std::list<cta::log::Param> params;
  params.push_back(cta::log::Param("userName", getUsername()));
  params.push_back(cta::log::Param("tapeVid", cmdLineArgs.m_vid));
  m_log(cta::log::INFO, "Started", params);

  readAndSetConfiguration(getUsername(), cmdLineArgs);

  setProcessCapabilities("cap_sys_rawio+ep");

  std::unique_ptr<castor::tape::tapeserver::drive::DriveInterface> drivePtr = createDrive();
  castor::tape::tapeserver::drive::DriveInterface &drive = *drivePtr.get();

  if (!isDriveSupportLbp(drive)) {
    m_log(cta::log::WARNING, "Drive does not support LBP", params);
    m_driveSupportLbp = false;
  } else {
    m_driveSupportLbp = true;
  };

  mountTape(m_vid);
  waitUntilTapeLoaded(drive, TAPE_LABEL_UNITREADY_TIMEOUT);

  int returnCode = 0;
  try {
    readTapeFiles(drive);
  } catch(cta::exception::EncryptionException &ex) {
    params.push_back(cta::log::Param("tapeReadError", ex.getMessage().str()));
    m_log(cta::log::ERR, "Failed to enable encrypyion for read of drive", params);
    throw cta::exception::EncryptionException(ex.what(), false);
  } catch(cta::exception::Exception &ne) {
    params.push_back(cta::log::Param("tapeReadError", ne.getMessage().str()));
    m_log(cta::log::ERR, "Failed to read the tape", params);
    returnCode = 1;
  }
  unloadTape(m_vid, drive);

  // Disable encryption (or at least try)
  if(m_isTapeEncrypted) {
    disableEncryption(drive);
  }

  dismountTape(m_vid);

  return returnCode;
}

//------------------------------------------------------------------------------
// readAndSetConfiguration
//------------------------------------------------------------------------------
void ReadtpCmd::readAndSetConfiguration(const std::string &userName, const ReadtpCmdLineArgs &cmdLineArgs) {
  m_vid = cmdLineArgs.m_vid;
  m_fSeqRangeList = cmdLineArgs.m_fSeqRangeList;
  m_xrootPrivateKeyPath = cmdLineArgs.m_xrootPrivateKeyPath;
  m_userName = userName;
  m_destinationFiles = readListFromFile(cmdLineArgs.m_destinationFileListURL);
  cta::tape::daemon::Tpconfig tpConfig;
  tpConfig  = cta::tape::daemon::Tpconfig::parseFile(castor::tape::TPCONFIGPATH);

  if (tpConfig.empty()) {
    cta::exception::Exception ex;
    ex.getMessage() << "Unable to obtain drive info as TPCONFIG is empty";
    throw ex;
  }
  const auto &tpConfigLine = tpConfig.begin()->second.value();
  m_devFilename    = tpConfigLine.devFilename;
  m_rawLibrarySlot = tpConfigLine.rawLibrarySlot;
  m_logicalLibrary = tpConfigLine.logicalLibrary;
  m_unitName       = tpConfigLine.unitName;

  const cta::rdbms::Login catalogueLogin = cta::rdbms::Login::parseFile(CATALOGUE_CONFIG_PATH);
  const uint64_t nbConns = 1;
  const uint64_t nbArchiveFileListingConns = 1;

  auto catalogueFactory = cta::catalogue::CatalogueFactoryFactory::create(m_dummyLog, // to supress catalogue output messages
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
  m_log(cta::log::INFO, "Read configuration", params);
}

//------------------------------------------------------------------------------
// isEncrypted
//------------------------------------------------------------------------------
bool ReadtpCmd::isEncrypted(const cta::common::dataStructures::Tape &tape) const {
  if(tape.encryptionKeyName) {
    return true;
  }
  return false;
}

//------------------------------------------------------------------------------
// readListFromFile
//------------------------------------------------------------------------------
std::list<std::string> ReadtpCmd::readListFromFile(const std::string &filename) const {
  std::ifstream file(filename);
  std::list<std::string> str_list;
  if (file.fail()) {
    throw std::runtime_error("Unable to open file " + filename);
  }

  std::string line;

  while(std::getline(file, line)) {
    // Strip out comments
    auto pos = line.find('#');
    if(pos != std::string::npos) {
      line.resize(pos);
    }

    // Extract the list items
    std::stringstream ss(line);
    while(!ss.eof()) {
      std::string item;
      ss >> item;
      // skip blank lines or lines consisting only of whitespace
      if(item.empty()) continue;

      str_list.push_back(item);
    }
  }
  return str_list;
}


//------------------------------------------------------------------------------
// setProcessCapabilities
//------------------------------------------------------------------------------
void ReadtpCmd::setProcessCapabilities(const std::string &capabilities) {
  m_capUtils.setProcText(capabilities);
  std::list<cta::log::Param> params;
  params.push_back(cta::log::Param("capabilities", capabilities));
  m_log(cta::log::DEBUG, "Set process capabilities", params);
}

//------------------------------------------------------------------------------
// createDrive
//------------------------------------------------------------------------------
std::unique_ptr<castor::tape::tapeserver::drive::DriveInterface>
  ReadtpCmd::createDrive() {
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
// isDriveSupportLbp
//------------------------------------------------------------------------------
bool ReadtpCmd::isDriveSupportLbp(
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
void ReadtpCmd::setLbpMode(
  castor::tape::tapeserver::drive::DriveInterface &drive) {
  std::list<cta::log::Param> params;
  params.push_back(cta::log::Param("userName", m_userName));
  params.push_back(cta::log::Param("tapeVid", m_vid));
  params.push_back(cta::log::Param("tapeDrive", m_unitName));
  params.push_back(cta::log::Param("logicalLibrary", m_logicalLibrary));
  params.push_back(cta::log::Param("useLbp",boolToStr(m_useLbp)));
  params.push_back(cta::log::Param("driveSupportLbp",boolToStr(m_driveSupportLbp)));

  if(m_useLbp) {
    if (m_driveSupportLbp) {
      // only crc32c lbp mode is supported
      drive.enableCRC32CLogicalBlockProtectionReadWrite();
      m_log(cta::log::INFO, "Enabling LBP on drive", params);
    } else {
      m_useLbp = false;
      drive.disableLogicalBlockProtection();
      m_log(cta::log::WARNING, "Disabling LBP on not supported drive", params);
    }
  } else {
    drive.disableLogicalBlockProtection();
    m_log(cta::log::INFO, "Disabling LBP on drive", params);
  }
}

//------------------------------------------------------------------------------
// mountTape
//------------------------------------------------------------------------------
void ReadtpCmd::mountTape(const std::string &vid) {
  std::unique_ptr<cta::mediachanger::LibrarySlot> librarySlotPtr;
  librarySlotPtr.reset(
    cta::mediachanger::LibrarySlotParser::parse(m_rawLibrarySlot));
  const cta::mediachanger::LibrarySlot &librarySlot = *librarySlotPtr.get();

  std::list<cta::log::Param> params;
  params.push_back(cta::log::Param("userName", m_userName));
  params.push_back(cta::log::Param("tapeVid", vid));
  params.push_back(cta::log::Param("tapeDrive", m_unitName));
  params.push_back(cta::log::Param("logicalLibrary", m_logicalLibrary));
  params.push_back(cta::log::Param("librarySlot", librarySlot.str()));
  params.push_back(cta::log::Param("useLbp",boolToStr(m_useLbp)));
  params.push_back(cta::log::Param("driveSupportLbp",boolToStr(m_driveSupportLbp)));

  m_log(cta::log::INFO, "Mounting tape", params);
  m_mc.mountTapeReadOnly(vid, librarySlot);
  m_log(cta::log::INFO, "Mounted tape", params);
}

//------------------------------------------------------------------------------
// waitUntilTapeLoaded
//------------------------------------------------------------------------------
void ReadtpCmd::waitUntilTapeLoaded(
  castor::tape::tapeserver::drive::DriveInterface &drive, const int timeoutSecond) {
  std::list<cta::log::Param> params;
  params.push_back(cta::log::Param("userName", m_userName));
  params.push_back(cta::log::Param("tapeVid", m_vid));
  params.push_back(cta::log::Param("tapeDrive", m_unitName));
  params.push_back(cta::log::Param("logicalLibrary", m_logicalLibrary));
  params.push_back(cta::log::Param("useLbp",boolToStr(m_useLbp)));
  params.push_back(cta::log::Param("driveSupportLbp",boolToStr(m_driveSupportLbp)));

  try {
    m_log(cta::log::INFO, "Loading tape", params);
    drive.waitUntilReady(timeoutSecond);
    m_log(cta::log::INFO, "Loaded tape", params);
  } catch(cta::exception::Exception &ne) {
    cta::exception::Exception ex;
    ex.getMessage() << "Failed to wait for tape to be loaded: " <<
      ne.getMessage().str();
    throw ex;
  }
}

//Basic class representing a read job
class BasicRetrieveJob: public cta::RetrieveJob {
public:
  BasicRetrieveJob() : cta::RetrieveJob(nullptr,
    cta::common::dataStructures::RetrieveRequest(),
    cta::common::dataStructures::ArchiveFile(), 1,
    cta::PositioningMethod::ByFSeq) {}
};


//------------------------------------------------------------------------------
// getNextDestinationUrl
//------------------------------------------------------------------------------
std::string ReadtpCmd::getNextDestinationUrl() {
  if (m_destinationFiles.empty()) {
    return "file:///dev/null";
  } else {
    std::string ret = m_destinationFiles.front();
    m_destinationFiles.pop_front();
    return ret;
  }
}

//------------------------------------------------------------------------------
// readTapeFiles
//------------------------------------------------------------------------------
void ReadtpCmd::readTapeFiles(
  castor::tape::tapeserver::drive::DriveInterface &drive) {
    cta::disk::RadosStriperPool striperPool;
    cta::disk::DiskFileFactory fileFactory(m_xrootPrivateKeyPath, 0, striperPool);

    catalogue::TapeSearchCriteria searchCriteria;
    searchCriteria.vid = m_vid;
    
    auto tapeList = m_catalogue->Tape()->getTapes(searchCriteria);
    if (tapeList.empty()) {
      std::list<cta::log::Param> params;
        params.push_back(cta::log::Param("userName", getUsername()));
        params.push_back(cta::log::Param("tapeVid", m_vid));
        params.push_back(cta::log::Param("tapeDrive", m_unitName));
        params.push_back(cta::log::Param("logicalLibrary", m_logicalLibrary));
        params.push_back(cta::log::Param("useLbp",boolToStr(m_useLbp)));
        params.push_back(cta::log::Param("driveSupportLbp",boolToStr(m_driveSupportLbp)));
        m_log(cta::log::ERR, "Failed to get tape from catalogue", params);
        return;
    }
    auto const& tape = tapeList.front();

    m_isTapeEncrypted = isEncrypted(tape);

    if(m_isTapeEncrypted) {
      try {
        configureEncryption(m_vid, drive);
      } catch(cta::exception::Exception &ex) {
        std::list<cta::log::Param> params;
        params.push_back(cta::log::Param("vid", m_vid));
        m_log(cta::log::ERR, "Configuring encryption failed", params);
        throw cta::exception::EncryptionException(ex.what(), false);
      }
    }

    TapeFseqRangeListSequence fSeqRangeListSequence(&m_fSeqRangeList);
    std::string destinationFile = getNextDestinationUrl();
    uint64_t fSeq;
    while (fSeqRangeListSequence.hasMore()) {
      try {
        fSeq = fSeqRangeListSequence.next();
        if (fSeq > tape.lastFSeq) {
          break; //reached end of tape
        }
        std::unique_ptr<cta::disk::WriteFile> wfptr;
        wfptr.reset(fileFactory.createWriteFile(destinationFile));
        cta::disk::WriteFile &wf = *wfptr.get();
        readTapeFile(drive, fSeq, wf, tape.labelFormat);
        m_nbSuccessReads++; // if readTapeFile returns, file was read successfully
        destinationFile = getNextDestinationUrl();
      } catch (tapeserver::readtp::NoSuchFSeqException&) {
        //Do nothing
      } catch(exception::Exception &ne) {
        std::list<cta::log::Param> params;
        params.push_back(cta::log::Param("userName", getUsername()));
        params.push_back(cta::log::Param("tapeVid", m_vid));
        params.push_back(cta::log::Param("destinationFile", destinationFile));
        params.push_back(cta::log::Param("tapeDrive", m_unitName));
        params.push_back(cta::log::Param("logicalLibrary", m_logicalLibrary));
        params.push_back(cta::log::Param("useLbp",boolToStr(m_useLbp)));
        params.push_back(cta::log::Param("driveSupportLbp",boolToStr(m_driveSupportLbp)));
        params.push_back(cta::log::Param("fSeq", fSeq));
        params.push_back(cta::log::Param("tapeReadError", ne.getMessage().str()));
        m_log(cta::log::ERR, "Failed to read file from tape", params);
        m_nbFailedReads++; 
      }
    }
  std::list<cta::log::Param> params;
  params.push_back(cta::log::Param("userName", getUsername()));
  params.push_back(cta::log::Param("tapeVid", m_vid));
  params.push_back(cta::log::Param("tapeDrive", m_unitName));
  params.push_back(cta::log::Param("logicalLibrary", m_logicalLibrary));
  params.push_back(cta::log::Param("useLbp",boolToStr(m_useLbp)));
  params.push_back(cta::log::Param("driveSupportLbp",boolToStr(m_driveSupportLbp)));
  params.push_back(cta::log::Param("nbReads", m_nbSuccessReads + m_nbFailedReads));
  params.push_back(cta::log::Param("nbSuccessfullReads", m_nbSuccessReads));
  params.push_back(cta::log::Param("nbFailedReads", m_nbFailedReads));

  m_log(cta::log::INFO, "Finished reading tape", params);

}

//------------------------------------------------------------------------------
// readTapeFile
//------------------------------------------------------------------------------
void ReadtpCmd::readTapeFile(
  castor::tape::tapeserver::drive::DriveInterface &drive, const uint64_t &fSeq, cta::disk::WriteFile &wf,
  const cta::common::dataStructures::Label::Format &labelFormat) {
  std::list<cta::log::Param> params;
  params.push_back(cta::log::Param("userName", m_userName));
  params.push_back(cta::log::Param("tapeVid", m_vid));
  params.push_back(cta::log::Param("fSeq", fSeq));
  params.push_back(cta::log::Param("tapeDrive", m_unitName));
  params.push_back(cta::log::Param("logicalLibrary", m_logicalLibrary));
  params.push_back(cta::log::Param("useLbp",boolToStr(m_useLbp)));
  params.push_back(cta::log::Param("driveSupportLbp",boolToStr(m_driveSupportLbp)));
  params.push_back(cta::log::Param("destinationURL", wf.URL()));

  castor::tape::tapeserver::daemon::VolumeInfo volInfo;
  volInfo.vid=m_vid;
  volInfo.nbFiles = 0;
  volInfo.mountType = cta::common::dataStructures::MountType::Retrieve;
  volInfo.labelFormat = labelFormat;

  const auto readSession = castor::tape::tapeFile::ReadSessionFactory::create(drive, volInfo, m_useLbp);

  catalogue::TapeFileSearchCriteria searchCriteria;
  searchCriteria.vid = m_vid;
  searchCriteria.fSeq = fSeq;
  auto itor = m_catalogue->ArchiveFile()->getArchiveFilesItor(searchCriteria);

  if (!itor.hasMore()) {
    throw tapeserver::readtp::NoSuchFSeqException();
  }

  m_log(cta::log::INFO, "Reading file from tape", params);

  const auto archiveFile = itor.next();

  BasicRetrieveJob fileToRecall;
  fileToRecall.retrieveRequest.archiveFileID = archiveFile.archiveFileID;
  fileToRecall.selectedCopyNb = 0;
  fileToRecall.archiveFile.tapeFiles.push_back(cta::common::dataStructures::TapeFile());
  fileToRecall.selectedTapeFile().fSeq = fSeq;
  fileToRecall.positioningMethod = cta::PositioningMethod::ByFSeq;

  const auto reader = castor::tape::tapeFile::FileReaderFactory::create(readSession, fileToRecall);
  auto checksum_adler32 = castor::tape::tapeserver::daemon::Payload::zeroAdler32();
  const size_t buffer_size = 1 * 1024 * 1024 * 1024;  // 1Gb
  size_t read_data_size = 0;
  // allocate one gigabyte buffer
  auto payload = std::make_unique<castor::tape::tapeserver::daemon::Payload>(buffer_size);
  try {
    while (1) {
      if (payload->remainingFreeSpace() <= reader->getBlockSize()) {
        // buffer is full, flush to file and update checksum
        read_data_size += payload->size();
        checksum_adler32 = payload->adler32(checksum_adler32);
        payload->write(wf);
        payload->reset();
      }
      payload->append(*reader);
    }
  } catch (cta::exception::EndOfFile&) {
    // File completely read
  }
  read_data_size += payload->size();
  checksum_adler32 = payload->adler32(checksum_adler32);
  payload->write(wf);
  auto cb = cta::checksum::ChecksumBlob(cta::checksum::ChecksumType::ADLER32, checksum_adler32);

  archiveFile.checksumBlob.validate(cb);  //exception thrown if checksums differ

  params.push_back(cta::log::Param("checksumType", "ADLER32"));
  std::stringstream sstream;
  sstream << std::hex << checksum_adler32;
  params.push_back(cta::log::Param("checksumValue", "0x" + sstream.str()));
  params.push_back(cta::log::Param("readFileSize", read_data_size));
  m_log(cta::log::INFO, "Read file from tape successfully", params);
}

//------------------------------------------------------------------------------
// unloadTape
//------------------------------------------------------------------------------
void ReadtpCmd::unloadTape(
  const std::string &vid, castor::tape::tapeserver::drive::DriveInterface &drive) {
  std::unique_ptr<cta::mediachanger::LibrarySlot> librarySlotPtr;
  librarySlotPtr.reset(
    cta::mediachanger::LibrarySlotParser::parse(m_rawLibrarySlot));
  const cta::mediachanger::LibrarySlot &librarySlot = *librarySlotPtr.get();

  std::list<cta::log::Param> params;
  params.push_back(cta::log::Param("userName", m_userName));
  params.push_back(cta::log::Param("tapeVid", m_vid));
  params.push_back(cta::log::Param("tapeDrive", m_unitName));
  params.push_back(cta::log::Param("logicalLibrary", m_logicalLibrary));
  params.push_back(cta::log::Param("useLbp",boolToStr(m_useLbp)));
  params.push_back(cta::log::Param("librarySlot", librarySlot.str()));
  params.push_back(cta::log::Param("driveSupportLbp",boolToStr(m_driveSupportLbp)));

  try {
    m_log(cta::log::INFO, "Unloading tape", params);
    drive.unloadTape();
    m_log(cta::log::INFO, "Unloaded tape", params);
  } catch (cta::exception::Exception &ne) {
    cta::exception::Exception ex;
    ex.getMessage() << "Failed to unload tape: " <<
      ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// dismountTape
//------------------------------------------------------------------------------
void ReadtpCmd::dismountTape(const std::string &vid) {
  std::unique_ptr<cta::mediachanger::LibrarySlot> librarySlotPtr;
  librarySlotPtr.reset(
    cta::mediachanger::LibrarySlotParser::parse(m_rawLibrarySlot));
  const cta::mediachanger::LibrarySlot &librarySlot = *librarySlotPtr.get();

  std::list<cta::log::Param> params;
  params.push_back(cta::log::Param("userName", m_userName));
  params.push_back(cta::log::Param("tapeVid", m_vid));
  params.push_back(cta::log::Param("tapeDrive", m_unitName));
  params.push_back(cta::log::Param("logicalLibrary", m_logicalLibrary));
  params.push_back(cta::log::Param("librarySlot", librarySlot.str()));
  params.push_back(cta::log::Param("useLbp",boolToStr(m_useLbp)));
  params.push_back(cta::log::Param("driveSupportLbp",boolToStr(m_driveSupportLbp)));

  try {
    m_log(cta::log::INFO, "Dismounting tape", params);
    m_mc.dismountTape(vid, librarySlot);
    m_log(cta::log::INFO, "Dismounted tape", params);
  } catch(cta::exception::Exception &ne) {
    cta::exception::Exception ex;
    ex.getMessage() << "Failed to dismount tape: " <<
      ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// rewindDrive
//------------------------------------------------------------------------------
void ReadtpCmd::rewindDrive(
  castor::tape::tapeserver::drive::DriveInterface &drive) {
  std::list<cta::log::Param> params;
  params.push_back(cta::log::Param("userName", m_userName));
  params.push_back(cta::log::Param("tapeVid", m_vid));
  params.push_back(cta::log::Param("tapeDrive", m_unitName));
  params.push_back(cta::log::Param("logicalLibrary", m_logicalLibrary));
  params.push_back(cta::log::Param("useLbp",boolToStr(m_useLbp)));
  params.push_back(cta::log::Param("driveSupportLbp",boolToStr(m_driveSupportLbp)));

  m_log(cta::log::INFO, "Rewinding tape", params);
  drive.rewind();
  m_log(cta::log::INFO, "Successfully rewound tape", params);
}

//------------------------------------------------------------------------------
// configureEncryption
//------------------------------------------------------------------------------
void ReadtpCmd::configureEncryption(
  const std::string &vid,
  castor::tape::tapeserver::drive::DriveInterface &drive) {
  try {
    const std::string DAEMON_CONFIG = "/etc/cta/cta-taped.conf";

    // Config file needed to find the cta-get-encryption-key script
    const cta::tape::daemon::TapedConfiguration tapedConfig =
      cta::tape::daemon::TapedConfiguration::createFromCtaConf(DAEMON_CONFIG, m_dummyLog);
    const std::string externalEncryptionKeyScript = tapedConfig.externalEncryptionKeyScript.value();
    const bool useEncryption = tapedConfig.useEncryption.value() == "yes";
    m_encryptionControl = std::make_unique<castor::tape::tapeserver::daemon::EncryptionControl>(useEncryption, externalEncryptionKeyScript);

    // We want those scoped params to last for the whole mount.
    // This will allow each session to be logged with its encryption
    // status:
    std::list<cta::log::Param> params;
    {
      auto encryptionStatus = m_encryptionControl->enable(drive, vid, castor::tape::tapeserver::daemon::EncryptionControl::SetTag::NO_SET_TAG);
      if (encryptionStatus.on) {
        params.push_back(cta::log::Param("encryption", "on"));
        params.push_back(cta::log::Param("encryptionKey", encryptionStatus.keyName));
        params.push_back(cta::log::Param("stdout", encryptionStatus.stdout));
        m_log(cta::log::INFO, "Drive encryption enabled for this mount", params);
      } else {
        params.push_back(cta::log::Param("encryption", "off"));
        m_log(cta::log::INFO, "Drive encryption not enabled for this mount", params);
      }
    }
  }
  catch (cta::exception::Exception& ex) {
    std::list<cta::log::Param> params;
    params.push_back(cta::log::Param("ErrorMessage", ex.getMessage().str()));
    m_log(cta::log::ERR, "Drive encryption could not be enabled for this mount.", params);
    throw ex;
  }
}

void ReadtpCmd::disableEncryption(castor::tape::tapeserver::drive::DriveInterface &drive) {
  // Disable encryption (or at least try)
  try {
    if (m_encryptionControl->disable(drive)) {
      m_log(cta::log::INFO, "Turned encryption off before unmounting");
    }
  } catch (cta::exception::Exception& ex) {
    m_log(cta::log::ERR, "Failed to turn off encryption before unmounting");
  }
}

//------------------------------------------------------------------------------
// printUsage
//------------------------------------------------------------------------------
void ReadtpCmd::printUsage(std::ostream &os) {
  ReadtpCmdLineArgs::printUsage(os);
}

//------------------------------------------------------------------------------
// boolToStr
//------------------------------------------------------------------------------
const char *ReadtpCmd::boolToStr(
  const bool value) {
  return value ? "true" : "false";
}

} // namespace readtp
} // namespace tapeserver
} // namespace cta
