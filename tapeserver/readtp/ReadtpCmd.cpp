/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "tapeserver/readtp/ReadtpCmd.hpp"

#include "castor/tape/tapeserver/file/FileReaderFactory.hpp"
#include "catalogue/Catalogue.hpp"
#include "catalogue/CatalogueFactory.hpp"
#include "catalogue/CatalogueFactoryFactory.hpp"
#include "catalogue/CatalogueItor.hpp"
#include "catalogue/TapeSearchCriteria.hpp"
#include "common/Constants.hpp"
#include "common/ReadTapeTestMode.hpp"
#include "common/exception/EncryptionException.hpp"
#include "common/log/DummyLogger.hpp"
#include "disk/DiskFile.hpp"
#include "mediachanger/LibrarySlotParser.hpp"
#include "rdbms/Login.hpp"
#include "scheduler/RetrieveJob.hpp"
#include "tapeserver/castor/tape/tapeserver/daemon/EncryptionControl.hpp"
#include "tapeserver/castor/tape/tapeserver/daemon/Payload.hpp"
#include "tapeserver/castor/tape/tapeserver/file/ReadSession.hpp"
#include "tapeserver/castor/tape/tapeserver/file/ReadSessionFactory.hpp"
#include "tapeserver/castor/tape/tapeserver/file/Structures.hpp"
#include "tapeserver/readtp/ReadtpCmdLineArgs.hpp"
#include "tapeserver/readtp/TapeFseqRange.hpp"
#include "tapeserver/readtp/TapeFseqRangeListSequence.hpp"

#include <chrono>
#include <numeric>

namespace cta::tapeserver::readtp {

//------------------------------------------------------------------------------
// exceptionThrowingMain
//------------------------------------------------------------------------------
int ReadtpCmd::exceptionThrowingMain(const int argc, char* const* const argv) {
  const ReadtpCmdLineArgs cmdLineArgs(argc, argv);
  if (cmdLineArgs.help) {
    printUsage(m_out);
    return 0;
  }

  auto useBlockId = cmdLineArgs.m_enablePositionByBlockID0 | cmdLineArgs.m_enablePositionByBlockID1 | cmdLineArgs.m_enablePositionByBlockID2;

  readAndSetConfiguration(getUsername(), cmdLineArgs);

  std::string testMode;

  switch (m_testMode) {
    case utils::ReadTapeTestMode::USE_FSEC:
      testMode = "FSec";
      break;
    case utils::ReadTapeTestMode::USE_BLOCK_ID_DEFAULT:
      testMode = "BlockID - Default";
      break;
    case utils::ReadTapeTestMode::USE_BLOCK_ID_1:
      testMode = "BlockID - Test 1";
      break;
    case utils::ReadTapeTestMode::USE_BLOCK_ID_2:
      testMode = "BlockID - Test 2";
      break;
    default:
      testMode = "unknown";
      break;
  }

  std::vector<cta::log::Param> params;
  params.emplace_back("userName", getUsername());
  params.emplace_back("tapeVid", cmdLineArgs.m_vid);
  params.push_back(cta::log::Param("searchBy", useBlockId  ? "BlockId" : "FSec"));
  params.push_back(cta::log::Param("testMode", testMode));
  m_log(cta::log::INFO, "Started", params);

  std::unique_ptr<castor::tape::tapeserver::drive::DriveInterface> drivePtr = createDrive();
  castor::tape::tapeserver::drive::DriveInterface& drive = *drivePtr.get();

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
  } catch (cta::exception::EncryptionException& ex) {
    params.emplace_back("tapeReadError", ex.getMessage().str());
    m_log(cta::log::ERR, "Failed to enable encryption for read of drive", params);
    throw cta::exception::EncryptionException(ex.what(), false);
  } catch (cta::exception::Exception& ne) {
    params.emplace_back("tapeReadError", ne.getMessage().str());
    m_log(cta::log::ERR, "Failed to read the tape", params);
    returnCode = 1;
  }
  unloadTape(m_vid, drive);

  // Disable encryption (or at least try)
  if (m_isTapeEncrypted) {
    disableEncryption(drive);
  }

  dismountTape(m_vid);

  return returnCode;
}

//------------------------------------------------------------------------------
// readAndSetConfiguration
//------------------------------------------------------------------------------
void ReadtpCmd::readAndSetConfiguration(const std::string& userName, const ReadtpCmdLineArgs& cmdLineArgs) {
  m_vid = cmdLineArgs.m_vid;
  m_fSeqRangeList = cmdLineArgs.m_fSeqRangeList;
  m_userName = userName;
  m_destinationFiles = readListFromFile(cmdLineArgs.m_destinationFileListURL);

  m_enableGlobalReadSession = cmdLineArgs.m_enableGlobalReadSession;
  if (cmdLineArgs.m_enablePositionByBlockID0) {
    m_testMode = cta::utils::ReadTapeTestMode::USE_BLOCK_ID_DEFAULT;
  } else if (cmdLineArgs.m_enablePositionByBlockID1) {
    m_testMode = cta::utils::ReadTapeTestMode::USE_BLOCK_ID_1;
  } else if (cmdLineArgs.m_enablePositionByBlockID2) {
    m_testMode = cta::utils::ReadTapeTestMode::USE_BLOCK_ID_2;
  } else {
    m_testMode = cta::utils::ReadTapeTestMode::USE_FSEC;
  }

  // Read taped config file
  const cta::tape::daemon::common::TapedConfiguration driveConfig =
    cta::tape::daemon::common::TapedConfiguration::createFromOptionalDriveName(cmdLineArgs.m_unitName, m_log);

  // Configure drive
  m_devFilename = driveConfig.driveDevice.value();
  m_rawLibrarySlot = driveConfig.driveControlPath.value();
  m_logicalLibrary = driveConfig.driveLogicalLibrary.value();
  m_unitName = driveConfig.driveName.value();

  // Configure rmc
  m_rmcProxy = std::make_unique<cta::mediachanger::RmcProxy>(driveConfig.rmcHost.value(),
                                                             driveConfig.rmcPort.value(),
                                                             driveConfig.rmcNetTimeout.value(),
                                                             driveConfig.rmcRequestAttempts.value());
  m_mc = std::make_unique<cta::mediachanger::MediaChangerFacade>(*(m_rmcProxy.get()), m_log);

  // Configure encryption
  const std::string externalEncryptionKeyScript = driveConfig.externalEncryptionKeyScript.value();
  const bool useEncryption = driveConfig.useEncryption.value() == "yes";
  m_encryptionControl =
    std::make_unique<castor::tape::tapeserver::daemon::EncryptionControl>(useEncryption, externalEncryptionKeyScript);

  // Configure catalogue
  const cta::rdbms::Login catalogueLogin = cta::rdbms::Login::parseFile(CATALOGUE_CONFIG_PATH);
  const uint64_t nbConns = 1;
  const uint64_t nbArchiveFileListingConns = 1;

  auto catalogueFactory =
    cta::catalogue::CatalogueFactoryFactory::create(m_dummyLog,  // to supress catalogue output messages
                                                    catalogueLogin,
                                                    nbConns,
                                                    nbArchiveFileListingConns);
  m_catalogue = catalogueFactory->create();

  std::vector<cta::log::Param> params;
  params.emplace_back("catalogueDbType", catalogueLogin.dbTypeToString(catalogueLogin.dbType));
  params.emplace_back("connectionString", catalogueLogin.connectionString);
  params.emplace_back("devFilename", m_devFilename);
  params.emplace_back("rawLibrarySlot", m_rawLibrarySlot);
  params.emplace_back("logicalLibrary", m_logicalLibrary);
  params.emplace_back("unitName", m_unitName);
  m_log(cta::log::INFO, "Read configuration", params);
}

//------------------------------------------------------------------------------
// isEncrypted
//------------------------------------------------------------------------------
bool ReadtpCmd::isEncrypted(const cta::common::dataStructures::Tape& tape) const {
  if (tape.encryptionKeyName) {
    return true;
  }
  return false;
}

//------------------------------------------------------------------------------
// readListFromFile
//------------------------------------------------------------------------------
std::list<std::string> ReadtpCmd::readListFromFile(const std::string& filename) const {
  std::ifstream file(filename);
  std::list<std::string> str_list;
  if (file.fail()) {
    throw std::runtime_error("Unable to open file " + filename);
  }

  std::string line;

  while (std::getline(file, line)) {
    // Strip out comments
    if (auto pos = line.find('#'); pos != std::string::npos) {
      line.resize(pos);
    }

    // Extract the list items
    std::stringstream ss(line);
    while (!ss.eof()) {
      std::string item;
      ss >> item;
      // skip blank lines or lines consisting only of whitespace
      if (item.empty()) {
        continue;
      }

      str_list.push_back(item);
    }
  }
  return str_list;
}

//------------------------------------------------------------------------------
// createDrive
//------------------------------------------------------------------------------
std::unique_ptr<castor::tape::tapeserver::drive::DriveInterface> ReadtpCmd::createDrive() {
  castor::tape::SCSI::DeviceVector dv(m_sysWrapper);
  castor::tape::SCSI::DeviceInfo driveInfo = dv.findBySymlink(m_devFilename);

  // Instantiate the drive object
  std::unique_ptr<castor::tape::tapeserver::drive::DriveInterface> drive(
    castor::tape::tapeserver::drive::createDrive(driveInfo, m_sysWrapper));

  if (nullptr == drive.get()) {
    cta::exception::Exception ex;
    ex.getMessage() << "Failed to instantiate drive object";
    throw ex;
  }

  return drive;
}

//------------------------------------------------------------------------------
// isDriveSupportLbp
//------------------------------------------------------------------------------
bool ReadtpCmd::isDriveSupportLbp(castor::tape::tapeserver::drive::DriveInterface& drive) const {
  castor::tape::tapeserver::drive::deviceInfo devInfo = drive.getDeviceInfo();
  if (devInfo.isPIsupported) {  //drive supports LBP
    return true;
  } else {
    return false;
  }
}

//------------------------------------------------------------------------------
// setLbpMode
//------------------------------------------------------------------------------
void ReadtpCmd::setLbpMode(castor::tape::tapeserver::drive::DriveInterface& drive) {
  std::vector<cta::log::Param> params;
  params.emplace_back("userName", m_userName);
  params.emplace_back("tapeVid", m_vid);
  params.emplace_back("tapeDrive", m_unitName);
  params.emplace_back("logicalLibrary", m_logicalLibrary);
  params.emplace_back("useLbp", boolToStr(m_useLbp));
  params.emplace_back("driveSupportLbp", boolToStr(m_driveSupportLbp));

  if (m_useLbp) {
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
void ReadtpCmd::mountTape(const std::string& vid) {
  std::unique_ptr<cta::mediachanger::LibrarySlot> librarySlotPtr;
  librarySlotPtr.reset(cta::mediachanger::LibrarySlotParser::parse(m_rawLibrarySlot));
  const cta::mediachanger::LibrarySlot& librarySlot = *librarySlotPtr.get();

  std::vector<cta::log::Param> params;
  params.emplace_back("userName", m_userName);
  params.emplace_back("tapeVid", vid);
  params.emplace_back("tapeDrive", m_unitName);
  params.emplace_back("logicalLibrary", m_logicalLibrary);
  params.emplace_back("librarySlot", librarySlot.str());
  params.emplace_back("useLbp", boolToStr(m_useLbp));
  params.emplace_back("driveSupportLbp", boolToStr(m_driveSupportLbp));

  m_mc->mountTapeReadOnly(vid, librarySlot);
  m_log(cta::log::INFO, "Mounted tape", params);
}

//------------------------------------------------------------------------------
// waitUntilTapeLoaded
//------------------------------------------------------------------------------
void ReadtpCmd::waitUntilTapeLoaded(castor::tape::tapeserver::drive::DriveInterface& drive, const int timeoutSecond) {
  std::vector<cta::log::Param> params;
  params.emplace_back("userName", m_userName);
  params.emplace_back("tapeVid", m_vid);
  params.emplace_back("tapeDrive", m_unitName);
  params.emplace_back("logicalLibrary", m_logicalLibrary);
  params.emplace_back("useLbp", boolToStr(m_useLbp));
  params.emplace_back("driveSupportLbp", boolToStr(m_driveSupportLbp));

  try {
    m_log(cta::log::INFO, "Loading tape", params);
    drive.waitUntilReady(timeoutSecond);
    m_log(cta::log::INFO, "Loaded tape", params);
  } catch (cta::exception::Exception& ne) {
    cta::exception::Exception ex;
    ex.getMessage() << "Failed to wait for tape to be loaded: " << ne.getMessage().str();
    throw ex;
  }
}

//Basic class representing a read job
class BasicRetrieveJob : public cta::RetrieveJob {
public:
  BasicRetrieveJob()
      : cta::RetrieveJob(nullptr,
                         cta::common::dataStructures::RetrieveRequest(),
                         cta::common::dataStructures::ArchiveFile(),
                         1,
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
void ReadtpCmd::readTapeFiles(castor::tape::tapeserver::drive::DriveInterface& drive) {
  cta::disk::DiskFileFactory fileFactory(0);

  catalogue::TapeSearchCriteria searchCriteria;
  searchCriteria.vid = m_vid;

  auto tapeList = m_catalogue->Tape()->getTapes(searchCriteria);
  if (tapeList.empty()) {
    std::vector<cta::log::Param> params;
    params.emplace_back("userName", getUsername());
    params.emplace_back("tapeVid", m_vid);
    params.emplace_back("tapeDrive", m_unitName);
    params.emplace_back("logicalLibrary", m_logicalLibrary);
    params.emplace_back("useLbp", boolToStr(m_useLbp));
    params.emplace_back("driveSupportLbp", boolToStr(m_driveSupportLbp));
    m_log(cta::log::ERR, "Failed to get tape from catalogue", params);
    return;
  }
  const auto& tape = tapeList.front();

  castor::tape::tapeserver::daemon::VolumeInfo volInfo;
  volInfo.vid = tape.vid;
  volInfo.nbFiles = tape.nbMasterFiles;
  volInfo.mountType = cta::common::dataStructures::MountType::Retrieve;
  volInfo.labelFormat = tape.labelFormat;
  volInfo.encryptionKeyName = tape.encryptionKeyName;
  volInfo.tapePool = tape.tapePoolName;

  m_isTapeEncrypted = isEncrypted(tape);

  if (m_isTapeEncrypted) {
    try {
      configureEncryption(volInfo, drive);
    } catch (cta::exception::Exception& ex) {
      std::vector<cta::log::Param> params;
      params.emplace_back("vid", m_vid);
      m_log(cta::log::ERR, "Configuring encryption failed", params);
      throw cta::exception::EncryptionException(ex.what(), false);
    }
  }

  std::unique_ptr<castor::tape::tapeFile::ReadSession> readSession;
    // We should test the impact of creating a single read session for all vs one-per-file
    if (m_enableGlobalReadSession) {
      readSession = castor::tape::tapeFile::ReadSessionFactory::create(drive, volInfo, m_useLbp, m_testMode);
    }
    TapeFseqRangeListSequence fSeqRangeListSequence(&m_fSeqRangeList);
    std::string destinationFile = getNextDestinationUrl();
    uint64_t fSeq;
    size_t totalDataSize = 0;

    std::vector<castor::tape::tapeFile::FileReader::BlockReadTimer> fileTimers;
    castor::tape::tapeFile::FileReader::ChronoTimer t_begin;

    while (fSeqRangeListSequence.hasMore()) {
      try {
        fSeq = fSeqRangeListSequence.next();
        if (fSeq > tape.lastFSeq) {
          break; //reached end of tape
        }
        std::unique_ptr<cta::disk::WriteFile> wfptr;
        wfptr.reset(fileFactory.createWriteFile(destinationFile));
        cta::disk::WriteFile &wf = *wfptr.get();
        // This is the old behaviour, which we want to be able to test against
        if (!m_enableGlobalReadSession) {
          readSession = castor::tape::tapeFile::ReadSessionFactory::create(drive, volInfo, m_useLbp, m_testMode);
        }
        auto [dataSize, readTimer] = readTapeFile(*readSession, fSeq, wf, volInfo);
        totalDataSize += dataSize;
        fileTimers.push_back(readTimer);
      m_nbSuccessReads++;  // if readTapeFile returns, file was read successfully
      destinationFile = getNextDestinationUrl();
    } catch (tapeserver::readtp::NoSuchFSeqException&) {
      //Do nothing
    } catch (exception::Exception& ne) {
      std::vector<cta::log::Param> params;
      params.emplace_back("userName", getUsername());
      params.emplace_back("tapeVid", m_vid);
      params.emplace_back("destinationFile", destinationFile);
      params.emplace_back("tapeDrive", m_unitName);
      params.emplace_back("logicalLibrary", m_logicalLibrary);
      params.emplace_back("useLbp", boolToStr(m_useLbp));
      params.emplace_back("driveSupportLbp", boolToStr(m_driveSupportLbp));
      params.emplace_back("fSeq", fSeq);
      params.emplace_back("tapeReadError", ne.getMessage().str());
      m_log(cta::log::ERR, "Failed to read file from tape", params);
      m_nbFailedReads++;
    }
  }

  // Get the average of all timings, except the first one (because actual positioning may happen here...)

  double elapsedTimeSec = t_begin.elapsedTime();
  auto throughputMBs = (static_cast<double>(totalDataSize) / elapsedTimeSec) / (1024 * 1024);
  std::vector<cta::log::Param> params;
  params.emplace_back("userName", getUsername());
  params.emplace_back("tapeVid", m_vid);
  params.emplace_back("tapeDrive", m_unitName);
  params.emplace_back("logicalLibrary", m_logicalLibrary);
  params.emplace_back("useLbp", boolToStr(m_useLbp));
  params.emplace_back("driveSupportLbp", boolToStr(m_driveSupportLbp));
  params.emplace_back("nbReads", m_nbSuccessReads + m_nbFailedReads);
  params.emplace_back("nbSuccessfullReads", m_nbSuccessReads);
  params.emplace_back("totalDataMB", totalDataSize / (1024 * 1024));
  params.emplace_back("totalElapsedTimeSec", elapsedTimeSec);
  params.emplace_back("globalThroughputMBs", throughputMBs);
  params.emplace_back("nbFailedReads", m_nbFailedReads);

  m_log(cta::log::INFO, "Finished reading tape", params);
}

//------------------------------------------------------------------------------
// readTapeFile
//------------------------------------------------------------------------------
std::tuple<size_t, castor::tape::tapeFile::FileReader::BlockReadTimer> ReadtpCmd::readTapeFile(castor::tape::tapeFile::ReadSession& readSession,
                             const uint64_t& fSeq,
                             cta::disk::WriteFile& wf,
                             const castor::tape::tapeserver::daemon::VolumeInfo& volInfo) {
  std::vector<cta::log::Param> params;
  params.emplace_back("userName", m_userName);
  params.emplace_back("tapeVid", m_vid);
  params.emplace_back("fSeq", fSeq);
  params.emplace_back("tapeDrive", m_unitName);
  params.emplace_back("logicalLibrary", m_logicalLibrary);
  params.emplace_back("useLbp", boolToStr(m_useLbp));
  params.emplace_back("driveSupportLbp", boolToStr(m_driveSupportLbp));
  params.emplace_back("destinationURL", wf.URL());

  catalogue::TapeFileSearchCriteria searchCriteria;
  searchCriteria.vid = m_vid;
  searchCriteria.fSeq = fSeq;
  auto itor = m_catalogue->ArchiveFile()->getArchiveFilesItor(searchCriteria);

  if (!itor.hasMore()) {
    throw tapeserver::readtp::NoSuchFSeqException();
  }

  m_log(cta::log::INFO, "Reading file from tape", params);

  auto archiveFile = itor.next();
  archiveFile.tapeFiles.removeAllVidsExcept(m_vid);

  BasicRetrieveJob fileToRecall;
  fileToRecall.retrieveRequest.archiveFileID = archiveFile.archiveFileID;
  fileToRecall.selectedCopyNb = 0;
  fileToRecall.archiveFile.tapeFiles.emplace_back();
  fileToRecall.selectedTapeFile().fSeq = fSeq;
  fileToRecall.selectedTapeFile().blockId = archiveFile.tapeFiles.front().blockId;
  fileToRecall.positioningMethod = m_testMode == utils::ReadTapeTestMode::USE_FSEC ? cta::PositioningMethod::ByFSeq : cta::PositioningMethod::ByBlock;

  const auto reader = castor::tape::tapeFile::FileReaderFactory::create(readSession, fileToRecall, m_testMode);
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
  auto readTimer = reader->getReaderTimer();

  params.emplace_back("checksumType", "ADLER32");
  std::stringstream sstream;
  sstream << std::hex << checksum_adler32;
  params.emplace_back("checksumValue", "0x" + sstream.str());
  params.emplace_back("readFileSize", read_data_size);
  params.emplace_back("timerPositionSec", readTimer.positioning);
  params.emplace_back("timerHeaderSec", std::accumulate(readTimer.headerBlocks.begin(), readTimer.headerBlocks.end(), 0.0));
  params.emplace_back("timerHeaderTMSec", readTimer.headerTM);
  params.emplace_back("timerDataSec", std::accumulate(readTimer.dataBlocks.begin(), readTimer.dataBlocks.end(), 0.0));
  params.emplace_back("timerDataTMSec", readTimer.dataTM);
  params.emplace_back("timerHeaderSec", std::accumulate(readTimer.trailerBlocks.begin(), readTimer.trailerBlocks.end(), 0.0));
  params.emplace_back("timerHeaderTMSec", readTimer.trailerTM);
  m_log(cta::log::INFO, "Read file from tape successfully", params);
  return {read_data_size, readTimer};
}

//------------------------------------------------------------------------------
// unloadTape
//------------------------------------------------------------------------------
void ReadtpCmd::unloadTape([[maybe_unused]] const std::string& vid,
                           castor::tape::tapeserver::drive::DriveInterface& drive) {
  std::unique_ptr<cta::mediachanger::LibrarySlot> librarySlotPtr;
  librarySlotPtr.reset(cta::mediachanger::LibrarySlotParser::parse(m_rawLibrarySlot));
  const cta::mediachanger::LibrarySlot& librarySlot = *librarySlotPtr.get();

  std::vector<cta::log::Param> params;
  params.emplace_back("userName", m_userName);
  params.emplace_back("tapeVid", m_vid);
  params.emplace_back("tapeDrive", m_unitName);
  params.emplace_back("logicalLibrary", m_logicalLibrary);
  params.emplace_back("useLbp", boolToStr(m_useLbp));
  params.emplace_back("librarySlot", librarySlot.str());
  params.emplace_back("driveSupportLbp", boolToStr(m_driveSupportLbp));

  try {
    m_log(cta::log::INFO, "Unloading tape", params);
    drive.unloadTape();
    m_log(cta::log::INFO, "Unloaded tape", params);
  } catch (cta::exception::Exception& ne) {
    cta::exception::Exception ex;
    ex.getMessage() << "Failed to unload tape: " << ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// dismountTape
//------------------------------------------------------------------------------
void ReadtpCmd::dismountTape(const std::string& vid) {
  std::unique_ptr<cta::mediachanger::LibrarySlot> librarySlotPtr;
  librarySlotPtr.reset(cta::mediachanger::LibrarySlotParser::parse(m_rawLibrarySlot));
  const cta::mediachanger::LibrarySlot& librarySlot = *librarySlotPtr.get();

  std::vector<cta::log::Param> params;
  params.emplace_back("userName", m_userName);
  params.emplace_back("tapeVid", m_vid);
  params.emplace_back("tapeDrive", m_unitName);
  params.emplace_back("logicalLibrary", m_logicalLibrary);
  params.emplace_back("librarySlot", librarySlot.str());
  params.emplace_back("useLbp", boolToStr(m_useLbp));
  params.emplace_back("driveSupportLbp", boolToStr(m_driveSupportLbp));

  try {
    m_log(cta::log::INFO, "Dismounting tape", params);
    m_mc->dismountTape(vid, librarySlot);
    m_log(cta::log::INFO, "Dismounted tape", params);
  } catch (cta::exception::Exception& ne) {
    cta::exception::Exception ex;
    ex.getMessage() << "Failed to dismount tape: " << ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// rewindDrive
//------------------------------------------------------------------------------
void ReadtpCmd::rewindDrive(castor::tape::tapeserver::drive::DriveInterface& drive) {
  std::vector<cta::log::Param> params;
  params.emplace_back("userName", m_userName);
  params.emplace_back("tapeVid", m_vid);
  params.emplace_back("tapeDrive", m_unitName);
  params.emplace_back("logicalLibrary", m_logicalLibrary);
  params.emplace_back("useLbp", boolToStr(m_useLbp));
  params.emplace_back("driveSupportLbp", boolToStr(m_driveSupportLbp));

  m_log(cta::log::INFO, "Rewinding tape", params);
  drive.rewind();
  m_log(cta::log::INFO, "Successfully rewound tape", params);
}

//------------------------------------------------------------------------------
// configureEncryption
//------------------------------------------------------------------------------
void ReadtpCmd::configureEncryption(castor::tape::tapeserver::daemon::VolumeInfo& volInfo,
                                    castor::tape::tapeserver::drive::DriveInterface& drive) {
  try {
    // We want those scoped params to last for the whole mount.
    // This will allow each session to be logged with its encryption
    // status:
    std::vector<cta::log::Param> params;
    {
      auto encryptionStatus = m_encryptionControl->enable(drive, volInfo, *m_catalogue, false);
      if (encryptionStatus.on) {
        params.emplace_back("encryption", "on");
        params.emplace_back("encryptionKeyName", encryptionStatus.keyName);
        params.emplace_back("scriptPath", m_encryptionControl->getScriptPath());
        params.emplace_back("stdout", encryptionStatus.stdout);
        m_log(cta::log::INFO, "Drive encryption enabled for this mount", params);
      } else {
        params.emplace_back("encryption", "off");
        m_log(cta::log::INFO, "Drive encryption not enabled for this mount", params);
      }
    }
  } catch (cta::exception::Exception& ex) {
    std::vector<cta::log::Param> params;
    params.emplace_back("ErrorMessage", ex.getMessage().str());
    m_log(cta::log::ERR, "Drive encryption could not be enabled for this mount.", params);
    throw ex;
  }
}

void ReadtpCmd::disableEncryption(castor::tape::tapeserver::drive::DriveInterface& drive) {
  // Disable encryption (or at least try)
  try {
    if (m_encryptionControl->disable(drive)) {
      m_log(cta::log::INFO, "Turned encryption off before unmounting");
    }
  } catch (cta::exception::Exception&) {
    m_log(cta::log::ERR, "Failed to turn off encryption before unmounting");
  }
}

//------------------------------------------------------------------------------
// printUsage
//------------------------------------------------------------------------------
void ReadtpCmd::printUsage(std::ostream& os) {
  ReadtpCmdLineArgs::printUsage(os);
}

//------------------------------------------------------------------------------
// boolToStr
//------------------------------------------------------------------------------
const char* ReadtpCmd::boolToStr(const bool value) {
  return value ? "true" : "false";
}

}  // namespace cta::tapeserver::readtp
