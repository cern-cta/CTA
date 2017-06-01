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

#define __STDC_CONSTANT_MACROS // For using stdint macros (stdint is included
// by inttypes.h, so we shoot first)
#include <stdint.h>
#include <inttypes.h>
#include <gtest/gtest.h>

#include "castor/messages/TapeserverProxyDummy.hpp"
#include "castor/tape/tapeserver/daemon/DataTransferSession.hpp"
#include "castor/tape/tapeserver/daemon/VolumeInfo.hpp"
#include "castor/tape/tapeserver/system/Wrapper.hpp"
#include "castor/tape/tapeserver/file/File.hpp"
#include "castor/tape/tapeserver/drive/FakeDrive.hpp"
#include "catalogue/InMemoryCatalogue.hpp"
#include "common/exception/Exception.hpp"
#include "common/log/DummyLogger.hpp"
#include "common/log/StringLogger.hpp"
#include "common/make_unique.hpp"
#include "common/processCap/ProcessCapDummy.hpp"
#include "common/threading/Thread.hpp"
#include "common/utils/utils.hpp"
#include "mediachanger/MediaChangerFacade.hpp"
//#include "smc_struct.h"
//#include "scheduler/DummyScheduler.hpp"
#include "scheduler/OStoreDB/OStoreDBFactory.hpp"
#include "scheduler/MountType.hpp"
//#include "nameserver/NameServer.hpp"
#include "scheduler/Scheduler.hpp"
#include "scheduler/testingMocks/MockRetrieveMount.hpp"
#include "scheduler/testingMocks/MockArchiveJob.hpp"
#include "scheduler/testingMocks/MockArchiveMount.hpp"
#include "tests/TempFile.hpp"
#include "objectstore/BackendRadosTestSwitch.hpp"

#include <dirent.h>
#include <fcntl.h>
#include <stdexcept>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <zlib.h>

using namespace castor::tape::tapeserver;
using namespace castor::tape::tapeserver::daemon;

namespace unitTests {

namespace {

/**
 * This structure is used to parameterize scheduler tests.
 */
struct DataTransferSessionTestParam {
  cta::SchedulerDatabaseFactory &dbFactory;

  DataTransferSessionTestParam(
    cta::SchedulerDatabaseFactory &dbFactory):
    dbFactory(dbFactory) {
 }
}; // struct DataTransferSessionTest

}

/**
 * The data transfer test is a parameterized test.  It takes a pair of name server
 * and scheduler database factories as a parameter.
 */
class DataTransferSessionTest: public ::testing::TestWithParam<DataTransferSessionTestParam> {
public:
  
  DataTransferSessionTest() { }


  class FailedToGetCatalogue: public std::exception {
  public:
    const char *what() const throw() {
      return "Failed to get catalogue";
    }
  };

  class FailedToGetScheduler: public std::exception {
  public:
    const char *what() const throw() {
      return "Failed to get scheduler";
    }
  };

    virtual void SetUp() {
    using namespace cta;

    const DataTransferSessionTestParam &param = GetParam();
    m_db = param.dbFactory.create();
    const uint64_t nbConns = 1;
    //m_catalogue = cta::make_unique<catalogue::SchemaCreatingSqliteCatalogue>(m_tempSqliteFile.path(), nbConns);
    m_catalogue = cta::make_unique<catalogue::InMemoryCatalogue>(nbConns);
    m_scheduler = cta::make_unique<Scheduler>(*m_catalogue, *m_db, 5, 2*1000*1000);
    
    strncpy(m_tmpDir, "/tmp/DataTransferSessionTestXXXXXX", sizeof(m_tmpDir));
    if(!mkdtemp(m_tmpDir)) {
      const std::string errMsg = cta::utils::errnoToString(errno);
      std::ostringstream msg;
      msg << "Failed to create directory with template"
        " /tmp/DataTransferSessionTestXXXXXX: " << errMsg;
      bzero(m_tmpDir, sizeof(m_tmpDir));
      throw cta::exception::Exception(msg.str());
    }
    
    struct stat statBuf;
    bzero(&statBuf, sizeof(statBuf));
    if(stat(m_tmpDir, &statBuf)) {
      const std::string errMsg = cta::utils::errnoToString(errno);
      std::ostringstream msg;
      msg << "Failed to stat directory " << m_tmpDir << ": " << errMsg;
      throw cta::exception::Exception(msg.str());
    }

    std::ostringstream cmd;
    cmd << "touch " << m_tmpDir << "/hello";
    system(cmd.str().c_str());
  }

  virtual void TearDown() {
    m_scheduler.reset();
    m_catalogue.reset();
    m_db.reset();
    
    // If Setup() created a temporary directory
    if(m_tmpDir) {

      // Openn the directory
      std::unique_ptr<DIR, std::function<int(DIR*)>>
        dir(opendir(m_tmpDir), closedir);
      if(NULL == dir.get()) {
        const std::string errMsg = cta::utils::errnoToString(errno);
        std::ostringstream msg;
        msg << "Failed to open directory " << m_tmpDir << ": " << errMsg;
        throw cta::exception::Exception(msg.str());
      }

      // Delete each of the files within the directory
      struct dirent *entry = NULL;
      while((entry = readdir(dir.get()))) {
        const std::string entryName(entry->d_name);
        if(entryName != "." && entryName != "..") {
          const std::string entryPath = std::string(m_tmpDir) + "/" + entryName;
          if(unlink(entryPath.c_str())) {
            const std::string errMsg = cta::utils::errnoToString(errno);
            std::ostringstream msg;
            msg << "Failed to unlink " << entryPath;
            throw cta::exception::Exception(msg.str());
          }
        }
      }

      // Delete the now empty directory
      if(rmdir(m_tmpDir)) {
        const std::string errMsg = cta::utils::errnoToString(errno);
        std::ostringstream msg;
        msg << "Failed to delete directory " << m_tmpDir << ": " << errMsg;
        throw cta::exception::Exception(msg.str());
      }
    }
  }

  cta::catalogue::Catalogue &getCatalogue() {
    cta::catalogue::Catalogue *const ptr = m_catalogue.get();
    if(NULL == ptr) {
      throw FailedToGetCatalogue();
    }
    return *ptr;
  }
    
  cta::Scheduler &getScheduler() {
    cta::Scheduler *const ptr = m_scheduler.get();
    if(NULL == ptr) {
      throw FailedToGetScheduler();
    }
    return *ptr;
  }
  
  void setupDefaultCatalogue() {
    using namespace cta;
    auto & catalogue=getCatalogue();

    const std::string mountPolicyName = "mount_group";
    const uint64_t archivePriority = 1;
    const uint64_t minArchiveRequestAge = 2;
    const uint64_t retrievePriority = 3;
    const uint64_t minRetrieveRequestAge = 4;
    const uint64_t maxDrivesAllowed = 5;
    const std::string mountPolicyComment = "create mount group";

    ASSERT_TRUE(catalogue.getMountPolicies().empty());

    catalogue.createMountPolicy(
      s_adminOnAdminHost,
      mountPolicyName,
      archivePriority,
      minArchiveRequestAge,
      retrievePriority,
      minRetrieveRequestAge,
      maxDrivesAllowed,
      mountPolicyComment);

    const std::list<common::dataStructures::MountPolicy> groups = catalogue.getMountPolicies();
    ASSERT_EQ(1, groups.size());
    const common::dataStructures::MountPolicy group = groups.front();
    ASSERT_EQ(mountPolicyName, group.name);
    ASSERT_EQ(archivePriority, group.archivePriority);
    ASSERT_EQ(minArchiveRequestAge, group.archiveMinRequestAge);
    ASSERT_EQ(retrievePriority, group.retrievePriority);
    ASSERT_EQ(minRetrieveRequestAge, group.retrieveMinRequestAge);
    ASSERT_EQ(maxDrivesAllowed, group.maxDrivesAllowed);
    ASSERT_EQ(mountPolicyComment, group.comment);

    const std::string ruleComment = "create requester mount-rule";
    cta::common::dataStructures::UserIdentity userIdentity;
    catalogue.createRequesterMountRule(s_adminOnAdminHost, mountPolicyName, s_diskInstance, s_userName, ruleComment);

    const std::list<common::dataStructures::RequesterMountRule> rules = catalogue.getRequesterMountRules();
    ASSERT_EQ(1, rules.size());

    const common::dataStructures::RequesterMountRule rule = rules.front();

    ASSERT_EQ(s_userName, rule.name);
    ASSERT_EQ(mountPolicyName, rule.mountPolicy);
    ASSERT_EQ(ruleComment, rule.comment);
    ASSERT_EQ(s_adminOnAdminHost.username, rule.creationLog.username);
    ASSERT_EQ(s_adminOnAdminHost.host, rule.creationLog.host);
    ASSERT_EQ(rule.creationLog, rule.lastModificationLog);

    common::dataStructures::StorageClass storageClass;
    storageClass.diskInstance = s_diskInstance;
    storageClass.name = s_storageClassName;
    storageClass.nbCopies = 1;
    storageClass.comment = "create storage class";
    m_catalogue->createStorageClass(s_adminOnAdminHost, storageClass);

    const uint16_t nbPartialTapes = 1;
    const std::string tapePoolComment = "Tape-pool comment";
    const bool tapePoolEncryption = false;
    ASSERT_NO_THROW(catalogue.createTapePool(s_adminOnAdminHost, s_tapePoolName,
      nbPartialTapes, tapePoolEncryption, tapePoolComment));
    const uint16_t copyNb = 1;
    const std::string archiveRouteComment = "Archive-route comment";
    catalogue.createArchiveRoute(s_adminOnAdminHost, s_diskInstance, s_storageClassName, copyNb, s_tapePoolName,
      archiveRouteComment);
  }

private:

  // Prevent copying
  DataTransferSessionTest(const DataTransferSessionTest &) = delete;

  // Prevent assignment
  DataTransferSessionTest & operator= (const DataTransferSessionTest &) = delete;

  std::unique_ptr<cta::SchedulerDatabase> m_db;
  std::unique_ptr<cta::catalogue::Catalogue> m_catalogue;
  std::unique_ptr<cta::Scheduler> m_scheduler;
  
protected:
  // Default parameters for storage classes, etc...
  const std::string s_userName = "user_name";
  const std::string s_diskInstance = "disk_instance";
  const std::string s_storageClassName = "TestStorageClass";
  const cta::common::dataStructures::SecurityIdentity s_adminOnAdminHost = { "admin1", "host1" };
  const std::string s_tapePoolName = "TestTapePool";
  const std::string s_libraryName = "TestLogicalLibrary";
  const std::string s_vid = "TstVid"; // We really need size <= 6 characters due to tape label format.
  //TempFile m_tempSqliteFile;
  /**
   * Temporary directory created with mkdtemp that will be used to contain the
   * destination remote files of the tests that need to create them.
   *
   * Please note that a new temporary directory is created and deleted for each
   * test by the Setup() and TearDown() methods.
   */
  char m_tmpDir[100];

}; // class DataTransferSessionTest

TEST_P(DataTransferSessionTest, DataTransferSessionGooddayRecall) {
  // 0) Prepare the logger for everyone
  cta::log::StringLogger logger("tapeServerUnitTest",cta::log::DEBUG);
  cta::log::LogContext logContext(logger);
  
  setupDefaultCatalogue();
  // 1) prepare the fake scheduler
  std::string vid = s_vid;
  // cta::MountType::Enum mountType = cta::MountType::RETRIEVE;

  // 3) Prepare the necessary environment (logger, plus system wrapper), 
  castor::tape::System::mockWrapper mockSys;
  mockSys.delegateToFake();
  mockSys.disableGMockCallsCounting();
  mockSys.fake.setupForVirtualDriveSLC6();
  //delete is unnecessary
  //pointer with ownership will be passed to the application,
  //which will do the delete 
  mockSys.fake.m_pathToDrive["/dev/nst0"] = new castor::tape::tapeserver::drive::FakeDrive;

  // 4) Create the scheduler
  auto & catalogue = getCatalogue();
  auto & scheduler = getScheduler();
  
  // Always use the same requester
  const cta::common::dataStructures::SecurityIdentity requester;

  // List to remember the path of each remote file so that the existance of the
  // files can be tested for at the end of the test
  std::list<std::string> remoteFilePaths;
  
  // 5) Create the environment for the migration to happen (library + tape) 
    const std::string libraryComment = "Library comment";
  catalogue.createLogicalLibrary(s_adminOnAdminHost, s_libraryName,
    libraryComment);
  {
    auto libraries = catalogue.getLogicalLibraries();
    ASSERT_EQ(1, libraries.size());
    ASSERT_EQ(s_libraryName, libraries.front().name);
    ASSERT_EQ(libraryComment, libraries.front().comment);
  }
  const uint64_t capacityInBytes = 12345678;
  const std::string tapeComment = "Tape comment";
  bool notDisabled = false;
  bool notFull = false;
  catalogue.createTape(s_adminOnAdminHost, s_vid, s_libraryName, s_tapePoolName, cta::nullopt, capacityInBytes,
    notDisabled, notFull, tapeComment);
  
  // 6) Prepare files for reading by writing them to the mock system
  {
    // Label the tape
    castor::tape::tapeFile::LabelSession ls(*mockSys.fake.m_pathToDrive["/dev/nst0"], 
        s_vid, false);
    mockSys.fake.m_pathToDrive["/dev/nst0"]->rewind();
    // And write to it
    castor::tape::tapeserver::daemon::VolumeInfo volInfo;
    volInfo.vid=s_vid;
    castor::tape::tapeFile::WriteSession ws(*mockSys.fake.m_pathToDrive["/dev/nst0"],
       volInfo , 0, true, false);

    // Write a few files on the virtual tape and modify the archive name space
    // so that it is in sync
    uint8_t data[1000];
    size_t archiveFileSize=sizeof(data);
    castor::tape::SCSI::Structures::zeroStruct(&data);
    for (int fseq=1; fseq <= 10 ; fseq ++) {
      // Create a path to a remote destination file
      std::ostringstream remoteFilePath;
      remoteFilePath << "file://" << m_tmpDir << "/test" << fseq;
      remoteFilePaths.push_back(remoteFilePath.str());

      // Create an archive file entry in the archive namespace
      cta::catalogue::TapeFileWritten tapeFileWritten;
      
      // Write the file to tape
      cta::MockArchiveMount mam(catalogue);
      std::unique_ptr<cta::ArchiveJob> aj(new cta::MockArchiveJob(mam, catalogue));
      aj->tapeFile.fSeq = fseq;
      aj->archiveFile.archiveFileID = fseq;
      castor::tape::tapeFile::WriteFile wf(&ws, *aj, archiveFileSize);
      tapeFileWritten.blockId = wf.getBlockId();
      // Write the data (one block)
      wf.write(data, archiveFileSize);
      // Close the file
      wf.close();

      // Create file entry in the archive namespace
      tapeFileWritten.archiveFileId=fseq;
      tapeFileWritten.checksumType="ADLER32";
      tapeFileWritten.checksumValue=cta::utils::getAdler32String(data, archiveFileSize);
      tapeFileWritten.vid=volInfo.vid;
      tapeFileWritten.size=archiveFileSize;
      tapeFileWritten.fSeq=fseq;
      tapeFileWritten.copyNb=1;
      tapeFileWritten.compressedSize=archiveFileSize; // No compression
      tapeFileWritten.diskInstance = s_diskInstance;
      tapeFileWritten.diskFileId = fseq;
      tapeFileWritten.diskFilePath = remoteFilePath.str();
      tapeFileWritten.diskFileUser = s_userName;
      tapeFileWritten.diskFileGroup = "someGroup";
      tapeFileWritten.diskFileRecoveryBlob = "B106";
      tapeFileWritten.storageClassName = s_storageClassName;
      tapeFileWritten.tapeDrive = "drive0";
      catalogue.filesWrittenToTape(std::set<cta::catalogue::TapeFileWritten>{tapeFileWritten});

      // Schedule the retrieval of the file
      std::string diskInstance="disk_instance";
      cta::common::dataStructures::RetrieveRequest rReq;
      rReq.archiveFileID=fseq;
      rReq.requester.name = s_userName;
      rReq.requester.group = "someGroup";
      rReq.dstURL = remoteFilePaths.back();
      std::list<std::string> archiveFilePaths;
      scheduler.queueRetrieve(diskInstance, rReq, logContext);
    }
  }
    
  // 6) Report the drive's existence and put it up in the drive register.
  cta::tape::daemon::TpconfigLine driveConfig("T10D6116", "TestLogicalLibrary", "/dev/tape_T10D6116", "manual");
  cta::common::dataStructures::DriveInfo driveInfo;
  driveInfo.driveName=driveConfig.unitName;
  driveInfo.logicalLibrary=driveConfig.rawLibrarySlot;
  // We need to create the drive in the registry before being able to put it up.
  scheduler.reportDriveStatus(driveInfo, cta::common::dataStructures::MountType::NoMount, cta::common::dataStructures::DriveStatus::Down);
  scheduler.setDesiredDriveState(s_adminOnAdminHost, driveConfig.unitName, true, false);

  // 7) Create the data transfer session
  DataTransferConfig castorConf;
  castorConf.bufsz = 1024*1024; // 1 MB memory buffers
  castorConf.nbBufs = 10;
  castorConf.bulkRequestRecallMaxBytes = UINT64_C(100)*1000*1000*1000;
  castorConf.bulkRequestRecallMaxFiles = 1000;
  castorConf.nbDiskThreads = 1;
  cta::log::DummyLogger dummyLog("dummy");
  cta::mediachanger::MediaChangerFacade mc(dummyLog);
  cta::server::ProcessCap capUtils;
  castor::messages::TapeserverProxyDummy initialProcess;
  castor::tape::tapeserver::daemon::DataTransferSession sess("tapeHost", logger, mockSys,
    driveConfig, mc, initialProcess, capUtils, castorConf, scheduler);

  // 8) Run the data transfer session
  sess.execute();

  // 9) Check the session git the correct VID
  ASSERT_EQ(s_vid, sess.getVid());

  // 10) Check the remote files exist and have the correct size
  for(auto & path: remoteFilePaths) {
    struct stat statBuf;
    bzero(&statBuf, sizeof(statBuf));
    const int statRc = stat(path.substr(7).c_str(), &statBuf); //remove the "file://" for stat-ing
    ASSERT_EQ(0, statRc);
    ASSERT_EQ(1000, statBuf.st_size); //same size of data
  }

  // 10) Check logs
  std::string logToCheck = logger.getLog();
  logToCheck += "";
  ASSERT_NE(std::string::npos, logToCheck.find("firmwareVersion=\"123A\" serialNumber=\"123456\" "
                                               "mountTotalCorrectedReadErrors=\"5\" mountTotalReadBytesProcessed=\"4096\" "
                                               "mountTotalUncorrectedReadErrors=\"1\" mountTotalNonMediumErrorCounts=\"2\""));
  ASSERT_NE(std::string::npos, logToCheck.find("firmwareVersion=\"123A\" serialNumber=\"123456\" lifetimeMediumEfficiencyPrct=\"100\" "
                                               "mountReadEfficiencyPrct=\"100\" mountWriteEfficiencyPrct=\"100\" "
                                               "mountReadTransients=\"10\" "
                                               "mountServoTemps=\"10\" mountServoTransients=\"5\" mountTemps=\"100\" "
                                               "mountTotalReadRetries=\"25\" mountTotalWriteRetries=\"25\" mountWriteTransients=\"10\""));
}

TEST_P(DataTransferSessionTest, DataTransferSessionWrongRecall) {
  // This test is the same as the previous one, with 
  // wrong parameters set for the recall, so that we fail 
  // to recall the first file and cancel the second.

  // 0) Prepare the logger for everyone
  cta::log::StringLogger logger("tapeServerUnitTest",cta::log::DEBUG);
  cta::log::LogContext logContext(logger);
  
  setupDefaultCatalogue();
  // 1) prepare the fake scheduler
  std::string vid = s_vid;
  // cta::MountType::Enum mountType = cta::MountType::RETRIEVE;

  // 3) Prepare the necessary environment (logger, plus system wrapper), 
  castor::tape::System::mockWrapper mockSys;
  mockSys.delegateToFake();
  mockSys.disableGMockCallsCounting();
  mockSys.fake.setupForVirtualDriveSLC6();
  //delete is unnecessary
  //pointer with ownership will be passed to the application,
  //which will do the delete 
  mockSys.fake.m_pathToDrive["/dev/nst0"] = new castor::tape::tapeserver::drive::FakeDrive;

  // 4) Create the scheduler
  auto & catalogue = getCatalogue();
  auto & scheduler = getScheduler();

  // Always use the same requester
  const cta::common::dataStructures::SecurityIdentity requester;

  // List to remember the path of each remote file so that the existance of the
  // files can be tested for at the end of the test
  std::list<std::string> remoteFilePaths;
  
  // 5) Create the environment for the migration to happen (library + tape) 
    const std::string libraryComment = "Library comment";
  catalogue.createLogicalLibrary(s_adminOnAdminHost, s_libraryName,
    libraryComment);
  {
    auto libraries = catalogue.getLogicalLibraries();
    ASSERT_EQ(1, libraries.size());
    ASSERT_EQ(s_libraryName, libraries.front().name);
    ASSERT_EQ(libraryComment, libraries.front().comment);
  }
  const uint64_t capacityInBytes = 12345678;
  const std::string tapeComment = "Tape comment";
  bool notDisabled = false;
  bool notFull = false;
  catalogue.createTape(s_adminOnAdminHost, s_vid, s_libraryName, s_tapePoolName, cta::nullopt, capacityInBytes,
    notDisabled, notFull, tapeComment);
  
  // 6) Prepare files for reading by writing them to the mock system
  {
    // Label the tape
    castor::tape::tapeFile::LabelSession ls(*mockSys.fake.m_pathToDrive["/dev/nst0"], 
        s_vid, false);
    mockSys.fake.m_pathToDrive["/dev/nst0"]->rewind();
    // And write to it
    castor::tape::tapeserver::daemon::VolumeInfo volInfo;
    volInfo.vid=s_vid;
    castor::tape::tapeFile::WriteSession ws(*mockSys.fake.m_pathToDrive["/dev/nst0"],
       volInfo , 0, true, false);

    // Write a few files on the virtual tape and modify the archive name space
    // so that it is in sync
    uint8_t data[1000];
    castor::tape::SCSI::Structures::zeroStruct(&data);
    int fseq=1;
    {
      // Create a path to a remote destination file
      std::ostringstream remoteFilePath;
      remoteFilePath << "file://" << m_tmpDir << "/test" << fseq;
      remoteFilePaths.push_back(remoteFilePath.str());
      
      // Write the file to tape
      const uint64_t archiveFileSize = 1000;
      cta::MockArchiveMount mam(catalogue);
      cta::MockRetrieveMount mrm;
      std::unique_ptr<cta::ArchiveJob> aj(new cta::MockArchiveJob(mam, catalogue));
      aj->tapeFile.fSeq = fseq;
      aj->archiveFile.archiveFileID = 1000 + fseq;
      castor::tape::tapeFile::WriteFile wf(&ws, *aj, archiveFileSize);
      // Write the data (one block)
      wf.write(data, sizeof(data));
      // Close the file
      wf.close();
      
      // Create a fictious file record on the tape to allow adding one to fseq=2 afterwards.
      cta::catalogue::TapeFileWritten tapeFileWritten;
      tapeFileWritten.archiveFileId=666;
      tapeFileWritten.checksumType="ADLER32";
      tapeFileWritten.checksumValue="0xDEADBEEF";
      tapeFileWritten.vid=volInfo.vid;
      tapeFileWritten.size=archiveFileSize;
      tapeFileWritten.fSeq=1;
      tapeFileWritten.blockId=0;
      tapeFileWritten.copyNb=1;
      tapeFileWritten.compressedSize=archiveFileSize; // No compression
      tapeFileWritten.diskInstance = s_diskInstance;
      tapeFileWritten.diskFileId = 1;
      tapeFileWritten.diskFilePath = "/somefile";
      tapeFileWritten.diskFileUser = s_userName;
      tapeFileWritten.diskFileGroup = "someGroup";
      tapeFileWritten.diskFileRecoveryBlob = "B106";
      tapeFileWritten.storageClassName = s_storageClassName;
      tapeFileWritten.tapeDrive = "drive0";
      catalogue.filesWrittenToTape(std::set<cta::catalogue::TapeFileWritten>{tapeFileWritten});
      
      // Create an archive file entry in the archive catalogue
      tapeFileWritten.archiveFileId=1000 + fseq;
      tapeFileWritten.checksumType="ADLER32";
      tapeFileWritten.checksumValue=cta::utils::getAdler32String(data, archiveFileSize);
      tapeFileWritten.vid=volInfo.vid;
      tapeFileWritten.size=archiveFileSize;
      tapeFileWritten.fSeq=fseq + 1;
      tapeFileWritten.blockId=wf.getBlockId() + 10000;
      tapeFileWritten.copyNb=1;
      tapeFileWritten.compressedSize=archiveFileSize; // No compression
      tapeFileWritten.diskInstance = s_diskInstance;
      tapeFileWritten.diskFileId = fseq;
      tapeFileWritten.diskFilePath = remoteFilePath.str();
      tapeFileWritten.diskFileUser = s_userName;
      tapeFileWritten.diskFileGroup = "someGroup";
      tapeFileWritten.diskFileRecoveryBlob = "B106";
      tapeFileWritten.storageClassName = s_storageClassName;
      tapeFileWritten.tapeDrive = "drive0";
      catalogue.filesWrittenToTape(std::set<cta::catalogue::TapeFileWritten>{tapeFileWritten});

      // Schedule the retrieval of the file
      std::string diskInstance="disk_instance";
      cta::common::dataStructures::RetrieveRequest rReq;
      rReq.archiveFileID=1000 + fseq;
      rReq.requester.name = s_userName;
      rReq.requester.group = "someGroup";
      rReq.dstURL = remoteFilePaths.back();
      std::list<std::string> archiveFilePaths;
      scheduler.queueRetrieve(diskInstance, rReq, logContext);
    }
  }
  
  // 6) Report the drive's existence and put it up in the drive register.
  cta::tape::daemon::TpconfigLine driveConfig("T10D6116", "TestLogicalLibrary", "/dev/tape_T10D6116", "manual");
  cta::common::dataStructures::DriveInfo driveInfo;
  driveInfo.driveName=driveConfig.unitName;
  driveInfo.logicalLibrary=driveConfig.rawLibrarySlot;
  // We need to create the drive in the registry before being able to put it up.
  scheduler.reportDriveStatus(driveInfo, cta::common::dataStructures::MountType::NoMount, cta::common::dataStructures::DriveStatus::Down);
  scheduler.setDesiredDriveState(s_adminOnAdminHost, driveConfig.unitName, true, false);

  // 7) Create the data transfer session
  DataTransferConfig castorConf;
  castorConf.bufsz = 1024*1024; // 1 MB memory buffers
  castorConf.nbBufs = 10;
  castorConf.bulkRequestRecallMaxBytes = UINT64_C(100)*1000*1000*1000;
  castorConf.bulkRequestRecallMaxFiles = 1000;
  castorConf.nbDiskThreads = 1;
  cta::log::DummyLogger dummyLog("dummy");
  cta::mediachanger::MediaChangerFacade mc(dummyLog);
  cta::server::ProcessCap capUtils;
  castor::messages::TapeserverProxyDummy initialProcess;
  DataTransferSession sess("tapeHost", logger, mockSys,
    driveConfig, mc, initialProcess, capUtils, castorConf, scheduler);

  // 8) Run the data transfer session
  ASSERT_NO_THROW(sess.execute());

  // 9) Check the session git the correct VID
  ASSERT_EQ(s_vid, sess.getVid());

  // 10) Check the remote files exist and have the correct size
  std::string temp = logger.getLog();
  ASSERT_NE(std::string::npos, logger.getLog().find("trying to position beyond the end of data"));

  // 11) Check logs for drive statistics
  std::string logToCheck = logger.getLog();
  logToCheck += "";
  ASSERT_NE(std::string::npos, logToCheck.find("firmwareVersion=\"123A\" serialNumber=\"123456\" "
                                               "mountTotalCorrectedReadErrors=\"5\" mountTotalReadBytesProcessed=\"4096\" "
                                               "mountTotalUncorrectedReadErrors=\"1\" mountTotalNonMediumErrorCounts=\"2\""));
  ASSERT_NE(std::string::npos, logToCheck.find("firmwareVersion=\"123A\" serialNumber=\"123456\" lifetimeMediumEfficiencyPrct=\"100\" "
                                               "mountReadEfficiencyPrct=\"100\" mountWriteEfficiencyPrct=\"100\" "
                                               "mountReadTransients=\"10\" "
                                               "mountServoTemps=\"10\" mountServoTransients=\"5\" mountTemps=\"100\" "
                                               "mountTotalReadRetries=\"25\" mountTotalWriteRetries=\"25\" mountWriteTransients=\"10\""));
}

TEST_P(DataTransferSessionTest, DataTransferSessionRAORecall) {
  // 0) Prepare the logger for everyone
  cta::log::StringLogger logger("tapeServerUnitTest",cta::log::DEBUG);
  cta::log::LogContext logContext(logger);
  
  setupDefaultCatalogue();
  // 1) prepare the fake scheduler
  std::string vid = s_vid;
  // cta::MountType::Enum mountType = cta::MountType::RETRIEVE;

  // 3) Prepare the necessary environment (logger, plus system wrapper), 
  castor::tape::System::mockWrapper mockSys;
  mockSys.delegateToFake();
  mockSys.disableGMockCallsCounting();
  mockSys.fake.setupForVirtualDriveSLC6();
  //delete is unnecessary
  //pointer with ownership will be passed to the application,
  //which will do the delete 
  mockSys.fake.m_pathToDrive["/dev/nst0"] = new castor::tape::tapeserver::drive::FakeDrive;

  // 4) Create the scheduler
  auto & catalogue = getCatalogue();
  auto & scheduler = getScheduler();
  
  // Always use the same requester
  const cta::common::dataStructures::SecurityIdentity requester;

  // List to remember the path of each remote file so that the existance of the
  // files can be tested for at the end of the test
  std::list<std::string> remoteFilePaths;
  
  // 5) Create the environment for the migration to happen (library + tape) 
    const std::string libraryComment = "Library comment";
  catalogue.createLogicalLibrary(s_adminOnAdminHost, s_libraryName,
    libraryComment);
  {
    auto libraries = catalogue.getLogicalLibraries();
    ASSERT_EQ(1, libraries.size());
    ASSERT_EQ(s_libraryName, libraries.front().name);
    ASSERT_EQ(libraryComment, libraries.front().comment);
  }
  const uint64_t capacityInBytes = 12345678;
  const std::string tapeComment = "Tape comment";
  bool notDisabled = false;
  bool notFull = false;
  catalogue.createTape(s_adminOnAdminHost, s_vid, s_libraryName, s_tapePoolName, cta::nullopt, capacityInBytes,
    notDisabled, notFull, tapeComment);

  int MAX_RECALLS = 50;
  int MAX_BULK_RECALLS = 31;
  std::vector<int> expectedOrder;
  std::vector<std::string> expectedFseqOrderLog;

  // 6) Prepare files for reading by writing them to the mock system
  {
    // Label the tape
    castor::tape::tapeFile::LabelSession ls(*mockSys.fake.m_pathToDrive["/dev/nst0"], 
        s_vid, false);
    mockSys.fake.m_pathToDrive["/dev/nst0"]->rewind();
    // And write to it
    castor::tape::tapeserver::daemon::VolumeInfo volInfo;
    volInfo.vid=s_vid;
    castor::tape::tapeFile::WriteSession ws(*mockSys.fake.m_pathToDrive["/dev/nst0"],
       volInfo , 0, true, false);

    // Write a few files on the virtual tape and modify the archive name space
    // so that it is in sync
    uint8_t data[1000];
    size_t archiveFileSize=sizeof(data);
    castor::tape::SCSI::Structures::zeroStruct(&data);
    int fseq;
    bool isFirst = true;
    for (fseq=1; fseq <= MAX_RECALLS ; fseq ++) {
      // Create a path to a remote destination file
      std::ostringstream remoteFilePath;
      remoteFilePath << "file://" << m_tmpDir << "/test" << fseq;
      remoteFilePaths.push_back(remoteFilePath.str());

      // Create an archive file entry in the archive namespace
      cta::catalogue::TapeFileWritten tapeFileWritten;
      
      // Write the file to tape
      cta::MockArchiveMount mam(catalogue);
      std::unique_ptr<cta::ArchiveJob> aj(new cta::MockArchiveJob(mam, catalogue));
      aj->tapeFile.fSeq = fseq;
      aj->archiveFile.archiveFileID = fseq;
      castor::tape::tapeFile::WriteFile wf(&ws, *aj, archiveFileSize);
      tapeFileWritten.blockId = wf.getBlockId();
      // Write the data (one block)
      wf.write(data, archiveFileSize);
      // Close the file
      wf.close();

      // Create file entry in the archive namespace
      tapeFileWritten.archiveFileId=fseq;
      tapeFileWritten.checksumType="ADLER32";
      tapeFileWritten.checksumValue=cta::utils::getAdler32String(data, archiveFileSize);
      tapeFileWritten.vid=volInfo.vid;
      tapeFileWritten.size=archiveFileSize;
      tapeFileWritten.fSeq=fseq;
      tapeFileWritten.copyNb=1;
      tapeFileWritten.compressedSize=archiveFileSize; // No compression
      tapeFileWritten.diskInstance = s_diskInstance;
      tapeFileWritten.diskFileId = fseq;
      tapeFileWritten.diskFilePath = remoteFilePath.str();
      tapeFileWritten.diskFileUser = s_userName;
      tapeFileWritten.diskFileGroup = "someGroup";
      tapeFileWritten.diskFileRecoveryBlob = "B106";
      tapeFileWritten.storageClassName = s_storageClassName;
      tapeFileWritten.tapeDrive = "drive0";
      catalogue.filesWrittenToTape(std::set<cta::catalogue::TapeFileWritten>{tapeFileWritten});

      // Schedule the retrieval of the file
      std::string diskInstance="disk_instance";
      cta::common::dataStructures::RetrieveRequest rReq;
      rReq.archiveFileID=fseq;
      rReq.requester.name = s_userName;
      rReq.requester.group = "someGroup";
      rReq.dstURL = remoteFilePaths.back();
      std::list<std::string> archiveFilePaths;
      scheduler.queueRetrieve(diskInstance, rReq, logContext);

      expectedOrder.push_back(fseq);

      bool apply_rao = false;
      bool add_expected = false;
      if (MAX_BULK_RECALLS < 2) {
        if (expectedOrder.size() % MAX_BULK_RECALLS == 0 ||
            fseq % MAX_RECALLS == 0) {
          add_expected = true;
        }
      }
      else if (MAX_BULK_RECALLS >= 30) {
        if ((expectedOrder.size() % 30 == 0) ||
            (fseq % MAX_RECALLS == 0) || (fseq % MAX_BULK_RECALLS == 0)) {
          apply_rao = true & isFirst;
          add_expected = true;
        }
      }
      else if ((fseq % MAX_BULK_RECALLS == 0) || (fseq % MAX_RECALLS == 0)) {
        apply_rao = true & isFirst;
        add_expected = true;
      }
      if (apply_rao) {
        std::reverse(expectedOrder.begin(), expectedOrder.end());
        isFirst = false;
      }
      if (add_expected) {
        std::stringstream expectedLogLine;
        std::copy(expectedOrder.begin(), expectedOrder.end(),
                std::ostream_iterator<int>(expectedLogLine, " "));
        expectedFseqOrderLog.push_back(expectedLogLine.str());
        expectedOrder.clear();
      }
    }
  }
    
  // 6) Report the drive's existence and put it up in the drive register.
  cta::tape::daemon::TpconfigLine driveConfig("T10D6116", "TestLogicalLibrary", "/dev/tape_T10D6116", "manual");
  cta::common::dataStructures::DriveInfo driveInfo;
  driveInfo.driveName=driveConfig.unitName;
  driveInfo.logicalLibrary=driveConfig.rawLibrarySlot;
  // We need to create the drive in the registry before being able to put it up.
  scheduler.reportDriveStatus(driveInfo, cta::common::dataStructures::MountType::NoMount, cta::common::dataStructures::DriveStatus::Down);
  scheduler.setDesiredDriveState(s_adminOnAdminHost, driveConfig.unitName, true, false);

  // 7) Create the data transfer session
  DataTransferConfig castorConf;
  castorConf.bufsz = 1024*1024; // 1 MB memory buffers
  castorConf.nbBufs = 10;
  castorConf.bulkRequestRecallMaxBytes = UINT64_C(100)*1000*1000*1000;
  castorConf.bulkRequestRecallMaxFiles = MAX_BULK_RECALLS - 1;
  castorConf.nbDiskThreads = 1;
  castorConf.useRAO = true;
  cta::log::DummyLogger dummyLog("dummy");
  cta::mediachanger::MediaChangerFacade mc(dummyLog);
  cta::server::ProcessCap capUtils;
  castor::messages::TapeserverProxyDummy initialProcess;
  castor::tape::tapeserver::daemon::DataTransferSession sess("tapeHost", logger, mockSys,
    driveConfig, mc, initialProcess, capUtils, castorConf, scheduler);

  // 8) Run the data transfer session
  sess.execute();

  // 9) Check the session git the correct VID
  ASSERT_EQ(s_vid, sess.getVid());

  // 10) Check the remote files exist and have the correct size
  for(auto & path: remoteFilePaths) {
    struct stat statBuf;
    bzero(&statBuf, sizeof(statBuf));
    const int statRc = stat(path.substr(7).c_str(), &statBuf); //remove the "file://" for stat-ing
    ASSERT_EQ(0, statRc);
    ASSERT_EQ(1000, statBuf.st_size); //same size of data
  }

  // 10) Check logs
  std::string logToCheck = logger.getLog();
  logToCheck += "";
  ASSERT_NE(std::string::npos, logToCheck.find("firmwareVersion=\"123A\" serialNumber=\"123456\" "
                                               "mountTotalCorrectedReadErrors=\"5\" mountTotalReadBytesProcessed=\"4096\" "
                                               "mountTotalUncorrectedReadErrors=\"1\" mountTotalNonMediumErrorCounts=\"2\""));
  ASSERT_NE(std::string::npos, logToCheck.find("firmwareVersion=\"123A\" serialNumber=\"123456\" lifetimeMediumEfficiencyPrct=\"100\" "
                                               "mountReadEfficiencyPrct=\"100\" mountWriteEfficiencyPrct=\"100\" "
                                               "mountReadTransients=\"10\" "
                                               "mountServoTemps=\"10\" mountServoTransients=\"5\" mountTemps=\"100\" "
                                               "mountTotalReadRetries=\"25\" mountTotalWriteRetries=\"25\" mountWriteTransients=\"10\""));
  for (std::string s : expectedFseqOrderLog) {
    ASSERT_NE(std::string::npos, logToCheck.find(s));
  }
}

TEST_P(DataTransferSessionTest, DataTransferSessionNoSuchDrive) {
  
  // 0) Prepare the logger for everyone
  cta::log::StringLogger logger("tapeServerUnitTest",cta::log::DEBUG);
  cta::log::LogContext logContext(logger);
  
  setupDefaultCatalogue();
  // 1) prepare the fake scheduler
  std::string vid = s_vid;
  // cta::MountType::Enum mountType = cta::MountType::RETRIEVE;

  // 3) Prepare the necessary environment (logger, plus system wrapper), 
  castor::tape::System::mockWrapper mockSys;
  mockSys.delegateToFake();
  mockSys.disableGMockCallsCounting();
  mockSys.fake.setupForVirtualDriveSLC6();
  //delete is unnecessary
  //pointer with ownership will be passed to the application,
  //which will do the delete 
  mockSys.fake.m_pathToDrive["/dev/nst0"] = new castor::tape::tapeserver::drive::FakeDrive;

  // 4) Create the scheduler
  auto & catalogue = getCatalogue();
  auto & scheduler = getScheduler();
  
  // Always use the same requester
  const cta::common::dataStructures::SecurityIdentity requester;
  
  // List to remember the path of each remote file so that the existance of the
  // files can be tested for at the end of the test
  std::list<std::string> remoteFilePaths;
  
  // 5) Create the environment for the migration to happen (library + tape) 
    const std::string libraryComment = "Library comment";
  catalogue.createLogicalLibrary(s_adminOnAdminHost, s_libraryName,
    libraryComment);
  {
    auto libraries = catalogue.getLogicalLibraries();
    ASSERT_EQ(1, libraries.size());
    ASSERT_EQ(s_libraryName, libraries.front().name);
    ASSERT_EQ(libraryComment, libraries.front().comment);
  }
  const uint64_t capacityInBytes = 12345678;
  const std::string tapeComment = "Tape comment";
  bool notDisabled = false;
  bool notFull = false;
  catalogue.createTape(s_adminOnAdminHost, s_vid, s_libraryName, s_tapePoolName, cta::nullopt, capacityInBytes,
    notDisabled, notFull, tapeComment);
  
  // 6) Prepare files for reading by writing them to the mock system
  {
    // Label the tape
    castor::tape::tapeFile::LabelSession ls(*mockSys.fake.m_pathToDrive["/dev/nst0"], 
        s_vid, false);
    mockSys.fake.m_pathToDrive["/dev/nst0"]->rewind();
    // And write to it
    castor::tape::tapeserver::daemon::VolumeInfo volInfo;
    volInfo.vid=s_vid;
    castor::tape::tapeFile::WriteSession ws(*mockSys.fake.m_pathToDrive["/dev/nst0"],
       volInfo , 0, true, false);

    // Write a few files on the virtual tape and modify the archive name space
    // so that it is in sync
    uint8_t data[1000];
    size_t archiveFileSize=sizeof(data);
    castor::tape::SCSI::Structures::zeroStruct(&data);
    for (int fseq=1; fseq <= 10 ; fseq ++) {
      // Create a path to a remote destination file
      std::ostringstream remoteFilePath;
      remoteFilePath << "file://" << m_tmpDir << "/test" << fseq;
      remoteFilePaths.push_back(remoteFilePath.str());

      // Create an archive file entry in the archive namespace
      cta::catalogue::TapeFileWritten tapeFileWritten;
      
      // Write the file to tape
      cta::MockArchiveMount mam(catalogue);
      std::unique_ptr<cta::ArchiveJob> aj(new cta::MockArchiveJob(mam, catalogue));
      aj->tapeFile.fSeq = fseq;
      aj->archiveFile.archiveFileID = fseq;
      castor::tape::tapeFile::WriteFile wf(&ws, *aj, archiveFileSize);
      tapeFileWritten.blockId = wf.getBlockId();
      // Write the data (one block)
      wf.write(data, archiveFileSize);
      // Close the file
      wf.close();

      // Create file entry in the archive namespace
      tapeFileWritten.archiveFileId=fseq;
      tapeFileWritten.checksumType="ADLER32";
      tapeFileWritten.checksumValue=cta::utils::getAdler32String(data, archiveFileSize);
      tapeFileWritten.vid=volInfo.vid;
      tapeFileWritten.size=archiveFileSize;
      tapeFileWritten.fSeq=fseq;
      tapeFileWritten.copyNb=1;
      tapeFileWritten.compressedSize=archiveFileSize; // No compression
      tapeFileWritten.diskInstance = s_diskInstance;
      tapeFileWritten.diskFileId = fseq;
      tapeFileWritten.diskFilePath = remoteFilePath.str();
      tapeFileWritten.diskFileUser = s_userName;
      tapeFileWritten.diskFileGroup = "someGroup";
      tapeFileWritten.diskFileRecoveryBlob = "B106";
      tapeFileWritten.storageClassName = s_storageClassName;
      tapeFileWritten.tapeDrive = "drive0";
      catalogue.filesWrittenToTape(std::set<cta::catalogue::TapeFileWritten>{tapeFileWritten});

      // Schedule the retrieval of the file
      std::string diskInstance="disk_instance";
      cta::common::dataStructures::RetrieveRequest rReq;
      rReq.archiveFileID=fseq;
      rReq.requester.name = s_userName;
      rReq.requester.group = "someGroup";
      rReq.dstURL = remoteFilePaths.back();
      std::list<std::string> archiveFilePaths;
      scheduler.queueRetrieve(diskInstance, rReq, logContext);
    }
  }
    
  // 7) Report the drive's existence and put it up in the drive register.
  cta::tape::daemon::TpconfigLine driveConfig("T10D6116", "TestLogicalLibrary", "/dev/noSuchDrive", "manual");
  cta::common::dataStructures::DriveInfo driveInfo;
  driveInfo.driveName=driveConfig.unitName;
  driveInfo.logicalLibrary=driveConfig.rawLibrarySlot;
  // We need to create the drive in the registry before being able to put it up.
  scheduler.reportDriveStatus(driveInfo, cta::common::dataStructures::MountType::NoMount, cta::common::dataStructures::DriveStatus::Down);
  scheduler.setDesiredDriveState(s_adminOnAdminHost, driveConfig.unitName, true, false);

  // 8) Create the data transfer session
  DataTransferConfig castorConf;
  castorConf.bufsz = 1024;
  castorConf.nbBufs = 10;
  cta::log::DummyLogger dummyLog("dummy");
  cta::mediachanger::MediaChangerFacade mc(dummyLog);
  castor::messages::TapeserverProxyDummy initialProcess;
  cta::server::ProcessCapDummy capUtils;
  DataTransferSession sess("tapeHost", logger, mockSys,
    driveConfig, mc, initialProcess, capUtils, castorConf, scheduler);
  ASSERT_NO_THROW(sess.execute());
  std::string temp = logger.getLog();
  ASSERT_NE(std::string::npos, logger.getLog().find("Could not stat path: /dev/noSuchDrive"));
}

TEST_P(DataTransferSessionTest, DataTransferSessionFailtoMount) {
  
  // 0) Prepare the logger for everyone
  cta::log::StringLogger logger("tapeServerUnitTest",cta::log::DEBUG);
  cta::log::LogContext logContext(logger);
  
  setupDefaultCatalogue();
  // 1) prepare the fake scheduler
  std::string vid = s_vid;
  // cta::MountType::Enum mountType = cta::MountType::RETRIEVE;

  // 3) Prepare the necessary environment (logger, plus system wrapper), 
  castor::tape::System::mockWrapper mockSys;
  mockSys.delegateToFake();
  mockSys.disableGMockCallsCounting();
  mockSys.fake.setupForVirtualDriveSLC6();
  //delete is unnecessary
  //pointer with ownership will be passed to the application,
  //which will do the delete 
  const bool failOnMount=true;
  mockSys.fake.m_pathToDrive["/dev/nst0"] = new castor::tape::tapeserver::drive::FakeDrive(failOnMount);
  
  // 4) Create the scheduler
  auto & catalogue = getCatalogue();
  auto & scheduler = getScheduler();
  
  // Always use the same requester
  const cta::common::dataStructures::SecurityIdentity requester;
  
  // List to remember the path of each remote file so that the existance of the
  // files can be tested for at the end of the test
  std::list<std::string> remoteFilePaths;
  
  // 5) Create the environment for the migration to happen (library + tape) 
    const std::string libraryComment = "Library comment";
  catalogue.createLogicalLibrary(s_adminOnAdminHost, s_libraryName,
    libraryComment);
  {
    auto libraries = catalogue.getLogicalLibraries();
    ASSERT_EQ(1, libraries.size());
    ASSERT_EQ(s_libraryName, libraries.front().name);
    ASSERT_EQ(libraryComment, libraries.front().comment);
  }
  const uint64_t capacityInBytes = 12345678;
  const std::string tapeComment = "Tape comment";
  bool notDisabled = false;
  bool notFull = false;
  catalogue.createTape(s_adminOnAdminHost, s_vid, s_libraryName, s_tapePoolName, cta::nullopt, capacityInBytes,
    notDisabled, notFull, tapeComment);
  
  // 6) Prepare files for reading by writing them to the mock system
  {
    // Label the tape
    castor::tape::tapeFile::LabelSession ls(*mockSys.fake.m_pathToDrive["/dev/nst0"], 
        s_vid, false);
    mockSys.fake.m_pathToDrive["/dev/nst0"]->rewind();
    // And write to it
    castor::tape::tapeserver::daemon::VolumeInfo volInfo;
    volInfo.vid=s_vid;
    castor::tape::tapeFile::WriteSession ws(*mockSys.fake.m_pathToDrive["/dev/nst0"],
       volInfo , 0, true, false);

    // Write a few files on the virtual tape and modify the archive name space
    // so that it is in sync
    uint8_t data[1000];
    size_t archiveFileSize=sizeof(data);
    castor::tape::SCSI::Structures::zeroStruct(&data);
    for (int fseq=1; fseq <= 10 ; fseq ++) {
      // Create a path to a remote destination file
      std::ostringstream remoteFilePath;
      remoteFilePath << "file://" << m_tmpDir << "/test" << fseq;
      remoteFilePaths.push_back(remoteFilePath.str());

      // Create an archive file entry in the archive namespace
      cta::catalogue::TapeFileWritten tapeFileWritten;
      
      // Write the file to tape
      cta::MockArchiveMount mam(catalogue);
      std::unique_ptr<cta::ArchiveJob> aj(new cta::MockArchiveJob(mam, catalogue));
      aj->tapeFile.fSeq = fseq;
      aj->archiveFile.archiveFileID = fseq;
      castor::tape::tapeFile::WriteFile wf(&ws, *aj, archiveFileSize);
      tapeFileWritten.blockId = wf.getBlockId();
      // Write the data (one block)
      wf.write(data, archiveFileSize);
      // Close the file
      wf.close();

      // Create file entry in the archive namespace
      tapeFileWritten.archiveFileId=fseq;
      tapeFileWritten.checksumType="ADLER32";
      tapeFileWritten.checksumValue=cta::utils::getAdler32String(data, archiveFileSize);
      tapeFileWritten.vid=volInfo.vid;
      tapeFileWritten.size=archiveFileSize;
      tapeFileWritten.fSeq=fseq;
      tapeFileWritten.copyNb=1;
      tapeFileWritten.compressedSize=archiveFileSize; // No compression
      tapeFileWritten.diskInstance = s_diskInstance;
      tapeFileWritten.diskFileId = fseq;
      tapeFileWritten.diskFilePath = remoteFilePath.str();
      tapeFileWritten.diskFileUser = s_userName;
      tapeFileWritten.diskFileGroup = "someGroup";
      tapeFileWritten.diskFileRecoveryBlob = "B106";
      tapeFileWritten.storageClassName = s_storageClassName;
      tapeFileWritten.tapeDrive = "drive0";
      catalogue.filesWrittenToTape(std::set<cta::catalogue::TapeFileWritten>{tapeFileWritten});

      // Schedule the retrieval of the file
      std::string diskInstance="disk_instance";
      cta::common::dataStructures::RetrieveRequest rReq;
      rReq.archiveFileID=fseq;
      rReq.requester.name = s_userName;
      rReq.requester.group = "someGroup";
      rReq.dstURL = remoteFilePaths.back();
      std::list<std::string> archiveFilePaths;
      scheduler.queueRetrieve(diskInstance, rReq, logContext);
    }
  }

  // 7) Report the drive's existence and put it up in the drive register.
  cta::tape::daemon::TpconfigLine driveConfig("T10D6116", "TestLogicalLibrary", "/dev/tape_T10D6116", "manual");
  cta::common::dataStructures::DriveInfo driveInfo;
  driveInfo.driveName=driveConfig.unitName;
  driveInfo.logicalLibrary=driveConfig.rawLibrarySlot;
  // We need to create the drive in the registry before being able to put it up.
  scheduler.reportDriveStatus(driveInfo, cta::common::dataStructures::MountType::NoMount, cta::common::dataStructures::DriveStatus::Down);
  scheduler.setDesiredDriveState(s_adminOnAdminHost, driveConfig.unitName, true, false);

  // 8) Create the data transfer session
  DataTransferConfig castorConf;
  castorConf.bufsz = 1024*1024; // 1 MB memory buffers
  castorConf.nbBufs = 10;
  castorConf.bulkRequestRecallMaxBytes = UINT64_C(100)*1000*1000*1000;
  castorConf.bulkRequestRecallMaxFiles = 1000;
  castorConf.nbDiskThreads = 3;
  cta::log::DummyLogger dummyLog("dummy");
  cta::mediachanger::MediaChangerFacade mc(dummyLog);
  cta::server::ProcessCap capUtils;
  castor::messages::TapeserverProxyDummy initialProcess;
  DataTransferSession sess("tapeHost", logger, mockSys,
    driveConfig, mc, initialProcess, capUtils, castorConf, scheduler);
  ASSERT_NO_THROW(sess.execute());
  std::string temp = logger.getLog();
  ASSERT_NE(std::string::npos, logger.getLog().find("Failed to mount the tape"));

  // 10) Check logs for drive statistics
  std::string logToCheck = logger.getLog();
  logToCheck += "";
  ASSERT_NE(std::string::npos, logToCheck.find("firmwareVersion=\"123A\" serialNumber=\"123456\" "
                                               "mountTotalCorrectedReadErrors=\"5\" mountTotalReadBytesProcessed=\"4096\" "
                                               "mountTotalUncorrectedReadErrors=\"1\" mountTotalNonMediumErrorCounts=\"2\""));
  ASSERT_NE(std::string::npos, logToCheck.find("firmwareVersion=\"123A\" serialNumber=\"123456\" lifetimeMediumEfficiencyPrct=\"100\" "
                                               "mountReadEfficiencyPrct=\"100\" mountWriteEfficiencyPrct=\"100\" "
                                               "mountReadTransients=\"10\" "
                                               "mountServoTemps=\"10\" mountServoTransients=\"5\" mountTemps=\"100\" "
                                               "mountTotalReadRetries=\"25\" mountTotalWriteRetries=\"25\" mountWriteTransients=\"10\""));
}

TEST_P(DataTransferSessionTest, DataTransferSessionGooddayMigration) {
   
  // 0) Prepare the logger for everyone
  cta::log::StringLogger logger("tapeServerUnitTest",cta::log::DEBUG);
  cta::log::LogContext logContext(logger);
  
  setupDefaultCatalogue();
  // 1) prepare the fake scheduler
  std::string vid = s_vid;
  // cta::MountType::Enum mountType = cta::MountType::RETRIEVE;

  // 3) Prepare the necessary environment (logger, plus system wrapper), 
  castor::tape::System::mockWrapper mockSys;
  mockSys.delegateToFake();
  mockSys.disableGMockCallsCounting();
  mockSys.fake.setupForVirtualDriveSLC6();
  
  // 4) Create the scheduler
  auto & catalogue = getCatalogue();
  auto & scheduler = getScheduler();
  
  // Always use the same requester
  const cta::common::dataStructures::SecurityIdentity requester("user", "group");
  
  // List to remember the path of each remote file so that the existance of the
  // files can be tested for at the end of the test
  std::list<std::string> remoteFilePaths;
  
  // 5) Create the environment for the migration to happen (library + tape) 
    const std::string libraryComment = "Library comment";
  catalogue.createLogicalLibrary(s_adminOnAdminHost, s_libraryName,
    libraryComment);
  {
    auto libraries = catalogue.getLogicalLibraries();
    ASSERT_EQ(1, libraries.size());
    ASSERT_EQ(s_libraryName, libraries.front().name);
    ASSERT_EQ(libraryComment, libraries.front().comment);
  }
  const uint64_t capacityInBytes = 12345678;
  const std::string tapeComment = "Tape comment";
  bool notDisabled = false;
  bool notFull = false;
  catalogue.createTape(s_adminOnAdminHost, s_vid, s_libraryName, s_tapePoolName, cta::nullopt, capacityInBytes,
    notDisabled, notFull, tapeComment);
  
  // Create the mount criteria
  catalogue.createMountPolicy(requester, "immediateMount", 1000, 0, 1000, 0, 1, "Policy comment");
  catalogue.createRequesterMountRule(requester, "immediateMount", s_diskInstance, requester.username, "Rule comment");

  //delete is unnecessary
  //pointer with ownership will be passed to the application,
  //which will do the delete
  mockSys.fake.m_pathToDrive["/dev/nst0"] = new castor::tape::tapeserver::drive::FakeDrive();
  
  // We can prepare files for writing on the drive.
  // Tempfiles are in this scope so they are kept alive 
  std::list<std::unique_ptr<unitTests::TempFile>> sourceFiles;
  std::list<uint64_t> archiveFileIds;
  {    
    // Label the tape
    castor::tape::tapeFile::LabelSession ls(*mockSys.fake.m_pathToDrive["/dev/nst0"], s_vid, false);
    catalogue.tapeLabelled(s_vid, "T10D6116", true);
    mockSys.fake.m_pathToDrive["/dev/nst0"]->rewind();
    
    // Create the files and schedule the archivals
    for(int fseq=1; fseq <= 10 ; fseq ++) { 
      // Create a source file.
      sourceFiles.emplace_back(cta::make_unique<unitTests::TempFile>());
      sourceFiles.back()->randomFill(1000);
      remoteFilePaths.push_back(sourceFiles.back()->path());
      // Schedule the archival of the file
      cta::common::dataStructures::ArchiveRequest ar;
      ar.checksumType="ADLER32";
      ar.checksumValue=sourceFiles.back()->adler32();
      ar.storageClass=s_storageClassName;
      ar.srcURL=std::string("file://") + sourceFiles.back()->path();
      ar.requester.name = requester.username;
      ar.requester.group = "group";
      ar.fileSize = 1000;
      ar.diskFileID = "x";
      ar.diskFileInfo.path = "y";
      ar.diskFileInfo.owner = "z";
      ar.diskFileInfo.group = "g";
      ar.diskFileInfo.recoveryBlob = "b";
      archiveFileIds.push_back(scheduler.queueArchive(s_diskInstance,ar,logContext));
    }
  }
  // Report the drive's existence and put it up in the drive register.
  cta::tape::daemon::TpconfigLine driveConfig("T10D6116", "TestLogicalLibrary", "/dev/tape_T10D6116", "manual");
  cta::common::dataStructures::DriveInfo driveInfo;
  driveInfo.driveName=driveConfig.unitName;
  driveInfo.logicalLibrary=driveConfig.rawLibrarySlot;
  // We need to create the drive in the registry before being able to put it up.
  scheduler.reportDriveStatus(driveInfo, cta::common::dataStructures::MountType::NoMount, cta::common::dataStructures::DriveStatus::Down);
  scheduler.setDesiredDriveState(s_adminOnAdminHost, driveConfig.unitName, true, false);

  // Create the data transfer session
  DataTransferConfig castorConf;
  castorConf.bufsz = 1024*1024; // 1 MB memory buffers
  castorConf.nbBufs = 10;
  castorConf.bulkRequestRecallMaxBytes = UINT64_C(100)*1000*1000*1000;
  castorConf.bulkRequestRecallMaxFiles = 1000;
  castorConf.nbDiskThreads = 1;
  cta::log::DummyLogger dummyLog("dummy");
  cta::mediachanger::MediaChangerFacade mc(dummyLog);
  cta::server::ProcessCap capUtils;
  castor::messages::TapeserverProxyDummy initialProcess;
  DataTransferSession sess("tapeHost", logger, mockSys, driveConfig, mc, initialProcess, capUtils, castorConf, scheduler);
  sess.execute();
  std::string logToCheck = logger.getLog();
  logToCheck += "";
  ASSERT_EQ(s_vid, sess.getVid());
  auto afiiter = archiveFileIds.begin();
  for(auto & sf: sourceFiles) {
    auto afi = *(afiiter++);
    auto afs = catalogue.getArchiveFileById(afi);
    ASSERT_EQ(1, afs.tapeFiles.size());
    ASSERT_EQ(sf->adler32(), afs.checksumValue);
    ASSERT_EQ(1000, afs.fileSize);
  }

  // Check logs for drive statistics
  ASSERT_NE(std::string::npos, logToCheck.find("firmwareVersion=\"123A\" serialNumber=\"123456\" "
                                         "mountTotalCorrectedWriteErrors=\"5\" mountTotalUncorrectedWriteErrors=\"1\" " 
                                         "mountTotalWriteBytesProcessed=\"4096\" mountTotalNonMediumErrorCounts=\"2\""));

  ASSERT_NE(std::string::npos, logToCheck.find("firmwareVersion=\"123A\" serialNumber=\"123456\" lifetimeMediumEfficiencyPrct=\"100\" "
                                         "mountReadEfficiencyPrct=\"100\" mountWriteEfficiencyPrct=\"100\" "
                                         "mountReadTransients=\"10\" "
                                         "mountServoTemps=\"10\" mountServoTransients=\"5\" mountTemps=\"100\" "
                                         "mountTotalReadRetries=\"25\" mountTotalWriteRetries=\"25\" mountWriteTransients=\"10\""));
}

//
// This test is the same as the previous one, except that the files are deleted
// from filesystem immediately. The disk tasks will then fail on open.
///
TEST_P(DataTransferSessionTest, DataTransferSessionMissingFilesMigration) {
  
  // 0) Prepare the logger for everyone
  cta::log::StringLogger logger("tapeServerUnitTest",cta::log::DEBUG);
  cta::log::LogContext logContext(logger);
  
  setupDefaultCatalogue();
  // 1) prepare the fake scheduler
  std::string vid = s_vid;
  // cta::MountType::Enum mountType = cta::MountType::RETRIEVE;

  // 3) Prepare the necessary environment (logger, plus system wrapper), 
  castor::tape::System::mockWrapper mockSys;
  mockSys.delegateToFake();
  mockSys.disableGMockCallsCounting();
  mockSys.fake.setupForVirtualDriveSLC6();
  
  // 4) Create the scheduler
  auto & catalogue = getCatalogue();
  auto & scheduler = getScheduler();
  
  // Always use the same requester
  const cta::common::dataStructures::SecurityIdentity requester("user", "group");
  
  // List to remember the path of each remote file so that the existance of the
  // files can be tested for at the end of the test
  std::list<std::string> remoteFilePaths;
  
  // 5) Create the environment for the migration to happen (library + tape) 
    const std::string libraryComment = "Library comment";
  catalogue.createLogicalLibrary(s_adminOnAdminHost, s_libraryName,
    libraryComment);
  {
    auto libraries = catalogue.getLogicalLibraries();
    ASSERT_EQ(1, libraries.size());
    ASSERT_EQ(s_libraryName, libraries.front().name);
    ASSERT_EQ(libraryComment, libraries.front().comment);
  }
  const uint64_t capacityInBytes = 12345678;
  const std::string tapeComment = "Tape comment";
  bool notDisabled = false;
  bool notFull = false;
  catalogue.createTape(s_adminOnAdminHost, s_vid, s_libraryName, s_tapePoolName, cta::nullopt, capacityInBytes,
    notDisabled, notFull, tapeComment);
  
  // Create the mount criteria
  catalogue.createMountPolicy(requester, "immediateMount", 1000, 0, 1000, 0, 1, "Policy comment");
  catalogue.createRequesterMountRule(requester, "immediateMount", s_diskInstance, requester.username, "Rule comment");

  //delete is unnecessary
  //pointer with ownership will be passed to the application,
  //which will do the delete
  mockSys.fake.m_pathToDrive["/dev/nst0"] = new castor::tape::tapeserver::drive::FakeDrive();
  
  // We can prepare files for writing on the drive.
  // Tempfiles are in this scope so they are kept alive 
  std::list<std::unique_ptr<unitTests::TempFile>> sourceFiles;
  std::list<uint64_t> archiveFileIds;
  {    
    // Label the tape
    castor::tape::tapeFile::LabelSession ls(*mockSys.fake.m_pathToDrive["/dev/nst0"], s_vid, false);
    catalogue.tapeLabelled(s_vid, "T10D6116", true);
    mockSys.fake.m_pathToDrive["/dev/nst0"]->rewind();
    
    // Create the files and schedule the archivals
    for(int fseq=1; fseq <= 10 ; fseq ++) { 
      // Create a source file.
      sourceFiles.emplace_back(cta::make_unique<unitTests::TempFile>());
      sourceFiles.back()->randomFill(1000);
      remoteFilePaths.push_back(sourceFiles.back()->path());
      // Schedule the archival of the file
      cta::common::dataStructures::ArchiveRequest ar;
      ar.checksumType="ADLER32";
      ar.checksumValue=sourceFiles.back()->adler32();
      ar.storageClass=s_storageClassName;
      ar.srcURL=std::string("file://") + sourceFiles.back()->path();
      ar.requester.name = requester.username;
      ar.requester.group = "group";
      ar.fileSize = 1000;
      ar.diskFileID = "x";
      ar.diskFileInfo.path = "y";
      ar.diskFileInfo.owner = "z";
      ar.diskFileInfo.group = "g";
      ar.diskFileInfo.recoveryBlob = "b";
      archiveFileIds.push_back(scheduler.queueArchive(s_diskInstance,ar,logContext));
      // Delete the file: the migration will fail.
      sourceFiles.clear();
    }
  }
  // Report the drive's existence and put it up in the drive register.
  cta::tape::daemon::TpconfigLine driveConfig("T10D6116", "TestLogicalLibrary", "/dev/tape_T10D6116", "manual");
  cta::common::dataStructures::DriveInfo driveInfo;
  driveInfo.driveName=driveConfig.unitName;
  driveInfo.logicalLibrary=driveConfig.rawLibrarySlot;
  // We need to create the drive in the registry before being able to put it up.
  scheduler.reportDriveStatus(driveInfo, cta::common::dataStructures::MountType::NoMount, cta::common::dataStructures::DriveStatus::Down);
  scheduler.setDesiredDriveState(s_adminOnAdminHost, driveConfig.unitName, true, false);

  // Create the data transfer session
  DataTransferConfig castorConf;
  castorConf.bufsz = 1024*1024; // 1 MB memory buffers
  castorConf.nbBufs = 10;
  castorConf.bulkRequestRecallMaxBytes = UINT64_C(100)*1000*1000*1000;
  castorConf.bulkRequestRecallMaxFiles = 1000;
  castorConf.nbDiskThreads = 1;
  cta::log::DummyLogger dummyLog("dummy");
  cta::mediachanger::MediaChangerFacade mc(dummyLog);
  cta::server::ProcessCap capUtils;
  castor::messages::TapeserverProxyDummy initialProcess;
  DataTransferSession sess("tapeHost", logger, mockSys, driveConfig, mc, initialProcess, capUtils, castorConf, scheduler);
  sess.execute();
  std::string temp = logger.getLog();
  temp += "";
  ASSERT_EQ(s_vid, sess.getVid());
  // We should no have logged a single successful file read.
  ASSERT_EQ(std::string::npos, logger.getLog().find("MSG=\"File successfully read from disk\""));
  // Check logs for drive statistics
  std::string logToCheck = logger.getLog();
  logToCheck += "";
  ASSERT_NE(std::string::npos, logToCheck.find("firmwareVersion=\"123A\" serialNumber=\"123456\" "
                                               "mountTotalCorrectedWriteErrors=\"5\" mountTotalUncorrectedWriteErrors=\"1\" " 
                                               "mountTotalWriteBytesProcessed=\"4096\" mountTotalNonMediumErrorCounts=\"2\""));

  ASSERT_NE(std::string::npos, logToCheck.find("firmwareVersion=\"123A\" serialNumber=\"123456\" lifetimeMediumEfficiencyPrct=\"100\" "
                                               "mountReadEfficiencyPrct=\"100\" mountWriteEfficiencyPrct=\"100\" "
                                               "mountReadTransients=\"10\" "
                                               "mountServoTemps=\"10\" mountServoTransients=\"5\" mountTemps=\"100\" "
                                               "mountTotalReadRetries=\"25\" mountTotalWriteRetries=\"25\" mountWriteTransients=\"10\""));
}

//
// This test is identical to the good day migration, but the tape will accept
// only a finite number of bytes and hence we will report a full tape skip the
// last migrations
//
TEST_P(DataTransferSessionTest, DataTransferSessionTapeFullMigration) {
  // 0) Prepare the logger for everyone
  cta::log::StringLogger logger("tapeServerUnitTest",cta::log::DEBUG);
  cta::log::LogContext logContext(logger);
  
  setupDefaultCatalogue();
  // 1) prepare the fake scheduler
  std::string vid = s_vid;
  // cta::MountType::Enum mountType = cta::MountType::RETRIEVE;

  // 3) Prepare the necessary environment (logger, plus system wrapper), 
  castor::tape::System::mockWrapper mockSys;
  mockSys.delegateToFake();
  mockSys.disableGMockCallsCounting();
  mockSys.fake.setupForVirtualDriveSLC6();
  
  // 4) Create the scheduler
  auto & catalogue = getCatalogue();
  auto & scheduler = getScheduler();
  
  // Always use the same requester
  const cta::common::dataStructures::SecurityIdentity requester("user", "group");
  
  // List to remember the path of each remote file so that the existance of the
  // files can be tested for at the end of the test
  std::list<std::string> remoteFilePaths;
  
  // 5) Create the environment for the migration to happen (library + tape) 
    const std::string libraryComment = "Library comment";
  catalogue.createLogicalLibrary(s_adminOnAdminHost, s_libraryName,
    libraryComment);
  {
    auto libraries = catalogue.getLogicalLibraries();
    ASSERT_EQ(1, libraries.size());
    ASSERT_EQ(s_libraryName, libraries.front().name);
    ASSERT_EQ(libraryComment, libraries.front().comment);
  }
  const uint64_t capacityInBytes = 12345678;
  const std::string tapeComment = "Tape comment";
  bool notDisabled = false;
  bool notFull = false;
  catalogue.createTape(s_adminOnAdminHost, s_vid, s_libraryName, s_tapePoolName, cta::nullopt, capacityInBytes,
    notDisabled, notFull, tapeComment);
  
  // Create the mount criteria
  catalogue.createMountPolicy(requester, "immediateMount", 1000, 0, 1000, 0, 1, "Policy comment");
  catalogue.createRequesterMountRule(requester, "immediateMount", s_diskInstance, requester.username, "Rule comment");

  //delete is unnecessary
  //pointer with ownership will be passed to the application,
  //which will do the delete
  const uint64_t tapeSize = 5000;
  mockSys.fake.m_pathToDrive["/dev/nst0"] = new castor::tape::tapeserver::drive::FakeDrive(tapeSize);
  
  // We can prepare files for writing on the drive.
  // Tempfiles are in this scope so they are kept alive 
  std::list<std::unique_ptr<unitTests::TempFile>> sourceFiles;
  std::list<uint64_t> archiveFileIds;
  {    
    // Label the tape
    castor::tape::tapeFile::LabelSession ls(*mockSys.fake.m_pathToDrive["/dev/nst0"], s_vid, false);
    catalogue.tapeLabelled(s_vid, "T10D6116", true);
    mockSys.fake.m_pathToDrive["/dev/nst0"]->rewind();
    
    // Create the files and schedule the archivals
    for(int fseq=1; fseq <= 10 ; fseq ++) { 
      // Create a source file.
      sourceFiles.emplace_back(cta::make_unique<unitTests::TempFile>());
      sourceFiles.back()->randomFill(1000);
      remoteFilePaths.push_back(sourceFiles.back()->path());
      // Schedule the archival of the file
      cta::common::dataStructures::ArchiveRequest ar;
      ar.checksumType="ADLER32";
      ar.checksumValue=sourceFiles.back()->adler32();
      ar.storageClass=s_storageClassName;
      ar.srcURL=std::string("file://") + sourceFiles.back()->path();
      ar.requester.name = requester.username;
      ar.requester.group = "group";
      ar.fileSize = 1000;
      ar.diskFileID = "x";
      ar.diskFileInfo.path = "y";
      ar.diskFileInfo.owner = "z";
      ar.diskFileInfo.group = "g";
      ar.diskFileInfo.recoveryBlob = "b";
      archiveFileIds.push_back(scheduler.queueArchive(s_diskInstance,ar,logContext));
    }
  }
  // Report the drive's existence and put it up in the drive register.
  cta::tape::daemon::TpconfigLine driveConfig("T10D6116", "TestLogicalLibrary", "/dev/tape_T10D6116", "manual");
  cta::common::dataStructures::DriveInfo driveInfo;
  driveInfo.driveName=driveConfig.unitName;
  driveInfo.logicalLibrary=driveConfig.rawLibrarySlot;
  // We need to create the drive in the registry before being able to put it up.
  scheduler.reportDriveStatus(driveInfo, cta::common::dataStructures::MountType::NoMount, cta::common::dataStructures::DriveStatus::Down);
  scheduler.setDesiredDriveState(s_adminOnAdminHost, driveConfig.unitName, true, false);

  // Create the data transfer session
  DataTransferConfig castorConf;
  castorConf.bufsz = 1024*1024; // 1 MB memory buffers
  castorConf.nbBufs = 10;
  castorConf.bulkRequestRecallMaxBytes = UINT64_C(100)*1000*1000*1000;
  castorConf.bulkRequestRecallMaxFiles = 1000;
  castorConf.nbDiskThreads = 1;
  cta::log::DummyLogger dummyLog("dummy");
  cta::mediachanger::MediaChangerFacade mc(dummyLog);
  cta::server::ProcessCap capUtils;
  castor::messages::TapeserverProxyDummy initialProcess;
  DataTransferSession sess("tapeHost", logger, mockSys, driveConfig, mc, initialProcess, capUtils, castorConf, scheduler);
  sess.execute();
  std::string temp = logger.getLog();
  temp += "";
  ASSERT_EQ(s_vid, sess.getVid());
  auto afiiter = archiveFileIds.begin();
  for(auto & sf: sourceFiles) {
    auto afi = *(afiiter++);
    // Only the first files made it through.
    if (afi <= 4) {
      auto afs = catalogue.getArchiveFileById(afi);
      ASSERT_EQ(1, afs.tapeFiles.size());
      ASSERT_EQ(sf->adler32(), afs.checksumValue);
      ASSERT_EQ(1000, afs.fileSize);
    } else {
      ASSERT_THROW(catalogue.getArchiveFileById(afi), cta::exception::Exception);
    }
    // The tape should now be marked as full
    cta::catalogue::TapeSearchCriteria crit;
    crit.vid = s_vid;
    auto tapes = catalogue.getTapes(crit);
    ASSERT_EQ(1, tapes.size());
    ASSERT_EQ(s_vid, tapes.front().vid);
    ASSERT_EQ(true, tapes.front().full);
  }
  // Check logs for drive statistics
  std::string logToCheck = logger.getLog();
  logToCheck += "";
  ASSERT_NE(std::string::npos, logToCheck.find("firmwareVersion=\"123A\" serialNumber=\"123456\" "
                                               "mountTotalCorrectedWriteErrors=\"5\" mountTotalUncorrectedWriteErrors=\"1\" " 
                                               "mountTotalWriteBytesProcessed=\"4096\" mountTotalNonMediumErrorCounts=\"2\""));

  ASSERT_NE(std::string::npos, logToCheck.find("firmwareVersion=\"123A\" serialNumber=\"123456\" lifetimeMediumEfficiencyPrct=\"100\" "
                                               "mountReadEfficiencyPrct=\"100\" mountWriteEfficiencyPrct=\"100\" "
                                               "mountReadTransients=\"10\" "
                                               "mountServoTemps=\"10\" mountServoTransients=\"5\" mountTemps=\"100\" "
                                               "mountTotalReadRetries=\"25\" mountTotalWriteRetries=\"25\" mountWriteTransients=\"10\""));
}

TEST_P(DataTransferSessionTest, DataTransferSessionTapeFullOnFlushMigration) {
  // 0) Prepare the logger for everyone
  cta::log::StringLogger logger("tapeServerUnitTest",cta::log::DEBUG);
  cta::log::LogContext logContext(logger);
  
  setupDefaultCatalogue();
  // 1) prepare the fake scheduler
  std::string vid = s_vid;
  // cta::MountType::Enum mountType = cta::MountType::RETRIEVE;

  // 3) Prepare the necessary environment (logger, plus system wrapper), 
  castor::tape::System::mockWrapper mockSys;
  mockSys.delegateToFake();
  mockSys.disableGMockCallsCounting();
  mockSys.fake.setupForVirtualDriveSLC6();
  
  // 4) Create the scheduler
  auto & catalogue = getCatalogue();
  auto & scheduler = getScheduler();
  
  // Always use the same requester
  const cta::common::dataStructures::SecurityIdentity requester("user", "group");
  
  // List to remember the path of each remote file so that the existance of the
  // files can be tested for at the end of the test
  std::list<std::string> remoteFilePaths;
  
  // 5) Create the environment for the migration to happen (library + tape) 
    const std::string libraryComment = "Library comment";
  catalogue.createLogicalLibrary(s_adminOnAdminHost, s_libraryName,
    libraryComment);
  {
    auto libraries = catalogue.getLogicalLibraries();
    ASSERT_EQ(1, libraries.size());
    ASSERT_EQ(s_libraryName, libraries.front().name);
    ASSERT_EQ(libraryComment, libraries.front().comment);
  }
  const uint64_t capacityInBytes = 12345678;
  const std::string tapeComment = "Tape comment";
  bool notDisabled = false;
  bool notFull = false;
  catalogue.createTape(s_adminOnAdminHost, s_vid, s_libraryName, s_tapePoolName, cta::nullopt, capacityInBytes,
    notDisabled, notFull, tapeComment);
  
  // Create the mount criteria
  catalogue.createMountPolicy(requester, "immediateMount", 1000, 0, 1000, 0, 1, "Policy comment");
  catalogue.createRequesterMountRule(requester, "immediateMount", s_diskInstance, requester.username, "Rule comment");

  //delete is unnecessary
  //pointer with ownership will be passed to the application,
  //which will do the delete
  const uint64_t tapeSize = 5000;
  mockSys.fake.m_pathToDrive["/dev/nst0"] = new castor::tape::tapeserver::drive::FakeDrive(tapeSize,
        castor::tape::tapeserver::drive::FakeDrive::OnFlush);
  
  // We can prepare files for writing on the drive.
  // Tempfiles are in this scope so they are kept alive 
  std::list<std::unique_ptr<unitTests::TempFile>> sourceFiles;
  std::list<uint64_t> archiveFileIds;
  {    
    // Label the tape
    castor::tape::tapeFile::LabelSession ls(*mockSys.fake.m_pathToDrive["/dev/nst0"], s_vid, false);
    catalogue.tapeLabelled(s_vid, "T10D6116", true);
    mockSys.fake.m_pathToDrive["/dev/nst0"]->rewind();
    
    // Create the files and schedule the archivals
    for(int fseq=1; fseq <= 10 ; fseq ++) { 
      // Create a source file.
      sourceFiles.emplace_back(cta::make_unique<unitTests::TempFile>());
      sourceFiles.back()->randomFill(1000);
      remoteFilePaths.push_back(sourceFiles.back()->path());
      // Schedule the archival of the file
      cta::common::dataStructures::ArchiveRequest ar;
      ar.checksumType="ADLER32";
      ar.checksumValue=sourceFiles.back()->adler32();
      ar.storageClass=s_storageClassName;
      ar.srcURL=std::string("file://") + sourceFiles.back()->path();
      ar.requester.name = requester.username;
      ar.requester.group = "group";
      ar.fileSize = 1000;
      ar.diskFileID = "x";
      ar.diskFileInfo.path = "y";
      ar.diskFileInfo.owner = "z";
      ar.diskFileInfo.group = "g";
      ar.diskFileInfo.recoveryBlob = "b";
      archiveFileIds.push_back(scheduler.queueArchive(s_diskInstance,ar,logContext));
    }
  }
  // Report the drive's existence and put it up in the drive register.
  cta::tape::daemon::TpconfigLine driveConfig("T10D6116", "TestLogicalLibrary", "/dev/tape_T10D6116", "manual");
  cta::common::dataStructures::DriveInfo driveInfo;
  driveInfo.driveName=driveConfig.unitName;
  driveInfo.logicalLibrary=driveConfig.rawLibrarySlot;
  // We need to create the drive in the registry before being able to put it up.
  scheduler.reportDriveStatus(driveInfo, cta::common::dataStructures::MountType::NoMount, cta::common::dataStructures::DriveStatus::Down);
  scheduler.setDesiredDriveState(s_adminOnAdminHost, driveConfig.unitName, true, false);

  // Create the data transfer session
  DataTransferConfig castorConf;
  castorConf.bufsz = 1024*1024; // 1 MB memory buffers
  castorConf.nbBufs = 10;
  castorConf.bulkRequestRecallMaxBytes = UINT64_C(100)*1000*1000*1000;
  castorConf.bulkRequestRecallMaxFiles = 1000;
  castorConf.nbDiskThreads = 1;
  cta::log::DummyLogger dummyLog("dummy");
  cta::mediachanger::MediaChangerFacade mc(dummyLog);
  cta::server::ProcessCap capUtils;
  castor::messages::TapeserverProxyDummy initialProcess;
  DataTransferSession sess("tapeHost", logger, mockSys, driveConfig, mc, initialProcess, capUtils, castorConf, scheduler);
  sess.execute();
  std::string temp = logger.getLog();
  temp += "";
  ASSERT_EQ(s_vid, sess.getVid());
  auto afiiter = archiveFileIds.begin();
  for(auto & sf: sourceFiles) {
    auto afi = *(afiiter++);
    // Only the first files made it through.
    if (afi <= 4) {
      auto afs = catalogue.getArchiveFileById(afi);
      ASSERT_EQ(1, afs.tapeFiles.size());
      ASSERT_EQ(sf->adler32(), afs.checksumValue);
      ASSERT_EQ(1000, afs.fileSize);
    } else {
      ASSERT_THROW(catalogue.getArchiveFileById(afi), cta::exception::Exception);
    }
    // The tape should now be marked as full
    cta::catalogue::TapeSearchCriteria crit;
    crit.vid = s_vid;
    auto tapes = catalogue.getTapes(crit);
    ASSERT_EQ(1, tapes.size());
    ASSERT_EQ(s_vid, tapes.front().vid);
    ASSERT_EQ(true, tapes.front().full);
  }
  // Check logs for drive statistics
  std::string logToCheck = logger.getLog();
  logToCheck += "";
  ASSERT_NE(std::string::npos, logToCheck.find("firmwareVersion=\"123A\" serialNumber=\"123456\" "
                                               "mountTotalCorrectedWriteErrors=\"5\" mountTotalUncorrectedWriteErrors=\"1\" "
                                               "mountTotalWriteBytesProcessed=\"4096\" mountTotalNonMediumErrorCounts=\"2\""));
  ASSERT_NE(std::string::npos, logToCheck.find("firmwareVersion=\"123A\" serialNumber=\"123456\" lifetimeMediumEfficiencyPrct=\"100\" "
                                               "mountReadEfficiencyPrct=\"100\" mountWriteEfficiencyPrct=\"100\" "
                                               "mountReadTransients=\"10\" "
                                               "mountServoTemps=\"10\" mountServoTransients=\"5\" mountTemps=\"100\" "
                                               "mountTotalReadRetries=\"25\" mountTotalWriteRetries=\"25\" mountWriteTransients=\"10\""));
}

#undef TEST_MOCK_DB
#ifdef TEST_MOCK_DB
static cta::MockSchedulerDatabaseFactory mockDbFactory;
INSTANTIATE_TEST_CASE_P(MockSchedulerTest, SchedulerTest,
  ::testing::Values(SchedulerTestParam(mockDbFactory)));
#endif

#define TEST_VFS
#ifdef TEST_VFS
static cta::OStoreDBFactory<cta::objectstore::BackendVFS> OStoreDBFactoryVFS;

INSTANTIATE_TEST_CASE_P(OStoreDBPlusMockSchedulerTestVFS, DataTransferSessionTest,
  ::testing::Values(DataTransferSessionTestParam(OStoreDBFactoryVFS)));
#endif

#ifdef TEST_RADOS
static cta::OStoreDBFactory<cta::objectstore::BackendRados> OStoreDBFactoryRados("rados://tapetest@tapetest");

INSTANTIATE_TEST_CASE_P(OStoreDBPlusMockSchedulerTestRados, DataTransferSessionTest,
  ::testing::Values(DataTransferSessionTestParam(OStoreDBFactoryRados)));
#endif

} // namespace unitTest
