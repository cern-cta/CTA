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
#include "catalogue/OracleCatalogue.hpp"
#include "catalogue/OracleCatalogueSchema.hpp"
#include "common/exception/Exception.hpp"
#include "common/log/DummyLogger.hpp"
#include "common/log/StringLogger.hpp"
#include "common/log/StdoutLogger.hpp"
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
#include "CleanerSession.hpp"

#ifdef STDOUT_LOGGING
#include "common/log/StdoutLogger.hpp"
#else

#include "common/log/DummyLogger.hpp"

#endif

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

const uint32_t DISK_FILE_OWNER_UID = 9751;
const uint32_t DISK_FILE_GID = 9752;
const uint32_t DISK_FILE_SOME_USER = 9753;
const uint32_t DISK_FILE_SOME_GROUP = 9754;

namespace {

/**
 * This structure is used to parameterize scheduler tests.
 */
struct DataTransferSessionTestParam {
  cta::SchedulerDatabaseFactory& dbFactory;

  explicit DataTransferSessionTestParam(
    cta::SchedulerDatabaseFactory& dbFactory) :
    dbFactory(dbFactory) {
  }
}; // struct DataTransferSessionTest

}

/**
 * The data transfer test is a parameterized test.  It takes a pair of name server
 * and scheduler database factories as a parameter.
 */
class DataTransferSessionTest : public ::testing::TestWithParam<DataTransferSessionTestParam> {
public:

  DataTransferSessionTest() :
    m_dummyLog("dummy", "dummy") {
  }


  class FailedToGetCatalogue : public std::exception {
  public:
    const char *what() const noexcept {
      return "Failed to get catalogue";
    }
  };

  class FailedToGetScheduler : public std::exception {
  public:
    const char *what() const noexcept {
      return "Failed to get scheduler";
    }
  };

#undef USE_ORACLE_CATALOGUE
#ifdef USE_ORACLE_CATALOGUE
  class OracleCatalogueExposingConnection: public cta::catalogue::OracleCatalogue {
  public:
    template <typename...Ts> OracleCatalogueExposingConnection(Ts&...args): cta::catalogue::OracleCatalogue(args...) {}
    cta::rdbms::Conn getConn() { return m_connPool.getConn(); }
  };
#endif

  void SetUp() override {
    using namespace cta;

    const DataTransferSessionTestParam& param = GetParam();
    const uint64_t nbConns = 1;
    const uint64_t nbArchiveFileListingConns = 1;
#ifdef USE_ORACLE_CATALOGUE
    cta::rdbms::Login login=cta::rdbms::Login::parseFile("/etc/cta/cta-catalogue.conf");

    m_catalogue = cta::make_unique<OracleCatalogueExposingConnection>(m_dummyLog, login.username, login.password, login.database,
        nbConns, nbArchiveFileListingConns, maxTriesToConnect);
    try {
      // If we decide to create an oracle catalogue, we have to prepare it.
      // This is a striped down version of CreateSchemaCmd.
      OracleCatalogueExposingConnection & oracleCatalogue = dynamic_cast<OracleCatalogueExposingConnection &>(*m_catalogue);
      auto conn=oracleCatalogue.getConn();
      for (auto &name: conn.getTableNames())
        if (name=="CTA_CATALOGUE") throw cta::exception::Exception("In SetUp(): schema is already populated.");
      cta::catalogue::OracleCatalogueSchema schema;
      conn.executeNonQueries(schema.sql);
    } catch (std::bad_cast &) {}
#else
    //m_catalogue = cta::make_unique<catalogue::SchemaCreatingSqliteCatalogue>(m_tempSqliteFile.path(), nbConns);
    m_catalogue = cta::make_unique<catalogue::InMemoryCatalogue>(m_dummyLog, nbConns, nbArchiveFileListingConns);
#endif
    m_db = param.dbFactory.create(m_catalogue);
    m_scheduler = cta::make_unique<Scheduler>(*m_catalogue, *m_db, 5, 2 * 1000 * 1000);

    strncpy(m_tmpDir, "/tmp/DataTransferSessionTestXXXXXX", sizeof(m_tmpDir));
    if (!mkdtemp(m_tmpDir)) {
      const std::string errMsg = cta::utils::errnoToString(errno);
      std::ostringstream msg;
      msg << "Failed to create directory with template"
             " /tmp/DataTransferSessionTestXXXXXX: " << errMsg;
      memset(m_tmpDir, 0, sizeof(m_tmpDir));
      throw cta::exception::Exception(msg.str());
    }

    struct stat statBuf;
    memset(&statBuf, 0, sizeof(statBuf));
    if (stat(m_tmpDir, &statBuf)) {
      const std::string errMsg = cta::utils::errnoToString(errno);
      std::ostringstream msg;
      msg << "Failed to stat directory " << m_tmpDir << ": " << errMsg;
      throw cta::exception::Exception(msg.str());
    }

    std::ostringstream cmd;
    cmd << "touch " << m_tmpDir << "/hello";
    system(cmd.str().c_str());
  }

  void TearDown() override {
    m_scheduler.reset();
    m_catalogue.reset();
    m_db.reset();

    // If Setup() created a temporary directory
    if (m_tmpDir) {

      // Open the directory
      std::unique_ptr<DIR, std::function<int(DIR*)>>
        dir(opendir(m_tmpDir), closedir);
      if (nullptr == dir) {
        const std::string errMsg = cta::utils::errnoToString(errno);
        std::ostringstream msg;
        msg << "Failed to open directory " << m_tmpDir << ": " << errMsg;
        throw cta::exception::Exception(msg.str());
      }

      // Delete each of the files within the directory
      struct dirent *entry = nullptr;
      while ((entry = readdir(dir.get()))) {
        const std::string entryName(entry->d_name);
        if (entryName != "." && entryName != "..") {
          const std::string entryPath = std::string(m_tmpDir) + "/" + entryName;
          if (unlink(entryPath.c_str())) {
            const std::string errMsg = cta::utils::errnoToString(errno);
            std::ostringstream msg;
            msg << "Failed to unlink " << entryPath;
            throw cta::exception::Exception(msg.str());
          }
        }
      }

      // Delete the now empty directory
      if (rmdir(m_tmpDir)) {
        const std::string errMsg = cta::utils::errnoToString(errno);
        std::ostringstream msg;
        msg << "Failed to delete directory " << m_tmpDir << ": " << errMsg;
        throw cta::exception::Exception(msg.str());
      }
    }
  }

  cta::catalogue::Catalogue& getCatalogue() {
    cta::catalogue::Catalogue *const ptr = m_catalogue.get();
    if (nullptr == ptr) {
      throw FailedToGetCatalogue();
    }
    return *ptr;
  }

  cta::Scheduler& getScheduler() {
    cta::Scheduler *const ptr = m_scheduler.get();
    if (nullptr == ptr) {
      throw FailedToGetScheduler();
    }
    return *ptr;
  }

  cta::catalogue::CreateTapeAttributes getDefaultTape() {
    cta::catalogue::CreateTapeAttributes tape;
    tape.vid = s_vid;
    tape.mediaType = s_mediaType;
    tape.vendor = s_vendor;
    tape.logicalLibraryName = s_libraryName;
    tape.tapePoolName = s_tapePoolName;
    tape.full = false;
    tape.state = cta::common::dataStructures::Tape::ACTIVE;
    tape.comment = "Comment";
    return tape;
  }

  cta::catalogue::CreateMountPolicyAttributes getDefaultMountPolicy() {
    cta::catalogue::CreateMountPolicyAttributes mountPolicy;
    mountPolicy.name = "mount_group";
    mountPolicy.archivePriority = 1;
    mountPolicy.minArchiveRequestAge = 2;
    mountPolicy.retrievePriority = 3;
    mountPolicy.minRetrieveRequestAge = 4;
    mountPolicy.comment = "create mount group";
    return mountPolicy;
  }

  cta::catalogue::CreateMountPolicyAttributes getImmediateMountMountPolicy() {
    cta::catalogue::CreateMountPolicyAttributes mountPolicy;
    mountPolicy.name = "immediateMount";
    mountPolicy.archivePriority = 1000;
    mountPolicy.minArchiveRequestAge = 0;
    mountPolicy.retrievePriority = 1000;
    mountPolicy.minRetrieveRequestAge = 0;
    mountPolicy.comment = "Immediate mount";
    return mountPolicy;
  }

  cta::common::dataStructures::VirtualOrganization getDefaultVirtualOrganization() {
    cta::common::dataStructures::VirtualOrganization vo;
    vo.name = "vo";
    vo.readMaxDrives = 1;
    vo.writeMaxDrives = 1;
    vo.maxFileSize = 0;
    vo.comment = "comment";
    return vo;
  }

  cta::common::dataStructures::TapeDrive getDefaultTapeDrive(const std::string& driveName) {
    cta::common::dataStructures::TapeDrive tapeDrive;
    tapeDrive.driveName = driveName;
    tapeDrive.host = "admin_host";
    tapeDrive.logicalLibrary = "VLSTK10";
    tapeDrive.mountType = cta::common::dataStructures::MountType::NoMount;
    tapeDrive.driveStatus = cta::common::dataStructures::DriveStatus::Up;
    tapeDrive.desiredUp = false;
    tapeDrive.desiredForceDown = false;
    tapeDrive.diskSystemName = "dummyDiskSystemName";
    tapeDrive.reservedBytes = 694498291384;
    tapeDrive.reservationSessionId = 0;
    return tapeDrive;
  }

  void setupDefaultCatalogue() {
    using namespace cta;
    auto& catalogue = getCatalogue();

    auto mountPolicy = getDefaultMountPolicy();

    const std::string mountPolicyName = mountPolicy.name;
    const uint64_t archivePriority = mountPolicy.archivePriority;
    const uint64_t minArchiveRequestAge = mountPolicy.minArchiveRequestAge;
    const uint64_t retrievePriority = mountPolicy.retrievePriority;
    const uint64_t minRetrieveRequestAge = mountPolicy.minRetrieveRequestAge;
    const std::string mountPolicyComment = "create mount group";

    ASSERT_TRUE(catalogue.getMountPolicies().empty());

    catalogue.createMountPolicy(
      s_adminOnAdminHost,
      mountPolicy);

    const std::list<common::dataStructures::MountPolicy> groups = catalogue.getMountPolicies();
    ASSERT_EQ(1, groups.size());
    const common::dataStructures::MountPolicy group = groups.front();
    ASSERT_EQ(mountPolicyName, group.name);
    ASSERT_EQ(archivePriority, group.archivePriority);
    ASSERT_EQ(minArchiveRequestAge, group.archiveMinRequestAge);
    ASSERT_EQ(retrievePriority, group.retrievePriority);
    ASSERT_EQ(minRetrieveRequestAge, group.retrieveMinRequestAge);
    ASSERT_EQ(mountPolicyComment, group.comment);

    const std::string ruleComment = "create requester mount-rule";
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

    cta::common::dataStructures::VirtualOrganization vo = getDefaultVirtualOrganization();
    catalogue.createVirtualOrganization(s_adminOnAdminHost, vo);

    common::dataStructures::StorageClass storageClass;
    storageClass.name = s_storageClassName;
    storageClass.nbCopies = 1;
    storageClass.vo.name = vo.name;
    storageClass.comment = "create storage class";
    m_catalogue->createStorageClass(s_adminOnAdminHost, storageClass);

    const uint16_t nbPartialTapes = 1;
    const std::string tapePoolComment = "Tape-pool comment";
    const bool tapePoolEncryption = false;
    const cta::optional<std::string> tapePoolSupply("value for the supply pool mechanism");

    ASSERT_NO_THROW(catalogue.createTapePool(s_adminOnAdminHost, s_tapePoolName, vo.name, nbPartialTapes, tapePoolEncryption,
                                             tapePoolSupply, tapePoolComment));
    const uint32_t copyNb = 1;
    const std::string archiveRouteComment = "Archive-route comment";
    catalogue.createArchiveRoute(s_adminOnAdminHost, s_storageClassName, copyNb, s_tapePoolName,
                                 archiveRouteComment);

    cta::catalogue::MediaType mediaType;
    mediaType.name = s_mediaType;
    mediaType.capacityInBytes = 12345678;
    mediaType.cartridge = "cartridge";
    mediaType.minLPos = 2696;
    mediaType.maxLPos = 171097;
    mediaType.nbWraps = 112;
    mediaType.comment = "comment";
    catalogue.createMediaType(s_adminOnAdminHost, mediaType);

    const std::string driveName = "T10D6116";
    const auto tapeDrive = getDefaultTapeDrive(driveName);
    catalogue.createTapeDrive(tapeDrive);
  }

  /**
   * Returns the map of Fseqs given by RAO from a string containing CTA logs
   * @param log the string containing the CTA logs
   * @return the map that gives for each RAO call, the associated ordered Fseqs according to the RAO algorithm result
   */
  std::map<size_t, std::vector<std::string>> getRAOFseqs(const std::string& log) {
    std::map<size_t, std::vector<std::string>> ret;
    std::string logCopy = log;
    std::string recallRAOOrderMsg = "Recall order of FSEQs using RAO: ";
    size_t recallRAOOrderMsgSize = recallRAOOrderMsg.size();
    size_t posRAOMsg = logCopy.find(recallRAOOrderMsg);
    size_t i = 0;
    while (posRAOMsg != std::string::npos) {
      size_t posFirstFseq = posRAOMsg + recallRAOOrderMsgSize;
      size_t posLastFseq = logCopy.find('\"', posFirstFseq);
      std::string stringFseq = logCopy.substr(posFirstFseq, posLastFseq - posFirstFseq);
      cta::utils::splitString(stringFseq, ' ', ret[i]);
      logCopy = logCopy.substr(posLastFseq);
      posRAOMsg = logCopy.find(recallRAOOrderMsg);
      i++;
    }
    return ret;
  }

private:

  // Prevent copying
  DataTransferSessionTest(const DataTransferSessionTest&) = delete;

  // Prevent assignment
  DataTransferSessionTest& operator=(const DataTransferSessionTest&) = delete;

  std::unique_ptr<cta::SchedulerDatabase> m_db;
  std::unique_ptr<cta::catalogue::Catalogue> m_catalogue;
  std::unique_ptr<cta::Scheduler> m_scheduler;

protected:
#ifdef STDOUT_LOGGING
  cta::log::StdoutLogger m_dummyLog;
#else
  cta::log::DummyLogger m_dummyLog;
#endif

  // Default parameters for storage classes, etc...
  const std::string s_userName = "user_name";
  const std::string s_diskInstance = "disk_instance";
  const std::string s_storageClassName = "TestStorageClass";
  const cta::common::dataStructures::SecurityIdentity s_adminOnAdminHost = {"admin1", "host1"};
  const std::string s_tapePoolName = "TestTapePool";
  const std::string s_libraryName = "TestLogicalLibrary";
  const std::string s_vid = "TstVid"; // We really need size <= 6 characters due to tape label format.
  const std::string s_mediaType = "LTO7M";
  const std::string s_vendor = "TestVendor";
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
  cta::log::StringLogger logger("dummy", "tapeServerUnitTest", cta::log::DEBUG);
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
  auto& catalogue = getCatalogue();
  auto& scheduler = getScheduler();

  // Always use the same requester
  const cta::common::dataStructures::SecurityIdentity requester;

  // List to remember the path of each remote file so that the existence of the
  // files can be tested for at the end of the test
  std::list<std::string> remoteFilePaths;

  // 5) Create the environment for the migration to happen (library + tape)
  const std::string libraryComment = "Library comment";
  const bool libraryIsDisabled = false;
  catalogue.createLogicalLibrary(s_adminOnAdminHost, s_libraryName,
                                 libraryIsDisabled, libraryComment);
  {
    auto libraries = catalogue.getLogicalLibraries();
    ASSERT_EQ(1, libraries.size());
    ASSERT_EQ(s_libraryName, libraries.front().name);
    ASSERT_EQ(libraryComment, libraries.front().comment);
  }

  {
    auto tape = getDefaultTape();
    catalogue.createTape(s_adminOnAdminHost, tape);
  }

  // 6) Prepare files for reading by writing them to the mock system
  {
    // Label the tape
    castor::tape::tapeFile::LabelSession ls(*mockSys.fake.m_pathToDrive["/dev/nst0"],
                                            s_vid, false);
    mockSys.fake.m_pathToDrive["/dev/nst0"]->rewind();
    // And write to it
    castor::tape::tapeserver::daemon::VolumeInfo volInfo;
    volInfo.vid = s_vid;
    castor::tape::tapeFile::WriteSession ws(*mockSys.fake.m_pathToDrive["/dev/nst0"],
                                            volInfo, 0, true, false);

    // Write a few files on the virtual tape and modify the archive name space
    // so that it is in sync
    uint8_t data[1000];
    size_t archiveFileSize = sizeof(data);
    castor::tape::SCSI::Structures::zeroStruct(&data);
    for (int fseq = 1; fseq <= 10; fseq++) {
      // Create a path to a remote destination file
      std::ostringstream remoteFilePath;
      remoteFilePath << "file://" << m_tmpDir << "/test" << fseq;
      remoteFilePaths.push_back(remoteFilePath.str());

      // Create an archive file entry in the archive namespace
      auto tapeFileWrittenUP = cta::make_unique<cta::catalogue::TapeFileWritten>();
      auto& tapeFileWritten = *tapeFileWrittenUP;
      std::set<cta::catalogue::TapeItemWrittenPointer> tapeFileWrittenSet;
      tapeFileWrittenSet.insert(tapeFileWrittenUP.release());

      // Write the file to tape
      cta::MockArchiveMount mam(catalogue);
      std::unique_ptr<cta::ArchiveJob> aj(new cta::MockArchiveJob(&mam, catalogue));
      aj->tapeFile.fSeq = fseq;
      aj->archiveFile.archiveFileID = fseq;
      castor::tape::tapeFile::WriteFile wf(&ws, *aj, archiveFileSize);
      tapeFileWritten.blockId = wf.getBlockId();
      // Write the data (one block)
      wf.write(data, archiveFileSize);
      // Close the file
      wf.close();

      // Create file entry in the archive namespace
      tapeFileWritten.archiveFileId = fseq;
      tapeFileWritten.checksumBlob.insert(cta::checksum::ADLER32, cta::utils::getAdler32(data, archiveFileSize));
      tapeFileWritten.vid = volInfo.vid;
      tapeFileWritten.size = archiveFileSize;
      tapeFileWritten.fSeq = fseq;
      tapeFileWritten.copyNb = 1;
      tapeFileWritten.diskInstance = s_diskInstance;
      tapeFileWritten.diskFileId = fseq;

      tapeFileWritten.diskFileOwnerUid = DISK_FILE_SOME_USER;
      tapeFileWritten.diskFileGid = DISK_FILE_SOME_GROUP;
      tapeFileWritten.storageClassName = s_storageClassName;
      tapeFileWritten.tapeDrive = "drive0";
      catalogue.filesWrittenToTape(tapeFileWrittenSet);

      // Schedule the retrieval of the file
      std::string diskInstance = "disk_instance";
      cta::common::dataStructures::RetrieveRequest rReq;
      rReq.archiveFileID = fseq;
      rReq.requester.name = s_userName;
      rReq.requester.group = "someGroup";
      rReq.dstURL = remoteFilePaths.back();
      rReq.diskFileInfo.path = "path/to/file";
      rReq.isVerifyOnly = false;
      std::list<std::string> archiveFilePaths;
      scheduler.queueRetrieve(diskInstance, rReq, logContext);
    }
  }
  scheduler.waitSchedulerDbSubthreadsComplete();

  // 6) Report the drive's existence and put it up in the drive register.
  cta::tape::daemon::TpconfigLine driveConfig("T10D6116", "TestLogicalLibrary", "/dev/tape_T10D6116", "dummy");
  cta::common::dataStructures::DriveInfo driveInfo;
  driveInfo.driveName = driveConfig.unitName;
  driveInfo.logicalLibrary = driveConfig.logicalLibrary;
  driveInfo.host = "host";
  // We need to create the drive in the registry before being able to put it up.
  scheduler.reportDriveStatus(driveInfo, cta::common::dataStructures::MountType::NoMount, cta::common::dataStructures::DriveStatus::Down, logContext);
  cta::common::dataStructures::DesiredDriveState driveState;
  driveState.up = true;
  driveState.forceDown = false;
  scheduler.setDesiredDriveState(s_adminOnAdminHost, driveConfig.unitName, driveState, logContext);

  // 7) Create the data transfer session
  DataTransferConfig castorConf;
  castorConf.bufsz = 1024 * 1024; // 1 MB memory buffers
  castorConf.nbBufs = 10;
  castorConf.bulkRequestRecallMaxBytes = UINT64_C(100) * 1000 * 1000 * 1000;
  castorConf.bulkRequestRecallMaxFiles = 1000;
  castorConf.nbDiskThreads = 1;
  castorConf.tapeLoadTimeout = 300;
  castorConf.useEncryption = false;
  cta::log::DummyLogger dummyLog("dummy", "dummy");
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
  for (const auto& path: remoteFilePaths) {
    struct stat statBuf;
    memset(&statBuf, 0, sizeof(statBuf));
    const int statRc = stat(path.substr(7).c_str(), &statBuf); //remove the "file://" for stat-ing
    ASSERT_EQ(0, statRc);
    ASSERT_EQ(1000, statBuf.st_size); //same size of data
  }

  // 10) Check logs
  std::string logToCheck = logger.getLog();
  logToCheck += "";
  ASSERT_NE(std::string::npos, logToCheck.find("MSG=\"Tape session started for read\" thread=\"TapeRead\" tapeDrive=\"T10D6116\" tapeVid=\"TstVid\" "
                                               "mountId=\"1\" vo=\"vo\" mediaType=\"LTO7M\" tapePool=\"TestTapePool\" "
                                               "logicalLibrary=\"TestLogicalLibrary\" mountType=\"Retrieve\" vendor=\"TestVendor\" "
                                               "capacityInBytes=\"12345678\""));
  ASSERT_NE(std::string::npos, logToCheck.find("firmwareVersion=\"123A\" serialNumber=\"123456\" "
                                               "mountTotalCorrectedReadErrors=\"5\" mountTotalReadBytesProcessed=\"4096\" "
                                               "mountTotalUncorrectedReadErrors=\"1\" mountTotalNonMediumErrorCounts=\"2\""));
  ASSERT_NE(std::string::npos, logToCheck.find("firmwareVersion=\"123A\" serialNumber=\"123456\" lifetimeMediumEfficiencyPrct=\"100\" "
                                               "mountReadEfficiencyPrct=\"100\" mountWriteEfficiencyPrct=\"100\" "
                                               "mountReadTransients=\"10\" "
                                               "mountServoTemps=\"10\" mountServoTransients=\"5\" mountTemps=\"100\" "
                                               "mountTotalReadRetries=\"25\" mountTotalWriteRetries=\"25\" mountWriteTransients=\"10\""));
}

TEST_P(DataTransferSessionTest, DataTransferSessionWrongChecksumRecall) {
  // 0) Prepare the logger for everyone
  cta::log::StringLogger logger("dummy", "tapeServerUnitTest",cta::log::DEBUG);
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

  // List to remember the path of each remote file so that the existence of the
  // files can be tested for at the end of the test
  std::list<std::string> remoteFilePaths;

  // 5) Create the environment for the migration to happen (library + tape)
    const std::string libraryComment = "Library comment";
  const bool libraryIsDisabled = false;
  catalogue.createLogicalLibrary(s_adminOnAdminHost, s_libraryName,
    libraryIsDisabled, libraryComment);
  {
    auto libraries = catalogue.getLogicalLibraries();
    ASSERT_EQ(1, libraries.size());
    ASSERT_EQ(s_libraryName, libraries.front().name);
    ASSERT_EQ(libraryComment, libraries.front().comment);
  }

  {
    auto tape = getDefaultTape();
    catalogue.createTape(s_adminOnAdminHost, tape);
  }

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
      auto tapeFileWrittenUP = cta::make_unique<cta::catalogue::TapeFileWritten>();
      auto &tapeFileWritten=*tapeFileWrittenUP;
      std::set<cta::catalogue::TapeItemWrittenPointer> tapeFileWrittenSet;
      tapeFileWrittenSet.insert(tapeFileWrittenUP.release());

      // Write the file to tape
      cta::MockArchiveMount mam(catalogue);
      std::unique_ptr<cta::ArchiveJob> aj(new cta::MockArchiveJob(&mam, catalogue));
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
      if (fseq == 4) {
        // Fourth file will have wrong checksum and will not be recalled
        tapeFileWritten.checksumBlob.insert(cta::checksum::ADLER32, cta::utils::getAdler32(data, archiveFileSize) + 1);  
      }
      else {
        tapeFileWritten.checksumBlob.insert(cta::checksum::ADLER32, cta::utils::getAdler32(data, archiveFileSize));
      }
      tapeFileWritten.vid=volInfo.vid;
      tapeFileWritten.size=archiveFileSize;
      tapeFileWritten.fSeq=fseq;
      tapeFileWritten.copyNb=1;
      tapeFileWritten.diskInstance = s_diskInstance;
      tapeFileWritten.diskFileId = fseq;

      tapeFileWritten.diskFileOwnerUid = DISK_FILE_SOME_USER;
      tapeFileWritten.diskFileGid = DISK_FILE_SOME_GROUP;
      tapeFileWritten.storageClassName = s_storageClassName;
      tapeFileWritten.tapeDrive = "drive0";
      catalogue.filesWrittenToTape(tapeFileWrittenSet);

      // Schedule the retrieval of the file
      std::string diskInstance="disk_instance";
      cta::common::dataStructures::RetrieveRequest rReq;
      rReq.archiveFileID=fseq;
      rReq.requester.name = s_userName;
      rReq.requester.group = "someGroup";
      rReq.dstURL = remoteFilePaths.back();
      rReq.diskFileInfo.path = "path/to/file";
      rReq.isVerifyOnly = false;
      std::list<std::string> archiveFilePaths;
      scheduler.queueRetrieve(diskInstance, rReq, logContext);
    }
  }
  scheduler.waitSchedulerDbSubthreadsComplete();

  // 6) Report the drive's existence and put it up in the drive register.
  cta::tape::daemon::TpconfigLine driveConfig("T10D6116", "TestLogicalLibrary", "/dev/tape_T10D6116", "dummy");
  cta::common::dataStructures::DriveInfo driveInfo;
  driveInfo.driveName=driveConfig.unitName;
  driveInfo.logicalLibrary=driveConfig.logicalLibrary;
  driveInfo.host="host";
  // We need to create the drive in the registry before being able to put it up.
  scheduler.reportDriveStatus(driveInfo, cta::common::dataStructures::MountType::NoMount, cta::common::dataStructures::DriveStatus::Down, logContext);
  cta::common::dataStructures::DesiredDriveState driveState;
  driveState.up = true;
  driveState.forceDown = false;
  scheduler.setDesiredDriveState(s_adminOnAdminHost, driveConfig.unitName, driveState, logContext);

  // 7) Create the data transfer session
  DataTransferConfig castorConf;
  castorConf.bufsz = 1024*1024; // 1 MB memory buffers
  castorConf.nbBufs = 10;
  castorConf.bulkRequestRecallMaxBytes = UINT64_C(100)*1000*1000*1000;
  castorConf.bulkRequestRecallMaxFiles = 1000;
  castorConf.nbDiskThreads = 1;
  castorConf.tapeLoadTimeout = 300;
  castorConf.useEncryption = false;
  cta::log::DummyLogger dummyLog("dummy", "dummy");
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
  int fseq = 1;
  for(auto & path: remoteFilePaths) {
    struct stat statBuf;
    bzero(&statBuf, sizeof(statBuf));
    int statRc = stat(path.substr(7).c_str(), &statBuf); //remove the "file://" for stat-ing
    // File with wrong checksum are not recalled 
    // Rest of the files were read (correct behaviour, unlike archive)
    if (fseq == 4) {
      ASSERT_EQ(-1, statRc); 
      ASSERT_EQ(errno, ENOENT);  
    } else {
      ASSERT_EQ(0, statRc);
      ASSERT_EQ(1000, statBuf.st_size); //files should be empty
    }
    fseq++;
  }

  // 10) Check logs
  std::string logToCheck = logger.getLog();
  logToCheck += "";
  ASSERT_NE(std::string::npos,logToCheck.find("MSG=\"Tape session started for read\" thread=\"TapeRead\" tapeDrive=\"T10D6116\" tapeVid=\"TstVid\" "
                                              "mountId=\"1\" vo=\"vo\" mediaType=\"LTO7M\" tapePool=\"TestTapePool\" "
                                              "logicalLibrary=\"TestLogicalLibrary\" mountType=\"Retrieve\" vendor=\"TestVendor\" "
                                              "capacityInBytes=\"12345678\""));
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
  // This test is the same as DataTransferSessionGooddayRecall, with
  // wrong parameters set for the recall, so that we fail
  // to recall the first file and cancel the second.

  // 0) Prepare the logger for everyone
  cta::log::StringLogger logger("dummy", "tapeServerUnitTest", cta::log::DEBUG);
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
  auto& catalogue = getCatalogue();
  auto& scheduler = getScheduler();

  // Always use the same requester
  const cta::common::dataStructures::SecurityIdentity requester;

  // List to remember the path of each remote file so that the existence of the
  // files can be tested for at the end of the test
  std::list<std::string> remoteFilePaths;

  // 5) Create the environment for the migration to happen (library + tape)
  const std::string libraryComment = "Library comment";
  const bool libraryIsDisabled = false;
  catalogue.createLogicalLibrary(s_adminOnAdminHost, s_libraryName,
                                 libraryIsDisabled, libraryComment);
  {
    auto libraries = catalogue.getLogicalLibraries();
    ASSERT_EQ(1, libraries.size());
    ASSERT_EQ(s_libraryName, libraries.front().name);
    ASSERT_EQ(libraryComment, libraries.front().comment);
  }

  {
    auto tape = getDefaultTape();
    catalogue.createTape(s_adminOnAdminHost, tape);
  }

  // 6) Prepare files for reading by writing them to the mock system
  {
    // Label the tape
    castor::tape::tapeFile::LabelSession ls(*mockSys.fake.m_pathToDrive["/dev/nst0"],
                                            s_vid, false);
    mockSys.fake.m_pathToDrive["/dev/nst0"]->rewind();
    // And write to it
    castor::tape::tapeserver::daemon::VolumeInfo volInfo;
    volInfo.vid = s_vid;
    castor::tape::tapeFile::WriteSession ws(*mockSys.fake.m_pathToDrive["/dev/nst0"],
                                            volInfo, 0, true, false);

    // Write a few files on the virtual tape and modify the archive name space
    // so that it is in sync
    uint8_t data[1000];
    castor::tape::SCSI::Structures::zeroStruct(&data);
    int fseq = 1;
    {
      // Create a path to a remote destination file
      std::ostringstream remoteFilePath;
      remoteFilePath << "file://" << m_tmpDir << "/test" << fseq;
      remoteFilePaths.push_back(remoteFilePath.str());

      // Write the file to tape
      const uint64_t archiveFileSize = 1000;
      cta::MockArchiveMount mam(catalogue);
      cta::MockRetrieveMount mrm(catalogue);
      std::unique_ptr<cta::ArchiveJob> aj(new cta::MockArchiveJob(&mam, catalogue));
      aj->tapeFile.fSeq = fseq;
      aj->archiveFile.archiveFileID = 1000 + fseq;
      castor::tape::tapeFile::WriteFile wf(&ws, *aj, archiveFileSize);
      // Write the data (one block)
      wf.write(data, sizeof(data));
      // Close the file
      wf.close();

      {
        // Create a fictious file record on the tape to allow adding one to fseq=2 afterwards.
        auto tapeFileWrittenUP = cta::make_unique<cta::catalogue::TapeFileWritten>();
        auto& tapeFileWritten = *tapeFileWrittenUP;
        std::set<cta::catalogue::TapeItemWrittenPointer> tapeFileWrittenSet;
        tapeFileWrittenSet.insert(tapeFileWrittenUP.release());
        tapeFileWritten.archiveFileId = 666;
        tapeFileWritten.checksumBlob.insert(cta::checksum::ADLER32, cta::checksum::ChecksumBlob::HexToByteArray("0xDEADBEEF"));
        tapeFileWritten.vid = volInfo.vid;
        tapeFileWritten.size = archiveFileSize;
        tapeFileWritten.fSeq = fseq;
        tapeFileWritten.blockId = 0;
        tapeFileWritten.copyNb = 1;
        tapeFileWritten.diskInstance = s_diskInstance;
        tapeFileWritten.diskFileId = std::to_string(fseq);
        tapeFileWritten.diskFileOwnerUid = DISK_FILE_SOME_USER;
        tapeFileWritten.diskFileGid = DISK_FILE_SOME_GROUP;
        tapeFileWritten.storageClassName = s_storageClassName;
        tapeFileWritten.tapeDrive = "drive0";
        catalogue.filesWrittenToTape(tapeFileWrittenSet);
      }

      {
        // Create an archive file entry in the archive catalogue
        auto tapeFileWrittenUP = cta::make_unique<cta::catalogue::TapeFileWritten>();
        auto& tapeFileWritten = *tapeFileWrittenUP;
        std::set<cta::catalogue::TapeItemWrittenPointer> tapeFileWrittenSet;
        tapeFileWrittenSet.insert(tapeFileWrittenUP.release());
        tapeFileWritten.archiveFileId = 1000 + fseq;
        tapeFileWritten.checksumBlob.insert(cta::checksum::ADLER32, cta::utils::getAdler32(data, archiveFileSize));
        tapeFileWritten.vid = volInfo.vid;
        tapeFileWritten.size = archiveFileSize;
        tapeFileWritten.fSeq = fseq + 1;
        tapeFileWritten.blockId = wf.getBlockId() + 10000;
        tapeFileWritten.copyNb = 1;
        tapeFileWritten.diskInstance = s_diskInstance;
        tapeFileWritten.diskFileId = std::to_string(fseq + 1);

        tapeFileWritten.diskFileOwnerUid = DISK_FILE_SOME_USER;
        tapeFileWritten.diskFileGid = DISK_FILE_SOME_GROUP;
        tapeFileWritten.storageClassName = s_storageClassName;
        tapeFileWritten.tapeDrive = "drive0";
        catalogue.filesWrittenToTape(tapeFileWrittenSet);
      }

      // Schedule the retrieval of the file
      std::string diskInstance = "disk_instance";
      cta::common::dataStructures::RetrieveRequest rReq;
      rReq.archiveFileID = 1000 + fseq;
      rReq.requester.name = s_userName;
      rReq.requester.group = "someGroup";
      rReq.dstURL = remoteFilePaths.back();
      std::list<std::string> archiveFilePaths;
      scheduler.queueRetrieve(diskInstance, rReq, logContext);
    }
  }
  scheduler.waitSchedulerDbSubthreadsComplete();

  // 6) Report the drive's existence and put it up in the drive register.
  cta::tape::daemon::TpconfigLine driveConfig("T10D6116", "TestLogicalLibrary", "/dev/tape_T10D6116", "dummy");
  cta::common::dataStructures::DriveInfo driveInfo;
  driveInfo.driveName = driveConfig.unitName;
  driveInfo.logicalLibrary = driveConfig.logicalLibrary;
  driveInfo.host = "host";
  // We need to create the drive in the registry before being able to put it up.
  scheduler.reportDriveStatus(driveInfo, cta::common::dataStructures::MountType::NoMount, cta::common::dataStructures::DriveStatus::Down, logContext);
  cta::common::dataStructures::DesiredDriveState driveState;
  driveState.up = true;
  driveState.forceDown = false;
  scheduler.setDesiredDriveState(s_adminOnAdminHost, driveConfig.unitName, driveState, logContext);

  // 7) Create the data transfer session
  DataTransferConfig castorConf;
  castorConf.bufsz = 1024 * 1024; // 1 MB memory buffers
  castorConf.nbBufs = 10;
  castorConf.bulkRequestRecallMaxBytes = UINT64_C(100) * 1000 * 1000 * 1000;
  castorConf.bulkRequestRecallMaxFiles = 1000;
  castorConf.nbDiskThreads = 1;
  castorConf.tapeLoadTimeout = 300;
  castorConf.useEncryption = false;
  cta::log::DummyLogger dummyLog("dummy", "dummy");
  cta::mediachanger::MediaChangerFacade mc(dummyLog);
  cta::server::ProcessCap capUtils;
  castor::messages::TapeserverProxyDummy initialProcess;
  DataTransferSession sess("tapeHost", logger, mockSys,
                           driveConfig, mc, initialProcess, capUtils, castorConf, scheduler);

  // 8) Run the data transfer session
  sess.execute();

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
  cta::log::StringLogger logger("dummy", "tapeServerUnitTest", cta::log::DEBUG);
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
  auto& catalogue = getCatalogue();
  auto& scheduler = getScheduler();

  // Always use the same requester
  const cta::common::dataStructures::SecurityIdentity requester;

  // List to remember the path of each remote file so that the existence of the
  // files can be tested for at the end of the test
  std::list<std::string> remoteFilePaths;

  // 5) Create the environment for the migration to happen (library + tape)
  const std::string libraryComment = "Library comment";
  const bool libraryIsDisabled = false;
  catalogue.createLogicalLibrary(s_adminOnAdminHost, s_libraryName,
                                 libraryIsDisabled, libraryComment);
  {
    auto libraries = catalogue.getLogicalLibraries();
    ASSERT_EQ(1, libraries.size());
    ASSERT_EQ(s_libraryName, libraries.front().name);
    ASSERT_EQ(libraryComment, libraries.front().comment);
  }

  {
    auto tape = getDefaultTape();
    catalogue.createTape(s_adminOnAdminHost, tape);
  }

  int MAX_RECALLS = 50;
  int MAX_BULK_RECALLS = 31;
  std::map<size_t, std::vector<std::string>> expectedRAOFseqOrder;
  //RAO for the fake drive is a std::reverse
  // 6) Prepare files for reading by writing them to the mock system
  {
    // Label the tape
    castor::tape::tapeFile::LabelSession ls(*mockSys.fake.m_pathToDrive["/dev/nst0"],
                                            s_vid, false);
    mockSys.fake.m_pathToDrive["/dev/nst0"]->rewind();
    // And write to it
    castor::tape::tapeserver::daemon::VolumeInfo volInfo;
    volInfo.vid = s_vid;
    castor::tape::tapeFile::WriteSession ws(*mockSys.fake.m_pathToDrive["/dev/nst0"],
                                            volInfo, 0, true, false);

    // Write a few files on the virtual tape and modify the archive name space
    // so that it is in sync
    uint8_t data[1000];
    size_t archiveFileSize = sizeof(data);
    castor::tape::SCSI::Structures::zeroStruct(&data);
    int fseq;
    for (fseq = 1; fseq <= MAX_RECALLS; fseq++) {
      expectedRAOFseqOrder[fseq / MAX_BULK_RECALLS].push_back(std::to_string(fseq));
      // Create a path to a remote destination file
      std::ostringstream remoteFilePath;
      remoteFilePath << "file://" << m_tmpDir << "/test" << fseq;
      remoteFilePaths.push_back(remoteFilePath.str());

      // Create an archive file entry in the archive namespace
      auto tapeFileWrittenUP = cta::make_unique<cta::catalogue::TapeFileWritten>();
      auto& tapeFileWritten = *tapeFileWrittenUP;
      std::set<cta::catalogue::TapeItemWrittenPointer> tapeFileWrittenSet;
      tapeFileWrittenSet.insert(tapeFileWrittenUP.release());

      // Write the file to tape
      cta::MockArchiveMount mam(catalogue);
      std::unique_ptr<cta::ArchiveJob> aj(new cta::MockArchiveJob(&mam, catalogue));
      aj->tapeFile.fSeq = fseq;
      aj->archiveFile.archiveFileID = fseq;
      castor::tape::tapeFile::WriteFile wf(&ws, *aj, archiveFileSize);
      tapeFileWritten.blockId = wf.getBlockId();
      // Write the data (one block)
      wf.write(data, archiveFileSize);
      // Close the file
      wf.close();

      // Create file entry in the archive namespace
      tapeFileWritten.archiveFileId = fseq;
      tapeFileWritten.checksumBlob.insert(cta::checksum::ADLER32, cta::utils::getAdler32(data, archiveFileSize));
      tapeFileWritten.vid = volInfo.vid;
      tapeFileWritten.size = archiveFileSize;
      tapeFileWritten.fSeq = fseq;
      tapeFileWritten.copyNb = 1;
      tapeFileWritten.diskInstance = s_diskInstance;
      tapeFileWritten.diskFileId = fseq;

      tapeFileWritten.diskFileOwnerUid = DISK_FILE_SOME_USER;
      tapeFileWritten.diskFileGid = DISK_FILE_SOME_GROUP;
      tapeFileWritten.storageClassName = s_storageClassName;
      tapeFileWritten.tapeDrive = "drive0";
      catalogue.filesWrittenToTape(tapeFileWrittenSet);

      // Schedule the retrieval of the file
      std::string diskInstance = "disk_instance";
      cta::common::dataStructures::RetrieveRequest rReq;
      rReq.archiveFileID = fseq;
      rReq.requester.name = s_userName;
      rReq.requester.group = "someGroup";
      rReq.dstURL = remoteFilePaths.back();
      rReq.isVerifyOnly = false;
      std::list<std::string> archiveFilePaths;
      scheduler.queueRetrieve(diskInstance, rReq, logContext);

    }
  }
  //As RAO with the fakedrive is a std::reverse of the vector of the files given in parameter,
  //we reverse all expected fseqs vectors
  std::reverse(expectedRAOFseqOrder[0].begin(), expectedRAOFseqOrder[0].end());
  std::reverse(expectedRAOFseqOrder[1].begin(), expectedRAOFseqOrder[1].end());
  scheduler.waitSchedulerDbSubthreadsComplete();

  // 6) Report the drive's existence and put it up in the drive register.
  cta::tape::daemon::TpconfigLine driveConfig("T10D6116", "TestLogicalLibrary", "/dev/tape_T10D6116", "dummy");
  cta::common::dataStructures::DriveInfo driveInfo;
  driveInfo.driveName = driveConfig.unitName;
  driveInfo.logicalLibrary = driveConfig.rawLibrarySlot;
  driveInfo.host = "host";
  // We need to create the drive in the registry before being able to put it up.
  scheduler.reportDriveStatus(driveInfo, cta::common::dataStructures::MountType::NoMount, cta::common::dataStructures::DriveStatus::Down, logContext);
  cta::common::dataStructures::DesiredDriveState driveState;
  driveState.up = true;
  driveState.forceDown = false;
  scheduler.setDesiredDriveState(s_adminOnAdminHost, driveConfig.unitName, driveState, logContext);

  // 7) Create the data transfer session
  DataTransferConfig castorConf;
  castorConf.bufsz = 1024 * 1024; // 1 MB memory buffers
  castorConf.nbBufs = 10;
  castorConf.bulkRequestRecallMaxBytes = UINT64_C(100) * 1000 * 1000 * 1000;
  castorConf.bulkRequestRecallMaxFiles = MAX_BULK_RECALLS - 1;
  castorConf.nbDiskThreads = 1;
  castorConf.useRAO = true;
  castorConf.tapeLoadTimeout = 300;
  castorConf.useEncryption = false;
  cta::log::DummyLogger dummyLog("dummy", "dummy");
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
  for (const auto& path: remoteFilePaths) {
    struct stat statBuf;
    memset(&statBuf, 0, sizeof(statBuf));
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

  ASSERT_EQ(expectedRAOFseqOrder, getRAOFseqs(logToCheck));
}

TEST_P(DataTransferSessionTest, DataTransferSessionRAORecallLinearAlgorithm) {
  // 0) Prepare the logger for everyone
  cta::log::StringLogger logger("dummy", "tapeServerUnitTest", cta::log::DEBUG);
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
  mockSys.fake.m_pathToDrive["/dev/nst0"] = new castor::tape::tapeserver::drive::FakeNonRAODrive;

  // 4) Create the scheduler
  auto& catalogue = getCatalogue();
  auto& scheduler = getScheduler();

  // Always use the same requester
  const cta::common::dataStructures::SecurityIdentity requester;

  // List to remember the path of each remote file so that the existence of the
  // files can be tested for at the end of the test
  std::list<std::string> remoteFilePaths;

  // 5) Create the environment for the migration to happen (library + tape)
  const std::string libraryComment = "Library comment";
  const bool libraryIsDisabled = false;
  catalogue.createLogicalLibrary(s_adminOnAdminHost, s_libraryName,
                                 libraryIsDisabled, libraryComment);
  {
    auto libraries = catalogue.getLogicalLibraries();
    ASSERT_EQ(1, libraries.size());
    ASSERT_EQ(s_libraryName, libraries.front().name);
    ASSERT_EQ(libraryComment, libraries.front().comment);
  }

  {
    auto tape = getDefaultTape();
    catalogue.createTape(s_adminOnAdminHost, tape);
  }

  int MAX_RECALLS = 50;
  int MAX_BULK_RECALLS = 31;
  std::map<size_t, std::vector<std::string>> expectedRAOOrder;
  // 6) Prepare files for reading by writing them to the mock system
  {
    // Label the tape
    castor::tape::tapeFile::LabelSession ls(*mockSys.fake.m_pathToDrive["/dev/nst0"],
                                            s_vid, false);
    mockSys.fake.m_pathToDrive["/dev/nst0"]->rewind();
    // And write to it
    castor::tape::tapeserver::daemon::VolumeInfo volInfo;
    volInfo.vid = s_vid;
    castor::tape::tapeFile::WriteSession ws(*mockSys.fake.m_pathToDrive["/dev/nst0"],
                                            volInfo, 0, true, false);

    // Write a few files on the virtual tape and modify the archive name space
    // so that it is in sync
    uint8_t data[1000];
    size_t archiveFileSize = sizeof(data);
    castor::tape::SCSI::Structures::zeroStruct(&data);
    int fseq;
    //For the RAO orders we will have two rao calls : first with 30 files,
    //the second with 20 files
    for (fseq = 1; fseq <= MAX_RECALLS; fseq++) {
      expectedRAOOrder[fseq / MAX_BULK_RECALLS].push_back(std::to_string(fseq));
      // Create a path to a remote destination file
      std::ostringstream remoteFilePath;
      remoteFilePath << "file://" << m_tmpDir << "/test" << fseq;
      remoteFilePaths.push_back(remoteFilePath.str());

      // Create an archive file entry in the archive namespace
      auto tapeFileWrittenUP = cta::make_unique<cta::catalogue::TapeFileWritten>();
      auto& tapeFileWritten = *tapeFileWrittenUP;
      std::set<cta::catalogue::TapeItemWrittenPointer> tapeFileWrittenSet;
      tapeFileWrittenSet.insert(tapeFileWrittenUP.release());

      // Write the file to tape
      cta::MockArchiveMount mam(catalogue);
      std::unique_ptr<cta::ArchiveJob> aj(new cta::MockArchiveJob(&mam, catalogue));
      aj->tapeFile.fSeq = fseq;
      aj->archiveFile.archiveFileID = fseq;
      castor::tape::tapeFile::WriteFile wf(&ws, *aj, archiveFileSize);
      tapeFileWritten.blockId = wf.getBlockId();
      // Write the data (one block)
      wf.write(data, archiveFileSize);
      // Close the file
      wf.close();

      // Create file entry in the archive namespace
      tapeFileWritten.archiveFileId = fseq;
      tapeFileWritten.checksumBlob.insert(cta::checksum::ADLER32, cta::utils::getAdler32(data, archiveFileSize));
      tapeFileWritten.vid = volInfo.vid;
      tapeFileWritten.size = archiveFileSize;
      tapeFileWritten.fSeq = fseq;
      tapeFileWritten.copyNb = 1;
      tapeFileWritten.diskInstance = s_diskInstance;
      tapeFileWritten.diskFileId = fseq;

      tapeFileWritten.diskFileOwnerUid = DISK_FILE_SOME_USER;
      tapeFileWritten.diskFileGid = DISK_FILE_SOME_GROUP;
      tapeFileWritten.storageClassName = s_storageClassName;
      tapeFileWritten.tapeDrive = "drive0";
      catalogue.filesWrittenToTape(tapeFileWrittenSet);

      // Schedule the retrieval of the file
      std::string diskInstance = "disk_instance";
      cta::common::dataStructures::RetrieveRequest rReq;
      rReq.archiveFileID = fseq;
      rReq.requester.name = s_userName;
      rReq.requester.group = "someGroup";
      rReq.dstURL = remoteFilePaths.back();
      rReq.isVerifyOnly = false;
      std::list<std::string> archiveFilePaths;
      scheduler.queueRetrieve(diskInstance, rReq, logContext);
    }
  }
  scheduler.waitSchedulerDbSubthreadsComplete();

  // 6) Report the drive's existence and put it up in the drive register.
  cta::tape::daemon::TpconfigLine driveConfig("T10D6116", "TestLogicalLibrary", "/dev/tape_T10D6116", "dummy");
  cta::common::dataStructures::DriveInfo driveInfo;
  driveInfo.driveName = driveConfig.unitName;
  driveInfo.logicalLibrary = driveConfig.rawLibrarySlot;
  driveInfo.host = "host";
  // We need to create the drive in the registry before being able to put it up.
  scheduler.reportDriveStatus(driveInfo, cta::common::dataStructures::MountType::NoMount, cta::common::dataStructures::DriveStatus::Down, logContext);
  cta::common::dataStructures::DesiredDriveState driveState;
  driveState.up = true;
  driveState.forceDown = false;
  scheduler.setDesiredDriveState(s_adminOnAdminHost, driveConfig.unitName, driveState, logContext);

  // 7) Create the data transfer session
  DataTransferConfig castorConf;
  castorConf.bufsz = 1024 * 1024; // 1 MB memory buffers
  castorConf.nbBufs = 10;
  castorConf.bulkRequestRecallMaxBytes = UINT64_C(100) * 1000 * 1000 * 1000;
  castorConf.bulkRequestRecallMaxFiles = MAX_BULK_RECALLS - 1;
  castorConf.nbDiskThreads = 1;
  castorConf.useRAO = true;
  castorConf.raoLtoAlgorithm = "linear";
  castorConf.tapeLoadTimeout = 300;
  castorConf.useEncryption = false;
  cta::log::DummyLogger dummyLog("dummy", "dummy");
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
  for (const auto& path: remoteFilePaths) {
    struct stat statBuf;
    memset(&statBuf, 0, sizeof(statBuf));
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

  ASSERT_EQ(expectedRAOOrder, getRAOFseqs(logToCheck));
}

TEST_P(DataTransferSessionTest, DataTransferSessionRAORecallRAOAlgoDoesNotExistShouldApplyLinear) {
  // 0) Prepare the logger for everyone
  cta::log::StringLogger logger("dummy", "tapeServerUnitTest", cta::log::DEBUG);
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
  mockSys.fake.m_pathToDrive["/dev/nst0"] = new castor::tape::tapeserver::drive::FakeNonRAODrive;

  // 4) Create the scheduler
  auto& catalogue = getCatalogue();
  auto& scheduler = getScheduler();

  // Always use the same requester
  const cta::common::dataStructures::SecurityIdentity requester;

  // List to remember the path of each remote file so that the existence of the
  // files can be tested for at the end of the test
  std::list<std::string> remoteFilePaths;

  // 5) Create the environment for the migration to happen (library + tape)
  const std::string libraryComment = "Library comment";
  const bool libraryIsDisabled = false;
  catalogue.createLogicalLibrary(s_adminOnAdminHost, s_libraryName,
                                 libraryIsDisabled, libraryComment);
  {
    auto libraries = catalogue.getLogicalLibraries();
    ASSERT_EQ(1, libraries.size());
    ASSERT_EQ(s_libraryName, libraries.front().name);
    ASSERT_EQ(libraryComment, libraries.front().comment);
  }

  {
    auto tape = getDefaultTape();
    catalogue.createTape(s_adminOnAdminHost, tape);
  }

  int MAX_RECALLS = 50;
  int MAX_BULK_RECALLS = 31;
  std::map<size_t, std::vector<std::string>> expectedRAOOrder;
  // 6) Prepare files for reading by writing them to the mock system
  {
    // Label the tape
    castor::tape::tapeFile::LabelSession ls(*mockSys.fake.m_pathToDrive["/dev/nst0"],
                                            s_vid, false);
    mockSys.fake.m_pathToDrive["/dev/nst0"]->rewind();
    // And write to it
    castor::tape::tapeserver::daemon::VolumeInfo volInfo;
    volInfo.vid = s_vid;
    castor::tape::tapeFile::WriteSession ws(*mockSys.fake.m_pathToDrive["/dev/nst0"],
                                            volInfo, 0, true, false);

    // Write a few files on the virtual tape and modify the archive name space
    // so that it is in sync
    uint8_t data[1000];
    size_t archiveFileSize = sizeof(data);
    castor::tape::SCSI::Structures::zeroStruct(&data);
    int fseq;
    //For the RAO orders we will have two rao calls : first with 30 files,
    //the second with 20 files
    for (fseq = 1; fseq <= MAX_RECALLS; fseq++) {
      expectedRAOOrder[fseq / MAX_BULK_RECALLS].push_back(std::to_string(fseq));
      // Create a path to a remote destination file
      std::ostringstream remoteFilePath;
      remoteFilePath << "file://" << m_tmpDir << "/test" << fseq;
      remoteFilePaths.push_back(remoteFilePath.str());

      // Create an archive file entry in the archive namespace
      auto tapeFileWrittenUP = cta::make_unique<cta::catalogue::TapeFileWritten>();
      auto& tapeFileWritten = *tapeFileWrittenUP;
      std::set<cta::catalogue::TapeItemWrittenPointer> tapeFileWrittenSet;
      tapeFileWrittenSet.insert(tapeFileWrittenUP.release());

      // Write the file to tape
      cta::MockArchiveMount mam(catalogue);
      std::unique_ptr<cta::ArchiveJob> aj(new cta::MockArchiveJob(&mam, catalogue));
      aj->tapeFile.fSeq = fseq;
      aj->archiveFile.archiveFileID = fseq;
      castor::tape::tapeFile::WriteFile wf(&ws, *aj, archiveFileSize);
      tapeFileWritten.blockId = wf.getBlockId();
      // Write the data (one block)
      wf.write(data, archiveFileSize);
      // Close the file
      wf.close();

      // Create file entry in the archive namespace
      tapeFileWritten.archiveFileId = fseq;
      tapeFileWritten.checksumBlob.insert(cta::checksum::ADLER32, cta::utils::getAdler32(data, archiveFileSize));
      tapeFileWritten.vid = volInfo.vid;
      tapeFileWritten.size = archiveFileSize;
      tapeFileWritten.fSeq = fseq;
      tapeFileWritten.copyNb = 1;
      tapeFileWritten.diskInstance = s_diskInstance;
      tapeFileWritten.diskFileId = fseq;

      tapeFileWritten.diskFileOwnerUid = DISK_FILE_SOME_USER;
      tapeFileWritten.diskFileGid = DISK_FILE_SOME_GROUP;
      tapeFileWritten.storageClassName = s_storageClassName;
      tapeFileWritten.tapeDrive = "drive0";
      catalogue.filesWrittenToTape(tapeFileWrittenSet);

      // Schedule the retrieval of the file
      std::string diskInstance = "disk_instance";
      cta::common::dataStructures::RetrieveRequest rReq;
      rReq.archiveFileID = fseq;
      rReq.requester.name = s_userName;
      rReq.requester.group = "someGroup";
      rReq.dstURL = remoteFilePaths.back();
      rReq.isVerifyOnly = false;
      std::list<std::string> archiveFilePaths;
      scheduler.queueRetrieve(diskInstance, rReq, logContext);
    }
  }
  scheduler.waitSchedulerDbSubthreadsComplete();

  // 6) Report the drive's existence and put it up in the drive register.
  cta::tape::daemon::TpconfigLine driveConfig("T10D6116", "TestLogicalLibrary", "/dev/tape_T10D6116", "dummy");
  cta::common::dataStructures::DriveInfo driveInfo;
  driveInfo.driveName = driveConfig.unitName;
  driveInfo.logicalLibrary = driveConfig.rawLibrarySlot;
  driveInfo.host = "host";
  // We need to create the drive in the registry before being able to put it up.
  scheduler.reportDriveStatus(driveInfo, cta::common::dataStructures::MountType::NoMount, cta::common::dataStructures::DriveStatus::Down, logContext);
  cta::common::dataStructures::DesiredDriveState driveState;
  driveState.up = true;
  driveState.forceDown = false;
  scheduler.setDesiredDriveState(s_adminOnAdminHost, driveConfig.unitName, driveState, logContext);

  // 7) Create the data transfer session
  DataTransferConfig castorConf;
  castorConf.bufsz = 1024 * 1024; // 1 MB memory buffers
  castorConf.nbBufs = 10;
  castorConf.bulkRequestRecallMaxBytes = UINT64_C(100) * 1000 * 1000 * 1000;
  castorConf.bulkRequestRecallMaxFiles = MAX_BULK_RECALLS - 1;
  castorConf.nbDiskThreads = 1;
  castorConf.useRAO = true;
  castorConf.tapeLoadTimeout = 300;
  castorConf.raoLtoAlgorithm = "DOES_NOT_EXIST";
  castorConf.useEncryption = false;

  cta::log::DummyLogger dummyLog("dummy", "dummy");
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
  for (const auto& path: remoteFilePaths) {
    struct stat statBuf;
    memset(&statBuf, 0, sizeof(statBuf));
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

  ASSERT_NE(std::string::npos,
            logToCheck.find("In RAOAlgorithmFactoryFactory::createAlgorithmFactory(), unable to determine the RAO algorithm to use"));

  ASSERT_EQ(expectedRAOOrder, getRAOFseqs(logToCheck));
}

TEST_P(DataTransferSessionTest, DataTransferSessionRAORecallSLTFRAOAlgorithm) {
  // 0) Prepare the logger for everyone
  cta::log::StringLogger logger("dummy", "tapeServerUnitTest", cta::log::DEBUG);
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
  mockSys.fake.m_pathToDrive["/dev/nst0"] = new castor::tape::tapeserver::drive::FakeNonRAODrive;

  // 4) Create the scheduler
  auto& catalogue = getCatalogue();
  auto& scheduler = getScheduler();

  // Always use the same requester
  const cta::common::dataStructures::SecurityIdentity requester;

  // List to remember the path of each remote file so that the existence of the
  // files can be tested for at the end of the test
  std::list<std::string> remoteFilePaths;

  // 5) Create the environment for the migration to happen (library + tape)
  const std::string libraryComment = "Library comment";
  const bool libraryIsDisabled = false;
  catalogue.createLogicalLibrary(s_adminOnAdminHost, s_libraryName,
                                 libraryIsDisabled, libraryComment);
  {
    auto libraries = catalogue.getLogicalLibraries();
    ASSERT_EQ(1, libraries.size());
    ASSERT_EQ(s_libraryName, libraries.front().name);
    ASSERT_EQ(libraryComment, libraries.front().comment);
  }

  {
    auto tape = getDefaultTape();
    catalogue.createTape(s_adminOnAdminHost, tape);
  }

  int MAX_RECALLS = 30;
  int MAX_BULK_RECALLS = 20;
  std::map<size_t, std::vector<std::string>> expectedRAOOrder;
  // 6) Prepare files for reading by writing them to the mock system
  {
    // Label the tape
    castor::tape::tapeFile::LabelSession ls(*mockSys.fake.m_pathToDrive["/dev/nst0"],
                                            s_vid, false);
    mockSys.fake.m_pathToDrive["/dev/nst0"]->rewind();
    // And write to it
    castor::tape::tapeserver::daemon::VolumeInfo volInfo;
    volInfo.vid = s_vid;
    castor::tape::tapeFile::WriteSession ws(*mockSys.fake.m_pathToDrive["/dev/nst0"],
                                            volInfo, 0, true, false);

    // Write a few files on the virtual tape and modify the archive name space
    // so that it is in sync
    uint8_t data[1000];
    size_t archiveFileSize = sizeof(data);
    castor::tape::SCSI::Structures::zeroStruct(&data);
    int fseq;
    //For the RAO orders we will have two rao calls : first with 30 files,
    //the second with 20 files
    for (fseq = 1; fseq <= MAX_RECALLS; fseq++) {
      expectedRAOOrder[fseq / MAX_BULK_RECALLS].push_back(std::to_string(fseq));
      // Create a path to a remote destination file
      std::ostringstream remoteFilePath;
      remoteFilePath << "file://" << m_tmpDir << "/test" << fseq;
      remoteFilePaths.push_back(remoteFilePath.str());

      // Create an archive file entry in the archive namespace
      auto tapeFileWrittenUP = cta::make_unique<cta::catalogue::TapeFileWritten>();
      auto& tapeFileWritten = *tapeFileWrittenUP;
      std::set<cta::catalogue::TapeItemWrittenPointer> tapeFileWrittenSet;
      tapeFileWrittenSet.insert(tapeFileWrittenUP.release());

      // Write the file to tape
      cta::MockArchiveMount mam(catalogue);
      std::unique_ptr<cta::ArchiveJob> aj(new cta::MockArchiveJob(&mam, catalogue));
      aj->tapeFile.fSeq = fseq;
      aj->archiveFile.archiveFileID = fseq;
      castor::tape::tapeFile::WriteFile wf(&ws, *aj, archiveFileSize);
      tapeFileWritten.blockId = wf.getBlockId();
      // Write the data (one block)
      wf.write(data, archiveFileSize);
      // Close the file
      wf.close();

      // Create file entry in the archive namespace
      tapeFileWritten.archiveFileId = fseq;
      tapeFileWritten.checksumBlob.insert(cta::checksum::ADLER32, cta::utils::getAdler32(data, archiveFileSize));
      tapeFileWritten.vid = volInfo.vid;
      tapeFileWritten.size = archiveFileSize;
      tapeFileWritten.fSeq = fseq;
      tapeFileWritten.copyNb = 1;
      tapeFileWritten.diskInstance = s_diskInstance;
      tapeFileWritten.diskFileId = fseq;

      tapeFileWritten.diskFileOwnerUid = DISK_FILE_SOME_USER;
      tapeFileWritten.diskFileGid = DISK_FILE_SOME_GROUP;
      tapeFileWritten.storageClassName = s_storageClassName;
      tapeFileWritten.tapeDrive = "drive0";
      catalogue.filesWrittenToTape(tapeFileWrittenSet);

      // Schedule the retrieval of the file
      std::string diskInstance = "disk_instance";
      cta::common::dataStructures::RetrieveRequest rReq;
      rReq.archiveFileID = fseq;
      rReq.requester.name = s_userName;
      rReq.requester.group = "someGroup";
      rReq.dstURL = remoteFilePaths.back();
      rReq.isVerifyOnly = false;
      std::list<std::string> archiveFilePaths;
      scheduler.queueRetrieve(diskInstance, rReq, logContext);
    }
  }
  scheduler.waitSchedulerDbSubthreadsComplete();

  // 6) Report the drive's existence and put it up in the drive register.
  cta::tape::daemon::TpconfigLine driveConfig("T10D6116", "TestLogicalLibrary", "/dev/tape_T10D6116", "dummy");
  cta::common::dataStructures::DriveInfo driveInfo;
  driveInfo.driveName = driveConfig.unitName;
  driveInfo.logicalLibrary = driveConfig.rawLibrarySlot;
  driveInfo.host = "host";
  // We need to create the drive in the registry before being able to put it up.
  scheduler.reportDriveStatus(driveInfo, cta::common::dataStructures::MountType::NoMount, cta::common::dataStructures::DriveStatus::Down, logContext);
  cta::common::dataStructures::DesiredDriveState driveState;
  driveState.up = true;
  driveState.forceDown = false;
  scheduler.setDesiredDriveState(s_adminOnAdminHost, driveConfig.unitName, driveState, logContext);

  // 7) Create the data transfer session
  DataTransferConfig castorConf;
  castorConf.bufsz = 1024 * 1024; // 1 MB memory buffers
  castorConf.nbBufs = 10;
  castorConf.bulkRequestRecallMaxBytes = UINT64_C(100) * 1000 * 1000 * 1000;
  castorConf.bulkRequestRecallMaxFiles = MAX_BULK_RECALLS - 1;
  castorConf.nbDiskThreads = 1;
  castorConf.useRAO = true;
  castorConf.tapeLoadTimeout = 300;
  castorConf.raoLtoAlgorithm = "sltf";
  castorConf.raoLtoAlgorithmOptions = "cost_heuristic_name:cta";
  castorConf.useEncryption = false;
  cta::log::DummyLogger dummyLog("dummy", "dummy");
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
  for (const auto& path: remoteFilePaths) {
    struct stat statBuf;
    memset(&statBuf, 0, sizeof(statBuf));
    const int statRc = stat(path.substr(7).c_str(), &statBuf); //remove the "file://" for stat-ing
    ASSERT_EQ(0, statRc);
    ASSERT_EQ(1000, statBuf.st_size); //same size of data
  }

  // 10) Check logs
  std::string logToCheck = logger.getLog();

  ASSERT_NE(std::string::npos, logToCheck.find("firmwareVersion=\"123A\" serialNumber=\"123456\" "
                                               "mountTotalCorrectedReadErrors=\"5\" mountTotalReadBytesProcessed=\"4096\" "
                                               "mountTotalUncorrectedReadErrors=\"1\" mountTotalNonMediumErrorCounts=\"2\""));
  ASSERT_NE(std::string::npos, logToCheck.find("firmwareVersion=\"123A\" serialNumber=\"123456\" lifetimeMediumEfficiencyPrct=\"100\" "
                                               "mountReadEfficiencyPrct=\"100\" mountWriteEfficiencyPrct=\"100\" "
                                               "mountReadTransients=\"10\" "
                                               "mountServoTemps=\"10\" mountServoTransients=\"5\" mountTemps=\"100\" "
                                               "mountTotalReadRetries=\"25\" mountTotalWriteRetries=\"25\" mountWriteTransients=\"10\""));

  ASSERT_NE(std::string::npos, logToCheck.find("In RAOManager::queryRAO(), successfully performed RAO."));
  ASSERT_NE(std::string::npos, logToCheck.find("executedRAOAlgorithm=\"sltf\""));

  ASSERT_EQ(expectedRAOOrder, getRAOFseqs(logToCheck));
}

TEST_P(DataTransferSessionTest, DataTransferSessionNoSuchDrive) {

  // 0) Prepare the logger for everyone
  cta::log::StringLogger logger("dummy", "tapeServerUnitTest", cta::log::DEBUG);
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
  auto& catalogue = getCatalogue();
  auto& scheduler = getScheduler();

  // Always use the same requester
  const cta::common::dataStructures::SecurityIdentity requester;

  // List to remember the path of each remote file so that the existence of the
  // files can be tested for at the end of the test
  std::list<std::string> remoteFilePaths;

  // 5) Create the environment for the migration to happen (library + tape)
  const std::string libraryComment = "Library comment";
  const bool libraryIsDisabled = false;
  catalogue.createLogicalLibrary(s_adminOnAdminHost, s_libraryName,
                                 libraryIsDisabled, libraryComment);
  {
    auto libraries = catalogue.getLogicalLibraries();
    ASSERT_EQ(1, libraries.size());
    ASSERT_EQ(s_libraryName, libraries.front().name);
    ASSERT_EQ(libraryComment, libraries.front().comment);
  }

  {
    auto tape = getDefaultTape();
    catalogue.createTape(s_adminOnAdminHost, tape);
  }

  // 6) Prepare files for reading by writing them to the mock system
  {
    // Label the tape
    castor::tape::tapeFile::LabelSession ls(*mockSys.fake.m_pathToDrive["/dev/nst0"],
                                            s_vid, false);
    mockSys.fake.m_pathToDrive["/dev/nst0"]->rewind();
    // And write to it
    castor::tape::tapeserver::daemon::VolumeInfo volInfo;
    volInfo.vid = s_vid;
    castor::tape::tapeFile::WriteSession ws(*mockSys.fake.m_pathToDrive["/dev/nst0"],
                                            volInfo, 0, true, false);

    // Write a few files on the virtual tape and modify the archive name space
    // so that it is in sync
    uint8_t data[1000];
    size_t archiveFileSize = sizeof(data);
    castor::tape::SCSI::Structures::zeroStruct(&data);
    for (int fseq = 1; fseq <= 10; fseq++) {
      // Create a path to a remote destination file
      std::ostringstream remoteFilePath;
      remoteFilePath << "file://" << m_tmpDir << "/test" << fseq;
      remoteFilePaths.push_back(remoteFilePath.str());

      // Create an archive file entry in the archive namespace
      auto tapeFileWrittenUP = cta::make_unique<cta::catalogue::TapeFileWritten>();
      auto& tapeFileWritten = *tapeFileWrittenUP;
      std::set<cta::catalogue::TapeItemWrittenPointer> tapeFileWrittenSet;
      tapeFileWrittenSet.insert(tapeFileWrittenUP.release());

      // Write the file to tape
      cta::MockArchiveMount mam(catalogue);
      std::unique_ptr<cta::ArchiveJob> aj(new cta::MockArchiveJob(&mam, catalogue));
      aj->tapeFile.fSeq = fseq;
      aj->archiveFile.archiveFileID = fseq;
      castor::tape::tapeFile::WriteFile wf(&ws, *aj, archiveFileSize);
      tapeFileWritten.blockId = wf.getBlockId();
      // Write the data (one block)
      wf.write(data, archiveFileSize);
      // Close the file
      wf.close();

      // Create file entry in the archive namespace
      tapeFileWritten.archiveFileId = fseq;
      tapeFileWritten.checksumBlob.insert(cta::checksum::ADLER32, cta::utils::getAdler32(data, archiveFileSize));
      tapeFileWritten.vid = volInfo.vid;
      tapeFileWritten.size = archiveFileSize;
      tapeFileWritten.fSeq = fseq;
      tapeFileWritten.copyNb = 1;
      tapeFileWritten.diskInstance = s_diskInstance;
      tapeFileWritten.diskFileId = fseq;

      tapeFileWritten.diskFileOwnerUid = DISK_FILE_SOME_USER;
      tapeFileWritten.diskFileGid = DISK_FILE_SOME_GROUP;
      tapeFileWritten.storageClassName = s_storageClassName;
      tapeFileWritten.tapeDrive = "drive0";
      catalogue.filesWrittenToTape(tapeFileWrittenSet);

      // Schedule the retrieval of the file
      std::string diskInstance = "disk_instance";
      cta::common::dataStructures::RetrieveRequest rReq;
      rReq.archiveFileID = fseq;
      rReq.requester.name = s_userName;
      rReq.requester.group = "someGroup";
      rReq.dstURL = remoteFilePaths.back();
      std::list<std::string> archiveFilePaths;
      scheduler.queueRetrieve(diskInstance, rReq, logContext);
    }
  }
  scheduler.waitSchedulerDbSubthreadsComplete();

  // 7) Report the drive's existence and put it up in the drive register.
  cta::tape::daemon::TpconfigLine driveConfig("T10D6116", "TestLogicalLibrary", "/dev/noSuchDrive", "dummy");
  cta::common::dataStructures::DriveInfo driveInfo;
  driveInfo.driveName = driveConfig.unitName;
  driveInfo.logicalLibrary = driveConfig.logicalLibrary;
  driveInfo.host = "host";
  // We need to create the drive in the registry before being able to put it up.
  scheduler.reportDriveStatus(driveInfo, cta::common::dataStructures::MountType::NoMount, cta::common::dataStructures::DriveStatus::Down, logContext);
  cta::common::dataStructures::DesiredDriveState driveState;
  driveState.up = true;
  driveState.forceDown = false;
  scheduler.setDesiredDriveState(s_adminOnAdminHost, driveConfig.unitName, driveState, logContext);

  // 8) Create the data transfer session
  DataTransferConfig castorConf;
  castorConf.bufsz = 1024;
  castorConf.tapeLoadTimeout = 300;
  castorConf.nbBufs = 10;
  castorConf.useEncryption = false;
  cta::log::DummyLogger dummyLog("dummy", "dummy");
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
  cta::log::StringLogger logger("dummy", "tapeServerUnitTest", cta::log::DEBUG);
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
  const bool failOnMount = true;
  mockSys.fake.m_pathToDrive["/dev/nst0"] = new castor::tape::tapeserver::drive::FakeDrive(failOnMount);

  // 4) Create the scheduler
  auto& catalogue = getCatalogue();
  auto& scheduler = getScheduler();

  // Always use the same requester
  const cta::common::dataStructures::SecurityIdentity requester;

  // List to remember the path of each remote file so that the existence of the
  // files can be tested for at the end of the test
  std::list<std::string> remoteFilePaths;

  // 5) Create the environment for the migration to happen (library + tape)
  const std::string libraryComment = "Library comment";
  const bool libraryIsDisabled = false;
  catalogue.createLogicalLibrary(s_adminOnAdminHost, s_libraryName,
                                 libraryIsDisabled, libraryComment);
  {
    auto libraries = catalogue.getLogicalLibraries();
    ASSERT_EQ(1, libraries.size());
    ASSERT_EQ(s_libraryName, libraries.front().name);
    ASSERT_EQ(libraryComment, libraries.front().comment);
  }

  {
    auto tape = getDefaultTape();
    catalogue.createTape(s_adminOnAdminHost, tape);
  }

  // 6) Prepare files for reading by writing them to the mock system
  {
    // Label the tape
    castor::tape::tapeFile::LabelSession ls(*mockSys.fake.m_pathToDrive["/dev/nst0"],
                                            s_vid, false);
    mockSys.fake.m_pathToDrive["/dev/nst0"]->rewind();
    // And write to it
    castor::tape::tapeserver::daemon::VolumeInfo volInfo;
    volInfo.vid = s_vid;
    castor::tape::tapeFile::WriteSession ws(*mockSys.fake.m_pathToDrive["/dev/nst0"],
                                            volInfo, 0, true, false);

    // Write a few files on the virtual tape and modify the archive name space
    // so that it is in sync
    uint8_t data[1000];
    size_t archiveFileSize = sizeof(data);
    castor::tape::SCSI::Structures::zeroStruct(&data);
    for (int fseq = 1; fseq <= 10; fseq++) {
      // Create a path to a remote destination file
      std::ostringstream remoteFilePath;
      remoteFilePath << "file://" << m_tmpDir << "/test" << fseq;
      remoteFilePaths.push_back(remoteFilePath.str());

      // Create an archive file entry in the archive namespace
      auto tapeFileWrittenUP = cta::make_unique<cta::catalogue::TapeFileWritten>();
      auto& tapeFileWritten = *tapeFileWrittenUP;
      std::set<cta::catalogue::TapeItemWrittenPointer> tapeFileWrittenSet;
      tapeFileWrittenSet.insert(tapeFileWrittenUP.release());

      // Write the file to tape
      cta::MockArchiveMount mam(catalogue);
      std::unique_ptr<cta::ArchiveJob> aj(new cta::MockArchiveJob(&mam, catalogue));
      aj->tapeFile.fSeq = fseq;
      aj->archiveFile.archiveFileID = fseq;
      castor::tape::tapeFile::WriteFile wf(&ws, *aj, archiveFileSize);
      tapeFileWritten.blockId = wf.getBlockId();
      // Write the data (one block)
      wf.write(data, archiveFileSize);
      // Close the file
      wf.close();

      // Create file entry in the archive namespace
      tapeFileWritten.archiveFileId = fseq;
      tapeFileWritten.checksumBlob.insert(cta::checksum::ADLER32, cta::utils::getAdler32(data, archiveFileSize));
      tapeFileWritten.vid = volInfo.vid;
      tapeFileWritten.size = archiveFileSize;
      tapeFileWritten.fSeq = fseq;
      tapeFileWritten.copyNb = 1;
      tapeFileWritten.diskInstance = s_diskInstance;
      tapeFileWritten.diskFileId = fseq;

      tapeFileWritten.diskFileOwnerUid = DISK_FILE_SOME_USER;
      tapeFileWritten.diskFileGid = DISK_FILE_SOME_GROUP;
      tapeFileWritten.storageClassName = s_storageClassName;
      tapeFileWritten.tapeDrive = "drive0";
      catalogue.filesWrittenToTape(tapeFileWrittenSet);

      // Schedule the retrieval of the file
      std::string diskInstance = "disk_instance";
      cta::common::dataStructures::RetrieveRequest rReq;
      rReq.archiveFileID = fseq;
      rReq.requester.name = s_userName;
      rReq.requester.group = "someGroup";
      rReq.dstURL = remoteFilePaths.back();
      std::list<std::string> archiveFilePaths;
      scheduler.queueRetrieve(diskInstance, rReq, logContext);
    }
  }
  scheduler.waitSchedulerDbSubthreadsComplete();

  // 7) Report the drive's existence and put it up in the drive register.
  cta::tape::daemon::TpconfigLine driveConfig("T10D6116", "TestLogicalLibrary", "/dev/tape_T10D6116", "dummy");
  cta::common::dataStructures::DriveInfo driveInfo;
  driveInfo.driveName = driveConfig.unitName;
  driveInfo.logicalLibrary = driveConfig.logicalLibrary;
  driveInfo.host = "host";
  // We need to create the drive in the registry before being able to put it up.
  scheduler.reportDriveStatus(driveInfo, cta::common::dataStructures::MountType::NoMount, cta::common::dataStructures::DriveStatus::Down, logContext);
  cta::common::dataStructures::DesiredDriveState driveState;
  driveState.up = true;
  driveState.forceDown = false;
  scheduler.setDesiredDriveState(s_adminOnAdminHost, driveConfig.unitName, driveState, logContext);

  // 8) Create the data transfer session
  DataTransferConfig castorConf;
  castorConf.bufsz = 1024 * 1024; // 1 MB memory buffers
  castorConf.nbBufs = 10;
  castorConf.bulkRequestRecallMaxBytes = UINT64_C(100) * 1000 * 1000 * 1000;
  castorConf.bulkRequestRecallMaxFiles = 1000;
  castorConf.nbDiskThreads = 3;
  castorConf.tapeLoadTimeout = 300;
  castorConf.useEncryption = false;
  cta::log::DummyLogger dummyLog("dummy", "dummy");
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
  cta::log::StringLogger logger("dummy", "tapeServerUnitTest", cta::log::DEBUG);
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
  auto& catalogue = getCatalogue();
  auto& scheduler = getScheduler();

  // Always use the same requester
  const cta::common::dataStructures::SecurityIdentity requester("user", "group");

  // List to remember the path of each remote file so that the existence of the
  // files can be tested for at the end of the test
  std::list<std::string> remoteFilePaths;

  // 5) Create the environment for the migration to happen (library + tape)
  const std::string libraryComment = "Library comment";
  const bool libraryIsDisabled = false;
  catalogue.createLogicalLibrary(s_adminOnAdminHost, s_libraryName,
                                 libraryIsDisabled, libraryComment);
  {
    auto libraries = catalogue.getLogicalLibraries();
    ASSERT_EQ(1, libraries.size());
    ASSERT_EQ(s_libraryName, libraries.front().name);
    ASSERT_EQ(libraryComment, libraries.front().comment);
  }

  {
    auto tape = getDefaultTape();
    catalogue.createTape(s_adminOnAdminHost, tape);
  }

  // Create the mount criteria
  auto mountPolicy = getImmediateMountMountPolicy();
  catalogue.createMountPolicy(requester, mountPolicy);
  std::string mountPolicyName = mountPolicy.name;
  catalogue.createRequesterMountRule(requester, mountPolicyName, s_diskInstance, requester.username, "Rule comment");

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
    catalogue.tapeLabelled(s_vid, "T10D6116");
    mockSys.fake.m_pathToDrive["/dev/nst0"]->rewind();

    // Create the files and schedule the archivals
    for (int fseq = 1; fseq <= 10; fseq++) {
      // Create a source file.
      sourceFiles.emplace_back(cta::make_unique<unitTests::TempFile>());
      sourceFiles.back()->randomFill(1000);
      remoteFilePaths.push_back(sourceFiles.back()->path());
      // Schedule the archival of the file
      cta::common::dataStructures::ArchiveRequest ar;
      ar.checksumBlob.insert(cta::checksum::ADLER32, sourceFiles.back()->adler32());
      ar.storageClass = s_storageClassName;
      ar.srcURL = std::string("file://") + sourceFiles.back()->path();
      ar.requester.name = requester.username;
      ar.requester.group = "group";
      ar.fileSize = 1000;
      ar.diskFileID = std::to_string(fseq);
      ar.diskFileInfo.path = "y";
      ar.diskFileInfo.owner_uid = DISK_FILE_OWNER_UID;
      ar.diskFileInfo.gid = DISK_FILE_GID;
      const auto archiveFileId = scheduler.checkAndGetNextArchiveFileId(s_diskInstance, ar.storageClass, ar.requester, logContext);
      archiveFileIds.push_back(archiveFileId);
      scheduler.queueArchiveWithGivenId(archiveFileId, s_diskInstance, ar, logContext);
    }
  }
  scheduler.waitSchedulerDbSubthreadsComplete();
  // Report the drive's existence and put it up in the drive register.
  cta::tape::daemon::TpconfigLine driveConfig("T10D6116", "TestLogicalLibrary", "/dev/tape_T10D6116", "dummy");
  cta::common::dataStructures::DriveInfo driveInfo;
  driveInfo.driveName = driveConfig.unitName;
  driveInfo.logicalLibrary = driveConfig.logicalLibrary;
  driveInfo.host = "host";
  // We need to create the drive in the registry before being able to put it up.
  scheduler.reportDriveStatus(driveInfo, cta::common::dataStructures::MountType::NoMount, cta::common::dataStructures::DriveStatus::Down, logContext);
  cta::common::dataStructures::DesiredDriveState driveState;
  driveState.up = true;
  driveState.forceDown = false;
  scheduler.setDesiredDriveState(s_adminOnAdminHost, driveConfig.unitName, driveState, logContext);

  // Create the data transfer session
  DataTransferConfig castorConf;
  castorConf.bufsz = 1024 * 1024; // 1 MB memory buffers
  castorConf.nbBufs = 10;
  castorConf.bulkRequestRecallMaxBytes = UINT64_C(100) * 1000 * 1000 * 1000;
  castorConf.bulkRequestRecallMaxFiles = 1000;
  castorConf.bulkRequestMigrationMaxBytes = UINT64_C(100) * 1000 * 1000 * 1000;
  castorConf.bulkRequestMigrationMaxFiles = 1000;
  castorConf.nbDiskThreads = 1;
  castorConf.tapeLoadTimeout = 300;
  castorConf.useEncryption = false;
  cta::log::DummyLogger dummyLog("dummy", "dummy");
  cta::mediachanger::MediaChangerFacade mc(dummyLog);
  cta::server::ProcessCap capUtils;
  castor::messages::TapeserverProxyDummy initialProcess;
  DataTransferSession sess("tapeHost", logger, mockSys, driveConfig, mc, initialProcess, capUtils, castorConf, scheduler);
  sess.execute();
  std::string logToCheck = logger.getLog();
  logToCheck += "";
  ASSERT_EQ(s_vid, sess.getVid());
  auto afiiter = archiveFileIds.begin();
  for (const auto& sf: sourceFiles) {
    auto afi = *(afiiter++);
    auto afs = catalogue.getArchiveFileById(afi);
    ASSERT_EQ(1, afs.tapeFiles.size());
    cta::checksum::ChecksumBlob checksumBlob;
    checksumBlob.insert(cta::checksum::ADLER32, sf->adler32());
    ASSERT_EQ(afs.checksumBlob, checksumBlob);
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

TEST_P(DataTransferSessionTest, DataTransferSessionWrongFileSizeMigration) {
  // This test is the same as DataTransferSessionGooddayMigration, with
  // wrong file size on the first file migrated. As a fix for #1096, all files
  // except the first should be written to tape and the catalogue

  // 0) Prepare the logger for everyone
  cta::log::StringLogger logger("dummy","tapeServerUnitTest",cta::log::DEBUG);
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

  // List to remember the path of each remote file so that the existence of the
  // files can be tested for at the end of the test
  std::list<std::string> remoteFilePaths;

  // 5) Create the environment for the migration to happen (library + tape)
    const std::string libraryComment = "Library comment";
  const bool libraryIsDisabled = false;
  catalogue.createLogicalLibrary(s_adminOnAdminHost, s_libraryName,
    libraryIsDisabled, libraryComment);
  {
    auto libraries = catalogue.getLogicalLibraries();
    ASSERT_EQ(1, libraries.size());
    ASSERT_EQ(s_libraryName, libraries.front().name);
    ASSERT_EQ(libraryComment, libraries.front().comment);
  }

  {
    auto tape = getDefaultTape();
    catalogue.createTape(s_adminOnAdminHost, tape);
  }

  // Create the mount criteria
  auto mountPolicy = getImmediateMountMountPolicy();
  catalogue.createMountPolicy(requester, mountPolicy);
  std::string mountPolicyName = mountPolicy.name;
  catalogue.createRequesterMountRule(requester, mountPolicyName, s_diskInstance, requester.username, "Rule comment");

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
    catalogue.tapeLabelled(s_vid, "T10D6116");
    mockSys.fake.m_pathToDrive["/dev/nst0"]->rewind();

    // Create the files and schedule the archivals

    //First a file with wrong checksum
    {
      int fseq = 1;
      sourceFiles.emplace_back(cta::make_unique<unitTests::TempFile>());
      sourceFiles.back()->randomFill(1000);
      remoteFilePaths.push_back(sourceFiles.back()->path());
      // Schedule the archival of the file
      cta::common::dataStructures::ArchiveRequest ar;
      ar.checksumBlob.insert(cta::checksum::ADLER32, sourceFiles.back()->adler32());
      ar.storageClass=s_storageClassName;
      ar.srcURL=std::string("file://") + sourceFiles.back()->path();
      ar.requester.name = requester.username;
      ar.requester.group = "group";
      ar.fileSize = 900;  // Wrong file size
      ar.diskFileID = std::to_string(fseq);
      ar.diskFileInfo.path = "y";
      ar.diskFileInfo.owner_uid = DISK_FILE_OWNER_UID;
      ar.diskFileInfo.gid = DISK_FILE_GID;
      const auto archiveFileId = scheduler.checkAndGetNextArchiveFileId(s_diskInstance, ar.storageClass, ar.requester, logContext);
      archiveFileIds.push_back(archiveFileId);
      scheduler.queueArchiveWithGivenId(archiveFileId,s_diskInstance,ar,logContext);
    }
    for(int fseq=2; fseq <= 10 ; fseq ++) {
      // Create a source file.
      sourceFiles.emplace_back(cta::make_unique<unitTests::TempFile>());
      sourceFiles.back()->randomFill(1000);
      remoteFilePaths.push_back(sourceFiles.back()->path());
      // Schedule the archival of the file
      cta::common::dataStructures::ArchiveRequest ar;
      ar.checksumBlob.insert(cta::checksum::ADLER32, sourceFiles.back()->adler32());
      ar.storageClass=s_storageClassName;
      ar.srcURL=std::string("file://") + sourceFiles.back()->path();
      ar.requester.name = requester.username;
      ar.requester.group = "group";
      ar.fileSize = 1000;
      ar.diskFileID = std::to_string(fseq);
      ar.diskFileInfo.path = "y";
      ar.diskFileInfo.owner_uid = DISK_FILE_OWNER_UID;
      ar.diskFileInfo.gid = DISK_FILE_GID;
      const auto archiveFileId = scheduler.checkAndGetNextArchiveFileId(s_diskInstance, ar.storageClass, ar.requester, logContext);
      archiveFileIds.push_back(archiveFileId);
      scheduler.queueArchiveWithGivenId(archiveFileId,s_diskInstance,ar,logContext);
    }
  }
  scheduler.waitSchedulerDbSubthreadsComplete();
  // Report the drive's existence and put it up in the drive register.
  cta::tape::daemon::TpconfigLine driveConfig("T10D6116", "TestLogicalLibrary", "/dev/tape_T10D6116", "dummy");
  cta::common::dataStructures::DriveInfo driveInfo;
  driveInfo.driveName=driveConfig.unitName;
  driveInfo.logicalLibrary=driveConfig.logicalLibrary;
  driveInfo.host="host";
  // We need to create the drive in the registry before being able to put it up.
  scheduler.reportDriveStatus(driveInfo, cta::common::dataStructures::MountType::NoMount, cta::common::dataStructures::DriveStatus::Down, logContext);
  cta::common::dataStructures::DesiredDriveState driveState;
  driveState.up = true;
  driveState.forceDown = false;
  scheduler.setDesiredDriveState(s_adminOnAdminHost, driveConfig.unitName, driveState, logContext);

  // Create the data transfer session
  DataTransferConfig castorConf;
  castorConf.bufsz = 1024*1024; // 1 MB memory buffers
  castorConf.nbBufs = 10;
  castorConf.bulkRequestRecallMaxBytes = UINT64_C(100)*1000*1000*1000;
  castorConf.bulkRequestRecallMaxFiles = 1000;
  castorConf.bulkRequestMigrationMaxBytes = UINT64_C(100)*1000*1000*1000;
  castorConf.bulkRequestMigrationMaxFiles = 1000;
  castorConf.nbDiskThreads = 1;
  castorConf.tapeLoadTimeout = 300;
  castorConf.useEncryption = false;
  cta::log::DummyLogger dummyLog("dummy", "dummy");
  cta::mediachanger::MediaChangerFacade mc(dummyLog);
  cta::server::ProcessCap capUtils;
  castor::messages::TapeserverProxyDummy initialProcess;
  DataTransferSession sess("tapeHost", logger, mockSys, driveConfig, mc, initialProcess, capUtils, castorConf, scheduler);
  sess.execute();
  std::string logToCheck = logger.getLog();
  logToCheck += "";
  ASSERT_EQ(s_vid, sess.getVid());
  
  auto afiiter = archiveFileIds.begin();
  // First file failed migration, rest made it to the catalogue (fixe for cta/CTA#1096)
  for(auto & sf: sourceFiles) {
    auto afi = *(afiiter++);
    if (afi == 1) {
      ASSERT_THROW(catalogue.getArchiveFileById(afi), cta::exception::Exception);
    } else {
      auto afs = catalogue.getArchiveFileById(afi);
      ASSERT_EQ(1, afs.tapeFiles.size());
      cta::checksum::ChecksumBlob checksumBlob;
      checksumBlob.insert(cta::checksum::ADLER32, sf->adler32());
      ASSERT_EQ(afs.checksumBlob, checksumBlob);
      ASSERT_EQ(1000, afs.fileSize); 
    }
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

TEST_P(DataTransferSessionTest, DataTransferSessionWrongChecksumMigration) {
  // This test is the same as DataTransferSessionGooddayMigration, with
  // wrong file checksum on the first file migrated.
  // Behaviour is different from production due to  cta/CTA#1100

  // 0) Prepare the logger for everyone
  cta::log::StringLogger logger("dummy","tapeServerUnitTest",cta::log::DEBUG);
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

  // List to remember the path of each remote file so that the existence of the
  // files can be tested for at the end of the test
  std::list<std::string> remoteFilePaths;

  // 5) Create the environment for the migration to happen (library + tape)
    const std::string libraryComment = "Library comment";
  const bool libraryIsDisabled = false;
  catalogue.createLogicalLibrary(s_adminOnAdminHost, s_libraryName,
    libraryIsDisabled, libraryComment);
  {
    auto libraries = catalogue.getLogicalLibraries();
    ASSERT_EQ(1, libraries.size());
    ASSERT_EQ(s_libraryName, libraries.front().name);
    ASSERT_EQ(libraryComment, libraries.front().comment);
  }

  {
    auto tape = getDefaultTape();
    catalogue.createTape(s_adminOnAdminHost, tape);
  }

  // Create the mount criteria
  auto mountPolicy = getImmediateMountMountPolicy();
  catalogue.createMountPolicy(requester, mountPolicy);
  std::string mountPolicyName = mountPolicy.name;
  catalogue.createRequesterMountRule(requester, mountPolicyName, s_diskInstance, requester.username, "Rule comment");

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
    catalogue.tapeLabelled(s_vid, "T10D6116");
    mockSys.fake.m_pathToDrive["/dev/nst0"]->rewind();

    // Create the files and schedule the archivals

    //First a file with wrong checksum
    {
      int fseq = 1;
      sourceFiles.emplace_back(cta::make_unique<unitTests::TempFile>());
      sourceFiles.back()->randomFill(1000);
      remoteFilePaths.push_back(sourceFiles.back()->path());
      // Schedule the archival of the file
      cta::common::dataStructures::ArchiveRequest ar;
      ar.checksumBlob.insert(cta::checksum::ADLER32, sourceFiles.back()->adler32() + 1); // Wrong reported checksum
      ar.storageClass=s_storageClassName;
      ar.srcURL=std::string("file://") + sourceFiles.back()->path();
      ar.requester.name = requester.username;
      ar.requester.group = "group";
      ar.fileSize = 1000;
      ar.diskFileID = std::to_string(fseq);
      ar.diskFileInfo.path = "y";
      ar.diskFileInfo.owner_uid = DISK_FILE_OWNER_UID;
      ar.diskFileInfo.gid = DISK_FILE_GID;
      const auto archiveFileId = scheduler.checkAndGetNextArchiveFileId(s_diskInstance, ar.storageClass, ar.requester, logContext);
      archiveFileIds.push_back(archiveFileId);
      scheduler.queueArchiveWithGivenId(archiveFileId,s_diskInstance,ar,logContext);
    }
    for(int fseq=2; fseq <= 10 ; fseq ++) {
      // Create a source file.
      sourceFiles.emplace_back(cta::make_unique<unitTests::TempFile>());
      sourceFiles.back()->randomFill(1000);
      remoteFilePaths.push_back(sourceFiles.back()->path());
      // Schedule the archival of the file
      cta::common::dataStructures::ArchiveRequest ar;
      ar.checksumBlob.insert(cta::checksum::ADLER32, sourceFiles.back()->adler32());
      ar.storageClass=s_storageClassName;
      ar.srcURL=std::string("file://") + sourceFiles.back()->path();
      ar.requester.name = requester.username;
      ar.requester.group = "group";
      ar.fileSize = 1000;
      ar.diskFileID = std::to_string(fseq);
      ar.diskFileInfo.path = "y";
      ar.diskFileInfo.owner_uid = DISK_FILE_OWNER_UID;
      ar.diskFileInfo.gid = DISK_FILE_GID;
      const auto archiveFileId = scheduler.checkAndGetNextArchiveFileId(s_diskInstance, ar.storageClass, ar.requester, logContext);
      archiveFileIds.push_back(archiveFileId);
      scheduler.queueArchiveWithGivenId(archiveFileId,s_diskInstance,ar,logContext);
    }
  }
  scheduler.waitSchedulerDbSubthreadsComplete();
  // Report the drive's existence and put it up in the drive register.
  cta::tape::daemon::TpconfigLine driveConfig("T10D6116", "TestLogicalLibrary", "/dev/tape_T10D6116", "dummy");
  cta::common::dataStructures::DriveInfo driveInfo;
  driveInfo.driveName=driveConfig.unitName;
  driveInfo.logicalLibrary=driveConfig.logicalLibrary;
  driveInfo.host="host";
  // We need to create the drive in the registry before being able to put it up.
  scheduler.reportDriveStatus(driveInfo, cta::common::dataStructures::MountType::NoMount, cta::common::dataStructures::DriveStatus::Down, logContext);
  cta::common::dataStructures::DesiredDriveState driveState;
  driveState.up = true;
  driveState.forceDown = false;
  scheduler.setDesiredDriveState(s_adminOnAdminHost, driveConfig.unitName, driveState, logContext);

  // Create the data transfer session
  DataTransferConfig castorConf;
  castorConf.bufsz = 1024*1024; // 1 MB memory buffers
  castorConf.nbBufs = 10;
  castorConf.bulkRequestRecallMaxBytes = UINT64_C(100)*1000*1000*1000;
  castorConf.bulkRequestRecallMaxFiles = 1000;
  castorConf.bulkRequestMigrationMaxBytes = UINT64_C(100)*1000*1000*1000;
  castorConf.bulkRequestMigrationMaxFiles = 1000;
  castorConf.nbDiskThreads = 1;
  castorConf.tapeLoadTimeout = 300;
  castorConf.useEncryption = false;
  cta::log::DummyLogger dummyLog("dummy", "dummy");
  cta::mediachanger::MediaChangerFacade mc(dummyLog);
  cta::server::ProcessCap capUtils;
  castor::messages::TapeserverProxyDummy initialProcess;
  DataTransferSession sess("tapeHost", logger, mockSys, driveConfig, mc, initialProcess, capUtils, castorConf, scheduler);
  sess.execute();
  std::string logToCheck = logger.getLog();
  logToCheck += "";
  ASSERT_EQ(s_vid, sess.getVid());
  
  auto afiiter = archiveFileIds.begin();
  // None of the files made it to the catalogue
  for(__attribute__ ((unused)) auto & sf: sourceFiles) {
    auto afi = *(afiiter++);
    ASSERT_THROW(catalogue.getArchiveFileById(afi), cta::exception::Exception);
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

TEST_P(DataTransferSessionTest, DataTransferSessionWrongFilesizeInMiddleOfBatchMigration) {
  // This test is the same as DataTransferSessionGooddayMigration, with
  // wrong file size on the fifth file migrated. As a fix for #1096, all files
  // except the fifth should be written to tape and the catalogue

  // 0) Prepare the logger for everyone
  cta::log::StringLogger logger("dummy","tapeServerUnitTest",cta::log::DEBUG);
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

  // List to remember the path of each remote file so that the existence of the
  // files can be tested for at the end of the test
  std::list<std::string> remoteFilePaths;

  // 5) Create the environment for the migration to happen (library + tape)
    const std::string libraryComment = "Library comment";
  const bool libraryIsDisabled = false;
  catalogue.createLogicalLibrary(s_adminOnAdminHost, s_libraryName,
    libraryIsDisabled, libraryComment);
  {
    auto libraries = catalogue.getLogicalLibraries();
    ASSERT_EQ(1, libraries.size());
    ASSERT_EQ(s_libraryName, libraries.front().name);
    ASSERT_EQ(libraryComment, libraries.front().comment);
  }

  {
    auto tape = getDefaultTape();
    catalogue.createTape(s_adminOnAdminHost, tape);
  }

  // Create the mount criteria
  auto mountPolicy = getImmediateMountMountPolicy();
  catalogue.createMountPolicy(requester, mountPolicy);
  std::string mountPolicyName = mountPolicy.name;
  catalogue.createRequesterMountRule(requester, mountPolicyName, s_diskInstance, requester.username, "Rule comment");

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
    catalogue.tapeLabelled(s_vid, "T10D6116");
    mockSys.fake.m_pathToDrive["/dev/nst0"]->rewind();

    // Create the files and schedule the archivals
    for(int fseq=1; fseq <= 4 ; fseq ++) {
      // Create a source file.
      sourceFiles.emplace_back(cta::make_unique<unitTests::TempFile>());
      sourceFiles.back()->randomFill(1000);
      remoteFilePaths.push_back(sourceFiles.back()->path());
      // Schedule the archival of the file
      cta::common::dataStructures::ArchiveRequest ar;
      ar.checksumBlob.insert(cta::checksum::ADLER32, sourceFiles.back()->adler32());
      ar.storageClass=s_storageClassName;
      ar.srcURL=std::string("file://") + sourceFiles.back()->path();
      ar.requester.name = requester.username;
      ar.requester.group = "group";
      ar.fileSize = 1000;
      ar.diskFileID = std::to_string(fseq);
      ar.diskFileInfo.path = "y";
      ar.diskFileInfo.owner_uid = DISK_FILE_OWNER_UID;
      ar.diskFileInfo.gid = DISK_FILE_GID;
      const auto archiveFileId = scheduler.checkAndGetNextArchiveFileId(s_diskInstance, ar.storageClass, ar.requester, logContext);
      archiveFileIds.push_back(archiveFileId);
      scheduler.queueArchiveWithGivenId(archiveFileId,s_diskInstance,ar,logContext);
    }
    {
      //Create a file with wrong file size in the middle of the jobs
      int fseq = 5;
      sourceFiles.emplace_back(cta::make_unique<unitTests::TempFile>());
      sourceFiles.back()->randomFill(1000);
      remoteFilePaths.push_back(sourceFiles.back()->path());
      // Schedule the archival of the file
      cta::common::dataStructures::ArchiveRequest ar;
      ar.checksumBlob.insert(cta::checksum::ADLER32, sourceFiles.back()->adler32());
      ar.storageClass=s_storageClassName;
      ar.srcURL=std::string("file://") + sourceFiles.back()->path();
      ar.requester.name = requester.username;
      ar.requester.group = "group";
      ar.fileSize = 900; // Wrong reported size
      ar.diskFileID = std::to_string(fseq);
      ar.diskFileInfo.path = "y";
      ar.diskFileInfo.owner_uid = DISK_FILE_OWNER_UID;
      ar.diskFileInfo.gid = DISK_FILE_GID;
      const auto archiveFileId = scheduler.checkAndGetNextArchiveFileId(s_diskInstance, ar.storageClass, ar.requester, logContext);
      archiveFileIds.push_back(archiveFileId);
      scheduler.queueArchiveWithGivenId(archiveFileId,s_diskInstance,ar,logContext);
    }
    // Create the rest of the files and schedule the archivals
    for(int fseq=6; fseq <= 10 ; fseq ++) {
      // Create a source file.
      sourceFiles.emplace_back(cta::make_unique<unitTests::TempFile>());
      sourceFiles.back()->randomFill(1000);
      remoteFilePaths.push_back(sourceFiles.back()->path());
      // Schedule the archival of the file
      cta::common::dataStructures::ArchiveRequest ar;
      ar.checksumBlob.insert(cta::checksum::ADLER32, sourceFiles.back()->adler32());
      ar.storageClass=s_storageClassName;
      ar.srcURL=std::string("file://") + sourceFiles.back()->path();
      ar.requester.name = requester.username;
      ar.requester.group = "group";
      ar.fileSize = 1000;
      ar.diskFileID = std::to_string(fseq);
      ar.diskFileInfo.path = "y";
      ar.diskFileInfo.owner_uid = DISK_FILE_OWNER_UID;
      ar.diskFileInfo.gid = DISK_FILE_GID;
      const auto archiveFileId = scheduler.checkAndGetNextArchiveFileId(s_diskInstance, ar.storageClass, ar.requester, logContext);
      archiveFileIds.push_back(archiveFileId);
      scheduler.queueArchiveWithGivenId(archiveFileId,s_diskInstance,ar,logContext);
    }
  }
  scheduler.waitSchedulerDbSubthreadsComplete();
  // Report the drive's existence and put it up in the drive register.
  cta::tape::daemon::TpconfigLine driveConfig("T10D6116", "TestLogicalLibrary", "/dev/tape_T10D6116", "dummy");
  cta::common::dataStructures::DriveInfo driveInfo;
  driveInfo.driveName=driveConfig.unitName;
  driveInfo.logicalLibrary=driveConfig.logicalLibrary;
  driveInfo.host="host";
  // We need to create the drive in the registry before being able to put it up.
  scheduler.reportDriveStatus(driveInfo, cta::common::dataStructures::MountType::NoMount, cta::common::dataStructures::DriveStatus::Down, logContext);
  cta::common::dataStructures::DesiredDriveState driveState;
  driveState.up = true;
  driveState.forceDown = false;
  scheduler.setDesiredDriveState(s_adminOnAdminHost, driveConfig.unitName, driveState, logContext);

  // Create the data transfer session
  DataTransferConfig castorConf;
  castorConf.bufsz = 1024*1024; // 1 MB memory buffers
  castorConf.nbBufs = 10;
  castorConf.bulkRequestRecallMaxBytes = UINT64_C(100)*1000*1000*1000;
  castorConf.bulkRequestRecallMaxFiles = 1000;
  castorConf.bulkRequestMigrationMaxBytes = UINT64_C(100)*1000*1000*1000;
  castorConf.bulkRequestMigrationMaxFiles = 1000;
  castorConf.nbDiskThreads = 1;
  castorConf.tapeLoadTimeout = 300;
  castorConf.useEncryption = false;
  cta::log::DummyLogger dummyLog("dummy", "dummy");
  cta::mediachanger::MediaChangerFacade mc(dummyLog);
  cta::server::ProcessCap capUtils;
  castor::messages::TapeserverProxyDummy initialProcess;
  DataTransferSession sess("tapeHost", logger, mockSys, driveConfig, mc, initialProcess, capUtils, castorConf, scheduler);
  sess.execute();
  std::string logToCheck = logger.getLog();
  logToCheck += "";
  ASSERT_EQ(s_vid, sess.getVid());
  auto afiiter = archiveFileIds.begin();
  int fseq = 1;
  for(auto & sf: sourceFiles) {
    auto afi = *(afiiter++);
    if (fseq != 5) {
      // Files queued without the wrong file size made it to the catalogue
      auto afs = catalogue.getArchiveFileById(afi);
      ASSERT_EQ(1, afs.tapeFiles.size());
      cta::checksum::ChecksumBlob checksumBlob;
      checksumBlob.insert(cta::checksum::ADLER32, sf->adler32());
      ASSERT_EQ(afs.checksumBlob, checksumBlob);
      ASSERT_EQ(1000, afs.fileSize);
    } else {
      ASSERT_THROW(catalogue.getArchiveFileById(afi), cta::exception::Exception);
    }
    fseq++;
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
// This test is the same as DataTransferSessionGooddayMigration, except that the files are deleted
// from filesystem immediately. The disk tasks will then fail on open.
///
TEST_P(DataTransferSessionTest, DataTransferSessionMissingFilesMigration) {

  // 0) Prepare the logger for everyone
  cta::log::StringLogger logger("dummy", "tapeServerUnitTest", cta::log::DEBUG);
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
  auto& catalogue = getCatalogue();
  auto& scheduler = getScheduler();

  // Always use the same requester
  const cta::common::dataStructures::SecurityIdentity requester("user", "group");

  // List to remember the path of each remote file so that the existence of the
  // files can be tested for at the end of the test
  std::list<std::string> remoteFilePaths;

  // 5) Create the environment for the migration to happen (library + tape)
  const std::string libraryComment = "Library comment";
  const bool libraryIsDisabled = false;
  catalogue.createLogicalLibrary(s_adminOnAdminHost, s_libraryName,
                                 libraryIsDisabled, libraryComment);
  {
    auto libraries = catalogue.getLogicalLibraries();
    ASSERT_EQ(1, libraries.size());
    ASSERT_EQ(s_libraryName, libraries.front().name);
    ASSERT_EQ(libraryComment, libraries.front().comment);
  }

  {
    auto tape = getDefaultTape();
    catalogue.createTape(s_adminOnAdminHost, tape);
  }

  // Create the mount criteria
  auto mountPolicy = getImmediateMountMountPolicy();
  catalogue.createMountPolicy(requester, mountPolicy);
  std::string mountPolicyName = mountPolicy.name;
  catalogue.createRequesterMountRule(requester, mountPolicyName, s_diskInstance, requester.username, "Rule comment");

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
    catalogue.tapeLabelled(s_vid, "T10D6116");
    mockSys.fake.m_pathToDrive["/dev/nst0"]->rewind();

    // Create the files and schedule the archivals
    for (int fseq = 1; fseq <= 10; fseq++) {
      // Create a source file.
      sourceFiles.emplace_back(cta::make_unique<unitTests::TempFile>());
      sourceFiles.back()->randomFill(1000);
      remoteFilePaths.push_back(sourceFiles.back()->path());
      // Schedule the archival of the file
      cta::common::dataStructures::ArchiveRequest ar;
      ar.checksumBlob.insert(cta::checksum::ADLER32, sourceFiles.back()->adler32());
      ar.storageClass = s_storageClassName;
      ar.srcURL = std::string("file://") + sourceFiles.back()->path();
      ar.requester.name = requester.username;
      ar.requester.group = "group";
      ar.fileSize = 1000;
      ar.diskFileID = "x";
      ar.diskFileID += std::to_string(fseq);
      ar.diskFileInfo.path = "y";
      ar.diskFileInfo.owner_uid = DISK_FILE_OWNER_UID;
      ar.diskFileInfo.gid = DISK_FILE_GID;
      const auto archiveFileId = scheduler.checkAndGetNextArchiveFileId(s_diskInstance, ar.storageClass, ar.requester, logContext);
      archiveFileIds.push_back(archiveFileId);
      scheduler.queueArchiveWithGivenId(archiveFileId, s_diskInstance, ar, logContext);
      // Delete the even files: the migration will work for half of them.
      if (!(fseq % 2)) sourceFiles.pop_back();
    }
  }
  scheduler.waitSchedulerDbSubthreadsComplete();
  // Report the drive's existence and put it up in the drive register.
  cta::tape::daemon::TpconfigLine driveConfig("T10D6116", "TestLogicalLibrary", "/dev/tape_T10D6116", "dummy");
  cta::common::dataStructures::DriveInfo driveInfo;
  driveInfo.driveName = driveConfig.unitName;
  driveInfo.logicalLibrary = driveConfig.logicalLibrary;
  driveInfo.host = "host";
  // We need to create the drive in the registry before being able to put it up.
  scheduler.reportDriveStatus(driveInfo, cta::common::dataStructures::MountType::NoMount, cta::common::dataStructures::DriveStatus::Down, logContext);
  cta::common::dataStructures::DesiredDriveState driveState;
  driveState.up = true;
  driveState.forceDown = false;
  scheduler.setDesiredDriveState(s_adminOnAdminHost, driveConfig.unitName, driveState, logContext);

  // Create the data transfer session
  DataTransferConfig castorConf;
  castorConf.bufsz = 1024 * 1024; // 1 MB memory buffers
  castorConf.nbBufs = 10;
  castorConf.bulkRequestRecallMaxBytes = UINT64_C(100) * 1000 * 1000 * 1000;
  castorConf.bulkRequestRecallMaxFiles = 1000;
  castorConf.bulkRequestMigrationMaxBytes = UINT64_C(100) * 1000 * 1000 * 1000;
  castorConf.bulkRequestMigrationMaxFiles = 1000;
  castorConf.nbDiskThreads = 1;
  castorConf.maxBytesBeforeFlush = 9999999;
  castorConf.maxFilesBeforeFlush = 9999999;
  castorConf.tapeLoadTimeout = 300;
  castorConf.useEncryption = false;
  cta::log::DummyLogger dummyLog("dummy", "dummy");
  cta::mediachanger::MediaChangerFacade mc(dummyLog);
  cta::server::ProcessCap capUtils;
  castor::messages::TapeserverProxyDummy initialProcess;
  DataTransferSession sess("tapeHost", logger, mockSys, driveConfig, mc, initialProcess, capUtils, castorConf, scheduler);
  sess.execute();
  std::string temp = logger.getLog();
  temp += "";
  ASSERT_EQ(s_vid, sess.getVid());
  // We should now have 5 successfully read files.
  size_t count = 0;
  std::string::size_type pos = 0;
  std::string successLog = "MSG=\"File successfully read from disk\"";
  while ((pos = logger.getLog().find(successLog, pos)) != std::string::npos) {
    pos += successLog.size();
    count++;
  }
  //std::cout << logger.getLog() << std::endl;
  ASSERT_EQ(5, count);
  cta::catalogue::TapeSearchCriteria tapeCriteria;
  tapeCriteria.vid = s_vid;
  auto tapeInfo = catalogue.getTapes(tapeCriteria);
  ASSERT_EQ(1, tapeInfo.size());
  // We should have max fseq at least 10. It could be higher is a retry manages to sneak in.
  ASSERT_LE(10, tapeInfo.begin()->lastFSeq);
  ASSERT_EQ(5 * 1000, tapeInfo.begin()->dataOnTapeInBytes);

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
  cta::log::StringLogger logger("dummy", "tapeServerUnitTest", cta::log::DEBUG);
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
  auto& catalogue = getCatalogue();
  auto& scheduler = getScheduler();

  // Always use the same requester
  const cta::common::dataStructures::SecurityIdentity requester("user", "group");

  // List to remember the path of each remote file so that the existence of the
  // files can be tested for at the end of the test
  std::list<std::string> remoteFilePaths;

  // 5) Create the environment for the migration to happen (library + tape)
  const std::string libraryComment = "Library comment";
  const bool libraryIsDisabled = false;
  catalogue.createLogicalLibrary(s_adminOnAdminHost, s_libraryName,
                                 libraryIsDisabled, libraryComment);
  {
    auto libraries = catalogue.getLogicalLibraries();
    ASSERT_EQ(1, libraries.size());
    ASSERT_EQ(s_libraryName, libraries.front().name);
    ASSERT_EQ(libraryComment, libraries.front().comment);
  }
  const std::string tapeComment = "Tape comment";

  {
    auto tape = getDefaultTape();
    catalogue.createTape(s_adminOnAdminHost, tape);
  }

  auto mountPolicy = getImmediateMountMountPolicy();
  catalogue.createMountPolicy(requester, mountPolicy);
  std::string mountPolicyName = mountPolicy.name;
  catalogue.createRequesterMountRule(requester, mountPolicyName, s_diskInstance, requester.username, "Rule comment");

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
    catalogue.tapeLabelled(s_vid, "T10D6116");
    mockSys.fake.m_pathToDrive["/dev/nst0"]->rewind();

    // Create the files and schedule the archivals
    for (int fseq = 1; fseq <= 10; fseq++) {
      // Create a source file.
      sourceFiles.emplace_back(cta::make_unique<unitTests::TempFile>());
      sourceFiles.back()->randomFill(1000);
      remoteFilePaths.push_back(sourceFiles.back()->path());
      // Schedule the archival of the file
      cta::common::dataStructures::ArchiveRequest ar;
      ar.checksumBlob.insert(cta::checksum::ADLER32, sourceFiles.back()->adler32());
      ar.storageClass = s_storageClassName;
      ar.srcURL = std::string("file://") + sourceFiles.back()->path();
      ar.requester.name = requester.username;
      ar.requester.group = "group";
      ar.fileSize = 1000;
      ar.diskFileID = std::to_string(fseq);
      ar.diskFileInfo.path = "y";
      ar.diskFileInfo.owner_uid = DISK_FILE_OWNER_UID;
      ar.diskFileInfo.gid = DISK_FILE_GID;
      const auto archiveFileId = scheduler.checkAndGetNextArchiveFileId(s_diskInstance, ar.storageClass, ar.requester, logContext);
      archiveFileIds.push_back(archiveFileId);
      scheduler.queueArchiveWithGivenId(archiveFileId, s_diskInstance, ar, logContext);
    }
  }
  scheduler.waitSchedulerDbSubthreadsComplete();
  // Report the drive's existence and put it up in the drive register.
  cta::tape::daemon::TpconfigLine driveConfig("T10D6116", "TestLogicalLibrary", "/dev/tape_T10D6116", "dummy");
  cta::common::dataStructures::DriveInfo driveInfo;
  driveInfo.driveName = driveConfig.unitName;
  driveInfo.logicalLibrary = driveConfig.logicalLibrary;
  driveInfo.host = "host";
  // We need to create the drive in the registry before being able to put it up.
  scheduler.reportDriveStatus(driveInfo, cta::common::dataStructures::MountType::NoMount, cta::common::dataStructures::DriveStatus::Down, logContext);
  cta::common::dataStructures::DesiredDriveState driveState;
  driveState.up = true;
  driveState.forceDown = false;
  scheduler.setDesiredDriveState(s_adminOnAdminHost, driveConfig.unitName, driveState, logContext);

  // Create the data transfer session
  DataTransferConfig castorConf;
  castorConf.bufsz = 1024 * 1024; // 1 MB memory buffers
  castorConf.nbBufs = 10;
  castorConf.bulkRequestRecallMaxBytes = UINT64_C(100) * 1000 * 1000 * 1000;
  castorConf.bulkRequestRecallMaxFiles = 1000;
  castorConf.bulkRequestMigrationMaxBytes = UINT64_C(100) * 1000 * 1000 * 1000;
  castorConf.bulkRequestMigrationMaxFiles = 1000;
  castorConf.nbDiskThreads = 1;
  castorConf.tapeLoadTimeout = 300;
  castorConf.useEncryption = false;
  cta::log::DummyLogger dummyLog("dummy", "dummy");
  cta::mediachanger::MediaChangerFacade mc(dummyLog);
  cta::server::ProcessCap capUtils;
  castor::messages::TapeserverProxyDummy initialProcess;
  DataTransferSession sess("tapeHost", logger, mockSys, driveConfig, mc, initialProcess, capUtils, castorConf, scheduler);
  sess.execute();
  std::string temp = logger.getLog();
  temp += "";
  ASSERT_EQ(s_vid, sess.getVid());
  cta::catalogue::TapeFileSearchCriteria criteria;
  auto afsItor = catalogue.getArchiveFilesItor(criteria);
  for (size_t i = 1; i <= sourceFiles.size(); ++i) {
    // Only the first files made it through.
    if (i <= 3) {
      ASSERT_TRUE(afsItor.hasMore());
      auto afs = afsItor.next();
      ASSERT_EQ(1, afs.tapeFiles.size());
      cta::checksum::ChecksumBlob checksumBlob;
      // Get the element of the list sourceFiles correspondent with afs.archiveFileID (https://stackoverflow.com/a/16747600)
      // archiveFileID starts on "1" that's why it removes one position in the list
      auto sourceFiles_front = sourceFiles.begin();
      std::advance(sourceFiles_front, afs.archiveFileID - 1);
      checksumBlob.insert(cta::checksum::ADLER32, (*sourceFiles_front)->adler32());
      ASSERT_EQ(afs.checksumBlob, checksumBlob);
      ASSERT_EQ(1000, afs.fileSize);
    }
    else {
      ASSERT_FALSE(afsItor.hasMore());
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
  ASSERT_NE(std::string::npos,
            logToCheck.find("MSG=\"Tape session started for write\" thread=\"TapeWrite\" tapeDrive=\"T10D6116\" tapeVid=\"TstVid\" "
                            "mountId=\"1\" vo=\"vo\" mediaType=\"LTO7M\" tapePool=\"TestTapePool\" logicalLibrary=\"TestLogicalLibrary\" "
                            "mountType=\"ArchiveForUser\" vendor=\"TestVendor\" capacityInBytes=\"12345678\""));
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
  cta::log::StringLogger logger("dummy", "tapeServerUnitTest", cta::log::DEBUG);
  cta::log::LogContext logContext(logger);

  setupDefaultCatalogue();
  // 1) prepare the fake scheduler
  // cta::MountType::Enum mountType = cta::MountType::RETRIEVE;

  // 3) Prepare the necessary environment (logger, plus system wrapper),
  castor::tape::System::mockWrapper mockSys;
  mockSys.delegateToFake();
  mockSys.disableGMockCallsCounting();
  mockSys.fake.setupForVirtualDriveSLC6();

  // 4) Create the scheduler
  auto& catalogue = getCatalogue();
  auto& scheduler = getScheduler();

  // Always use the same requester
  const cta::common::dataStructures::SecurityIdentity requester("user", "group");

  // List to remember the path of each remote file so that the existence of the
  // files can be tested for at the end of the test
  std::list<std::string> remoteFilePaths;

  // 5) Create the environment for the migration to happen (library + tape)
  const std::string libraryComment = "Library comment";
  const bool libraryIsDisabled = false;
  catalogue.createLogicalLibrary(s_adminOnAdminHost, s_libraryName,
                                 libraryIsDisabled, libraryComment);
  {
    auto libraries = catalogue.getLogicalLibraries();
    ASSERT_EQ(1, libraries.size());
    ASSERT_EQ(s_libraryName, libraries.front().name);
    ASSERT_EQ(libraryComment, libraries.front().comment);
  }

  {
    auto tape = getDefaultTape();
    catalogue.createTape(s_adminOnAdminHost, tape);
  }

  // Create the mount criteria
  auto mountPolicy = getImmediateMountMountPolicy();
  catalogue.createMountPolicy(requester, mountPolicy);
  std::string mountPolicyName = mountPolicy.name;
  catalogue.createRequesterMountRule(requester, mountPolicyName, s_diskInstance, requester.username, "Rule comment");

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
    catalogue.tapeLabelled(s_vid, "T10D6116");
    mockSys.fake.m_pathToDrive["/dev/nst0"]->rewind();

    // Create the files and schedule the archivals
    for (int fseq = 1; fseq <= 10; fseq++) {
      // Create a source file.
      sourceFiles.emplace_back(cta::make_unique<unitTests::TempFile>());
      sourceFiles.back()->randomFill(1000);
      remoteFilePaths.push_back(sourceFiles.back()->path());
      // Schedule the archival of the file
      cta::common::dataStructures::ArchiveRequest ar;
      ar.checksumBlob.insert(cta::checksum::ADLER32, sourceFiles.back()->adler32());
      ar.storageClass = s_storageClassName;
      ar.srcURL = std::string("file://") + sourceFiles.back()->path();
      ar.requester.name = requester.username;
      ar.requester.group = "group";
      ar.fileSize = 1000;
      ar.diskFileID = std::to_string(fseq);
      ar.diskFileInfo.path = "y";
      ar.diskFileInfo.owner_uid = DISK_FILE_OWNER_UID;
      ar.diskFileInfo.gid = DISK_FILE_GID;
      const auto archiveFileId = scheduler.checkAndGetNextArchiveFileId(s_diskInstance, ar.storageClass, ar.requester, logContext);
      archiveFileIds.push_back(archiveFileId);
      scheduler.queueArchiveWithGivenId(archiveFileId, s_diskInstance, ar, logContext);
    }
  }
  scheduler.waitSchedulerDbSubthreadsComplete();
  // Report the drive's existence and put it up in the drive register.
  cta::tape::daemon::TpconfigLine driveConfig("T10D6116", "TestLogicalLibrary", "/dev/tape_T10D6116", "dummy");
  cta::common::dataStructures::DriveInfo driveInfo;
  driveInfo.driveName = driveConfig.unitName;
  driveInfo.logicalLibrary = driveConfig.logicalLibrary;
  driveInfo.host = "host";
  // We need to create the drive in the registry before being able to put it up.
  scheduler.reportDriveStatus(driveInfo, cta::common::dataStructures::MountType::NoMount, cta::common::dataStructures::DriveStatus::Down, logContext);
  cta::common::dataStructures::DesiredDriveState driveState;
  driveState.up = true;
  driveState.forceDown = false;
  scheduler.setDesiredDriveState(s_adminOnAdminHost, driveConfig.unitName, driveState, logContext);

  // Create the data transfer session
  DataTransferConfig castorConf;
  castorConf.bufsz = 1024 * 1024; // 1 MB memory buffers
  castorConf.nbBufs = 10;
  castorConf.bulkRequestRecallMaxBytes = UINT64_C(100) * 1000 * 1000 * 1000;
  castorConf.bulkRequestRecallMaxFiles = 1000;
  castorConf.bulkRequestMigrationMaxBytes = UINT64_C(100) * 1000 * 1000 * 1000;
  castorConf.bulkRequestMigrationMaxFiles = 1000;
  castorConf.nbDiskThreads = 1;
  castorConf.tapeLoadTimeout = 300;
  castorConf.useEncryption = false;
  cta::log::DummyLogger dummyLog("dummy", "dummy");
  cta::mediachanger::MediaChangerFacade mc(dummyLog);
  cta::server::ProcessCap capUtils;
  castor::messages::TapeserverProxyDummy initialProcess;
  DataTransferSession sess("tapeHost", logger, mockSys, driveConfig, mc, initialProcess, capUtils, castorConf, scheduler);
  sess.execute();
  std::string temp = logger.getLog();
  temp += "";
  ASSERT_EQ(s_vid, sess.getVid());
  cta::catalogue::TapeFileSearchCriteria criteria;
  auto afsItor = catalogue.getArchiveFilesItor(criteria);
  for (size_t i = 1; i <= sourceFiles.size(); ++i) {
    // Only the first files made it through.
    if (i <= 3) {
      ASSERT_TRUE(afsItor.hasMore());
      auto afs = afsItor.next();
      ASSERT_EQ(1, afs.tapeFiles.size());
      cta::checksum::ChecksumBlob checksumBlob;
      // Get the element of the list sourceFiles correspondent with afs.archiveFileID (https://stackoverflow.com/a/16747600)
      // archiveFileID starts on "1" that's why it removes one position in the list
      auto sourceFiles_front = sourceFiles.begin();
      std::advance(sourceFiles_front, afs.archiveFileID - 1);
      checksumBlob.insert(cta::checksum::ADLER32, (*sourceFiles_front)->adler32());
      ASSERT_EQ(afs.checksumBlob, checksumBlob);
      ASSERT_EQ(1000, afs.fileSize);
    }
    else {
      ASSERT_FALSE(afsItor.hasMore());
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

TEST_P(DataTransferSessionTest, CleanerSessionFailsShouldPutTheDriveDown) {
  // 0) Prepare the logger for everyone
  cta::log::StringLogger logger("dummy", "tapeServerUnitTest", cta::log::DEBUG);
  cta::log::LogContext logContext(logger);

  setupDefaultCatalogue();
  // 1) prepare the fake scheduler
  // cta::MountType::Enum mountType = cta::MountType::RETRIEVE;

  // 3) Prepare the necessary environment (logger, plus system wrapper),
  castor::tape::System::mockWrapper mockSys;
  mockSys.delegateToFake();
  mockSys.disableGMockCallsCounting();
  mockSys.fake.setupForVirtualDriveSLC6();

  // 4) Create the scheduler
  auto& catalogue = getCatalogue();
  auto& scheduler = getScheduler();

  // Always use the same requester
  const cta::common::dataStructures::SecurityIdentity requester("user", "group");

  // List to remember the path of each remote file so that the existence of the
  // files can be tested for at the end of the test
  std::list<std::string> remoteFilePaths;

  // 5) Create the environment for the migration to happen (library + tape)
  const std::string libraryComment = "Library comment";
  const bool libraryIsDisabled = false;
  catalogue.createLogicalLibrary(s_adminOnAdminHost, s_libraryName,
                                 libraryIsDisabled, libraryComment);
  {
    auto libraries = catalogue.getLogicalLibraries();
    ASSERT_EQ(1, libraries.size());
    ASSERT_EQ(s_libraryName, libraries.front().name);
    ASSERT_EQ(libraryComment, libraries.front().comment);
  }

  {
    cta::catalogue::CreateTapeAttributes tape = getDefaultTape();
    catalogue.createTape(s_adminOnAdminHost, tape);
  }

  // Create the mount criteria
  auto mountPolicy = getImmediateMountMountPolicy();
  catalogue.createMountPolicy(requester, mountPolicy);
  std::string mountPolicyName = mountPolicy.name;
  catalogue.createRequesterMountRule(requester, mountPolicyName, s_diskInstance, requester.username, "Rule comment");

  //delete is unnecessary
  //pointer with ownership will be passed to the application,
  //which will do the delete
  const uint64_t tapeSize = 5000;
  mockSys.fake.m_pathToDrive["/dev/nst0"] = new castor::tape::tapeserver::drive::FakeDrive(tapeSize,
                                                                                           castor::tape::tapeserver::drive::FakeDrive::OnFlush);

  // Report the drive's existence and put it up in the drive register.
  cta::tape::daemon::TpconfigLine driveConfig("T10D6116", "TestLogicalLibrary", "/dev/tape_T10D6116", "dummy");
  cta::common::dataStructures::DriveInfo driveInfo;
  driveInfo.driveName = driveConfig.unitName;
  driveInfo.logicalLibrary = driveConfig.logicalLibrary;
  driveInfo.host = "host";
  // We need to create the drive in the registry before being able to put it up.
  scheduler.reportDriveStatus(driveInfo, cta::common::dataStructures::MountType::NoMount, cta::common::dataStructures::DriveStatus::Down, logContext);
  cta::common::dataStructures::DesiredDriveState driveState;
  driveState.up = true;
  driveState.forceDown = false;
  scheduler.setDesiredDriveState(s_adminOnAdminHost, driveConfig.unitName, driveState, logContext);

  // Create cleaner session
  DataTransferConfig castorConf;
  castorConf.bufsz = 1024 * 1024; // 1 MB memory buffers
  castorConf.nbBufs = 10;
  castorConf.bulkRequestRecallMaxBytes = UINT64_C(100) * 1000 * 1000 * 1000;
  castorConf.bulkRequestRecallMaxFiles = 1000;
  castorConf.bulkRequestMigrationMaxBytes = UINT64_C(100) * 1000 * 1000 * 1000;
  castorConf.bulkRequestMigrationMaxFiles = 1000;
  castorConf.nbDiskThreads = 1;
  castorConf.tapeLoadTimeout = 300;
  castorConf.useEncryption = false;
  cta::log::DummyLogger dummyLog("dummy", "dummy");
  cta::mediachanger::MediaChangerFacade mc(dummyLog);
  cta::server::ProcessCapDummy capUtils;
  castor::messages::TapeserverProxyDummy initialProcess;
  CleanerSession cleanerSession(
    capUtils,
    mc,
    logger,
    driveConfig,
    mockSys,
    s_vid,
    false,
    0,
    "",
    catalogue,
    scheduler
  );
  auto endOfSessionAction = cleanerSession.execute();
  //the tape has not been labeled so the cleanerSession should have failed and put the drive down.
  cta::common::dataStructures::DesiredDriveState newDriveState = scheduler.getDesiredDriveState(driveConfig.unitName, logContext);
  ASSERT_FALSE(newDriveState.up);
  ASSERT_EQ(castor::tape::tapeserver::daemon::Session::MARK_DRIVE_AS_DOWN, endOfSessionAction);
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
