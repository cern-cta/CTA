/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#define __STDC_CONSTANT_MACROS  // For using stdint macros (stdint is included
// by inttypes.h, so we shoot first)
#include "TapeSession.hpp"

#include "catalogue/CatalogueItor.hpp"
#include "catalogue/CreateMountPolicyAttributes.hpp"
#include "catalogue/CreateTapeAttributes.hpp"
#include "catalogue/InMemoryCatalogue.hpp"
#include "catalogue/MediaType.hpp"
#include "catalogue/OracleCatalogueSchema.hpp"
#include "catalogue/TapeItemWrittenPointer.hpp"
#include "catalogue/rdbms/oracle/OracleCatalogue.hpp"
#include "common/dataStructures/DiskInstance.hpp"
#include "common/dataStructures/LogicalLibrary.hpp"
#include "common/dataStructures/MountPolicy.hpp"
#include "common/dataStructures/RequesterMountRule.hpp"
#include "common/exception/Exception.hpp"
#include "common/exception/LostDatabaseConnection.hpp"
#include "common/log/StringLogger.hpp"
#include "common/process/threading/Thread.hpp"
#include "common/utils/utils.hpp"
#include "mediachanger/MediaChangerFacade.hpp"
#include "scheduler/MountType.hpp"
#include "scheduler/Scheduler.hpp"
#include "scheduler/testingMocks/MockArchiveJob.hpp"
#include "scheduler/testingMocks/MockArchiveMount.hpp"
#include "scheduler/testingMocks/MockRetrieveMount.hpp"
#include "taped/drive/FakeDrive.hpp"
#include "taped/file/Exceptions.hpp"
#include "taped/file/FileWriter.hpp"
#include "taped/file/LabelSession.hpp"
#include "taped/file/WriteSession.hpp"
#include "taped/session/VolumeInfo.hpp"
#include "taped/system/Wrapper.hpp"
#include "tests/TempFile.hpp"

#include <atomic>
#include <cstdio>
#include <dirent.h>
#include <fcntl.h>
#include <gtest/gtest.h>
#include <inttypes.h>
#include <new>
#include <ranges>
#include <stdexcept>
#include <stdint.h>
#include <string_view>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <type_traits>
#include <unistd.h>
#include <zlib.h>

#ifdef CTA_PGSCHED
#include "scheduler/rdbms/RelationalDBTestFactory.hpp"
#else
#include "objectstore/BackendRadosTestSwitch.hpp"
#include "scheduler/OStoreDB/OStoreDBFactory.hpp"
#endif

#ifdef STDOUT_LOGGING
#include "common/log/StdoutLogger.hpp"
#else
#include "common/log/DummyLogger.hpp"
#endif

using namespace cta::tape;
using namespace cta::tape::daemon;

namespace unitTests {

// Count report messages without depending on the logger's output format.
size_t countLogMessages(const std::string& log, std::string_view message) {
  size_t count = 0;
  for (size_t pos = log.find(message); pos != std::string::npos; pos = log.find(message, pos + message.size())) {
    ++count;
  }
  return count;
}

const uint32_t DISK_FILE_OWNER_UID = 9751;
const uint32_t DISK_FILE_GID = 9752;
const uint32_t DISK_FILE_SOME_USER = 9753;
const uint32_t DISK_FILE_SOME_GROUP = 9754;

namespace {

/**
 * This structure is used to parameterize scheduler tests.
 */
struct TapeSessionTestParam {
  cta::SchedulerDatabaseFactory& dbFactory;

  explicit TapeSessionTestParam(cta::SchedulerDatabaseFactory& dbFactory) : dbFactory(dbFactory) {}
};  // struct TapeSessionTest

}  // namespace

// Keep exception scenarios in subprocesses: an unjoined reporter can otherwise hang
// destruction or access the next test's tracker. A timeout is a test failure.
enum class TransferFailurePoint {
  None,
  Metadata,
  StartingStatus,
  MetadataDatabase,
  FetchDatabase,
  CompleteDatabase,
  MetadataAllocation,
  MetadataLogic,
  MetadataUnknown,
  Discovery,
  DeviceEnumeration,
  MissingDrive,
  OpenCta,
  OpenStandard,
  CompleteCta,
  CompleteStandard,
  ReportDown,
  DesiredDown,
  ReportUp,
  FetchCta,
  FetchStandard,
  ReservationDenied,
  ReservationCta,
  ReservationStandard,
  RequeueStandard,
  TapeMountedCta,
  TapeMountedStandard,
  TapeMountedAndUnload,
  TapeMountedAndComplete
};

class TransferArchiveJobWithDestructionCounter : public cta::SchedulerDatabase::ArchiveJob {
public:
  explicit TransferArchiveJobWithDestructionCounter(std::atomic<unsigned int>& counter) : m_counter(counter) {
    archiveFile.archiveFileID = 1;
    archiveFile.fileSize = 0;
    tapeFile.copyNb = 1;
    tapeFile.vid = "TSTVID";
    tapeFile.fSeq = 1;
    reportType = ReportType::NoReportRequired;
  }

  ~TransferArchiveJobWithDestructionCounter() override { ++m_counter; }

  void failTransfer(const std::string&, cta::log::LogContext&) override {}

  void failReport(const std::string&, cta::log::LogContext&) override {}

  void bumpUpTapeFileCount(uint64_t) override {}

  void initialize(const cta::rdbms::Rset&, bool) override {}

  bool releaseToPool() override { return true; }

private:
  std::atomic<unsigned int>& m_counter;
};

class FailingTransferArchiveDbMount : public cta::SchedulerDatabase::ArchiveMount {
public:
  explicit FailingTransferArchiveDbMount(TransferFailurePoint point) : failure(point) { nbFilesCurrentlyOnTape = 0; }

  TransferFailurePoint failure;
  unsigned int fetchAttempts = 0;
  std::atomic<unsigned int> jobDestructions = 0;

  const MountInfo& getMountInfo() override { return mountInfo; }

  std::list<std::unique_ptr<cta::SchedulerDatabase::ArchiveJob>>
  getNextJobBatch(uint64_t, uint64_t, cta::log::LogContext&) override {
    ++fetchAttempts;
    if (failure == TransferFailurePoint::FetchDatabase) {
      throw cta::exception::LostDatabaseConnection("injected fetch disconnection");
    }
    if (failure == TransferFailurePoint::FetchCta) {
      throw cta::exception::Exception("injected archive fetch failure");
    }
    if (failure == TransferFailurePoint::FetchStandard) {
      throw std::runtime_error("injected archive fetch failure");
    }
    std::list<std::unique_ptr<cta::SchedulerDatabase::ArchiveJob>> jobs;
    if (fetchAttempts == 1
        && (failure == TransferFailurePoint::TapeMountedCta || failure == TransferFailurePoint::TapeMountedStandard
            || failure == TransferFailurePoint::TapeMountedAndUnload
            || failure == TransferFailurePoint::TapeMountedAndComplete)) {
      jobs.emplace_back(std::make_unique<TransferArchiveJobWithDestructionCounter>(jobDestructions));
    }
    return jobs;
  }

  void setDriveStatus(cta::common::dataStructures::DriveStatus,
                      cta::common::dataStructures::MountType,
                      time_t,
                      const std::optional<std::string>&) override {}

  void setTapeSessionStats(const TapeTransferStats&) override {}

  void setJobBatchTransferred(std::list<std::unique_ptr<cta::SchedulerDatabase::ArchiveJob>>&,
                              cta::log::LogContext&) override {}

  uint64_t requeueJobBatch(const std::list<std::string>&, cta::log::LogContext&) const override { return 0; }
};

template<typename Base>
class FailingTransferMount : public Base {
public:
  FailingTransferMount(cta::catalogue::Catalogue& catalogue, TransferFailurePoint point)
      : Base(catalogue),
        failure(point) {
    if constexpr (std::is_same_v<Base, cta::MockArchiveMount>) {
      this->m_dbMount = std::make_unique<FailingTransferArchiveDbMount>(point);
      this->m_sessionRunning = true;
    }
  }

  TransferFailurePoint failure;
  unsigned int completionAttempts = 0;
  unsigned int startingAttempts = 0;
  mutable unsigned int mountedAttempts = 0;

  std::string getVid() const override {
    switch (failure) {
      case TransferFailurePoint::MetadataDatabase:
        throw cta::exception::LostDatabaseConnection("injected metadata disconnection");
      case TransferFailurePoint::MetadataAllocation:
        throw std::bad_alloc();
      case TransferFailurePoint::MetadataLogic:
        throw std::logic_error("injected metadata logic failure");
      case TransferFailurePoint::MetadataUnknown:
        throw 42;
      default:
        break;
    }
    if (failure == TransferFailurePoint::Metadata) {
      throw std::runtime_error("injected metadata failure");
    }
    return "TSTVID";
  }

  cta::common::dataStructures::MountType getMountType() const override {
    if constexpr (std::is_same_v<Base, cta::MockArchiveMount>) {
      return repack ? cta::common::dataStructures::MountType::ArchiveForRepack :
                      cta::common::dataStructures::MountType::ArchiveForUser;
    } else {
      return cta::common::dataStructures::MountType::Retrieve;
    }
  }

  bool repack = false;

  uint32_t getNbFiles() const override { return 0; }

  std::string getMountTransactionId() const override { return "1234567890"; }

  cta::common::dataStructures::Label::Format getLabelFormat() const override {
    return cta::common::dataStructures::Label::Format::CTA;
  }

  std::optional<std::string> getEncryptionKeyName() const override { return std::nullopt; }

  std::string getPoolName() const override { return "TestTapePool"; }

  std::string getVo() const override { return "vo"; }

  std::string getMediaType() const override { return "LTO7M"; }

  std::string getVendor() const override { return "TestVendor"; }

  uint64_t getCapacityInBytes() const override { return 1024 * 1024; }

  void setDriveStatus(cta::common::dataStructures::DriveStatus status,
                      const std::optional<std::string>& reason = std::nullopt) override {
    if (status == cta::common::dataStructures::DriveStatus::Down) {
      downReason = reason;
    }
    if (status == cta::common::dataStructures::DriveStatus::Starting) {
      ++startingAttempts;
      if (failure == TransferFailurePoint::StartingStatus) {
        throw std::runtime_error("injected starting status failure");
      }
    }
  }

  void setTapeSessionStats(const TapeTransferStats& stats) override {
    ++statsReports;
    lastReportedStats = stats;
  }

  std::optional<std::string> downReason;
  unsigned int statsReports = 0;
  TapeTransferStats lastReportedStats;

  unsigned int archiveFetchAttempts() const {
    if constexpr (std::is_same_v<Base, cta::MockArchiveMount>) {
      return static_cast<FailingTransferArchiveDbMount&>(*this->m_dbMount).fetchAttempts;
    }
    return 0;
  }

  unsigned int archiveJobDestructions() const {
    if constexpr (std::is_same_v<Base, cta::MockArchiveMount>) {
      return static_cast<FailingTransferArchiveDbMount&>(*this->m_dbMount).jobDestructions.load();
    }
    return 0;
  }

  void setTapeMounted(cta::log::LogContext&) const override {
    ++mountedAttempts;
    if (failure == TransferFailurePoint::TapeMountedCta || failure == TransferFailurePoint::TapeMountedAndUnload
        || failure == TransferFailurePoint::TapeMountedAndComplete) {
      throw cta::exception::Exception("injected mounted publication failure");
    }
    throw std::runtime_error("injected mounted publication failure");
  }

  void complete() override {
    ++completionAttempts;
    if (failure == TransferFailurePoint::CompleteDatabase) {
      throw cta::exception::LostDatabaseConnection("injected completion disconnection");
    }
    if (failure == TransferFailurePoint::CompleteCta) {
      throw cta::exception::Exception("injected completion failure");
    }
    if (failure == TransferFailurePoint::CompleteStandard || failure == TransferFailurePoint::TapeMountedAndComplete) {
      throw std::runtime_error("injected completion failure");
    }
  }
};

class TransferJobWithDestructionCounter : public cta::MockRetrieveJob {
public:
  TransferJobWithDestructionCounter(cta::RetrieveMount& mount, std::atomic<unsigned int>& counter)
      : cta::MockRetrieveJob(mount),
        m_counter(counter) {
    archiveFile.archiveFileID = 1;
    archiveFile.fileSize = 1;
    archiveFile.tapeFiles.front().vid = "TSTVID";
  }

  ~TransferJobWithDestructionCounter() override { ++m_counter; }

private:
  std::atomic<unsigned int>& m_counter;
};

class FailingTransferRetrieveMount : public FailingTransferMount<cta::MockRetrieveMount> {
public:
  using FailingTransferMount::FailingTransferMount;
  std::atomic<unsigned int> jobDestructions = 0;
  std::string destination;
  unsigned int reservationAttempts = 0;
  unsigned int requeueAttempts = 0;

  bool needsJob() const {
    return failure == TransferFailurePoint::ReservationDenied || failure == TransferFailurePoint::ReservationCta
           || failure == TransferFailurePoint::ReservationStandard || failure == TransferFailurePoint::RequeueStandard
           || failure == TransferFailurePoint::TapeMountedCta || failure == TransferFailurePoint::TapeMountedStandard
           || failure == TransferFailurePoint::TapeMountedAndUnload
           || failure == TransferFailurePoint::TapeMountedAndComplete;
  }

  bool testReserveDiskSpace(const cta::DiskSpaceReservationRequest&, cta::log::LogContext&) override {
    ++reservationAttempts;
    if (failure == TransferFailurePoint::ReservationCta) {
      throw cta::exception::Exception("injected reservation failure");
    }
    if (failure == TransferFailurePoint::ReservationStandard) {
      throw std::runtime_error("injected reservation failure");
    }
    return failure != TransferFailurePoint::RequeueStandard && failure != TransferFailurePoint::ReservationDenied;
  }

  void requeueJobBatch(std::vector<std::unique_ptr<cta::RetrieveJob>>&, cta::log::LogContext&) override {
    ++requeueAttempts;
    if (failure == TransferFailurePoint::RequeueStandard) {
      throw std::runtime_error("injected requeue failure");
    }
  }

  void diskComplete() override { complete(); }

  std::list<std::unique_ptr<cta::RetrieveJob>> getNextJobBatch(uint64_t, uint64_t, cta::log::LogContext&) override {
    ++getJobs;
    if (failure == TransferFailurePoint::FetchDatabase) {
      throw cta::exception::LostDatabaseConnection("injected fetch disconnection");
    }
    if (failure == TransferFailurePoint::FetchCta) {
      throw cta::exception::Exception("injected fetch failure");
    }
    if (failure == TransferFailurePoint::FetchStandard) {
      throw std::runtime_error("injected fetch failure");
    }
    std::list<std::unique_ptr<cta::RetrieveJob>> jobs;
    if (getJobs == 1 && needsJob()) {
      auto job = std::make_unique<TransferJobWithDestructionCounter>(*this, jobDestructions);
      job->retrieveRequest.dstURL = destination;
      jobs.emplace_back(std::move(job));
    }
    return jobs;
  }
};

class FailingTransferScheduler : public cta::Scheduler {
public:
  using cta::Scheduler::Scheduler;
  TransferFailurePoint failure = TransferFailurePoint::Metadata;
  unsigned int downAttempts = 0;
  unsigned int upAttempts = 0;
  unsigned int desiredDownAttempts = 0;
  std::optional<std::string> downReason;

  void reportDriveStatus(const cta::common::dataStructures::DriveInfo&,
                         cta::common::dataStructures::MountType,
                         cta::common::dataStructures::DriveStatus status,
                         cta::log::LogContext&) override {
    if (status == cta::common::dataStructures::DriveStatus::Down) {
      ++downAttempts;
      if (failure == TransferFailurePoint::ReportDown) {
        throw std::runtime_error("injected reported down failure");
      }
    } else if (status == cta::common::dataStructures::DriveStatus::Up) {
      ++upAttempts;
      if (failure == TransferFailurePoint::ReportUp) {
        throw std::runtime_error("injected reported up failure");
      }
    }
  }

  void setDesiredDriveState(const std::string&,
                            const cta::common::dataStructures::DesiredDriveState& state,
                            cta::log::LogContext&) override {
    EXPECT_FALSE(state.up);
    ++desiredDownAttempts;
    downReason = state.reason;
    if (failure == TransferFailurePoint::DesiredDown) {
      throw std::runtime_error("injected desired down failure");
    }
  }
};

class TransferDriveWithDestructionCounter : public cta::tape::drive::FakeDrive {
public:
  explicit TransferDriveWithDestructionCounter(unsigned int& counter) : m_counter(counter) {}

  ~TransferDriveWithDestructionCounter() override { ++m_counter; }

private:
  unsigned int& m_counter;
};

size_t transferTestThreadCount() {
  const auto closeTasks = [](DIR* tasks) { closedir(tasks); };
  std::unique_ptr<DIR, decltype(closeTasks)> tasks(opendir("/proc/self/task"), closeTasks);
  if (!tasks) {
    throw std::runtime_error("Cannot inspect session thread cleanup");
  }
  size_t count = 0;
  while (auto* entry = readdir(tasks.get())) {
    if (entry->d_name[0] != '.') {
      ++count;
    }
  }
  return count;
}

/**
 * The data transfer test is a parameterized test.  It takes a pair of name server
 * and scheduler database factories as a parameter.
 */
class TapeSessionTest : public ::testing::TestWithParam<TapeSessionTestParam> {
public:
  TapeSessionTest() : m_dummyLog("dummy", "dummy") {}

  class FailedToGetCatalogue : public std::exception {
  public:
    const char* what() const noexcept { return "Failed to get catalogue"; }
  };

  class FailedToGetScheduler : public std::exception {
  public:
    const char* what() const noexcept { return "Failed to get scheduler"; }
  };

#undef USE_ORACLE_CATALOGUE
#ifdef USE_ORACLE_CATALOGUE
  class OracleCatalogueExposingConnection : public cta::catalogue::OracleCatalogue {
  public:
    template<typename... Ts>
    OracleCatalogueExposingConnection(Ts&... args) : cta::catalogue::OracleCatalogue(args...) {}

    cta::rdbms::Conn getConn() { return m_connPool.getConn(); }
  };
#endif

  void SetUp() override {
    using namespace cta;

    const TapeSessionTestParam& param = GetParam();
    const uint64_t nbConns = 1;
    const uint64_t nbArchiveFileListingConns = 1;
#ifdef USE_ORACLE_CATALOGUE
    cta::rdbms::Login login = cta::rdbms::Login::parseFile("/etc/cta/cta-catalogue.conf");

    m_catalogue = std::make_unique<OracleCatalogueExposingConnection>(m_dummyLog,
                                                                      login,
                                                                      nbConns,
                                                                      nbArchiveFileListingConns,
                                                                      maxTriesToConnect);
    try {
      // If we decide to create an oracle catalogue, we have to prepare it.
      // This is a striped down version of CreateSchemaCmd.
      OracleCatalogueExposingConnection& oracleCatalogue =
        dynamic_cast<OracleCatalogueExposingConnection&>(*m_catalogue);
      auto conn = oracleCatalogue.getConn();
      for (auto& name : conn.getTableNames()) {
        if (name == "CTA_CATALOGUE") {
          throw cta::exception::Exception("In SetUp(): schema is already populated.");
        }
      }
      cta::catalogue::OracleCatalogueSchema schema;
      conn.executeNonQueries(schema.sql);
    } catch (std::bad_cast&) {}
#else
    //m_catalogue = std::make_unique<catalogue::SchemaCreatingSqliteCatalogue>(m_tempSqliteFile.path(), nbConns);
    m_catalogue = std::make_unique<catalogue::InMemoryCatalogue>(m_dummyLog, nbConns, nbArchiveFileListingConns);
#endif
    m_db = param.dbFactory.create(m_catalogue);
    // These tests exercise transfers, including single-file failures, as soon as work is queued.
    m_scheduler = std::make_unique<Scheduler>(*m_catalogue, *m_db, "schedulerBackendName", 1);

    strncpy(m_tmpDir, "/tmp/TapeSessionTestXXXXXX", sizeof(m_tmpDir));
    if (!mkdtemp(m_tmpDir)) {
      const std::string errMsg = cta::utils::errnoToString(errno);
      std::ostringstream msg;
      msg << "Failed to create directory with template"
             " /tmp/TapeSessionTestXXXXXX: "
          << errMsg;
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
      std::unique_ptr<DIR, std::function<int(DIR*)>> dir(opendir(m_tmpDir), closedir);
      if (nullptr == dir) {
        const std::string errMsg = cta::utils::errnoToString(errno);
        std::ostringstream msg;
        msg << "Failed to open directory " << m_tmpDir << ": " << errMsg;
        throw cta::exception::Exception(msg.str());
      }

      // Delete each of the files within the directory
      struct dirent* entry = nullptr;
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
    cta::catalogue::Catalogue* const ptr = m_catalogue.get();
    if (nullptr == ptr) {
      throw FailedToGetCatalogue();
    }
    return *ptr;
  }

  cta::Scheduler& getScheduler() {
    cta::Scheduler* const ptr = m_scheduler.get();
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

  const cta::common::dataStructures::DiskInstance getDefaultDiskInstance() const {
    cta::common::dataStructures::DiskInstance di;
    di.name = s_diskInstance;
    di.comment = "comment";
    return di;
  }

  cta::common::dataStructures::VirtualOrganization getDefaultVirtualOrganization() {
    cta::common::dataStructures::VirtualOrganization vo;
    vo.name = "vo";
    vo.readMaxDrives = 1;
    vo.writeMaxDrives = 1;
    vo.maxFileSize = 0;
    vo.comment = "comment";
    vo.diskInstanceName = getDefaultDiskInstance().name;
    vo.isRepackVo = false;
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

    ASSERT_TRUE(catalogue.MountPolicy()->getMountPolicies().empty());

    catalogue.MountPolicy()->createMountPolicy(s_adminOnAdminHost, mountPolicy);

    const auto groups = catalogue.MountPolicy()->getMountPolicies();
    ASSERT_EQ(1, groups.size());
    auto& group = groups.front();
    ASSERT_EQ(mountPolicyName, group.name);
    ASSERT_EQ(archivePriority, group.archivePriority);
    ASSERT_EQ(minArchiveRequestAge, group.archiveMinRequestAge);
    ASSERT_EQ(retrievePriority, group.retrievePriority);
    ASSERT_EQ(minRetrieveRequestAge, group.retrieveMinRequestAge);
    ASSERT_EQ(mountPolicyComment, group.comment);

    const auto di = getDefaultDiskInstance();
    catalogue.DiskInstance()->createDiskInstance(s_adminOnAdminHost, di.name, di.comment);

    const std::string ruleComment = "create requester mount-rule";
    catalogue.RequesterMountRule()->createRequesterMountRule(s_adminOnAdminHost,
                                                             mountPolicyName,
                                                             di.name,
                                                             s_userName,
                                                             ruleComment);

    const auto rules = catalogue.RequesterMountRule()->getRequesterMountRules();
    ASSERT_EQ(1, rules.size());

    auto& rule = rules.front();

    ASSERT_EQ(s_userName, rule.name);
    ASSERT_EQ(mountPolicyName, rule.mountPolicy);
    ASSERT_EQ(ruleComment, rule.comment);
    ASSERT_EQ(s_adminOnAdminHost.username, rule.creationLog.username);
    ASSERT_EQ(s_adminOnAdminHost.host, rule.creationLog.host);
    ASSERT_EQ(rule.creationLog, rule.lastModificationLog);

    cta::common::dataStructures::VirtualOrganization vo = getDefaultVirtualOrganization();
    catalogue.VO()->createVirtualOrganization(s_adminOnAdminHost, vo);

    common::dataStructures::StorageClass storageClass;
    storageClass.name = s_storageClassName;
    storageClass.nbCopies = 1;
    storageClass.vo.name = vo.name;
    storageClass.comment = "create storage class";
    m_catalogue->StorageClass()->createStorageClass(s_adminOnAdminHost, storageClass);

    const uint16_t nbPartialTapes = 1;
    const std::string tapePoolComment = "Tape-pool comment";
    const std::optional<std::string> encryptionKeyName = std::nullopt;
    const std::vector<std::string> tapePoolSupplyList;

    ASSERT_NO_THROW(catalogue.TapePool()->createTapePool(s_adminOnAdminHost,
                                                         s_tapePoolName,
                                                         vo.name,
                                                         nbPartialTapes,
                                                         encryptionKeyName,
                                                         tapePoolSupplyList,
                                                         tapePoolComment));
    const uint32_t copyNb = 1;
    const std::string archiveRouteComment = "Archive-route comment";
    catalogue.ArchiveRoute()->createArchiveRoute(s_adminOnAdminHost,
                                                 s_storageClassName,
                                                 copyNb,
                                                 cta::common::dataStructures::ArchiveRouteType::DEFAULT,
                                                 s_tapePoolName,
                                                 archiveRouteComment);

    cta::catalogue::MediaType mediaType;
    mediaType.name = s_mediaType;
    mediaType.capacityInBytes = 12345678;
    mediaType.cartridge = "cartridge";
    mediaType.minLPos = 2696;
    mediaType.maxLPos = 171097;
    mediaType.nbWraps = 112;
    mediaType.comment = "comment";
    catalogue.MediaType()->createMediaType(s_adminOnAdminHost, mediaType);

    const std::string driveName = "T10D6116";
    const auto tapeDrive = getDefaultTapeDrive(driveName);
    catalogue.DriveState()->createTapeDrive(tapeDrive);
  }

  /**
   * Returns the map of Fseqs given by RAO from a string containing CTA logs
   * @param log the string containing the CTA logs
   * @return the map that gives for each RAO call, the associated ordered Fseqs according to the RAO algorithm result
   */
  std::map<size_t, std::vector<std::string>> getRAOFseqs(const std::string& log) {
    std::map<size_t, std::vector<std::string>> ret;
    size_t i = 0;
    for (size_t endPos, logPos = 0; logPos != std::string::npos; logPos = endPos) {
      if (log.find("Recall order of FSEQs") == std::string::npos) {
        endPos = log.find('\n', logPos);
        continue;
      };
      if (log.find("useRAO=\"true\"") == std::string::npos) {
        endPos = log.find('\n', logPos);
        continue;
      };
      logPos = log.find("recallOrder=", logPos);
      if (logPos == std::string::npos) {
        endPos = logPos;
        continue;
      }
      logPos = log.find('\"', logPos);
      if (logPos == std::string::npos) {
        break;
      }
      endPos = log.find('\"', ++logPos);
      if (endPos == logPos) {
        endPos = log.find('\n', logPos);
        continue;
      }
      auto strFseq = log.substr(logPos, endPos - logPos);
      cta::utils::splitString(strFseq, ' ', ret[i++]);
      // Move to next line
      endPos = log.find('\n', logPos);
    }
    return ret;
  }

  template<typename Mount>
  void checkExceptionCleanup(TransferFailurePoint point, bool cleanupFails = false, bool repack = false) {
    // Bound failures involving virtual hardware and in-memory queues. Neither
    // a robot nor a disk server is contacted by these scenarios.
    alarm(5);
    const auto threadsBefore = transferTestThreadCount();
    cta::log::StringLogger logger("dummy", "transferFailureTest", cta::log::DEBUG);
    FailingTransferScheduler scheduler(getCatalogue(), *m_db, "schedulerBackendName");
    scheduler.failure = point;
    Mount mount(getCatalogue(), point);
    mount.repack = repack;
    const bool fatal = point == TransferFailurePoint::MetadataAllocation || point == TransferFailurePoint::MetadataLogic
                       || point == TransferFailurePoint::MetadataUnknown;
    const bool startupFails = fatal || point == TransferFailurePoint::Metadata
                              || point == TransferFailurePoint::MetadataDatabase
                              || point == TransferFailurePoint::StartingStatus;
    const bool workerStarts =
      point == TransferFailurePoint::TapeMountedCta || point == TransferFailurePoint::TapeMountedStandard
      || point == TransferFailurePoint::TapeMountedAndUnload || point == TransferFailurePoint::TapeMountedAndComplete;
    if constexpr (std::is_same_v<Mount, FailingTransferRetrieveMount>) {
      mount.destination = "file://" + std::string(m_tmpDir) + "/failed-recall";
    }
    cta::tape::System::mockWrapper system;
    system.delegateToFake();
    system.disableGMockCallsCounting();
    system.fake.setupForVirtualDriveSLC6();
    cta::common::dataStructures::DriveInfo info("T10D6116",
                                                "host",
                                                "TestLogicalLibrary",
                                                "/dev/tape_T10D6116",
                                                "dummy");
    const bool discoveryFails = point == TransferFailurePoint::Discovery
                                || point == TransferFailurePoint::DeviceEnumeration
                                || point == TransferFailurePoint::MissingDrive;
    const bool openFails = point == TransferFailurePoint::OpenCta || point == TransferFailurePoint::OpenStandard
                           || point == TransferFailurePoint::ReportDown || point == TransferFailurePoint::DesiredDown;
    unsigned int driveDestructions = 0;
    if (point == TransferFailurePoint::DeviceEnumeration) {
      EXPECT_CALL(system, opendir(testing::_))
        .WillOnce(testing::Throw(std::runtime_error("injected discovery failure")));
    } else if (point == TransferFailurePoint::MissingDrive) {
      system.fake.m_stats.at(info.devFilename).st_rdev = static_cast<dev_t>(-1);
    } else if (discoveryFails) {
      system.fake.m_stats.erase(info.devFilename);
    } else if (openFails) {
      EXPECT_CALL(system, getDriveByPath("/dev/nst0"))
        .WillOnce(testing::Invoke([point](const std::string&) -> cta::tape::drive::DriveInterface* {
          if (point == TransferFailurePoint::OpenCta) {
            throw cta::exception::Exception("injected drive open failure");
          }
          throw std::runtime_error("injected drive open failure");
        }));
    } else if (!startupFails) {
      auto* drive = new TransferDriveWithDestructionCounter(driveDestructions);
      if (cleanupFails) {
        drive->setFailurePoint(cta::tape::drive::FakeDrive::FailurePoint::DisableLogicalBlockProtection);
      }
      if (point == TransferFailurePoint::TapeMountedAndUnload) {
        drive->setFailurePoint(cta::tape::drive::FakeDrive::FailurePoint::UnloadTape);
      }
      system.fake.m_pathToDrive["/dev/nst0"] = drive;
    }
    TransfersConfig config;
    config.buffer_count = 2;
    config.buffer_size_bytes = 1024;
    config.disk_io_threads = 1;
    config.archive.fetch_max_files = 1;
    config.archive.fetch_max_bytes = 1024;
    config.retrieve.fetch_max_files = 1;
    config.retrieve.fetch_max_bytes = 1024;
    config.retrieve.rao.enabled = false;
    config.encryption.enabled = false;
    config.no_block_move_timeout_secs = 600;
    cta::mediachanger::RmcProxy proxy;
    cta::mediachanger::MediaChangerFacade changer(proxy, logger);
    std::optional<TapeSessionResult> result;
    TapeSession session(logger, system, info, changer, mount, config, 1, scheduler);
    const auto& tracker = session.tracker();
    EXPECT_FALSE(tracker.state().has_value());
    EXPECT_EQ(&mount, tracker.mount());
    // Operational failures return recovery decisions; fatal failures retain their original type.
    if (point == TransferFailurePoint::MetadataAllocation) {
      EXPECT_THROW(session.execute(), std::bad_alloc);
    } else if (point == TransferFailurePoint::MetadataLogic) {
      EXPECT_THROW(session.execute(), std::logic_error);
    } else if (point == TransferFailurePoint::MetadataUnknown) {
      try {
        session.execute();
        ADD_FAILURE() << "Expected the injected non-standard exception";
      } catch (int value) {
        EXPECT_EQ(42, value);
      } catch (...) {
        ADD_FAILURE() << "Fatal exception type was not preserved";
      }
    } else {
      EXPECT_NO_THROW(result = session.execute());
      EXPECT_TRUE(result.has_value());
      EXPECT_EQ(cta::tape::session::TapeSessionState::Finished, tracker.state());
    }
    EXPECT_EQ(point != TransferFailurePoint::None && point != TransferFailurePoint::ReservationDenied,
              tracker.outcomeSnapshot().hasFailures);
    if (startupFails) {
      EXPECT_FALSE(tracker.mountAttempted());
      EXPECT_EQ(0, driveDestructions);
    }
    // Check cleanup even if a recoverable case incorrectly throws and leaves no result.
    if (result) {
      EXPECT_EQ(!tracker.hasFailures(), result->successful);
      EXPECT_EQ(!(discoveryFails || openFails || cleanupFails), result->driveReusable);
    }
    // The mount is borrowed. Finalize it once, including when startup or a
    // publication throws, and keep it alive until every worker has stopped.
    EXPECT_EQ(1, mount.completionAttempts);
    EXPECT_EQ(threadsBefore, transferTestThreadCount());
    EXPECT_EQ(&mount, tracker.mount());
    if (!startupFails) {
      const auto expectedType = std::is_same_v<Mount, FailingTransferRetrieveMount> ?
                                  cta::tape::session::SessionType::Retrieve :
                                  cta::tape::session::SessionType::Archive;
      EXPECT_EQ(expectedType, tracker.type());
      EXPECT_EQ(1, countLogMessages(logger.getLog(), "Tape session finished"));
      EXPECT_GE(mount.statsReports, 1U);
      EXPECT_EQ(tracker.stats().tape.filesCount, mount.lastReportedStats.filesCount);
    }
    if (point == TransferFailurePoint::None || point == TransferFailurePoint::Discovery) {
      EXPECT_EQ(cta::tape::session::TapeSessionState::Finished, tracker.state());
      EXPECT_EQ(0, tracker.stats().tape.filesCount);
      EXPECT_EQ(0, tracker.stats().tape.dataVolume);
      EXPECT_FALSE(tracker.progress().fileBeingMoved);
      EXPECT_FALSE(tracker.mountAttempted());
      EXPECT_EQ(point != TransferFailurePoint::None, tracker.outcomeSnapshot().hasFailures);
      if (point == TransferFailurePoint::None) {
        EXPECT_EQ(1, tracker.outcomeSnapshot().events[static_cast<size_t>(TapeSessionEvent::EmptyMount)]);
        EXPECT_NE(std::string::npos, logger.getLog().find("Info_emptyMount"));
      } else {
        EXPECT_TRUE(tracker.hasFailures());
      }
    }
    if (cleanupFails) {
      ASSERT_TRUE(mount.downReason);
      EXPECT_THAT(*mount.downReason, testing::HasSubstr("[cta-taped] ERROR Drive cleanup failed: "));
      EXPECT_THAT(*mount.downReason, testing::HasSubstr("Failed to disable logical block protection"));
    }
    if (discoveryFails || openFails) {
      EXPECT_EQ(1, scheduler.downAttempts);
      EXPECT_EQ(1, scheduler.desiredDownAttempts);
      ASSERT_TRUE(scheduler.downReason);
      EXPECT_THAT(*scheduler.downReason, testing::HasSubstr("[cta-taped] ERROR Session drive access failed: "));
      std::string_view stage = "Configured drive lookup failed: Could not stat path";
      if (openFails) {
        stage = "Drive opening failed: injected drive open failure";
      } else if (point == TransferFailurePoint::DeviceEnumeration) {
        stage = "Drive discovery failed: injected discovery failure";
      } else if (point == TransferFailurePoint::MissingDrive) {
        stage = "Configured drive lookup failed: Could not find tape device";
      }
      EXPECT_THAT(*scheduler.downReason, testing::HasSubstr(std::string(stage)));
    } else if (!startupFails) {
      EXPECT_EQ(1, driveDestructions);
      if (!workerStarts) {
        EXPECT_EQ(1, scheduler.upAttempts);
      }
    }
    if constexpr (std::is_same_v<Mount, FailingTransferRetrieveMount>) {
      if (mount.needsJob()) {
        EXPECT_EQ(1, mount.jobDestructions.load());
        EXPECT_EQ(1, mount.reservationAttempts);
      }
      if (point == TransferFailurePoint::RequeueStandard || point == TransferFailurePoint::ReservationDenied) {
        EXPECT_EQ(1, mount.requeueAttempts);
      }
      if (point == TransferFailurePoint::ReservationDenied) {
        EXPECT_FALSE(tracker.mountAttempted());
        EXPECT_FALSE(tracker.hasFailures());
        EXPECT_EQ(
          1,
          tracker.outcomeSnapshot().events[static_cast<size_t>(TapeSessionEvent::DiskSpaceReservationTestFailure)]);
      }
      if (workerStarts) {
        EXPECT_EQ(1, mount.mountedAttempts);
        EXPECT_NE(std::string::npos, logger.getLog().find("injected mounted publication failure"));
        if (point == TransferFailurePoint::TapeMountedAndUnload) {
          EXPECT_EQ(1, tracker.failureStats().at(TapeSessionFailure::TapeUnload));
        } else {
          EXPECT_NE(std::string::npos, logger.getLog().find("Cleaner dismounted tape"));
        }
      }
      if (point == TransferFailurePoint::FetchCta || point == TransferFailurePoint::FetchStandard
          || point == TransferFailurePoint::FetchDatabase) {
        EXPECT_EQ(1, mount.getJobs);
      }
    }
    if constexpr (std::is_same_v<Mount, FailingTransferMount<cta::MockArchiveMount>>) {
      if (point == TransferFailurePoint::FetchCta || point == TransferFailurePoint::FetchStandard
          || point == TransferFailurePoint::FetchDatabase) {
        EXPECT_EQ(1, mount.archiveFetchAttempts());
      }
    }
    if constexpr (std::is_same_v<Mount, FailingTransferMount<cta::MockArchiveMount>>) {
      if (workerStarts) {
        EXPECT_EQ(1, mount.archiveJobDestructions());
        EXPECT_EQ(1, mount.mountedAttempts);
        EXPECT_NE(std::string::npos, logger.getLog().find("injected mounted publication failure"));
        if (point == TransferFailurePoint::TapeMountedAndUnload) {
          EXPECT_EQ(1, tracker.failureStats().at(TapeSessionFailure::TapeUnload));
        } else {
          EXPECT_NE(std::string::npos, logger.getLog().find("Cleaner dismounted tape"));
        }
      }
    }
    const auto* testResult = ::testing::UnitTest::GetInstance()->current_test_info()->result();
    for (int i = 0; i < testResult->total_part_count(); ++i) {
      const auto& part = testResult->GetTestPartResult(i);
      if (part.failed()) {
        std::fprintf(stderr, "%s\n", part.summary());
      }
    }
    alarm(0);
  }

private:
  // Prevent copying
  TapeSessionTest(const TapeSessionTest&) = delete;

  // Prevent assignment
  TapeSessionTest& operator=(const TapeSessionTest&) = delete;

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
  const std::string s_vid = "TSTVID";  // We really need size <= 6 characters due to tape label format.
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

};  // class TapeSessionTest

/*
 * If an archive mount has no jobs, the session finishes without loading tape.
 * The mount is still completed and no worker is left running.
 */
TEST_P(TapeSessionTest, ArchiveEmptyMountCleansUp) {
  ::testing::FLAGS_gtest_death_test_style = "threadsafe";
  ASSERT_EXIT(
    {
      checkExceptionCleanup<FailingTransferMount<cta::MockArchiveMount>>(TransferFailurePoint::None);
      _exit(::testing::Test::HasFailure() ? 1 : 0);
    },
    testing::ExitedWithCode(0),
    "");
}

/*
 * If a retrieve mount has no jobs, the session finishes without loading tape.
 * The mount is still completed and no worker is left running.
 */
TEST_P(TapeSessionTest, RetrieveEmptyMountCleansUp) {
  ::testing::FLAGS_gtest_death_test_style = "threadsafe";
  ASSERT_EXIT(
    {
      checkExceptionCleanup<FailingTransferRetrieveMount>(TransferFailurePoint::None);
      _exit(::testing::Test::HasFailure() ? 1 : 0);
    },
    testing::ExitedWithCode(0),
    "");
}

TEST_P(TapeSessionTest, ArchiveMetadataDisconnectionReturnsRecovery) {
  ::testing::FLAGS_gtest_death_test_style = "threadsafe";
  ASSERT_EXIT(
    {
      checkExceptionCleanup<FailingTransferMount<cta::MockArchiveMount>>(TransferFailurePoint::MetadataDatabase);
      _exit(::testing::Test::HasFailure() ? 1 : 0);
    },
    testing::ExitedWithCode(0),
    "");
}

TEST_P(TapeSessionTest, ArchiveFetchDisconnectionReturnsRecovery) {
  ::testing::FLAGS_gtest_death_test_style = "threadsafe";
  ASSERT_EXIT(
    {
      checkExceptionCleanup<FailingTransferMount<cta::MockArchiveMount>>(TransferFailurePoint::FetchDatabase);
      _exit(::testing::Test::HasFailure() ? 1 : 0);
    },
    testing::ExitedWithCode(0),
    "");
}

TEST_P(TapeSessionTest, ArchiveCompletionDisconnectionReturnsRecovery) {
  ::testing::FLAGS_gtest_death_test_style = "threadsafe";
  ASSERT_EXIT(
    {
      checkExceptionCleanup<FailingTransferMount<cta::MockArchiveMount>>(TransferFailurePoint::CompleteDatabase);
      _exit(::testing::Test::HasFailure() ? 1 : 0);
    },
    testing::ExitedWithCode(0),
    "");
}

TEST_P(TapeSessionTest, ArchiveAllocationFailureBeforeMountFinalizes) {
  ::testing::FLAGS_gtest_death_test_style = "threadsafe";
  ASSERT_EXIT(
    {
      checkExceptionCleanup<FailingTransferMount<cta::MockArchiveMount>>(TransferFailurePoint::MetadataAllocation);
      _exit(::testing::Test::HasFailure() ? 1 : 0);
    },
    testing::ExitedWithCode(0),
    "");
}

TEST_P(TapeSessionTest, ArchiveLogicFailureBeforeMountFinalizes) {
  ::testing::FLAGS_gtest_death_test_style = "threadsafe";
  ASSERT_EXIT(
    {
      checkExceptionCleanup<FailingTransferMount<cta::MockArchiveMount>>(TransferFailurePoint::MetadataLogic);
      _exit(::testing::Test::HasFailure() ? 1 : 0);
    },
    testing::ExitedWithCode(0),
    "");
}

TEST_P(TapeSessionTest, ArchiveUnknownFailureBeforeMountFinalizes) {
  ::testing::FLAGS_gtest_death_test_style = "threadsafe";
  ASSERT_EXIT(
    {
      checkExceptionCleanup<FailingTransferMount<cta::MockArchiveMount>>(TransferFailurePoint::MetadataUnknown);
      _exit(::testing::Test::HasFailure() ? 1 : 0);
    },
    testing::ExitedWithCode(0),
    "");
}

TEST_P(TapeSessionTest, RetrieveMetadataDisconnectionReturnsRecovery) {
  ::testing::FLAGS_gtest_death_test_style = "threadsafe";
  ASSERT_EXIT(
    {
      checkExceptionCleanup<FailingTransferRetrieveMount>(TransferFailurePoint::MetadataDatabase);
      _exit(::testing::Test::HasFailure() ? 1 : 0);
    },
    testing::ExitedWithCode(0),
    "");
}

TEST_P(TapeSessionTest, RetrieveFetchDisconnectionReturnsRecovery) {
  ::testing::FLAGS_gtest_death_test_style = "threadsafe";
  ASSERT_EXIT(
    {
      checkExceptionCleanup<FailingTransferRetrieveMount>(TransferFailurePoint::FetchDatabase);
      _exit(::testing::Test::HasFailure() ? 1 : 0);
    },
    testing::ExitedWithCode(0),
    "");
}

TEST_P(TapeSessionTest, RetrieveCompletionDisconnectionReturnsRecovery) {
  ::testing::FLAGS_gtest_death_test_style = "threadsafe";
  ASSERT_EXIT(
    {
      checkExceptionCleanup<FailingTransferRetrieveMount>(TransferFailurePoint::CompleteDatabase);
      _exit(::testing::Test::HasFailure() ? 1 : 0);
    },
    testing::ExitedWithCode(0),
    "");
}

TEST_P(TapeSessionTest, RetrieveAllocationFailureBeforeMountFinalizes) {
  ::testing::FLAGS_gtest_death_test_style = "threadsafe";
  ASSERT_EXIT(
    {
      checkExceptionCleanup<FailingTransferRetrieveMount>(TransferFailurePoint::MetadataAllocation);
      _exit(::testing::Test::HasFailure() ? 1 : 0);
    },
    testing::ExitedWithCode(0),
    "");
}

TEST_P(TapeSessionTest, RetrieveLogicFailureBeforeMountFinalizes) {
  ::testing::FLAGS_gtest_death_test_style = "threadsafe";
  ASSERT_EXIT(
    {
      checkExceptionCleanup<FailingTransferRetrieveMount>(TransferFailurePoint::MetadataLogic);
      _exit(::testing::Test::HasFailure() ? 1 : 0);
    },
    testing::ExitedWithCode(0),
    "");
}

TEST_P(TapeSessionTest, RetrieveUnknownFailureBeforeMountFinalizes) {
  ::testing::FLAGS_gtest_death_test_style = "threadsafe";
  ASSERT_EXIT(
    {
      checkExceptionCleanup<FailingTransferRetrieveMount>(TransferFailurePoint::MetadataUnknown);
      _exit(::testing::Test::HasFailure() ? 1 : 0);
    },
    testing::ExitedWithCode(0),
    "");
}

/*
 * If archive mount metadata throws, the session exits cleanly.
 * Mount finalization must run even before hardware setup starts.
 */
TEST_P(TapeSessionTest, ArchiveMetadataFailureCleansUp) {
  ::testing::FLAGS_gtest_death_test_style = "threadsafe";
  ASSERT_EXIT(
    {
      checkExceptionCleanup<FailingTransferMount<cta::MockArchiveMount>>(TransferFailurePoint::Metadata);
      _exit(::testing::Test::HasFailure() ? 1 : 0);
    },
    testing::ExitedWithCode(0),
    "");
}

/*
 * If publishing archive startup status throws, the session exits cleanly.
 * Mount finalization must run even before hardware setup starts.
 */
TEST_P(TapeSessionTest, ArchiveStartingStatusFailureCleansUp) {
  ::testing::FLAGS_gtest_death_test_style = "threadsafe";
  ASSERT_EXIT(
    {
      checkExceptionCleanup<FailingTransferMount<cta::MockArchiveMount>>(TransferFailurePoint::StartingStatus);
      _exit(::testing::Test::HasFailure() ? 1 : 0);
    },
    testing::ExitedWithCode(0),
    "");
}

TEST_P(TapeSessionTest, RepackMountTypeIsVisibleUntilSessionFinishes) {
  ::testing::FLAGS_gtest_death_test_style = "threadsafe";
  ASSERT_EXIT(
    {
      checkExceptionCleanup<FailingTransferMount<cta::MockArchiveMount>>(TransferFailurePoint::None, false, true);
      _exit(::testing::Test::HasFailure() ? 1 : 0);
    },
    testing::ExitedWithCode(0),
    "");
}

// Verify operation categories retain the underlying failure details.
TEST_P(TapeSessionTest, ArchiveDeviceEnumerationReason) {
  ::testing::FLAGS_gtest_death_test_style = "threadsafe";
  ASSERT_EXIT(
    {
      checkExceptionCleanup<FailingTransferMount<cta::MockArchiveMount>>(TransferFailurePoint::DeviceEnumeration);
      _exit(::testing::Test::HasFailure() ? 1 : 0);
    },
    testing::ExitedWithCode(0),
    "");
}

TEST_P(TapeSessionTest, ArchiveMissingDriveReason) {
  ::testing::FLAGS_gtest_death_test_style = "threadsafe";
  ASSERT_EXIT(
    {
      checkExceptionCleanup<FailingTransferMount<cta::MockArchiveMount>>(TransferFailurePoint::MissingDrive);
      _exit(::testing::Test::HasFailure() ? 1 : 0);
    },
    testing::ExitedWithCode(0),
    "");
}

TEST_P(TapeSessionTest, ArchiveCleanupFailureReason) {
  ::testing::FLAGS_gtest_death_test_style = "threadsafe";
  ASSERT_EXIT(
    {
      checkExceptionCleanup<FailingTransferMount<cta::MockArchiveMount>>(TransferFailurePoint::TapeMountedStandard,
                                                                         true);
      _exit(::testing::Test::HasFailure() ? 1 : 0);
    },
    testing::ExitedWithCode(0),
    "");
}

TEST_P(TapeSessionTest, RetrieveDeviceEnumerationReason) {
  ::testing::FLAGS_gtest_death_test_style = "threadsafe";
  ASSERT_EXIT(
    {
      checkExceptionCleanup<FailingTransferRetrieveMount>(TransferFailurePoint::DeviceEnumeration);
      _exit(::testing::Test::HasFailure() ? 1 : 0);
    },
    testing::ExitedWithCode(0),
    "");
}

TEST_P(TapeSessionTest, RetrieveMissingDriveReason) {
  ::testing::FLAGS_gtest_death_test_style = "threadsafe";
  ASSERT_EXIT(
    {
      checkExceptionCleanup<FailingTransferRetrieveMount>(TransferFailurePoint::MissingDrive);
      _exit(::testing::Test::HasFailure() ? 1 : 0);
    },
    testing::ExitedWithCode(0),
    "");
}

TEST_P(TapeSessionTest, RetrieveCleanupFailureReason) {
  ::testing::FLAGS_gtest_death_test_style = "threadsafe";
  ASSERT_EXIT(
    {
      checkExceptionCleanup<FailingTransferRetrieveMount>(TransferFailurePoint::TapeMountedStandard, true);
      _exit(::testing::Test::HasFailure() ? 1 : 0);
    },
    testing::ExitedWithCode(0),
    "");
}

/*
 * If archive drive discovery fails, the session requests drive down.
 * The mount must still complete without leaving workers behind.
 */
TEST_P(TapeSessionTest, ArchiveDiscoveryFailureCleansUp) {
  ::testing::FLAGS_gtest_death_test_style = "threadsafe";
  ASSERT_EXIT(
    {
      checkExceptionCleanup<FailingTransferMount<cta::MockArchiveMount>>(TransferFailurePoint::Discovery);
      _exit(::testing::Test::HasFailure() ? 1 : 0);
    },
    testing::ExitedWithCode(0),
    "");
}

/*
 * If opening the archive drive throws a CTA exception, the session requests drive down.
 * The mount must still complete without leaving workers behind.
 */
TEST_P(TapeSessionTest, ArchiveOpenCtaFailureCleansUp) {
  ::testing::FLAGS_gtest_death_test_style = "threadsafe";
  ASSERT_EXIT(
    {
      checkExceptionCleanup<FailingTransferMount<cta::MockArchiveMount>>(TransferFailurePoint::OpenCta);
      _exit(::testing::Test::HasFailure() ? 1 : 0);
    },
    testing::ExitedWithCode(0),
    "");
}

/*
 * If opening the archive drive throws a standard exception, the session requests drive down.
 * The mount must still complete without leaving workers behind.
 */
TEST_P(TapeSessionTest, ArchiveOpenStandardFailureCleansUp) {
  ::testing::FLAGS_gtest_death_test_style = "threadsafe";
  ASSERT_EXIT(
    {
      checkExceptionCleanup<FailingTransferMount<cta::MockArchiveMount>>(TransferFailurePoint::OpenStandard);
      _exit(::testing::Test::HasFailure() ? 1 : 0);
    },
    testing::ExitedWithCode(0),
    "");
}

/*
 * If archive mount completion throws a CTA exception, the session exits cleanly.
 * It must attempt completion only once and release its workers.
 */
TEST_P(TapeSessionTest, ArchiveCompleteCtaFailureCleansUp) {
  ::testing::FLAGS_gtest_death_test_style = "threadsafe";
  ASSERT_EXIT(
    {
      checkExceptionCleanup<FailingTransferMount<cta::MockArchiveMount>>(TransferFailurePoint::CompleteCta);
      _exit(::testing::Test::HasFailure() ? 1 : 0);
    },
    testing::ExitedWithCode(0),
    "");
}

/*
 * If archive mount completion throws a standard exception, the session exits cleanly.
 * It must attempt completion only once and release its workers.
 */
TEST_P(TapeSessionTest, ArchiveCompleteStandardFailureCleansUp) {
  ::testing::FLAGS_gtest_death_test_style = "threadsafe";
  ASSERT_EXIT(
    {
      checkExceptionCleanup<FailingTransferMount<cta::MockArchiveMount>>(TransferFailurePoint::CompleteStandard);
      _exit(::testing::Test::HasFailure() ? 1 : 0);
    },
    testing::ExitedWithCode(0),
    "");
}

/*
 * If archive drive-down publication fails during open, cleanup still runs.
 * The session must not leave the mount or workers active.
 */
TEST_P(TapeSessionTest, ArchiveReportDownFailureCleansUp) {
  ::testing::FLAGS_gtest_death_test_style = "threadsafe";
  ASSERT_EXIT(
    {
      checkExceptionCleanup<FailingTransferMount<cta::MockArchiveMount>>(TransferFailurePoint::ReportDown);
      _exit(::testing::Test::HasFailure() ? 1 : 0);
    },
    testing::ExitedWithCode(0),
    "");
}

/*
 * If requesting archive drive down fails during open, cleanup still runs.
 * The session must not leave the mount or workers active.
 */
TEST_P(TapeSessionTest, ArchiveDesiredDownFailureCleansUp) {
  ::testing::FLAGS_gtest_death_test_style = "threadsafe";
  ASSERT_EXIT(
    {
      checkExceptionCleanup<FailingTransferMount<cta::MockArchiveMount>>(TransferFailurePoint::DesiredDown);
      _exit(::testing::Test::HasFailure() ? 1 : 0);
    },
    testing::ExitedWithCode(0),
    "");
}

/*
 * If archive drive-up publication fails, the session still finalizes the mount.
 * The failure must not leave a worker running.
 */
TEST_P(TapeSessionTest, ArchiveReportUpFailureCleansUp) {
  ::testing::FLAGS_gtest_death_test_style = "threadsafe";
  ASSERT_EXIT(
    {
      checkExceptionCleanup<FailingTransferMount<cta::MockArchiveMount>>(TransferFailurePoint::ReportUp);
      _exit(::testing::Test::HasFailure() ? 1 : 0);
    },
    testing::ExitedWithCode(0),
    "");
}

/*
 * If retrieve mount metadata throws, the session exits cleanly.
 * Mount finalization must run even before hardware setup starts.
 */
TEST_P(TapeSessionTest, RetrieveMetadataFailureCleansUp) {
  ::testing::FLAGS_gtest_death_test_style = "threadsafe";
  ASSERT_EXIT(
    {
      checkExceptionCleanup<FailingTransferRetrieveMount>(TransferFailurePoint::Metadata);
      _exit(::testing::Test::HasFailure() ? 1 : 0);
    },
    testing::ExitedWithCode(0),
    "");
}

/*
 * If publishing retrieve startup status throws, the session exits cleanly.
 * Mount finalization must run even before hardware setup starts.
 */
TEST_P(TapeSessionTest, RetrieveStartingStatusFailureCleansUp) {
  ::testing::FLAGS_gtest_death_test_style = "threadsafe";
  ASSERT_EXIT(
    {
      checkExceptionCleanup<FailingTransferRetrieveMount>(TransferFailurePoint::StartingStatus);
      _exit(::testing::Test::HasFailure() ? 1 : 0);
    },
    testing::ExitedWithCode(0),
    "");
}

/*
 * If retrieve drive discovery fails, the session requests drive down.
 * The mount must still complete without leaving workers behind.
 */
TEST_P(TapeSessionTest, RetrieveDiscoveryFailureCleansUp) {
  ::testing::FLAGS_gtest_death_test_style = "threadsafe";
  ASSERT_EXIT(
    {
      checkExceptionCleanup<FailingTransferRetrieveMount>(TransferFailurePoint::Discovery);
      _exit(::testing::Test::HasFailure() ? 1 : 0);
    },
    testing::ExitedWithCode(0),
    "");
}

/*
 * If opening the retrieve drive throws a CTA exception, the session requests drive down.
 * The mount must still complete without leaving workers behind.
 */
TEST_P(TapeSessionTest, RetrieveOpenCtaFailureCleansUp) {
  ::testing::FLAGS_gtest_death_test_style = "threadsafe";
  ASSERT_EXIT(
    {
      checkExceptionCleanup<FailingTransferRetrieveMount>(TransferFailurePoint::OpenCta);
      _exit(::testing::Test::HasFailure() ? 1 : 0);
    },
    testing::ExitedWithCode(0),
    "");
}

/*
 * If opening the retrieve drive throws a standard exception, the session requests drive down.
 * The mount must still complete without leaving workers behind.
 */
TEST_P(TapeSessionTest, RetrieveOpenStandardFailureCleansUp) {
  ::testing::FLAGS_gtest_death_test_style = "threadsafe";
  ASSERT_EXIT(
    {
      checkExceptionCleanup<FailingTransferRetrieveMount>(TransferFailurePoint::OpenStandard);
      _exit(::testing::Test::HasFailure() ? 1 : 0);
    },
    testing::ExitedWithCode(0),
    "");
}

/*
 * If retrieve mount completion throws a CTA exception, the session exits cleanly.
 * It must attempt completion only once and release its workers.
 */
TEST_P(TapeSessionTest, RetrieveCompleteCtaFailureCleansUp) {
  ::testing::FLAGS_gtest_death_test_style = "threadsafe";
  ASSERT_EXIT(
    {
      checkExceptionCleanup<FailingTransferRetrieveMount>(TransferFailurePoint::CompleteCta);
      _exit(::testing::Test::HasFailure() ? 1 : 0);
    },
    testing::ExitedWithCode(0),
    "");
}

/*
 * If retrieve mount completion throws a standard exception, the session exits cleanly.
 * It must attempt completion only once and release its workers.
 */
TEST_P(TapeSessionTest, RetrieveCompleteStandardFailureCleansUp) {
  ::testing::FLAGS_gtest_death_test_style = "threadsafe";
  ASSERT_EXIT(
    {
      checkExceptionCleanup<FailingTransferRetrieveMount>(TransferFailurePoint::CompleteStandard);
      _exit(::testing::Test::HasFailure() ? 1 : 0);
    },
    testing::ExitedWithCode(0),
    "");
}

/*
 * If retrieve drive-down publication fails during open, cleanup still runs.
 * The session must not leave the mount or workers active.
 */
TEST_P(TapeSessionTest, RetrieveReportDownFailureCleansUp) {
  ::testing::FLAGS_gtest_death_test_style = "threadsafe";
  ASSERT_EXIT(
    {
      checkExceptionCleanup<FailingTransferRetrieveMount>(TransferFailurePoint::ReportDown);
      _exit(::testing::Test::HasFailure() ? 1 : 0);
    },
    testing::ExitedWithCode(0),
    "");
}

/*
 * If requesting retrieve drive down fails during open, cleanup still runs.
 * The session must not leave the mount or workers active.
 */
TEST_P(TapeSessionTest, RetrieveDesiredDownFailureCleansUp) {
  ::testing::FLAGS_gtest_death_test_style = "threadsafe";
  ASSERT_EXIT(
    {
      checkExceptionCleanup<FailingTransferRetrieveMount>(TransferFailurePoint::DesiredDown);
      _exit(::testing::Test::HasFailure() ? 1 : 0);
    },
    testing::ExitedWithCode(0),
    "");
}

/*
 * If retrieve drive-up publication fails, the session still finalizes the mount.
 * The failure must not leave a worker running.
 */
TEST_P(TapeSessionTest, RetrieveReportUpFailureCleansUp) {
  ::testing::FLAGS_gtest_death_test_style = "threadsafe";
  ASSERT_EXIT(
    {
      checkExceptionCleanup<FailingTransferRetrieveMount>(TransferFailurePoint::ReportUp);
      _exit(::testing::Test::HasFailure() ? 1 : 0);
    },
    testing::ExitedWithCode(0),
    "");
}

/*
 * If fetching retrieve jobs throws a CTA exception, the session completes cleanup.
 * The failed fetch must not retain a job or worker.
 */
TEST_P(TapeSessionTest, RetrieveFetchCtaFailureCleansUp) {
  ::testing::FLAGS_gtest_death_test_style = "threadsafe";
  ASSERT_EXIT(
    {
      checkExceptionCleanup<FailingTransferRetrieveMount>(TransferFailurePoint::FetchCta);
      _exit(::testing::Test::HasFailure() ? 1 : 0);
    },
    testing::ExitedWithCode(0),
    "");
}

/*
 * If fetching retrieve jobs throws a standard exception, the session completes cleanup.
 * The failed fetch must not retain a job or worker.
 */
TEST_P(TapeSessionTest, RetrieveFetchStandardFailureCleansUp) {
  ::testing::FLAGS_gtest_death_test_style = "threadsafe";
  ASSERT_EXIT(
    {
      checkExceptionCleanup<FailingTransferRetrieveMount>(TransferFailurePoint::FetchStandard);
      _exit(::testing::Test::HasFailure() ? 1 : 0);
    },
    testing::ExitedWithCode(0),
    "");
}

/*
 * If fetching archive jobs throws a CTA exception, the session completes cleanup.
 * The failed fetch must not retain a job or worker.
 */
TEST_P(TapeSessionTest, ArchiveFetchCtaFailureCleansUp) {
  ::testing::FLAGS_gtest_death_test_style = "threadsafe";
  ASSERT_EXIT(
    {
      checkExceptionCleanup<FailingTransferMount<cta::MockArchiveMount>>(TransferFailurePoint::FetchCta);
      _exit(::testing::Test::HasFailure() ? 1 : 0);
    },
    testing::ExitedWithCode(0),
    "");
}

/*
 * If fetching archive jobs throws a standard exception, the session completes cleanup.
 * The failed fetch must not retain a job or worker.
 */
TEST_P(TapeSessionTest, ArchiveFetchStandardFailureCleansUp) {
  ::testing::FLAGS_gtest_death_test_style = "threadsafe";
  ASSERT_EXIT(
    {
      checkExceptionCleanup<FailingTransferMount<cta::MockArchiveMount>>(TransferFailurePoint::FetchStandard);
      _exit(::testing::Test::HasFailure() ? 1 : 0);
    },
    testing::ExitedWithCode(0),
    "");
}

TEST_P(TapeSessionTest, RetrieveReservationDeniedReturnsSuccess) {
  ::testing::FLAGS_gtest_death_test_style = "threadsafe";
  ASSERT_EXIT(
    {
      checkExceptionCleanup<FailingTransferRetrieveMount>(TransferFailurePoint::ReservationDenied);
      _exit(::testing::Test::HasFailure() ? 1 : 0);
    },
    testing::ExitedWithCode(0),
    "");
}

/*
 * If retrieve disk reservation throws a CTA exception, the session completes cleanup.
 * The failed reservation must not retain a job or worker.
 */
TEST_P(TapeSessionTest, RetrieveReservationCtaFailureCleansUp) {
  ::testing::FLAGS_gtest_death_test_style = "threadsafe";
  ASSERT_EXIT(
    {
      checkExceptionCleanup<FailingTransferRetrieveMount>(TransferFailurePoint::ReservationCta);
      _exit(::testing::Test::HasFailure() ? 1 : 0);
    },
    testing::ExitedWithCode(0),
    "");
}

/*
 * If retrieve disk reservation throws a standard exception, the session completes cleanup.
 * The failed reservation must not retain a job or worker.
 */
TEST_P(TapeSessionTest, RetrieveReservationStandardFailureCleansUp) {
  ::testing::FLAGS_gtest_death_test_style = "threadsafe";
  ASSERT_EXIT(
    {
      checkExceptionCleanup<FailingTransferRetrieveMount>(TransferFailurePoint::ReservationStandard);
      _exit(::testing::Test::HasFailure() ? 1 : 0);
    },
    testing::ExitedWithCode(0),
    "");
}

/*
 * If requeueing a retrieve job throws, the session completes cleanup.
 * The failed requeue must not retain a job or worker.
 */
TEST_P(TapeSessionTest, RetrieveRequeueStandardFailureCleansUp) {
  ::testing::FLAGS_gtest_death_test_style = "threadsafe";
  ASSERT_EXIT(
    {
      checkExceptionCleanup<FailingTransferRetrieveMount>(TransferFailurePoint::RequeueStandard);
      _exit(::testing::Test::HasFailure() ? 1 : 0);
    },
    testing::ExitedWithCode(0),
    "");
}

/*
 * If retrieve tape-mounted publication throws a CTA exception, cleanup still runs.
 * The retrieve job and worker must be released.
 */
TEST_P(TapeSessionTest, RetrieveTapeMountedCtaFailureCleansUp) {
  ::testing::FLAGS_gtest_death_test_style = "threadsafe";
  ASSERT_EXIT(
    {
      checkExceptionCleanup<FailingTransferRetrieveMount>(TransferFailurePoint::TapeMountedCta);
      _exit(::testing::Test::HasFailure() ? 1 : 0);
    },
    testing::ExitedWithCode(0),
    "");
}

/*
 * If retrieve tape-mounted publication throws a standard exception, cleanup still runs.
 * The retrieve job and worker must be released.
 */
TEST_P(TapeSessionTest, RetrieveTapeMountedStandardFailureCleansUp) {
  ::testing::FLAGS_gtest_death_test_style = "threadsafe";
  ASSERT_EXIT(
    {
      checkExceptionCleanup<FailingTransferRetrieveMount>(TransferFailurePoint::TapeMountedStandard);
      _exit(::testing::Test::HasFailure() ? 1 : 0);
    },
    testing::ExitedWithCode(0),
    "");
}

/*
 * If retrieve tape-mounted publication and tape unloading both fail, cleanup still runs.
 * Successful robotic dismount permits reuse; the unload error must still be recorded.
 */
TEST_P(TapeSessionTest, RetrieveTapeMountedAndUnloadFailureCleansUp) {
  ::testing::FLAGS_gtest_death_test_style = "threadsafe";
  ASSERT_EXIT(
    {
      checkExceptionCleanup<FailingTransferRetrieveMount>(TransferFailurePoint::TapeMountedAndUnload);
      _exit(::testing::Test::HasFailure() ? 1 : 0);
    },
    testing::ExitedWithCode(0),
    "");
}

/*
 * If archive tape-mounted publication throws a CTA exception, cleanup still runs.
 * The archive job and worker must be released.
 */
TEST_P(TapeSessionTest, ArchiveTapeMountedCtaFailureCleansUp) {
  ::testing::FLAGS_gtest_death_test_style = "threadsafe";
  ASSERT_EXIT(
    {
      checkExceptionCleanup<FailingTransferMount<cta::MockArchiveMount>>(TransferFailurePoint::TapeMountedCta);
      _exit(::testing::Test::HasFailure() ? 1 : 0);
    },
    testing::ExitedWithCode(0),
    "");
}

/*
 * If archive tape-mounted publication throws a standard exception, cleanup still runs.
 * The archive job and worker must be released.
 */
TEST_P(TapeSessionTest, ArchiveTapeMountedStandardFailureCleansUp) {
  ::testing::FLAGS_gtest_death_test_style = "threadsafe";
  ASSERT_EXIT(
    {
      checkExceptionCleanup<FailingTransferMount<cta::MockArchiveMount>>(TransferFailurePoint::TapeMountedStandard);
      _exit(::testing::Test::HasFailure() ? 1 : 0);
    },
    testing::ExitedWithCode(0),
    "");
}

/*
 * If archive tape-mounted publication and tape unloading both fail, cleanup still runs.
 * Successful robotic dismount permits reuse; the unload error must still be recorded.
 */
TEST_P(TapeSessionTest, ArchiveTapeMountedAndUnloadFailureCleansUp) {
  ::testing::FLAGS_gtest_death_test_style = "threadsafe";
  ASSERT_EXIT(
    {
      checkExceptionCleanup<FailingTransferMount<cta::MockArchiveMount>>(TransferFailurePoint::TapeMountedAndUnload);
      _exit(::testing::Test::HasFailure() ? 1 : 0);
    },
    testing::ExitedWithCode(0),
    "");
}

// Completion reporting must not undo ejection after an earlier mounted-session failure.
TEST_P(TapeSessionTest, ArchiveCompletionFailureAfterMountCleansUp) {
  ::testing::FLAGS_gtest_death_test_style = "threadsafe";
  ASSERT_EXIT(
    (checkExceptionCleanup<FailingTransferMount<cta::MockArchiveMount>>(TransferFailurePoint::TapeMountedAndComplete),
     _exit(::testing::Test::HasFailure() ? 1 : 0)),
    ::testing::ExitedWithCode(0),
    "");
}

TEST_P(TapeSessionTest, RetrieveCompletionFailureAfterMountCleansUp) {
  ::testing::FLAGS_gtest_death_test_style = "threadsafe";
  ASSERT_EXIT((checkExceptionCleanup<FailingTransferRetrieveMount>(TransferFailurePoint::TapeMountedAndComplete),
               _exit(::testing::Test::HasFailure() ? 1 : 0)),
              ::testing::ExitedWithCode(0),
              "");
}

// Verify the public timing fields at all three reporting levels against the legacy sum.
void checkTransferTimingLogs(const std::string& output, const std::string& fileMessage) {
  unsigned files = 0, threads = 0, sessions = 0;
  for (const auto& line : cta::utils::splitStringToVector(output, '\n')) {
    const bool file = line.find("MSG=\"" + fileMessage + "\"") != std::string::npos;
    const bool thread = line.find("MSG=\"Tape thread complete\"") != std::string::npos;
    const bool session = line.find("MSG=\"Tape session finished\"") != std::string::npos;
    if (!file && !thread && !session) {
      continue;
    }
    SCOPED_TRACE(line);
    const auto value = [&](const std::string& field) {
      const auto pos = line.find(" " + field + "=\"");
      return pos == std::string::npos ? 0.0 : std::stod(line.substr(pos + field.size() + 3));
    };
    const double expected = value("checksumingTime") + value("readWriteTime") + value("flushTime")
                            + value("waitDataTime") + value("waitFreeMemoryTime") + value("waitInstructionsTime")
                            + value("waitReportingTime");
    EXPECT_GT(value("waitReportingTime"), 0);
    // Text logs round each floating-point field independently.
    EXPECT_NEAR(expected, value("transferTime"), expected * 0.00001);
    files += file;
    threads += thread;
    sessions += session;
  }
  EXPECT_GT(files, 0);
  EXPECT_EQ(1, threads);
  EXPECT_EQ(1, sessions);
}

/*
 * If retrieve requests target valid tape files, the session recalls them successfully.
 * The test checks the resulting disk files and completed scheduler jobs.
 */
TEST_P(TapeSessionTest, TapeSessionGooddayRecall) {
  // 0) Prepare the logger for everyone
  cta::log::StringLogger logger("dummy", "tapedUnitTest", cta::log::DEBUG);
  cta::log::LogContext logContext(logger);

  setupDefaultCatalogue();
  // 1) prepare the fake scheduler
  std::string vid = s_vid;
  // cta::MountType::Enum mountType = cta::MountType::RETRIEVE;

  // 3) Prepare the necessary environment (logger, plus system wrapper),
  cta::tape::System::mockWrapper mockSys;
  mockSys.delegateToFake();
  mockSys.disableGMockCallsCounting();
  mockSys.fake.setupForVirtualDriveSLC6();
  //delete is unnecessary
  //pointer with ownership will be passed to the application,
  //which will do the delete
  mockSys.fake.m_pathToDrive["/dev/nst0"] = new cta::tape::drive::FakeDrive;

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
  std::optional<std::string> physicalLibraryName;
  catalogue.LogicalLibrary()->createLogicalLibrary(s_adminOnAdminHost,
                                                   s_libraryName,
                                                   libraryIsDisabled,
                                                   physicalLibraryName,
                                                   libraryComment);
  {
    auto libraries = catalogue.LogicalLibrary()->getLogicalLibraries();
    ASSERT_EQ(1, libraries.size());
    ASSERT_EQ(s_libraryName, libraries.front().name);
    ASSERT_EQ(libraryComment, libraries.front().comment);
  }

  {
    auto tape = getDefaultTape();
    catalogue.Tape()->createTape(s_adminOnAdminHost, tape);
  }

  // 6) Prepare files for reading by writing them to the mock system
  {
    // Label the tape
    cta::tape::tapeFile::LabelSession::label(mockSys.fake.m_pathToDrive["/dev/nst0"], s_vid, false);
    mockSys.fake.m_pathToDrive["/dev/nst0"]->rewind();
    // And write to it
    cta::tape::daemon::VolumeInfo volInfo;
    volInfo.vid = s_vid;
    auto writeSession = std::make_unique<cta::tape::tapeFile::WriteSession>(*mockSys.fake.m_pathToDrive["/dev/nst0"],
                                                                            volInfo,
                                                                            0,
                                                                            true,
                                                                            false);

    // Write a few files on the virtual tape and modify the archive name space
    // so that it is in sync
    uint8_t data[1000];
    bool doSleep = false;  // set to true to debug entrypoint with gdb
    size_t archiveFileSize = sizeof(data);
    cta::tape::SCSI::Structures::zeroStruct(&data);
    for (int fseq = 1; fseq <= 10; fseq++) {
      // Create a path to a remote destination file
      std::ostringstream remoteFilePath;
      remoteFilePath << "file://" << m_tmpDir << "/test" << fseq;
      remoteFilePaths.push_back(remoteFilePath.str());

      // Create an archive file entry in the archive namespace
      auto tapeFileWrittenUP = std::make_unique<cta::catalogue::TapeFileWritten>();
      auto& tapeFileWritten = *tapeFileWrittenUP;
      std::set<cta::catalogue::TapeItemWrittenPointer> tapeFileWrittenSet;
      tapeFileWrittenSet.insert(tapeFileWrittenUP.release());

      // Write the file to tape
      cta::MockArchiveMount mam(catalogue);
      std::unique_ptr<cta::ArchiveJob> aj(new cta::MockArchiveJob(&mam, catalogue));
      aj->tapeFile.fSeq = fseq;
      aj->archiveFile.archiveFileID = fseq;
      cta::tape::tapeFile::FileWriter writer(*writeSession, *aj, archiveFileSize);
      tapeFileWritten.blockId = writer.getBlockId();
      // Write the data (one block)
      writer.write(data, archiveFileSize);
      // Close the file
      writer.close();

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
      catalogue.TapeFile()->filesWrittenToTape(tapeFileWrittenSet);

      // Schedule the retrieval of the file
      std::string diskInstance = s_diskInstance;
      cta::common::dataStructures::RetrieveRequest rReq;
      rReq.archiveFileID = fseq;
      rReq.requester.name = s_userName;
      rReq.requester.group = "someGroup";
      rReq.dstURL = remoteFilePaths.back();
      rReq.diskFileInfo.path = "path/to/file";
      rReq.isVerifyOnly = false;
      // Populate creationLog (required for PostgreSQL scheduler)
      rReq.creationLog.username = s_userName;
      rReq.creationLog.host = "test-host";
      rReq.creationLog.time = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
      // Populate errorReportURL (required for PostgreSQL scheduler - cannot be empty)
      rReq.errorReportURL = "test://error-report-url";
      std::list<std::string> archiveFilePaths;
      // add a sleep here so I can have enough time to attach to the test!!
      while (doSleep) {
        sleep(5);
      }
      scheduler.queueRetrieve(diskInstance, rReq, logContext);
    }
  }
  scheduler.waitSchedulerDbSubthreadsComplete();

  // 6) Report the drive's existence and put it up in the drive register.
  cta::common::dataStructures::DriveInfo driveInfo("T10D6116",
                                                   "host",
                                                   "TestLogicalLibrary",
                                                   "/dev/tape_T10D6116",
                                                   "dummy");
  // We need to create the drive in the registry before being able to put it up.
  scheduler.reportDriveStatus(driveInfo,
                              cta::common::dataStructures::MountType::NoMount,
                              cta::common::dataStructures::DriveStatus::Down,
                              logContext);
  cta::common::dataStructures::DesiredDriveState driveState;
  driveState.up = true;
  driveState.forceDown = false;
  scheduler.setDesiredDriveState(driveInfo.driveName, driveState, logContext);

  // 7) Create the data transfer session
  TransfersConfig dataTransferConf;
  dataTransferConf.buffer_count = 0;
  dataTransferConf.buffer_size_bytes = 0;
  dataTransferConf.disk_io_threads = 0;
  dataTransferConf.archive.fetch_max_bytes = 0;
  dataTransferConf.archive.fetch_max_files = 0;
  dataTransferConf.archive.flush_max_bytes = 0;
  dataTransferConf.archive.flush_max_files = 0;
  dataTransferConf.archive.underfill.watch_period_secs = 0;
  dataTransferConf.archive.underfill.minimum_samples = 0;
  dataTransferConf.archive.underfill.start_threshold_percent = 0;
  dataTransferConf.archive.underfill.recovery_threshold_percent = 0;
  dataTransferConf.retrieve.fetch_max_bytes = 0;
  dataTransferConf.retrieve.fetch_max_files = 0;
  dataTransferConf.retrieve.rao.enabled = false;
  dataTransferConf.retrieve.rao.lto_algorithm.clear();
  dataTransferConf.encryption.external_key_script.clear();
  dataTransferConf.no_block_move_timeout_secs = 600;
  uint32_t tapeLoadTimeoutSecs = 300;
  dataTransferConf.buffer_size_bytes = 1024 * 1024;  // 1 MB memory buffers
  dataTransferConf.buffer_count = 10;
  dataTransferConf.retrieve.fetch_max_bytes = UINT64_C(100) * 1000 * 1000 * 1000;
  dataTransferConf.retrieve.fetch_max_files = 1000;
  dataTransferConf.disk_io_threads = 1;
  tapeLoadTimeoutSecs = 300;
  dataTransferConf.encryption.enabled = false;
  dataTransferConf.no_block_move_timeout_secs = 600;
  cta::log::DummyLogger dummyLog("dummy", "dummy");
  cta::mediachanger::RmcProxy rmcProxy;
  cta::mediachanger::MediaChangerFacade mc(rmcProxy, dummyLog);
  auto tapeMount = scheduler.getNextMount(driveInfo.logicalLibrary, driveInfo.driveName, logContext);
  ASSERT_NE(nullptr, tapeMount) << logger.getLog();
  cta::tape::daemon::TapeSession
    sess(logger, mockSys, driveInfo, mc, *tapeMount, dataTransferConf, tapeLoadTimeoutSecs, scheduler);
  // 8) Run the data transfer session
  sess.execute();

  // The real read path must publish its final counters before reporting TapeSession completion.
  const auto& tracker = sess.tracker();
  EXPECT_EQ(cta::tape::session::SessionType::Retrieve, tracker.type());
  EXPECT_EQ(cta::tape::session::TapeSessionState::Finished, tracker.state());
  EXPECT_EQ(remoteFilePaths.size(), tracker.stats().tape.filesCount);
  EXPECT_EQ(1000 * remoteFilePaths.size(), tracker.stats().tape.dataVolume);
  EXPECT_FALSE(tracker.hasFailures());
  EXPECT_FALSE(tracker.progress().fileBeingMoved);
  EXPECT_TRUE(tracker.activeDiskFiles().empty());
  EXPECT_EQ(1, countLogMessages(logger.getLog(), "Tape session finished"));
  EXPECT_NE(std::string::npos, logger.getLog().find("filesCount=\"10\""));
  EXPECT_NE(std::string::npos, logger.getLog().find("dataVolume=\"10000\""));

  // 9) Check the session git the correct VID
  ASSERT_EQ(s_vid, sess.getVid());

  // 10) Check the remote files exist and have the correct size
  for (const auto& path : remoteFilePaths) {
    struct stat statBuf;
    memset(&statBuf, 0, sizeof(statBuf));
    const int statRc = stat(path.substr(7).c_str(), &statBuf);  //remove the "file://" for stat-ing
    ASSERT_EQ(0, statRc);
    ASSERT_EQ(1000, statBuf.st_size);  //same size of data
  }

  // 10) Check logs
  // 10) Check logs
  std::string logToCheck = logger.getLog();
  checkTransferTimingLogs(logToCheck, "File successfully read from tape");
  auto logLines = cta::utils::splitStringToVector(logToCheck, '\n');

  // Check if any of the lines contains all of these substrings
  ASSERT_TRUE(std::ranges::any_of(logLines, [](std::string_view line) {
    return cta::utils::containsAllSubStrings(line,
                                             {
                                               R"(MSG="Tape session started for read")",
                                               R"(thread="TapeRead")",
                                               R"(tapeDrive="T10D6116")",
                                               R"(tapeVid="TSTVID")",
                                               R"(mountId="1")",
                                               R"(vo="vo")",
                                               R"(tapePool="TestTapePool")",
                                               R"(mediaType="LTO7M")",
                                               R"(logicalLibrary="TestLogicalLibrary")",
                                               R"(mountType="Retrieve")",
                                               R"(labelFormat="0000")",
                                               R"(vendor="TestVendor")",
                                               R"(capacityInBytes="12345678")",
                                             });
  }));

  ASSERT_TRUE(std::ranges::any_of(logLines, [](std::string_view line) {
    return cta::utils::containsAllSubStrings(line,
                                             {
                                               R"(firmwareVersion="123A")",
                                               R"(serialNumber="123456")",
                                               R"(mountTotalCorrectedReadErrors="5")",
                                               R"(mountTotalReadBytesProcessed="4096")",
                                               R"(mountTotalUncorrectedReadErrors="1")",
                                               R"(mountTotalNonMediumErrorCounts="2")",
                                             });
  }));

  ASSERT_TRUE(std::ranges::any_of(logLines, [](std::string_view line) {
    return cta::utils::containsAllSubStrings(line,
                                             {
                                               R"(firmwareVersion="123A")",
                                               R"(serialNumber="123456")",
                                               R"(lifetimeMediumEfficiencyPrct="100.0")",
                                               R"(mountReadEfficiencyPrct="100.0")",
                                               R"(mountWriteEfficiencyPrct="100.0")",
                                               R"(mountReadTransients="10)",
                                               R"(mountServoTemps="10")",
                                               R"(mountServoTransients="5")",
                                               R"(mountTemps="100")",
                                               R"(mountTotalReadRetries="25")",
                                               R"(mountTotalWriteRetries="25")",
                                               R"(mountWriteTransients="10")",
                                             });
  }));
}

// this test will issue several retrieve requests it seems
/*
 * If a recalled file has the wrong checksum, the session reports the file failure.
 * Other retrieve requests must still be handled by the session.
 */
TEST_P(TapeSessionTest, TapeSessionWrongChecksumRecall) {
  // 0) Prepare the logger for everyone
  cta::log::StringLogger logger("dummy", "tapedUnitTest", cta::log::DEBUG);
  cta::log::LogContext logContext(logger);

  setupDefaultCatalogue();
  // 1) prepare the fake scheduler
  std::string vid = s_vid;
  // cta::MountType::Enum mountType = cta::MountType::RETRIEVE;

  // 3) Prepare the necessary environment (logger, plus system wrapper),
  cta::tape::System::mockWrapper mockSys;
  mockSys.delegateToFake();
  mockSys.disableGMockCallsCounting();
  mockSys.fake.setupForVirtualDriveSLC6();
  //delete is unnecessary
  //pointer with ownership will be passed to the application,
  //which will do the delete
  mockSys.fake.m_pathToDrive["/dev/nst0"] = new cta::tape::drive::FakeDrive;

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
  std::optional<std::string> physicalLibraryName;
  catalogue.LogicalLibrary()->createLogicalLibrary(s_adminOnAdminHost,
                                                   s_libraryName,
                                                   libraryIsDisabled,
                                                   physicalLibraryName,
                                                   libraryComment);
  {
    auto libraries = catalogue.LogicalLibrary()->getLogicalLibraries();
    ASSERT_EQ(1, libraries.size());
    ASSERT_EQ(s_libraryName, libraries.front().name);
    ASSERT_EQ(libraryComment, libraries.front().comment);
  }

  {
    auto tape = getDefaultTape();
    catalogue.Tape()->createTape(s_adminOnAdminHost, tape);
  }

  // 6) Prepare files for reading by writing them to the mock system
  {
    // Label the tape
    cta::tape::tapeFile::LabelSession::label(mockSys.fake.m_pathToDrive["/dev/nst0"], s_vid, false);
    mockSys.fake.m_pathToDrive["/dev/nst0"]->rewind();
    // And write to it
    cta::tape::daemon::VolumeInfo volInfo;
    volInfo.vid = s_vid;
    auto writeSession = std::make_unique<cta::tape::tapeFile::WriteSession>(*mockSys.fake.m_pathToDrive["/dev/nst0"],
                                                                            volInfo,
                                                                            0,
                                                                            true,
                                                                            false);

    // Write a few files on the virtual tape and modify the archive name space
    // so that it is in sync
    uint8_t data[1000];
    size_t archiveFileSize = sizeof(data);
    cta::tape::SCSI::Structures::zeroStruct(&data);
    for (int fseq = 1; fseq <= 10; fseq++) {
      // Create a path to a remote destination file
      std::ostringstream remoteFilePath;
      remoteFilePath << "file://" << m_tmpDir << "/test" << fseq;
      remoteFilePaths.push_back(remoteFilePath.str());

      // Create an archive file entry in the archive namespace
      auto tapeFileWrittenUP = std::make_unique<cta::catalogue::TapeFileWritten>();
      auto& tapeFileWritten = *tapeFileWrittenUP;
      std::set<cta::catalogue::TapeItemWrittenPointer> tapeFileWrittenSet;
      tapeFileWrittenSet.insert(tapeFileWrittenUP.release());

      // Write the file to tape
      cta::MockArchiveMount mam(catalogue);
      std::unique_ptr<cta::ArchiveJob> aj(new cta::MockArchiveJob(&mam, catalogue));
      aj->tapeFile.fSeq = fseq;
      aj->archiveFile.archiveFileID = fseq;
      cta::tape::tapeFile::FileWriter writer(*writeSession, *aj, archiveFileSize);
      tapeFileWritten.blockId = writer.getBlockId();
      // Write the data (one block)
      writer.write(data, archiveFileSize);
      // Close the file
      writer.close();

      // Create file entry in the archive namespace
      tapeFileWritten.archiveFileId = fseq;
      if (fseq == 4) {
        // Fourth file will have wrong checksum and will not be recalled
        tapeFileWritten.checksumBlob.insert(cta::checksum::ADLER32, cta::utils::getAdler32(data, archiveFileSize) + 1);
      } else {
        tapeFileWritten.checksumBlob.insert(cta::checksum::ADLER32, cta::utils::getAdler32(data, archiveFileSize));
      }
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
      catalogue.TapeFile()->filesWrittenToTape(tapeFileWrittenSet);

      // Schedule the retrieval of the file
      std::string diskInstance = s_diskInstance;
      cta::common::dataStructures::RetrieveRequest rReq;
      rReq.archiveFileID = fseq;
      rReq.requester.name = s_userName;
      rReq.requester.group = "someGroup";
      rReq.dstURL = remoteFilePaths.back();
      rReq.diskFileInfo.path = "path/to/file";
      rReq.isVerifyOnly = false;
      // Populate creationLog (required for PostgreSQL scheduler)
      rReq.creationLog.username = s_userName;
      rReq.creationLog.host = "test-host";
      rReq.creationLog.time = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
      // Populate errorReportURL (required for PostgreSQL scheduler - cannot be empty)
      rReq.errorReportURL = "test://error-report-url";
      std::list<std::string> archiveFilePaths;
      scheduler.queueRetrieve(diskInstance, rReq, logContext);
    }
  }
  scheduler.waitSchedulerDbSubthreadsComplete();

  // 6) Report the drive's existence and put it up in the drive register.
  cta::common::dataStructures::DriveInfo driveInfo("T10D6116",
                                                   "host",
                                                   "TestLogicalLibrary",
                                                   "/dev/tape_T10D6116",
                                                   "dummy");
  // We need to create the drive in the registry before being able to put it up.
  scheduler.reportDriveStatus(driveInfo,
                              cta::common::dataStructures::MountType::NoMount,
                              cta::common::dataStructures::DriveStatus::Down,
                              logContext);
  cta::common::dataStructures::DesiredDriveState driveState;
  driveState.up = true;
  driveState.forceDown = false;
  scheduler.setDesiredDriveState(driveInfo.driveName, driveState, logContext);

  // 7) Create the data transfer session
  TransfersConfig dataTransferConf;
  dataTransferConf.buffer_count = 0;
  dataTransferConf.buffer_size_bytes = 0;
  dataTransferConf.disk_io_threads = 0;
  dataTransferConf.archive.fetch_max_bytes = 0;
  dataTransferConf.archive.fetch_max_files = 0;
  dataTransferConf.archive.flush_max_bytes = 0;
  dataTransferConf.archive.flush_max_files = 0;
  dataTransferConf.archive.underfill.watch_period_secs = 0;
  dataTransferConf.archive.underfill.minimum_samples = 0;
  dataTransferConf.archive.underfill.start_threshold_percent = 0;
  dataTransferConf.archive.underfill.recovery_threshold_percent = 0;
  dataTransferConf.retrieve.fetch_max_bytes = 0;
  dataTransferConf.retrieve.fetch_max_files = 0;
  dataTransferConf.retrieve.rao.enabled = false;
  dataTransferConf.retrieve.rao.lto_algorithm.clear();
  dataTransferConf.encryption.external_key_script.clear();
  dataTransferConf.no_block_move_timeout_secs = 600;
  uint32_t tapeLoadTimeoutSecs = 300;
  dataTransferConf.buffer_size_bytes = 1024 * 1024;  // 1 MB memory buffers
  dataTransferConf.buffer_count = 10;
  dataTransferConf.retrieve.fetch_max_bytes = UINT64_C(100) * 1000 * 1000 * 1000;
  dataTransferConf.retrieve.fetch_max_files = 1000;
  dataTransferConf.disk_io_threads = 1;
  tapeLoadTimeoutSecs = 300;
  dataTransferConf.encryption.enabled = false;
  dataTransferConf.no_block_move_timeout_secs = 600;
  cta::log::DummyLogger dummyLog("dummy", "dummy");
  cta::mediachanger::RmcProxy rmcProxy;
  cta::mediachanger::MediaChangerFacade mc(rmcProxy, dummyLog);
  auto tapeMount = scheduler.getNextMount(driveInfo.logicalLibrary, driveInfo.driveName, logContext);
  ASSERT_NE(nullptr, tapeMount) << logger.getLog();
  cta::tape::daemon::TapeSession
    sess(logger, mockSys, driveInfo, mc, *tapeMount, dataTransferConf, tapeLoadTimeoutSecs, scheduler);

  // 8) Run the data transfer session
  const auto result = sess.execute();
  EXPECT_TRUE(sess.tracker().outcomeSnapshot().hasFailures);
  EXPECT_FALSE(result.successful);

  // 9) Check the session git the correct VID
  ASSERT_EQ(s_vid, sess.getVid());

  // 10) Check the remote files exist and have the correct size
  int fseq = 1;
  for (auto& path : remoteFilePaths) {
    struct stat statBuf;
    bzero(&statBuf, sizeof(statBuf));
    int statRc = stat(path.substr(7).c_str(), &statBuf);  //remove the "file://" for stat-ing
    // File with wrong checksum are not recalled
    // Rest of the files were read (correct behaviour, unlike archive)
    if (fseq == 4) {
      ASSERT_EQ(-1, statRc);
      ASSERT_EQ(errno, ENOENT);
    } else {
      ASSERT_EQ(0, statRc);
      ASSERT_EQ(1000, statBuf.st_size);  //files should be empty
    }
    fseq++;
  }

  // 10) Check logs
  std::string logToCheck = logger.getLog();
  auto logLines = cta::utils::splitStringToVector(logToCheck, '\n');

  // Check if any of the lines contains all of these substrings
  ASSERT_TRUE(std::ranges::any_of(logLines, [](std::string_view line) {
    return cta::utils::containsAllSubStrings(line,
                                             {
                                               R"(MSG="Tape session started for read")",
                                               R"(thread="TapeRead")",
                                               R"(tapeDrive="T10D6116")",
                                               R"(tapeVid="TSTVID")",
                                               R"(mountId="1")",
                                               R"(vo="vo")",
                                               R"(tapePool="TestTapePool")",
                                               R"(mediaType="LTO7M")",
                                               R"(logicalLibrary="TestLogicalLibrary")",
                                               R"(mountType="Retrieve")",
                                               R"(labelFormat="0000")",
                                               R"(vendor="TestVendor")",
                                               R"(capacityInBytes="12345678")",
                                             });
  }));

  ASSERT_TRUE(std::ranges::any_of(logLines, [](std::string_view line) {
    return cta::utils::containsAllSubStrings(line,
                                             {
                                               R"(firmwareVersion="123A")",
                                               R"(serialNumber="123456")",
                                               R"(mountTotalCorrectedReadErrors="5")",
                                               R"(mountTotalReadBytesProcessed="4096")",
                                               R"(mountTotalUncorrectedReadErrors="1")",
                                               R"(mountTotalNonMediumErrorCounts="2")",
                                             });
  }));

  ASSERT_TRUE(std::ranges::any_of(logLines, [](std::string_view line) {
    return cta::utils::containsAllSubStrings(line,
                                             {
                                               R"(firmwareVersion="123A")",
                                               R"(serialNumber="123456")",
                                               R"(lifetimeMediumEfficiencyPrct="100.0")",
                                               R"(mountReadEfficiencyPrct="100.0")",
                                               R"(mountWriteEfficiencyPrct="100.0")",
                                               R"(mountReadTransients="10)",
                                               R"(mountServoTemps="10")",
                                               R"(mountServoTransients="5")",
                                               R"(mountTemps="100")",
                                               R"(mountTotalReadRetries="25")",
                                               R"(mountTotalWriteRetries="25")",
                                               R"(mountWriteTransients="10")",
                                             });
  }));
}

// disk_file_path throws an exception in this test

/*
 * If recall parameters are wrong, the first retrieval fails and the next is cancelled.
 * The session must report both outcomes without treating them as successful recalls.
 */
TEST_P(TapeSessionTest, TapeSessionWrongRecall) {
  // This test is the same as TapeSessionGooddayRecall, with
  // wrong parameters set for the recall, so that we fail
  // to recall the first file and cancel the second.

  // 0) Prepare the logger for everyone
  cta::log::StringLogger logger("dummy", "tapedUnitTest", cta::log::DEBUG);
  cta::log::LogContext logContext(logger);

  setupDefaultCatalogue();
  // 1) prepare the fake scheduler
  std::string vid = s_vid;
  // cta::MountType::Enum mountType = cta::MountType::RETRIEVE;

  // 3) Prepare the necessary environment (logger, plus system wrapper),
  cta::tape::System::mockWrapper mockSys;
  mockSys.delegateToFake();
  mockSys.disableGMockCallsCounting();
  mockSys.fake.setupForVirtualDriveSLC6();
  //delete is unnecessary
  //pointer with ownership will be passed to the application,
  //which will do the delete
  mockSys.fake.m_pathToDrive["/dev/nst0"] = new cta::tape::drive::FakeDrive;

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
  std::optional<std::string> physicalLibraryName;
  catalogue.LogicalLibrary()->createLogicalLibrary(s_adminOnAdminHost,
                                                   s_libraryName,
                                                   libraryIsDisabled,
                                                   physicalLibraryName,
                                                   libraryComment);
  {
    auto libraries = catalogue.LogicalLibrary()->getLogicalLibraries();
    ASSERT_EQ(1, libraries.size());
    ASSERT_EQ(s_libraryName, libraries.front().name);
    ASSERT_EQ(libraryComment, libraries.front().comment);
  }

  {
    auto tape = getDefaultTape();
    catalogue.Tape()->createTape(s_adminOnAdminHost, tape);
  }

  // 6) Prepare files for reading by writing them to the mock system
  {
    // Label the tape
    cta::tape::tapeFile::LabelSession::label(mockSys.fake.m_pathToDrive["/dev/nst0"], s_vid, false);
    mockSys.fake.m_pathToDrive["/dev/nst0"]->rewind();
    // And write to it
    cta::tape::daemon::VolumeInfo volInfo;
    volInfo.vid = s_vid;
    auto writeSession = std::make_unique<cta::tape::tapeFile::WriteSession>(*mockSys.fake.m_pathToDrive["/dev/nst0"],
                                                                            volInfo,
                                                                            0,
                                                                            true,
                                                                            false);

    // Write a few files on the virtual tape and modify the archive name space
    // so that it is in sync
    uint8_t data[1000];
    cta::tape::SCSI::Structures::zeroStruct(&data);
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
      cta::tape::tapeFile::FileWriter writer(*writeSession, *aj, archiveFileSize);
      // Write the data (one block)
      writer.write(data, sizeof(data));
      // Close the file
      writer.close();

      {
        // Create a fictious file record on the tape to allow adding one to fseq=2 afterwards.
        auto tapeFileWrittenUP = std::make_unique<cta::catalogue::TapeFileWritten>();
        auto& tapeFileWritten = *tapeFileWrittenUP;
        std::set<cta::catalogue::TapeItemWrittenPointer> tapeFileWrittenSet;
        tapeFileWrittenSet.insert(tapeFileWrittenUP.release());
        tapeFileWritten.archiveFileId = 666;
        tapeFileWritten.checksumBlob.insert(cta::checksum::ADLER32,
                                            cta::checksum::ChecksumBlob::HexToByteArray("0xDEADBEEF"));
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
        catalogue.TapeFile()->filesWrittenToTape(tapeFileWrittenSet);
      }

      {
        // Create an archive file entry in the archive catalogue
        auto tapeFileWrittenUP = std::make_unique<cta::catalogue::TapeFileWritten>();
        auto& tapeFileWritten = *tapeFileWrittenUP;
        std::set<cta::catalogue::TapeItemWrittenPointer> tapeFileWrittenSet;
        tapeFileWrittenSet.insert(tapeFileWrittenUP.release());
        tapeFileWritten.archiveFileId = 1000 + fseq;
        tapeFileWritten.checksumBlob.insert(cta::checksum::ADLER32, cta::utils::getAdler32(data, archiveFileSize));
        tapeFileWritten.vid = volInfo.vid;
        tapeFileWritten.size = archiveFileSize;
        tapeFileWritten.fSeq = fseq + 1;
        tapeFileWritten.blockId = writer.getBlockId() + 10000;
        tapeFileWritten.copyNb = 1;
        tapeFileWritten.diskInstance = s_diskInstance;
        tapeFileWritten.diskFileId = std::to_string(fseq + 1);

        tapeFileWritten.diskFileOwnerUid = DISK_FILE_SOME_USER;
        tapeFileWritten.diskFileGid = DISK_FILE_SOME_GROUP;
        tapeFileWritten.storageClassName = s_storageClassName;
        tapeFileWritten.tapeDrive = "drive0";
        catalogue.TapeFile()->filesWrittenToTape(tapeFileWrittenSet);
      }

      // Schedule the retrieval of the file
      std::string diskInstance = s_diskInstance;
      cta::common::dataStructures::RetrieveRequest rReq;
      rReq.archiveFileID = 1000 + fseq;
      rReq.requester.name = s_userName;
      rReq.requester.group = "someGroup";
      rReq.dstURL = remoteFilePaths.back();
      rReq.diskFileInfo.path = "path/to/file";
      rReq.isVerifyOnly = false;
      std::list<std::string> archiveFilePaths;
      // Populate creationLog (required for PostgreSQL scheduler)
      rReq.creationLog.username = s_userName;
      rReq.creationLog.host = "test-host";
      rReq.creationLog.time = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
      // Populate errorReportURL (required for PostgreSQL scheduler - cannot be empty)
      rReq.errorReportURL = "test://error-report-url";
      scheduler.queueRetrieve(diskInstance, rReq, logContext);
    }
  }
  scheduler.waitSchedulerDbSubthreadsComplete();

  // 6) Report the drive's existence and put it up in the drive register.
  cta::common::dataStructures::DriveInfo driveInfo("T10D6116",
                                                   "host",
                                                   "TestLogicalLibrary",
                                                   "/dev/tape_T10D6116",
                                                   "dummy");
  // We need to create the drive in the registry before being able to put it up.
  scheduler.reportDriveStatus(driveInfo,
                              cta::common::dataStructures::MountType::NoMount,
                              cta::common::dataStructures::DriveStatus::Down,
                              logContext);
  cta::common::dataStructures::DesiredDriveState driveState;
  driveState.up = true;
  driveState.forceDown = false;
  scheduler.setDesiredDriveState(driveInfo.driveName, driveState, logContext);

  // 7) Create the data transfer session
  TransfersConfig dataTransferConf;
  dataTransferConf.buffer_count = 0;
  dataTransferConf.buffer_size_bytes = 0;
  dataTransferConf.disk_io_threads = 0;
  dataTransferConf.archive.fetch_max_bytes = 0;
  dataTransferConf.archive.fetch_max_files = 0;
  dataTransferConf.archive.flush_max_bytes = 0;
  dataTransferConf.archive.flush_max_files = 0;
  dataTransferConf.archive.underfill.watch_period_secs = 0;
  dataTransferConf.archive.underfill.minimum_samples = 0;
  dataTransferConf.archive.underfill.start_threshold_percent = 0;
  dataTransferConf.archive.underfill.recovery_threshold_percent = 0;
  dataTransferConf.retrieve.fetch_max_bytes = 0;
  dataTransferConf.retrieve.fetch_max_files = 0;
  dataTransferConf.retrieve.rao.enabled = false;
  dataTransferConf.retrieve.rao.lto_algorithm.clear();
  dataTransferConf.encryption.external_key_script.clear();
  dataTransferConf.no_block_move_timeout_secs = 600;
  uint32_t tapeLoadTimeoutSecs = 300;
  dataTransferConf.buffer_size_bytes = 1024 * 1024;  // 1 MB memory buffers
  dataTransferConf.buffer_count = 10;
  dataTransferConf.retrieve.fetch_max_bytes = UINT64_C(100) * 1000 * 1000 * 1000;
  dataTransferConf.retrieve.fetch_max_files = 1000;
  dataTransferConf.disk_io_threads = 1;
  tapeLoadTimeoutSecs = 300;
  dataTransferConf.encryption.enabled = false;
  dataTransferConf.no_block_move_timeout_secs = 600;
  cta::log::DummyLogger dummyLog("dummy", "dummy");
  cta::mediachanger::RmcProxy rmcProxy;
  cta::mediachanger::MediaChangerFacade mc(rmcProxy, dummyLog);
  auto tapeMount = scheduler.getNextMount(driveInfo.logicalLibrary, driveInfo.driveName, logContext);
  ASSERT_NE(nullptr, tapeMount) << logger.getLog();
  TapeSession sess(logger, mockSys, driveInfo, mc, *tapeMount, dataTransferConf, tapeLoadTimeoutSecs, scheduler);

  // 8) Run the data transfer session
  sess.execute();

  // 9) Check the session git the correct VID
  ASSERT_EQ(s_vid, sess.getVid());

  // 10) Check the remote files exist and have the correct size
  std::string temp = logger.getLog();
  ASSERT_NE(std::string::npos, logger.getLog().find("trying to position beyond the end of data"));

  // 11) Check logs for drive statistics
  std::string logToCheck = logger.getLog();
  auto logLines = cta::utils::splitStringToVector(logToCheck, '\n');

  // Check if any of the lines contains all of these substrings
  ASSERT_TRUE(std::ranges::any_of(logLines, [](std::string_view line) {
    return cta::utils::containsAllSubStrings(line,
                                             {
                                               R"(firmwareVersion="123A")",
                                               R"(serialNumber="123456")",
                                               R"(mountTotalCorrectedReadErrors="5")",
                                               R"(mountTotalReadBytesProcessed="4096")",
                                               R"(mountTotalUncorrectedReadErrors="1")",
                                               R"(mountTotalNonMediumErrorCounts="2")",
                                             });
  }));

  ASSERT_TRUE(std::ranges::any_of(logLines, [](std::string_view line) {
    return cta::utils::containsAllSubStrings(line,
                                             {
                                               R"(firmwareVersion="123A")",
                                               R"(serialNumber="123456")",
                                               R"(lifetimeMediumEfficiencyPrct="100.0")",
                                               R"(mountReadEfficiencyPrct="100.0")",
                                               R"(mountWriteEfficiencyPrct="100.0")",
                                               R"(mountReadTransients="10)",
                                               R"(mountServoTemps="10")",
                                               R"(mountServoTransients="5")",
                                               R"(mountTemps="100")",
                                               R"(mountTotalReadRetries="25")",
                                               R"(mountTotalWriteRetries="25")",
                                               R"(mountWriteTransients="10")",
                                             });
  }));
}

/*
 * If retrieve jobs enable RAO, the session orders tape reads accordingly.
 * The recorded read order verifies that the scheduling hint reached the tape reader.
 */
TEST_P(TapeSessionTest, TapeSessionRAORecall) {
  // 0) Prepare the logger for everyone
  cta::log::StringLogger logger("dummy", "tapedUnitTest", cta::log::DEBUG);
  cta::log::LogContext logContext(logger);

  setupDefaultCatalogue();
  // 1) prepare the fake scheduler
  std::string vid = s_vid;
  // cta::MountType::Enum mountType = cta::MountType::RETRIEVE;

  // 3) Prepare the necessary environment (logger, plus system wrapper),
  cta::tape::System::mockWrapper mockSys;
  mockSys.delegateToFake();
  mockSys.disableGMockCallsCounting();
  mockSys.fake.setupForVirtualDriveSLC6();
  //delete is unnecessary
  //pointer with ownership will be passed to the application,
  //which will do the delete
  mockSys.fake.m_pathToDrive["/dev/nst0"] = new cta::tape::drive::FakeDrive;

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
  std::optional<std::string> physicalLibraryName;
  catalogue.LogicalLibrary()->createLogicalLibrary(s_adminOnAdminHost,
                                                   s_libraryName,
                                                   libraryIsDisabled,
                                                   physicalLibraryName,
                                                   libraryComment);
  {
    auto libraries = catalogue.LogicalLibrary()->getLogicalLibraries();
    ASSERT_EQ(1, libraries.size());
    ASSERT_EQ(s_libraryName, libraries.front().name);
    ASSERT_EQ(libraryComment, libraries.front().comment);
  }

  {
    auto tape = getDefaultTape();
    catalogue.Tape()->createTape(s_adminOnAdminHost, tape);
  }

  int MAX_RECALLS = 50;
  int MAX_BULK_RECALLS = 31;
  std::map<size_t, std::vector<std::string>> expectedRAOFseqOrder;
  //RAO for the fake drive is a std::reverse
  // 6) Prepare files for reading by writing them to the mock system
  {
    // Label the tape
    cta::tape::tapeFile::LabelSession::label(mockSys.fake.m_pathToDrive["/dev/nst0"], s_vid, false);
    mockSys.fake.m_pathToDrive["/dev/nst0"]->rewind();
    // And write to it
    cta::tape::daemon::VolumeInfo volInfo;
    volInfo.vid = s_vid;
    auto writeSession = std::make_unique<cta::tape::tapeFile::WriteSession>(*mockSys.fake.m_pathToDrive["/dev/nst0"],
                                                                            volInfo,
                                                                            0,
                                                                            true,
                                                                            false);

    // Write a few files on the virtual tape and modify the archive name space
    // so that it is in sync
    uint8_t data[1000];
    size_t archiveFileSize = sizeof(data);
    cta::tape::SCSI::Structures::zeroStruct(&data);

    for (int fseq = 1; fseq <= MAX_RECALLS; fseq++) {
      expectedRAOFseqOrder[fseq / MAX_BULK_RECALLS].push_back(std::to_string(fseq));
      // Create a path to a remote destination file
      std::ostringstream remoteFilePath;
      remoteFilePath << "file://" << m_tmpDir << "/test" << fseq;
      remoteFilePaths.push_back(remoteFilePath.str());

      // Create an archive file entry in the archive namespace
      auto tapeFileWrittenUP = std::make_unique<cta::catalogue::TapeFileWritten>();
      auto& tapeFileWritten = *tapeFileWrittenUP;
      std::set<cta::catalogue::TapeItemWrittenPointer> tapeFileWrittenSet;
      tapeFileWrittenSet.insert(tapeFileWrittenUP.release());

      // Write the file to tape
      cta::MockArchiveMount mam(catalogue);
      std::unique_ptr<cta::ArchiveJob> aj(new cta::MockArchiveJob(&mam, catalogue));
      aj->tapeFile.fSeq = fseq;
      aj->archiveFile.archiveFileID = fseq;
      cta::tape::tapeFile::FileWriter writer(*writeSession, *aj, archiveFileSize);
      tapeFileWritten.blockId = writer.getBlockId();
      // Write the data (one block)
      writer.write(data, archiveFileSize);
      // Close the file
      writer.close();

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
      catalogue.TapeFile()->filesWrittenToTape(tapeFileWrittenSet);

      // Schedule the retrieval of the file
      std::string diskInstance = s_diskInstance;
      cta::common::dataStructures::RetrieveRequest rReq;
      rReq.archiveFileID = fseq;
      rReq.requester.name = s_userName;
      rReq.requester.group = "someGroup";
      rReq.dstURL = remoteFilePaths.back();
      rReq.diskFileInfo.path = "path/to/file";
      rReq.isVerifyOnly = false;
      // Populate creationLog (required for PostgreSQL scheduler)
      rReq.creationLog.username = s_userName;
      rReq.creationLog.host = "test-host";
      rReq.creationLog.time = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
      // Populate errorReportURL (required for PostgreSQL scheduler - cannot be empty)
      rReq.errorReportURL = "test://error-report-url";
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
  cta::common::dataStructures::DriveInfo driveInfo("T10D6116",
                                                   "host",
                                                   "TestLogicalLibrary",
                                                   "/dev/tape_T10D6116",
                                                   "dummy");
  // We need to create the drive in the registry before being able to put it up.
  scheduler.reportDriveStatus(driveInfo,
                              cta::common::dataStructures::MountType::NoMount,
                              cta::common::dataStructures::DriveStatus::Down,
                              logContext);
  cta::common::dataStructures::DesiredDriveState driveState;
  driveState.up = true;
  driveState.forceDown = false;
  scheduler.setDesiredDriveState(driveInfo.driveName, driveState, logContext);

  // 7) Create the data transfer session
  TransfersConfig dataTransferConf;
  dataTransferConf.buffer_count = 0;
  dataTransferConf.buffer_size_bytes = 0;
  dataTransferConf.disk_io_threads = 0;
  dataTransferConf.archive.fetch_max_bytes = 0;
  dataTransferConf.archive.fetch_max_files = 0;
  dataTransferConf.archive.flush_max_bytes = 0;
  dataTransferConf.archive.flush_max_files = 0;
  dataTransferConf.archive.underfill.watch_period_secs = 0;
  dataTransferConf.archive.underfill.minimum_samples = 0;
  dataTransferConf.archive.underfill.start_threshold_percent = 0;
  dataTransferConf.archive.underfill.recovery_threshold_percent = 0;
  dataTransferConf.retrieve.fetch_max_bytes = 0;
  dataTransferConf.retrieve.fetch_max_files = 0;
  dataTransferConf.retrieve.rao.enabled = false;
  dataTransferConf.retrieve.rao.lto_algorithm.clear();
  dataTransferConf.encryption.external_key_script.clear();
  dataTransferConf.no_block_move_timeout_secs = 600;
  uint32_t tapeLoadTimeoutSecs = 300;
  dataTransferConf.buffer_size_bytes = 1024 * 1024;  // 1 MB memory buffers
  dataTransferConf.buffer_count = 10;
  dataTransferConf.retrieve.fetch_max_bytes = UINT64_C(100) * 1000 * 1000 * 1000;
  dataTransferConf.retrieve.fetch_max_files = MAX_BULK_RECALLS - 1;
  dataTransferConf.disk_io_threads = 1;
  dataTransferConf.retrieve.rao.enabled = true;
  tapeLoadTimeoutSecs = 300;
  dataTransferConf.encryption.enabled = false;
  dataTransferConf.no_block_move_timeout_secs = 600;
  cta::log::DummyLogger dummyLog("dummy", "dummy");
  cta::mediachanger::RmcProxy rmcProxy;
  cta::mediachanger::MediaChangerFacade mc(rmcProxy, dummyLog);
  auto tapeMount = scheduler.getNextMount(driveInfo.logicalLibrary, driveInfo.driveName, logContext);
  ASSERT_NE(nullptr, tapeMount) << logger.getLog();
  cta::tape::daemon::TapeSession
    sess(logger, mockSys, driveInfo, mc, *tapeMount, dataTransferConf, tapeLoadTimeoutSecs, scheduler);

  // 8) Run the data transfer session
  sess.execute();

  // 9) Check the session git the correct VID
  ASSERT_EQ(s_vid, sess.getVid());

  // 10) Check the remote files exist and have the correct size
  for (const auto& path : remoteFilePaths) {
    struct stat statBuf;
    memset(&statBuf, 0, sizeof(statBuf));
    const int statRc = stat(path.substr(7).c_str(), &statBuf);  //remove the "file://" for stat-ing
    ASSERT_EQ(0, statRc);
    ASSERT_EQ(1000, statBuf.st_size);  //same size of data
  }

  // 10) Check logs
  std::string logToCheck = logger.getLog();
  auto logLines = cta::utils::splitStringToVector(logToCheck, '\n');

  // Check if any of the lines contains all of these substrings
  ASSERT_TRUE(std::ranges::any_of(logLines, [](std::string_view line) {
    return cta::utils::containsAllSubStrings(line,
                                             {
                                               R"(firmwareVersion="123A")",
                                               R"(serialNumber="123456")",
                                               R"(mountTotalCorrectedReadErrors="5")",
                                               R"(mountTotalReadBytesProcessed="4096")",
                                               R"(mountTotalUncorrectedReadErrors="1")",
                                               R"(mountTotalNonMediumErrorCounts="2")",
                                             });
  }));

  ASSERT_TRUE(std::ranges::any_of(logLines, [](std::string_view line) {
    return cta::utils::containsAllSubStrings(line,
                                             {
                                               R"(firmwareVersion="123A")",
                                               R"(serialNumber="123456")",
                                               R"(lifetimeMediumEfficiencyPrct="100.0")",
                                               R"(mountReadEfficiencyPrct="100.0")",
                                               R"(mountWriteEfficiencyPrct="100.0")",
                                               R"(mountReadTransients="10)",
                                               R"(mountServoTemps="10")",
                                               R"(mountServoTransients="5")",
                                               R"(mountTemps="100")",
                                               R"(mountTotalReadRetries="25")",
                                               R"(mountTotalWriteRetries="25")",
                                               R"(mountWriteTransients="10")",
                                             });
  }));

  ASSERT_EQ(expectedRAOFseqOrder, getRAOFseqs(logToCheck));
}

/*
 * If the linear RAO algorithm is selected, retrieval follows its computed order.
 * The test checks the algorithm recorded in the log and the resulting file sequence.
 */
TEST_P(TapeSessionTest, TapeSessionRAORecallLinearAlgorithm) {
  // 0) Prepare the logger for everyone
  cta::log::StringLogger logger("dummy", "tapedUnitTest", cta::log::DEBUG);
  cta::log::LogContext logContext(logger);

  setupDefaultCatalogue();
  // 1) prepare the fake scheduler
  std::string vid = s_vid;
  // cta::MountType::Enum mountType = cta::MountType::RETRIEVE;

  // 3) Prepare the necessary environment (logger, plus system wrapper),
  cta::tape::System::mockWrapper mockSys;
  mockSys.delegateToFake();
  mockSys.disableGMockCallsCounting();
  mockSys.fake.setupForVirtualDriveSLC6();
  //delete is unnecessary
  //pointer with ownership will be passed to the application,
  //which will do the delete
  mockSys.fake.m_pathToDrive["/dev/nst0"] = new cta::tape::drive::FakeNonRAODrive;

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
  std::optional<std::string> physicalLibraryName;
  catalogue.LogicalLibrary()->createLogicalLibrary(s_adminOnAdminHost,
                                                   s_libraryName,
                                                   libraryIsDisabled,
                                                   physicalLibraryName,
                                                   libraryComment);
  {
    auto libraries = catalogue.LogicalLibrary()->getLogicalLibraries();
    ASSERT_EQ(1, libraries.size());
    ASSERT_EQ(s_libraryName, libraries.front().name);
    ASSERT_EQ(libraryComment, libraries.front().comment);
  }

  {
    auto tape = getDefaultTape();
    catalogue.Tape()->createTape(s_adminOnAdminHost, tape);
  }

  int MAX_RECALLS = 50;
  int MAX_BULK_RECALLS = 31;
  std::map<size_t, std::vector<std::string>> expectedRAOOrder;
  // 6) Prepare files for reading by writing them to the mock system
  {
    // Label the tape
    cta::tape::tapeFile::LabelSession::label(mockSys.fake.m_pathToDrive["/dev/nst0"], s_vid, false);
    mockSys.fake.m_pathToDrive["/dev/nst0"]->rewind();
    // And write to it
    cta::tape::daemon::VolumeInfo volInfo;
    volInfo.vid = s_vid;
    auto writeSession = std::make_unique<cta::tape::tapeFile::WriteSession>(*mockSys.fake.m_pathToDrive["/dev/nst0"],
                                                                            volInfo,
                                                                            0,
                                                                            true,
                                                                            false);

    // Write a few files on the virtual tape and modify the archive name space
    // so that it is in sync
    uint8_t data[1000];
    size_t archiveFileSize = sizeof(data);
    cta::tape::SCSI::Structures::zeroStruct(&data);

    // For the RAO orders we will have two rao calls : first with 30 files,
    // the second with 20 files
    for (int fseq = 1; fseq <= MAX_RECALLS; fseq++) {
      expectedRAOOrder[fseq / MAX_BULK_RECALLS].push_back(std::to_string(fseq));
      // Create a path to a remote destination file
      std::ostringstream remoteFilePath;
      remoteFilePath << "file://" << m_tmpDir << "/test" << fseq;
      remoteFilePaths.push_back(remoteFilePath.str());

      // Create an archive file entry in the archive namespace
      auto tapeFileWrittenUP = std::make_unique<cta::catalogue::TapeFileWritten>();
      auto& tapeFileWritten = *tapeFileWrittenUP;
      std::set<cta::catalogue::TapeItemWrittenPointer> tapeFileWrittenSet;
      tapeFileWrittenSet.insert(tapeFileWrittenUP.release());

      // Write the file to tape
      cta::MockArchiveMount mam(catalogue);
      std::unique_ptr<cta::ArchiveJob> aj(new cta::MockArchiveJob(&mam, catalogue));
      aj->tapeFile.fSeq = fseq;
      aj->archiveFile.archiveFileID = fseq;
      cta::tape::tapeFile::FileWriter writer(*writeSession, *aj, archiveFileSize);
      tapeFileWritten.blockId = writer.getBlockId();
      // Write the data (one block)
      writer.write(data, archiveFileSize);
      // Close the file
      writer.close();

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
      catalogue.TapeFile()->filesWrittenToTape(tapeFileWrittenSet);

      // Schedule the retrieval of the file
      std::string diskInstance = s_diskInstance;
      cta::common::dataStructures::RetrieveRequest rReq;
      rReq.archiveFileID = fseq;
      rReq.requester.name = s_userName;
      rReq.requester.group = "someGroup";
      rReq.dstURL = remoteFilePaths.back();
      rReq.diskFileInfo.path = "path/to/file";
      rReq.isVerifyOnly = false;
      // Populate creationLog (required for PostgreSQL scheduler)
      rReq.creationLog.username = s_userName;
      rReq.creationLog.host = "test-host";
      rReq.creationLog.time = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
      // Populate errorReportURL (required for PostgreSQL scheduler - cannot be empty)
      rReq.errorReportURL = "test://error-report-url";
      std::list<std::string> archiveFilePaths;
      scheduler.queueRetrieve(diskInstance, rReq, logContext);
    }
  }
  scheduler.waitSchedulerDbSubthreadsComplete();

  // 6) Report the drive's existence and put it up in the drive register.
  cta::common::dataStructures::DriveInfo driveInfo("T10D6116",
                                                   "host",
                                                   "TestLogicalLibrary",
                                                   "/dev/tape_T10D6116",
                                                   "dummy");
  // We need to create the drive in the registry before being able to put it up.
  scheduler.reportDriveStatus(driveInfo,
                              cta::common::dataStructures::MountType::NoMount,
                              cta::common::dataStructures::DriveStatus::Down,
                              logContext);
  cta::common::dataStructures::DesiredDriveState driveState;
  driveState.up = true;
  driveState.forceDown = false;
  scheduler.setDesiredDriveState(driveInfo.driveName, driveState, logContext);

  // 7) Create the data transfer session
  TransfersConfig dataTransferConf;
  dataTransferConf.buffer_count = 0;
  dataTransferConf.buffer_size_bytes = 0;
  dataTransferConf.disk_io_threads = 0;
  dataTransferConf.archive.fetch_max_bytes = 0;
  dataTransferConf.archive.fetch_max_files = 0;
  dataTransferConf.archive.flush_max_bytes = 0;
  dataTransferConf.archive.flush_max_files = 0;
  dataTransferConf.archive.underfill.watch_period_secs = 0;
  dataTransferConf.archive.underfill.minimum_samples = 0;
  dataTransferConf.archive.underfill.start_threshold_percent = 0;
  dataTransferConf.archive.underfill.recovery_threshold_percent = 0;
  dataTransferConf.retrieve.fetch_max_bytes = 0;
  dataTransferConf.retrieve.fetch_max_files = 0;
  dataTransferConf.retrieve.rao.enabled = false;
  dataTransferConf.retrieve.rao.lto_algorithm.clear();
  dataTransferConf.encryption.external_key_script.clear();
  dataTransferConf.no_block_move_timeout_secs = 600;
  uint32_t tapeLoadTimeoutSecs = 300;
  dataTransferConf.buffer_size_bytes = 1024 * 1024;  // 1 MB memory buffers
  dataTransferConf.buffer_count = 10;
  dataTransferConf.retrieve.fetch_max_bytes = UINT64_C(100) * 1000 * 1000 * 1000;
  dataTransferConf.retrieve.fetch_max_files = MAX_BULK_RECALLS - 1;
  dataTransferConf.disk_io_threads = 1;
  dataTransferConf.retrieve.rao.enabled = true;
  dataTransferConf.retrieve.rao.lto_algorithm = "linear";
  tapeLoadTimeoutSecs = 300;
  dataTransferConf.encryption.enabled = false;
  dataTransferConf.no_block_move_timeout_secs = 600;
  cta::log::DummyLogger dummyLog("dummy", "dummy");
  cta::mediachanger::RmcProxy rmcProxy;
  cta::mediachanger::MediaChangerFacade mc(rmcProxy, dummyLog);
  auto tapeMount = scheduler.getNextMount(driveInfo.logicalLibrary, driveInfo.driveName, logContext);
  ASSERT_NE(nullptr, tapeMount) << logger.getLog();
  cta::tape::daemon::TapeSession
    sess(logger, mockSys, driveInfo, mc, *tapeMount, dataTransferConf, tapeLoadTimeoutSecs, scheduler);

  // 8) Run the data transfer session
  sess.execute();

  // 9) Check the session git the correct VID
  ASSERT_EQ(s_vid, sess.getVid());

  // 10) Check the remote files exist and have the correct size
  for (const auto& path : remoteFilePaths) {
    struct stat statBuf;
    memset(&statBuf, 0, sizeof(statBuf));
    const int statRc = stat(path.substr(7).c_str(), &statBuf);  //remove the "file://" for stat-ing
    ASSERT_EQ(0, statRc);
    ASSERT_EQ(1000, statBuf.st_size);  //same size of data
  }

  // 10) Check logs
  std::string logToCheck = logger.getLog();
  auto logLines = cta::utils::splitStringToVector(logToCheck, '\n');

  // Check if any of the lines contains all of these substrings
  ASSERT_TRUE(std::ranges::any_of(logLines, [](std::string_view line) {
    return cta::utils::containsAllSubStrings(line,
                                             {
                                               R"(firmwareVersion="123A")",
                                               R"(serialNumber="123456")",
                                               R"(mountTotalCorrectedReadErrors="5")",
                                               R"(mountTotalReadBytesProcessed="4096")",
                                               R"(mountTotalUncorrectedReadErrors="1")",
                                               R"(mountTotalNonMediumErrorCounts="2")",
                                             });
  }));

  ASSERT_TRUE(std::ranges::any_of(logLines, [](std::string_view line) {
    return cta::utils::containsAllSubStrings(line,
                                             {
                                               R"(firmwareVersion="123A")",
                                               R"(serialNumber="123456")",
                                               R"(lifetimeMediumEfficiencyPrct="100.0")",
                                               R"(mountReadEfficiencyPrct="100.0")",
                                               R"(mountWriteEfficiencyPrct="100.0")",
                                               R"(mountReadTransients="10)",
                                               R"(mountServoTemps="10")",
                                               R"(mountServoTransients="5")",
                                               R"(mountTemps="100")",
                                               R"(mountTotalReadRetries="25")",
                                               R"(mountTotalWriteRetries="25")",
                                               R"(mountWriteTransients="10")",
                                             });
  }));

  ASSERT_EQ(expectedRAOOrder, getRAOFseqs(logToCheck));
}

/*
 * If the configured RAO algorithm does not exist, retrieval falls back to linear order.
 * The session must still complete the recall with a valid ordering.
 */
TEST_P(TapeSessionTest, TapeSessionRAORecallRAOAlgoDoesNotExistShouldApplyLinear) {
  // 0) Prepare the logger for everyone
  cta::log::StringLogger logger("dummy", "tapedUnitTest", cta::log::DEBUG);
  cta::log::LogContext logContext(logger);

  setupDefaultCatalogue();
  // 1) prepare the fake scheduler
  std::string vid = s_vid;
  // cta::MountType::Enum mountType = cta::MountType::RETRIEVE;

  // 3) Prepare the necessary environment (logger, plus system wrapper),
  cta::tape::System::mockWrapper mockSys;
  mockSys.delegateToFake();
  mockSys.disableGMockCallsCounting();
  mockSys.fake.setupForVirtualDriveSLC6();
  //delete is unnecessary
  //pointer with ownership will be passed to the application,
  //which will do the delete
  mockSys.fake.m_pathToDrive["/dev/nst0"] = new cta::tape::drive::FakeNonRAODrive;

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
  std::optional<std::string> physicalLibraryName;
  catalogue.LogicalLibrary()->createLogicalLibrary(s_adminOnAdminHost,
                                                   s_libraryName,
                                                   libraryIsDisabled,
                                                   physicalLibraryName,
                                                   libraryComment);
  {
    auto libraries = catalogue.LogicalLibrary()->getLogicalLibraries();
    ASSERT_EQ(1, libraries.size());
    ASSERT_EQ(s_libraryName, libraries.front().name);
    ASSERT_EQ(libraryComment, libraries.front().comment);
  }

  {
    auto tape = getDefaultTape();
    catalogue.Tape()->createTape(s_adminOnAdminHost, tape);
  }

  int MAX_RECALLS = 50;
  int MAX_BULK_RECALLS = 31;
  std::map<size_t, std::vector<std::string>> expectedRAOOrder;
  // 6) Prepare files for reading by writing them to the mock system
  {
    // Label the tape
    cta::tape::tapeFile::LabelSession::label(mockSys.fake.m_pathToDrive["/dev/nst0"], s_vid, false);
    mockSys.fake.m_pathToDrive["/dev/nst0"]->rewind();
    // And write to it
    cta::tape::daemon::VolumeInfo volInfo;
    volInfo.vid = s_vid;
    auto writeSession = std::make_unique<cta::tape::tapeFile::WriteSession>(*mockSys.fake.m_pathToDrive["/dev/nst0"],
                                                                            volInfo,
                                                                            0,
                                                                            true,
                                                                            false);

    // Write a few files on the virtual tape and modify the archive name space
    // so that it is in sync
    uint8_t data[1000];
    size_t archiveFileSize = sizeof(data);
    cta::tape::SCSI::Structures::zeroStruct(&data);

    // For the RAO orders we will have two rao calls : first with 30 files,
    // the second with 20 files
    for (int fseq = 1; fseq <= MAX_RECALLS; fseq++) {
      expectedRAOOrder[fseq / MAX_BULK_RECALLS].push_back(std::to_string(fseq));
      // Create a path to a remote destination file
      std::ostringstream remoteFilePath;
      remoteFilePath << "file://" << m_tmpDir << "/test" << fseq;
      remoteFilePaths.push_back(remoteFilePath.str());

      // Create an archive file entry in the archive namespace
      auto tapeFileWrittenUP = std::make_unique<cta::catalogue::TapeFileWritten>();
      auto& tapeFileWritten = *tapeFileWrittenUP;
      std::set<cta::catalogue::TapeItemWrittenPointer> tapeFileWrittenSet;
      tapeFileWrittenSet.insert(tapeFileWrittenUP.release());

      // Write the file to tape
      cta::MockArchiveMount mam(catalogue);
      std::unique_ptr<cta::ArchiveJob> aj(new cta::MockArchiveJob(&mam, catalogue));
      aj->tapeFile.fSeq = fseq;
      aj->archiveFile.archiveFileID = fseq;
      cta::tape::tapeFile::FileWriter writer(*writeSession, *aj, archiveFileSize);
      tapeFileWritten.blockId = writer.getBlockId();
      // Write the data (one block)
      writer.write(data, archiveFileSize);
      // Close the file
      writer.close();

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
      catalogue.TapeFile()->filesWrittenToTape(tapeFileWrittenSet);

      // Schedule the retrieval of the file
      std::string diskInstance = s_diskInstance;
      cta::common::dataStructures::RetrieveRequest rReq;
      rReq.archiveFileID = fseq;
      rReq.requester.name = s_userName;
      rReq.requester.group = "someGroup";
      rReq.dstURL = remoteFilePaths.back();
      rReq.diskFileInfo.path = "path/to/file";
      rReq.isVerifyOnly = false;
      // Populate creationLog (required for PostgreSQL scheduler)
      rReq.creationLog.username = s_userName;
      rReq.creationLog.host = "test-host";
      rReq.creationLog.time = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
      // Populate errorReportURL (required for PostgreSQL scheduler - cannot be empty)
      rReq.errorReportURL = "test://error-report-url";
      std::list<std::string> archiveFilePaths;
      scheduler.queueRetrieve(diskInstance, rReq, logContext);
    }
  }
  scheduler.waitSchedulerDbSubthreadsComplete();

  // 6) Report the drive's existence and put it up in the drive register.
  cta::common::dataStructures::DriveInfo driveInfo("T10D6116",
                                                   "host",
                                                   "TestLogicalLibrary",
                                                   "/dev/tape_T10D6116",
                                                   "dummy");
  // We need to create the drive in the registry before being able to put it up.
  scheduler.reportDriveStatus(driveInfo,
                              cta::common::dataStructures::MountType::NoMount,
                              cta::common::dataStructures::DriveStatus::Down,
                              logContext);
  cta::common::dataStructures::DesiredDriveState driveState;
  driveState.up = true;
  driveState.forceDown = false;
  scheduler.setDesiredDriveState(driveInfo.driveName, driveState, logContext);

  // 7) Create the data transfer session
  TransfersConfig dataTransferConf;
  dataTransferConf.buffer_count = 0;
  dataTransferConf.buffer_size_bytes = 0;
  dataTransferConf.disk_io_threads = 0;
  dataTransferConf.archive.fetch_max_bytes = 0;
  dataTransferConf.archive.fetch_max_files = 0;
  dataTransferConf.archive.flush_max_bytes = 0;
  dataTransferConf.archive.flush_max_files = 0;
  dataTransferConf.archive.underfill.watch_period_secs = 0;
  dataTransferConf.archive.underfill.minimum_samples = 0;
  dataTransferConf.archive.underfill.start_threshold_percent = 0;
  dataTransferConf.archive.underfill.recovery_threshold_percent = 0;
  dataTransferConf.retrieve.fetch_max_bytes = 0;
  dataTransferConf.retrieve.fetch_max_files = 0;
  dataTransferConf.retrieve.rao.enabled = false;
  dataTransferConf.retrieve.rao.lto_algorithm.clear();
  dataTransferConf.encryption.external_key_script.clear();
  dataTransferConf.no_block_move_timeout_secs = 600;
  uint32_t tapeLoadTimeoutSecs = 300;
  dataTransferConf.buffer_size_bytes = 1024 * 1024;  // 1 MB memory buffers
  dataTransferConf.buffer_count = 10;
  dataTransferConf.retrieve.fetch_max_bytes = UINT64_C(100) * 1000 * 1000 * 1000;
  dataTransferConf.retrieve.fetch_max_files = MAX_BULK_RECALLS - 1;
  dataTransferConf.disk_io_threads = 1;
  dataTransferConf.retrieve.rao.enabled = true;
  tapeLoadTimeoutSecs = 300;
  dataTransferConf.retrieve.rao.lto_algorithm = "DOES_NOT_EXIST";
  dataTransferConf.encryption.enabled = false;
  dataTransferConf.no_block_move_timeout_secs = 600;

  cta::log::DummyLogger dummyLog("dummy", "dummy");
  cta::mediachanger::RmcProxy rmcProxy;
  cta::mediachanger::MediaChangerFacade mc(rmcProxy, dummyLog);
  auto tapeMount = scheduler.getNextMount(driveInfo.logicalLibrary, driveInfo.driveName, logContext);
  ASSERT_NE(nullptr, tapeMount) << logger.getLog();
  cta::tape::daemon::TapeSession
    sess(logger, mockSys, driveInfo, mc, *tapeMount, dataTransferConf, tapeLoadTimeoutSecs, scheduler);

  // 8) Run the data transfer session
  sess.execute();

  // 9) Check the session git the correct VID
  ASSERT_EQ(s_vid, sess.getVid());

  // 10) Check the remote files exist and have the correct size
  for (const auto& path : remoteFilePaths) {
    struct stat statBuf;
    memset(&statBuf, 0, sizeof(statBuf));
    const int statRc = stat(path.substr(7).c_str(), &statBuf);  //remove the "file://" for stat-ing
    ASSERT_EQ(0, statRc);
    ASSERT_EQ(1000, statBuf.st_size);  //same size of data
  }

  // 10) Check logs
  std::string logToCheck = logger.getLog();
  auto logLines = cta::utils::splitStringToVector(logToCheck, '\n');

  // Check if any of the lines contains all of these substrings
  ASSERT_TRUE(std::ranges::any_of(logLines, [](std::string_view line) {
    return cta::utils::containsAllSubStrings(line,
                                             {
                                               R"(firmwareVersion="123A")",
                                               R"(serialNumber="123456")",
                                               R"(mountTotalCorrectedReadErrors="5")",
                                               R"(mountTotalReadBytesProcessed="4096")",
                                               R"(mountTotalUncorrectedReadErrors="1")",
                                               R"(mountTotalNonMediumErrorCounts="2")",
                                             });
  }));

  ASSERT_TRUE(std::ranges::any_of(logLines, [](std::string_view line) {
    return cta::utils::containsAllSubStrings(line,
                                             {
                                               R"(firmwareVersion="123A")",
                                               R"(serialNumber="123456")",
                                               R"(lifetimeMediumEfficiencyPrct="100.0")",
                                               R"(mountReadEfficiencyPrct="100.0")",
                                               R"(mountWriteEfficiencyPrct="100.0")",
                                               R"(mountReadTransients="10)",
                                               R"(mountServoTemps="10")",
                                               R"(mountServoTransients="5")",
                                               R"(mountTemps="100")",
                                               R"(mountTotalReadRetries="25")",
                                               R"(mountTotalWriteRetries="25")",
                                               R"(mountWriteTransients="10")",
                                             });
  }));

  ASSERT_NE(std::string::npos,
            logToCheck.find(
              "In RAOAlgorithmFactoryFactory::createAlgorithmFactory(), unable to determine the RAO algorithm to use"));

  ASSERT_EQ(expectedRAOOrder, getRAOFseqs(logToCheck));
}

/*
 * If the SLTF RAO algorithm is selected, retrieval follows its computed order.
 * The log and file sequence confirm that the requested algorithm ran.
 */
TEST_P(TapeSessionTest, TapeSessionRAORecallSLTFRAOAlgorithm) {
  // 0) Prepare the logger for everyone
  cta::log::StringLogger logger("dummy", "tapedUnitTest", cta::log::DEBUG);
  cta::log::LogContext logContext(logger);

  setupDefaultCatalogue();
  // 1) prepare the fake scheduler
  std::string vid = s_vid;
  // cta::MountType::Enum mountType = cta::MountType::RETRIEVE;

  // 3) Prepare the necessary environment (logger, plus system wrapper),
  cta::tape::System::mockWrapper mockSys;
  mockSys.delegateToFake();
  mockSys.disableGMockCallsCounting();
  mockSys.fake.setupForVirtualDriveSLC6();
  //delete is unnecessary
  //pointer with ownership will be passed to the application,
  //which will do the delete
  mockSys.fake.m_pathToDrive["/dev/nst0"] = new cta::tape::drive::FakeNonRAODrive;

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
  std::optional<std::string> physicalLibraryName;
  catalogue.LogicalLibrary()->createLogicalLibrary(s_adminOnAdminHost,
                                                   s_libraryName,
                                                   libraryIsDisabled,
                                                   physicalLibraryName,
                                                   libraryComment);
  {
    auto libraries = catalogue.LogicalLibrary()->getLogicalLibraries();
    ASSERT_EQ(1, libraries.size());
    ASSERT_EQ(s_libraryName, libraries.front().name);
    ASSERT_EQ(libraryComment, libraries.front().comment);
  }

  {
    auto tape = getDefaultTape();
    catalogue.Tape()->createTape(s_adminOnAdminHost, tape);
  }

  int MAX_RECALLS = 30;
  int MAX_BULK_RECALLS = 20;
  std::map<size_t, std::vector<std::string>> expectedRAOOrder;
  // 6) Prepare files for reading by writing them to the mock system
  {
    // Label the tape
    cta::tape::tapeFile::LabelSession::label(mockSys.fake.m_pathToDrive["/dev/nst0"], s_vid, false);
    mockSys.fake.m_pathToDrive["/dev/nst0"]->rewind();
    // And write to it
    cta::tape::daemon::VolumeInfo volInfo;
    volInfo.vid = s_vid;
    auto writeSession = std::make_unique<cta::tape::tapeFile::WriteSession>(*mockSys.fake.m_pathToDrive["/dev/nst0"],
                                                                            volInfo,
                                                                            0,
                                                                            true,
                                                                            false);

    // Write a few files on the virtual tape and modify the archive name space
    // so that it is in sync
    uint8_t data[1000];
    size_t archiveFileSize = sizeof(data);
    cta::tape::SCSI::Structures::zeroStruct(&data);

    // For the RAO orders we will have two rao calls : first with 30 files,
    // the second with 20 files
    for (int fseq = 1; fseq <= MAX_RECALLS; fseq++) {
      expectedRAOOrder[fseq / MAX_BULK_RECALLS].push_back(std::to_string(fseq));
      // Create a path to a remote destination file
      std::ostringstream remoteFilePath;
      remoteFilePath << "file://" << m_tmpDir << "/test" << fseq;
      remoteFilePaths.push_back(remoteFilePath.str());

      // Create an archive file entry in the archive namespace
      auto tapeFileWrittenUP = std::make_unique<cta::catalogue::TapeFileWritten>();
      auto& tapeFileWritten = *tapeFileWrittenUP;
      std::set<cta::catalogue::TapeItemWrittenPointer> tapeFileWrittenSet;
      tapeFileWrittenSet.insert(tapeFileWrittenUP.release());

      // Write the file to tape
      cta::MockArchiveMount mam(catalogue);
      std::unique_ptr<cta::ArchiveJob> aj(new cta::MockArchiveJob(&mam, catalogue));
      aj->tapeFile.fSeq = fseq;
      aj->archiveFile.archiveFileID = fseq;
      cta::tape::tapeFile::FileWriter writer(*writeSession, *aj, archiveFileSize);
      tapeFileWritten.blockId = writer.getBlockId();
      // Write the data (one block)
      writer.write(data, archiveFileSize);
      // Close the file
      writer.close();

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
      catalogue.TapeFile()->filesWrittenToTape(tapeFileWrittenSet);

      // Schedule the retrieval of the file
      std::string diskInstance = s_diskInstance;
      cta::common::dataStructures::RetrieveRequest rReq;
      rReq.archiveFileID = fseq;
      rReq.requester.name = s_userName;
      rReq.requester.group = "someGroup";
      rReq.dstURL = remoteFilePaths.back();
      rReq.diskFileInfo.path = "path/to/file";
      rReq.isVerifyOnly = false;
      // Populate creationLog (required for PostgreSQL scheduler)
      rReq.creationLog.username = s_userName;
      rReq.creationLog.host = "test-host";
      rReq.creationLog.time = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
      // Populate errorReportURL (required for PostgreSQL scheduler - cannot be empty)
      rReq.errorReportURL = "test://error-report-url";
      std::list<std::string> archiveFilePaths;
      scheduler.queueRetrieve(diskInstance, rReq, logContext);
    }
  }
  scheduler.waitSchedulerDbSubthreadsComplete();

  // 6) Report the drive's existence and put it up in the drive register.
  cta::common::dataStructures::DriveInfo driveInfo("T10D6116",
                                                   "host",
                                                   "TestLogicalLibrary",
                                                   "/dev/tape_T10D6116",
                                                   "dummy");
  // We need to create the drive in the registry before being able to put it up.
  scheduler.reportDriveStatus(driveInfo,
                              cta::common::dataStructures::MountType::NoMount,
                              cta::common::dataStructures::DriveStatus::Down,
                              logContext);
  cta::common::dataStructures::DesiredDriveState driveState;
  driveState.up = true;
  driveState.forceDown = false;
  scheduler.setDesiredDriveState(driveInfo.driveName, driveState, logContext);

  // 7) Create the data transfer session
  TransfersConfig dataTransferConf;
  dataTransferConf.buffer_count = 0;
  dataTransferConf.buffer_size_bytes = 0;
  dataTransferConf.disk_io_threads = 0;
  dataTransferConf.archive.fetch_max_bytes = 0;
  dataTransferConf.archive.fetch_max_files = 0;
  dataTransferConf.archive.flush_max_bytes = 0;
  dataTransferConf.archive.flush_max_files = 0;
  dataTransferConf.archive.underfill.watch_period_secs = 0;
  dataTransferConf.archive.underfill.minimum_samples = 0;
  dataTransferConf.archive.underfill.start_threshold_percent = 0;
  dataTransferConf.archive.underfill.recovery_threshold_percent = 0;
  dataTransferConf.retrieve.fetch_max_bytes = 0;
  dataTransferConf.retrieve.fetch_max_files = 0;
  dataTransferConf.retrieve.rao.enabled = false;
  dataTransferConf.retrieve.rao.lto_algorithm.clear();
  dataTransferConf.encryption.external_key_script.clear();
  dataTransferConf.no_block_move_timeout_secs = 600;
  uint32_t tapeLoadTimeoutSecs = 300;
  dataTransferConf.buffer_size_bytes = 1024 * 1024;  // 1 MB memory buffers
  dataTransferConf.buffer_count = 10;
  dataTransferConf.retrieve.fetch_max_bytes = UINT64_C(100) * 1000 * 1000 * 1000;
  dataTransferConf.retrieve.fetch_max_files = MAX_BULK_RECALLS - 1;
  dataTransferConf.disk_io_threads = 1;
  dataTransferConf.retrieve.rao.enabled = true;
  tapeLoadTimeoutSecs = 300;
  dataTransferConf.retrieve.rao.lto_algorithm = "sltf";
  dataTransferConf.encryption.enabled = false;
  dataTransferConf.no_block_move_timeout_secs = 600;
  cta::log::DummyLogger dummyLog("dummy", "dummy");
  cta::mediachanger::RmcProxy rmcProxy;
  cta::mediachanger::MediaChangerFacade mc(rmcProxy, dummyLog);
  auto tapeMount = scheduler.getNextMount(driveInfo.logicalLibrary, driveInfo.driveName, logContext);
  ASSERT_NE(nullptr, tapeMount) << logger.getLog();
  cta::tape::daemon::TapeSession
    sess(logger, mockSys, driveInfo, mc, *tapeMount, dataTransferConf, tapeLoadTimeoutSecs, scheduler);

  // 8) Run the data transfer session
  sess.execute();

  // 9) Check the session git the correct VID
  ASSERT_EQ(s_vid, sess.getVid());

  // 10) Check the remote files exist and have the correct size
  for (const auto& path : remoteFilePaths) {
    struct stat statBuf;
    memset(&statBuf, 0, sizeof(statBuf));
    const int statRc = stat(path.substr(7).c_str(), &statBuf);  //remove the "file://" for stat-ing
    ASSERT_EQ(0, statRc);
    ASSERT_EQ(1000, statBuf.st_size);  //same size of data
  }

  // 10) Check logs
  std::string logToCheck = logger.getLog();
  auto logLines = cta::utils::splitStringToVector(logToCheck, '\n');

  // Check if any of the lines contains all of these substrings
  ASSERT_TRUE(std::ranges::any_of(logLines, [](std::string_view line) {
    return cta::utils::containsAllSubStrings(line,
                                             {
                                               R"(firmwareVersion="123A")",
                                               R"(serialNumber="123456")",
                                               R"(mountTotalCorrectedReadErrors="5")",
                                               R"(mountTotalReadBytesProcessed="4096")",
                                               R"(mountTotalUncorrectedReadErrors="1")",
                                               R"(mountTotalNonMediumErrorCounts="2")",
                                             });
  }));

  ASSERT_TRUE(std::ranges::any_of(logLines, [](std::string_view line) {
    return cta::utils::containsAllSubStrings(line,
                                             {
                                               R"(firmwareVersion="123A")",
                                               R"(serialNumber="123456")",
                                               R"(lifetimeMediumEfficiencyPrct="100.0")",
                                               R"(mountReadEfficiencyPrct="100.0")",
                                               R"(mountWriteEfficiencyPrct="100.0")",
                                               R"(mountReadTransients="10)",
                                               R"(mountServoTemps="10")",
                                               R"(mountServoTransients="5")",
                                               R"(mountTemps="100")",
                                               R"(mountTotalReadRetries="25")",
                                               R"(mountTotalWriteRetries="25")",
                                               R"(mountWriteTransients="10")",
                                             });
  }));

  ASSERT_NE(std::string::npos, logToCheck.find("In RAOManager::queryRAO(), successfully performed RAO."));
  ASSERT_NE(std::string::npos, logToCheck.find("executedRAOAlgorithm=\"sltf\""));

  ASSERT_EQ(expectedRAOOrder, getRAOFseqs(logToCheck));
}

/*
 * If the configured tape drive is unavailable, the session cannot run a transfer.
 * The failure must be reported instead of treating the mount as successful.
 */
TEST_P(TapeSessionTest, TapeSessionNoSuchDrive) {
  // 0) Prepare the logger for everyone
  cta::log::StringLogger logger("dummy", "tapedUnitTest", cta::log::DEBUG);
  cta::log::LogContext logContext(logger);

  setupDefaultCatalogue();
  // 1) prepare the fake scheduler
  std::string vid = s_vid;
  // cta::MountType::Enum mountType = cta::MountType::RETRIEVE;

  // 3) Prepare the necessary environment (logger, plus system wrapper),
  cta::tape::System::mockWrapper mockSys;
  mockSys.delegateToFake();
  mockSys.disableGMockCallsCounting();
  mockSys.fake.setupForVirtualDriveSLC6();
  //delete is unnecessary
  //pointer with ownership will be passed to the application,
  //which will do the delete
  mockSys.fake.m_pathToDrive["/dev/nst0"] = new cta::tape::drive::FakeDrive;

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
  std::optional<std::string> physicalLibraryName;
  catalogue.LogicalLibrary()->createLogicalLibrary(s_adminOnAdminHost,
                                                   s_libraryName,
                                                   libraryIsDisabled,
                                                   physicalLibraryName,
                                                   libraryComment);
  {
    auto libraries = catalogue.LogicalLibrary()->getLogicalLibraries();
    ASSERT_EQ(1, libraries.size());
    ASSERT_EQ(s_libraryName, libraries.front().name);
    ASSERT_EQ(libraryComment, libraries.front().comment);
  }

  {
    auto tape = getDefaultTape();
    catalogue.Tape()->createTape(s_adminOnAdminHost, tape);
  }

  // 6) Prepare files for reading by writing them to the mock system
  {
    // Label the tape
    cta::tape::tapeFile::LabelSession::label(mockSys.fake.m_pathToDrive["/dev/nst0"], s_vid, false);
    mockSys.fake.m_pathToDrive["/dev/nst0"]->rewind();
    // And write to it
    cta::tape::daemon::VolumeInfo volInfo;
    volInfo.vid = s_vid;
    auto writeSession = std::make_unique<cta::tape::tapeFile::WriteSession>(*mockSys.fake.m_pathToDrive["/dev/nst0"],
                                                                            volInfo,
                                                                            0,
                                                                            true,
                                                                            false);

    // Write a few files on the virtual tape and modify the archive name space
    // so that it is in sync
    uint8_t data[1000];
    size_t archiveFileSize = sizeof(data);
    cta::tape::SCSI::Structures::zeroStruct(&data);
    for (int fseq = 1; fseq <= 10; fseq++) {
      // Create a path to a remote destination file
      std::ostringstream remoteFilePath;
      remoteFilePath << "file://" << m_tmpDir << "/test" << fseq;
      remoteFilePaths.push_back(remoteFilePath.str());

      // Create an archive file entry in the archive namespace
      auto tapeFileWrittenUP = std::make_unique<cta::catalogue::TapeFileWritten>();
      auto& tapeFileWritten = *tapeFileWrittenUP;
      std::set<cta::catalogue::TapeItemWrittenPointer> tapeFileWrittenSet;
      tapeFileWrittenSet.insert(tapeFileWrittenUP.release());

      // Write the file to tape
      cta::MockArchiveMount mam(catalogue);
      std::unique_ptr<cta::ArchiveJob> aj(new cta::MockArchiveJob(&mam, catalogue));
      aj->tapeFile.fSeq = fseq;
      aj->archiveFile.archiveFileID = fseq;
      cta::tape::tapeFile::FileWriter writer(*writeSession, *aj, archiveFileSize);
      tapeFileWritten.blockId = writer.getBlockId();
      // Write the data (one block)
      writer.write(data, archiveFileSize);
      // Close the file
      writer.close();

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
      catalogue.TapeFile()->filesWrittenToTape(tapeFileWrittenSet);

      // Schedule the retrieval of the file
      std::string diskInstance = s_diskInstance;
      cta::common::dataStructures::RetrieveRequest rReq;
      rReq.archiveFileID = fseq;
      rReq.requester.name = s_userName;
      rReq.requester.group = "someGroup";
      rReq.dstURL = remoteFilePaths.back();
      rReq.diskFileInfo.path = "path/to/file";
      rReq.isVerifyOnly = false;
      // Populate creationLog (required for PostgreSQL scheduler)
      rReq.creationLog.username = s_userName;
      rReq.creationLog.host = "test-host";
      rReq.creationLog.time = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
      // Populate errorReportURL (required for PostgreSQL scheduler - cannot be empty)
      rReq.errorReportURL = "test://error-report-url";
      std::list<std::string> archiveFilePaths;
      scheduler.queueRetrieve(diskInstance, rReq, logContext);
    }
  }
  scheduler.waitSchedulerDbSubthreadsComplete();

  // 7) Report the drive's existence and put it up in the drive register.
  cta::common::dataStructures::DriveInfo driveInfo("T10D6116",
                                                   "host",
                                                   "TestLogicalLibrary",
                                                   "/dev/noSuchDrive",
                                                   "dummy");
  // We need to create the drive in the registry before being able to put it up.
  scheduler.reportDriveStatus(driveInfo,
                              cta::common::dataStructures::MountType::NoMount,
                              cta::common::dataStructures::DriveStatus::Down,
                              logContext);
  cta::common::dataStructures::DesiredDriveState driveState;
  driveState.up = true;
  driveState.forceDown = false;
  scheduler.setDesiredDriveState(driveInfo.driveName, driveState, logContext);

  // 8) Create the data transfer session
  TransfersConfig dataTransferConf;
  dataTransferConf.buffer_count = 0;
  dataTransferConf.buffer_size_bytes = 0;
  dataTransferConf.disk_io_threads = 0;
  dataTransferConf.archive.fetch_max_bytes = 0;
  dataTransferConf.archive.fetch_max_files = 0;
  dataTransferConf.archive.flush_max_bytes = 0;
  dataTransferConf.archive.flush_max_files = 0;
  dataTransferConf.archive.underfill.watch_period_secs = 0;
  dataTransferConf.archive.underfill.minimum_samples = 0;
  dataTransferConf.archive.underfill.start_threshold_percent = 0;
  dataTransferConf.archive.underfill.recovery_threshold_percent = 0;
  dataTransferConf.retrieve.fetch_max_bytes = 0;
  dataTransferConf.retrieve.fetch_max_files = 0;
  dataTransferConf.retrieve.rao.enabled = false;
  dataTransferConf.retrieve.rao.lto_algorithm.clear();
  dataTransferConf.encryption.external_key_script.clear();
  dataTransferConf.no_block_move_timeout_secs = 600;
  uint32_t tapeLoadTimeoutSecs = 300;
  dataTransferConf.buffer_size_bytes = 1024;
  tapeLoadTimeoutSecs = 300;
  dataTransferConf.buffer_count = 10;
  dataTransferConf.encryption.enabled = false;
  dataTransferConf.no_block_move_timeout_secs = 600;
  cta::log::DummyLogger dummyLog("dummy", "dummy");
  cta::mediachanger::RmcProxy rmcProxy;
  cta::mediachanger::MediaChangerFacade mc(rmcProxy, dummyLog);
  auto tapeMount = scheduler.getNextMount(driveInfo.logicalLibrary, driveInfo.driveName, logContext);
  ASSERT_NE(nullptr, tapeMount) << logger.getLog();
  TapeSession sess(logger, mockSys, driveInfo, mc, *tapeMount, dataTransferConf, tapeLoadTimeoutSecs, scheduler);
  ASSERT_NO_THROW(sess.execute());
  std::string temp = logger.getLog();
  ASSERT_NE(std::string::npos, logger.getLog().find("Session drive access failed: Configured drive lookup failed"));
}

/*
 * If the tape cannot be mounted, the session reports the mount failure.
 * No file transfer should be reported as successful.
 */
TEST_P(TapeSessionTest, TapeSessionFailtoMount) {
  // 0) Prepare the logger for everyone
  cta::log::StringLogger logger("dummy", "tapedUnitTest", cta::log::DEBUG);
  cta::log::LogContext logContext(logger);

  setupDefaultCatalogue();
  // 1) prepare the fake scheduler
  std::string vid = s_vid;
  // cta::MountType::Enum mountType = cta::MountType::RETRIEVE;

  // 3) Prepare the necessary environment (logger, plus system wrapper),
  cta::tape::System::mockWrapper mockSys;
  mockSys.delegateToFake();
  mockSys.disableGMockCallsCounting();
  mockSys.fake.setupForVirtualDriveSLC6();
  //delete is unnecessary
  //pointer with ownership will be passed to the application,
  //which will do the delete
  const bool failOnMount = true;
  mockSys.fake.m_pathToDrive["/dev/nst0"] = new cta::tape::drive::FakeDrive(failOnMount);

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
  std::optional<std::string> physicalLibraryName;
  catalogue.LogicalLibrary()->createLogicalLibrary(s_adminOnAdminHost,
                                                   s_libraryName,
                                                   libraryIsDisabled,
                                                   physicalLibraryName,
                                                   libraryComment);
  {
    auto libraries = catalogue.LogicalLibrary()->getLogicalLibraries();
    ASSERT_EQ(1, libraries.size());
    ASSERT_EQ(s_libraryName, libraries.front().name);
    ASSERT_EQ(libraryComment, libraries.front().comment);
  }

  {
    auto tape = getDefaultTape();
    catalogue.Tape()->createTape(s_adminOnAdminHost, tape);
  }

  // 6) Prepare files for reading by writing them to the mock system
  {
    // Label the tape
    cta::tape::tapeFile::LabelSession::label(mockSys.fake.m_pathToDrive["/dev/nst0"], s_vid, false);
    mockSys.fake.m_pathToDrive["/dev/nst0"]->rewind();
    // And write to it
    cta::tape::daemon::VolumeInfo volInfo;
    volInfo.vid = s_vid;
    auto writeSession = std::make_unique<cta::tape::tapeFile::WriteSession>(*mockSys.fake.m_pathToDrive["/dev/nst0"],
                                                                            volInfo,
                                                                            0,
                                                                            true,
                                                                            false);

    // Write a few files on the virtual tape and modify the archive name space
    // so that it is in sync
    uint8_t data[1000];
    size_t archiveFileSize = sizeof(data);
    cta::tape::SCSI::Structures::zeroStruct(&data);
    for (int fseq = 1; fseq <= 10; fseq++) {
      // Create a path to a remote destination file
      std::ostringstream remoteFilePath;
      remoteFilePath << "file://" << m_tmpDir << "/test" << fseq;
      remoteFilePaths.push_back(remoteFilePath.str());

      // Create an archive file entry in the archive namespace
      auto tapeFileWrittenUP = std::make_unique<cta::catalogue::TapeFileWritten>();
      auto& tapeFileWritten = *tapeFileWrittenUP;
      std::set<cta::catalogue::TapeItemWrittenPointer> tapeFileWrittenSet;
      tapeFileWrittenSet.insert(tapeFileWrittenUP.release());

      // Write the file to tape
      cta::MockArchiveMount mam(catalogue);
      std::unique_ptr<cta::ArchiveJob> aj(new cta::MockArchiveJob(&mam, catalogue));
      aj->tapeFile.fSeq = fseq;
      aj->archiveFile.archiveFileID = fseq;
      cta::tape::tapeFile::FileWriter writer(*writeSession, *aj, archiveFileSize);
      tapeFileWritten.blockId = writer.getBlockId();
      // Write the data (one block)
      writer.write(data, archiveFileSize);
      // Close the file
      writer.close();

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
      catalogue.TapeFile()->filesWrittenToTape(tapeFileWrittenSet);

      // Schedule the retrieval of the file
      std::string diskInstance = s_diskInstance;
      cta::common::dataStructures::RetrieveRequest rReq;
      rReq.archiveFileID = fseq;
      rReq.requester.name = s_userName;
      rReq.requester.group = "someGroup";
      rReq.dstURL = remoteFilePaths.back();
      rReq.diskFileInfo.path = "path/to/file";
      rReq.isVerifyOnly = false;
      // Populate creationLog (required for PostgreSQL scheduler)
      rReq.creationLog.username = s_userName;
      rReq.creationLog.host = "test-host";
      rReq.creationLog.time = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
      // Populate errorReportURL (required for PostgreSQL scheduler - cannot be empty)
      rReq.errorReportURL = "test://error-report-url";
      std::list<std::string> archiveFilePaths;
      scheduler.queueRetrieve(diskInstance, rReq, logContext);
    }
  }
  scheduler.waitSchedulerDbSubthreadsComplete();

  // 7) Report the drive's existence and put it up in the drive register.
  cta::common::dataStructures::DriveInfo driveInfo("T10D6116",
                                                   "host",
                                                   "TestLogicalLibrary",
                                                   "/dev/tape_T10D6116",
                                                   "dummy");
  // We need to create the drive in the registry before being able to put it up.
  scheduler.reportDriveStatus(driveInfo,
                              cta::common::dataStructures::MountType::NoMount,
                              cta::common::dataStructures::DriveStatus::Down,
                              logContext);
  cta::common::dataStructures::DesiredDriveState driveState;
  driveState.up = true;
  driveState.forceDown = false;
  scheduler.setDesiredDriveState(driveInfo.driveName, driveState, logContext);

  // 8) Create the data transfer session
  TransfersConfig dataTransferConf;
  dataTransferConf.buffer_count = 0;
  dataTransferConf.buffer_size_bytes = 0;
  dataTransferConf.disk_io_threads = 0;
  dataTransferConf.archive.fetch_max_bytes = 0;
  dataTransferConf.archive.fetch_max_files = 0;
  dataTransferConf.archive.flush_max_bytes = 0;
  dataTransferConf.archive.flush_max_files = 0;
  dataTransferConf.archive.underfill.watch_period_secs = 0;
  dataTransferConf.archive.underfill.minimum_samples = 0;
  dataTransferConf.archive.underfill.start_threshold_percent = 0;
  dataTransferConf.archive.underfill.recovery_threshold_percent = 0;
  dataTransferConf.retrieve.fetch_max_bytes = 0;
  dataTransferConf.retrieve.fetch_max_files = 0;
  dataTransferConf.retrieve.rao.enabled = false;
  dataTransferConf.retrieve.rao.lto_algorithm.clear();
  dataTransferConf.encryption.external_key_script.clear();
  dataTransferConf.no_block_move_timeout_secs = 600;
  uint32_t tapeLoadTimeoutSecs = 300;
  dataTransferConf.buffer_size_bytes = 1024 * 1024;  // 1 MB memory buffers
  dataTransferConf.buffer_count = 10;
  dataTransferConf.retrieve.fetch_max_bytes = UINT64_C(100) * 1000 * 1000 * 1000;
  dataTransferConf.retrieve.fetch_max_files = 1000;
  dataTransferConf.disk_io_threads = 3;
  tapeLoadTimeoutSecs = 300;
  dataTransferConf.encryption.enabled = false;
  dataTransferConf.no_block_move_timeout_secs = 600;
  cta::log::DummyLogger dummyLog("dummy", "dummy");
  cta::mediachanger::RmcProxy rmcProxy;
  cta::mediachanger::MediaChangerFacade mc(rmcProxy, dummyLog);
  auto tapeMount = scheduler.getNextMount(driveInfo.logicalLibrary, driveInfo.driveName, logContext);
  ASSERT_NE(nullptr, tapeMount) << logger.getLog();
  TapeSession sess(logger, mockSys, driveInfo, mc, *tapeMount, dataTransferConf, tapeLoadTimeoutSecs, scheduler);
  ASSERT_NO_THROW(sess.execute());
  std::string temp = logger.getLog();
  ASSERT_NE(std::string::npos, logger.getLog().find("Failed to mount the tape"));

  // 10) Check logs for drive statistics
  std::string logToCheck = logger.getLog();
  auto logLines = cta::utils::splitStringToVector(logToCheck, '\n');

  // Check if any of the lines contains all of these substrings
  ASSERT_TRUE(std::ranges::any_of(logLines, [](std::string_view line) {
    return cta::utils::containsAllSubStrings(line,
                                             {
                                               R"(firmwareVersion="123A")",
                                               R"(serialNumber="123456")",
                                               R"(mountTotalCorrectedReadErrors="5")",
                                               R"(mountTotalReadBytesProcessed="4096")",
                                               R"(mountTotalUncorrectedReadErrors="1")",
                                               R"(mountTotalNonMediumErrorCounts="2")",
                                             });
  }));

  ASSERT_TRUE(std::ranges::any_of(logLines, [](std::string_view line) {
    return cta::utils::containsAllSubStrings(line,
                                             {
                                               R"(firmwareVersion="123A")",
                                               R"(serialNumber="123456")",
                                               R"(lifetimeMediumEfficiencyPrct="100.0")",
                                               R"(mountReadEfficiencyPrct="100.0")",
                                               R"(mountWriteEfficiencyPrct="100.0")",
                                               R"(mountReadTransients="10)",
                                               R"(mountServoTemps="10")",
                                               R"(mountServoTransients="5")",
                                               R"(mountTemps="100")",
                                               R"(mountTotalReadRetries="25")",
                                               R"(mountTotalWriteRetries="25")",
                                               R"(mountWriteTransients="10")",
                                             });
  }));
}

/*
 * If archive jobs contain valid disk files, the session writes them to tape.
 * The test checks the catalogue and job state after migration.
 */
TEST_P(TapeSessionTest, TapeSessionGooddayMigration) {
  // 0) Prepare the logger for everyone
  cta::log::StringLogger logger("dummy", "tapedUnitTest", cta::log::DEBUG);
  cta::log::LogContext logContext(logger);

  setupDefaultCatalogue();
  // 1) prepare the fake scheduler
  std::string vid = s_vid;
  // cta::MountType::Enum mountType = cta::MountType::RETRIEVE;

  // 3) Prepare the necessary environment (logger, plus system wrapper),
  cta::tape::System::mockWrapper mockSys;
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
  std::optional<std::string> physicalLibraryName;
  catalogue.LogicalLibrary()->createLogicalLibrary(s_adminOnAdminHost,
                                                   s_libraryName,
                                                   libraryIsDisabled,
                                                   physicalLibraryName,
                                                   libraryComment);
  {
    auto libraries = catalogue.LogicalLibrary()->getLogicalLibraries();
    ASSERT_EQ(1, libraries.size());
    ASSERT_EQ(s_libraryName, libraries.front().name);
    ASSERT_EQ(libraryComment, libraries.front().comment);
  }

  {
    auto tape = getDefaultTape();
    catalogue.Tape()->createTape(s_adminOnAdminHost, tape);
  }

  // Create the mount criteria
  auto mountPolicy = getImmediateMountMountPolicy();
  catalogue.MountPolicy()->createMountPolicy(requester, mountPolicy);
  std::string mountPolicyName = mountPolicy.name;

  catalogue.RequesterMountRule()->createRequesterMountRule(requester,
                                                           mountPolicyName,
                                                           s_diskInstance,
                                                           requester.username,
                                                           "Rule comment");

  //delete is unnecessary
  //pointer with ownership will be passed to the application,
  //which will do the delete
  mockSys.fake.m_pathToDrive["/dev/nst0"] = new cta::tape::drive::FakeDrive();

  // We can prepare files for writing on the drive.
  // Tempfiles are in this scope so they are kept alive
  std::list<std::unique_ptr<unitTests::TempFile>> sourceFiles;
  std::list<uint64_t> archiveFileIds;
  {
    // Label the tape
    cta::tape::tapeFile::LabelSession::label(mockSys.fake.m_pathToDrive["/dev/nst0"], s_vid, false);
    catalogue.Tape()->tapeLabelled(s_vid, "T10D6116");
    mockSys.fake.m_pathToDrive["/dev/nst0"]->rewind();

    // Create the files and schedule the archivals
    for (int fseq = 1; fseq <= 10; fseq++) {
      // Create a source file.
      sourceFiles.emplace_back(std::make_unique<unitTests::TempFile>());
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
      // Populate archiveReportURL (required for PostgreSQL scheduler - cannot be empty)
      ar.archiveReportURL = "test://archive-report-url";
      ar.archiveErrorReportURL = "test://error-report-url";
      const auto archiveFileId =
        scheduler.checkAndGetNextArchiveFileId(s_diskInstance, ar.storageClass, ar.requester, logContext);
      archiveFileIds.push_back(archiveFileId);
      scheduler.queueArchiveWithGivenId(archiveFileId, s_diskInstance, ar, logContext);
    }
  }
  scheduler.waitSchedulerDbSubthreadsComplete();
  // Report the drive's existence and put it up in the drive register.
  cta::common::dataStructures::DriveInfo driveInfo("T10D6116",
                                                   "host",
                                                   "TestLogicalLibrary",
                                                   "/dev/tape_T10D6116",
                                                   "dummy");
  // We need to create the drive in the registry before being able to put it up.
  scheduler.reportDriveStatus(driveInfo,
                              cta::common::dataStructures::MountType::NoMount,
                              cta::common::dataStructures::DriveStatus::Down,
                              logContext);
  cta::common::dataStructures::DesiredDriveState driveState;
  driveState.up = true;
  driveState.forceDown = false;
  scheduler.setDesiredDriveState(driveInfo.driveName, driveState, logContext);

  // Create the data transfer session
  TransfersConfig dataTransferConf;
  dataTransferConf.buffer_count = 0;
  dataTransferConf.buffer_size_bytes = 0;
  dataTransferConf.disk_io_threads = 0;
  dataTransferConf.archive.fetch_max_bytes = 0;
  dataTransferConf.archive.fetch_max_files = 0;
  dataTransferConf.archive.flush_max_bytes = 0;
  dataTransferConf.archive.flush_max_files = 0;
  dataTransferConf.archive.underfill.watch_period_secs = 0;
  dataTransferConf.archive.underfill.minimum_samples = 0;
  dataTransferConf.archive.underfill.start_threshold_percent = 0;
  dataTransferConf.archive.underfill.recovery_threshold_percent = 0;
  dataTransferConf.retrieve.fetch_max_bytes = 0;
  dataTransferConf.retrieve.fetch_max_files = 0;
  dataTransferConf.retrieve.rao.enabled = false;
  dataTransferConf.retrieve.rao.lto_algorithm.clear();
  dataTransferConf.encryption.external_key_script.clear();
  dataTransferConf.no_block_move_timeout_secs = 600;
  uint32_t tapeLoadTimeoutSecs = 300;
  dataTransferConf.buffer_size_bytes = 1024 * 1024;  // 1 MB memory buffers
  dataTransferConf.buffer_count = 10;
  dataTransferConf.retrieve.fetch_max_bytes = UINT64_C(100) * 1000 * 1000 * 1000;
  dataTransferConf.retrieve.fetch_max_files = 1000;
  dataTransferConf.archive.fetch_max_bytes = UINT64_C(100) * 1000 * 1000 * 1000;
  dataTransferConf.archive.fetch_max_files = 1000;
  dataTransferConf.disk_io_threads = 1;
  tapeLoadTimeoutSecs = 300;
  dataTransferConf.encryption.enabled = false;
  dataTransferConf.no_block_move_timeout_secs = 600;
  cta::log::DummyLogger dummyLog("dummy", "dummy");
  cta::mediachanger::RmcProxy rmcProxy;
  cta::mediachanger::MediaChangerFacade mc(rmcProxy, dummyLog);
  auto tapeMount = scheduler.getNextMount(driveInfo.logicalLibrary, driveInfo.driveName, logContext);
  ASSERT_NE(nullptr, tapeMount) << logger.getLog();
  TapeSession sess(logger, mockSys, driveInfo, mc, *tapeMount, dataTransferConf, tapeLoadTimeoutSecs, scheduler);
  sess.execute();
  const auto& tracker = sess.tracker();
  EXPECT_EQ(cta::tape::session::SessionType::Archive, tracker.type());
  EXPECT_EQ(cta::tape::session::TapeSessionState::Finished, tracker.state());
  EXPECT_EQ(sourceFiles.size(), tracker.stats().tape.filesCount);
  EXPECT_EQ(1000 * sourceFiles.size(), tracker.stats().tape.dataVolume);
  EXPECT_FALSE(tracker.hasFailures());
  EXPECT_FALSE(tracker.progress().fileBeingMoved);
  EXPECT_TRUE(tracker.activeDiskFiles().empty());
  EXPECT_EQ(1, countLogMessages(logger.getLog(), "Tape session finished"));
  EXPECT_NE(std::string::npos, logger.getLog().find("filesCount=\"10\""));
  EXPECT_NE(std::string::npos, logger.getLog().find("dataVolume=\"10000\""));
  std::string logToCheck = logger.getLog();
  checkTransferTimingLogs(logToCheck, "File successfully transmitted to drive");
  ASSERT_EQ(s_vid, sess.getVid());
  auto afiiter = archiveFileIds.begin();
  for (const auto& sf : sourceFiles) {
    auto afi = *(afiiter++);
    auto afs = catalogue.ArchiveFile()->getArchiveFileById(afi);
    ASSERT_EQ(1, afs.tapeFiles.size());
    cta::checksum::ChecksumBlob checksumBlob;
    checksumBlob.insert(cta::checksum::ADLER32, sf->adler32());
    ASSERT_EQ(afs.checksumBlob, checksumBlob);
    ASSERT_EQ(1000, afs.fileSize);
  }

  // Check logs for drive statistics
  auto logLines = cta::utils::splitStringToVector(logToCheck, '\n');

  // Check if any of the lines contains all of these substrings
  ASSERT_TRUE(std::ranges::any_of(logLines, [](std::string_view line) {
    return cta::utils::containsAllSubStrings(line,
                                             {
                                               R"(firmwareVersion="123A")",
                                               R"(serialNumber="123456")",
                                               R"(mountTotalCorrectedWriteErrors="5")",
                                               R"(mountTotalWriteBytesProcessed="4096")",
                                               R"(mountTotalUncorrectedWriteErrors="1")",
                                               R"(mountTotalNonMediumErrorCounts="2")",
                                             });
  }));

  ASSERT_TRUE(std::ranges::any_of(logLines, [](std::string_view line) {
    return cta::utils::containsAllSubStrings(line,
                                             {
                                               R"(firmwareVersion="123A")",
                                               R"(serialNumber="123456")",
                                               R"(lifetimeMediumEfficiencyPrct="100.0")",
                                               R"(mountReadEfficiencyPrct="100.0")",
                                               R"(mountWriteEfficiencyPrct="100.0")",
                                               R"(mountReadTransients="10)",
                                               R"(mountServoTemps="10")",
                                               R"(mountServoTransients="5")",
                                               R"(mountTemps="100")",
                                               R"(mountTotalReadRetries="25")",
                                               R"(mountTotalWriteRetries="25")",
                                               R"(mountWriteTransients="10")",
                                             });
  }));
}

/*
 * If the first archive file has the wrong size, only that file fails.
 * Later files in the batch must still reach tape and the catalogue.
 */
TEST_P(TapeSessionTest, TapeSessionWrongFileSizeMigration) {
  // This test is the same as TapeSessionGooddayMigration, with
  // wrong file size on the first file migrated. As a fix for #1096, all files
  // except the first should be written to tape and the catalogue

  // 0) Prepare the logger for everyone
  cta::log::StringLogger logger("dummy", "tapedUnitTest", cta::log::DEBUG);
  cta::log::LogContext logContext(logger);

  setupDefaultCatalogue();
  // 1) prepare the fake scheduler
  std::string vid = s_vid;
  // cta::MountType::Enum mountType = cta::MountType::RETRIEVE;

  // 3) Prepare the necessary environment (logger, plus system wrapper),
  cta::tape::System::mockWrapper mockSys;
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
  std::optional<std::string> physicalLibraryName;
  catalogue.LogicalLibrary()->createLogicalLibrary(s_adminOnAdminHost,
                                                   s_libraryName,
                                                   libraryIsDisabled,
                                                   physicalLibraryName,
                                                   libraryComment);
  {
    auto libraries = catalogue.LogicalLibrary()->getLogicalLibraries();
    ASSERT_EQ(1, libraries.size());
    ASSERT_EQ(s_libraryName, libraries.front().name);
    ASSERT_EQ(libraryComment, libraries.front().comment);
  }

  {
    auto tape = getDefaultTape();
    catalogue.Tape()->createTape(s_adminOnAdminHost, tape);
  }

  // Create the mount criteria
  auto mountPolicy = getImmediateMountMountPolicy();
  catalogue.MountPolicy()->createMountPolicy(requester, mountPolicy);
  std::string mountPolicyName = mountPolicy.name;

  catalogue.RequesterMountRule()->createRequesterMountRule(requester,
                                                           mountPolicyName,
                                                           s_diskInstance,
                                                           requester.username,
                                                           "Rule comment");

  //delete is unnecessary
  //pointer with ownership will be passed to the application,
  //which will do the delete
  mockSys.fake.m_pathToDrive["/dev/nst0"] = new cta::tape::drive::FakeDrive();

  // We can prepare files for writing on the drive.
  // Tempfiles are in this scope so they are kept alive
  std::list<std::unique_ptr<unitTests::TempFile>> sourceFiles;
  std::list<uint64_t> archiveFileIds;
  {
    // Label the tape
    cta::tape::tapeFile::LabelSession::label(mockSys.fake.m_pathToDrive["/dev/nst0"], s_vid, false);
    catalogue.Tape()->tapeLabelled(s_vid, "T10D6116");
    mockSys.fake.m_pathToDrive["/dev/nst0"]->rewind();

    // Create the files and schedule the archivals
    const int problematicFseq = 1;
    for (int fseq = 1; fseq <= 10; fseq++) {
      // Create a source file.
      sourceFiles.emplace_back(std::make_unique<unitTests::TempFile>());
      sourceFiles.back()->randomFill(1000);
      remoteFilePaths.push_back(sourceFiles.back()->path());
      // Schedule the archival of the file
      cta::common::dataStructures::ArchiveRequest ar;
      ar.checksumBlob.insert(cta::checksum::ADLER32, sourceFiles.back()->adler32());
      ar.storageClass = s_storageClassName;
      ar.srcURL = std::string("file://") + sourceFiles.back()->path();
      ar.requester.name = requester.username;
      ar.requester.group = "group";
      ar.fileSize = (fseq != problematicFseq) ? 1000 : 900;  // 900 is wrong reported size
      ar.diskFileID = std::to_string(fseq);
      ar.diskFileInfo.path = "y";
      ar.diskFileInfo.owner_uid = DISK_FILE_OWNER_UID;
      ar.diskFileInfo.gid = DISK_FILE_GID;
      // Populate archiveReportURL (required for PostgreSQL scheduler - cannot be empty)
      ar.archiveReportURL = "test://archive-report-url";
      ar.archiveErrorReportURL = "test://error-report-url";
      const auto archiveFileId =
        scheduler.checkAndGetNextArchiveFileId(s_diskInstance, ar.storageClass, ar.requester, logContext);
      archiveFileIds.push_back(archiveFileId);
      scheduler.queueArchiveWithGivenId(archiveFileId, s_diskInstance, ar, logContext);
    }
  }
  scheduler.waitSchedulerDbSubthreadsComplete();
  // Report the drive's existence and put it up in the drive register.
  cta::common::dataStructures::DriveInfo driveInfo("T10D6116",
                                                   "host",
                                                   "TestLogicalLibrary",
                                                   "/dev/tape_T10D6116",
                                                   "dummy");
  // We need to create the drive in the registry before being able to put it up.
  scheduler.reportDriveStatus(driveInfo,
                              cta::common::dataStructures::MountType::NoMount,
                              cta::common::dataStructures::DriveStatus::Down,
                              logContext);
  cta::common::dataStructures::DesiredDriveState driveState;
  driveState.up = true;
  driveState.forceDown = false;
  scheduler.setDesiredDriveState(driveInfo.driveName, driveState, logContext);

  // Create the data transfer session
  TransfersConfig dataTransferConf;
  dataTransferConf.buffer_count = 0;
  dataTransferConf.buffer_size_bytes = 0;
  dataTransferConf.disk_io_threads = 0;
  dataTransferConf.archive.fetch_max_bytes = 0;
  dataTransferConf.archive.fetch_max_files = 0;
  dataTransferConf.archive.flush_max_bytes = 0;
  dataTransferConf.archive.flush_max_files = 0;
  dataTransferConf.archive.underfill.watch_period_secs = 0;
  dataTransferConf.archive.underfill.minimum_samples = 0;
  dataTransferConf.archive.underfill.start_threshold_percent = 0;
  dataTransferConf.archive.underfill.recovery_threshold_percent = 0;
  dataTransferConf.retrieve.fetch_max_bytes = 0;
  dataTransferConf.retrieve.fetch_max_files = 0;
  dataTransferConf.retrieve.rao.enabled = false;
  dataTransferConf.retrieve.rao.lto_algorithm.clear();
  dataTransferConf.encryption.external_key_script.clear();
  dataTransferConf.no_block_move_timeout_secs = 600;
  uint32_t tapeLoadTimeoutSecs = 300;
  dataTransferConf.buffer_size_bytes = 1024 * 1024;  // 1 MB memory buffers
  dataTransferConf.buffer_count = 10;
  dataTransferConf.retrieve.fetch_max_bytes = UINT64_C(100) * 1000 * 1000 * 1000;
  dataTransferConf.retrieve.fetch_max_files = 1000;
  dataTransferConf.archive.fetch_max_bytes = UINT64_C(100) * 1000 * 1000 * 1000;
  dataTransferConf.archive.fetch_max_files = 1000;
  dataTransferConf.disk_io_threads = 1;
  tapeLoadTimeoutSecs = 300;
  dataTransferConf.encryption.enabled = false;
  dataTransferConf.no_block_move_timeout_secs = 600;
  cta::log::DummyLogger dummyLog("dummy", "dummy");
  cta::mediachanger::RmcProxy rmcProxy;
  cta::mediachanger::MediaChangerFacade mc(rmcProxy, dummyLog);
  auto tapeMount = scheduler.getNextMount(driveInfo.logicalLibrary, driveInfo.driveName, logContext);
  ASSERT_NE(nullptr, tapeMount) << logger.getLog();
  TapeSession sess(logger, mockSys, driveInfo, mc, *tapeMount, dataTransferConf, tapeLoadTimeoutSecs, scheduler);
  const auto result = sess.execute();
  EXPECT_TRUE(sess.tracker().outcomeSnapshot().hasFailures);
  EXPECT_FALSE(result.successful);
  std::string logToCheck = logger.getLog();
  ASSERT_EQ(s_vid, sess.getVid());

  auto afiiter = archiveFileIds.begin();
  // First file failed migration, rest made it to the catalogue (fixe for cta/CTA#1096)
  for (auto& sf : sourceFiles) {
    auto afi = *(afiiter++);
    if (afi == 1) {
      ASSERT_THROW(catalogue.ArchiveFile()->getArchiveFileById(afi), cta::exception::Exception);
    } else {
      auto afs = catalogue.ArchiveFile()->getArchiveFileById(afi);
      ASSERT_EQ(1, afs.tapeFiles.size());
      cta::checksum::ChecksumBlob checksumBlob;
      checksumBlob.insert(cta::checksum::ADLER32, sf->adler32());
      ASSERT_EQ(afs.checksumBlob, checksumBlob);
      ASSERT_EQ(1000, afs.fileSize);
    }
  }

  // Check logs for drive statistics
  auto logLines = cta::utils::splitStringToVector(logToCheck, '\n');

  // Check if any of the lines contains all of these substrings
  ASSERT_TRUE(std::ranges::any_of(logLines, [](std::string_view line) {
    return cta::utils::containsAllSubStrings(line,
                                             {
                                               R"(firmwareVersion="123A")",
                                               R"(serialNumber="123456")",
                                               R"(mountTotalCorrectedWriteErrors="5")",
                                               R"(mountTotalWriteBytesProcessed="4096")",
                                               R"(mountTotalUncorrectedWriteErrors="1")",
                                               R"(mountTotalNonMediumErrorCounts="2")",
                                             });
  }));

  ASSERT_TRUE(std::ranges::any_of(logLines, [](std::string_view line) {
    return cta::utils::containsAllSubStrings(line,
                                             {
                                               R"(firmwareVersion="123A")",
                                               R"(serialNumber="123456")",
                                               R"(lifetimeMediumEfficiencyPrct="100.0")",
                                               R"(mountReadEfficiencyPrct="100.0")",
                                               R"(mountWriteEfficiencyPrct="100.0")",
                                               R"(mountReadTransients="10)",
                                               R"(mountServoTemps="10")",
                                               R"(mountServoTransients="5")",
                                               R"(mountTemps="100")",
                                               R"(mountTotalReadRetries="25")",
                                               R"(mountTotalWriteRetries="25")",
                                               R"(mountWriteTransients="10")",
                                             });
  }));
}

/*
 * If the first archive file has the wrong checksum, the session reports its failure.
 * The test records the current behavior for the remaining batch.
 */
TEST_P(TapeSessionTest, TapeSessionWrongChecksumMigration) {
  // This test is the same as TapeSessionGooddayMigration, with
  // wrong file checksum on the first file migrated.
  // Behaviour is different from production due to  cta/CTA#1100

  // 0) Prepare the logger for everyone
  cta::log::StringLogger logger("dummy", "tapedUnitTest", cta::log::DEBUG);
  cta::log::LogContext logContext(logger);

  setupDefaultCatalogue();
  // 1) prepare the fake scheduler
  std::string vid = s_vid;
  // cta::MountType::Enum mountType = cta::MountType::RETRIEVE;

  // 3) Prepare the necessary environment (logger, plus system wrapper),
  cta::tape::System::mockWrapper mockSys;
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
  std::optional<std::string> physicalLibraryName;
  catalogue.LogicalLibrary()->createLogicalLibrary(s_adminOnAdminHost,
                                                   s_libraryName,
                                                   libraryIsDisabled,
                                                   physicalLibraryName,
                                                   libraryComment);
  {
    auto libraries = catalogue.LogicalLibrary()->getLogicalLibraries();
    ASSERT_EQ(1, libraries.size());
    ASSERT_EQ(s_libraryName, libraries.front().name);
    ASSERT_EQ(libraryComment, libraries.front().comment);
  }

  {
    auto tape = getDefaultTape();
    catalogue.Tape()->createTape(s_adminOnAdminHost, tape);
  }

  // Create the mount criteria
  auto mountPolicy = getImmediateMountMountPolicy();
  catalogue.MountPolicy()->createMountPolicy(requester, mountPolicy);
  std::string mountPolicyName = mountPolicy.name;

  catalogue.RequesterMountRule()->createRequesterMountRule(requester,
                                                           mountPolicyName,
                                                           s_diskInstance,
                                                           requester.username,
                                                           "Rule comment");

  //delete is unnecessary
  //pointer with ownership will be passed to the application,
  //which will do the delete
  mockSys.fake.m_pathToDrive["/dev/nst0"] = new cta::tape::drive::FakeDrive();

  // We can prepare files for writing on the drive.
  // Tempfiles are in this scope so they are kept alive
  std::list<std::unique_ptr<unitTests::TempFile>> sourceFiles;
  std::list<uint64_t> archiveFileIds;
  const uint64_t problematicFileId = 5;
  {
    // Label the tape
    cta::tape::tapeFile::LabelSession::label(mockSys.fake.m_pathToDrive["/dev/nst0"], s_vid, false);
    catalogue.Tape()->tapeLabelled(s_vid, "T10D6116");
    mockSys.fake.m_pathToDrive["/dev/nst0"]->rewind();

    // Create the files and schedule the archivals

    //First a file with wrong checksum
    for (uint64_t fileId = 1; fileId <= 10; fileId++) {
      // Create a source file.
      sourceFiles.emplace_back(std::make_unique<unitTests::TempFile>());
      sourceFiles.back()->randomFill(1000);
      remoteFilePaths.push_back(sourceFiles.back()->path());
      // Schedule the archival of the file
      cta::common::dataStructures::ArchiveRequest ar;
      ar.checksumBlob.insert(cta::checksum::ADLER32,
                             (fileId != problematicFileId) ?
                               sourceFiles.back()->adler32() :      // Correct reported checksum
                               sourceFiles.back()->adler32() + 1);  // Wrong reported checksum

      ar.storageClass = s_storageClassName;
      ar.srcURL = std::string("file://") + sourceFiles.back()->path();
      ar.requester.name = requester.username;
      ar.requester.group = "group";
      ar.fileSize = 1000;
      ar.diskFileID = std::to_string(fileId);
      ar.diskFileInfo.path = "y";
      ar.diskFileInfo.owner_uid = DISK_FILE_OWNER_UID;
      ar.diskFileInfo.gid = DISK_FILE_GID;
      // Populate archiveReportURL (required for PostgreSQL scheduler - cannot be empty)
      ar.archiveReportURL = "test://archive-report-url";
      ar.archiveErrorReportURL = "test://error-report-url";
      const auto archiveFileId =
        scheduler.checkAndGetNextArchiveFileId(s_diskInstance, ar.storageClass, ar.requester, logContext);
      archiveFileIds.push_back(archiveFileId);
      scheduler.queueArchiveWithGivenId(archiveFileId, s_diskInstance, ar, logContext);
    }
  }
  scheduler.waitSchedulerDbSubthreadsComplete();
  // Report the drive's existence and put it up in the drive register.
  cta::common::dataStructures::DriveInfo driveInfo("T10D6116",
                                                   "host",
                                                   "TestLogicalLibrary",
                                                   "/dev/tape_T10D6116",
                                                   "dummy");
  // We need to create the drive in the registry before being able to put it up.
  scheduler.reportDriveStatus(driveInfo,
                              cta::common::dataStructures::MountType::NoMount,
                              cta::common::dataStructures::DriveStatus::Down,
                              logContext);
  cta::common::dataStructures::DesiredDriveState driveState;
  driveState.up = true;
  driveState.forceDown = false;
  scheduler.setDesiredDriveState(driveInfo.driveName, driveState, logContext);

  // Create the data transfer session
  TransfersConfig dataTransferConf;
  dataTransferConf.buffer_count = 0;
  dataTransferConf.buffer_size_bytes = 0;
  dataTransferConf.disk_io_threads = 0;
  dataTransferConf.archive.fetch_max_bytes = 0;
  dataTransferConf.archive.fetch_max_files = 0;
  dataTransferConf.archive.flush_max_bytes = 0;
  dataTransferConf.archive.flush_max_files = 0;
  dataTransferConf.archive.underfill.watch_period_secs = 0;
  dataTransferConf.archive.underfill.minimum_samples = 0;
  dataTransferConf.archive.underfill.start_threshold_percent = 0;
  dataTransferConf.archive.underfill.recovery_threshold_percent = 0;
  dataTransferConf.retrieve.fetch_max_bytes = 0;
  dataTransferConf.retrieve.fetch_max_files = 0;
  dataTransferConf.retrieve.rao.enabled = false;
  dataTransferConf.retrieve.rao.lto_algorithm.clear();
  dataTransferConf.encryption.external_key_script.clear();
  dataTransferConf.no_block_move_timeout_secs = 600;
  uint32_t tapeLoadTimeoutSecs = 300;
  dataTransferConf.buffer_size_bytes = 1024 * 1024;  // 1 MB memory buffers
  dataTransferConf.buffer_count = 10;
  dataTransferConf.retrieve.fetch_max_bytes = UINT64_C(100) * 1000 * 1000 * 1000;
  dataTransferConf.retrieve.fetch_max_files = 1000;
  dataTransferConf.archive.fetch_max_bytes = UINT64_C(100) * 1000 * 1000 * 1000;
  dataTransferConf.archive.fetch_max_files = 1000;
  dataTransferConf.disk_io_threads = 1;
  tapeLoadTimeoutSecs = 300;
  dataTransferConf.encryption.enabled = false;
  dataTransferConf.no_block_move_timeout_secs = 600;
  cta::log::DummyLogger dummyLog("dummy", "dummy");
  cta::mediachanger::RmcProxy rmcProxy;
  cta::mediachanger::MediaChangerFacade mc(rmcProxy, dummyLog);
  auto tapeMount = scheduler.getNextMount(driveInfo.logicalLibrary, driveInfo.driveName, logContext);
  ASSERT_NE(nullptr, tapeMount) << logger.getLog();
  TapeSession sess(logger, mockSys, driveInfo, mc, *tapeMount, dataTransferConf, tapeLoadTimeoutSecs, scheduler);
  const auto result = sess.execute();
  EXPECT_TRUE(sess.tracker().outcomeSnapshot().hasFailures);
  EXPECT_FALSE(result.successful);
  std::string logToCheck = logger.getLog();
  ASSERT_EQ(s_vid, sess.getVid());

  // We don't have any guarantee that the order the files were enqueued in
  // is the same as the order in which the data transfer session picks up the jobs.
  // Therefore we must rely on the order of the jobs in the actual queue
  // and not the order in which we enqueued them
  std::vector<uint64_t> queuedArchiveFileIds;
  auto jobsMap = scheduler.getPendingArchiveJobs(logContext);
  for (const auto& [key, jobList] : jobsMap) {
    for (const auto& job : jobList) {
      queuedArchiveFileIds.push_back(job.archiveFileID);
    }
  }

  // Everything up to the wrong checksum in the queue should be transferred correctly
  // Everything afterwards will be requeued (to be picked up later again)
  for (const auto& fileNumber : queuedArchiveFileIds) {
    if (fileNumber < problematicFileId) {
      // Files queued without the wrong checksum made it to the catalogue
      auto afs = catalogue.ArchiveFile()->getArchiveFileById(fileNumber);
    } else {
      // Remaining files were re-queued
      ASSERT_THROW(catalogue.ArchiveFile()->getArchiveFileById(fileNumber), cta::exception::Exception);
    }
  }

  // Check logs for drive statistics
  auto logLines = cta::utils::splitStringToVector(logToCheck, '\n');

  // Check if any of the lines contains all of these substrings
  ASSERT_TRUE(std::ranges::any_of(logLines, [](std::string_view line) {
    return cta::utils::containsAllSubStrings(line,
                                             {
                                               R"(firmwareVersion="123A")",
                                               R"(serialNumber="123456")",
                                               R"(mountTotalCorrectedWriteErrors="5")",
                                               R"(mountTotalWriteBytesProcessed="4096")",
                                               R"(mountTotalUncorrectedWriteErrors="1")",
                                               R"(mountTotalNonMediumErrorCounts="2")",
                                             });
  }));

  ASSERT_TRUE(std::ranges::any_of(logLines, [](std::string_view line) {
    return cta::utils::containsAllSubStrings(line,
                                             {
                                               R"(firmwareVersion="123A")",
                                               R"(serialNumber="123456")",
                                               R"(lifetimeMediumEfficiencyPrct="100.0")",
                                               R"(mountReadEfficiencyPrct="100.0")",
                                               R"(mountWriteEfficiencyPrct="100.0")",
                                               R"(mountReadTransients="10)",
                                               R"(mountServoTemps="10")",
                                               R"(mountServoTransients="5")",
                                               R"(mountTemps="100")",
                                               R"(mountTotalReadRetries="25")",
                                               R"(mountTotalWriteRetries="25")",
                                               R"(mountWriteTransients="10")",
                                             });
  }));
}

/*
 * If a middle archive file has the wrong size, that file fails.
 * The other files must still reach tape and the catalogue.
 */
TEST_P(TapeSessionTest, TapeSessionWrongFilesizeInMiddleOfBatchMigration) {
  // This test is the same as TapeSessionGooddayMigration, with
  // wrong file size on the fifth file migrated. As a fix for #1096, all files
  // except the fifth should be written to tape and the catalogue

  // 0) Prepare the logger for everyone
  cta::log::StringLogger logger("dummy", "tapedUnitTest", cta::log::DEBUG);
  cta::log::LogContext logContext(logger);

  setupDefaultCatalogue();
  // 1) prepare the fake scheduler
  std::string vid = s_vid;
  // cta::MountType::Enum mountType = cta::MountType::RETRIEVE;

  // 3) Prepare the necessary environment (logger, plus system wrapper),
  cta::tape::System::mockWrapper mockSys;
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
  std::optional<std::string> physicalLibraryName;
  catalogue.LogicalLibrary()->createLogicalLibrary(s_adminOnAdminHost,
                                                   s_libraryName,
                                                   libraryIsDisabled,
                                                   physicalLibraryName,
                                                   libraryComment);
  {
    auto libraries = catalogue.LogicalLibrary()->getLogicalLibraries();
    ASSERT_EQ(1, libraries.size());
    ASSERT_EQ(s_libraryName, libraries.front().name);
    ASSERT_EQ(libraryComment, libraries.front().comment);
  }

  {
    auto tape = getDefaultTape();
    catalogue.Tape()->createTape(s_adminOnAdminHost, tape);
  }

  // Create the mount criteria
  auto mountPolicy = getImmediateMountMountPolicy();
  catalogue.MountPolicy()->createMountPolicy(requester, mountPolicy);
  std::string mountPolicyName = mountPolicy.name;

  catalogue.RequesterMountRule()->createRequesterMountRule(requester,
                                                           mountPolicyName,
                                                           s_diskInstance,
                                                           requester.username,
                                                           "Rule comment");

  //delete is unnecessary
  //pointer with ownership will be passed to the application,
  //which will do the delete
  mockSys.fake.m_pathToDrive["/dev/nst0"] = new cta::tape::drive::FakeDrive();

  // We can prepare files for writing on the drive.
  // Tempfiles are in this scope so they are kept alive
  std::list<std::unique_ptr<unitTests::TempFile>> sourceFiles;
  std::list<uint64_t> archiveFileIds;
  const uint64_t problematicFseq = 5;
  {
    // Label the tape
    cta::tape::tapeFile::LabelSession::label(mockSys.fake.m_pathToDrive["/dev/nst0"], s_vid, false);
    catalogue.Tape()->tapeLabelled(s_vid, "T10D6116");
    mockSys.fake.m_pathToDrive["/dev/nst0"]->rewind();

    // Create the files and schedule the archivals
    for (uint64_t fseq = 1; fseq <= 10; fseq++) {
      // Create a source file.
      sourceFiles.emplace_back(std::make_unique<unitTests::TempFile>());
      sourceFiles.back()->randomFill(1000);
      remoteFilePaths.push_back(sourceFiles.back()->path());
      // Schedule the archival of the file
      cta::common::dataStructures::ArchiveRequest ar;
      ar.checksumBlob.insert(cta::checksum::ADLER32, sourceFiles.back()->adler32());
      ar.storageClass = s_storageClassName;
      ar.srcURL = std::string("file://") + sourceFiles.back()->path();
      ar.requester.name = requester.username;
      ar.requester.group = "group";
      ar.fileSize = (fseq != problematicFseq) ? 1000 : 900;  // 900 is wrong reported size
      ar.diskFileID = std::to_string(fseq);
      ar.diskFileInfo.path = "y";
      ar.diskFileInfo.owner_uid = DISK_FILE_OWNER_UID;
      ar.diskFileInfo.gid = DISK_FILE_GID;
      // Populate archiveReportURL (required for PostgreSQL scheduler - cannot be empty)
      ar.archiveReportURL = "test://archive-report-url";
      ar.archiveErrorReportURL = "test://error-report-url";
      const auto archiveFileId =
        scheduler.checkAndGetNextArchiveFileId(s_diskInstance, ar.storageClass, ar.requester, logContext);
      archiveFileIds.push_back(archiveFileId);
      scheduler.queueArchiveWithGivenId(archiveFileId, s_diskInstance, ar, logContext);
    }
  }
  scheduler.waitSchedulerDbSubthreadsComplete();
  // Report the drive's existence and put it up in the drive register.
  cta::common::dataStructures::DriveInfo driveInfo("T10D6116",
                                                   "host",
                                                   "TestLogicalLibrary",
                                                   "/dev/tape_T10D6116",
                                                   "dummy");
  // We need to create the drive in the registry before being able to put it up.
  scheduler.reportDriveStatus(driveInfo,
                              cta::common::dataStructures::MountType::NoMount,
                              cta::common::dataStructures::DriveStatus::Down,
                              logContext);
  cta::common::dataStructures::DesiredDriveState driveState;
  driveState.up = true;
  driveState.forceDown = false;
  scheduler.setDesiredDriveState(driveInfo.driveName, driveState, logContext);

  // Create the data transfer session
  TransfersConfig dataTransferConf;
  dataTransferConf.buffer_count = 0;
  dataTransferConf.buffer_size_bytes = 0;
  dataTransferConf.disk_io_threads = 0;
  dataTransferConf.archive.fetch_max_bytes = 0;
  dataTransferConf.archive.fetch_max_files = 0;
  dataTransferConf.archive.flush_max_bytes = 0;
  dataTransferConf.archive.flush_max_files = 0;
  dataTransferConf.archive.underfill.watch_period_secs = 0;
  dataTransferConf.archive.underfill.minimum_samples = 0;
  dataTransferConf.archive.underfill.start_threshold_percent = 0;
  dataTransferConf.archive.underfill.recovery_threshold_percent = 0;
  dataTransferConf.retrieve.fetch_max_bytes = 0;
  dataTransferConf.retrieve.fetch_max_files = 0;
  dataTransferConf.retrieve.rao.enabled = false;
  dataTransferConf.retrieve.rao.lto_algorithm.clear();
  dataTransferConf.encryption.external_key_script.clear();
  dataTransferConf.no_block_move_timeout_secs = 600;
  uint32_t tapeLoadTimeoutSecs = 300;
  dataTransferConf.buffer_size_bytes = 1024 * 1024;  // 1 MB memory buffers
  dataTransferConf.buffer_count = 10;
  dataTransferConf.retrieve.fetch_max_bytes = UINT64_C(100) * 1000 * 1000 * 1000;
  dataTransferConf.retrieve.fetch_max_files = 1000;
  dataTransferConf.archive.fetch_max_bytes = UINT64_C(100) * 1000 * 1000 * 1000;
  dataTransferConf.archive.fetch_max_files = 1000;
  dataTransferConf.disk_io_threads = 1;
  tapeLoadTimeoutSecs = 300;
  dataTransferConf.encryption.enabled = false;
  dataTransferConf.no_block_move_timeout_secs = 600;
  cta::log::DummyLogger dummyLog("dummy", "dummy");
  cta::mediachanger::RmcProxy rmcProxy;
  cta::mediachanger::MediaChangerFacade mc(rmcProxy, dummyLog);
  auto tapeMount = scheduler.getNextMount(driveInfo.logicalLibrary, driveInfo.driveName, logContext);
  ASSERT_NE(nullptr, tapeMount) << logger.getLog();
  TapeSession sess(logger, mockSys, driveInfo, mc, *tapeMount, dataTransferConf, tapeLoadTimeoutSecs, scheduler);
  const auto result = sess.execute();
  EXPECT_TRUE(sess.tracker().outcomeSnapshot().hasFailures);
  EXPECT_FALSE(result.successful);
  std::string logToCheck = logger.getLog();
  ASSERT_EQ(s_vid, sess.getVid());
  auto afiiter = archiveFileIds.begin();
  uint64_t fseq = 1;
  for (auto& sf : sourceFiles) {
    auto afi = *(afiiter++);
    if (fseq != problematicFseq) {
      // Files queued without the wrong file size made it to the catalogue
      auto afs = catalogue.ArchiveFile()->getArchiveFileById(afi);
      ASSERT_EQ(1, afs.tapeFiles.size());
      cta::checksum::ChecksumBlob checksumBlob;
      checksumBlob.insert(cta::checksum::ADLER32, sf->adler32());
      ASSERT_EQ(afs.checksumBlob, checksumBlob);
      ASSERT_EQ(1000, afs.fileSize);
    } else {
      ASSERT_THROW(catalogue.ArchiveFile()->getArchiveFileById(afi), cta::exception::Exception);
    }
    fseq++;
  }

  // Check logs for drive statistics
  auto logLines = cta::utils::splitStringToVector(logToCheck, '\n');

  // Check if any of the lines contains all of these substrings
  ASSERT_TRUE(std::ranges::any_of(logLines, [](std::string_view line) {
    return cta::utils::containsAllSubStrings(line,
                                             {
                                               R"(firmwareVersion="123A")",
                                               R"(serialNumber="123456")",
                                               R"(mountTotalCorrectedWriteErrors="5")",
                                               R"(mountTotalWriteBytesProcessed="4096")",
                                               R"(mountTotalUncorrectedWriteErrors="1")",
                                               R"(mountTotalNonMediumErrorCounts="2")",
                                             });
  }));

  ASSERT_TRUE(std::ranges::any_of(logLines, [](std::string_view line) {
    return cta::utils::containsAllSubStrings(line,
                                             {
                                               R"(firmwareVersion="123A")",
                                               R"(serialNumber="123456")",
                                               R"(lifetimeMediumEfficiencyPrct="100.0")",
                                               R"(mountReadEfficiencyPrct="100.0")",
                                               R"(mountWriteEfficiencyPrct="100.0")",
                                               R"(mountReadTransients="10)",
                                               R"(mountServoTemps="10")",
                                               R"(mountServoTransients="5")",
                                               R"(mountTemps="100")",
                                               R"(mountTotalReadRetries="25")",
                                               R"(mountTotalWriteRetries="25")",
                                               R"(mountWriteTransients="10")",
                                             });
  }));
}

//
// This test is the same as TapeSessionGooddayMigration, except that the files are deleted
// from filesystem immediately. The disk tasks will then fail on open.
///
/*
 * If archive source files are missing, the session reports those job failures.
 * It must not claim that missing data was written to tape.
 */
TEST_P(TapeSessionTest, TapeSessionMissingFilesMigration) {
  // 0) Prepare the logger for everyone
  cta::log::StringLogger logger("dummy", "tapedUnitTest", cta::log::DEBUG);
  cta::log::LogContext logContext(logger);

  setupDefaultCatalogue();
  // 1) prepare the fake scheduler
  std::string vid = s_vid;
  // cta::MountType::Enum mountType = cta::MountType::RETRIEVE;

  // 3) Prepare the necessary environment (logger, plus system wrapper),
  cta::tape::System::mockWrapper mockSys;
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
  std::optional<std::string> physicalLibraryName;
  catalogue.LogicalLibrary()->createLogicalLibrary(s_adminOnAdminHost,
                                                   s_libraryName,
                                                   libraryIsDisabled,
                                                   physicalLibraryName,
                                                   libraryComment);
  {
    auto libraries = catalogue.LogicalLibrary()->getLogicalLibraries();
    ASSERT_EQ(1, libraries.size());
    ASSERT_EQ(s_libraryName, libraries.front().name);
    ASSERT_EQ(libraryComment, libraries.front().comment);
  }

  {
    auto tape = getDefaultTape();
    catalogue.Tape()->createTape(s_adminOnAdminHost, tape);
  }

  // Create the mount criteria
  auto mountPolicy = getImmediateMountMountPolicy();
  catalogue.MountPolicy()->createMountPolicy(requester, mountPolicy);
  std::string mountPolicyName = mountPolicy.name;

  catalogue.RequesterMountRule()->createRequesterMountRule(requester,
                                                           mountPolicyName,
                                                           s_diskInstance,
                                                           requester.username,
                                                           "Rule comment");

  //delete is unnecessary
  //pointer with ownership will be passed to the application,
  //which will do the delete
  mockSys.fake.m_pathToDrive["/dev/nst0"] = new cta::tape::drive::FakeDrive();

  // We can prepare files for writing on the drive.
  // Tempfiles are in this scope so they are kept alive
  std::list<std::unique_ptr<unitTests::TempFile>> sourceFiles;
  std::list<uint64_t> archiveFileIds;
  {
    // Label the tape
    cta::tape::tapeFile::LabelSession::label(mockSys.fake.m_pathToDrive["/dev/nst0"], s_vid, false);
    catalogue.Tape()->tapeLabelled(s_vid, "T10D6116");
    mockSys.fake.m_pathToDrive["/dev/nst0"]->rewind();

    // Create the files and schedule the archivals
    for (int fseq = 1; fseq <= 10; fseq++) {
      // Create a source file.
      sourceFiles.emplace_back(std::make_unique<unitTests::TempFile>());
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
      // Populate archiveReportURL (required for PostgreSQL scheduler - cannot be empty)
      ar.archiveReportURL = "test://archive-report-url";
      ar.archiveErrorReportURL = "test://error-report-url";
      const auto archiveFileId =
        scheduler.checkAndGetNextArchiveFileId(s_diskInstance, ar.storageClass, ar.requester, logContext);
      archiveFileIds.push_back(archiveFileId);
      scheduler.queueArchiveWithGivenId(archiveFileId, s_diskInstance, ar, logContext);
      // Delete the even files: the migration will work for half of them.
      if (!(fseq % 2)) {
        sourceFiles.pop_back();
      }
    }
  }
  scheduler.waitSchedulerDbSubthreadsComplete();
  // Report the drive's existence and put it up in the drive register.
  cta::common::dataStructures::DriveInfo driveInfo("T10D6116",
                                                   "host",
                                                   "TestLogicalLibrary",
                                                   "/dev/tape_T10D6116",
                                                   "dummy");
  // We need to create the drive in the registry before being able to put it up.
  scheduler.reportDriveStatus(driveInfo,
                              cta::common::dataStructures::MountType::NoMount,
                              cta::common::dataStructures::DriveStatus::Down,
                              logContext);
  cta::common::dataStructures::DesiredDriveState driveState;
  driveState.up = true;
  driveState.forceDown = false;
  scheduler.setDesiredDriveState(driveInfo.driveName, driveState, logContext);

  // Create the data transfer session
  TransfersConfig dataTransferConf;
  dataTransferConf.buffer_count = 0;
  dataTransferConf.buffer_size_bytes = 0;
  dataTransferConf.disk_io_threads = 0;
  dataTransferConf.archive.fetch_max_bytes = 0;
  dataTransferConf.archive.fetch_max_files = 0;
  dataTransferConf.archive.flush_max_bytes = 0;
  dataTransferConf.archive.flush_max_files = 0;
  dataTransferConf.archive.underfill.watch_period_secs = 0;
  dataTransferConf.archive.underfill.minimum_samples = 0;
  dataTransferConf.archive.underfill.start_threshold_percent = 0;
  dataTransferConf.archive.underfill.recovery_threshold_percent = 0;
  dataTransferConf.retrieve.fetch_max_bytes = 0;
  dataTransferConf.retrieve.fetch_max_files = 0;
  dataTransferConf.retrieve.rao.enabled = false;
  dataTransferConf.retrieve.rao.lto_algorithm.clear();
  dataTransferConf.encryption.external_key_script.clear();
  dataTransferConf.no_block_move_timeout_secs = 600;
  uint32_t tapeLoadTimeoutSecs = 300;
  dataTransferConf.buffer_size_bytes = 1024 * 1024;  // 1 MB memory buffers
  dataTransferConf.buffer_count = 10;
  dataTransferConf.retrieve.fetch_max_bytes = UINT64_C(100) * 1000 * 1000 * 1000;
  dataTransferConf.retrieve.fetch_max_files = 1000;
  dataTransferConf.archive.fetch_max_bytes = UINT64_C(100) * 1000 * 1000 * 1000;
  dataTransferConf.archive.fetch_max_files = 1000;
  dataTransferConf.disk_io_threads = 1;
  dataTransferConf.archive.flush_max_bytes = 9999999;
  dataTransferConf.archive.flush_max_files = 9999999;
  tapeLoadTimeoutSecs = 300;
  dataTransferConf.encryption.enabled = false;
  dataTransferConf.no_block_move_timeout_secs = 600;
  cta::log::DummyLogger dummyLog("dummy", "dummy");
  cta::mediachanger::RmcProxy rmcProxy;
  cta::mediachanger::MediaChangerFacade mc(rmcProxy, dummyLog);
  auto tapeMount = scheduler.getNextMount(driveInfo.logicalLibrary, driveInfo.driveName, logContext);
  ASSERT_NE(nullptr, tapeMount) << logger.getLog();
  TapeSession sess(logger, mockSys, driveInfo, mc, *tapeMount, dataTransferConf, tapeLoadTimeoutSecs, scheduler);
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

  ASSERT_EQ(5, count);
  cta::catalogue::TapeSearchCriteria tapeCriteria;
  tapeCriteria.vid = s_vid;
  auto tapeInfo = catalogue.Tape()->getTapes(tapeCriteria);
  ASSERT_EQ(1, tapeInfo.size());
  // We should have max fseq at least 10. It could be higher is a retry manages to sneak in.
  ASSERT_LE(10, tapeInfo.begin()->lastFSeq);
  ASSERT_EQ(5 * 1000, tapeInfo.begin()->dataOnTapeInBytes);

  // Check logs for drive statistics
  std::string logToCheck = logger.getLog();
  auto logLines = cta::utils::splitStringToVector(logToCheck, '\n');

  // Check if any of the lines contains all of these substrings
  ASSERT_TRUE(std::ranges::any_of(logLines, [](std::string_view line) {
    return cta::utils::containsAllSubStrings(line,
                                             {
                                               R"(firmwareVersion="123A")",
                                               R"(serialNumber="123456")",
                                               R"(mountTotalCorrectedWriteErrors="5")",
                                               R"(mountTotalWriteBytesProcessed="4096")",
                                               R"(mountTotalUncorrectedWriteErrors="1")",
                                               R"(mountTotalNonMediumErrorCounts="2")",
                                             });
  }));

  ASSERT_TRUE(std::ranges::any_of(logLines, [](std::string_view line) {
    return cta::utils::containsAllSubStrings(line,
                                             {
                                               R"(firmwareVersion="123A")",
                                               R"(serialNumber="123456")",
                                               R"(lifetimeMediumEfficiencyPrct="100.0")",
                                               R"(mountReadEfficiencyPrct="100.0")",
                                               R"(mountWriteEfficiencyPrct="100.0")",
                                               R"(mountReadTransients="10)",
                                               R"(mountServoTemps="10")",
                                               R"(mountServoTransients="5")",
                                               R"(mountTemps="100")",
                                               R"(mountTotalReadRetries="25")",
                                               R"(mountTotalWriteRetries="25")",
                                               R"(mountWriteTransients="10")",
                                             });
  }));
}

//
// This test is identical to the good day migration, but the tape will accept
// only a finite number of bytes and hence we will report a full tape skip the
// last migrations
//
/*
 * If the tape fills during migration, the session stops writing to that tape.
 * The affected jobs and tape state must reflect the capacity failure.
 */
TEST_P(TapeSessionTest, TapeSessionTapeFullMigration) {
  // 0) Prepare the logger for everyone
  cta::log::StringLogger logger("dummy", "tapedUnitTest", cta::log::DEBUG);
  cta::log::LogContext logContext(logger);

  setupDefaultCatalogue();
  // 1) prepare the fake scheduler
  std::string vid = s_vid;
  // cta::MountType::Enum mountType = cta::MountType::RETRIEVE;

  // 3) Prepare the necessary environment (logger, plus system wrapper),
  cta::tape::System::mockWrapper mockSys;
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
  std::optional<std::string> physicalLibraryName;
  catalogue.LogicalLibrary()->createLogicalLibrary(s_adminOnAdminHost,
                                                   s_libraryName,
                                                   libraryIsDisabled,
                                                   physicalLibraryName,
                                                   libraryComment);
  {
    auto libraries = catalogue.LogicalLibrary()->getLogicalLibraries();
    ASSERT_EQ(1, libraries.size());
    ASSERT_EQ(s_libraryName, libraries.front().name);
    ASSERT_EQ(libraryComment, libraries.front().comment);
  }
  const std::string tapeComment = "Tape comment";

  {
    auto tape = getDefaultTape();
    catalogue.Tape()->createTape(s_adminOnAdminHost, tape);
  }

  auto mountPolicy = getImmediateMountMountPolicy();
  catalogue.MountPolicy()->createMountPolicy(requester, mountPolicy);
  std::string mountPolicyName = mountPolicy.name;

  catalogue.RequesterMountRule()->createRequesterMountRule(requester,
                                                           mountPolicyName,
                                                           s_diskInstance,
                                                           requester.username,
                                                           "Rule comment");

  //delete is unnecessary
  //pointer with ownership will be passed to the application,
  //which will do the delete
  const uint64_t tapeSize = 5000;
  mockSys.fake.m_pathToDrive["/dev/nst0"] = new cta::tape::drive::FakeDrive(tapeSize);

  // We can prepare files for writing on the drive.
  // Tempfiles are in this scope so they are kept alive
  std::list<std::unique_ptr<unitTests::TempFile>> sourceFiles;
  std::list<uint64_t> archiveFileIds;
  {
    // Label the tape
    cta::tape::tapeFile::LabelSession::label(mockSys.fake.m_pathToDrive["/dev/nst0"], s_vid, false);
    catalogue.Tape()->tapeLabelled(s_vid, "T10D6116");
    mockSys.fake.m_pathToDrive["/dev/nst0"]->rewind();

    // Create the files and schedule the archivals
    for (int fseq = 1; fseq <= 10; fseq++) {
      // Create a source file.
      sourceFiles.emplace_back(std::make_unique<unitTests::TempFile>());
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
      // Populate archiveReportURL (required for PostgreSQL scheduler - cannot be empty)
      ar.archiveReportURL = "test://archive-report-url";
      ar.archiveErrorReportURL = "test://error-report-url";
      const auto archiveFileId =
        scheduler.checkAndGetNextArchiveFileId(s_diskInstance, ar.storageClass, ar.requester, logContext);
      archiveFileIds.push_back(archiveFileId);
      scheduler.queueArchiveWithGivenId(archiveFileId, s_diskInstance, ar, logContext);
    }
  }
  scheduler.waitSchedulerDbSubthreadsComplete();
  // Report the drive's existence and put it up in the drive register.
  cta::common::dataStructures::DriveInfo driveInfo("T10D6116",
                                                   "host",
                                                   "TestLogicalLibrary",
                                                   "/dev/tape_T10D6116",
                                                   "dummy");
  // We need to create the drive in the registry before being able to put it up.
  scheduler.reportDriveStatus(driveInfo,
                              cta::common::dataStructures::MountType::NoMount,
                              cta::common::dataStructures::DriveStatus::Down,
                              logContext);
  cta::common::dataStructures::DesiredDriveState driveState;
  driveState.up = true;
  driveState.forceDown = false;
  scheduler.setDesiredDriveState(driveInfo.driveName, driveState, logContext);

  // Create the data transfer session
  TransfersConfig dataTransferConf;
  dataTransferConf.buffer_count = 0;
  dataTransferConf.buffer_size_bytes = 0;
  dataTransferConf.disk_io_threads = 0;
  dataTransferConf.archive.fetch_max_bytes = 0;
  dataTransferConf.archive.fetch_max_files = 0;
  dataTransferConf.archive.flush_max_bytes = 0;
  dataTransferConf.archive.flush_max_files = 0;
  dataTransferConf.archive.underfill.watch_period_secs = 0;
  dataTransferConf.archive.underfill.minimum_samples = 0;
  dataTransferConf.archive.underfill.start_threshold_percent = 0;
  dataTransferConf.archive.underfill.recovery_threshold_percent = 0;
  dataTransferConf.retrieve.fetch_max_bytes = 0;
  dataTransferConf.retrieve.fetch_max_files = 0;
  dataTransferConf.retrieve.rao.enabled = false;
  dataTransferConf.retrieve.rao.lto_algorithm.clear();
  dataTransferConf.encryption.external_key_script.clear();
  dataTransferConf.no_block_move_timeout_secs = 600;
  uint32_t tapeLoadTimeoutSecs = 300;
  dataTransferConf.buffer_size_bytes = 1024 * 1024;  // 1 MB memory buffers
  dataTransferConf.buffer_count = 10;
  dataTransferConf.retrieve.fetch_max_bytes = UINT64_C(100) * 1000 * 1000 * 1000;
  dataTransferConf.retrieve.fetch_max_files = 1000;
  dataTransferConf.archive.fetch_max_bytes = UINT64_C(100) * 1000 * 1000 * 1000;
  dataTransferConf.archive.fetch_max_files = 1000;
  dataTransferConf.disk_io_threads = 1;
  tapeLoadTimeoutSecs = 300;
  dataTransferConf.encryption.enabled = false;
  dataTransferConf.no_block_move_timeout_secs = 600;
  cta::log::DummyLogger dummyLog("dummy", "dummy");
  cta::mediachanger::RmcProxy rmcProxy;
  cta::mediachanger::MediaChangerFacade mc(rmcProxy, dummyLog);
  auto tapeMount = scheduler.getNextMount(driveInfo.logicalLibrary, driveInfo.driveName, logContext);
  ASSERT_NE(nullptr, tapeMount) << logger.getLog();
  TapeSession sess(logger, mockSys, driveInfo, mc, *tapeMount, dataTransferConf, tapeLoadTimeoutSecs, scheduler);
  sess.execute();
  std::string temp = logger.getLog();
  temp += "";
  ASSERT_EQ(s_vid, sess.getVid());
  cta::catalogue::TapeFileSearchCriteria criteria;
  auto afsItor = catalogue.ArchiveFile()->getArchiveFilesItor(criteria);
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
    } else {
      ASSERT_FALSE(afsItor.hasMore());
    }
    // The tape should now be marked as full
    cta::catalogue::TapeSearchCriteria crit;
    crit.vid = s_vid;
    auto tapes = catalogue.Tape()->getTapes(crit);
    ASSERT_EQ(1, tapes.size());
    ASSERT_EQ(s_vid, tapes.front().vid);
    ASSERT_EQ(true, tapes.front().full);
  }
  // Check logs for drive statistics
  std::string logToCheck = logger.getLog();
  auto logLines = cta::utils::splitStringToVector(logToCheck, '\n');

  // Check if any of the lines contains all of these substrings
  ASSERT_TRUE(std::ranges::any_of(logLines, [](std::string_view line) {
    return cta::utils::containsAllSubStrings(line,
                                             {
                                               R"(MSG="Tape session started for write")",
                                               R"(thread="TapeWrite")",
                                               R"(tapeDrive="T10D6116")",
                                               R"(tapeVid="TSTVID")",
                                               R"(mountId="1")",
                                               R"(vo="vo")",
                                               R"(tapePool="TestTapePool")",
                                               R"(mediaType="LTO7M")",
                                               R"(logicalLibrary="TestLogicalLibrary")",
                                               R"(mountType="ArchiveForUser")",
                                               R"(vendor="TestVendor")",
                                               R"(capacityInBytes="12345678")",
                                             });
  }));

  ASSERT_TRUE(std::ranges::any_of(logLines, [](std::string_view line) {
    return cta::utils::containsAllSubStrings(line,
                                             {
                                               R"(firmwareVersion="123A")",
                                               R"(serialNumber="123456")",
                                               R"(mountTotalCorrectedWriteErrors="5")",
                                               R"(mountTotalWriteBytesProcessed="4096")",
                                               R"(mountTotalUncorrectedWriteErrors="1")",
                                               R"(mountTotalNonMediumErrorCounts="2")",
                                             });
  }));

  ASSERT_TRUE(std::ranges::any_of(logLines, [](std::string_view line) {
    return cta::utils::containsAllSubStrings(line,
                                             {
                                               R"(firmwareVersion="123A")",
                                               R"(serialNumber="123456")",
                                               R"(lifetimeMediumEfficiencyPrct="100.0")",
                                               R"(mountReadEfficiencyPrct="100.0")",
                                               R"(mountWriteEfficiencyPrct="100.0")",
                                               R"(mountReadTransients="10)",
                                               R"(mountServoTemps="10")",
                                               R"(mountServoTransients="5")",
                                               R"(mountTemps="100")",
                                               R"(mountTotalReadRetries="25")",
                                               R"(mountTotalWriteRetries="25")",
                                               R"(mountWriteTransients="10")",
                                             });
  }));
}

/*
 * If the tape fills while flushing archive data, the session handles the late failure.
 * The affected jobs and tape state must reflect the capacity failure.
 */
TEST_P(TapeSessionTest, TapeSessionTapeFullOnFlushMigration) {
  // 0) Prepare the logger for everyone
  cta::log::StringLogger logger("dummy", "tapedUnitTest", cta::log::DEBUG);
  cta::log::LogContext logContext(logger);

  setupDefaultCatalogue();
  // 1) prepare the fake scheduler
  // cta::MountType::Enum mountType = cta::MountType::RETRIEVE;

  // 3) Prepare the necessary environment (logger, plus system wrapper),
  cta::tape::System::mockWrapper mockSys;
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
  std::optional<std::string> physicalLibraryName;
  catalogue.LogicalLibrary()->createLogicalLibrary(s_adminOnAdminHost,
                                                   s_libraryName,
                                                   libraryIsDisabled,
                                                   physicalLibraryName,
                                                   libraryComment);
  {
    auto libraries = catalogue.LogicalLibrary()->getLogicalLibraries();
    ASSERT_EQ(1, libraries.size());
    ASSERT_EQ(s_libraryName, libraries.front().name);
    ASSERT_EQ(libraryComment, libraries.front().comment);
  }

  {
    auto tape = getDefaultTape();
    catalogue.Tape()->createTape(s_adminOnAdminHost, tape);
  }

  // Create the mount criteria
  auto mountPolicy = getImmediateMountMountPolicy();
  catalogue.MountPolicy()->createMountPolicy(requester, mountPolicy);
  std::string mountPolicyName = mountPolicy.name;

  catalogue.RequesterMountRule()->createRequesterMountRule(requester,
                                                           mountPolicyName,
                                                           s_diskInstance,
                                                           requester.username,
                                                           "Rule comment");

  //delete is unnecessary
  //pointer with ownership will be passed to the application,
  //which will do the delete
  const uint64_t tapeSize = 5000;
  mockSys.fake.m_pathToDrive["/dev/nst0"] =
    new cta::tape::drive::FakeDrive(tapeSize, cta::tape::drive::FakeDrive::OnFlush);

  // We can prepare files for writing on the drive.
  // Tempfiles are in this scope so they are kept alive
  std::list<std::unique_ptr<unitTests::TempFile>> sourceFiles;
  std::list<uint64_t> archiveFileIds;
  {
    // Label the tape
    cta::tape::tapeFile::LabelSession::label(mockSys.fake.m_pathToDrive["/dev/nst0"], s_vid, false);
    catalogue.Tape()->tapeLabelled(s_vid, "T10D6116");
    mockSys.fake.m_pathToDrive["/dev/nst0"]->rewind();

    // Create the files and schedule the archivals
    for (int fseq = 1; fseq <= 10; fseq++) {
      // Create a source file.
      sourceFiles.emplace_back(std::make_unique<unitTests::TempFile>());
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
      // Populate archiveReportURL (required for PostgreSQL scheduler - cannot be empty)
      ar.archiveReportURL = "test://archive-report-url";
      ar.archiveErrorReportURL = "test://error-report-url";
      const auto archiveFileId =
        scheduler.checkAndGetNextArchiveFileId(s_diskInstance, ar.storageClass, ar.requester, logContext);
      archiveFileIds.push_back(archiveFileId);
      scheduler.queueArchiveWithGivenId(archiveFileId, s_diskInstance, ar, logContext);
    }
  }
  scheduler.waitSchedulerDbSubthreadsComplete();
  // Report the drive's existence and put it up in the drive register.
  cta::common::dataStructures::DriveInfo driveInfo("T10D6116",
                                                   "host",
                                                   "TestLogicalLibrary",
                                                   "/dev/tape_T10D6116",
                                                   "dummy");
  // We need to create the drive in the registry before being able to put it up.
  scheduler.reportDriveStatus(driveInfo,
                              cta::common::dataStructures::MountType::NoMount,
                              cta::common::dataStructures::DriveStatus::Down,
                              logContext);
  cta::common::dataStructures::DesiredDriveState driveState;
  driveState.up = true;
  driveState.forceDown = false;
  scheduler.setDesiredDriveState(driveInfo.driveName, driveState, logContext);

  // Create the data transfer session
  TransfersConfig dataTransferConf;
  dataTransferConf.buffer_count = 0;
  dataTransferConf.buffer_size_bytes = 0;
  dataTransferConf.disk_io_threads = 0;
  dataTransferConf.archive.fetch_max_bytes = 0;
  dataTransferConf.archive.fetch_max_files = 0;
  dataTransferConf.archive.flush_max_bytes = 0;
  dataTransferConf.archive.flush_max_files = 0;
  dataTransferConf.archive.underfill.watch_period_secs = 0;
  dataTransferConf.archive.underfill.minimum_samples = 0;
  dataTransferConf.archive.underfill.start_threshold_percent = 0;
  dataTransferConf.archive.underfill.recovery_threshold_percent = 0;
  dataTransferConf.retrieve.fetch_max_bytes = 0;
  dataTransferConf.retrieve.fetch_max_files = 0;
  dataTransferConf.retrieve.rao.enabled = false;
  dataTransferConf.retrieve.rao.lto_algorithm.clear();
  dataTransferConf.encryption.external_key_script.clear();
  dataTransferConf.no_block_move_timeout_secs = 600;
  uint32_t tapeLoadTimeoutSecs = 300;
  dataTransferConf.buffer_size_bytes = 1024 * 1024;  // 1 MB memory buffers
  dataTransferConf.buffer_count = 10;
  dataTransferConf.retrieve.fetch_max_bytes = UINT64_C(100) * 1000 * 1000 * 1000;
  dataTransferConf.retrieve.fetch_max_files = 1000;
  dataTransferConf.archive.fetch_max_bytes = UINT64_C(100) * 1000 * 1000 * 1000;
  dataTransferConf.archive.fetch_max_files = 1000;
  dataTransferConf.disk_io_threads = 1;
  tapeLoadTimeoutSecs = 300;
  dataTransferConf.encryption.enabled = false;
  dataTransferConf.no_block_move_timeout_secs = 600;
  cta::log::DummyLogger dummyLog("dummy", "dummy");
  cta::mediachanger::RmcProxy rmcProxy;
  cta::mediachanger::MediaChangerFacade mc(rmcProxy, dummyLog);
  auto tapeMount = scheduler.getNextMount(driveInfo.logicalLibrary, driveInfo.driveName, logContext);
  ASSERT_NE(nullptr, tapeMount) << logger.getLog();
  TapeSession sess(logger, mockSys, driveInfo, mc, *tapeMount, dataTransferConf, tapeLoadTimeoutSecs, scheduler);
  sess.execute();
  std::string temp = logger.getLog();
  temp += "";
  ASSERT_EQ(s_vid, sess.getVid());
  cta::catalogue::TapeFileSearchCriteria criteria;
  auto afsItor = catalogue.ArchiveFile()->getArchiveFilesItor(criteria);
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
    } else {
      ASSERT_FALSE(afsItor.hasMore());
    }
    // The tape should now be marked as full
    cta::catalogue::TapeSearchCriteria crit;
    crit.vid = s_vid;
    auto tapes = catalogue.Tape()->getTapes(crit);
    ASSERT_EQ(1, tapes.size());
    ASSERT_EQ(s_vid, tapes.front().vid);
    ASSERT_EQ(true, tapes.front().full);
  }
  // Check logs for drive statistics
  std::string logToCheck = logger.getLog();
  auto logLines = cta::utils::splitStringToVector(logToCheck, '\n');

  // Check if any of the lines contains all of these substrings
  ASSERT_TRUE(std::ranges::any_of(logLines, [](std::string_view line) {
    return cta::utils::containsAllSubStrings(line,
                                             {
                                               R"(firmwareVersion="123A")",
                                               R"(serialNumber="123456")",
                                               R"(mountTotalCorrectedWriteErrors="5")",
                                               R"(mountTotalWriteBytesProcessed="4096")",
                                               R"(mountTotalUncorrectedWriteErrors="1")",
                                               R"(mountTotalNonMediumErrorCounts="2")",
                                             });
  }));

  ASSERT_TRUE(std::ranges::any_of(logLines, [](std::string_view line) {
    return cta::utils::containsAllSubStrings(line,
                                             {
                                               R"(firmwareVersion="123A")",
                                               R"(serialNumber="123456")",
                                               R"(lifetimeMediumEfficiencyPrct="100.0")",
                                               R"(mountReadEfficiencyPrct="100.0")",
                                               R"(mountWriteEfficiencyPrct="100.0")",
                                               R"(mountReadTransients="10)",
                                               R"(mountServoTemps="10")",
                                               R"(mountServoTransients="5")",
                                               R"(mountTemps="100")",
                                               R"(mountTotalReadRetries="25")",
                                               R"(mountTotalWriteRetries="25")",
                                               R"(mountWriteTransients="10")",
                                             });
  }));
}

#undef TEST_MOCK_DB
#ifdef TEST_MOCK_DB
static cta::MockSchedulerDatabaseFactory mockDbFactory;
#ifdef CTA_PGSCHED
INSTANTIATE_TEST_CASE_P(MockSchedulerTest,
                        RelationalDBSchedulerTest,
                        ::testing::Values(SchedulerTestParam(mockDbFactory)));
#else
INSTANTIATE_TEST_CASE_P(MockSchedulerTest, SchedulerTest, ::testing::Values(SchedulerTestParam(mockDbFactory)));
#endif
#endif

#ifdef CTA_PGSCHED
static cta::RelationalDBTestFactory RelationalDBTestFactoryStatic;

INSTANTIATE_TEST_CASE_P(RelationalDBPlusMockSchedulerTest,
                        TapeSessionTest,
                        ::testing::Values(TapeSessionTestParam(RelationalDBTestFactoryStatic)));
#else
#define TEST_VFS
#ifdef TEST_VFS
static cta::OStoreDBFactory<cta::objectstore::BackendVFS> OStoreDBFactoryVFS;

INSTANTIATE_TEST_CASE_P(OStoreDBPlusMockSchedulerTestVFS,
                        TapeSessionTest,
                        ::testing::Values(TapeSessionTestParam(OStoreDBFactoryVFS)));
#endif

#ifdef TEST_RADOS
static cta::OStoreDBFactory<cta::objectstore::BackendRados> OStoreDBFactoryRados("rados://tapetest@tapetest");

INSTANTIATE_TEST_CASE_P(OStoreDBPlusMockSchedulerTestRados,
                        TapeSessionTest,
                        ::testing::Values(TapeSessionTestParam(OStoreDBFactoryRados)));
#endif
#endif

}  // namespace unitTests
