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
#include <gtest/gtest.h>

#include "DriveHandler.hpp"
#include "ProcessManager.hpp"
#include "catalogue/InMemoryCatalogue.hpp"
#include "common/log/Constants.hpp"
#include "common/log/LogContext.hpp"
#include "common/log/StringLogger.hpp"
#include "scheduler/OStoreDB/OStoreDBFactory.hpp"
#include "tapeserver/castor/messages/TapeserverProxyDummy.hpp"
#include "tapeserver/castor/tape/tapeserver/daemon/TapeSessionReporter.hpp"
#include "tests/TempFile.hpp"
#include "scheduler/Scheduler.hpp"
#include "scheduler/testingMocks/MockArchiveMount.hpp"
#include "tapeserver/castor/tape/tapeserver/file/FileWriter.hpp"

#include <utility>
#include <sys/signalfd.h>

namespace unitTests {

using namespace cta::tape::daemon;

namespace {
/**
 * This structure is used to parameterize scheduler tests.
 */
struct DriveHandlerTestParam {
  cta::SchedulerDatabaseFactory& dbFactory;

  explicit DriveHandlerTestParam(cta::SchedulerDatabaseFactory& dbFactory) : dbFactory(dbFactory) {}
};
}

class DriveHandlerTest : public ::testing::TestWithParam<DriveHandlerTestParam> {
public:
  std::unique_ptr<cta::SchedulerDatabase> m_db;
  std::unique_ptr<cta::catalogue::Catalogue> m_catalogue;
  std::unique_ptr<cta::Scheduler> m_scheduler;
  cta::log::DummyLogger m_dummyLog;

  DriveHandlerTest() : m_dummyLog("dummy", "dummy") {}

  class FailedToGetSchedulerDB : public std::exception {
  public:
    const char *what() const noexcept override { return "Failed to get object store db."; }
  };

  cta::objectstore::OStoreDBWrapperInterface& getSchedulerDB() {
    cta::objectstore::OStoreDBWrapperInterface *const ptr =
      dynamic_cast<cta::objectstore::OStoreDBWrapperInterface *const>(m_db.get());
    if (nullptr == ptr) {
      throw FailedToGetSchedulerDB();
    }
    return *ptr;
  }

  void SetUp() override {
    using namespace cta;

    const DriveHandlerTestParam& param = GetParam();
    const uint64_t nbConns = 1;
    const uint64_t nbArchiveFileListingConns = 1;

    m_catalogue = std::make_unique<catalogue::InMemoryCatalogue>(m_dummyLog, nbConns, nbArchiveFileListingConns);
    m_db = param.dbFactory.create(m_catalogue);
    m_scheduler = std::make_unique<Scheduler>(*m_catalogue, *m_db, 5, 2 * 1000 * 1000);
  }

  void TearDown() override {
    m_scheduler.reset();
    m_catalogue.reset();
    m_db.reset();
  }
};

class DummyTapeSessionReporter : cta::threading::Thread {
private:
  cta::tape::daemon::DriveHandler& m_handler;
  std::string m_vid;

public:
  explicit DummyTapeSessionReporter(cta::tape::daemon::DriveHandler& handler) : m_handler(handler) {

  }

  void reportState(cta::tape::session::SessionState state) {
    m_fifo.push(new Report(state));
  }

  void setVid(const std::string& vid) {
    m_vid = vid;
  }

  void finish() {
    m_fifo.push(nullptr);
  }

  void startThreads() {
    start();
  }

  void waitThreads() {
    try {
      wait();
    } catch (...) {

    }
  }

private:
  void run() override {
    while (true) {
      sleep(1);
      std::unique_ptr<Report> currentReport(m_fifo.pop());
      if (nullptr == currentReport) {
        break;
      }
      try {
        currentReport->execute(*this);
      } catch (const std::exception& e) {}
    }
  }

  class Report {
  public:
    explicit Report(cta::tape::session::SessionState state) : m_state(state) {};

    ~Report() = default;

    void execute(DummyTapeSessionReporter& parent) const {
      serializers::WatchdogMessage watchdogMessage;
      watchdogMessage.set_reportingstate(true);
      watchdogMessage.set_reportingbytes(false);
      watchdogMessage.set_totaldiskbytesmoved(0);
      watchdogMessage.set_totaltapebytesmoved(0);
      watchdogMessage.set_sessionstate((uint32_t) m_state);
      watchdogMessage.set_sessiontype((uint32_t) cta::tape::session::SessionType::Undetermined);
      watchdogMessage.set_vid(parent.m_vid);
      std::string buffer;
      if (!watchdogMessage.SerializeToString(&buffer)) {
        throw cta::exception::Exception(std::string("In DriveHandlerProxy::reportState(): could not serialize: ") +
                                        watchdogMessage.InitializationErrorString());
      }
      parent.m_handler.getSocketPair().send(buffer);
    }

    cta::tape::session::SessionState m_state;
  };

  /**
   * m_fifo is holding all the report waiting to be processed
   */
  cta::threading::BlockingQueue<Report *> m_fifo;
};


TEST_P(DriveHandlerTest, TriggerCleanerSessionAtTheEndOfSession) {

  // Create the log context
  cta::log::StringLogger dlog("dummy", "unitTest", cta::log::DEBUG);
  cta::log::LogContext lc(dlog);

  // Create the config files and initiate the catalogue and the scheduler
  cta::tape::daemon::TpconfigLine driveConfig("T10D6116", "TestLogicalLibrary", "/dev/tape_T10D6116", "dummy");
  cta::common::dataStructures::DriveInfo driveInfo;
  driveInfo.driveName = driveConfig.unitName;
  driveInfo.logicalLibrary = driveConfig.logicalLibrary;
  driveInfo.host = "host";

  m_catalogue.reset(nullptr);

  char envXrdSecPROTOCOL[] = "XrdSecPROTOCOL=krb5";
  char envKRB5CCNAME[] = "KRB5CCNAME=/tmp/cta/krb5cc_0";
  char envXrdSecSSSKT[] = "XrdSecSSSKT=/var/tmp/cta-test-temporary-kt";

  putenv(envXrdSecPROTOCOL);
  putenv(envKRB5CCNAME);
  putenv(envXrdSecSSSKT);

  TempFile ctaConf, catalogueConfig, tpConfig;
  catalogueConfig.stringFill("in_memory");
  ctaConf.stringFill("ObjectStore BackendPath ");
  ctaConf.stringAppend(getSchedulerDB().getBackend().getParams()->toURL());
  ctaConf.stringAppend("\n"
                       "taped CatalogueConfigFile ");
  ctaConf.stringAppend(catalogueConfig.path());
  ctaConf.stringAppend("\n"
                       "taped BufferCount 1\n"
                       "taped TpConfigPath ");
  ctaConf.stringAppend(tpConfig.path());
  auto completeConfig = cta::tape::daemon::TapedConfiguration::createFromCtaConf(ctaConf.path());

  // Create the process manager and drive handler
  cta::tape::daemon::ProcessManager processManager(lc);

  std::unique_ptr<cta::tape::daemon::DriveHandler> driveHandler(
    new cta::tape::daemon::DriveHandler(completeConfig, driveConfig, processManager));

  processManager.addHandler(std::move(driveHandler));

  // Get back the drive handler and let the reporter send messages
  DriveHandler& handler = dynamic_cast<DriveHandler&>(processManager.at("drive:T10D6116"));
  DummyTapeSessionReporter reporter(handler);

  {
    reporter.startThreads();
    reporter.reportState(cta::tape::session::SessionState::Scheduling);
    reporter.setVid("T12345");
    reporter.reportState(cta::tape::session::SessionState::Mounting);
    reporter.reportState(cta::tape::session::SessionState::Running);
    reporter.reportState(cta::tape::session::SessionState::Fatal);
    reporter.finish();
  }

  // Run the process manager that should exit after Fatal state
  processManager.run();
  reporter.waitThreads();

  std::string logToCheck = dlog.getLog();
  ASSERT_EQ(std::string::npos, logToCheck.find("Should run cleaner but VID is missing"));
  ASSERT_NE(std::string::npos, logToCheck.find("starting cleaner"));
}

#undef TEST_MOCK_DB
#ifdef TEST_MOCK_DB
static cta::MockSchedulerDatabaseFactory mockDbFactory;
INSTANTIATE_TEST_CASE_P(MockSchedulerTest, SchedulerTest, ::testing::Values(SchedulerTestParam(mockDbFactory)));
#endif

#define TEST_VFS
#ifdef TEST_VFS
static cta::OStoreDBFactory<cta::objectstore::BackendVFS> OStoreDBFactoryVFS;

INSTANTIATE_TEST_CASE_P(OStoreDBPlusMockSchedulerTestVFS, DriveHandlerTest,
                        ::testing::Values(DriveHandlerTestParam(OStoreDBFactoryVFS)));
#endif

#ifdef TEST_RADOS
static cta::OStoreDBFactory<cta::objectstore::BackendRados> OStoreDBFactoryRados("rados://tapetest@tapetest");

INSTANTIATE_TEST_CASE_P(OStoreDBPlusMockSchedulerTestRados, DataTransferSessionTest,
                        ::testing::Values(DataTransferSessionTestParam(OStoreDBFactoryRados)));
#endif

}  // namespace unitTests