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
#include <gmock/gmock.h>

#include "DriveHandler.hpp"
#include "DriveHandlerProxy.hpp"
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
#include "tapeserver/castor/tape/tapeserver/file/FileWriter.hpp"

#include "common/log/StdoutLogger.hpp"
#include <iostream>

#include <utility>
#include <sys/signalfd.h>

namespace unitTests {

using namespace cta::tape::daemon;

using testing::_;

namespace {
/**
 * This structure is used to parameterize scheduler tests.
 */
struct DriveHandlerTestParam {
  cta::SchedulerDatabaseFactory& dbFactory;

  explicit DriveHandlerTestParam(cta::SchedulerDatabaseFactory& dbFactory) : dbFactory(dbFactory) {}
};
}

/**
 * Class that mimics the tape session reporter to change the data session states
 */
class DummyTapeSessionReporter : cta::threading::Thread {
public:
  explicit DummyTapeSessionReporter(const std::shared_ptr<cta::server::SocketPair>& socketPair)
    : m_socketPair(socketPair) {}

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
    // Wait for 1 second - the drive handler should be configured by that time
    sleep(1);
    while (true) {
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
      watchdogMessage.set_sessionstate(static_cast<uint32_t>(m_state));
      watchdogMessage.set_sessiontype(static_cast<uint32_t>(cta::tape::session::SessionType::Undetermined));
      watchdogMessage.set_vid(parent.m_vid);
      std::string buffer;
      if (!watchdogMessage.SerializeToString(&buffer)) {
        throw cta::exception::Exception(std::string("In DriveHandlerProxy::reportState(): could not serialize: ") +
                                        watchdogMessage.InitializationErrorString());
      }
      parent.m_socketPair->send(buffer);
    }

    cta::tape::session::SessionState m_state;
  };

  /**
   * m_fifo is holding all the report waiting to be processed
   */
  cta::threading::BlockingQueue<Report *> m_fifo;

  const std::shared_ptr<cta::server::SocketPair>& m_socketPair;

  /**
   * Tape VID will be passed to the tape session reporter at specific point of time.
   * It is essential for the cleaner session that VID is reported during state change
   */
  std::string m_vid;
};

/**
 * Mock class for DriveHandlerProxy to call fake reportHeartbeat()
 */
class MockDriveHandlerProxy : public DriveHandlerProxy {
public:
  explicit MockDriveHandlerProxy(const std::shared_ptr<cta::server::SocketPair>& socketPair)
    : DriveHandlerProxy(socketPair) {
  }

  MOCK_METHOD2(reportHeartbeat, void(uint64_t totalTapeBytesMoved, uint64_t totalDiskBytesMoved));
};

/**
 * The data handler test is a parameterized test. It takes a scheduler database factory as a parameter.
 */
class DriveHandlerTest : public ::testing::TestWithParam<DriveHandlerTestParam> {
public:
  std::unique_ptr<cta::SchedulerDatabase> m_db;
  std::unique_ptr<cta::catalogue::Catalogue> m_catalogue;
  std::unique_ptr<cta::Scheduler> m_scheduler;
  cta::log::StringLogger m_dummyLog;
  cta::log::LogContext m_lc;
  cta::tape::daemon::TpconfigLine m_driveConfig;

  TempFile m_ctaConf, m_catalogueConfig, m_tpConfig;

  TapedConfiguration m_completeConfig;

  DriveHandlerTest() : m_dummyLog("dummy", "unitTest", cta::log::DEBUG), m_lc(m_dummyLog),
                       m_driveConfig("T10D6116", "TestLogicalLibrary", "/dev/tape_T10D6116", "dummy") {}

  class FailedToGetSchedulerDB : public std::exception {
  public:
    const char *what() const noexcept override { return "Failed to get object store db."; }
  };

  cta::objectstore::OStoreDBWrapperInterface& getSchedulerDB() const {
    auto *const ptr = dynamic_cast<cta::objectstore::OStoreDBWrapperInterface *const>(m_db.get());
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

    // Create the config files and initiate the catalogue and the scheduler
    cta::common::dataStructures::DriveInfo driveInfo;
    driveInfo.driveName = m_driveConfig.unitName;
    driveInfo.logicalLibrary = m_driveConfig.logicalLibrary;
    driveInfo.host = "host";

    // DriveHandler will try to re-init the catalogue. SQLite (InMemory) catalogue will try to create new schema,
    // it will fail. We need to reset m_catalogue here to work around this bug
    // TODO: fix the bug in SchemaCreatingSqliteCatalogue()
    m_catalogue.reset(nullptr);

    m_catalogueConfig.stringFill("in_memory");
    m_ctaConf.stringFill("ObjectStore BackendPath ");
    std::unique_ptr<cta::objectstore::Backend::Parameters> params(getSchedulerDB().getBackend().getParams());
    m_ctaConf.stringAppend(params->toURL());
    m_ctaConf.stringAppend("\n"
                           "taped CatalogueConfigFile ");
    m_ctaConf.stringAppend(m_catalogueConfig.path());
    m_ctaConf.stringAppend("\n"
                           "taped BufferCount 1\n"
                           "taped TpConfigPath ");
    m_ctaConf.stringAppend(m_tpConfig.path());
    m_ctaConf.stringAppend("\n"
                           "taped WatchdogDownUpTransitionTimeout 0");
    m_completeConfig = cta::tape::daemon::TapedConfiguration::createFromCtaConf(m_ctaConf.path());

    m_socketPair = std::make_shared<cta::server::SocketPair>();
    m_mockProxy = std::make_shared<testing::NiceMock<MockDriveHandlerProxy>>(m_socketPair);
    ON_CALL(*m_mockProxy, reportHeartbeat(_, _)).WillByDefault(
      testing::Invoke(this, &DriveHandlerTest::reportHeartbeat));
  }

  void TearDown() override {
    m_scheduler.reset();
    m_catalogue.reset();
    m_db.reset();
  }

  void reportHeartbeat(uint64_t totalTapeBytesMoved, uint64_t totalDiskBytesMoved) {
    m_lc.log(cta::log::DEBUG, "DriveHandlerTest::reportHeartbeat()");
    serializers::WatchdogMessage watchdogMessage;
    watchdogMessage.set_reportingstate(false);
    watchdogMessage.set_reportingbytes(true);
    watchdogMessage.set_totaltapebytesmoved(totalTapeBytesMoved);
    watchdogMessage.set_totaldiskbytesmoved(totalDiskBytesMoved);
    std::string buffer;
    if (!watchdogMessage.SerializeToString(&buffer)) {
      throw cta::exception::Exception(std::string("In DriveHandlerProxy::reportHeartbeat(): could not serialize: ")+
          watchdogMessage.InitializationErrorString());
    }
    m_lc.log(cta::log::DEBUG, "DriveHandlerTest::reportHeartbeat(): Send Message");
    m_socketPair->send(buffer);
    m_lc.log(cta::log::DEBUG, "DriveHandlerTest::reportHeartbeat(): Message has been sent");

    // Listen for any possible messages from the parent process
    // If it is a state change, echo it back to parent
    try {
      serializers::WatchdogMessage message;
      m_lc.log(cta::log::DEBUG, "DriveHandlerTest::reportHeartbeat(): Receive Message");
      const auto datagram = m_socketPair->receive();
      m_lc.log(cta::log::DEBUG, "DriveHandlerTest::reportHeartbeat(): Message has been received");
      if (!message.ParseFromString(datagram)) {
        // Use the tolerant parser to assess the situation.
        message.ParsePartialFromString(datagram);
        throw cta::exception::Exception(
          std::string("In SubprocessHandler::ProcessingStatus(): could not parse message: ") +
          message.InitializationErrorString());
      }
      if (message.reportingstate()) {
        m_mockProxy->reportState(static_cast<cta::tape::session::SessionState>(message.sessionstate()),
                                 static_cast<cta::tape::session::SessionType>(message.sessiontype()),
                                 message.vid());
      }
    }
    catch (cta::server::SocketPair::NothingToReceive&) {
      m_lc.log(cta::log::DEBUG, "DriveHandlerTest::reportHeartbeat(): Nothing to Receive");
    }
  }

protected:
  std::shared_ptr<cta::server::SocketPair> m_socketPair;
  std::shared_ptr<testing::NiceMock<MockDriveHandlerProxy>> m_mockProxy;
};

char envXrdSecPROTOCOL[] = "XrdSecPROTOCOL=krb5";
char envKRB5CCNAME[] = "KRB5CCNAME=/tmp/cta/krb5cc_0";
char envXrdSecSSSKT[] = "XrdSecSSSKT=/var/tmp/cta-test-temporary-kt";

void setEnvVars() {
  putenv(envXrdSecPROTOCOL);
  putenv(envKRB5CCNAME);
  putenv(envXrdSecSSSKT);
}

TEST_P(DriveHandlerTest, TriggerCleanerSessionAtTheEndOfSession) {
  // Create the process manager and drive handler
  cta::tape::daemon::ProcessManager processManager(m_lc);

  auto driveHandler = std::make_unique<DriveHandler>(m_completeConfig, m_driveConfig, processManager, m_mockProxy);
  processManager.addHandler(std::move(driveHandler));

  // Get back the drive handler and let the reporter send messages
  DummyTapeSessionReporter reporter(m_socketPair);

  // Imitate intercommunication
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
  setEnvVars();
  processManager.run();
  reporter.waitThreads();

  std::string logToCheck = m_dummyLog.getLog();
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

INSTANTIATE_TEST_SUITE_P(OStoreDBPlusMockSchedulerTestVFS, DriveHandlerTest,
                         ::testing::Values(DriveHandlerTestParam(OStoreDBFactoryVFS)));
#endif

#ifdef TEST_RADOS
static cta::OStoreDBFactory<cta::objectstore::BackendRados> OStoreDBFactoryRados("rados://tapetest@tapetest");

INSTANTIATE_TEST_CASE_P(OStoreDBPlusMockSchedulerTestRados, DataTransferSessionTest,
                        ::testing::Values(DataTransferSessionTestParam(OStoreDBFactoryRados)));
#endif

}  // namespace unitTests