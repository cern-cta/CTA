/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#include "castor/tape/tapeserver/daemon/DiskWriteThreadPool.hpp"

#include "castor/tape/tapeserver/daemon/MemBlock.hpp"
#include "castor/tape/tapeserver/daemon/MigrationMemoryManager.hpp"
#include "castor/tape/tapeserver/daemon/RecallReportPacker.hpp"
#include "castor/tape/tapeserver/daemon/RecallTaskInjector.hpp"
#include "castor/tape/tapeserver/daemon/ReportPackerInterface.hpp"
#include "castor/tape/tapeserver/daemon/TapeserverProxyMock.hpp"
#include "catalogue/dummy/DummyCatalogue.hpp"
#include "common/exception/NotImplementedException.hpp"
#include "common/log/LogContext.hpp"
#include "common/log/StringLogger.hpp"
#include "scheduler/TapeMountDummy.hpp"

#include <gtest/gtest.h>

namespace unitTests {

class TestingDatabaseRetrieveMount : public cta::SchedulerDatabase::RetrieveMount {
  const MountInfo& getMountInfo() override { throw cta::exception::NotImplementedException(); }

  std::list<std::unique_ptr<cta::SchedulerDatabase::RetrieveJob>>
  getNextJobBatch(uint64_t filesRequested, uint64_t bytesRequested, cta::log::LogContext& logContext) override {
    throw cta::exception::NotImplementedException();
  }

  void setDriveStatus(cta::common::dataStructures::DriveStatus status,
                      cta::common::dataStructures::MountType mountType,
                      time_t completionTime,
                      const std::optional<std::string>& reason) override {
    throw cta::exception::NotImplementedException();
  }

  void setTapeSessionStats(const castor::tape::tapeserver::daemon::TapeSessionStats& stats) override {
    throw cta::exception::NotImplementedException();
  }

  void setJobBatchTransferred(std::list<std::unique_ptr<cta::SchedulerDatabase::RetrieveJob>>& jobsBatch,
                              cta::log::LogContext& lc) override {
    throw cta::exception::NotImplementedException();
  }

  void flushAsyncSuccessReports(std::list<cta::SchedulerDatabase::RetrieveJob*>& jobsBatch,
                                cta::log::LogContext& lc) override {
    throw cta::exception::NotImplementedException();
  }

  void addDiskSystemToSkip(const cta::SchedulerDatabase::RetrieveMount::DiskSystemToSkip& diskSystemToSkip) override {
    throw cta::exception::NotImplementedException();
  }

  void requeueJobBatch(std::list<std::unique_ptr<cta::SchedulerDatabase::RetrieveJob>>& jobBatch,
                       cta::log::LogContext& logContext) override {
    throw cta::exception::NotImplementedException();
  }

  uint64_t requeueJobBatch(const std::list<std::string>& jobIDsList, cta::log::LogContext& logContext) const override {
    throw cta::exception::NotImplementedException();
  }

  void putQueueToSleep(const std::string& diskSystemName,
                       const uint64_t sleepTime,
                       cta::log::LogContext& logContext) override {
    throw cta::exception::NotImplementedException();
  }
};

class TestingRetrieveMount : public cta::RetrieveMount {
public:
  TestingRetrieveMount(cta::catalogue::Catalogue& catalogue,
                       std::unique_ptr<cta::SchedulerDatabase::RetrieveMount> dbrm)
      : RetrieveMount(catalogue, std::move(dbrm)) {}
};

class TestingRetrieveJob : public cta::RetrieveJob {
public:
  TestingRetrieveJob()
      : cta::RetrieveJob(nullptr,
                         cta::common::dataStructures::RetrieveRequest(),
                         cta::common::dataStructures::ArchiveFile(),
                         1,
                         cta::PositioningMethod::ByBlock) {}
};

using namespace castor::tape::tapeserver::daemon;
using namespace castor::tape::tapeserver::client;

struct MockRecallReportPacker : public RecallReportPacker {
  void reportCompletedJob(std::unique_ptr<cta::RetrieveJob> successfulRetrieveJob, cta::log::LogContext& lc) override {
    cta::threading::MutexLocker ml(m_mutex);
    completeJobs++;
  }

  void reportFailedJob(std::unique_ptr<cta::RetrieveJob> failedRetrieveJob,
                       const cta::exception::Exception& ex,
                       cta::log::LogContext& lc) override {
    cta::threading::MutexLocker ml(m_mutex);
    failedJobs++;
  }

  void disableBulk() override {}

  void reportEndOfSession(cta::log::LogContext& lc) override {
    cta::threading::MutexLocker ml(m_mutex);
    endSessions++;
  }

  void reportEndOfSessionWithErrors(const std::string& msg, cta::log::LogContext& lc) override {
    cta::threading::MutexLocker ml(m_mutex);
    endSessionsWithError++;
  }

  void setDiskDone() override {
    cta::threading::MutexLocker mutexLocker(m_mutex);
    m_diskThreadComplete = true;
  }

  void setTapeDone() override {
    cta::threading::MutexLocker mutexLocker(m_mutex);
    m_tapeThreadComplete = true;
  }

  bool allThreadsDone() override {
    cta::threading::MutexLocker mutexLocker(m_mutex);
    return m_tapeThreadComplete && m_diskThreadComplete;
  }

  MockRecallReportPacker(cta::RetrieveMount* rm, cta::log::LogContext lc)
      : RecallReportPacker(rm, lc),
        completeJobs(0),
        failedJobs(0),
        endSessions(0),
        endSessionsWithError(0) {}

  cta::threading::Mutex m_mutex;
  int completeJobs;
  int failedJobs;
  int endSessions;
  int endSessionsWithError;
  bool m_tapeThreadComplete;
  bool m_diskThreadComplete;
};

struct MockTaskInjector : public RecallTaskInjector {
  MOCK_METHOD3(requestInjection, void(int maxFiles, int maxBlocks, bool lastCall));
};

TEST(castor_tape_tapeserver_daemon, DiskWriteThreadPoolTest) {
  using ::testing::_;

  cta::log::StringLogger log("dummy", "castor_tape_tapeserver_daemon_DiskWriteThreadPoolTest", cta::log::DEBUG);
  cta::log::LogContext lc(log);

  std::unique_ptr<cta::SchedulerDatabase::RetrieveMount> dbrm(new TestingDatabaseRetrieveMount);
  std::unique_ptr<cta::catalogue::Catalogue> catalogue(new cta::catalogue::DummyCatalogue);
  TestingRetrieveMount trm(*catalogue, std::move(dbrm));
  MockRecallReportPacker report(&trm, lc);

  RecallMemoryManager mm(10, 100, lc);

  ::testing::NiceMock<cta::tape::daemon::TapeserverProxyMock> tspd;
  cta::TapeMountDummy tmd;
  RecallWatchDog rwd(1, 1, tspd, tmd, "", lc);

  DiskWriteThreadPool dwtp(2, report, rwd, lc, 0);
  dwtp.startThreads();
  report.setTapeDone();

  for (int i = 0; i < 5; ++i) {
    std::unique_ptr<TestingRetrieveJob> fileToRecall(new TestingRetrieveJob());
    fileToRecall->retrieveRequest.archiveFileID = i + 1;
    fileToRecall->retrieveRequest.dstURL = "/dev/null";
    fileToRecall->selectedCopyNb = 1;
    cta::common::dataStructures::TapeFile tf;
    tf.copyNb = 1;
    fileToRecall->archiveFile.tapeFiles.push_back(tf);
    fileToRecall->selectedTapeFile().blockId = 1;
    auto t = std::make_unique<DiskWriteTask>(std::move(fileToRecall), mm);
    auto mb = mm.getFreeBlock();
    mb->m_fileid = i + 1;
    mb->m_fileBlock = 0;
    t->pushDataBlock(std::move(mb));
    t->pushDataBlock(nullptr);
    dwtp.push(std::move(t));
  }

  dwtp.finish();
  dwtp.waitThreads();
  ASSERT_EQ(5, report.completeJobs);
  ASSERT_EQ(1, report.endSessions);
}

}  // namespace unitTests
