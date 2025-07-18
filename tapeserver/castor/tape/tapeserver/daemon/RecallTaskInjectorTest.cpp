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

#include "castor/tape/tapeserver/daemon/TapeserverProxyMock.hpp"
#include "castor/tape/tapeserver/daemon/DiskWriteThreadPool.hpp"
#include "castor/tape/tapeserver/daemon/RecallTaskInjector.hpp"
#include "castor/tape/tapeserver/daemon/TapeReadSingleThread.hpp"
#include "castor/tape/tapeserver/daemon/TapeSessionReporter.hpp"
#include "castor/tape/tapeserver/daemon/TaskWatchDog.hpp"
#include "castor/tape/tapeserver/drive/FakeDrive.hpp"
#include "catalogue/dummy/DummyCatalogue.hpp"
#include "common/log/DummyLogger.hpp"
#include "common/log/StringLogger.hpp"
#include "mediachanger/MediaChangerFacade.hpp"
#include "scheduler/SchedulerDatabase.hpp"
#include "scheduler/TapeMountDummy.hpp"
#include "scheduler/testingMocks/MockRetrieveMount.hpp"

using namespace castor::tape::tapeserver::daemon;
using namespace castor::tape;


namespace unitTests
{
  const int blockSize=4096;
  class castor_tape_tapeserver_daemonTest: public ::testing::Test {
  protected:

    void SetUp() {
    }

    void TearDown() {
    }

  }; // class castor_tape_tapeserver_daemonTest

  struct MockRecallReportPacker : public RecallReportPacker {
    void reportCompletedJob(std::unique_ptr<cta::RetrieveJob> successfulRetrieveJob, cta::log::LogContext& lc) override {
      cta::threading::MutexLocker ml(m_mutex);
      completeJobs++;
    }
    void reportFailedJob(std::unique_ptr<cta::RetrieveJob> failedRetrieveJob, const cta::exception::Exception & ex, cta::log::LogContext& lc) override {
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
    MockRecallReportPacker(cta::RetrieveMount *rm,cta::log::LogContext lc):
     RecallReportPacker(rm,lc), completeJobs(0), failedJobs(0),
      endSessions(0), endSessionsWithError(0) {}
    cta::threading::Mutex m_mutex;
    int completeJobs;
    int failedJobs;
    int endSessions;
    int endSessionsWithError;
  };

  class FakeDiskWriteThreadPool: public DiskWriteThreadPool
  {
  public:
    using DiskWriteThreadPool::m_tasks;
    FakeDiskWriteThreadPool(RecallReportPacker &rrp, RecallWatchDog &rwd,
      cta::log::LogContext & lc):
      DiskWriteThreadPool(1, rrp, rwd, lc, 0) {}
    virtual ~FakeDiskWriteThreadPool() {};
  };

  class FakeSingleTapeReadThread : public TapeSingleThreadInterface<TapeReadTask>
  {
  public:
    using TapeSingleThreadInterface<TapeReadTask>::m_tasks;

    FakeSingleTapeReadThread(tapeserver::drive::DriveInterface& drive,
      cta::mediachanger::MediaChangerFacade & mc,
      tapeserver::daemon::TapeSessionReporter & tsr,
      const tapeserver::daemon::VolumeInfo& volInfo,
      const uint32_t tapeLoadTimeout,
      cta::log::LogContext & lc):
    TapeSingleThreadInterface<TapeReadTask>(drive, mc, tsr, volInfo, lc, false, "", tapeLoadTimeout){}

    ~FakeSingleTapeReadThread(){
      const unsigned int size= m_tasks.size();
      for(unsigned int i=0;i<size;++i){
        delete m_tasks.pop();
      }
    }

    virtual void run ()
    {
      m_tasks.push(nullptr);
    }

    virtual void push(TapeReadTask* t){
      m_tasks.push(t);
    }

    virtual void countTapeLogError(const std::string & error) {};
    protected:
      virtual void logSCSIMetrics() {};
  };

  class TestingDatabaseRetrieveMount: public cta::SchedulerDatabase::RetrieveMount {
    const MountInfo & getMountInfo() override { throw std::runtime_error("Not implemented"); }
    std::list<std::unique_ptr<cta::SchedulerDatabase::RetrieveJob> > getNextJobBatch(uint64_t filesRequested, uint64_t bytesRequested,
      cta::log::LogContext& logContext) override { throw std::runtime_error("Not implemented");}

    void
    setDriveStatus(cta::common::dataStructures::DriveStatus status, cta::common::dataStructures::MountType mountType,
                   time_t completionTime, const std::optional<std::string>& reason) override {
      throw std::runtime_error("Not implemented");
    }
    void setTapeSessionStats(const castor::tape::tapeserver::daemon::TapeSessionStats &stats) override { throw std::runtime_error("Not implemented"); }

    void setJobBatchTransferred(std::list<std::unique_ptr<cta::SchedulerDatabase::RetrieveJob>>& jobsBatch,
                                cta::log::LogContext& lc) override {
      throw std::runtime_error("Not implemented");
    }

    void flushAsyncSuccessReports(std::list<cta::SchedulerDatabase::RetrieveJob*>& jobsBatch,
                                  cta::log::LogContext& lc) override {
      throw std::runtime_error("Not implemented");
    }

    void addDiskSystemToSkip(const cta::SchedulerDatabase::RetrieveMount::DiskSystemToSkip &diskSystemToSkip) override { throw std::runtime_error("Not implemented"); }
    void requeueJobBatch(std::list<std::unique_ptr<cta::SchedulerDatabase::RetrieveJob>>& jobBatch, cta::log::LogContext& logContext) override { throw std::runtime_error("Not implemented"); }
    uint64_t requeueJobBatch(const std::list<std::string>& jobIDsList, cta::log::LogContext& logContext) const override { throw std::runtime_error("Not implemented"); }
    void putQueueToSleep(const std::string &diskSystemName, const uint64_t sleepTime, cta::log::LogContext &logContext) override { throw std::runtime_error("Not implemented"); }
  };

  TEST_F(castor_tape_tapeserver_daemonTest, RecallTaskInjectorNominal) {
    const int nbJobs=15;
    const int maxNbJobsInjectedAtOnce = 6;
    cta::log::StringLogger log("dummy","castor_tape_tapeserver_daemon_RecallTaskInjectorTest",cta::log::DEBUG);
    cta::log::LogContext lc(log);
    RecallMemoryManager mm(50U, 50U, lc);
    castor::tape::tapeserver::drive::FakeDrive drive;

    auto catalogue = cta::catalogue::DummyCatalogue();
    cta::MockRetrieveMount trm(catalogue);
    trm.createRetrieveJobs(nbJobs);
    //EXPECT_CALL(trm, internalGetNextJob()).Times(nbJobs+1);

    ::testing::NiceMock<cta::tape::daemon::TapeserverProxyMock> tspd;
    cta::TapeMountDummy tmd;
    RecallWatchDog rwd(1,1,tspd,tmd,"",lc);
    std::unique_ptr<cta::SchedulerDatabase::RetrieveMount> dbrm(new TestingDatabaseRetrieveMount());
    MockRecallReportPacker mrrp(&trm,lc);
    FakeDiskWriteThreadPool diskWrite(mrrp,rwd,lc);
    cta::log::DummyLogger dummyLog("dummy","dummy");
    cta::mediachanger::RmcProxy rmcProxy;
    cta::mediachanger::MediaChangerFacade mc(rmcProxy, dummyLog);
    ::testing::NiceMock<cta::tape::daemon::TapeserverProxyMock> initialProcess;
    castor::tape::tapeserver::daemon::VolumeInfo volume;
    volume.vid="V12345";
    volume.mountType=cta::common::dataStructures::MountType::Retrieve;
    castor::tape::tapeserver::daemon::TapeSessionReporter gsr(initialProcess, cta::tape::daemon::DriveConfigEntry(),
                                                              "0.0.0.0", lc);
    FakeSingleTapeReadThread tapeRead(drive, mc, gsr, volume, 60, lc);
    tapeserver::daemon::RecallTaskInjector rti(mm, tapeRead, diskWrite, trm, maxNbJobsInjectedAtOnce, blockSize, rwd, lc);

    bool noFilesToRecall;
    ASSERT_EQ(true, rti.synchronousFetch(noFilesToRecall));
    ASSERT_FALSE(noFilesToRecall);

    //Jobs are no longer injected at synchronousFetch, only when the threads start
    //ASSERT_EQ(maxNbJobsInjectedAtOnce, diskWrite.m_tasks.size());
    //ASSERT_EQ(maxNbJobsInjectedAtOnce, tapeRead.m_tasks.size());

    rti.startThreads();
    rti.requestInjection(false);
    rti.requestInjection(true);
    rti.finish();
    ASSERT_NO_THROW(rti.waitThreads());
    ASSERT_EQ(nbJobs+1, trm.getJobs);

    //pushed nbFile*2 files + 1 end of work
    ASSERT_EQ(nbJobs+1, diskWrite.m_tasks.size());
    ASSERT_EQ(nbJobs+1, tapeRead.m_tasks.size());

    for(int i=0; i<nbJobs; ++i)
    {
      delete diskWrite.m_tasks.pop();
      delete tapeRead.m_tasks.pop();
    }

    for(int i=0; i<1; ++i)
    {
      DiskWriteTask* diskWriteTask=diskWrite.m_tasks.pop();
      TapeReadTask* tapeReadTask=tapeRead.m_tasks.pop();

      //static_cast is needed otherwise compilation fails on SL5 with a raw nullptr
      ASSERT_EQ(static_cast<DiskWriteTask*>(nullptr), diskWriteTask);
      ASSERT_EQ(static_cast<TapeReadTask*>(nullptr), tapeReadTask);
      delete diskWriteTask;
      delete tapeReadTask;
    }
  }

  TEST_F(castor_tape_tapeserver_daemonTest, RecallTaskInjectorNoFiles) {
    cta::log::StringLogger log("dummy","castor_tape_tapeserver_daemon_RecallTaskInjectorTest",cta::log::DEBUG);
    cta::log::LogContext lc(log);
    RecallMemoryManager mm(50U, 50U, lc);
    castor::tape::tapeserver::drive::FakeDrive drive;
    auto catalogue = cta::catalogue::DummyCatalogue();
    cta::MockRetrieveMount trm(catalogue);
    trm.createRetrieveJobs(0);
    //EXPECT_CALL(trm, internalGetNextJob()).Times(1); //no work: single call to getnextjob

    ::testing::NiceMock<cta::tape::daemon::TapeserverProxyMock> tspd;
    cta::TapeMountDummy tmd;
    RecallWatchDog rwd(1,1,tspd,tmd,"",lc);
    std::unique_ptr<cta::SchedulerDatabase::RetrieveMount> dbrm(new TestingDatabaseRetrieveMount());
    MockRecallReportPacker mrrp(&trm,lc);
    FakeDiskWriteThreadPool diskWrite(mrrp,rwd,lc);
    cta::log::DummyLogger dummyLog("dummy","dummy");
    cta::mediachanger::RmcProxy rmcProxy;
    cta::mediachanger::MediaChangerFacade mc(rmcProxy, dummyLog);
    ::testing::NiceMock<cta::tape::daemon::TapeserverProxyMock> initialProcess;
    castor::tape::tapeserver::daemon::VolumeInfo volume;
    volume.vid="V12345";
    volume.mountType=cta::common::dataStructures::MountType::Retrieve;
    castor::tape::tapeserver::daemon::TapeSessionReporter tsr(initialProcess, cta::tape::daemon::DriveConfigEntry(),
                                                              "0.0.0.0", lc);
    FakeSingleTapeReadThread tapeRead(drive, mc, tsr, volume, 60, lc);
    tapeserver::daemon::RecallTaskInjector rti(mm, tapeRead, diskWrite, trm, 6, blockSize, rwd, lc);

    bool noFilesToRecall;
    ASSERT_FALSE(rti.synchronousFetch(noFilesToRecall));
    ASSERT_EQ(0U, diskWrite.m_tasks.size());
    ASSERT_EQ(0U, tapeRead.m_tasks.size());
    ASSERT_EQ(1, trm.getJobs);
    ASSERT_TRUE(noFilesToRecall);
  }
}
