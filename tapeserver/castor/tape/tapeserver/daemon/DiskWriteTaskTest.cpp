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

#include <memory>

#include "tapeserver/castor/tape/tapeserver/daemon/DiskWriteThreadPool.hpp"
#include "tapeserver/castor/tape/tapeserver/daemon/RecallTaskInjector.hpp"
#include "tapeserver/castor/tape/tapeserver/daemon/RecallReportPacker.hpp"
#include "tapeserver/castor/tape/tapeserver/daemon/ReportPackerInterface.hpp"
#include "common/log/LogContext.hpp"
#include "catalogue/DummyCatalogue.hpp"
#include "common/log/StringLogger.hpp"
#include "tapeserver/castor/tape/tapeserver/daemon/MigrationMemoryManager.hpp"
#include "tapeserver/castor/tape/tapeserver/daemon/MemBlock.hpp"
#include "tapeserver/castor/messages/TapeserverProxyDummy.hpp"
#include "scheduler/TapeMountDummy.hpp"
#include "scheduler/SchedulerDatabase.hpp"
#include "scheduler/Scheduler.hpp"
#include "scheduler/testingMocks/MockRetrieveMount.hpp"

namespace unitTests{
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
    void flushAsyncSuccessReports(std::list<cta::SchedulerDatabase::RetrieveJob*>& jobsBatch, cta::log::LogContext& lc) override { throw std::runtime_error("Not implemented"); }
    void addDiskSystemToSkip(const cta::SchedulerDatabase::RetrieveMount::DiskSystemToSkip &diskSystemToSkip) override { throw std::runtime_error("Not implemented"); }
    void requeueJobBatch(std::list<std::unique_ptr<cta::SchedulerDatabase::RetrieveJob>>& jobBatch, cta::log::LogContext& logContext) override { throw std::runtime_error("Not implemented"); }
    void putQueueToSleep(const std::string &diskSystemName, const uint64_t sleepTime, cta::log::LogContext &logContext) override { throw std::runtime_error("Not implemented"); }
    bool reserveDiskSpace(const cta::DiskSpaceReservationRequest &request, const std::string &externalFreeDiskSpaceScript, cta::log::LogContext& logContext) override{ throw std::runtime_error("Not implemented"); }
    bool testReserveDiskSpace(const cta::DiskSpaceReservationRequest &request, const std::string &externalFreeDiskSpaceScript, cta::log::LogContext& logContext) override{ throw std::runtime_error("Not implemented"); }
  };
  
  class TestingRetrieveMount: public cta::RetrieveMount {
  public:
    TestingRetrieveMount(cta::catalogue::Catalogue &catalogue, std::unique_ptr<cta::SchedulerDatabase::RetrieveMount> dbrm): RetrieveMount(catalogue, std::move(dbrm)) {
    }
  };
  
  class TestingRetrieveJob: public cta::RetrieveJob {
  public:
    TestingRetrieveJob(cta::RetrieveMount & rm): cta::RetrieveJob(&rm,
    cta::common::dataStructures::RetrieveRequest(), 
    cta::common::dataStructures::ArchiveFile(), 1,
    cta::PositioningMethod::ByBlock) {}
  };
  
  using namespace castor::tape::tapeserver::daemon;
  using namespace castor::tape::tapeserver::client;
  using namespace cta::disk;
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
    
    MockRecallReportPacker(cta::RetrieveMount *rm,cta::log::LogContext lc):
     RecallReportPacker(rm,lc), completeJobs(0), failedJobs(0),
      endSessions(0), endSessionsWithError(0) {}
    cta::threading::Mutex m_mutex;
    int completeJobs;
    int failedJobs;
    int endSessions;
    int endSessionsWithError;
    bool m_tapeThreadComplete;
    bool m_diskThreadComplete;
  };

  TEST(castor_tape_tapeserver_daemon, DiskWriteTaskFailedBlock){
    using ::testing::_;
    
    cta::log::StringLogger log("dummy","castor_tape_tapeserver_daemon_DiskWriteTaskFailedBlock",cta::log::DEBUG);
    cta::log::LogContext lc(log);
    
    std::unique_ptr<cta::SchedulerDatabase::RetrieveMount> dbrm(new TestingDatabaseRetrieveMount());
    std::unique_ptr<cta::catalogue::Catalogue> catalogue(new cta::catalogue::DummyCatalogue);
    TestingRetrieveMount trm(*catalogue, std::move(dbrm));
    MockRecallReportPacker report(&trm,lc);
    RecallMemoryManager mm(10,100,lc);
    cta::disk::RadosStriperPool striperPool;
    DiskFileFactory fileFactory("", 0, striperPool);
    
    cta::MockRetrieveMount mrm(*catalogue);
    std::unique_ptr<TestingRetrieveJob> fileToRecall(new TestingRetrieveJob(mrm));
    fileToRecall->retrieveRequest.archiveFileID = 1;
    fileToRecall->selectedCopyNb=1;
    cta::common::dataStructures::TapeFile tf;
    tf.copyNb = 1;
    fileToRecall->archiveFile.tapeFiles.push_back(tf);
    DiskWriteTask t(fileToRecall.release(),mm);
    for(int i=0;i<6;++i){
      MemBlock* mb=mm.getFreeBlock();
      mb->m_fileid=0;
      mb->m_fileBlock=i;
      if(5==i){
        mb->markAsFailed("Test error");
      }
      t.pushDataBlock(mb);
    }
    MemBlock* mb=mm.getFreeBlock();

    t.pushDataBlock(mb);
    t.pushDataBlock(nullptr);
    castor::messages::TapeserverProxyDummy tspd;
    cta::TapeMountDummy tmd;
    RecallWatchDog rwd(1,1,tspd,tmd,"", lc);
    t.execute(report,lc,fileFactory,rwd, 0);
    ASSERT_EQ(1, report.failedJobs);
  }
}

