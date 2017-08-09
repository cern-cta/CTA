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

#include "castor/tape/tapeserver/daemon/DiskWriteThreadPool.hpp"
#include "castor/tape/tapeserver/daemon/RecallTaskInjector.hpp"
#include "castor/tape/tapeserver/daemon/RecallReportPacker.hpp"
#include "castor/tape/tapeserver/daemon/ReportPackerInterface.hpp"
#include "common/log/LogContext.hpp"
#include "common/log/StringLogger.hpp"
#include "castor/tape/tapeserver/daemon/MigrationMemoryManager.hpp"
#include "castor/tape/tapeserver/daemon/MemBlock.hpp"
#include "castor/messages/TapeserverProxyDummy.hpp"
#include "scheduler/TapeMountDummy.hpp"

#include "scheduler/Scheduler.hpp"
#include "scheduler/testingMocks/MockRetrieveMount.hpp"

#include <memory>
#include <gtest/gtest.h>

namespace unitTests{
  class TestingDatabaseRetrieveMount: public cta::SchedulerDatabase::RetrieveMount {
    const MountInfo & getMountInfo() override { throw std::runtime_error("Not implemented"); }
    std::list<std::unique_ptr<cta::SchedulerDatabase::RetrieveJob> > getNextJobBatch(uint64_t filesRequested, uint64_t bytesRequested, cta::log::LogContext& logContext) override { throw std::runtime_error("Not implemented");}
    void complete(time_t completionTime) override { throw std::runtime_error("Not implemented"); }
    void setDriveStatus(cta::common::dataStructures::DriveStatus status, time_t completionTime) override { throw std::runtime_error("Not implemented"); }
    void setTapeSessionStats(const castor::tape::tapeserver::daemon::TapeSessionStats &stats) override { throw std::runtime_error("Not implemented"); }
  };
  
  class TestingRetrieveMount: public cta::RetrieveMount {
  public:
    TestingRetrieveMount(std::unique_ptr<cta::SchedulerDatabase::RetrieveMount> dbrm): RetrieveMount(std::move(dbrm)) {
    }
  };
  
  class TestingRetrieveJob: public cta::RetrieveJob {
  public:
    TestingRetrieveJob(cta::RetrieveMount & rm): cta::RetrieveJob(rm,
    cta::common::dataStructures::RetrieveRequest(), 
    cta::common::dataStructures::ArchiveFile(), 1,
    cta::PositioningMethod::ByBlock) {}
  };
  
  using namespace castor::tape::tapeserver::daemon;
  using namespace castor::tape::tapeserver::client;
  using namespace castor::tape::diskFile;
  struct MockRecallReportPacker : public RecallReportPacker {
    void reportCompletedJob(std::unique_ptr<cta::RetrieveJob> successfulRetrieveJob) {
      cta::threading::MutexLocker ml(m_mutex);
      completeJobs++;
    }
    void reportFailedJob(std::unique_ptr<cta::RetrieveJob> failedRetrieveJob) {
      cta::threading::MutexLocker ml(m_mutex);
      failedJobs++;
    }
    void disableBulk() {}
    void reportEndOfSession() {
      cta::threading::MutexLocker ml(m_mutex);
      endSessions++;
    }
    void reportEndOfSessionWithErrors(const std::string msg, int error_code) {
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
  
  TEST(castor_tape_tapeserver_daemon, DiskWriteTaskFailedBlock){
    using ::testing::_;
    
    cta::log::StringLogger log("castor_tape_tapeserver_daemon_DiskWriteTaskFailedBlock",cta::log::DEBUG);
    cta::log::LogContext lc(log);
    
    std::unique_ptr<cta::SchedulerDatabase::RetrieveMount> dbrm(new TestingDatabaseRetrieveMount());
    TestingRetrieveMount trm(std::move(dbrm));
    MockRecallReportPacker report(&trm,lc);
    RecallMemoryManager mm(10,100,lc);
    castor::tape::file::RadosStriperPool striperPool;
    DiskFileFactory fileFactory("", 0, striperPool);
    
    cta::MockRetrieveMount mrm;
    std::unique_ptr<TestingRetrieveJob> fileToRecall(new TestingRetrieveJob(mrm));
    fileToRecall->retrieveRequest.archiveFileID = 1;
    fileToRecall->selectedCopyNb=1;
    fileToRecall->archiveFile.tapeFiles[1];
    DiskWriteTask t(fileToRecall.release(),mm);
    for(int i=0;i<6;++i){
      MemBlock* mb=mm.getFreeBlock();
      mb->m_fileid=0;
      mb->m_fileBlock=i;
      if(5==i){
        mb->markAsFailed("Test error",666);
      }
      t.pushDataBlock(mb);
    }
    MemBlock* mb=mm.getFreeBlock();

    t.pushDataBlock(mb);
    t.pushDataBlock(NULL);
    castor::messages::TapeserverProxyDummy tspd;
    cta::TapeMountDummy tmd;
    RecallWatchDog rwd(1,1,tspd,tmd,"", lc);
    t.execute(report,lc,fileFactory,rwd, 0);
    ASSERT_EQ(1, report.failedJobs);
  }
}

