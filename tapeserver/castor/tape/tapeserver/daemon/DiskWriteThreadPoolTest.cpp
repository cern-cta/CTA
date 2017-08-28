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
    TestingRetrieveJob(): cta::RetrieveJob(*((cta::RetrieveMount *)NULL),
    cta::common::dataStructures::RetrieveRequest(), 
    cta::common::dataStructures::ArchiveFile(), 1,
    cta::PositioningMethod::ByBlock) {}
  };
  

  
  using namespace castor::tape::tapeserver::daemon;
  using namespace castor::tape::tapeserver::client;
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
    MockRecallReportPacker(cta::RetrieveMount *rm, cta::log::LogContext lc):
      RecallReportPacker(rm,lc), completeJobs(0), failedJobs(0),
      endSessions(0), endSessionsWithError(0) {}
    cta::threading::Mutex m_mutex;
    int completeJobs;
    int failedJobs;
    int endSessions;
    int endSessionsWithError;
  };
  
  struct MockTaskInjector : public RecallTaskInjector{
    MOCK_METHOD3(requestInjection, void(int maxFiles, int maxBlocks, bool lastCall));
  };
  
  TEST(castor_tape_tapeserver_daemon, DiskWriteThreadPoolTest){
    using ::testing::_;
    
    cta::log::StringLogger log("castor_tape_tapeserver_daemon_DiskWriteThreadPoolTest",cta::log::DEBUG);
    cta::log::LogContext lc(log);
    
    std::unique_ptr<cta::SchedulerDatabase::RetrieveMount> dbrm(new TestingDatabaseRetrieveMount);
    TestingRetrieveMount trm(std::move(dbrm));
    MockRecallReportPacker report(&trm,lc);    
    
    RecallMemoryManager mm(10,100,lc);
    
    castor::messages::TapeserverProxyDummy tspd;
    cta::TapeMountDummy tmd;
    RecallWatchDog rwd(1,1,tspd,tmd,"", lc);
    
    DiskWriteThreadPool dwtp(2,report,rwd,lc,"/dev/null", 0);
    dwtp.startThreads();
       
    for(int i=0;i<5;++i){
      std::unique_ptr<TestingRetrieveJob> fileToRecall(new TestingRetrieveJob());
      fileToRecall->retrieveRequest.archiveFileID = i+1;
      fileToRecall->retrieveRequest.dstURL = "/dev/null";
      fileToRecall->selectedCopyNb=1;
      fileToRecall->archiveFile.tapeFiles[1];
      fileToRecall->selectedTapeFile().blockId = 1;
      DiskWriteTask* t=new DiskWriteTask(fileToRecall.release(),mm);
      MemBlock* mb=mm.getFreeBlock();
      mb->m_fileid=i+1;
      mb->m_fileBlock=0;
      t->pushDataBlock(mb);
      t->pushDataBlock(NULL);
      dwtp.push(t);
    }
     
    dwtp.finish();
    dwtp.waitThreads();
    ASSERT_EQ(5, report.completeJobs);
    ASSERT_EQ(1, report.endSessions);
  }

}

