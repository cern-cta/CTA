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

#include "castor/legacymsg/RmcProxyDummy.hpp"
#include "castor/log/StringLogger.hpp"
#include "castor/mediachanger/MediaChangerFacade.hpp"
#include "castor/mediachanger/MmcProxyDummy.hpp"
#include "castor/messages/AcsProxyDummy.hpp"
#include "castor/messages/TapeserverProxyDummy.hpp"
#include "castor/server/ProcessCapDummy.hpp"
#include "castor/tape/tapeserver/daemon/DiskWriteThreadPool.hpp"
#include "castor/tape/tapeserver/daemon/RecallTaskInjector.hpp"
#include "castor/tape/tapeserver/daemon/TapeServerReporter.hpp"
#include "castor/tape/tapeserver/daemon/TapeReadSingleThread.hpp"
#include "castor/tape/tapeserver/daemon/TpconfigLine.hpp"
#include "castor/tape/tapeserver/drive/FakeDrive.hpp"
#include "castor/utils/utils.hpp"

#include <gtest/gtest.h>

using namespace castor::tape::tapeserver::daemon;
using namespace castor::tape;
const int blockSize=4096;

namespace unitTests
{
  
  class castor_tape_tapeserver_daemonTest: public ::testing::Test {
  protected:

    void SetUp() {
    }

    void TearDown() {
    }

    class MockRetrieveJob: public cta::RetrieveJob {
    public:
      MockRetrieveJob() {
      }

      ~MockRetrieveJob() throw() {
      }


      MOCK_METHOD2(complete, void(const uint32_t checksumOfTransfer, const uint64_t fileSizeOfTransfer));
      MOCK_METHOD1(failed, void(const cta::exception::Exception &ex));
    }; // class MockRetrieveJob

    class MockRetrieveMount: public cta::RetrieveMount {
    public:
      MockRetrieveMount(int nbRecallJobs) {
        createRetrieveJobs(nbRecallJobs);
      }

      ~MockRetrieveMount() throw() {
      }

      std::unique_ptr<cta::RetrieveJob> getNextJob() {
        internalGetNextJob();
        if(m_jobs.empty()) {
          return std::unique_ptr<cta::RetrieveJob>();
        } else {
          std::unique_ptr<cta::RetrieveJob> job =  std::move(m_jobs.front());
          m_jobs.pop_front();
          return job;
        }
      }

      MOCK_METHOD0(internalGetNextJob, cta::RetrieveJob*());
      MOCK_METHOD0(complete, void());

    private:

      std::list<std::unique_ptr<cta::RetrieveJob>> m_jobs;

      void createRetrieveJobs(const unsigned int nbJobs) {
        for(unsigned int i = 0; i < nbJobs; i++) {
          m_jobs.push_back(std::unique_ptr<cta::RetrieveJob>(
            new MockRetrieveJob()));
        }
      }
    }; // class MockRetrieveMount

  }; // class castor_tape_tapeserver_daemonTest

  class FakeDiskWriteThreadPool: public DiskWriteThreadPool
  {
  public:
    using DiskWriteThreadPool::m_tasks;
    FakeDiskWriteThreadPool(castor::log::LogContext & lc):
      DiskWriteThreadPool(1,*((RecallReportPacker*)NULL),
      *((RecallWatchDog*)NULL),lc, "RFIO","/dev/null",0){}
    virtual ~FakeDiskWriteThreadPool() {};
  };

  class FakeSingleTapeReadThread : public TapeSingleThreadInterface<TapeReadTask>
  {
  public:
    using TapeSingleThreadInterface<TapeReadTask>::m_tasks;

    FakeSingleTapeReadThread(tapeserver::drive::DriveInterface& drive,
      castor::mediachanger::MediaChangerFacade & mc,
      tapeserver::daemon::TapeServerReporter & tsr,
      const tapeserver::daemon::VolumeInfo& volInfo, 
      castor::server::ProcessCap& cap,
      castor::log::LogContext & lc):
    TapeSingleThreadInterface<TapeReadTask>(drive, mc, tsr, volInfo,cap, lc){}

    ~FakeSingleTapeReadThread(){
      const unsigned int size= m_tasks.size();
      for(unsigned int i=0;i<size;++i){
        delete m_tasks.pop();
      }
    }
    
    virtual void run () 
    {
      m_tasks.push(NULL);
    }
    
    virtual void push(TapeReadTask* t){
      m_tasks.push(t);
    }

    virtual void countTapeLogError(const std::string & error) {};
  };

  TEST_F(castor_tape_tapeserver_daemonTest, RecallTaskInjectorNominal) {
    const int nbJobs=15;
    const int maxNbJobsInjectedAtOnce = 6;
    castor::log::StringLogger log("castor_tape_tapeserver_daemon_RecallTaskInjectorTest");
    castor::log::LogContext lc(log);
    RecallMemoryManager mm(50U, 50U, lc);
    castor::tape::tapeserver::drive::FakeDrive drive;
    
    MockRetrieveMount trm(nbJobs);
    EXPECT_CALL(trm, internalGetNextJob()).Times(nbJobs+1);
    
    FakeDiskWriteThreadPool diskWrite(lc);
    castor::messages::AcsProxyDummy acs;
    castor::mediachanger::MmcProxyDummy mmc;
    castor::legacymsg::RmcProxyDummy rmc;
    castor::mediachanger::MediaChangerFacade mc(acs, mmc, rmc);
    castor::messages::TapeserverProxyDummy initialProcess;
    castor::tape::tapeserver::daemon::VolumeInfo volume;
    volume.density="8000GC";
    volume.vid="V12345";
    volume.mountType=cta::MountType::RETRIEVE;
    castor::tape::tapeserver::daemon::TapeServerReporter gsr(initialProcess, DriveConfig(), "0.0.0.0", volume, lc);
    castor::server::ProcessCapDummy cap;
    FakeSingleTapeReadThread tapeRead(drive, mc, gsr, volume, cap, lc);
    tapeserver::daemon::RecallTaskInjector rti(mm, tapeRead, diskWrite, trm, maxNbJobsInjectedAtOnce, blockSize, lc);

    ASSERT_EQ(true, rti.synchronousInjection());
    ASSERT_EQ(maxNbJobsInjectedAtOnce+1, diskWrite.m_tasks.size());
    ASSERT_EQ(maxNbJobsInjectedAtOnce+1, tapeRead.m_tasks.size());

    rti.startThreads();
    rti.requestInjection(false);
    rti.requestInjection(true);
    rti.finish();
    rti.waitThreads();

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

      //static_cast is needed otherwise compilation fails on SL5 with a raw NULL
      ASSERT_EQ(static_cast<DiskWriteTask*>(NULL), diskWriteTask);
      ASSERT_EQ(static_cast<TapeReadTask*>(NULL), tapeReadTask);
      delete diskWriteTask;
      delete tapeReadTask;
    }
  }
  
  TEST_F(castor_tape_tapeserver_daemonTest, RecallTaskInjectorNoFiles) {
    castor::log::StringLogger log("castor_tape_tapeserver_daemon_RecallTaskInjectorTest");
    castor::log::LogContext lc(log);
    RecallMemoryManager mm(50U, 50U, lc);
    castor::tape::tapeserver::drive::FakeDrive drive;
    
    MockRetrieveMount trm(0);
    EXPECT_CALL(trm, internalGetNextJob()).Times(1); //no work: single call to getnextjob
    
    FakeDiskWriteThreadPool diskWrite(lc);
    castor::messages::AcsProxyDummy acs;
    castor::mediachanger::MmcProxyDummy mmc;
    castor::legacymsg::RmcProxyDummy rmc;
    castor::mediachanger::MediaChangerFacade mc(acs, mmc, rmc);
    castor::messages::TapeserverProxyDummy initialProcess;  
    castor::tape::tapeserver::daemon::VolumeInfo volume;
    volume.density="8000GC";
    volume.vid="V12345";
    volume.mountType=cta::MountType::RETRIEVE;
    castor::server::ProcessCapDummy cap;
    castor::tape::tapeserver::daemon::TapeServerReporter tsr(initialProcess, DriveConfig(), "0.0.0.0", volume, lc);  
    FakeSingleTapeReadThread tapeRead(drive, mc, tsr, volume, cap, lc);

    tapeserver::daemon::RecallTaskInjector rti(mm, tapeRead, diskWrite, trm, 6, blockSize, lc);

    ASSERT_EQ(false, rti.synchronousInjection());
    ASSERT_EQ(0U, diskWrite.m_tasks.size());
    ASSERT_EQ(0U, tapeRead.m_tasks.size());
  }
}
