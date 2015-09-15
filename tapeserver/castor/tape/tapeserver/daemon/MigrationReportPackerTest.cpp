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

#include "castor/log/StringLogger.hpp"
#include "castor/tape/tapeserver/daemon/MigrationReportPacker.hpp"
#include "castor/tape/tapeserver/drive/DriveInterface.hpp"
#include "serrno.h"
//#include "scheduler/mockDB/MockSchedulerDatabase.hpp"

#include <gtest/gtest.h>

using ::testing::_;
using ::testing::Invoke;
using namespace castor::tape;

namespace unitTests {
  
  class castor_tape_tapeserver_daemonTest: public ::testing::Test {
  protected:

    void SetUp() {
    }

    void TearDown() {
    }

    class MockArchiveJob: public cta::ArchiveJob {
    public:
      MockArchiveJob(): cta::ArchiveJob(*((cta::ArchiveMount *)NULL), 
        *((cta::NameServer *)NULL), cta::ArchiveFile(), 
        cta::RemotePathAndStatus(), cta::NameServerTapeFile()) {
      }

      ~MockArchiveJob() throw() {
      }


      MOCK_METHOD0(complete, void());
      MOCK_METHOD1(failed, void(const cta::exception::Exception &ex));
    }; // class MockArchiveJob

    class MockArchiveMount: public cta::ArchiveMount {
    public:
      MockArchiveMount(): cta::ArchiveMount(*((cta::NameServer *)NULL)) {
        const unsigned int nbArchiveJobs = 2;
        createArchiveJobs(nbArchiveJobs);
      }

      ~MockArchiveMount() throw() {
      }

      std::unique_ptr<cta::ArchiveJob> getNextJob() {
        internalGetNextJob();
        if(m_jobs.empty()) {
          return std::unique_ptr<cta::ArchiveJob>();
        } else {
          std::unique_ptr<cta::ArchiveJob> job =  std::move(m_jobs.front());
          m_jobs.pop_front();
          return job;
        }
      }

      MOCK_METHOD0(internalGetNextJob, cta::ArchiveJob*());
      MOCK_METHOD0(complete, void());

    private:

      std::list<std::unique_ptr<cta::ArchiveJob>> m_jobs;

      void createArchiveJobs(const unsigned int nbJobs) {
        for(unsigned int i = 0; i < nbJobs; i++) {
          m_jobs.push_back(std::unique_ptr<cta::ArchiveJob>(
            new MockArchiveJob()));
        }
      }
    }; // class MockArchiveMount

  }; // class castor_tape_tapeserver_daemonTest
  
  TEST_F(castor_tape_tapeserver_daemonTest, MigrationReportPackerNominal) {
    MockArchiveMount tam;

    ::testing::InSequence dummy;
    std::unique_ptr<cta::ArchiveJob> job1;
    {
      std::unique_ptr<MockArchiveJob> mockJob(new MockArchiveJob());
      EXPECT_CALL(*mockJob, complete()).Times(1);
      job1.reset(mockJob.release());
    }
    std::unique_ptr<cta::ArchiveJob> job2;
    {
      std::unique_ptr<MockArchiveJob> mockJob(new MockArchiveJob());
      EXPECT_CALL(*mockJob, complete()).Times(1);
      job2.reset(mockJob.release());
    }
    EXPECT_CALL(tam, complete()).Times(1);

    castor::log::StringLogger log("castor_tape_tapeserver_daemon_MigrationReportPackerNominal");
    castor::log::LogContext lc(log);
    tapeserver::daemon::MigrationReportPacker mrp(&tam,lc);
    mrp.startThreads();

    mrp.reportCompletedJob(std::move(job1));
    mrp.reportCompletedJob(std::move(job2));

    const tapeserver::drive::compressionStats statsCompress;
    mrp.reportFlush(statsCompress);
    mrp.reportEndOfSession();
    mrp.waitThread(); //here

    std::string temp = log.getLog();
    ASSERT_NE(std::string::npos, temp.find("Reported to the client that a batch of files was written on tape"));
  }

  TEST_F(castor_tape_tapeserver_daemonTest, MigrationReportPackerFailure) {
    MockArchiveMount tam;

    ::testing::InSequence dummy;
    std::unique_ptr<cta::ArchiveJob> job1;
    {
      std::unique_ptr<MockArchiveJob> mockJob(new MockArchiveJob());
      job1.reset(mockJob.release());
    }
    std::unique_ptr<cta::ArchiveJob> job2;
    {
      std::unique_ptr<MockArchiveJob> mockJob(new MockArchiveJob());
      job2.reset(mockJob.release());
    }
    std::unique_ptr<cta::ArchiveJob> job3;
    {
      std::unique_ptr<MockArchiveJob> mockJob(new MockArchiveJob());
      EXPECT_CALL(*mockJob, failed(_)).Times(1);
      job3.reset(mockJob.release());
    }
    EXPECT_CALL(tam, complete()).Times(1);

    castor::log::StringLogger log("castor_tape_tapeserver_daemon_MigrationReportPackerFailure");
    castor::log::LogContext lc(log);  
    tapeserver::daemon::MigrationReportPacker mrp(&tam,lc);
    mrp.startThreads();

    mrp.reportCompletedJob(std::move(job1));
    mrp.reportCompletedJob(std::move(job2));

    const std::string error_msg = "ERROR_TEST_MSG";
    const castor::exception::Exception ex(error_msg);
    mrp.reportFailedJob(std::move(job3),ex);

    const tapeserver::drive::compressionStats statsCompress;
    mrp.reportFlush(statsCompress);
    mrp.reportEndOfSession();
    mrp.waitThread();

    std::string temp = log.getLog();
    ASSERT_NE(std::string::npos, temp.find(error_msg));
  }

  TEST_F(castor_tape_tapeserver_daemonTest, MigrationReportPackerOneByteFile) {
    MockArchiveMount tam;

    ::testing::InSequence dummy;
    std::unique_ptr<cta::ArchiveJob> migratedBigFile;
    {
      std::unique_ptr<MockArchiveJob> mockJob(new MockArchiveJob());
      EXPECT_CALL(*mockJob, complete()).Times(1);
      migratedBigFile.reset(mockJob.release());
    }
    std::unique_ptr<cta::ArchiveJob> migratedFileSmall;
    {
      std::unique_ptr<MockArchiveJob> mockJob(new MockArchiveJob());
      EXPECT_CALL(*mockJob, complete()).Times(1);
      migratedFileSmall.reset(mockJob.release());
    }
    std::unique_ptr<cta::ArchiveJob> migratedNullFile;
    {
      std::unique_ptr<MockArchiveJob> mockJob(new MockArchiveJob());
      EXPECT_CALL(*mockJob, complete()).Times(1);
      migratedNullFile.reset(mockJob.release());
    }
    EXPECT_CALL(tam, complete()).Times(1);

    migratedBigFile->archiveFile.size=100000;
    migratedFileSmall->archiveFile.size=1;
    migratedNullFile->archiveFile.size=0;
    
    castor::log::StringLogger log("castor_tape_tapeserver_daemon_MigrationReportPackerOneByteFile");
    castor::log::LogContext lc(log);  
    tapeserver::daemon::MigrationReportPacker mrp(&tam,lc);
    mrp.startThreads();

    mrp.reportCompletedJob(std::move(migratedBigFile));
    mrp.reportCompletedJob(std::move(migratedFileSmall));
    mrp.reportCompletedJob(std::move(migratedNullFile));
    tapeserver::drive::compressionStats stats;
    stats.toTape=(100000+1)/3;
    mrp.reportFlush(stats);
    mrp.reportEndOfSession();
    mrp.waitThread();

    std::string temp = log.getLog();
    ASSERT_NE(std::string::npos, temp.find("Reported to the client that a batch of files was written on tape"));
  } 
}
