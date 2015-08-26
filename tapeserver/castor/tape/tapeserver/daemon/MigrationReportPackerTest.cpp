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
#include "scheduler/mockDB/MockSchedulerDatabase.hpp"

#include <gtest/gtest.h>

namespace unitTests {
  
  class TestingArchiveMount: public cta::ArchiveMount {
  public:
    TestingArchiveMount(std::unique_ptr<cta::SchedulerDatabase::ArchiveMount> dbam): ArchiveMount(std::move(dbam)) {
    }
    ~TestingArchiveMount() throw() {}
    void complete() {
      complete_();
    }
    void failed(const std::exception &ex) {
      failed_(ex);
    }
    MOCK_METHOD0(complete_, void());
    MOCK_METHOD1(failed_, void(const std::exception &ex));
  };
  
  class TestingArchiveJob: public cta::ArchiveJob {
  public:
    TestingArchiveJob() {
    }
    ~TestingArchiveJob() throw() {}
    void complete() {
      complete_();
    }
    void failed(const cta::exception::Exception &ex) {
      failed_(ex);
    }
    MOCK_METHOD0(complete_, void());
    MOCK_METHOD1(failed_, void(const cta::exception::Exception &ex));
  };
  
  class TestingDBArchiveJob: public cta::SchedulerDatabase::ArchiveMount {
    virtual const MountInfo & getMountInfo() {
      return mountInfo;
    }
  };
  
  const std::string error="ERROR_TEST";
  using namespace castor::tape;
  const tapeserver::drive::compressionStats statsCompress;
  using ::testing::_;
  
TEST(castor_tape_tapeserver_daemon, MigrationReportPackerNominal) {
  std::unique_ptr<cta::SchedulerDatabase::ArchiveMount> dbam(new TestingDBArchiveJob);
  TestingArchiveMount tam(std::move(dbam));
  ::testing::InSequence dummy;
  EXPECT_CALL(tam, complete_()).Times(1);
  
  castor::log::StringLogger log("castor_tape_tapeserver_daemon_MigrationReportPackerNominal");
  castor::log::LogContext lc(log);
  tapeserver::daemon::MigrationReportPacker mrp(&tam,lc);
  mrp.startThreads();
  
  std::unique_ptr<TestingArchiveJob> migratedFile(new TestingArchiveJob());
  
  mrp.reportCompletedJob(std::move(migratedFile),0,0);
  mrp.reportFlush(statsCompress);
  mrp.reportEndOfSession();
  mrp.waitThread(); //here
  
  std::string temp = log.getLog();
  ASSERT_NE(std::string::npos, temp.find("Reported to the client that a batch of files was written on tape"));
}

TEST(castor_tape_tapeserver_daemon, MigrationReportPackerFailure) {
  std::unique_ptr<cta::SchedulerDatabase::ArchiveMount> dbam(new TestingDBArchiveJob);
  TestingArchiveMount tam(std::move(dbam));
  ::testing::InSequence dummy;
  EXPECT_CALL(tam, failed_(_)).Times(1);
  
  castor::log::StringLogger log("castor_tape_tapeserver_daemon_MigrationReportPackerFailure");
  castor::log::LogContext lc(log);  
  tapeserver::daemon::MigrationReportPacker mrp(&tam,lc);
  mrp.startThreads();
  
  std::unique_ptr<TestingArchiveJob> migratedFile(new TestingArchiveJob());
  std::unique_ptr<TestingArchiveJob> failed(new TestingArchiveJob());
  
  mrp.reportCompletedJob(std::move(migratedFile),0,0);
  mrp.reportFailedJob(std::move(failed),error,-1);
  mrp.reportFlush(statsCompress);
  mrp.reportEndOfSessionWithErrors(error,-1);
  mrp.waitThread();
  
  std::string temp = log.getLog();
  ASSERT_NE(std::string::npos, temp.find("Reported end of session with error to client after sending file errors"));
} 

TEST(castor_tape_tapeserver_daemon, MigrationReportPackerFailureGoodEnd) {  
  std::unique_ptr<cta::SchedulerDatabase::ArchiveMount> dbam(new TestingDBArchiveJob);
  TestingArchiveMount tam(std::move(dbam));
  ::testing::InSequence dummy;
  EXPECT_CALL(tam, failed_(_)).Times(1);
  
  castor::log::StringLogger log("castor_tape_tapeserver_daemon_MigrationReportPackerFailureGoodEnd");
  castor::log::LogContext lc(log);  
  tapeserver::daemon::MigrationReportPacker mrp(&tam,lc);
  mrp.startThreads();
  
  std::unique_ptr<TestingArchiveJob> migratedFile(new TestingArchiveJob());
  std::unique_ptr<TestingArchiveJob> failed(new TestingArchiveJob());
  
  mrp.reportCompletedJob(std::move(migratedFile),0,0);
  mrp.reportFailedJob(std::move(failed),error,-1);
  mrp.reportFlush(statsCompress);
  mrp.reportEndOfSession();
  mrp.waitThread();
   
  std::string temp = log.getLog();
  ASSERT_NE(std::string::npos, temp.find("Reported end of session with error to client due to previous file errors")); 
} 

TEST(castor_tape_tapeserver_daemon, MigrationReportPackerGoodBadEnd) {
  std::unique_ptr<cta::SchedulerDatabase::ArchiveMount> dbam(new TestingDBArchiveJob);
  TestingArchiveMount tam(std::move(dbam));
  ::testing::InSequence dummy;
  EXPECT_CALL(tam, failed_(_)).Times(1);
  
  castor::log::StringLogger log("castor_tape_tapeserver_daemon_MigrationReportPackerGoodBadEnd");
  castor::log::LogContext lc(log);  
  tapeserver::daemon::MigrationReportPacker mrp(&tam,lc);
  mrp.startThreads();
  
  std::unique_ptr<TestingArchiveJob> migratedFile(new TestingArchiveJob());
  
  mrp.reportCompletedJob(std::move(migratedFile),0,0);
  mrp.reportFlush(statsCompress);
  mrp.reportEndOfSessionWithErrors(error,-1);
  mrp.waitThread();
  
  std::string temp = log.getLog();
  ASSERT_NE(std::string::npos, temp.find("Reported end of session with error to client"));
}

TEST(castor_tape_tapeserver_daemon, MigrationReportPackerOneByteFile) {
  std::unique_ptr<cta::SchedulerDatabase::ArchiveMount> dbam(new TestingDBArchiveJob);
  TestingArchiveMount tam(std::move(dbam));
  ::testing::InSequence dummy;
  EXPECT_CALL(tam, complete_()).Times(1);
  
  castor::log::StringLogger log("castor_tape_tapeserver_daemon_MigrationReportPackerOneByteFile");
  castor::log::LogContext lc(log);  
  tapeserver::daemon::MigrationReportPacker mrp(&tam,lc);
  mrp.startThreads();
  
  std::unique_ptr<TestingArchiveJob> migratedBigFile(new TestingArchiveJob());
  std::unique_ptr<TestingArchiveJob> migratedFileSmall(new TestingArchiveJob());
  std::unique_ptr<TestingArchiveJob> migrateNullFile(new TestingArchiveJob());
  migratedBigFile->archiveFile.size=100000;
  migratedFileSmall->archiveFile.size=1;
  migrateNullFile->archiveFile.size=0;
  
  mrp.reportCompletedJob(std::move(migratedBigFile),0,0);
  mrp.reportCompletedJob(std::move(migratedFileSmall),0,0);
  mrp.reportCompletedJob(std::move(migrateNullFile),0,0);
  tapeserver::drive::compressionStats stats;
  stats.toTape=(100000+1)/3;
  mrp.reportFlush(stats);
  mrp.reportEndOfSession();
  mrp.waitThread();
} 
}
