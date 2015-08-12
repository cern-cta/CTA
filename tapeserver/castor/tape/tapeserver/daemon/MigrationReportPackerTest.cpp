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
#include "castor/tape/tapeserver/client/FakeClient.hpp"
#include "serrno.h"
#include "scheduler/mockDB/MockSchedulerDatabase.hpp"

#include <gtest/gtest.h>

namespace unitTests {
  const std::string error="ERROR_TEST";
  using namespace castor::tape;
  const tapeserver::drive::compressionStats statsCompress;
  using ::testing::_;
  
TEST(castor_tape_tapeserver_daemon, MigrationReportPackerNominal) {
  MockClient client;
  ::testing::InSequence dummy;
  EXPECT_CALL(client, reportMigrationResults(_,_)).Times(1);
  EXPECT_CALL(client, reportEndOfSession(_)).Times(1);
  
  castor::log::StringLogger log("castor_tape_tapeserver_daemon_MigrationReportPackerNominal");
  castor::log::LogContext lc(log);
  std::unique_ptr<cta::MockSchedulerDatabase> mdb(new cta::MockSchedulerDatabase);
  tapeserver::daemon::MigrationReportPacker mrp(dynamic_cast<cta::ArchiveMount *>((mdb->getNextMount("ll","drive")).get()),lc);
  mrp.startThreads();
  
  tapegateway::FileToMigrateStruct migratedFile;
  
  mrp.reportCompletedJob(migratedFile,0,0);
  mrp.reportCompletedJob(migratedFile,0,0);
  mrp.reportFlush(statsCompress);
  mrp.reportEndOfSession();
  mrp.waitThread();
  
  std::string temp = log.getLog();
  ASSERT_NE(std::string::npos, temp.find("A file was successfully written on the tape"));
}

TEST(castor_tape_tapeserver_daemon, MigrationReportPackerFaillure) {
  testing::StrictMock<MockClient> client;
  ::testing::InSequence dummy;
  EXPECT_CALL(client, reportMigrationResults(_,_)).Times(1);
  EXPECT_CALL(client, reportEndOfSessionWithError(error,-1,_)).Times(1);
  
  castor::log::StringLogger log("castor_tape_tapeserver_daemon_MigrationReportPackerFaillure");
  castor::log::LogContext lc(log);
  
  
  std::unique_ptr<cta::MockSchedulerDatabase> mdb(new cta::MockSchedulerDatabase);
  tapeserver::daemon::MigrationReportPacker mrp(dynamic_cast<cta::ArchiveMount *>((mdb->getNextMount("ll","drive")).get()),lc);
  mrp.startThreads();
  
  tapegateway::FileToMigrateStruct migratedFile;
  tapegateway::FileToMigrateStruct failed;
  mrp.reportCompletedJob(migratedFile,0,0);
  mrp.reportCompletedJob(migratedFile,0,0);  
  mrp.reportCompletedJob(migratedFile,0,0);
  mrp.reportFailedJob(failed,error,-1);
  mrp.reportFlush(statsCompress);
  mrp.reportEndOfSessionWithErrors(error,-1);
  mrp.waitThread();
  
  std::string temp = log.getLog();
  ASSERT_NE(std::string::npos, temp.find("Reported failed file(s) to the client before end of session"));
} 

TEST(castor_tape_tapeserver_daemon, MigrationReportPackerFaillureGoodEnd) {
  testing::StrictMock<MockClient> client;
  ::testing::InSequence dummy;
  EXPECT_CALL(client, reportMigrationResults(_,_)).Times(1);
  EXPECT_CALL(client, reportEndOfSessionWithError(_,SEINTERNAL,_)).Times(1);
  castor::log::StringLogger log("castor_tape_tapeserver_daemon_MigrationReportPackerFaillureGoodEnd");
  castor::log::LogContext lc(log);
  
  
  std::unique_ptr<cta::MockSchedulerDatabase> mdb(new cta::MockSchedulerDatabase);
  tapeserver::daemon::MigrationReportPacker mrp(dynamic_cast<cta::ArchiveMount *>((mdb->getNextMount("ll","drive")).get()),lc);
  mrp.startThreads();
  
  tapegateway::FileToMigrateStruct migratedFile;
  tapegateway::FileToMigrateStruct failed;
  mrp.reportCompletedJob(migratedFile,0,0);
  mrp.reportCompletedJob(migratedFile,0,0);  
  mrp.reportCompletedJob(migratedFile,0,0);
  mrp.reportFailedJob(failed,error,-1);
  mrp.reportFlush(statsCompress);
  mrp.reportEndOfSession();
  mrp.waitThread();
   
  std::string temp = log.getLog();
  ASSERT_NE(std::string::npos, temp.find("Reported end of session with error to client due to previous file errors"));
 
} 

TEST(castor_tape_tapeserver_daemon, MigrationReportPackerGoodBadEnd) {
  testing::StrictMock<MockClient> client;
  ::testing::InSequence dummy;
  EXPECT_CALL(client, reportMigrationResults(_,_)).Times(1);
  EXPECT_CALL(client, reportEndOfSessionWithError(_,SEINTERNAL,_)).Times(1);
  
  castor::log::StringLogger log("castor_tape_tapeserver_daemon_MigrationReportPackerGoodBadEnd");
  castor::log::LogContext lc(log);
 
  
  std::unique_ptr<cta::MockSchedulerDatabase> mdb(new cta::MockSchedulerDatabase);
  tapeserver::daemon::MigrationReportPacker mrp(dynamic_cast<cta::ArchiveMount *>((mdb->getNextMount("ll","drive")).get()),lc);
  mrp.startThreads();
  
  tapegateway::FileToMigrateStruct migratedFile;
  tapegateway::FileToMigrateStruct failed;
  mrp.reportCompletedJob(migratedFile,0,0);
  mrp.reportCompletedJob(migratedFile,0,0);  
  mrp.reportCompletedJob(migratedFile,0,0);

  mrp.reportFlush(statsCompress);
  mrp.reportEndOfSessionWithErrors(error,-1);
  mrp.waitThread();
  
  std::string temp = log.getLog();
  ASSERT_NE(std::string::npos, temp.find("Reported end of session with error to client"));
} 

MATCHER(checkCompressFileSize,"compressedFileSize is zero"){ 
  bool b=true;
  typedef std::vector<
                      castor::tape::tapegateway::FileMigratedNotificationStruct*
                      >::iterator iterator;
  iterator e = arg.successfulMigrations().end();
  iterator it = arg.successfulMigrations().begin();

  for(;it!=e;++it){
    b = b && ((*it)->compressedFileSize() > 0);
  }
  return b;
}

TEST(castor_tape_tapeserver_daemon, MigrationReportPackerOneByteFile) {
  
  //using ::testing::Each;
  testing::StrictMock<MockClient> client;
  ::testing::InSequence dummy;
  EXPECT_CALL(client, reportMigrationResults(checkCompressFileSize(),_)).Times(1);
  EXPECT_CALL(client, reportEndOfSession(_)).Times(1);
  
  castor::log::StringLogger log("castor_tape_tapeserver_daemon_MigrationReportPackerGoodBadEnd");
  castor::log::LogContext lc(log);
 
  
  std::unique_ptr<cta::MockSchedulerDatabase> mdb(new cta::MockSchedulerDatabase);
  tapeserver::daemon::MigrationReportPacker mrp(dynamic_cast<cta::ArchiveMount *>((mdb->getNextMount("ll","drive")).get()),lc);
  mrp.startThreads();
  
  tapegateway::FileToMigrateStruct migratedFileSmall;
  migratedFileSmall.setFileSize(1);
  tapegateway::FileToMigrateStruct migratedBigFile;
  migratedBigFile.setFileSize(100000);
  tapegateway::FileToMigrateStruct migrateNullFile;
  migratedBigFile.setFileSize(0);
  
  mrp.reportCompletedJob(migratedBigFile,0,0);
  mrp.reportCompletedJob(migratedFileSmall,0,0);
  mrp.reportCompletedJob(migrateNullFile,0,0);
  
  tapeserver::drive::compressionStats stats;
  stats.toTape=(100000+1)/3;
  mrp.reportFlush(stats);
  mrp.reportEndOfSession();
  mrp.waitThread();
} 
}
