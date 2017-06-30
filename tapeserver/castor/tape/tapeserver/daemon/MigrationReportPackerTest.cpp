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

#include "common/log/StringLogger.hpp"
#include "castor/tape/tapeserver/daemon/MigrationReportPacker.hpp"
#include "castor/tape/tapeserver/drive/DriveInterface.hpp"
#include "catalogue/CatalogueFactory.hpp"
#include "scheduler/testingMocks/MockArchiveMount.hpp"

#include <gtest/gtest.h>

using ::testing::_;
using ::testing::Invoke;
using namespace castor::tape;

namespace unitTests {
  
  class castor_tape_tapeserver_daemon_MigrationReportPackerTest: public ::testing::Test {
  protected:

    void SetUp() {
      using namespace cta;
      using namespace cta::catalogue;

      rdbms::Login catalogueLogin(rdbms::Login::DBTYPE_IN_MEMORY, "", "", "");
      const uint64_t nbConns = 1;
      const uint64_t nbArchiveFileListingConns = 0;
      m_catalogue = CatalogueFactory::create(catalogueLogin, nbConns, nbArchiveFileListingConns);
    }

    void TearDown() {
      m_catalogue.reset();
    }

    std::unique_ptr<cta::catalogue::Catalogue> m_catalogue;

  }; // class castor_tape_tapeserver_daemon_MigrationReportPackerTest
  
  class MockArchiveJobExternalStats: public cta::MockArchiveJob {
  public:
    MockArchiveJobExternalStats(cta::ArchiveMount & am, cta::catalogue::Catalogue & catalogue, 
       int & completes, int &failures):
    MockArchiveJob(am, catalogue), completesRef(completes), failuresRef(failures) {}
    
    virtual bool complete() {
      completesRef++;
      return false;
    }
    
    
    virtual void failed(const cta::exception::Exception& ex) {
      failuresRef++;
    }
    
  private:
    int & completesRef;
    int & failuresRef;
  };
  
  TEST_F(castor_tape_tapeserver_daemon_MigrationReportPackerTest, MigrationReportPackerNominal) {
    cta::MockArchiveMount tam(*m_catalogue);

    ::testing::InSequence dummy;
    std::unique_ptr<cta::ArchiveJob> job1;
    int job1completes(0), job1failures(0);
    {
      std::unique_ptr<cta::MockArchiveJob> mockJob(
        new MockArchiveJobExternalStats(tam, *m_catalogue, job1completes, job1failures));
      job1.reset(mockJob.release());
    }
    std::unique_ptr<cta::ArchiveJob> job2;
    int job2completes(0), job2failures(0);
    {
      std::unique_ptr<cta::MockArchiveJob> mockJob(
        new MockArchiveJobExternalStats(tam, *m_catalogue, job2completes, job2failures));
      job2.reset(mockJob.release());
    }

    cta::log::StringLogger log("castor_tape_tapeserver_daemon_MigrationReportPackerNominal",cta::log::DEBUG);
    cta::log::LogContext lc(log);
    tapeserver::daemon::MigrationReportPacker mrp(&tam,lc);
    mrp.startThreads();

    mrp.reportCompletedJob(std::move(job1), lc);
    mrp.reportCompletedJob(std::move(job2), lc);

    const tapeserver::drive::compressionStats statsCompress;
    mrp.reportFlush(statsCompress, lc);
    mrp.reportEndOfSession(lc);
    mrp.reportTestGoingToEnd(lc);
    mrp.waitThread(); //here

    std::string temp = log.getLog();
    ASSERT_NE(std::string::npos, temp.find("Reported to the client that a batch of files was written on tape"));
    ASSERT_EQ(1, tam.completes);
    ASSERT_EQ(1, job1completes);
    ASSERT_EQ(1, job2completes);
  }

  TEST_F(castor_tape_tapeserver_daemon_MigrationReportPackerTest, MigrationReportPackerFailure) {
    cta::MockArchiveMount tam(*m_catalogue);

    ::testing::InSequence dummy;
    std::unique_ptr<cta::ArchiveJob> job1;
    {
      std::unique_ptr<cta::MockArchiveJob> mockJob(new cta::MockArchiveJob(tam, *m_catalogue));
      job1.reset(mockJob.release());
    }
    std::unique_ptr<cta::ArchiveJob> job2;
    {
      std::unique_ptr<cta::MockArchiveJob> mockJob(new cta::MockArchiveJob(tam, *m_catalogue));
      job2.reset(mockJob.release());
    }
    std::unique_ptr<cta::ArchiveJob> job3;
    int job3completes(0), job3failures(0);
    {
      std::unique_ptr<cta::MockArchiveJob> mockJob(
        new MockArchiveJobExternalStats(tam, *m_catalogue, job3completes, job3failures));
      job3.reset(mockJob.release());
    }
    
    cta::log::StringLogger log("castor_tape_tapeserver_daemon_MigrationReportPackerFailure",cta::log::DEBUG);
    cta::log::LogContext lc(log);  
    tapeserver::daemon::MigrationReportPacker mrp(&tam,lc);
    mrp.startThreads();

    mrp.reportCompletedJob(std::move(job1), lc);
    mrp.reportCompletedJob(std::move(job2), lc);

    const std::string error_msg = "ERROR_TEST_MSG";
    const cta::exception::Exception ex(error_msg);
    mrp.reportFailedJob(std::move(job3),ex, lc);

    const tapeserver::drive::compressionStats statsCompress;
    mrp.reportFlush(statsCompress, lc);
    mrp.reportEndOfSession(lc);
    mrp.reportTestGoingToEnd(lc);
    mrp.waitThread();

    std::string temp = log.getLog();
    ASSERT_NE(std::string::npos, temp.find(error_msg));
    ASSERT_EQ(1, tam.completes);
    ASSERT_EQ(1, job3failures);
  }

  TEST_F(castor_tape_tapeserver_daemon_MigrationReportPackerTest, MigrationReportPackerOneByteFile) {
    cta::MockArchiveMount tam(*m_catalogue);

    ::testing::InSequence dummy;
    std::unique_ptr<cta::ArchiveJob> migratedBigFile;
    int migratedBigFileCompletes(0), migratedBigFileFailures(0);
    {
      std::unique_ptr<cta::MockArchiveJob> mockJob(
        new MockArchiveJobExternalStats(tam, *m_catalogue, migratedBigFileCompletes, migratedBigFileFailures));
      migratedBigFile.reset(mockJob.release());
    }
    std::unique_ptr<cta::ArchiveJob> migratedFileSmall;
    int migratedFileSmallCompletes(0), migratedFileSmallFailures(0);
    {
      std::unique_ptr<cta::MockArchiveJob> mockJob(
        new MockArchiveJobExternalStats(tam, *m_catalogue, migratedFileSmallCompletes, migratedFileSmallFailures));
      migratedFileSmall.reset(mockJob.release());
    }
    std::unique_ptr<cta::ArchiveJob> migratedNullFile;
    int migratedNullFileCompletes(0), migratedNullFileFailures(0);
    {
      std::unique_ptr<cta::MockArchiveJob> mockJob(
        new MockArchiveJobExternalStats(tam, *m_catalogue, migratedNullFileCompletes, migratedNullFileFailures));
      migratedNullFile.reset(mockJob.release());
    }

    migratedBigFile->archiveFile.fileSize=100000;
    migratedFileSmall->archiveFile.fileSize=1;
    migratedNullFile->archiveFile.fileSize=0;
    
    cta::log::StringLogger log("castor_tape_tapeserver_daemon_MigrationReportPackerOneByteFile",cta::log::DEBUG);
    cta::log::LogContext lc(log);  
    tapeserver::daemon::MigrationReportPacker mrp(&tam,lc);
    mrp.startThreads();

    mrp.reportCompletedJob(std::move(migratedBigFile), lc);
    mrp.reportCompletedJob(std::move(migratedFileSmall), lc);
    mrp.reportCompletedJob(std::move(migratedNullFile), lc);
    tapeserver::drive::compressionStats stats;
    stats.toTape=(100000+1)/3;
    mrp.reportFlush(stats, lc);
    mrp.reportEndOfSession(lc);
    mrp.reportTestGoingToEnd(lc);
    mrp.waitThread();

    std::string temp = log.getLog();
    ASSERT_NE(std::string::npos, temp.find("Reported to the client that a batch of files was written on tape"));
    ASSERT_EQ(1, tam.completes);
    ASSERT_EQ(1, migratedBigFileCompletes);
    ASSERT_EQ(1, migratedFileSmallCompletes);
    ASSERT_EQ(1, migratedNullFileCompletes);
  } 
}
