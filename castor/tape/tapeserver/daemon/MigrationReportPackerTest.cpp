/******************************************************************************
 *                      MigrationReportPackerTest.cpp
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
#include "castor/tape/tapeserver/daemon/MigrationReportPacker.hpp"
#include "castor/tape/tapeserver/daemon/FakeClient.hpp"
#include "castor/log/StringLogger.hpp"
#include <gtest/gtest.h>

namespace {
  const std::string error="ERROR_TEST";
  using namespace castor::tape;
  
TEST(castor_tape_tapeserver_daemon, MigrationReportPackerNominal) {
  FakeClient client;
  castor::log::StringLogger log("castor_tape_tapeserver_daemon_MigrationReportPackerNominal");
  castor::log::LogContext lc(log);
  tapeserver::daemon::MigrationReportPacker mrp(client,lc);
  mrp.startThreads();
  
  tapegateway::FileToMigrateStruct migratedFile;
  
  mrp.reportCompletedJob(migratedFile);
  mrp.reportCompletedJob(migratedFile);
  mrp.reportFlush();
  mrp.reportEndOfSession();
  mrp.waitThread();
  
  std::string temp = log.getLog();
  ASSERT_NE(std::string::npos, temp.find("A file was successfully written on the tape"));
  
  ASSERT_EQ(2,client.current_succes);
  ASSERT_EQ(0,client.current_failled);
}

TEST(castor_tape_tapeserver_daemon, MigrationReportPackerFaillure) {
  FakeClient client;
  castor::log::StringLogger log("castor_tape_tapeserver_daemon_MigrationReportPackerFaillure");
  castor::log::LogContext lc(log);
  
  tapeserver::daemon::MigrationReportPacker mrp(client,lc);
  mrp.startThreads();
  
  tapegateway::FileToMigrateStruct migratedFile;
  tapegateway::FileToMigrateStruct failed;
  mrp.reportCompletedJob(migratedFile);
  mrp.reportCompletedJob(migratedFile);  
  mrp.reportCompletedJob(migratedFile);
  mrp.reportFailedJob(failed,error,-1);
  mrp.reportFlush();
  mrp.reportEndOfSessionWithErrors(error,-1);
  mrp.waitThread();
  
  std::string temp = log.getLog();
  ASSERT_NE(std::string::npos, temp.find("A flush on tape has been reported but a writing error happened before"));
  
  //We expect init_value twice because nothing should have been written 
  ASSERT_EQ(init_value,client.current_succes);
  ASSERT_EQ(init_value,client.current_failled);
} 

TEST(castor_tape_tapeserver_daemon, MigrationReportPackerFaillureGoodEnd) {
  FakeClient client;
  castor::log::StringLogger log("castor_tape_tapeserver_daemon_MigrationReportPackerFaillureGoodEnd");
  castor::log::LogContext lc(log);
  
  tapeserver::daemon::MigrationReportPacker mrp(client,lc);
  mrp.startThreads();
  
  tapegateway::FileToMigrateStruct migratedFile;
  tapegateway::FileToMigrateStruct failed;
  mrp.reportCompletedJob(migratedFile);
  mrp.reportCompletedJob(migratedFile);  
  mrp.reportCompletedJob(migratedFile);
  mrp.reportFailedJob(failed,error,-1);
  mrp.reportFlush();
  mrp.reportEndOfSession();
  mrp.waitThread();
   
  std::string temp = log.getLog();
  ASSERT_NE(std::string::npos, temp.find("Nominal EndofSession has been reported  but an writing error on the tape happened before"));
  
  //We expect init_value twice because nothing should have been written 
  ASSERT_EQ(init_value,client.current_succes);
  ASSERT_EQ(init_value,client.current_failled);
} 

TEST(castor_tape_tapeserver_daemon, MigrationReportPackerGoodBadEnd) {
  FakeClient client;
  castor::log::StringLogger log("castor_tape_tapeserver_daemon_MigrationReportPackerGoodBadEnd");
  castor::log::LogContext lc(log);
 
  tapeserver::daemon::MigrationReportPacker mrp(client,lc);
  mrp.startThreads();
  
  tapegateway::FileToMigrateStruct migratedFile;
  tapegateway::FileToMigrateStruct failed;
  mrp.reportCompletedJob(migratedFile);
  mrp.reportCompletedJob(migratedFile);  
  mrp.reportCompletedJob(migratedFile);

  mrp.reportFlush();
  mrp.reportEndOfSessionWithErrors(error,-1);
  mrp.waitThread();
  
  std::string temp = log.getLog();
  ASSERT_NE(std::string::npos, temp.find("EndofSessionWithErrors has been reported  but NO writing error on the tape was detected"));
  
  //We expect init_value twice because nothing should have been written 
  ASSERT_EQ(3,client.current_succes);
  ASSERT_EQ(0,client.current_failled);
} 
}
