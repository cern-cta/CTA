/******************************************************************************
 *                      RecallReportPackerTest.cpp
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
#include "castor/tape/tapeserver/daemon/RecallReportPacker.hpp"
#include "castor/tape/tapeserver/client/FakeClient.hpp"
#include "castor/log/StringLogger.hpp"
#include <gtest/gtest.h>

using ::testing::_;
namespace unitTests{
TEST(castor_tape_tapeserver_daemon, RecallReportPackerNominal) {
  MockClient client;
  ::testing::InSequence dummy;
  EXPECT_CALL(client, reportRecallResults(_,_)).Times(1);
  EXPECT_CALL(client, reportEndOfSession(_)).Times(1);
  
  castor::log::StringLogger log("castor_tape_tapeserver_RecallReportPackerNominal");
  castor::log::LogContext lc(log);
  tapeserver::daemon::RecallReportPacker rrp(client,2,lc);
  rrp.startThreads();
  
  tapegateway::FileToRecallStruct recalledFiled;
  rrp.reportCompletedJob(recalledFiled);
  rrp.reportCompletedJob(recalledFiled);
  rrp.reportEndOfSession();
  rrp.waitThread();
  
  std::string temp = log.getLog();
  ASSERT_NE(std::string::npos, temp.find("Nominal RecallReportPacker::EndofSession has been reported"));
 
}

TEST(castor_tape_tapeserver_daemon, RecallReportPackerCumulated) {
  MockClient client;
  ::testing::InSequence dummy;
  EXPECT_CALL(client, reportRecallResults(_,_)).Times(2);
  EXPECT_CALL(client, reportEndOfSession(_)).Times(1);
  
  castor::log::StringLogger log("castor_tape_tapeserver_RecallReportPackerCumulated");
  castor::log::LogContext lc(log);
  tapeserver::daemon::RecallReportPacker rrp(client,2,lc);
  rrp.startThreads();
  
  tapegateway::FileToRecallStruct recalledFiled;
  rrp.reportCompletedJob(recalledFiled);
  rrp.reportCompletedJob(recalledFiled);
  rrp.reportCompletedJob(recalledFiled);
  rrp.reportEndOfSession();
  rrp.waitThread();

  std::string temp = log.getLog();
  ASSERT_NE(std::string::npos, temp.find("Nominal RecallReportPacker::EndofSession has been reported"));
}

TEST(castor_tape_tapeserver_daemon, RecallReportPackerBadBadEnd) {
  std::string error_msg="ERROR_TEST_MSG";
  int error_code=std::numeric_limits<int>::max();
  //FakeClient client;
  MockClient client;
  ::testing::InSequence dummy;
  EXPECT_CALL(client, reportRecallResults(_,_)).Times(2);
  EXPECT_CALL(client, reportEndOfSessionWithError(error_msg,error_code,_)).Times(1);
  
  castor::log::StringLogger log("castor_tape_tapeserver_RecallReportPackerBadBadEnd");
  castor::log::LogContext lc(log);

  tapeserver::daemon::RecallReportPacker rrp(client,2,lc);
  rrp.startThreads();

  tapegateway::FileToRecallStruct recalledFiled;
  rrp.reportCompletedJob(recalledFiled);
  rrp.reportCompletedJob(recalledFiled);
  rrp.reportCompletedJob(recalledFiled);
  rrp.reportFailedJob(recalledFiled,error_msg,error_code);
  rrp.reportEndOfSessionWithErrors(error_msg,error_code);
  rrp.waitThread();

  std::string temp = log.getLog();
  ASSERT_NE(std::string::npos, temp.find(error_msg));
}

TEST(castor_tape_tapeserver_daemon, RecallReportPackerBadGoodEnd) {

  std::string error_msg="ERROR_TEST_MSG";  

  MockClient client;
  ::testing::InSequence dummy;
  EXPECT_CALL(client, reportRecallResults(_,_)).Times(2);
  EXPECT_CALL(client, 
    reportEndOfSessionWithError("RecallReportPacker::EndofSession has been reported  but an error happened somewhere in the process",SEINTERNAL,_)
          ).Times(1);

  castor::log::StringLogger log("castor_tape_tapeserver_RecallReportPackerBadBadEnd");
  castor::log::LogContext lc(log);
  
  tapeserver::daemon::RecallReportPacker rrp(client,2,lc);
  rrp.startThreads();
  
  
  tapegateway::FileToRecallStruct recalledFiled;
  rrp.reportCompletedJob(recalledFiled);
  rrp.reportCompletedJob(recalledFiled);
  rrp.reportCompletedJob(recalledFiled);
  rrp.reportFailedJob(recalledFiled,error_msg,-1);
  rrp.reportEndOfSession();
  rrp.waitThread();

  std::string temp = log.getLog();
  ASSERT_NE(std::string::npos, temp.find("EndofSession has been reported  but an error happened somewhere in the process"));
}
TEST(castor_tape_tapeserver_daemon, RecallReportPackerGoodBadEnd) {
  
  std::string error_msg="ERROR_TEST_MSG";
  int error_code=std::numeric_limits<int>::max();
  
  MockClient client;
  ::testing::InSequence dummy;
  EXPECT_CALL(client, reportRecallResults(_,_)).Times(1);
  EXPECT_CALL(client, 
          reportEndOfSessionWithError("RecallReportPacker::EndofSessionWithErrors has been reported  but NO error was detected during the process",SEINTERNAL,_)
          ).Times(1);

  castor::log::StringLogger log("castor_tape_tapeserver_RecallReportPackerBadBadEnd");
  castor::log::LogContext lc(log);
  
  tapeserver::daemon::RecallReportPacker rrp(client,2,lc);
  rrp.startThreads();
  
  tapegateway::FileToRecallStruct recalledFiled;
  rrp.reportCompletedJob(recalledFiled);
  rrp.reportCompletedJob(recalledFiled);
  rrp.reportEndOfSessionWithErrors(error_msg,error_code);
  rrp.waitThread();

  std::string temp = log.getLog();
  ASSERT_NE(std::string::npos, temp.find("EndofSessionWithErrors has been reported  but NO error was detected during the process"));
}

TEST(castor_tape_tapeserver_daemon, RecallReportPackerFaillure) {
  
  std::string error_msg="ERROR_TEST_MSG";
  int error_code=std::numeric_limits<int>::max();
  
  using namespace ::testing;
  
  MockClient client;
  ::testing::InSequence dummy;
  EXPECT_CALL(client, reportRecallResults(_,_)).WillRepeatedly(Throw(castor::tape::Exception("")));
  EXPECT_CALL(client, reportEndOfSessionWithError(_,SEINTERNAL,_)).Times(1);

  castor::log::StringLogger log("castor_tape_tapeserver_RecallReportPackerBadBadEnd");
  castor::log::LogContext lc(log);
  
  tapeserver::daemon::RecallReportPacker rrp(client,2,lc);
  rrp.startThreads();
  
  tapegateway::FileToRecallStruct recalledFiled;
  rrp.reportCompletedJob(recalledFiled);
  rrp.reportCompletedJob(recalledFiled);
  rrp.reportEndOfSessionWithErrors(error_msg,error_code);
  rrp.waitThread();

  std::string temp = log.getLog();
  ASSERT_NE(std::string::npos, temp.find("Successfully closed client's session after the failed report RecallResult"));
}
}
