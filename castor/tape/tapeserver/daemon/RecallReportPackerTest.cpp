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

namespace {
  
TEST(castor_tape_tapeserver_daemon, RecallReportPackerNominal) {
  FakeClient client;

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
  
  ASSERT_EQ(2,client.current_succes_recall);
  ASSERT_EQ(0,client.current_failled_recall);
}

TEST(castor_tape_tapeserver_daemon, RecallReportPackerCumulated) {
  FakeClient client;
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

  //we except only one success file because the 2 previous have already been flushed
  ASSERT_EQ(1,client.current_succes_recall);
  ASSERT_EQ(0,client.current_failled_recall);
}

TEST(castor_tape_tapeserver_daemon, RecallReportPackerBadBadEnd) {
  FakeClient client;
  castor::log::StringLogger log("castor_tape_tapeserver_RecallReportPackerBadBadEnd");
  castor::log::LogContext lc(log);

  tapeserver::daemon::RecallReportPacker rrp(client,2,lc);
  rrp.startThreads();

  std::string error_msg="ERROR_TEST_MSG";
  int error_code=std::numeric_limits<int>::max();

  tapegateway::FileToRecallStruct recalledFiled;
  rrp.reportCompletedJob(recalledFiled);
  rrp.reportCompletedJob(recalledFiled);
  rrp.reportCompletedJob(recalledFiled);
  rrp.reportFailedJob(recalledFiled,error_msg,error_code);
  rrp.reportEndOfSessionWithErrors(error_msg,error_code);
  rrp.waitThread();

  std::string temp = log.getLog();
  ASSERT_NE(std::string::npos, temp.find(error_msg));
  ASSERT_EQ(error_code,client.error_code);
  //we except only one success file because the 2 previous have already been flushed
  ASSERT_EQ(1,client.current_succes_recall);
  ASSERT_EQ(1,client.current_failled_recall);
}

TEST(castor_tape_tapeserver_daemon, RecallReportPackerBadGoodEnd) {
  FakeClient client;
  castor::log::StringLogger log("castor_tape_tapeserver_RecallReportPackerBadBadEnd");
  castor::log::LogContext lc(log);
  
  tapeserver::daemon::RecallReportPacker rrp(client,2,lc);
  rrp.startThreads();
  
  std::string error_msg="ERROR_TEST_MSG";
  
  tapegateway::FileToRecallStruct recalledFiled;
  rrp.reportCompletedJob(recalledFiled);
  rrp.reportCompletedJob(recalledFiled);
  rrp.reportCompletedJob(recalledFiled);
  rrp.reportFailedJob(recalledFiled,error_msg,-1);
  rrp.reportEndOfSession();
  rrp.waitThread();

  std::string temp = log.getLog();
  ASSERT_NE(std::string::npos, temp.find("EndofSession has been reported  but an error happened somewhere in the process"));
  ASSERT_EQ(SEINTERNAL,client.error_code);
  //we except only one success file because the 2 previous have already been flushed
  ASSERT_EQ(1,client.current_succes_recall);
  ASSERT_EQ(1,client.current_failled_recall);
}
TEST(castor_tape_tapeserver_daemon, RecallReportPackerGoodBadEnd) {
  FakeClient client;
  castor::log::StringLogger log("castor_tape_tapeserver_RecallReportPackerBadBadEnd");
  castor::log::LogContext lc(log);
  
  tapeserver::daemon::RecallReportPacker rrp(client,2,lc);
  rrp.startThreads();
  
  std::string error_msg="ERROR_TEST_MSG";
  int error_code=std::numeric_limits<int>::max();
  
  tapegateway::FileToRecallStruct recalledFiled;
  rrp.reportCompletedJob(recalledFiled);
  rrp.reportCompletedJob(recalledFiled);
  rrp.reportEndOfSessionWithErrors(error_msg,error_code);
  rrp.waitThread();

  std::string temp = log.getLog();
  ASSERT_NE(std::string::npos, temp.find("EndofSessionWithErrors has been reported  but NO error was detected during the process"));
  ASSERT_EQ(SEINTERNAL,client.error_code);
  //we except only one success file because the 2 previous have already been flushed
  ASSERT_EQ(2,client.current_succes_recall);
  ASSERT_EQ(0,client.current_failled_recall);
}
}
