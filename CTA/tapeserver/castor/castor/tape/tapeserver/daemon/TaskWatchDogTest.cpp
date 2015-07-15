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

#include "castor/tape/tapeserver/daemon/TaskWatchDog.hpp"
#include "castor/tape/tapeserver/daemon/ReportPackerInterface.hpp"
#include "castor/tape/tapeserver/client/FakeClient.hpp"
#include "castor/messages/TapeserverProxyDummy.hpp"
#include "castor/log/StringLogger.hpp"
#include <gtest/gtest.h>

namespace unitTests {
  using namespace castor::tape;
  using ::testing::_;
  
  struct MockReportPacker : 
  public tapeserver::daemon::ReportPackerInterface<tapeserver::daemon::detail::Recall> {
    // TODO MOCK_METHOD1(reportStuckOn, void(FileStruct& file));
    
    MockReportPacker(tapeserver::client::ClientInterface & tg, castor::log::LogContext lc):
    tapeserver::daemon::ReportPackerInterface<castor::tape::tapeserver::daemon::detail::Recall>(tg,lc){
    
    }
  };
  


TEST(castor_tape_tapeserver_daemon, WatchdogTestStuckWithNothing) {
  const double periodToReport = 10; // We wont report in practice
  const double stuckPeriod = 0.01;
  const double pollPeriod = 0.01;
  
  castor::log::StringLogger log("castor_tape_tapeserver_daemon_WatchdogTestStuck");
  castor::log::LogContext lc(log);
  
  MockClient mockClient;
  castor::messages::TapeserverProxyDummy dummyInitialProcess;


  tapeserver::daemon::RecallWatchDog watchdog(periodToReport,
    stuckPeriod,dummyInitialProcess,"testTapeDrive",lc,pollPeriod);
  
  watchdog.startThread();
  usleep(100000);
  watchdog.stopAndWaitThread();
  //we dont tell the watchdog we are working on file, 
  //it should not report as being stuck
  ASSERT_EQ(std::string::npos, log.getLog().find("No tape block movement for too long"));
}

TEST(castor_tape_tapeserver_daemon, MigrationWatchdogTestStuck) {
  const double reportPeriod = 10; // We wont report in practice
  const double stuckPeriod = 0.01;
  const double pollPeriod = 0.01;
  
  castor::log::StringLogger log("castor_tape_tapeserver_daemon_WatchdogTestStuck");
  castor::log::LogContext lc(log);
  
  MockClient mockClient;
  castor::messages::TapeserverProxyDummy dummyInitialProcess;
  
  // We will poll for a 
  tapeserver::daemon::MigrationWatchDog watchdog(reportPeriod,stuckPeriod,
    dummyInitialProcess,"testTapeDrive",  lc, pollPeriod);
  
  watchdog.startThread();
  tapegateway::FileToMigrateStruct file;
  watchdog.notifyBeginNewJob(file);
  usleep(100000);
  watchdog.stopAndWaitThread();
  // This time the internal watchdog should have triggered
  ASSERT_NE(std::string::npos, log.getLog().find("No tape block movement for too long"));
}

}