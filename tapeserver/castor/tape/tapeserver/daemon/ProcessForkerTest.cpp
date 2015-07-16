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

#include "castor/log/DummyLogger.hpp"
#include "castor/tape/tapeserver/daemon/ProcessForker.hpp"
#include "castor/tape/tapeserver/daemon/ProcessForkerProxySocket.hpp"
#include "castor/utils/SmartFd.hpp"

#include <gtest/gtest.h>
#include <memory>
#include <sys/types.h>
#include <sys/socket.h>

namespace unitTests {

class castor_tape_tapeserver_daemon_ProcessForkerTest : public ::testing::Test {
protected:

  virtual void SetUp() {
  }

  virtual void TearDown() {
  }
};

TEST_F(castor_tape_tapeserver_daemon_ProcessForkerTest, constructor) {
  using namespace castor::tape::tapeserver::daemon;

  int cmdPair[2] = {-1, -1};
  ASSERT_EQ(0, socketpair(AF_UNIX, SOCK_STREAM, 0, cmdPair));
  castor::utils::SmartFd cmdSenderSocket(cmdPair[0]);
  castor::utils::SmartFd cmdReceiverSocket(cmdPair[1]);

  int reaperPair[2] = {-1, -1};
  ASSERT_EQ(0, socketpair(AF_UNIX, SOCK_STREAM, 0, reaperPair));
  castor::utils::SmartFd reaperSenderSocket(reaperPair[0]);
  castor::utils::SmartFd reaperReceiverSocket(reaperPair[1]);

  const std::string programName = "unittests";
  const std::string hostName = "hostName";
  castor::log::DummyLogger log(programName);
  char argv0[12] = "tapeserverd";
  TapeDaemonConfig config;
  std::auto_ptr<ProcessForker> processForker;
  ASSERT_NO_THROW(processForker.reset(
    new ProcessForker(log, cmdReceiverSocket.get(), reaperSenderSocket.get(),
      hostName, argv0, config)));
  cmdReceiverSocket.release();
}

TEST_F(castor_tape_tapeserver_daemon_ProcessForkerTest, socketproxy) {
  using namespace castor::tape::tapeserver::daemon;

  int cmdPair[2] = {-1, -1};
  ASSERT_EQ(0, socketpair(AF_UNIX, SOCK_STREAM, 0, cmdPair));
  castor::utils::SmartFd cmdSenderSocket(cmdPair[0]);
  castor::utils::SmartFd cmdReceiverSocket(cmdPair[1]);

  int reaperPair[2] = {-1, -1};
  ASSERT_EQ(0, socketpair(AF_UNIX, SOCK_STREAM, 0, reaperPair));
  castor::utils::SmartFd reaperSenderSocket(reaperPair[0]);
  castor::utils::SmartFd reaperReceiverSocket(reaperPair[1]);
  
  const std::string programName = "unittests";
  const std::string hostName = "hostName";
  castor::log::DummyLogger log(programName);
  char argv0[12] = "tapeserverd";
  TapeDaemonConfig config;
  std::auto_ptr<ProcessForker> processForker;
  ASSERT_NO_THROW(processForker.reset(
    new ProcessForker(log, cmdReceiverSocket.get(), reaperSenderSocket.get(),
      hostName, argv0, config)));
  cmdReceiverSocket.release();

  std::auto_ptr<ProcessForkerProxySocket> processForkerProxy;
  ASSERT_NO_THROW(processForkerProxy.reset(
    new ProcessForkerProxySocket(log, cmdSenderSocket.get())));
  cmdSenderSocket.release();
} 

} // namespace unitTests
