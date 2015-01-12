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

#include "castor/legacymsg/CupvProxyDummy.hpp"
#include "castor/legacymsg/VdqmProxyDummy.hpp"
#include "castor/legacymsg/VmgrProxyDummy.hpp"
#include "castor/log/DummyLogger.hpp"
#include "castor/tape/tapeserver/daemon/ProcessForkerProxyDummy.hpp"
#include "castor/tape/tapeserver/daemon/TestingVdqmConnectionHandler.hpp"
#include "castor/tape/reactor/DummyZMQReactor.hpp"
#include "castor/utils/SmartFd.hpp"

#include <gtest/gtest.h>

namespace unitTests {

class castor_tape_tapeserver_daemon_VdqmConnectionHandlerTest:
  public ::testing::Test {
protected:

  castor_tape_tapeserver_daemon_VdqmConnectionHandlerTest():
    m_log("unittests"),
    m_reactor(m_log),
    m_netTimeout(1),
    m_isGrantedReturnValue(true),
    m_cupv(m_isGrantedReturnValue),
    m_catalogue(
      m_netTimeout,
      m_log,
      m_processForker,
      m_cupv,
      m_vdqm,
      m_vmgr,
      "hostName",
      m_catalogueConfig) {
  }

  castor::log::DummyLogger m_log;

  castor::tape::reactor::DummyZMQReactor m_reactor;

  const int m_netTimeout;

  castor::tape::tapeserver::daemon::ProcessForkerProxyDummy m_processForker;

  const bool m_isGrantedReturnValue;

  castor::legacymsg::CupvProxyDummy m_cupv;

  castor::legacymsg::VdqmProxyDummy m_vdqm;

  castor::legacymsg::VmgrProxyDummy m_vmgr;

  castor::tape::tapeserver::daemon::CatalogueConfig m_catalogueConfig;

  castor::tape::tapeserver::daemon::Catalogue m_catalogue;

  castor::tape::tapeserver::daemon::TapeDaemonConfig m_config;

  virtual void SetUp() {
  }

  virtual void TearDown() {
  }
};

TEST_F(castor_tape_tapeserver_daemon_VdqmConnectionHandlerTest, getPeerHostName) {
  using namespace castor::tape::tapeserver::daemon;
    
  int pair[2] = {-1, -1};
  ASSERT_EQ(0, socketpair(AF_UNIX, SOCK_STREAM, 0, pair));
  castor::utils::SmartFd pairElement0(pair[0]);
  castor::utils::SmartFd pairElement1(pair[1]);
      
  TestingVdqmConnectionHandler handler(
    pairElement0.get(),
    m_reactor,
    m_log,
    m_catalogue,
    m_config);

  std::string peerHostName;
  ASSERT_NO_THROW(peerHostName = handler.getPeerHostName(pairElement0.get()));
  ASSERT_FALSE(peerHostName.empty());
}

TEST_F(castor_tape_tapeserver_daemon_VdqmConnectionHandlerTest, connectionIsFromTrustedVdqmHost) {
  using namespace castor::tape::tapeserver::daemon;

  int pair[2] = {-1, -1};
  ASSERT_EQ(0, socketpair(AF_UNIX, SOCK_STREAM, 0, pair));
  castor::utils::SmartFd pairElement0(pair[0]);
  castor::utils::SmartFd pairElement1(pair[1]);

  TestingVdqmConnectionHandler handler(
    pairElement0.get(),
    m_reactor,
    m_log,
    m_catalogue,
    m_config);

  ASSERT_NO_THROW(handler.connectionIsFromTrustedVdqmHost());
  ASSERT_FALSE(handler.connectionIsFromTrustedVdqmHost());
}

} // namespace unitTests
