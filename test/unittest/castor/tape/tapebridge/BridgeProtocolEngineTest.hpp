/******************************************************************************
 *    test/unittest/castor/tape/tapebridge/BridgeProtocolEngineTest.hpp
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
 * @author Steven.Murray@cern.ch
 *****************************************************************************/

#ifndef TEST_UNITTEST_CASTOR_TAPE_TAPEBRIDGE_BRIDGEPROTOCOLENGINETEST_HPP
#define TEST_UNITTEST_CASTOR_TAPE_TAPEBRIDGE_BRIDGEPROTOCOLENGINETEST_HPP 1

#include "castor/tape/tapebridge/BridgeProtocolEngine.hpp"
#include "h/serrno.h"

#include <cppunit/extensions/HelperMacros.h>
#include <exception>
#include <memory>
#include <stdint.h>
#include <stdlib.h>

namespace castor     {
namespace tape       {
namespace tapebridge {

class BridgeProtocolEngineTest: public CppUnit::TestFixture {
private:

  class DummyFileCloser: public IFileCloser {
  public:

    std::vector<int> m_closedFds;

    ~DummyFileCloser() throw() {
      // Do nothing
    }

    int closeFd(const int fd) {
      m_closedFds.push_back(fd);
      return 0;
    }
  }; // class DummyFileCloser

  class AlwaysFalseBoolFunctor: public utils::BoolFunctor {
  public:

    bool operator()() const {
      return false;
    }

    ~AlwaysFalseBoolFunctor() throw() {
       // Do nothing
     }
  };

public:

  BridgeProtocolEngineTest() {
  }

  void setUp() {
  }

  void tearDown() {
  }

  void testConstructor() {
    DummyFileCloser fileCloser;
    const int initialRtcpdSock = 12;

    {
      const BulkRequestConfigParams bulkRequestConfigParams;
      const TapeFlushConfigParams   tapeFlushConfigParams;
      const Cuuid_t                 &cuuid = nullCuuid;
      const int                     rtcpdListenSock  = 11;
      const legacymsg::RtcpJobRqstMsgBody jobRequest = {
        0, // volReqId
        0, // clientPort;
        0, // clientEuid;
        0, // clientEgid;
        "clientHost", // clientHost
        "dgn", // dgn
        "unit", // driveUnit
        "user", // clientUserName
      };
      const tapegateway::Volume volume;
      const uint32_t            nbFilesOnDestinationTape = 0;
      AlwaysFalseBoolFunctor    stoppingGracefully;
      Counter<uint64_t>         tapebridgeTransactionCounter(0);

      BridgeProtocolEngine engine(
        fileCloser,
        bulkRequestConfigParams,
        tapeFlushConfigParams,
        cuuid,
        rtcpdListenSock,
        initialRtcpdSock,
        jobRequest,
        volume,
        nbFilesOnDestinationTape,
        stoppingGracefully,
        tapebridgeTransactionCounter);
    }

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Check that only one socket has been closed",
      (std::vector<int>::size_type)1,
      fileCloser.m_closedFds.size());
    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Check that initialRtcpdSock was the socket that was closed",
      initialRtcpdSock,
      fileCloser.m_closedFds.front());
  }

  void testUnixDomain() {
  }

  CPPUNIT_TEST_SUITE(BridgeProtocolEngineTest);

  CPPUNIT_TEST(testConstructor);
  CPPUNIT_TEST(testUnixDomain);

  CPPUNIT_TEST_SUITE_END();
};

CPPUNIT_TEST_SUITE_REGISTRATION(BridgeProtocolEngineTest);

} // namespace tapebridge
} // namespace tape
} // namespace castor

#endif // TEST_UNITTEST_CASTOR_TAPE_TAPEBRIDGE_BRIDGEPROTOCOLENGINETEST_HPP
