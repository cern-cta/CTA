/******************************************************************************
 *                test/castor/tape/tapebridge/BridgeProtocolEngineTest.hpp
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

#ifndef TEST_CASTOR_TAPE_TAPEBRIDGE_BRIDGEPROTOCOLENGINETEST_HPP
#define TEST_CASTOR_TAPE_TAPEBRIDGE_BRIDGEPROTOCOLENGINETEST_HPP 1

#include "test_exception.hpp"
#include "castor/tape/legacymsg/RtcpJobRqstMsgBody.hpp"
#include "castor/tape/tapebridge/BridgeProtocolEngine.hpp"
#include "castor/tape/tapebridge/Counter.hpp"
#include "castor/tape/tapegateway/Volume.hpp"
#include "h/Cuuid.h"
#include "h/rtcpd_constants.h"

#include <cppunit/extensions/HelperMacros.h>
#include <exception>
#include <stdlib.h>

class BridgeProtocolEngineTest: public CppUnit::TestFixture {
private:

  /**
   * BoolFunctor that always returns false.
   */
  class FalseBoolFunctor : public castor::tape::tapebridge::BoolFunctor {
  public:

    /**
     * Always returns false.
     */
    bool operator()() {
      return false;
    }
  };

  castor::tape::legacymsg::RtcpJobRqstMsgBody    m_jobRequest;
  castor::tape::tapegateway::Volume              m_volume;
  FalseBoolFunctor                               m_stoppingGracefully;
  castor::tape::tapebridge::Counter<uint64_t>    m_tapebridgeTransactionCounter;
  castor::tape::tapebridge::BridgeProtocolEngine *m_engine;

public:

  BridgeProtocolEngineTest():
    m_tapebridgeTransactionCounter(0), m_engine(NULL) {
    memset(&m_jobRequest, '\0', sizeof(m_jobRequest));
  }

  void setUp() {
    const Cuuid_t  &cuuid                   = nullCuuid; // Dummy value
    const int      listenSock               = 0;         // Dummy value
    const int      initialRtcpdSock         = 0;         // Dummy value
    const uint32_t nbFilesOnDestinationTape = 0;         // Dummy value

    setenv("PATH_CONFIG", "/etc/castor/castor.conf", 1);
    unsetenv("RTCPD_THREAD_POOL");

    m_tapebridgeTransactionCounter.reset(0);
    m_engine = new castor::tape::tapebridge::BridgeProtocolEngine(
      cuuid,
      listenSock,
      initialRtcpdSock,
      m_jobRequest,
      m_volume,
      nbFilesOnDestinationTape,
      m_stoppingGracefully,
      m_tapebridgeTransactionCounter
    );
  }

  void tearDown() {
    setenv("PATH_CONFIG", "/etc/castor/castor.conf", 1);
    unsetenv("RTCPD_THREAD_POOL");
    delete(m_engine);
  }

  void testGetNbRtcpdDiskIOThreadsInvalidEnv() {
    castor::tape::tapebridge::ConfigParamAndSource<uint32_t>
      nbRtcpdDiskIOThreads("UNKNOWN NAME", 0, "UNKNOWN SOURCE");

    CPPUNIT_ASSERT_EQUAL_MESSAGE("setenv RTCPD_THREAD_POOL",
      0,
      setenv("RTCPD_THREAD_POOL", "INVALID_UINT_VALUE", 1));

    CPPUNIT_ASSERT_THROW_MESSAGE("Invalid environment variable",
      nbRtcpdDiskIOThreads = m_engine->getNbRtcpdDiskIOThreads(),
      castor::exception::InvalidArgument);

    std::string exceptionMsg = "UNKNOWN";
    try {
      nbRtcpdDiskIOThreads = m_engine->getNbRtcpdDiskIOThreads();
    } catch(castor::exception::InvalidArgument &ex) {
      exceptionMsg = ex.getMessage().str();
    }
    CPPUNIT_ASSERT_EQUAL_MESSAGE("exceptionMsg",
      std::string("Configuration parameter is not a valid unsigned integer:"
        " name=RTCPD/THREAD_POOL"
        " value=INVALID_UINT_VALUE"
        " source=environment variable"),
      exceptionMsg);
  }

  void testGetNbRtcpdDiskIOThreadsEnv() {
    castor::tape::tapebridge::ConfigParamAndSource<uint32_t>
      nbRtcpdDiskIOThreads("UNKNOWN NAME", 0, "UNKNOWN SOURCE");

    CPPUNIT_ASSERT_EQUAL_MESSAGE("setenv RTCPD_THREAD_POOL",
      0,
      setenv("RTCPD_THREAD_POOL", "11111", 1));

    CPPUNIT_ASSERT_NO_THROW_MESSAGE(
      "getNbRtcpdDiskIOThreads",
      nbRtcpdDiskIOThreads = m_engine->getNbRtcpdDiskIOThreads());

    CPPUNIT_ASSERT_EQUAL_MESSAGE("nbRtcpdDiskIOThreads.name",
      std::string("RTCPD/THREAD_POOL"),
      nbRtcpdDiskIOThreads.name);

    CPPUNIT_ASSERT_EQUAL_MESSAGE("nbRtcpdDiskIOThreads.value",
      (uint32_t)11111,
      nbRtcpdDiskIOThreads.value);

    CPPUNIT_ASSERT_EQUAL_MESSAGE("nbRtcpdDiskIOThreads.source",
      std::string("environment variable"),
      nbRtcpdDiskIOThreads.source);
  }

  void testGetNbRtcpdDiskIOThreadsInvalidPathConfig() {
    castor::tape::tapebridge::ConfigParamAndSource<uint32_t>
      nbRtcpdDiskIOThreads("UNKNOWN NAME", 0, "UNKNOWN SOURCE");

    CPPUNIT_ASSERT_EQUAL_MESSAGE("setenv PATH_CONFIG",
      0,
      setenv("PATH_CONFIG", "INVALID PATH CONFIG", 1));

    CPPUNIT_ASSERT_NO_THROW_MESSAGE(
      "getNbRtcpdDiskIOThreads",
      nbRtcpdDiskIOThreads = m_engine->getNbRtcpdDiskIOThreads());

    CPPUNIT_ASSERT_EQUAL_MESSAGE("nbRtcpdDiskIOThreads.name",
      std::string("RTCPD/THREAD_POOL"),
      nbRtcpdDiskIOThreads.name);

    CPPUNIT_ASSERT_EQUAL_MESSAGE("nbRtcpdDiskIOThreads.value",
      (uint32_t)RTCPD_THREAD_POOL,
      nbRtcpdDiskIOThreads.value);

    CPPUNIT_ASSERT_EQUAL_MESSAGE("nbRtcpdDiskIOThreads.source",
      std::string("compile-time default"),
      nbRtcpdDiskIOThreads.source);
  }


  void testGetNbRtcpdDiskIOThreadsInvalidLocalCastorConf() {
    castor::tape::tapebridge::ConfigParamAndSource<uint32_t>
      nbRtcpdDiskIOThreads("UNKNOWN NAME", 0, "UNKNOWN SOURCE");

    CPPUNIT_ASSERT_EQUAL_MESSAGE("setenv PATH_CONFIG",
      0,
      setenv("PATH_CONFIG", "getNbRtcpdDiskIOThreads_invalid_castor.conf", 1));

    CPPUNIT_ASSERT_THROW_MESSAGE("Invalid local castor.conf variable",
      nbRtcpdDiskIOThreads = m_engine->getNbRtcpdDiskIOThreads(),
      castor::exception::InvalidArgument);

    std::string exceptionMsg = "UNKNOWN";
    try {
      nbRtcpdDiskIOThreads = m_engine->getNbRtcpdDiskIOThreads();
    } catch(castor::exception::InvalidArgument &ex) {
      exceptionMsg = ex.getMessage().str();
    }
    CPPUNIT_ASSERT_EQUAL_MESSAGE("exceptionMsg",
      std::string("Configuration parameter is not a valid unsigned integer:"
        " name=RTCPD/THREAD_POOL"
        " value=INVALID_UINT_VALUE"
        " source=castor.conf"),
      exceptionMsg);
  }

  void testGetNbRtcpdDiskIOThreadsLocalCastorConf() {
    castor::tape::tapebridge::ConfigParamAndSource<uint32_t>
      nbRtcpdDiskIOThreads("UNKNOWN NAME", 0, "UNKNOWN SOURCE");

    CPPUNIT_ASSERT_EQUAL_MESSAGE("setenv PATH_CONFIG",
      0,
      setenv("PATH_CONFIG", "getNbRtcpdDiskIOThreads_castor.conf", 1));

    CPPUNIT_ASSERT_NO_THROW_MESSAGE(
      "getNbRtcpdDiskIOThreads",
      nbRtcpdDiskIOThreads = m_engine->getNbRtcpdDiskIOThreads());

    CPPUNIT_ASSERT_EQUAL_MESSAGE("nbRtcpdDiskIOThreads.name",
      std::string("RTCPD/THREAD_POOL"),
      nbRtcpdDiskIOThreads.name);

    CPPUNIT_ASSERT_EQUAL_MESSAGE("nbRtcpdDiskIOThreads.value",
      (uint32_t)22222,
      nbRtcpdDiskIOThreads.value);

    CPPUNIT_ASSERT_EQUAL_MESSAGE("nbRtcpdDiskIOThreads.source",
      std::string("castor.conf"),
      nbRtcpdDiskIOThreads.source);
  }

  CPPUNIT_TEST_SUITE(BridgeProtocolEngineTest);
  CPPUNIT_TEST(testGetNbRtcpdDiskIOThreadsInvalidEnv);
  CPPUNIT_TEST(testGetNbRtcpdDiskIOThreadsEnv);
  CPPUNIT_TEST(testGetNbRtcpdDiskIOThreadsInvalidPathConfig);
  CPPUNIT_TEST(testGetNbRtcpdDiskIOThreadsInvalidLocalCastorConf);
  CPPUNIT_TEST(testGetNbRtcpdDiskIOThreadsLocalCastorConf);
  CPPUNIT_TEST_SUITE_END();
};

CPPUNIT_TEST_SUITE_REGISTRATION(BridgeProtocolEngineTest);

#endif // TEST_CASTOR_TAPE_TAPEBRIDGE_BRIDGEPROTOCOLENGINETEST_HPP
