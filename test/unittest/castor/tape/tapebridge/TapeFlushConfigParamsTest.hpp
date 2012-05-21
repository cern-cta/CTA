/******************************************************************************
 *    test/unittest/castor/tape/tapebridge/TapeFlushConfigParamsTest.hpp
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

#ifndef TEST_UNITTEST_CASTOR_TAPE_TAPEBRIDGE_TAPEFLUSHCONFIGPARAMSTEST_HPP
#define TEST_UNITTEST_CASTOR_TAPE_TAPEBRIDGE_TAPEFLUSHCONFIGPARAMSTEST_HPP 1

#include "castor/exception/InvalidArgument.hpp"
#include "castor/tape/tapebridge/Constants.hpp"
#include "castor/tape/tapebridge/TapeFlushConfigParams.hpp"
#include "h/rtcpd_constants.h"
#include "h/tapebridge_constants.h"
#include "test/unittest/castor/tape/tapebridge/TestingTapeFlushConfigParams.hpp"

#include <cppunit/extensions/HelperMacros.h>
#include <exception>
#include <stdint.h>
#include <stdlib.h>

namespace castor     {
namespace tape       {
namespace tapebridge {

class TapeFlushConfigParamsTest: public CppUnit::TestFixture {
public:

  TapeFlushConfigParamsTest() {
  }

  void setUp() {
    setenv("PATH_CONFIG", "/etc/castor/castor.conf", 1);
    unsetenv("RTCPD_THREAD_POOL");
    unsetenv("TAPEBRIDGE_TAPEFLUSHMODE");
    unsetenv("TAPEBRIDGE_MAXBYTESBEFOREFLUSH");
    unsetenv("TAPEBRIDGE_MAXFILESBEFOREFLUSH");
  }

  void tearDown() {
    setUp();
  }

  void testDetermineTapeFlushModeInvalidEnv() {
    TestingTapeFlushConfigParams params;

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "setenv TAPEBRIDGE_TAPEFLUSHMODE",
      0,
      setenv("TAPEBRIDGE_TAPEFLUSHMODE",
        "INVALID_ENUM_VALUE", 1));

    CPPUNIT_ASSERT_THROW_MESSAGE(
      "Invalid environment variable",
      params.determineTapeFlushMode(),
      castor::exception::InvalidArgument);
  }

  void testDetermineTapeFlushModeEnvN_FLUSHES_PER_FILE() {
    TestingTapeFlushConfigParams params;

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "setenv TAPEBRIDGE_TAPEFLUSHMODE",
      0,
      setenv("TAPEBRIDGE_TAPEFLUSHMODE", "N_FLUSHES_PER_FILE", 1));

    CPPUNIT_ASSERT_NO_THROW_MESSAGE(
      "determineTapeFlushMode",
      params.determineTapeFlushMode());

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "getCategory()",
      std::string("TAPEBRIDGE"),
      params.getTapeFlushMode().getCategory());

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "getName()",
      std::string("TAPEFLUSHMODE"),
      params.getTapeFlushMode().getName());

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "getValue()",
      TAPEBRIDGE_N_FLUSHES_PER_FILE,
      params.getTapeFlushMode().getValue());

    CPPUNIT_ASSERT_EQUAL_MESSAGE("getSource()",
      ConfigParamSource::ENVIRONMENT_VARIABLE,
      params.getTapeFlushMode().getSource());
  }

  void testDetermineTapeFlushModeEnvONE_FLUSH_PER_N_FILES() {
    TestingTapeFlushConfigParams params;

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "setenv TAPEBRIDGE_TAPEFLUSHMODE",
      0,
      setenv("TAPEBRIDGE_TAPEFLUSHMODE", "ONE_FLUSH_PER_N_FILES", 1));

    CPPUNIT_ASSERT_NO_THROW_MESSAGE(
      "determineTapeFlushMode",
      params.determineTapeFlushMode());

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "getCategory()",
      std::string("TAPEBRIDGE"),
      params.getTapeFlushMode().getCategory());

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "getName()",
      std::string("TAPEFLUSHMODE"),
      params.getTapeFlushMode().getName());

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "getValue()",
      TAPEBRIDGE_ONE_FLUSH_PER_N_FILES,
      params.getTapeFlushMode().getValue());

    CPPUNIT_ASSERT_EQUAL_MESSAGE("getSource()",
      ConfigParamSource::ENVIRONMENT_VARIABLE,
      params.getTapeFlushMode().getSource());
  }

  void testDetermineTapeFlushModeInvalidPathConfig() {
    TestingTapeFlushConfigParams params;

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "setenv PATH_CONFIG",
      0,
      setenv("PATH_CONFIG", "INVALID PATH CONFIG", 1));

    CPPUNIT_ASSERT_NO_THROW_MESSAGE(
      "determineTapeFlushMode",
      params.determineTapeFlushMode());

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "getCategory()",
      std::string("TAPEBRIDGE"),
      params.getTapeFlushMode().getCategory());

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "getName()",
      std::string("TAPEFLUSHMODE"),
      params.getTapeFlushMode().getName());

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "getValue()",
      castor::tape::tapebridge::TAPEBRIDGE_TAPEFLUSHMODE,
      params.getTapeFlushMode().getValue());

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "getSource()",
      ConfigParamSource::COMPILE_TIME_DEFAULT,
      params.getTapeFlushMode().getSource());
  }

  void testDetermineTapeFlushModeInvalidLocalCastorConf() {
    TestingTapeFlushConfigParams params;

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "setenv PATH_CONFIG",
      0,
      setenv("PATH_CONFIG", "determineTapeFlushMode_invalid_castor.conf", 1));

    CPPUNIT_ASSERT_THROW_MESSAGE(
      "Invalid local castor.conf variable",
      params.determineTapeFlushMode(),
      castor::exception::InvalidArgument);
  }

  void testDetermineTapeFlushModeLocalCastorConfN_FLUSHES_PER_FILE() {
    TestingTapeFlushConfigParams params;

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "setenv PATH_CONFIG",
      0,
      setenv("PATH_CONFIG",
        "determineTapeFlushModeN_FLUSHES_PER_FILE_castor.conf", 1));

    CPPUNIT_ASSERT_NO_THROW_MESSAGE(
      "determineTapeFlushMode",
      params.determineTapeFlushMode());

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "getCategory()",
      std::string("TAPEBRIDGE"),
      params.getTapeFlushMode().getCategory());

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "getName()",
      std::string("TAPEFLUSHMODE"),
      params.getTapeFlushMode().getName());

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "getValue()",
      TAPEBRIDGE_N_FLUSHES_PER_FILE,
      params.getTapeFlushMode().getValue());

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "getSource()",
      ConfigParamSource::CASTOR_CONF,
      params.getTapeFlushMode().getSource());
  }

  void testDetermineTapeFlushModeLocalCastorConfONE_FLUSH_PER_N_FILES() {
    TestingTapeFlushConfigParams params;

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "setenv PATH_CONFIG",
      0,
      setenv("PATH_CONFIG",
        "determineTapeFlushModeONE_FLUSH_PER_N_FILES_castor.conf", 1));

    CPPUNIT_ASSERT_NO_THROW_MESSAGE(
      "determineTapeFlushMode",
      params.determineTapeFlushMode());

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "getCategory()",
      std::string("TAPEBRIDGE"),
      params.getTapeFlushMode().getCategory());

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "getName()",
      std::string("TAPEFLUSHMODE"),
      params.getTapeFlushMode().getName());

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "getValue()",
      TAPEBRIDGE_ONE_FLUSH_PER_N_FILES,
      params.getTapeFlushMode().getValue());

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "getSource()",
      ConfigParamSource::CASTOR_CONF,
      params.getTapeFlushMode().getSource());
  }

  void testDetermineMaxBytesBeforeFlushInvalidEnv() {
    TestingTapeFlushConfigParams params;

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "setenv TAPEBRIDGE_MAXBYTESBEFOREFLUSH",
      0,
      setenv("TAPEBRIDGE_MAXBYTESBEFOREFLUSH", "INVALID_UINT_VALUE", 1));

    CPPUNIT_ASSERT_THROW_MESSAGE("Invalid environment variable",
      params.determineMaxBytesBeforeFlush(),
      castor::exception::InvalidArgument);
  }

  void testDetermineMaxBytesBeforeFlushEnv() {
    TestingTapeFlushConfigParams params;

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "setenv TAPEBRIDGE_MAXBYTESBEFOREFLUSH",
      0,
      setenv("TAPEBRIDGE_MAXBYTESBEFOREFLUSH", "11111", 1));

    CPPUNIT_ASSERT_NO_THROW_MESSAGE(
      "determineMaxBytesBeforeFlush",
      params.determineMaxBytesBeforeFlush());

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "getCategory()",
      std::string("TAPEBRIDGE"),
      params.getMaxBytesBeforeFlush().getCategory());

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "getName()",
      std::string("MAXBYTESBEFOREFLUSH"),
      params.getMaxBytesBeforeFlush().getName());

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "getValue()",
      (uint64_t)11111,
      params.getMaxBytesBeforeFlush().getValue());

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "getSource()",
      ConfigParamSource::ENVIRONMENT_VARIABLE,
      params.getMaxBytesBeforeFlush().getSource());
  }

  void testDetermineMaxBytesBeforeFlushInvalidPathConfig() {
    TestingTapeFlushConfigParams params;

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "setenv PATH_CONFIG",
      0,
      setenv("PATH_CONFIG", "INVALID PATH CONFIG", 1));

    CPPUNIT_ASSERT_NO_THROW_MESSAGE(
      "determineMaxBytesBeforeFlush()",
      params.determineMaxBytesBeforeFlush());

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "getCategory()",
      std::string("TAPEBRIDGE"),
      params.getMaxBytesBeforeFlush().getCategory());

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "getName()",
      std::string("MAXBYTESBEFOREFLUSH"),
      params.getMaxBytesBeforeFlush().getName());

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "getValue()",
      castor::tape::tapebridge::TAPEBRIDGE_MAXBYTESBEFOREFLUSH,
      params.getMaxBytesBeforeFlush().getValue());

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "getSource()",
      ConfigParamSource::COMPILE_TIME_DEFAULT,
      params.getMaxBytesBeforeFlush().getSource());
  }

  void testDetermineMaxBytesBeforeFlushInvalidLocalCastorConf() {
    TestingTapeFlushConfigParams params;

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "setenv PATH_CONFIG",
      0,
      setenv("PATH_CONFIG", "determineMaxBytesBeforeFlush_invalid_castor.conf",
        1));

    CPPUNIT_ASSERT_THROW_MESSAGE(
      "Invalid local castor.conf variable",
      params.determineMaxBytesBeforeFlush(),
      castor::exception::InvalidArgument);
  }

  void testDetermineMaxBytesBeforeFlushLocalCastorConf() {
    TestingTapeFlushConfigParams params;

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "setenv PATH_CONFIG",
      0,
      setenv("PATH_CONFIG", "determineMaxBytesBeforeFlush_castor.conf", 1));

    CPPUNIT_ASSERT_NO_THROW_MESSAGE(
      "determineMaxBytesBeforeFlush",
      params.determineMaxBytesBeforeFlush());

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "getCategory()",
      std::string("TAPEBRIDGE"),
      params.getMaxBytesBeforeFlush().getCategory());

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "getName()",
      std::string("MAXBYTESBEFOREFLUSH"),
      params.getMaxBytesBeforeFlush().getName());

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "getValue()",
      (uint64_t)33333,
      params.getMaxBytesBeforeFlush().getValue());

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "getSource()",
      ConfigParamSource::CASTOR_CONF,
      params.getMaxBytesBeforeFlush().getSource());
  }

  void testDetermineMaxFilesBeforeFlushInvalidEnv() {
    TestingTapeFlushConfigParams params;

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "setenv TAPEBRIDGE_MAXFILESBEFOREFLUSH",
      0,
      setenv("TAPEBRIDGE_MAXFILESBEFOREFLUSH", "INVALID_UINT_VALUE", 1));

    CPPUNIT_ASSERT_THROW_MESSAGE(
      "Invalid environment variable",
      params.determineMaxFilesBeforeFlush(),
      castor::exception::InvalidArgument);
  }

  void testDetermineMaxFilesBeforeFlushEnv() {
    TestingTapeFlushConfigParams params;

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "setenv TAPEBRIDGE_MAXFILESBEFOREFLUSH",
      0,
      setenv("TAPEBRIDGE_MAXFILESBEFOREFLUSH", "11111", 1));

    CPPUNIT_ASSERT_NO_THROW_MESSAGE(
      "determineMaxFilesBeforeFlush()",
      params.determineMaxFilesBeforeFlush());

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "getCategory()",
      std::string("TAPEBRIDGE"),
      params.getMaxFilesBeforeFlush().getCategory());

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "getName()",
      std::string("MAXFILESBEFOREFLUSH"),
      params.getMaxFilesBeforeFlush().getName());

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "getValue()",
      (uint64_t)11111,
      params.getMaxFilesBeforeFlush().getValue());

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "getSource()",
      ConfigParamSource::ENVIRONMENT_VARIABLE,
      params.getMaxFilesBeforeFlush().getSource());
  }

  void testDetermineMaxFilesBeforeFlushInvalidPathConfig() {
    TestingTapeFlushConfigParams params;

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "setenv PATH_CONFIG",
      0,
      setenv("PATH_CONFIG", "INVALID PATH CONFIG", 1));

    CPPUNIT_ASSERT_NO_THROW_MESSAGE(
      "determineMaxFilesBeforeFlush()",
      params.determineMaxFilesBeforeFlush());

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "getCategory()",
      std::string("TAPEBRIDGE"),
      params.getMaxFilesBeforeFlush().getCategory());

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "getName()",
      std::string("MAXFILESBEFOREFLUSH"),
      params.getMaxFilesBeforeFlush().getName());

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "getValue()",
      castor::tape::tapebridge::TAPEBRIDGE_MAXFILESBEFOREFLUSH,
      params.getMaxFilesBeforeFlush().getValue());

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "getSource()",
      ConfigParamSource::COMPILE_TIME_DEFAULT,
      params.getMaxFilesBeforeFlush().getSource());
  }

  void testDetermineMaxFilesBeforeFlushInvalidLocalCastorConf() {
    TestingTapeFlushConfigParams params;

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "setenv PATH_CONFIG",
      0,
      setenv("PATH_CONFIG", "determineMaxFilesBeforeFlush_invalid_castor.conf",
        1));

    CPPUNIT_ASSERT_THROW_MESSAGE(
      "Invalid local castor.conf variable",
      params.determineMaxFilesBeforeFlush(),
      castor::exception::InvalidArgument);
  }

  void testDetermineMaxFilesBeforeFlushLocalCastorConf() {
    TestingTapeFlushConfigParams params;

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "setenv PATH_CONFIG",
      0,
      setenv("PATH_CONFIG", "determineMaxFilesBeforeFlush_castor.conf", 1));

    CPPUNIT_ASSERT_NO_THROW_MESSAGE(
      "determineMaxFilesBeforeFlush()",
      params.determineMaxFilesBeforeFlush());

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "getCategory()",
      std::string("TAPEBRIDGE"),
      params.getMaxFilesBeforeFlush().getCategory());

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "getName()",
      std::string("MAXFILESBEFOREFLUSH"),
      params.getMaxFilesBeforeFlush().getName());

    {
      const ConfigParam<uint64_t> &maxFilesBeforeFlush =
        params.getMaxFilesBeforeFlush();

      CPPUNIT_ASSERT_NO_THROW_MESSAGE(
        "Checking maxFilesBeforeFlush.getValue() does not throw an exception",
        maxFilesBeforeFlush.getValue());

      CPPUNIT_ASSERT_EQUAL_MESSAGE(
        "maxFilesBeforeFlush.getValue()",
        (uint64_t)44444,
        maxFilesBeforeFlush.getValue());
    }

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "getSource()",
      ConfigParamSource::CASTOR_CONF,
      params.getMaxFilesBeforeFlush().getSource());
  }

  void testDetermineTapeFlushConfigParamsCastorConf() {
    TapeFlushConfigParams params;

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "setenv PATH_CONFIG",
      0,
      setenv("PATH_CONFIG", "determineTapeFlushConfigParams_castor.conf", 1));

    CPPUNIT_ASSERT_NO_THROW_MESSAGE(
      "determineConfigParams",
      params.determineConfigParams());

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "getCategory()",
      std::string("TAPEBRIDGE"),
      params.getMaxBytesBeforeFlush().getCategory());

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "getName()",
      std::string("MAXBYTESBEFOREFLUSH"),
      params.getMaxBytesBeforeFlush().getName());

    {
      const ConfigParam<uint64_t> &maxBytesBeforeFlush =
        params.getMaxBytesBeforeFlush();

      CPPUNIT_ASSERT_NO_THROW_MESSAGE(
        "Checking maxBytesBeforeFlush.getValue() does not throw an exception",
        maxBytesBeforeFlush.getValue());

      CPPUNIT_ASSERT_EQUAL_MESSAGE(
        "maxBytesBeforeFlush.getValue()",
        (uint64_t)12345,
        maxBytesBeforeFlush.getValue());
    }

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "getSource()",
      ConfigParamSource::CASTOR_CONF,
      params.getMaxBytesBeforeFlush().getSource());

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "getCategory()",
      std::string("TAPEBRIDGE"),
      params.getMaxFilesBeforeFlush().getCategory());

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "getName()",
      std::string("MAXFILESBEFOREFLUSH"),
      params.getMaxFilesBeforeFlush().getName());

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "getValue()",
      (uint64_t)67890,
      params.getMaxFilesBeforeFlush().getValue());

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "getSource()",
      ConfigParamSource::CASTOR_CONF,
      params.getMaxFilesBeforeFlush().getSource());

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "getCategory()",
      std::string("TAPEBRIDGE"),
      params.getTapeFlushMode().getCategory());

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "getName()",
      std::string("TAPEFLUSHMODE"),
      params.getTapeFlushMode().getName());

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "getValue()",
      TAPEBRIDGE_ONE_FLUSH_PER_N_FILES,
      params.getTapeFlushMode().getValue());

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "getSource()",
      ConfigParamSource::CASTOR_CONF,
      params.getTapeFlushMode().getSource());
  }

  CPPUNIT_TEST_SUITE(TapeFlushConfigParamsTest);

  CPPUNIT_TEST(testDetermineTapeFlushModeInvalidEnv);
  CPPUNIT_TEST(testDetermineTapeFlushModeEnvN_FLUSHES_PER_FILE);
  CPPUNIT_TEST(testDetermineTapeFlushModeEnvONE_FLUSH_PER_N_FILES);
  CPPUNIT_TEST(testDetermineTapeFlushModeInvalidPathConfig);
  CPPUNIT_TEST(testDetermineTapeFlushModeInvalidLocalCastorConf);
  CPPUNIT_TEST(testDetermineTapeFlushModeLocalCastorConfN_FLUSHES_PER_FILE);
  CPPUNIT_TEST(testDetermineTapeFlushModeLocalCastorConfONE_FLUSH_PER_N_FILES);

  CPPUNIT_TEST(testDetermineMaxBytesBeforeFlushInvalidEnv);
  CPPUNIT_TEST(testDetermineMaxBytesBeforeFlushEnv);
  CPPUNIT_TEST(testDetermineMaxBytesBeforeFlushInvalidPathConfig);
  CPPUNIT_TEST(testDetermineMaxBytesBeforeFlushInvalidLocalCastorConf);
  CPPUNIT_TEST(testDetermineMaxBytesBeforeFlushLocalCastorConf);

  CPPUNIT_TEST(testDetermineMaxFilesBeforeFlushInvalidEnv);
  CPPUNIT_TEST(testDetermineMaxFilesBeforeFlushEnv);
  CPPUNIT_TEST(testDetermineMaxFilesBeforeFlushInvalidPathConfig);
  CPPUNIT_TEST(testDetermineMaxFilesBeforeFlushInvalidLocalCastorConf);
  CPPUNIT_TEST(testDetermineMaxFilesBeforeFlushLocalCastorConf);

  CPPUNIT_TEST(testDetermineTapeFlushConfigParamsCastorConf);

  CPPUNIT_TEST_SUITE_END();
};

CPPUNIT_TEST_SUITE_REGISTRATION(TapeFlushConfigParamsTest);

} // namespace tapebridge
} // namespace tape
} // namespace castor

#endif // TEST_UNITTEST_CASTOR_TAPE_TAPEBRIDGE_TAPEFLUSHCONFIGPARAMSTEST_HPP
