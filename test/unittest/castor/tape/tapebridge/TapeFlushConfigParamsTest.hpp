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

#include <cppunit/extensions/HelperMacros.h>
#include <exception>
#include <stdint.h>
#include <stdlib.h>

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
    castor::tape::tapebridge::TapeFlushConfigParams params;

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "setenv TAPEBRIDGE_TAPEFLUSHMODE",
      0,
      setenv("TAPEBRIDGE_TAPEFLUSHMODE",
        "INVALID_ENUM_VALUE", 1));

    CPPUNIT_ASSERT_THROW_MESSAGE("Invalid environment variable",
      params.determineTapeFlushMode(),
      castor::exception::InvalidArgument);

    std::string exceptionMsg = "UNKNOWN";
    try {
      params.determineTapeFlushMode();
    } catch(castor::exception::InvalidArgument &ex) {
      exceptionMsg = ex.getMessage().str();
    }
    CPPUNIT_ASSERT_EQUAL_MESSAGE("exceptionMsg",
      std::string(
        "Value of configuration parameter is not valid"
        ": expected N_FLUSHES_PER_FILE, ONE_FLUSH_PER_FILE or"
        " ONE_FLUSH_PER_N_FILES"
        ": category=TAPEBRIDGE"
        " name=TAPEFLUSHMODE"
        " value=INVALID_ENUM_VALUE"
        " source=environment variable"),
      exceptionMsg);
  }

  void testDetermineTapeFlushModeEnvN_FLUSHES_PER_FILE() {
    castor::tape::tapebridge::TapeFlushConfigParams params;

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "setenv TAPEBRIDGE_TAPEFLUSHMODE",
      0,
      setenv("TAPEBRIDGE_TAPEFLUSHMODE", "N_FLUSHES_PER_FILE", 1));

    CPPUNIT_ASSERT_NO_THROW_MESSAGE(
      "determineTapeFlushMode",
      params.determineTapeFlushMode());

    CPPUNIT_ASSERT_EQUAL_MESSAGE("category",
      std::string("TAPEBRIDGE"),
      params.getTapeFlushMode().category);

    CPPUNIT_ASSERT_EQUAL_MESSAGE("name",
      std::string("TAPEFLUSHMODE"),
      params.getTapeFlushMode().name);

    CPPUNIT_ASSERT_EQUAL_MESSAGE("value",
      TAPEBRIDGE_N_FLUSHES_PER_FILE,
      params.getTapeFlushMode().value);

    CPPUNIT_ASSERT_EQUAL_MESSAGE("source",
      std::string("environment variable"),
      params.getTapeFlushMode().source);
  }

  void testDetermineTapeFlushModeEnvONE_FLUSH_PER_FILE() {
    castor::tape::tapebridge::TapeFlushConfigParams params;

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "setenv TAPEBRIDGE_TAPEFLUSHMODE",
      0,
      setenv("TAPEBRIDGE_TAPEFLUSHMODE", "ONE_FLUSH_PER_FILE", 1));

    CPPUNIT_ASSERT_NO_THROW_MESSAGE(
      "determineTapeFlushMode",
      params.determineTapeFlushMode());

    CPPUNIT_ASSERT_EQUAL_MESSAGE("category",
      std::string("TAPEBRIDGE"),
      params.getTapeFlushMode().category);

    CPPUNIT_ASSERT_EQUAL_MESSAGE("name",
      std::string("TAPEFLUSHMODE"),
      params.getTapeFlushMode().name);

    CPPUNIT_ASSERT_EQUAL_MESSAGE("value",
      TAPEBRIDGE_ONE_FLUSH_PER_FILE,
      params.getTapeFlushMode().value);

    CPPUNIT_ASSERT_EQUAL_MESSAGE("source",
      std::string("environment variable"),
      params.getTapeFlushMode().source);
  }

  void testDetermineTapeFlushModeEnvONE_FLUSH_PER_N_FILES() {
    castor::tape::tapebridge::TapeFlushConfigParams params;

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "setenv TAPEBRIDGE_TAPEFLUSHMODE",
      0,
      setenv("TAPEBRIDGE_TAPEFLUSHMODE", "ONE_FLUSH_PER_N_FILES", 1));

    CPPUNIT_ASSERT_NO_THROW_MESSAGE(
      "determineTapeFlushMode",
      params.determineTapeFlushMode());

    CPPUNIT_ASSERT_EQUAL_MESSAGE("category",
      std::string("TAPEBRIDGE"),
      params.getTapeFlushMode().category);

    CPPUNIT_ASSERT_EQUAL_MESSAGE("name",
      std::string("TAPEFLUSHMODE"),
      params.getTapeFlushMode().name);

    CPPUNIT_ASSERT_EQUAL_MESSAGE("value",
      TAPEBRIDGE_ONE_FLUSH_PER_N_FILES,
      params.getTapeFlushMode().value);

    CPPUNIT_ASSERT_EQUAL_MESSAGE("source",
      std::string("environment variable"),
      params.getTapeFlushMode().source);
  }

  void testDetermineTapeFlushModeInvalidPathConfig() {
    castor::tape::tapebridge::TapeFlushConfigParams params;

    CPPUNIT_ASSERT_EQUAL_MESSAGE("setenv PATH_CONFIG",
      0,
      setenv("PATH_CONFIG", "INVALID PATH CONFIG", 1));

    CPPUNIT_ASSERT_NO_THROW_MESSAGE(
      "determineTapeFlushMode",
      params.determineTapeFlushMode());

    CPPUNIT_ASSERT_EQUAL_MESSAGE("category",
      std::string("TAPEBRIDGE"),
      params.getTapeFlushMode().category);

    CPPUNIT_ASSERT_EQUAL_MESSAGE("name",
      std::string("TAPEFLUSHMODE"),
      params.getTapeFlushMode().name);

    CPPUNIT_ASSERT_EQUAL_MESSAGE("value",
      castor::tape::tapebridge::TAPEBRIDGE_TAPEFLUSHMODE,
      params.getTapeFlushMode().value);

    CPPUNIT_ASSERT_EQUAL_MESSAGE("source",
      std::string("compile-time default"),
      params.getTapeFlushMode().source);
  }

  void testDetermineTapeFlushModeInvalidLocalCastorConf() {
    castor::tape::tapebridge::TapeFlushConfigParams params;

    CPPUNIT_ASSERT_EQUAL_MESSAGE("setenv PATH_CONFIG",
      0,
      setenv("PATH_CONFIG", "determineTapeFlushMode_invalid_castor.conf", 1));

    CPPUNIT_ASSERT_THROW_MESSAGE("Invalid local castor.conf variable",
      params.determineTapeFlushMode(),
      castor::exception::InvalidArgument);

    std::string exceptionMsg = "UNKNOWN";
    try {
      params.determineTapeFlushMode();
    } catch(castor::exception::InvalidArgument &ex) {
      exceptionMsg = ex.getMessage().str();
    }
    CPPUNIT_ASSERT_EQUAL_MESSAGE("exceptionMsg",
      std::string(
        "Value of configuration parameter is not valid"
        ": expected N_FLUSHES_PER_FILE, ONE_FLUSH_PER_FILE or"
        " ONE_FLUSH_PER_N_FILES"
        ": category=TAPEBRIDGE"
        " name=TAPEFLUSHMODE"
        " value=INVALID_ENUM_VALUE"
        " source=castor.conf"),
      exceptionMsg);
  }

  void testDetermineTapeFlushModeLocalCastorConfN_FLUSHES_PER_FILE() {
    castor::tape::tapebridge::TapeFlushConfigParams params;

    CPPUNIT_ASSERT_EQUAL_MESSAGE("setenv PATH_CONFIG",
      0,
      setenv("PATH_CONFIG",
        "determineTapeFlushModeN_FLUSHES_PER_FILE_castor.conf", 1));

    CPPUNIT_ASSERT_NO_THROW_MESSAGE(
      "determineTapeFlushMode",
      params.determineTapeFlushMode());

    CPPUNIT_ASSERT_EQUAL_MESSAGE("category",
      std::string("TAPEBRIDGE"),
      params.getTapeFlushMode().category);

    CPPUNIT_ASSERT_EQUAL_MESSAGE("name",
      std::string("TAPEFLUSHMODE"),
      params.getTapeFlushMode().name);

    CPPUNIT_ASSERT_EQUAL_MESSAGE("value",
      TAPEBRIDGE_N_FLUSHES_PER_FILE,
      params.getTapeFlushMode().value);

    CPPUNIT_ASSERT_EQUAL_MESSAGE("source",
      std::string("castor.conf"),
      params.getTapeFlushMode().source);
  }

  void testDetermineTapeFlushModeLocalCastorConfONE_FLUSH_PER_FILE() {
    castor::tape::tapebridge::TapeFlushConfigParams params;

    CPPUNIT_ASSERT_EQUAL_MESSAGE("setenv PATH_CONFIG",
      0,
      setenv("PATH_CONFIG",
        "determineTapeFlushModeONE_FLUSH_PER_FILE_castor.conf", 1));

    CPPUNIT_ASSERT_NO_THROW_MESSAGE(
      "determineTapeFlushMode",
      params.determineTapeFlushMode());

    CPPUNIT_ASSERT_EQUAL_MESSAGE("category",
      std::string("TAPEBRIDGE"),
      params.getTapeFlushMode().category);

    CPPUNIT_ASSERT_EQUAL_MESSAGE("name",
      std::string("TAPEFLUSHMODE"),
      params.getTapeFlushMode().name);

    CPPUNIT_ASSERT_EQUAL_MESSAGE("value",
      TAPEBRIDGE_ONE_FLUSH_PER_FILE,
      params.getTapeFlushMode().value);

    CPPUNIT_ASSERT_EQUAL_MESSAGE("source",
      std::string("castor.conf"),
      params.getTapeFlushMode().source);
  }

  void testDetermineTapeFlushModeLocalCastorConfONE_FLUSH_PER_N_FILES() {
    castor::tape::tapebridge::TapeFlushConfigParams params;

    CPPUNIT_ASSERT_EQUAL_MESSAGE("setenv PATH_CONFIG",
      0,
      setenv("PATH_CONFIG",
        "determineTapeFlushModeONE_FLUSH_PER_N_FILES_castor.conf", 1));

    CPPUNIT_ASSERT_NO_THROW_MESSAGE(
      "determineTapeFlushMode",
      params.determineTapeFlushMode());

    CPPUNIT_ASSERT_EQUAL_MESSAGE("category",
      std::string("TAPEBRIDGE"),
      params.getTapeFlushMode().category);

    CPPUNIT_ASSERT_EQUAL_MESSAGE("name",
      std::string("TAPEFLUSHMODE"),
      params.getTapeFlushMode().name);

    CPPUNIT_ASSERT_EQUAL_MESSAGE("value",
      TAPEBRIDGE_ONE_FLUSH_PER_N_FILES,
      params.getTapeFlushMode().value);

    CPPUNIT_ASSERT_EQUAL_MESSAGE("source",
      std::string("castor.conf"),
      params.getTapeFlushMode().source);
  }

  void testDetermineMaxBytesBeforeFlushInvalidEnv() {
    castor::tape::tapebridge::TapeFlushConfigParams params;

    CPPUNIT_ASSERT_EQUAL_MESSAGE("setenv TAPEBRIDGE_MAXBYTESBEFOREFLUSH",
      0,
      setenv("TAPEBRIDGE_MAXBYTESBEFOREFLUSH", "INVALID_UINT_VALUE", 1));

    CPPUNIT_ASSERT_THROW_MESSAGE("Invalid environment variable",
      params.determineMaxBytesBeforeFlush(),
      castor::exception::InvalidArgument);

    std::string exceptionMsg = "UNKNOWN";
    try {
      params.determineMaxBytesBeforeFlush();
    } catch(castor::exception::InvalidArgument &ex) {
      exceptionMsg = ex.getMessage().str();
    }
    CPPUNIT_ASSERT_EQUAL_MESSAGE("exceptionMsg",
      std::string("Configuration parameter is not a valid unsigned integer"
        ": category=TAPEBRIDGE"
        " name=MAXBYTESBEFOREFLUSH"
        " value=INVALID_UINT_VALUE"
        " source=environment variable"),
      exceptionMsg);
  }

  void testDetermineMaxBytesBeforeFlushEnv() {
    castor::tape::tapebridge::TapeFlushConfigParams params;

    CPPUNIT_ASSERT_EQUAL_MESSAGE("setenv TAPEBRIDGE_MAXBYTESBEFOREFLUSH",
      0,
      setenv("TAPEBRIDGE_MAXBYTESBEFOREFLUSH", "11111", 1));

    CPPUNIT_ASSERT_NO_THROW_MESSAGE(
      "determineMaxBytesBeforeFlush",
      params.determineMaxBytesBeforeFlush());

    CPPUNIT_ASSERT_EQUAL_MESSAGE("category",
      std::string("TAPEBRIDGE"),
      params.getMaxBytesBeforeFlush().category);

    CPPUNIT_ASSERT_EQUAL_MESSAGE("name",
      std::string("MAXBYTESBEFOREFLUSH"),
      params.getMaxBytesBeforeFlush().name);

    CPPUNIT_ASSERT_EQUAL_MESSAGE("value",
      (uint64_t)11111,
      params.getMaxBytesBeforeFlush().value);

    CPPUNIT_ASSERT_EQUAL_MESSAGE("source",
      std::string("environment variable"),
      params.getMaxBytesBeforeFlush().source);
  }

  void testDetermineMaxBytesBeforeFlushInvalidPathConfig() {
    castor::tape::tapebridge::TapeFlushConfigParams params;

    CPPUNIT_ASSERT_EQUAL_MESSAGE("setenv PATH_CONFIG",
      0,
      setenv("PATH_CONFIG", "INVALID PATH CONFIG", 1));

    CPPUNIT_ASSERT_NO_THROW_MESSAGE("determineMaxBytesBeforeFlush",
      params.determineMaxBytesBeforeFlush());

    CPPUNIT_ASSERT_EQUAL_MESSAGE("category",
      std::string("TAPEBRIDGE"),
      params.getMaxBytesBeforeFlush().category);

    CPPUNIT_ASSERT_EQUAL_MESSAGE("name",
      std::string("MAXBYTESBEFOREFLUSH"),
      params.getMaxBytesBeforeFlush().name);

    CPPUNIT_ASSERT_EQUAL_MESSAGE("value",
      castor::tape::tapebridge::TAPEBRIDGE_MAXBYTESBEFOREFLUSH,
      params.getMaxBytesBeforeFlush().value);

    CPPUNIT_ASSERT_EQUAL_MESSAGE("source",
      std::string("compile-time default"),
      params.getMaxBytesBeforeFlush().source);
  }

  void testDetermineMaxBytesBeforeFlushInvalidLocalCastorConf() {
    castor::tape::tapebridge::TapeFlushConfigParams params;

    CPPUNIT_ASSERT_EQUAL_MESSAGE("setenv PATH_CONFIG",
      0,
      setenv("PATH_CONFIG", "determineMaxBytesBeforeFlush_invalid_castor.conf",
        1));

    CPPUNIT_ASSERT_THROW_MESSAGE("Invalid local castor.conf variable",
      params.determineMaxBytesBeforeFlush(),
      castor::exception::InvalidArgument);

    std::string exceptionMsg = "UNKNOWN";
    try {
      params.determineMaxBytesBeforeFlush();
    } catch(castor::exception::InvalidArgument &ex) {
      exceptionMsg = ex.getMessage().str();
    }
    CPPUNIT_ASSERT_EQUAL_MESSAGE("exceptionMsg",
      std::string("Configuration parameter is not a valid unsigned integer"
        ": category=TAPEBRIDGE"
        " name=MAXBYTESBEFOREFLUSH"
        " value=INVALID_UINT_VALUE"
        " source=castor.conf"),
      exceptionMsg);
  }

  void testDetermineMaxBytesBeforeFlushLocalCastorConf() {
    castor::tape::tapebridge::TapeFlushConfigParams params;

    CPPUNIT_ASSERT_EQUAL_MESSAGE("setenv PATH_CONFIG",
      0,
      setenv("PATH_CONFIG", "determineMaxBytesBeforeFlush_castor.conf", 1));

    CPPUNIT_ASSERT_NO_THROW_MESSAGE(
      "determineMaxBytesBeforeFlush",
      params.determineMaxBytesBeforeFlush());

    CPPUNIT_ASSERT_EQUAL_MESSAGE("category",
      std::string("TAPEBRIDGE"),
      params.getMaxBytesBeforeFlush().category);

    CPPUNIT_ASSERT_EQUAL_MESSAGE("name",
      std::string("MAXBYTESBEFOREFLUSH"),
      params.getMaxBytesBeforeFlush().name);

    CPPUNIT_ASSERT_EQUAL_MESSAGE("value",
      (uint64_t)33333,
      params.getMaxBytesBeforeFlush().value);

    CPPUNIT_ASSERT_EQUAL_MESSAGE("source",
      std::string("castor.conf"),
      params.getMaxBytesBeforeFlush().source);
  }

  void testDetermineMaxFilesBeforeFlushInvalidEnv() {
    castor::tape::tapebridge::TapeFlushConfigParams params;

    CPPUNIT_ASSERT_EQUAL_MESSAGE("setenv TAPEBRIDGE_MAXFILESBEFOREFLUSH",
      0,
      setenv("TAPEBRIDGE_MAXFILESBEFOREFLUSH", "INVALID_UINT_VALUE", 1));

    CPPUNIT_ASSERT_THROW_MESSAGE("Invalid environment variable",
      params.determineMaxFilesBeforeFlush(),
      castor::exception::InvalidArgument);

    std::string exceptionMsg = "UNKNOWN";
    try {
      params.determineMaxFilesBeforeFlush();
    } catch(castor::exception::InvalidArgument &ex) {
      exceptionMsg = ex.getMessage().str();
    }
    CPPUNIT_ASSERT_EQUAL_MESSAGE("exceptionMsg",
      std::string("Configuration parameter is not a valid unsigned integer"
        ": category=TAPEBRIDGE"
        " name=MAXFILESBEFOREFLUSH"
        " value=INVALID_UINT_VALUE"
        " source=environment variable"),
      exceptionMsg);
  }

  void testDetermineMaxFilesBeforeFlushEnv() {
    castor::tape::tapebridge::TapeFlushConfigParams params;

    CPPUNIT_ASSERT_EQUAL_MESSAGE("setenv TAPEBRIDGE_MAXFILESBEFOREFLUSH",
      0,
      setenv("TAPEBRIDGE_MAXFILESBEFOREFLUSH", "11111", 1));

    CPPUNIT_ASSERT_NO_THROW_MESSAGE(
      "determineMaxFilesBeforeFlush",
      params.determineMaxFilesBeforeFlush());

    CPPUNIT_ASSERT_EQUAL_MESSAGE("category",
      std::string("TAPEBRIDGE"),
      params.getMaxFilesBeforeFlush().category);

    CPPUNIT_ASSERT_EQUAL_MESSAGE("name",
      std::string("MAXFILESBEFOREFLUSH"),
      params.getMaxFilesBeforeFlush().name);

    CPPUNIT_ASSERT_EQUAL_MESSAGE("value",
      (uint64_t)11111,
      params.getMaxFilesBeforeFlush().value);

    CPPUNIT_ASSERT_EQUAL_MESSAGE("source",
      std::string("environment variable"),
      params.getMaxFilesBeforeFlush().source);
  }

  void testDetermineMaxFilesBeforeFlushInvalidPathConfig() {
    castor::tape::tapebridge::TapeFlushConfigParams params;

    CPPUNIT_ASSERT_EQUAL_MESSAGE("setenv PATH_CONFIG",
      0,
      setenv("PATH_CONFIG", "INVALID PATH CONFIG", 1));

    CPPUNIT_ASSERT_NO_THROW_MESSAGE("determineMaxFilesBeforeFlush",
      params.determineMaxFilesBeforeFlush());

    CPPUNIT_ASSERT_EQUAL_MESSAGE("category",
      std::string("TAPEBRIDGE"),
      params.getMaxFilesBeforeFlush().category);

    CPPUNIT_ASSERT_EQUAL_MESSAGE("name",
      std::string("MAXFILESBEFOREFLUSH"),
      params.getMaxFilesBeforeFlush().name);

    CPPUNIT_ASSERT_EQUAL_MESSAGE("value",
      castor::tape::tapebridge::TAPEBRIDGE_MAXFILESBEFOREFLUSH,
      params.getMaxFilesBeforeFlush().value);

    CPPUNIT_ASSERT_EQUAL_MESSAGE("source",
      std::string("compile-time default"),
      params.getMaxFilesBeforeFlush().source);
  }

  void testDetermineMaxFilesBeforeFlushInvalidLocalCastorConf() {
    castor::tape::tapebridge::TapeFlushConfigParams params;

    CPPUNIT_ASSERT_EQUAL_MESSAGE("setenv PATH_CONFIG",
      0,
      setenv("PATH_CONFIG", "determineMaxFilesBeforeFlush_invalid_castor.conf",
        1));

    CPPUNIT_ASSERT_THROW_MESSAGE("Invalid local castor.conf variable",
      params.determineMaxFilesBeforeFlush(),
      castor::exception::InvalidArgument);

    std::string exceptionMsg = "UNKNOWN";
    try {
      params.determineMaxFilesBeforeFlush();
    } catch(castor::exception::InvalidArgument &ex) {
      exceptionMsg = ex.getMessage().str();
    }
    CPPUNIT_ASSERT_EQUAL_MESSAGE("exceptionMsg",
      std::string("Configuration parameter is not a valid unsigned integer"
        ": category=TAPEBRIDGE"
        " name=MAXFILESBEFOREFLUSH"
        " value=INVALID_UINT_VALUE"
        " source=castor.conf"),
      exceptionMsg);
  }

  void testDetermineMaxFilesBeforeFlushLocalCastorConf() {
    castor::tape::tapebridge::TapeFlushConfigParams params;

    CPPUNIT_ASSERT_EQUAL_MESSAGE("setenv PATH_CONFIG",
      0,
      setenv("PATH_CONFIG", "determineMaxFilesBeforeFlush_castor.conf", 1));

    CPPUNIT_ASSERT_NO_THROW_MESSAGE(
      "determineMaxFilesBeforeFlush",
      params.determineMaxFilesBeforeFlush());

    CPPUNIT_ASSERT_EQUAL_MESSAGE("category",
      std::string("TAPEBRIDGE"),
      params.getMaxFilesBeforeFlush().category);

    CPPUNIT_ASSERT_EQUAL_MESSAGE("name",
      std::string("MAXFILESBEFOREFLUSH"),
      params.getMaxFilesBeforeFlush().name);

    CPPUNIT_ASSERT_EQUAL_MESSAGE("value",
      (uint64_t)44444,
      params.getMaxFilesBeforeFlush().value);

    CPPUNIT_ASSERT_EQUAL_MESSAGE("source",
      std::string("castor.conf"),
      params.getMaxFilesBeforeFlush().source);
  }

  void testDetermineTapeFlushConfigParamsCastorConf() {
    castor::tape::tapebridge::TapeFlushConfigParams params;

    CPPUNIT_ASSERT_EQUAL_MESSAGE("setenv PATH_CONFIG",
      0,
      setenv("PATH_CONFIG", "determineTapeFlushConfigParams_castor.conf", 1));

    CPPUNIT_ASSERT_NO_THROW_MESSAGE(
      "determineTapeFlushConfigParams",
      params.determineTapeFlushConfigParams());

    CPPUNIT_ASSERT_EQUAL_MESSAGE("category",
      std::string("TAPEBRIDGE"),
      params.getMaxBytesBeforeFlush().category);

    CPPUNIT_ASSERT_EQUAL_MESSAGE("name",
      std::string("MAXBYTESBEFOREFLUSH"),
      params.getMaxBytesBeforeFlush().name);

    CPPUNIT_ASSERT_EQUAL_MESSAGE("value",
      (uint64_t)12345,
      params.getMaxBytesBeforeFlush().value);

    CPPUNIT_ASSERT_EQUAL_MESSAGE("source",
      std::string("castor.conf"),
      params.getMaxBytesBeforeFlush().source);

    CPPUNIT_ASSERT_EQUAL_MESSAGE("category",
      std::string("TAPEBRIDGE"),
      params.getMaxFilesBeforeFlush().category);

    CPPUNIT_ASSERT_EQUAL_MESSAGE("name",
      std::string("MAXFILESBEFOREFLUSH"),
      params.getMaxFilesBeforeFlush().name);

    CPPUNIT_ASSERT_EQUAL_MESSAGE("value",
      (uint64_t)67890,
      params.getMaxFilesBeforeFlush().value);

    CPPUNIT_ASSERT_EQUAL_MESSAGE("source",
      std::string("castor.conf"),
      params.getMaxFilesBeforeFlush().source);

    CPPUNIT_ASSERT_EQUAL_MESSAGE("category",
      std::string("TAPEBRIDGE"),
      params.getTapeFlushMode().category);

    CPPUNIT_ASSERT_EQUAL_MESSAGE("name",
      std::string("TAPEFLUSHMODE"),
      params.getTapeFlushMode().name);

    CPPUNIT_ASSERT_EQUAL_MESSAGE("value",
      TAPEBRIDGE_ONE_FLUSH_PER_N_FILES,
      params.getTapeFlushMode().value);

    CPPUNIT_ASSERT_EQUAL_MESSAGE("source",
      std::string("castor.conf"),
      params.getTapeFlushMode().source);
  }

  CPPUNIT_TEST_SUITE(TapeFlushConfigParamsTest);

  CPPUNIT_TEST(testDetermineTapeFlushModeInvalidEnv);
  CPPUNIT_TEST(testDetermineTapeFlushModeEnvN_FLUSHES_PER_FILE);
  CPPUNIT_TEST(testDetermineTapeFlushModeEnvONE_FLUSH_PER_FILE);
  CPPUNIT_TEST(testDetermineTapeFlushModeEnvONE_FLUSH_PER_N_FILES);
  CPPUNIT_TEST(testDetermineTapeFlushModeInvalidPathConfig);
  CPPUNIT_TEST(testDetermineTapeFlushModeInvalidLocalCastorConf);
  CPPUNIT_TEST(testDetermineTapeFlushModeLocalCastorConfN_FLUSHES_PER_FILE);
  CPPUNIT_TEST(testDetermineTapeFlushModeLocalCastorConfONE_FLUSH_PER_FILE);
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

#endif // TEST_UNITTEST_CASTOR_TAPE_TAPEBRIDGE_TAPEFLUSHCONFIGPARAMSTEST_HPP
