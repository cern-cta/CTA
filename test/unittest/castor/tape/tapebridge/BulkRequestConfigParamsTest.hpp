/******************************************************************************
 *    test/unittest/castor/tape/tapebridge/BulkRequestConfigParamsTest.hpp
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

#ifndef TEST_UNITTEST_CASTOR_TAPE_TAPEBRIDGE_BULKREQUESTCONFIGPARAMSTEST_HPP
#define TEST_UNITTEST_CASTOR_TAPE_TAPEBRIDGE_BULKREQUESTCONFIGPARAMSTEST_HPP 1

#include "castor/exception/InvalidArgument.hpp"
#include "castor/tape/tapebridge/BulkRequestConfigParams.hpp"
#include "castor/tape/tapebridge/Constants.hpp"
#include "h/rtcpd_constants.h"
#include "h/tapebridge_constants.h"
#include "test/unittest/castor/tape/tapebridge/TestingBulkRequestConfigParams.hpp"

#include <cppunit/extensions/HelperMacros.h>
#include <exception>
#include <stdint.h>
#include <stdlib.h>

namespace castor     {
namespace tape       {
namespace tapebridge {

class BulkRequestConfigParamsTest: public CppUnit::TestFixture {
public:

  BulkRequestConfigParamsTest() {
  }

  void setUp() {
    setenv("PATH_CONFIG", "/etc/castor/castor.conf", 1);
    unsetenv("TAPEBRIDGE_BULKREQUESTMIGRATIONMAXBYTES");
    unsetenv("TAPEBRIDGE_BULKREQUESTMIGRATIONMAXFILES");
    unsetenv("TAPEBRIDGE_BULKREQUESTRECALLMAXBYTES");
    unsetenv("TAPEBRIDGE_BULKREQUESTRECALLMAXFILES");
  }

  void tearDown() {
    setUp();
  }

  void testDetermineBulkRequestMigrationMaxBytesInvalidEnv() {
    TestingBulkRequestConfigParams params;

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "setenv TAPEBRIDGE_BULKREQUESTMIGRATIONMAXBYTES",
      0,
      setenv("TAPEBRIDGE_BULKREQUESTMIGRATIONMAXBYTES",
        "INVALID_UINT_VALUE", 1));

    CPPUNIT_ASSERT_THROW_MESSAGE("Invalid environment variable",
      params.determineBulkRequestMigrationMaxBytes(),
      castor::exception::InvalidArgument);
  }

  void testDetermineBulkRequestMigrationMaxBytesEnv() {
    TestingBulkRequestConfigParams params;

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "setenv TAPEBRIDGE_BULKREQUESTMIGRATIONMAXBYTES",
      0,
      setenv("TAPEBRIDGE_BULKREQUESTMIGRATIONMAXBYTES", "10000", 1));

    CPPUNIT_ASSERT_NO_THROW_MESSAGE(
      "determineBulkRequestMigrationMaxBytes",
      params.determineBulkRequestMigrationMaxBytes());

    CPPUNIT_ASSERT_EQUAL_MESSAGE("getCategory()",
      std::string("TAPEBRIDGE"),
      params.getBulkRequestMigrationMaxBytes().getCategory());

    CPPUNIT_ASSERT_EQUAL_MESSAGE("getName()",
      std::string("BULKREQUESTMIGRATIONMAXBYTES"),
      params.getBulkRequestMigrationMaxBytes().getName());

    CPPUNIT_ASSERT_EQUAL_MESSAGE("getValue()",
      (uint64_t)10000,
      params.getBulkRequestMigrationMaxBytes().getValue());

    CPPUNIT_ASSERT_EQUAL_MESSAGE("getSource()",
      ConfigParamSource::ENVIRONMENT_VARIABLE,
      params.getBulkRequestMigrationMaxBytes().getSource());
  }

  void testDetermineBulkRequestMigrationMaxBytesInvalidPathConfig() {
    TestingBulkRequestConfigParams params;

    CPPUNIT_ASSERT_EQUAL_MESSAGE("setenv PATH_CONFIG",
      0,
      setenv("PATH_CONFIG", "INVALID PATH CONFIG", 1));

    CPPUNIT_ASSERT_NO_THROW_MESSAGE("determineBulkRequestMigrationMaxBytes",
      params.determineBulkRequestMigrationMaxBytes());

    CPPUNIT_ASSERT_EQUAL_MESSAGE("getCategory()",
      std::string("TAPEBRIDGE"),
      params.getBulkRequestMigrationMaxBytes().getCategory());

    CPPUNIT_ASSERT_EQUAL_MESSAGE("getName()",
      std::string("BULKREQUESTMIGRATIONMAXBYTES"),
      params.getBulkRequestMigrationMaxBytes().getName());

    CPPUNIT_ASSERT_EQUAL_MESSAGE("getValue()",
      castor::tape::tapebridge::TAPEBRIDGE_BULKREQUESTMIGRATIONMAXBYTES,
      params.getBulkRequestMigrationMaxBytes().getValue());

    CPPUNIT_ASSERT_EQUAL_MESSAGE("getSource()",
      ConfigParamSource::COMPILE_TIME_DEFAULT,
      params.getBulkRequestMigrationMaxBytes().getSource());
  }

  void testDetermineBulkRequestMigrationMaxBytesInvalidLocalCastorConf() {
    TestingBulkRequestConfigParams params;

    CPPUNIT_ASSERT_EQUAL_MESSAGE("setenv PATH_CONFIG",
      0,
      setenv(
        "PATH_CONFIG",
        "determineBulkRequestMigrationMaxBytes_invalid_castor.conf",
        1));

    CPPUNIT_ASSERT_THROW_MESSAGE("Invalid local castor.conf variable",
      params.determineBulkRequestMigrationMaxBytes(),
      castor::exception::InvalidArgument);
  }

  void testDetermineBulkRequestMigrationMaxBytesLocalCastorConf() {
    TestingBulkRequestConfigParams params;

    CPPUNIT_ASSERT_EQUAL_MESSAGE("setenv PATH_CONFIG",
      0,
      setenv(
        "PATH_CONFIG",
        "determineBulkRequestMigrationMaxBytes_castor.conf",
        1));

    CPPUNIT_ASSERT_NO_THROW_MESSAGE(
      "determineBulkRequestMigrationMaxBytes",
      params.determineBulkRequestMigrationMaxBytes());

    CPPUNIT_ASSERT_EQUAL_MESSAGE("getCategory()",
      std::string("TAPEBRIDGE"),
      params.getBulkRequestMigrationMaxBytes().getCategory());

    CPPUNIT_ASSERT_EQUAL_MESSAGE("getName()",
      std::string("BULKREQUESTMIGRATIONMAXBYTES"),
      params.getBulkRequestMigrationMaxBytes().getName());

    CPPUNIT_ASSERT_EQUAL_MESSAGE("getValue()",
      (uint64_t)11111,
      params.getBulkRequestMigrationMaxBytes().getValue());

    CPPUNIT_ASSERT_EQUAL_MESSAGE("getSource()",
      ConfigParamSource::CASTOR_CONF,
      params.getBulkRequestMigrationMaxBytes().getSource());
  }

  void testDetermineBulkRequestMigrationMaxFilesInvalidEnv() {
    TestingBulkRequestConfigParams params;

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "setenv TAPEBRIDGE_BULKREQUESTMIGRATIONMAXFILES",
      0,
      setenv("TAPEBRIDGE_BULKREQUESTMIGRATIONMAXFILES",
        "INVALID_UINT_VALUE", 1));

    CPPUNIT_ASSERT_THROW_MESSAGE("Invalid environment variable",
      params.determineBulkRequestMigrationMaxFiles(),
      castor::exception::InvalidArgument);
  }

  void testDetermineBulkRequestMigrationMaxFilesEnv() {
    TestingBulkRequestConfigParams params;

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "setenv TAPEBRIDGE_BULKREQUESTMIGRATIONMAXFILES",
      0,
      setenv("TAPEBRIDGE_BULKREQUESTMIGRATIONMAXFILES", "12222", 1));

    CPPUNIT_ASSERT_NO_THROW_MESSAGE(
      "determineBulkRequestMigrationMaxFiles",
      params.determineBulkRequestMigrationMaxFiles());

    CPPUNIT_ASSERT_EQUAL_MESSAGE("getCategory()",
      std::string("TAPEBRIDGE"),
      params.getBulkRequestMigrationMaxFiles().getCategory());

    CPPUNIT_ASSERT_EQUAL_MESSAGE("getName()",
      std::string("BULKREQUESTMIGRATIONMAXFILES"),
      params.getBulkRequestMigrationMaxFiles().getName());

    CPPUNIT_ASSERT_EQUAL_MESSAGE("getValue()",
      (uint64_t)12222,
      params.getBulkRequestMigrationMaxFiles().getValue());

    CPPUNIT_ASSERT_EQUAL_MESSAGE("getSource()",
      ConfigParamSource::ENVIRONMENT_VARIABLE,
      params.getBulkRequestMigrationMaxFiles().getSource());
  }

  void testDetermineBulkRequestMigrationMaxFilesInvalidPathConfig() {
    TestingBulkRequestConfigParams params;

    CPPUNIT_ASSERT_EQUAL_MESSAGE("setenv PATH_CONFIG",
      0,
      setenv("PATH_CONFIG", "INVALID PATH CONFIG", 1));

    CPPUNIT_ASSERT_NO_THROW_MESSAGE("determineBulkRequestMigrationMaxFiles",
      params.determineBulkRequestMigrationMaxFiles());

    CPPUNIT_ASSERT_EQUAL_MESSAGE("getCategory()",
      std::string("TAPEBRIDGE"),
      params.getBulkRequestMigrationMaxFiles().getCategory());

    CPPUNIT_ASSERT_EQUAL_MESSAGE("getName()",
      std::string("BULKREQUESTMIGRATIONMAXFILES"),
      params.getBulkRequestMigrationMaxFiles().getName());

    CPPUNIT_ASSERT_EQUAL_MESSAGE("getValue()",
      castor::tape::tapebridge::TAPEBRIDGE_BULKREQUESTMIGRATIONMAXFILES,
      params.getBulkRequestMigrationMaxFiles().getValue());

    CPPUNIT_ASSERT_EQUAL_MESSAGE("getSource()",
      ConfigParamSource::COMPILE_TIME_DEFAULT,
      params.getBulkRequestMigrationMaxFiles().getSource());
  }

  void testDetermineBulkRequestMigrationMaxFilesInvalidLocalCastorConf() {
    TestingBulkRequestConfigParams params;

    CPPUNIT_ASSERT_EQUAL_MESSAGE("setenv PATH_CONFIG",
      0,
      setenv(
        "PATH_CONFIG",
        "determineBulkRequestMigrationMaxFiles_invalid_castor.conf",
        1));

    CPPUNIT_ASSERT_THROW_MESSAGE("Invalid local castor.conf variable",
      params.determineBulkRequestMigrationMaxFiles(),
      castor::exception::InvalidArgument);
  }

  void testDetermineBulkRequestMigrationMaxFilesLocalCastorConf() {
    TestingBulkRequestConfigParams params;

    CPPUNIT_ASSERT_EQUAL_MESSAGE("setenv PATH_CONFIG",
      0,
      setenv(
        "PATH_CONFIG",
        "determineBulkRequestMigrationMaxFiles_castor.conf",
        1));

    CPPUNIT_ASSERT_NO_THROW_MESSAGE(
      "determineBulkRequestMigrationMaxFiles",
      params.determineBulkRequestMigrationMaxFiles());

    CPPUNIT_ASSERT_EQUAL_MESSAGE("getCategory()",
      std::string("TAPEBRIDGE"),
      params.getBulkRequestMigrationMaxFiles().getCategory());

    CPPUNIT_ASSERT_EQUAL_MESSAGE("getName()",
      std::string("BULKREQUESTMIGRATIONMAXFILES"),
      params.getBulkRequestMigrationMaxFiles().getName());

    CPPUNIT_ASSERT_EQUAL_MESSAGE("getValue()",
      (uint64_t)13333,
      params.getBulkRequestMigrationMaxFiles().getValue());

    CPPUNIT_ASSERT_EQUAL_MESSAGE("getSource()",
      ConfigParamSource::CASTOR_CONF,
      params.getBulkRequestMigrationMaxFiles().getSource());
  }

  void testDetermineBulkRequestRecallMaxBytesInvalidEnv() {
    TestingBulkRequestConfigParams params;

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "setenv TAPEBRIDGE_BULKREQUESTRECALLMAXBYTES",
      0,
      setenv("TAPEBRIDGE_BULKREQUESTRECALLMAXBYTES",
        "INVALID_UINT_VALUE", 1));

    CPPUNIT_ASSERT_THROW_MESSAGE("Invalid environment variable",
      params.determineBulkRequestRecallMaxBytes(),
      castor::exception::InvalidArgument);
  }

  void testDetermineBulkRequestRecallMaxBytesEnv() {
    TestingBulkRequestConfigParams params;

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "setenv TAPEBRIDGE_BULKREQUESTRECALLMAXBYTES",
      0,
      setenv("TAPEBRIDGE_BULKREQUESTRECALLMAXBYTES", "14444", 1));

    CPPUNIT_ASSERT_NO_THROW_MESSAGE(
      "determineBulkRequestRecallMaxBytes",
      params.determineBulkRequestRecallMaxBytes());

    CPPUNIT_ASSERT_EQUAL_MESSAGE("getCategory()",
      std::string("TAPEBRIDGE"),
      params.getBulkRequestRecallMaxBytes().getCategory());

    CPPUNIT_ASSERT_EQUAL_MESSAGE("getName()",
      std::string("BULKREQUESTRECALLMAXBYTES"),
      params.getBulkRequestRecallMaxBytes().getName());

    CPPUNIT_ASSERT_EQUAL_MESSAGE("getValue()",
      (uint64_t)14444,
      params.getBulkRequestRecallMaxBytes().getValue());

    CPPUNIT_ASSERT_EQUAL_MESSAGE("getSource()",
      ConfigParamSource::ENVIRONMENT_VARIABLE,
      params.getBulkRequestRecallMaxBytes().getSource());
  }

  void testDetermineBulkRequestRecallMaxBytesInvalidPathConfig() {
    TestingBulkRequestConfigParams params;

    CPPUNIT_ASSERT_EQUAL_MESSAGE("setenv PATH_CONFIG",
      0,
      setenv("PATH_CONFIG", "INVALID PATH CONFIG", 1));

    CPPUNIT_ASSERT_NO_THROW_MESSAGE("determineBulkRequestRecallMaxBytes",
      params.determineBulkRequestRecallMaxBytes());

    CPPUNIT_ASSERT_EQUAL_MESSAGE("getCategory()",
      std::string("TAPEBRIDGE"),
      params.getBulkRequestRecallMaxBytes().getCategory());

    CPPUNIT_ASSERT_EQUAL_MESSAGE("getName()",
      std::string("BULKREQUESTRECALLMAXBYTES"),
      params.getBulkRequestRecallMaxBytes().getName());

    CPPUNIT_ASSERT_EQUAL_MESSAGE("getValue()",
      castor::tape::tapebridge::TAPEBRIDGE_BULKREQUESTRECALLMAXBYTES,
      params.getBulkRequestRecallMaxBytes().getValue());

    CPPUNIT_ASSERT_EQUAL_MESSAGE("getSource()",
      ConfigParamSource::COMPILE_TIME_DEFAULT,
      params.getBulkRequestRecallMaxBytes().getSource());
  }

  void testDetermineBulkRequestRecallMaxBytesInvalidLocalCastorConf() {
    TestingBulkRequestConfigParams params;

    CPPUNIT_ASSERT_EQUAL_MESSAGE("setenv PATH_CONFIG",
      0,
      setenv(
        "PATH_CONFIG",
        "determineBulkRequestRecallMaxBytes_invalid_castor.conf",
        1));

    CPPUNIT_ASSERT_THROW_MESSAGE("Invalid local castor.conf variable",
      params.determineBulkRequestRecallMaxBytes(),
      castor::exception::InvalidArgument);
  }

  void testDetermineBulkRequestRecallMaxBytesLocalCastorConf() {
    TestingBulkRequestConfigParams params;

    CPPUNIT_ASSERT_EQUAL_MESSAGE("setenv PATH_CONFIG",
      0,
      setenv(
        "PATH_CONFIG",
        "determineBulkRequestRecallMaxBytes_castor.conf",
        1));

    CPPUNIT_ASSERT_NO_THROW_MESSAGE(
      "determineBulkRequestRecallMaxBytes",
      params.determineBulkRequestRecallMaxBytes());

    CPPUNIT_ASSERT_EQUAL_MESSAGE("getCategory()",
      std::string("TAPEBRIDGE"),
      params.getBulkRequestRecallMaxBytes().getCategory());

    CPPUNIT_ASSERT_EQUAL_MESSAGE("getName()",
      std::string("BULKREQUESTRECALLMAXBYTES"),
      params.getBulkRequestRecallMaxBytes().getName());

    CPPUNIT_ASSERT_EQUAL_MESSAGE("getValue()",
      (uint64_t)15555,
      params.getBulkRequestRecallMaxBytes().getValue());

    CPPUNIT_ASSERT_EQUAL_MESSAGE("getSource()",
      ConfigParamSource::CASTOR_CONF,
      params.getBulkRequestRecallMaxBytes().getSource());
  }

  void testDetermineBulkRequestRecallMaxFilesInvalidEnv() {
    TestingBulkRequestConfigParams params;

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "setenv TAPEBRIDGE_BULKREQUESTRECALLMAXFILES",
      0,
      setenv("TAPEBRIDGE_BULKREQUESTRECALLMAXFILES",
        "INVALID_UINT_VALUE", 1));

    CPPUNIT_ASSERT_THROW_MESSAGE("Invalid environment variable",
      params.determineBulkRequestRecallMaxFiles(),
      castor::exception::InvalidArgument);
  }

  void testDetermineBulkRequestRecallMaxFilesEnv() {
    TestingBulkRequestConfigParams params;

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "setenv TAPEBRIDGE_BULKREQUESTRECALLMAXFILES",
      0,
      setenv("TAPEBRIDGE_BULKREQUESTRECALLMAXFILES", "16666", 1));

    CPPUNIT_ASSERT_NO_THROW_MESSAGE(
      "determineBulkRequestRecallMaxFiles",
      params.determineBulkRequestRecallMaxFiles());

    CPPUNIT_ASSERT_EQUAL_MESSAGE("getCategory()",
      std::string("TAPEBRIDGE"),
      params.getBulkRequestRecallMaxFiles().getCategory());

    CPPUNIT_ASSERT_EQUAL_MESSAGE("getName()",
      std::string("BULKREQUESTRECALLMAXFILES"),
      params.getBulkRequestRecallMaxFiles().getName());

    CPPUNIT_ASSERT_EQUAL_MESSAGE("getValue()",
      (uint64_t)16666,
      params.getBulkRequestRecallMaxFiles().getValue());

    CPPUNIT_ASSERT_EQUAL_MESSAGE("getSource()",
      ConfigParamSource::ENVIRONMENT_VARIABLE,
      params.getBulkRequestRecallMaxFiles().getSource());
  }

  void testDetermineBulkRequestRecallMaxFilesInvalidPathConfig() {
    TestingBulkRequestConfigParams params;

    CPPUNIT_ASSERT_EQUAL_MESSAGE("setenv PATH_CONFIG",
      0,
      setenv("PATH_CONFIG", "INVALID PATH CONFIG", 1));

    CPPUNIT_ASSERT_NO_THROW_MESSAGE("determineBulkRequestRecallMaxFiles",
      params.determineBulkRequestRecallMaxFiles());

    CPPUNIT_ASSERT_EQUAL_MESSAGE("getCategory()",
      std::string("TAPEBRIDGE"),
      params.getBulkRequestRecallMaxFiles().getCategory());

    CPPUNIT_ASSERT_EQUAL_MESSAGE("getName()",
      std::string("BULKREQUESTRECALLMAXFILES"),
      params.getBulkRequestRecallMaxFiles().getName());

    CPPUNIT_ASSERT_EQUAL_MESSAGE("getValue()",
      castor::tape::tapebridge::TAPEBRIDGE_BULKREQUESTRECALLMAXFILES,
      params.getBulkRequestRecallMaxFiles().getValue());

    CPPUNIT_ASSERT_EQUAL_MESSAGE("getSource()",
      ConfigParamSource::COMPILE_TIME_DEFAULT,
      params.getBulkRequestRecallMaxFiles().getSource());
  }

  void testDetermineBulkRequestRecallMaxFilesInvalidLocalCastorConf() {
    TestingBulkRequestConfigParams params;

    CPPUNIT_ASSERT_EQUAL_MESSAGE("setenv PATH_CONFIG",
      0,
      setenv(
        "PATH_CONFIG",
        "determineBulkRequestRecallMaxFiles_invalid_castor.conf",
        1));

    CPPUNIT_ASSERT_THROW_MESSAGE("Invalid local castor.conf variable",
      params.determineBulkRequestRecallMaxFiles(),
      castor::exception::InvalidArgument);
  }

  void testDetermineBulkRequestRecallMaxFilesLocalCastorConf() {
    TestingBulkRequestConfigParams params;

    CPPUNIT_ASSERT_EQUAL_MESSAGE("setenv PATH_CONFIG",
      0,
      setenv(
        "PATH_CONFIG",
        "determineBulkRequestRecallMaxFiles_castor.conf",
        1));

    CPPUNIT_ASSERT_NO_THROW_MESSAGE(
      "determineBulkRequestRecallMaxFiles",
      params.determineBulkRequestRecallMaxFiles());

    CPPUNIT_ASSERT_EQUAL_MESSAGE("getCategory()",
      std::string("TAPEBRIDGE"),
      params.getBulkRequestRecallMaxFiles().getCategory());

    CPPUNIT_ASSERT_EQUAL_MESSAGE("getName()",
      std::string("BULKREQUESTRECALLMAXFILES"),
      params.getBulkRequestRecallMaxFiles().getName());

    CPPUNIT_ASSERT_EQUAL_MESSAGE("getValue()",
      (uint64_t)17777,
      params.getBulkRequestRecallMaxFiles().getValue());

    CPPUNIT_ASSERT_EQUAL_MESSAGE("getSource()",
      ConfigParamSource::CASTOR_CONF,
      params.getBulkRequestRecallMaxFiles().getSource());
  }

  void testDetermineBulkRequestConfigParamsCastorConf() {
    BulkRequestConfigParams params;

    CPPUNIT_ASSERT_EQUAL_MESSAGE("setenv PATH_CONFIG",
      0,
      setenv(
        "PATH_CONFIG",
        "determineBulkRequestConfigParams_castor.conf",
        1));

    CPPUNIT_ASSERT_NO_THROW_MESSAGE(
      "determineConfigParams",
      params.determineConfigParams());

    CPPUNIT_ASSERT_EQUAL_MESSAGE("getCategory()",
      std::string("TAPEBRIDGE"),
      params.getBulkRequestMigrationMaxBytes().getCategory());

    CPPUNIT_ASSERT_EQUAL_MESSAGE("getName()",
      std::string("BULKREQUESTMIGRATIONMAXBYTES"),
      params.getBulkRequestMigrationMaxBytes().getName());

    CPPUNIT_ASSERT_EQUAL_MESSAGE("getValue()",
      (uint64_t)18888,
      params.getBulkRequestMigrationMaxBytes().getValue());

    CPPUNIT_ASSERT_EQUAL_MESSAGE("getSource()",
      ConfigParamSource::CASTOR_CONF,
      params.getBulkRequestMigrationMaxBytes().getSource());

    CPPUNIT_ASSERT_EQUAL_MESSAGE("getCategory()",
      std::string("TAPEBRIDGE"),
      params.getBulkRequestMigrationMaxFiles().getCategory());

    CPPUNIT_ASSERT_EQUAL_MESSAGE("getName()",
      std::string("BULKREQUESTMIGRATIONMAXFILES"),
      params.getBulkRequestMigrationMaxFiles().getName());

    CPPUNIT_ASSERT_EQUAL_MESSAGE("getValue()",
      (uint64_t)19999,
      params.getBulkRequestMigrationMaxFiles().getValue());

    CPPUNIT_ASSERT_EQUAL_MESSAGE("getCategory()",
      std::string("TAPEBRIDGE"),
      params.getBulkRequestRecallMaxBytes().getCategory());

    CPPUNIT_ASSERT_EQUAL_MESSAGE("getName()",
      std::string("BULKREQUESTRECALLMAXBYTES"),
      params.getBulkRequestRecallMaxBytes().getName());

    CPPUNIT_ASSERT_EQUAL_MESSAGE("getValue()",
      (uint64_t)20000,
      params.getBulkRequestRecallMaxBytes().getValue());

    CPPUNIT_ASSERT_EQUAL_MESSAGE("getSource()",
      ConfigParamSource::CASTOR_CONF,
      params.getBulkRequestRecallMaxBytes().getSource());

    CPPUNIT_ASSERT_EQUAL_MESSAGE("getCategory()",
      std::string("TAPEBRIDGE"),
      params.getBulkRequestRecallMaxFiles().getCategory());

    CPPUNIT_ASSERT_EQUAL_MESSAGE("getName()",
      std::string("BULKREQUESTRECALLMAXFILES"),
      params.getBulkRequestRecallMaxFiles().getName());

    CPPUNIT_ASSERT_EQUAL_MESSAGE("getValue()",
      (uint64_t)21111,
      params.getBulkRequestRecallMaxFiles().getValue());
  }

  CPPUNIT_TEST_SUITE(BulkRequestConfigParamsTest);

  CPPUNIT_TEST(testDetermineBulkRequestMigrationMaxBytesInvalidEnv);
  CPPUNIT_TEST(testDetermineBulkRequestMigrationMaxBytesEnv);
  CPPUNIT_TEST(testDetermineBulkRequestMigrationMaxBytesInvalidPathConfig);
  CPPUNIT_TEST(testDetermineBulkRequestMigrationMaxBytesInvalidLocalCastorConf);
  CPPUNIT_TEST(testDetermineBulkRequestMigrationMaxBytesLocalCastorConf);

  CPPUNIT_TEST(testDetermineBulkRequestMigrationMaxFilesInvalidEnv);
  CPPUNIT_TEST(testDetermineBulkRequestMigrationMaxFilesEnv);
  CPPUNIT_TEST(testDetermineBulkRequestMigrationMaxFilesInvalidPathConfig);
  CPPUNIT_TEST(testDetermineBulkRequestMigrationMaxFilesInvalidLocalCastorConf);
  CPPUNIT_TEST(testDetermineBulkRequestMigrationMaxFilesLocalCastorConf);

  CPPUNIT_TEST(testDetermineBulkRequestRecallMaxBytesInvalidEnv);
  CPPUNIT_TEST(testDetermineBulkRequestRecallMaxBytesEnv);
  CPPUNIT_TEST(testDetermineBulkRequestRecallMaxBytesInvalidPathConfig);
  CPPUNIT_TEST(testDetermineBulkRequestRecallMaxBytesInvalidLocalCastorConf);
  CPPUNIT_TEST(testDetermineBulkRequestRecallMaxBytesLocalCastorConf);

  CPPUNIT_TEST(testDetermineBulkRequestRecallMaxFilesInvalidEnv);
  CPPUNIT_TEST(testDetermineBulkRequestRecallMaxFilesEnv);
  CPPUNIT_TEST(testDetermineBulkRequestRecallMaxFilesInvalidPathConfig);
  CPPUNIT_TEST(testDetermineBulkRequestRecallMaxFilesInvalidLocalCastorConf);
  CPPUNIT_TEST(testDetermineBulkRequestRecallMaxFilesLocalCastorConf);

  CPPUNIT_TEST(testDetermineBulkRequestConfigParamsCastorConf);

  CPPUNIT_TEST_SUITE_END();
};

CPPUNIT_TEST_SUITE_REGISTRATION(BulkRequestConfigParamsTest);

} // namespace tapebridge
} // namespace tape
} // namespace castor

#endif // TEST_UNITTEST_CASTOR_TAPE_TAPEBRIDGE_BULKREQUESTCONFIGPARAMSTEST_HPP
