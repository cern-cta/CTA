/******************************************************************************
 *                test/castor/tape/tapebridge/VdqmRequestHandlerTest.hpp
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

#ifndef TEST_CASTOR_TAPE_TAPEBRIDGE_VDQMREQUESTHANDLERTEST_HPP
#define TEST_CASTOR_TAPE_TAPEBRIDGE_VDQMREQUESTHANDLERTEST_HPP 1

#include "castor/tape/tapebridge/Constants.hpp"
#include "castor/tape/tapebridge/VdqmRequestHandler.hpp"

#include <cppunit/extensions/HelperMacros.h>
#include <exception>
#include <stdlib.h>

class VdqmRequestHandlerTest: public CppUnit::TestFixture {
public:

  VdqmRequestHandlerTest() {
  }

  void setUp() {
    setenv("PATH_CONFIG", "/etc/castor/castor.conf", 1);
    unsetenv("TAPEBRIDGED_USEBUFFEREDTAPEMARKSOVERMULTIPLEFILES");
  }

  void tearDown() {
    setenv("PATH_CONFIG", "/etc/castor/castor.conf", 1);
    unsetenv("TAPEBRIDGED_USEBUFFEREDTAPEMARKSOVERMULTIPLEFILES");
  }

  void testGetUseBufferedTapeMarksOverMultipleFilesInvalidEnv() {
    const uint32_t nbDrives = 0;
    castor::tape::tapebridge::VdqmRequestHandler handler(nbDrives);
    castor::tape::tapebridge::ConfigParamAndSource<bool>
      useBufferedTapeMarksOverMultipleFiles("UNKNOWN NAME", false,
        "UNKNOWN SOURCE");

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "setenv TAPEBRIDGED_USEBUFFEREDTAPEMARKSOVERMULTIPLEFILES",
      0,
      setenv("TAPEBRIDGED_USEBUFFEREDTAPEMARKSOVERMULTIPLEFILES",
        "INVALID_BOOL_VALUE", 1));

    CPPUNIT_ASSERT_THROW_MESSAGE("Invalid environment variable",
      useBufferedTapeMarksOverMultipleFiles =
        handler.getUseBufferedTapeMarksOverMultipleFiles(),
      castor::exception::InvalidArgument);

    std::string exceptionMsg = "UNKNOWN";
    try {
      useBufferedTapeMarksOverMultipleFiles =
        handler.getUseBufferedTapeMarksOverMultipleFiles();
    } catch(castor::exception::InvalidArgument &ex) {
      exceptionMsg = ex.getMessage().str();
    }
    CPPUNIT_ASSERT_EQUAL_MESSAGE("exceptionMsg",
      std::string("Configuration parameter is not a valid unsigned integer:"
        " name=TAPEBRIDGED/USEBUFFEREDTAPEMARKSOVERMULTIPLEFILES"
        " value=INVALID_BOOL_VALUE"
        " source=environment variable"),
      exceptionMsg);
  }

  void testGetUseBufferedTapeMarksOverMultipleFilesEnvTrue() {
    const uint32_t nbDrives = 0;
    castor::tape::tapebridge::VdqmRequestHandler handler(nbDrives);
    castor::tape::tapebridge::ConfigParamAndSource<bool>
      useBufferedTapeMarksOverMultipleFiles("UNKNOWN NAME", false,
        "UNKNOWN SOURCE");

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "setenv TAPEBRIDGED_USEBUFFEREDTAPEMARKSOVERMULTIPLEFILES",
      0,
      setenv("TAPEBRIDGED_USEBUFFEREDTAPEMARKSOVERMULTIPLEFILES", "true", 1));

    CPPUNIT_ASSERT_NO_THROW_MESSAGE(
      "getUseBufferedTapeMarksOverMultipleFiles",
      useBufferedTapeMarksOverMultipleFiles =
        handler.getUseBufferedTapeMarksOverMultipleFiles());

    CPPUNIT_ASSERT_EQUAL_MESSAGE("useBufferedTapeMarksOverMultipleFiles.name",
      std::string("TAPEBRIDGED/USEBUFFEREDTAPEMARKSOVERMULTIPLEFILES"),
      useBufferedTapeMarksOverMultipleFiles.name);

    CPPUNIT_ASSERT_EQUAL_MESSAGE("useBufferedTapeMarksOverMultipleFiles.value",
      true,
      useBufferedTapeMarksOverMultipleFiles.value);

    CPPUNIT_ASSERT_EQUAL_MESSAGE("useBufferedTapeMarksOverMultipleFiles.source",
      std::string("environment variable"),
      useBufferedTapeMarksOverMultipleFiles.source);
  }

  void testGetUseBufferedTapeMarksOverMultipleFilesEnvFalse() {
    const uint32_t nbDrives = 0;
    castor::tape::tapebridge::VdqmRequestHandler handler(nbDrives);
    castor::tape::tapebridge::ConfigParamAndSource<bool>
      useBufferedTapeMarksOverMultipleFiles("UNKNOWN NAME", false,
        "UNKNOWN SOURCE");

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "setenv TAPEBRIDGED_USEBUFFEREDTAPEMARKSOVERMULTIPLEFILES",
      0,
      setenv("TAPEBRIDGED_USEBUFFEREDTAPEMARKSOVERMULTIPLEFILES", "false", 1));

    CPPUNIT_ASSERT_NO_THROW_MESSAGE(
      "getUseBufferedTapeMarksOverMultipleFiles",
      useBufferedTapeMarksOverMultipleFiles =
        handler.getUseBufferedTapeMarksOverMultipleFiles());

    CPPUNIT_ASSERT_EQUAL_MESSAGE("useBufferedTapeMarksOverMultipleFiles.name",
      std::string("TAPEBRIDGED/USEBUFFEREDTAPEMARKSOVERMULTIPLEFILES"),
      useBufferedTapeMarksOverMultipleFiles.name);

    CPPUNIT_ASSERT_EQUAL_MESSAGE("useBufferedTapeMarksOverMultipleFiles.value",
      false,
      useBufferedTapeMarksOverMultipleFiles.value);

    CPPUNIT_ASSERT_EQUAL_MESSAGE("useBufferedTapeMarksOverMultipleFiles.source",
      std::string("environment variable"),
      useBufferedTapeMarksOverMultipleFiles.source);
  }

  void testGetUseBufferedTapeMarksOverMultipleFilesInvalidPathConfig() {
    const uint32_t nbDrives = 0;
    castor::tape::tapebridge::VdqmRequestHandler handler(nbDrives);
    castor::tape::tapebridge::ConfigParamAndSource<bool>
      useBufferedTapeMarksOverMultipleFiles("UNKNOWN NAME", false,
        "UNKNOWN SOURCE");

    CPPUNIT_ASSERT_EQUAL_MESSAGE("setenv PATH_CONFIG",
      0,
      setenv("PATH_CONFIG", "INVALID PATH CONFIG", 1));

    CPPUNIT_ASSERT_NO_THROW_MESSAGE(
      "getUseBufferedTapeMarksOverMultipleFiles",
      useBufferedTapeMarksOverMultipleFiles =
        handler.getUseBufferedTapeMarksOverMultipleFiles());

    CPPUNIT_ASSERT_EQUAL_MESSAGE("useBufferedTapeMarksOverMultipleFiles.name",
      std::string("TAPEBRIDGED/USEBUFFEREDTAPEMARKSOVERMULTIPLEFILES"),
      useBufferedTapeMarksOverMultipleFiles.name);

    CPPUNIT_ASSERT_EQUAL_MESSAGE("useBufferedTapeMarksOverMultipleFiles.value",
      castor::tape::tapebridge::
        TAPEBRIDGED_USEBUFFEREDTAPEMARKSOVERMULTIPLEFILES,
      useBufferedTapeMarksOverMultipleFiles.value);

    CPPUNIT_ASSERT_EQUAL_MESSAGE("useBufferedTapeMarksOverMultipleFiles.source",
      std::string("compile-time default"),
      useBufferedTapeMarksOverMultipleFiles.source);
  }

  void testGetUseBufferedTapeMarksOverMultipleFilesInvalidLocalCastorConf() {
    const uint32_t nbDrives = 0;
    castor::tape::tapebridge::VdqmRequestHandler handler(nbDrives);
    castor::tape::tapebridge::ConfigParamAndSource<bool>
      useBufferedTapeMarksOverMultipleFiles("UNKNOWN NAME", false,
        "UNKNOWN SOURCE");

    CPPUNIT_ASSERT_EQUAL_MESSAGE("setenv PATH_CONFIG",
      0,
      setenv("PATH_CONFIG",
        "getUseBufferedTapeMarksOverMultipleFiles_invalid_castor.conf", 1));

    CPPUNIT_ASSERT_THROW_MESSAGE("Invalid local castor.conf variable",
      useBufferedTapeMarksOverMultipleFiles =
        handler.getUseBufferedTapeMarksOverMultipleFiles(),
      castor::exception::InvalidArgument);

    std::string exceptionMsg = "UNKNOWN";
    try {
      useBufferedTapeMarksOverMultipleFiles =
        handler.getUseBufferedTapeMarksOverMultipleFiles();
    } catch(castor::exception::InvalidArgument &ex) {
      exceptionMsg = ex.getMessage().str();
    }
    CPPUNIT_ASSERT_EQUAL_MESSAGE("exceptionMsg",
      std::string("Configuration parameter is not a valid unsigned integer:"
        " name=TAPEBRIDGED/USEBUFFEREDTAPEMARKSOVERMULTIPLEFILES"
        " value=INVALID_BOOL_VALUE"
        " source=castor.conf"),
      exceptionMsg);
  }

  void testGetUseBufferedTapeMarksOverMultipleFilesLocalCastorConfTrue() {
    const uint32_t nbDrives = 0;
    castor::tape::tapebridge::VdqmRequestHandler handler(nbDrives);
    castor::tape::tapebridge::ConfigParamAndSource<bool>
      useBufferedTapeMarksOverMultipleFiles("UNKNOWN NAME", false,
        "UNKNOWN SOURCE");

    CPPUNIT_ASSERT_EQUAL_MESSAGE("setenv PATH_CONFIG",
      0,
      setenv("PATH_CONFIG",
        "getUseBufferedTapeMarksOverMultipleFilesTrue_castor.conf", 1));

    CPPUNIT_ASSERT_NO_THROW_MESSAGE(
      "getUseBufferedTapeMarksOverMultipleFiles",
      useBufferedTapeMarksOverMultipleFiles =
        handler.getUseBufferedTapeMarksOverMultipleFiles());

    CPPUNIT_ASSERT_EQUAL_MESSAGE("useBufferedTapeMarksOverMultipleFiles.name",
      std::string("TAPEBRIDGED/USEBUFFEREDTAPEMARKSOVERMULTIPLEFILES"),
      useBufferedTapeMarksOverMultipleFiles.name);

    CPPUNIT_ASSERT_EQUAL_MESSAGE("useBufferedTapeMarksOverMultipleFiles.value",
      true,
      useBufferedTapeMarksOverMultipleFiles.value);

    CPPUNIT_ASSERT_EQUAL_MESSAGE("useBufferedTapeMarksOverMultipleFiles.source",
      std::string("castor.conf"),
      useBufferedTapeMarksOverMultipleFiles.source);
  }

  void testGetUseBufferedTapeMarksOverMultipleFilesLocalCastorConfFalse() {
    const uint32_t nbDrives = 0;
    castor::tape::tapebridge::VdqmRequestHandler handler(nbDrives);
    castor::tape::tapebridge::ConfigParamAndSource<bool>
      useBufferedTapeMarksOverMultipleFiles("UNKNOWN NAME", false,
        "UNKNOWN SOURCE");

    CPPUNIT_ASSERT_EQUAL_MESSAGE("setenv PATH_CONFIG",
      0,
      setenv("PATH_CONFIG",
        "getUseBufferedTapeMarksOverMultipleFilesFalse_castor.conf", 1));

    CPPUNIT_ASSERT_NO_THROW_MESSAGE(
      "getUseBufferedTapeMarksOverMultipleFiles",
      useBufferedTapeMarksOverMultipleFiles =
        handler.getUseBufferedTapeMarksOverMultipleFiles());

    CPPUNIT_ASSERT_EQUAL_MESSAGE("useBufferedTapeMarksOverMultipleFiles.name",
      std::string("TAPEBRIDGED/USEBUFFEREDTAPEMARKSOVERMULTIPLEFILES"),
      useBufferedTapeMarksOverMultipleFiles.name);

    CPPUNIT_ASSERT_EQUAL_MESSAGE("useBufferedTapeMarksOverMultipleFiles.value",
      false,
      useBufferedTapeMarksOverMultipleFiles.value);

    CPPUNIT_ASSERT_EQUAL_MESSAGE("useBufferedTapeMarksOverMultipleFiles.source",
      std::string("castor.conf"),
      useBufferedTapeMarksOverMultipleFiles.source);
  }

  CPPUNIT_TEST_SUITE(VdqmRequestHandlerTest);
  CPPUNIT_TEST(
    testGetUseBufferedTapeMarksOverMultipleFilesInvalidEnv);
  CPPUNIT_TEST(
    testGetUseBufferedTapeMarksOverMultipleFilesEnvTrue);
  CPPUNIT_TEST(
    testGetUseBufferedTapeMarksOverMultipleFilesEnvFalse);
  CPPUNIT_TEST(
    testGetUseBufferedTapeMarksOverMultipleFilesInvalidPathConfig);
  CPPUNIT_TEST(
    testGetUseBufferedTapeMarksOverMultipleFilesInvalidLocalCastorConf);
  CPPUNIT_TEST(
    testGetUseBufferedTapeMarksOverMultipleFilesLocalCastorConfTrue);
  CPPUNIT_TEST(
    testGetUseBufferedTapeMarksOverMultipleFilesLocalCastorConfFalse);
  CPPUNIT_TEST_SUITE_END();
};

CPPUNIT_TEST_SUITE_REGISTRATION(VdqmRequestHandlerTest);

#endif // TEST_CASTOR_TAPE_TAPEBRIDGE_VDQMREQUESTHANDLERTEST_HPP
