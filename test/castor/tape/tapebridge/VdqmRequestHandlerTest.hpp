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

#include "castor/exception/InvalidArgument.hpp"
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
    unsetenv("TAPEBRIDGE_USEBUFFEREDTAPEMARKSOVERMULTIPLEFILES");
    unsetenv("TAPEBRIDGE_MAXBYTESBEFOREFLUSH");
    unsetenv("TAPEBRIDGE_MAXFILESBEFOREFLUSH");
  }

  void tearDown() {
    setenv("PATH_CONFIG", "/etc/castor/castor.conf", 1);
    unsetenv("TAPEBRIDGE_USEBUFFEREDTAPEMARKSOVERMULTIPLEFILES");
    unsetenv("TAPEBRIDGE_MAXBYTESBEFOREFLUSH");
    unsetenv("TAPEBRIDGE_MAXFILESBEFOREFLUSH");
  }

  void testGetUseBufferedTapeMarksOverMultipleFilesInvalidEnv() {
    const uint32_t nbDrives = 0;
    castor::tape::tapebridge::VdqmRequestHandler handler(nbDrives);
    castor::tape::tapebridge::ConfigParamAndSource<bool>
      useBufferedTapeMarksOverMultipleFiles("UNKNOWN NAME", false,
        "UNKNOWN SOURCE");

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "setenv TAPEBRIDGE_USEBUFFEREDTAPEMARKSOVERMULTIPLEFILES",
      0,
      setenv("TAPEBRIDGE_USEBUFFEREDTAPEMARKSOVERMULTIPLEFILES",
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
        " name=TAPEBRIDGE/USEBUFFEREDTAPEMARKSOVERMULTIPLEFILES"
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
      "setenv TAPEBRIDGE_USEBUFFEREDTAPEMARKSOVERMULTIPLEFILES",
      0,
      setenv("TAPEBRIDGE_USEBUFFEREDTAPEMARKSOVERMULTIPLEFILES", "true", 1));

    CPPUNIT_ASSERT_NO_THROW_MESSAGE(
      "getUseBufferedTapeMarksOverMultipleFiles",
      useBufferedTapeMarksOverMultipleFiles =
        handler.getUseBufferedTapeMarksOverMultipleFiles());

    CPPUNIT_ASSERT_EQUAL_MESSAGE("useBufferedTapeMarksOverMultipleFiles.name",
      std::string("TAPEBRIDGE/USEBUFFEREDTAPEMARKSOVERMULTIPLEFILES"),
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
      "setenv TAPEBRIDGE_USEBUFFEREDTAPEMARKSOVERMULTIPLEFILES",
      0,
      setenv("TAPEBRIDGE_USEBUFFEREDTAPEMARKSOVERMULTIPLEFILES", "false", 1));

    CPPUNIT_ASSERT_NO_THROW_MESSAGE(
      "getUseBufferedTapeMarksOverMultipleFiles",
      useBufferedTapeMarksOverMultipleFiles =
        handler.getUseBufferedTapeMarksOverMultipleFiles());

    CPPUNIT_ASSERT_EQUAL_MESSAGE("useBufferedTapeMarksOverMultipleFiles.name",
      std::string("TAPEBRIDGE/USEBUFFEREDTAPEMARKSOVERMULTIPLEFILES"),
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
      std::string("TAPEBRIDGE/USEBUFFEREDTAPEMARKSOVERMULTIPLEFILES"),
      useBufferedTapeMarksOverMultipleFiles.name);

    CPPUNIT_ASSERT_EQUAL_MESSAGE("useBufferedTapeMarksOverMultipleFiles.value",
      castor::tape::tapebridge::
        TAPEBRIDGE_USEBUFFEREDTAPEMARKSOVERMULTIPLEFILES,
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
        " name=TAPEBRIDGE/USEBUFFEREDTAPEMARKSOVERMULTIPLEFILES"
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
      std::string("TAPEBRIDGE/USEBUFFEREDTAPEMARKSOVERMULTIPLEFILES"),
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
      std::string("TAPEBRIDGE/USEBUFFEREDTAPEMARKSOVERMULTIPLEFILES"),
      useBufferedTapeMarksOverMultipleFiles.name);

    CPPUNIT_ASSERT_EQUAL_MESSAGE("useBufferedTapeMarksOverMultipleFiles.value",
      false,
      useBufferedTapeMarksOverMultipleFiles.value);

    CPPUNIT_ASSERT_EQUAL_MESSAGE("useBufferedTapeMarksOverMultipleFiles.source",
      std::string("castor.conf"),
      useBufferedTapeMarksOverMultipleFiles.source);
  }

  void testGetMaxBytesBeforeFlushInvalidEnv() {
    const uint32_t nbDrives = 0;
    castor::tape::tapebridge::VdqmRequestHandler handler(nbDrives);
    castor::tape::tapebridge::ConfigParamAndSource<uint64_t>
      maxBytesBeforeFlush("UNKNOWN NAME", 0, "UNKNOWN SOURCE");

    CPPUNIT_ASSERT_EQUAL_MESSAGE("setenv TAPEBRIDGE_MAXBYTESBEFOREFLUSH",
      0,
      setenv("TAPEBRIDGE_MAXBYTESBEFOREFLUSH", "INVALID_UINT_VALUE", 1));

    CPPUNIT_ASSERT_THROW_MESSAGE("Invalid environment variable",
      maxBytesBeforeFlush = handler.getMaxBytesBeforeFlush(),
      castor::exception::InvalidArgument);

    std::string exceptionMsg = "UNKNOWN";
    try {
      maxBytesBeforeFlush = handler.getMaxBytesBeforeFlush();
    } catch(castor::exception::InvalidArgument &ex) {
      exceptionMsg = ex.getMessage().str();
    }
    CPPUNIT_ASSERT_EQUAL_MESSAGE("exceptionMsg",
      std::string("Configuration parameter is not a valid unsigned integer:"
        " name=TAPEBRIDGE/MAXBYTESBEFOREFLUSH"
        " value=INVALID_UINT_VALUE"
        " source=environment variable"),
      exceptionMsg);
  }

  void testGetMaxBytesBeforeFlushEnv() {
    const uint32_t nbDrives = 0;
    castor::tape::tapebridge::VdqmRequestHandler handler(nbDrives);
    castor::tape::tapebridge::ConfigParamAndSource<uint64_t>
      maxBytesBeforeFlush("UNKNOWN NAME", 0, "UNKNOWN SOURCE");

    CPPUNIT_ASSERT_EQUAL_MESSAGE("setenv TAPEBRIDGE_MAXBYTESBEFOREFLUSH",
      0,
      setenv("TAPEBRIDGE_MAXBYTESBEFOREFLUSH", "11111", 1));

    CPPUNIT_ASSERT_NO_THROW_MESSAGE(
      "getMaxBytesBeforeFlush",
      maxBytesBeforeFlush = handler.getMaxBytesBeforeFlush());

    CPPUNIT_ASSERT_EQUAL_MESSAGE("getMaxBytesBeforeFlush.name",
      std::string("TAPEBRIDGE/MAXBYTESBEFOREFLUSH"),
      maxBytesBeforeFlush.name);

    CPPUNIT_ASSERT_EQUAL_MESSAGE("maxBytesBeforeFlush.value",
      (uint64_t)11111,
      maxBytesBeforeFlush.value);

    CPPUNIT_ASSERT_EQUAL_MESSAGE("maxBytesBeforeFlush.source",
      std::string("environment variable"),
      maxBytesBeforeFlush.source);
  }

  void testGetMaxBytesBeforeFlushInvalidPathConfig() {
    const uint32_t nbDrives = 0;
    castor::tape::tapebridge::VdqmRequestHandler handler(nbDrives);
    castor::tape::tapebridge::ConfigParamAndSource<uint64_t>
      maxBytesBeforeFlush("UNKNOWN NAME", 0, "UNKNOWN SOURCE");

    CPPUNIT_ASSERT_EQUAL_MESSAGE("setenv PATH_CONFIG",
      0,
      setenv("PATH_CONFIG", "INVALID PATH CONFIG", 1));

    CPPUNIT_ASSERT_NO_THROW_MESSAGE("getMaxBytesBeforeFlush",
      maxBytesBeforeFlush = handler.getMaxBytesBeforeFlush());

    CPPUNIT_ASSERT_EQUAL_MESSAGE("maxBytesBeforeFlush.name",
      std::string("TAPEBRIDGE/MAXBYTESBEFOREFLUSH"),
      maxBytesBeforeFlush.name);

    CPPUNIT_ASSERT_EQUAL_MESSAGE("maxBytesBeforeFlush.value",
      castor::tape::tapebridge::
        TAPEBRIDGE_MAXBYTESBEFOREFLUSH,
      maxBytesBeforeFlush.value);

    CPPUNIT_ASSERT_EQUAL_MESSAGE("maxBytesBeforeFlush.source",
      std::string("compile-time default"),
      maxBytesBeforeFlush.source);
  }

  void testGetMaxBytesBeforeFlushInvalidLocalCastorConf() {
    const uint32_t nbDrives = 0;
    castor::tape::tapebridge::VdqmRequestHandler handler(nbDrives);
    castor::tape::tapebridge::ConfigParamAndSource<uint64_t>
      maxBytesBeforeFlush("UNKNOWN NAME", 0, "UNKNOWN SOURCE");

    CPPUNIT_ASSERT_EQUAL_MESSAGE("setenv PATH_CONFIG",
      0,
      setenv("PATH_CONFIG", "getMaxBytesBeforeFlush_invalid_castor.conf", 1));

    CPPUNIT_ASSERT_THROW_MESSAGE("Invalid local castor.conf variable",
      maxBytesBeforeFlush = handler.getMaxBytesBeforeFlush(),
      castor::exception::InvalidArgument);

    std::string exceptionMsg = "UNKNOWN";
    try {
      maxBytesBeforeFlush = handler.getMaxBytesBeforeFlush();
    } catch(castor::exception::InvalidArgument &ex) {
      exceptionMsg = ex.getMessage().str();
    }
    CPPUNIT_ASSERT_EQUAL_MESSAGE("exceptionMsg",
      std::string("Configuration parameter is not a valid unsigned integer:"
        " name=TAPEBRIDGE/MAXBYTESBEFOREFLUSH"
        " value=INVALID_UINT_VALUE"
        " source=castor.conf"),
      exceptionMsg);
  }

  void testGetMaxBytesBeforeFlushLocalCastorConf() {
    const uint32_t nbDrives = 0;
    castor::tape::tapebridge::VdqmRequestHandler handler(nbDrives);
    castor::tape::tapebridge::ConfigParamAndSource<uint64_t>
      maxBytesBeforeFlush("UNKNOWN NAME", 0, "UNKNOWN SOURCE");

    CPPUNIT_ASSERT_EQUAL_MESSAGE("setenv PATH_CONFIG",
      0,
      setenv("PATH_CONFIG", "getMaxBytesBeforeFlush_castor.conf", 1));

    CPPUNIT_ASSERT_NO_THROW_MESSAGE(
      "getMaxBytesBeforeFlush",
      maxBytesBeforeFlush = handler.getMaxBytesBeforeFlush());

    CPPUNIT_ASSERT_EQUAL_MESSAGE("maxBytesBeforeFlush.name",
      std::string("TAPEBRIDGE/MAXBYTESBEFOREFLUSH"),
      maxBytesBeforeFlush.name);

    CPPUNIT_ASSERT_EQUAL_MESSAGE("maxBytesBeforeFlush.value",
      (uint64_t)33333,
      maxBytesBeforeFlush.value);

    CPPUNIT_ASSERT_EQUAL_MESSAGE("maxBytesBeforeFlush.source",
      std::string("castor.conf"),
      maxBytesBeforeFlush.source);
  }

  void testGetMaxFilesBeforeFlushInvalidEnv() {
    const uint32_t nbDrives = 0;
    castor::tape::tapebridge::VdqmRequestHandler handler(nbDrives);
    castor::tape::tapebridge::ConfigParamAndSource<uint64_t>
      maxFilesBeforeFlush("UNKNOWN NAME", 0, "UNKNOWN SOURCE");

    CPPUNIT_ASSERT_EQUAL_MESSAGE("setenv TAPEBRIDGE_MAXFILESBEFOREFLUSH",
      0,
      setenv("TAPEBRIDGE_MAXFILESBEFOREFLUSH", "INVALID_UINT_VALUE", 1));

    CPPUNIT_ASSERT_THROW_MESSAGE("Invalid environment variable",
      maxFilesBeforeFlush = handler.getMaxFilesBeforeFlush(),
      castor::exception::InvalidArgument);

    std::string exceptionMsg = "UNKNOWN";
    try {
      maxFilesBeforeFlush = handler.getMaxFilesBeforeFlush();
    } catch(castor::exception::InvalidArgument &ex) {
      exceptionMsg = ex.getMessage().str();
    }
    CPPUNIT_ASSERT_EQUAL_MESSAGE("exceptionMsg",
      std::string("Configuration parameter is not a valid unsigned integer:"
        " name=TAPEBRIDGE/MAXFILESBEFOREFLUSH"
        " value=INVALID_UINT_VALUE"
        " source=environment variable"),
      exceptionMsg);
  }

  void testGetMaxFilesBeforeFlushEnv() {
    const uint32_t nbDrives = 0;
    castor::tape::tapebridge::VdqmRequestHandler handler(nbDrives);
    castor::tape::tapebridge::ConfigParamAndSource<uint64_t>
      maxFilesBeforeFlush("UNKNOWN NAME", 0, "UNKNOWN SOURCE");

    CPPUNIT_ASSERT_EQUAL_MESSAGE("setenv TAPEBRIDGE_MAXFILESBEFOREFLUSH",
      0,
      setenv("TAPEBRIDGE_MAXFILESBEFOREFLUSH", "11111", 1));

    CPPUNIT_ASSERT_NO_THROW_MESSAGE(
      "getMaxFilesBeforeFlush",
      maxFilesBeforeFlush = handler.getMaxFilesBeforeFlush());

    CPPUNIT_ASSERT_EQUAL_MESSAGE("getMaxFilesBeforeFlush.name",
      std::string("TAPEBRIDGE/MAXFILESBEFOREFLUSH"),
      maxFilesBeforeFlush.name);

    CPPUNIT_ASSERT_EQUAL_MESSAGE("maxFilesBeforeFlush.value",
      (uint64_t)11111,
      maxFilesBeforeFlush.value);

    CPPUNIT_ASSERT_EQUAL_MESSAGE("maxFilesBeforeFlush.source",
      std::string("environment variable"),
      maxFilesBeforeFlush.source);
  }

  void testGetMaxFilesBeforeFlushInvalidPathConfig() {
    const uint32_t nbDrives = 0;
    castor::tape::tapebridge::VdqmRequestHandler handler(nbDrives);
    castor::tape::tapebridge::ConfigParamAndSource<uint64_t>
      maxFilesBeforeFlush("UNKNOWN NAME", 0, "UNKNOWN SOURCE");

    CPPUNIT_ASSERT_EQUAL_MESSAGE("setenv PATH_CONFIG",
      0,
      setenv("PATH_CONFIG", "INVALID PATH CONFIG", 1));

    CPPUNIT_ASSERT_NO_THROW_MESSAGE("getMaxFilesBeforeFlush",
      maxFilesBeforeFlush = handler.getMaxFilesBeforeFlush());

    CPPUNIT_ASSERT_EQUAL_MESSAGE("maxFilesBeforeFlush.name",
      std::string("TAPEBRIDGE/MAXFILESBEFOREFLUSH"),
      maxFilesBeforeFlush.name);

    CPPUNIT_ASSERT_EQUAL_MESSAGE("maxFilesBeforeFlush.value",
      castor::tape::tapebridge::
        TAPEBRIDGE_MAXFILESBEFOREFLUSH,
      maxFilesBeforeFlush.value);

    CPPUNIT_ASSERT_EQUAL_MESSAGE("maxFilesBeforeFlush.source",
      std::string("compile-time default"),
      maxFilesBeforeFlush.source);
  }

  void testGetMaxFilesBeforeFlushInvalidLocalCastorConf() {
    const uint32_t nbDrives = 0;
    castor::tape::tapebridge::VdqmRequestHandler handler(nbDrives);
    castor::tape::tapebridge::ConfigParamAndSource<uint64_t>
      maxFilesBeforeFlush("UNKNOWN NAME", 0, "UNKNOWN SOURCE");

    CPPUNIT_ASSERT_EQUAL_MESSAGE("setenv PATH_CONFIG",
      0,
      setenv("PATH_CONFIG", "getMaxFilesBeforeFlush_invalid_castor.conf", 1));

    CPPUNIT_ASSERT_THROW_MESSAGE("Invalid local castor.conf variable",
      maxFilesBeforeFlush = handler.getMaxFilesBeforeFlush(),
      castor::exception::InvalidArgument);

    std::string exceptionMsg = "UNKNOWN";
    try {
      maxFilesBeforeFlush = handler.getMaxFilesBeforeFlush();
    } catch(castor::exception::InvalidArgument &ex) {
      exceptionMsg = ex.getMessage().str();
    }
    CPPUNIT_ASSERT_EQUAL_MESSAGE("exceptionMsg",
      std::string("Configuration parameter is not a valid unsigned integer:"
        " name=TAPEBRIDGE/MAXFILESBEFOREFLUSH"
        " value=INVALID_UINT_VALUE"
        " source=castor.conf"),
      exceptionMsg);
  }

  void testGetMaxFilesBeforeFlushLocalCastorConf() {
    const uint32_t nbDrives = 0;
    castor::tape::tapebridge::VdqmRequestHandler handler(nbDrives);
    castor::tape::tapebridge::ConfigParamAndSource<uint64_t>
      maxFilesBeforeFlush("UNKNOWN NAME", 0, "UNKNOWN SOURCE");

    CPPUNIT_ASSERT_EQUAL_MESSAGE("setenv PATH_CONFIG",
      0,
      setenv("PATH_CONFIG", "getMaxFilesBeforeFlush_castor.conf", 1));

    CPPUNIT_ASSERT_NO_THROW_MESSAGE(
      "getMaxFilesBeforeFlush",
      maxFilesBeforeFlush = handler.getMaxFilesBeforeFlush());

    CPPUNIT_ASSERT_EQUAL_MESSAGE("maxFilesBeforeFlush.name",
      std::string("TAPEBRIDGE/MAXFILESBEFOREFLUSH"),
      maxFilesBeforeFlush.name);

    CPPUNIT_ASSERT_EQUAL_MESSAGE("maxFilesBeforeFlush.value",
      (uint64_t)44444,
      maxFilesBeforeFlush.value);

    CPPUNIT_ASSERT_EQUAL_MESSAGE("maxFilesBeforeFlush.source",
      std::string("castor.conf"),
      maxFilesBeforeFlush.source);
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

  CPPUNIT_TEST(testGetMaxBytesBeforeFlushInvalidEnv);
  CPPUNIT_TEST(testGetMaxBytesBeforeFlushEnv);
  CPPUNIT_TEST(testGetMaxBytesBeforeFlushInvalidPathConfig);
  CPPUNIT_TEST(testGetMaxBytesBeforeFlushInvalidLocalCastorConf);
  CPPUNIT_TEST(testGetMaxBytesBeforeFlushLocalCastorConf);

  CPPUNIT_TEST(testGetMaxFilesBeforeFlushInvalidEnv);
  CPPUNIT_TEST(testGetMaxFilesBeforeFlushEnv);
  CPPUNIT_TEST(testGetMaxFilesBeforeFlushInvalidPathConfig);
  CPPUNIT_TEST(testGetMaxFilesBeforeFlushInvalidLocalCastorConf);
  CPPUNIT_TEST(testGetMaxFilesBeforeFlushLocalCastorConf);

  CPPUNIT_TEST_SUITE_END();
};

CPPUNIT_TEST_SUITE_REGISTRATION(VdqmRequestHandlerTest);

#endif // TEST_CASTOR_TAPE_TAPEBRIDGE_VDQMREQUESTHANDLERTEST_HPP
