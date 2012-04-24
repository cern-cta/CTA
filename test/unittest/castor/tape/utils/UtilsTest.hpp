/******************************************************************************
 *                test/unittest/castor/tape/utils/UtilsTest.hpp
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

#ifndef TEST_UNITTEST_CASTOR_TAPE_UTILS_UTILSTEST_HPP
#define TEST_UNITTEST_CASTOR_TAPE_UTILS_UTILSTEST_HPP 1

#include "castor/tape/utils/utils.hpp"
#include "test/unittest/test_exception.hpp"

#include <cppunit/extensions/HelperMacros.h>
#include <list>
#include <sstream>
#include <stdlib.h>
#include <string>
#include <sys/time.h>
#include <unistd.h>
#include <vector>

namespace castor {
namespace tape   {
namespace utils {

class UtilsTest: public CppUnit::TestFixture {
private:
  void readFileIntoList_stdException(const char *const filename,
    std::list<std::string> &lines) {
    try {
      readFileIntoList(filename, lines);
    } catch(castor::exception::Exception &ex) {
      test_exception te(ex.getMessage().str());

      throw te;
    }
  }

  void appendPathToEnvVar_stdException(const std::string &envVarName,
    const std::string &pathToBeAppended) {
    try {
      appendPathToEnvVar(envVarName, pathToBeAppended);
    } catch(castor::exception::Exception &ex) {
      test_exception te(ex.getMessage().str());

    throw te;
    }
  }

  std::string timevalToString(const timeval &tv) {
    std::ostringstream oss;

    oss << "{tv_sec=" << tv.tv_sec << ", tv_usec=" << tv.tv_usec << "}";

    return oss.str();
  }

public:

  void setUp() {
  }

  void tearDown() {
  }

  void testToHex() {
    const uint32_t number          = 0xdeadface;
    const char     *expectedResult = "deadface";

    {
      char buf[8];
      CPPUNIT_ASSERT_THROW_MESSAGE(
        "Failed to detect a buffer that is too small",
        toHex(number, buf),
        castor::exception::InvalidArgument);
    }

    {
      char buf[9];
      toHex(number, buf);

      CPPUNIT_ASSERT_MESSAGE(
        "toHex did not give the expected result",
        strcmp(expectedResult, buf) == 0);
    }
  }

  void testParseTpconfig() {
    TpconfigLines lines;

    CPPUNIT_ASSERT_THROW_MESSAGE(
      "Failed to detect invalid path",
      parseTpconfigFile("/SillyPath", lines),
      castor::exception::Exception);

    CPPUNIT_ASSERT_NO_THROW_MESSAGE("Failed to parse TPCONFIG",
      parseTpconfigFile(castor::tape::TPCONFIGPATH,
      lines));
  }

  void testCopyStringNullDst() {
    char dummy[6] = "Dummy";

    CPPUNIT_ASSERT_THROW_MESSAGE("NULL dst",
      copyString(NULL, 0, dummy),
      castor::exception::Exception);
  }

  void testCopyStringNullSrc() {
    char dummy[6] = "Dummy";

    CPPUNIT_ASSERT_THROW_MESSAGE("NULL src",
      copyString(dummy, sizeof(dummy), NULL),
      castor::exception::Exception);
  }

  void testCopyString() {
    char src[12]  = "Hello World";
    char dst[12];

    copyString(dst, src);
    CPPUNIT_ASSERT_MESSAGE("Copying \"Hello World\"",
      strcmp(dst, src) == 0);
  }

  void testReadFileIntoList_sevenEntryList() {
    const char *const filename = "sevenEntryList.txt";
    std::list<std::string> lines;

    CPPUNIT_ASSERT_NO_THROW_MESSAGE("readFileIntoList_stdException",
      readFileIntoList_stdException(filename, lines));

    size_t expectedNbLines = 7;
    CPPUNIT_ASSERT_EQUAL_MESSAGE("number of lines",
      expectedNbLines, lines.size());

    std::list<std::string>::iterator itor = lines.begin();
    CPPUNIT_ASSERT_MESSAGE("First line"   , *(itor++) == "First line"  );
    CPPUNIT_ASSERT_MESSAGE("Second line"  , *(itor++) == "Second line" );
    CPPUNIT_ASSERT_MESSAGE("Third line"   , *(itor++) == "Third line"  );
    CPPUNIT_ASSERT_MESSAGE("Fourth line"  , *(itor++) == "Fourth line" );
    CPPUNIT_ASSERT_MESSAGE("Fifth line"   , *(itor++) == "Fifth line"  );
    CPPUNIT_ASSERT_MESSAGE("Sixth line"   , *(itor++) == "Sixth line"  );
    CPPUNIT_ASSERT_MESSAGE("Seventh line" , *(itor++) == "Seventh line");
    CPPUNIT_ASSERT_MESSAGE("No more lines", itor == lines.end());
  }

  void testReadFileIntoList_eightEntryListLastThreeEmpty() {
    const char *const filename = "eightEntryListLastThreeEmpty.txt";
    std::list<std::string> lines;

    CPPUNIT_ASSERT_NO_THROW_MESSAGE("readFileIntoList_stdException",
      readFileIntoList_stdException(filename, lines));

    size_t expectedNbLines = 8;
    CPPUNIT_ASSERT_EQUAL_MESSAGE("number of lines",
      expectedNbLines, lines.size());

    std::list<std::string>::iterator itor = lines.begin();
    CPPUNIT_ASSERT_MESSAGE("First line"   , *(itor++) == "First line" );
    CPPUNIT_ASSERT_MESSAGE("Second line"  , *(itor++) == "Second line");
    CPPUNIT_ASSERT_MESSAGE("Third line"   , *(itor++) == "Third line" );
    CPPUNIT_ASSERT_MESSAGE("Fourth line"  , *(itor++) == "Fourth line");
    CPPUNIT_ASSERT_MESSAGE("Fifth line"   , *(itor++) == "Fifth line" );
    CPPUNIT_ASSERT_MESSAGE("Sixth line"   , *(itor++) == ""           );
    CPPUNIT_ASSERT_MESSAGE("Seventh line" , *(itor++) == ""           );
    CPPUNIT_ASSERT_MESSAGE("Eighth line"  , *(itor++) == ""           );
    CPPUNIT_ASSERT_MESSAGE("No more lines", itor == lines.end()       );
  }

  void testReadFileIntoList_twelveEntryListThirdAndFourthEmpty() {
    const char *const filename = "twelveEntryListThirdAndFourthEmpty.txt";
    std::list<std::string> lines;

    CPPUNIT_ASSERT_NO_THROW_MESSAGE("readFileIntoList_stdException",
      readFileIntoList_stdException(filename, lines));

    size_t expectedNbLines = 12;
    CPPUNIT_ASSERT_EQUAL_MESSAGE("number of lines",
      expectedNbLines, lines.size());

    std::list<std::string>::iterator itor = lines.begin();
    CPPUNIT_ASSERT_MESSAGE("First line"   , *(itor++) == "First line"   );
    CPPUNIT_ASSERT_MESSAGE("Second line"  , *(itor++) == "Second line"  );
    CPPUNIT_ASSERT_MESSAGE("Third line"   , *(itor++) == ""             );
    CPPUNIT_ASSERT_MESSAGE("Fourth line"  , *(itor++) == ""             );
    CPPUNIT_ASSERT_MESSAGE("Fifth line"   , *(itor++) == "Fifth line"   );
    CPPUNIT_ASSERT_MESSAGE("Sixth line"   , *(itor++) == "Sixth line"   );
    CPPUNIT_ASSERT_MESSAGE("Seventh line" , *(itor++) == "Seventh line" );
    CPPUNIT_ASSERT_MESSAGE("Eighth line"  , *(itor++) == "Eighth line"  );
    CPPUNIT_ASSERT_MESSAGE("Nineth line"  , *(itor++) == "Nineth line"  );
    CPPUNIT_ASSERT_MESSAGE("Tenth line"   , *(itor++) == "Tenth line"   );
    CPPUNIT_ASSERT_MESSAGE("Eleventh line", *(itor++) == "Eleventh line");
    CPPUNIT_ASSERT_MESSAGE("Twelfth line" , *(itor++) == "Twelfth line" );
    CPPUNIT_ASSERT_MESSAGE("No more lines", itor == lines.end()         );
  }

  void testReadFileIntoList_twelveEntryListFifthAndTenthEmpty() {
    const char *const filename = "twelveEntryListFifthAndTenthEmpty.txt";
    std::list<std::string> lines;

    CPPUNIT_ASSERT_NO_THROW_MESSAGE("readFileIntoList_stdException",
      readFileIntoList_stdException(filename, lines));

    size_t expectedNbLines = 12;
    CPPUNIT_ASSERT_EQUAL_MESSAGE("number of lines",
      expectedNbLines, lines.size());

    std::list<std::string>::iterator itor = lines.begin();
    CPPUNIT_ASSERT_MESSAGE("First line"   , *(itor++) == "First line"   );
    CPPUNIT_ASSERT_MESSAGE("Second line"  , *(itor++) == "Second line"  );
    CPPUNIT_ASSERT_MESSAGE("Third line"   , *(itor++) == "Third line"   );
    CPPUNIT_ASSERT_MESSAGE("Fourth line"  , *(itor++) == "Fourth line"  );
    CPPUNIT_ASSERT_MESSAGE("Fifth line"   , *(itor++) == ""             );
    CPPUNIT_ASSERT_MESSAGE("Sixth line"   , *(itor++) == "Sixth line"   );
    CPPUNIT_ASSERT_MESSAGE("Seventh line" , *(itor++) == "Seventh line" );
    CPPUNIT_ASSERT_MESSAGE("Eighth line"  , *(itor++) == "Eighth line"  );
    CPPUNIT_ASSERT_MESSAGE("Nineth line"  , *(itor++) == "Nineth line"  );
    CPPUNIT_ASSERT_MESSAGE("Tenth line"   , *(itor++) == ""             );
    CPPUNIT_ASSERT_MESSAGE("Eleventh line", *(itor++) == "Eleventh line");
    CPPUNIT_ASSERT_MESSAGE("Twelfth line" , *(itor++) == "Twelfth line" );
    CPPUNIT_ASSERT_MESSAGE("No more lines", itor == lines.end()         );
  }

  void testAppendPathToUnsetEnvVar() {
    const std::string envVarName       = "TESTPATH";
    const std::string pathToBeAppended = "/testing/testing/1/2/3";
    const char        *getenvResult    = NULL;

    CPPUNIT_ASSERT_EQUAL_MESSAGE("unsetenv",
      0, unsetenv(envVarName.c_str()));

    CPPUNIT_ASSERT_EQUAL_MESSAGE("getenv of unset variable",
      (char *)0, getenv(envVarName.c_str()));

    CPPUNIT_ASSERT_NO_THROW_MESSAGE("appendPathToEnvVar_stdException",
      appendPathToEnvVar_stdException(envVarName, pathToBeAppended));

    getenvResult = getenv(envVarName.c_str());
    CPPUNIT_ASSERT_MESSAGE("getenv", NULL != getenvResult);
    CPPUNIT_ASSERT_MESSAGE("path appended", pathToBeAppended == getenvResult);
  }

  void testAppendPathToEmptyEnvVar() {
    const std::string envVarName       = "TESTPATH";
    const std::string pathToBeAppended = "/testing/testing/4/5/6";
    const char        *getenvResult    = NULL;

    const int overwrite = 1;
    CPPUNIT_ASSERT_EQUAL_MESSAGE("setenv variable  to empty string",
      0, setenv(envVarName.c_str(), "", overwrite));

    getenvResult = getenv(envVarName.c_str());
    CPPUNIT_ASSERT_MESSAGE("getenv", NULL != getenvResult);

    CPPUNIT_ASSERT_EQUAL_MESSAGE("getenv value is empty string",
      '\0', *getenvResult);

    CPPUNIT_ASSERT_NO_THROW_MESSAGE("appendPathToEnvVar_stdException",
      appendPathToEnvVar_stdException(envVarName, pathToBeAppended));

    getenvResult = getenv(envVarName.c_str());
    CPPUNIT_ASSERT_MESSAGE("getenv", NULL != getenvResult);
    CPPUNIT_ASSERT_MESSAGE("path appended", pathToBeAppended == getenvResult);
  }

  void testAppendPathToNonEmptyEnvVar() {
    const std::string envVarName       = "TESTPATH";
    const std::string firstPath        = "/the/first/path";
    const std::string pathToBeAppended = "/testing/testing/4/5/6";
    const std::string combinedPaths    = firstPath + ":" + pathToBeAppended;
    const char        *getenvResult    = NULL;

    const int overwrite = 1;
    CPPUNIT_ASSERT_EQUAL_MESSAGE("setenv",
      0, setenv(envVarName.c_str(), firstPath.c_str(), overwrite));

    getenvResult = getenv(envVarName.c_str());
    CPPUNIT_ASSERT_MESSAGE("getenv", NULL != getenvResult);

    CPPUNIT_ASSERT_MESSAGE("getenv value is first path",
      firstPath == getenvResult);

    CPPUNIT_ASSERT_NO_THROW_MESSAGE("appendPathToEnvVar_stdException",
      appendPathToEnvVar_stdException(envVarName, pathToBeAppended));

    getenvResult = getenv(envVarName.c_str());
    CPPUNIT_ASSERT_MESSAGE("getenv", NULL != getenvResult);
    CPPUNIT_ASSERT_MESSAGE("path appended", combinedPaths == getenvResult);
  }

  void testSplitString() {
    const std::string line("col1 col2 col3 col4 col5 col6 col7 col8");
    std::vector<std::string> columns;

    splitString(line, ' ', columns);

    CPPUNIT_ASSERT_EQUAL_MESSAGE("columns.size()",
      (std::vector<std::string>::size_type)8, columns.size());
    CPPUNIT_ASSERT_EQUAL_MESSAGE("columns[0]",
      std::string("col1"), columns[0]);
    CPPUNIT_ASSERT_EQUAL_MESSAGE("columns[0]",
      std::string("col2"), columns[1]);
    CPPUNIT_ASSERT_EQUAL_MESSAGE("columns[0]",
      std::string("col3"), columns[2]);
    CPPUNIT_ASSERT_EQUAL_MESSAGE("columns[0]",
      std::string("col4"), columns[3]);
    CPPUNIT_ASSERT_EQUAL_MESSAGE("columns[0]",
      std::string("col5"), columns[4]);
    CPPUNIT_ASSERT_EQUAL_MESSAGE("columns[0]",
      std::string("col6"), columns[5]);
    CPPUNIT_ASSERT_EQUAL_MESSAGE("columns[0]",
      std::string("col7"), columns[6]);
    CPPUNIT_ASSERT_EQUAL_MESSAGE("columns[0]",
      std::string("col8"), columns[7]);
  }

  void testTimevalGreaterThan_BigSecSmallSec_BigUsecSmallUsec() {
    const timeval bigger   = {6, 5};
    const timeval smaller  = {5, 4};
    const bool    expected = true;

    std::ostringstream oss;
    oss << "timevalGreaterThan(" << timevalToString(bigger) << ", " <<
      timevalToString(smaller) << ") = " << expected;
    CPPUNIT_ASSERT_EQUAL_MESSAGE(oss.str(), expected,
      timevalGreaterThan(bigger, smaller));
  }

  void testTimevalGreaterThan_BigSecSmallSec_BigUsecSmallUsec_swapped() {
    const timeval bigger   = {6, 5};
    const timeval smaller  = {5, 4};
    const bool    expected = false;

    std::ostringstream oss;
    oss << "timevalGreaterThan(" << timevalToString(smaller) << ", " <<
      timevalToString(bigger) << ") = " << expected;
    CPPUNIT_ASSERT_EQUAL_MESSAGE(oss.str(), expected,
      timevalGreaterThan(smaller, bigger));
  }

  void testTimevalGreaterThan_BigSecSmallSec_SmallUsecBigUsec() {
    const timeval bigger   = {4, 3};
    const timeval smaller  = {2, 7};
    const bool    expected = true;

    std::ostringstream oss;
    oss << "timevalGreaterThan(" << timevalToString(bigger) << ", " <<
      timevalToString(smaller) << ") = " << expected;
    CPPUNIT_ASSERT_EQUAL_MESSAGE(oss.str(), expected,
      timevalGreaterThan(bigger, smaller));
  }

  void testTimevalGreaterThan_BigSecSmallSec_SmallUsecBigUsec_swapped() {
    const timeval bigger   = {4, 3};
    const timeval smaller  = {2, 7};
    const bool    expected = false;

    std::ostringstream oss;
    oss << "timevalGreaterThan(" << timevalToString(smaller) << ", " <<
      timevalToString(bigger) << ") = false";
    CPPUNIT_ASSERT_EQUAL_MESSAGE(oss.str(), expected,
      timevalGreaterThan(smaller, bigger));
  }

  void testTimevalGreaterThan_EqualSec_EqualUsec()  {
    const timeval a         = {8, 9};
    const timeval b         = {8, 9};
    const bool    expected  = false;
  
    std::ostringstream oss;
    oss << "timevalGreaterThan(" << timevalToString(a) << ", " <<
      timevalToString(b) << ") = " << expected;
    CPPUNIT_ASSERT_EQUAL_MESSAGE(oss.str(), expected, timevalGreaterThan(a, b));
  }

  void testTimevalAbsDiff_BigSecSmallSec_BigUsecSmallUsec() {
    const timeval bigger   = {6, 5};
    const timeval smaller  = {5, 4};
    const timeval expected = {1, 1};
    const timeval actual   = timevalAbsDiff(bigger,
      smaller);
    const bool    isAMatch = expected.tv_sec == actual.tv_sec &&
      expected.tv_usec == actual.tv_usec;

    std::ostringstream oss;
    oss << "timevalAbsDiff(" << timevalToString(bigger) << ", " <<
      timevalToString(smaller) << ") = " << timevalToString(expected);
    CPPUNIT_ASSERT_EQUAL_MESSAGE(oss.str(), true, isAMatch);
  }

  void testTimevalAbsDiff_BigSecSmallSec_BigUsecSmallUsec_swapped() {
    const timeval bigger   = {6, 5};
    const timeval smaller  = {5, 4};
    const timeval expected = {1, 1};
    const timeval actual   = timevalAbsDiff(smaller,
      bigger);
    const bool    isAMatch = expected.tv_sec == actual.tv_sec &&
      expected.tv_usec == actual.tv_usec;

    std::ostringstream oss;
    oss << "timevalAbsDiff(" << timevalToString(smaller) << ", " <<
      timevalToString(bigger) << ") = " << timevalToString(expected);
    CPPUNIT_ASSERT_EQUAL_MESSAGE(oss.str(), true, isAMatch);
  }

  void testTimevalAbsDiff_BigSecSmallSec_SmallUsecBigUsec() {
    const timeval bigger   = {4, 3};
    const timeval smaller  = {2, 7};
    const timeval expected = {1, 999996};
    const timeval actual   = timevalAbsDiff(bigger,
      smaller);
    const bool    isAMatch = expected.tv_sec == actual.tv_sec &&
      expected.tv_usec == actual.tv_usec;

    std::ostringstream oss;
    oss << "timevalAbsDiff(" << timevalToString(bigger) << ", " <<
      timevalToString(smaller) << ") = " << timevalToString(expected);
    CPPUNIT_ASSERT_EQUAL_MESSAGE(oss.str(), true, isAMatch);
  }

  void testTimevalAbsDiff_BigSecSmallSec_SmallUsecBigUsec_swapped() {
    const timeval bigger   = {4, 3};
    const timeval smaller  = {2, 7};
    const timeval expected = {1, 999996};
    const timeval actual   = timevalAbsDiff(smaller,
      bigger);
    const bool    isAMatch = expected.tv_sec == actual.tv_sec &&
      expected.tv_usec == actual.tv_usec;

    std::ostringstream oss;
    oss << "timevalAbsDiff(" << timevalToString(smaller) << ", " <<
      timevalToString(bigger) << ") = " << timevalToString(expected);
    CPPUNIT_ASSERT_EQUAL_MESSAGE(oss.str(), true, isAMatch);
  }

  void testTimevalAbsDiff_EqualSec_EqualUsec()  {
    const timeval a        = {8, 9};
    const timeval b        = {8, 9};
    const timeval expected = {0, 0};
    const timeval actual   = timevalAbsDiff(a, b);
    const bool    isAMatch = expected.tv_sec == actual.tv_sec &&
      expected.tv_usec == actual.tv_usec;
  
    std::ostringstream oss;
    oss << "timevalAbsDiff(" << timevalToString(a) << ", " <<
      timevalToString(b) << ") = " << timevalToString(expected);
    CPPUNIT_ASSERT_EQUAL_MESSAGE(oss.str(), true, isAMatch);
  }

  void testTimevalToDouble() {
    const timeval tv       = {1234, 999992};
    const double  expected = 1234.999992;
    const double  actual   = timevalToDouble(tv);

    std::ostringstream oss;
    oss << "timevalToDouble(" << timevalToString(tv) << ") = " << expected;
    CPPUNIT_ASSERT_EQUAL_MESSAGE(oss.str(), expected, actual);
  }

  void testGetTimeOfDay() {
    timeval startTime    = {0, 0};
    timeval endTime      = {0, 0};
    timeval testDuration = {0, 0};
    const unsigned int sleepDuration = 1;

    getTimeOfDay(&startTime, NULL);
    sleep(sleepDuration);
    getTimeOfDay(&endTime, NULL);

    const bool startTimeIsBeforeEndTime =
      timevalGreaterThan(endTime, startTime);

    CPPUNIT_ASSERT_EQUAL_MESSAGE("startTime is before endTime",
      true, startTimeIsBeforeEndTime);

    testDuration = timevalAbsDiff(startTime, endTime);

    CPPUNIT_ASSERT_EQUAL_MESSAGE("Test duration is accuracte to the second",
      true, sleepDuration == testDuration.tv_sec);
  }

  CPPUNIT_TEST_SUITE(UtilsTest);
  CPPUNIT_TEST(testToHex);
  CPPUNIT_TEST(testCopyStringNullDst);
  CPPUNIT_TEST(testCopyStringNullSrc);
  CPPUNIT_TEST(testCopyString);
  CPPUNIT_TEST(testReadFileIntoList_sevenEntryList);
  CPPUNIT_TEST(testReadFileIntoList_eightEntryListLastThreeEmpty);
  CPPUNIT_TEST(testReadFileIntoList_twelveEntryListThirdAndFourthEmpty);
  CPPUNIT_TEST(testReadFileIntoList_twelveEntryListFifthAndTenthEmpty);
  CPPUNIT_TEST(testAppendPathToUnsetEnvVar);
  CPPUNIT_TEST(testAppendPathToEmptyEnvVar);
  CPPUNIT_TEST(testAppendPathToNonEmptyEnvVar);
  CPPUNIT_TEST(testSplitString);
  CPPUNIT_TEST(testTimevalGreaterThan_BigSecSmallSec_BigUsecSmallUsec);
  CPPUNIT_TEST(testTimevalGreaterThan_BigSecSmallSec_BigUsecSmallUsec_swapped);
  CPPUNIT_TEST(testTimevalGreaterThan_BigSecSmallSec_SmallUsecBigUsec);
  CPPUNIT_TEST(testTimevalGreaterThan_BigSecSmallSec_SmallUsecBigUsec_swapped);
  CPPUNIT_TEST(testTimevalGreaterThan_EqualSec_EqualUsec);
  CPPUNIT_TEST(testTimevalAbsDiff_BigSecSmallSec_BigUsecSmallUsec);
  CPPUNIT_TEST(testTimevalAbsDiff_BigSecSmallSec_BigUsecSmallUsec_swapped);
  CPPUNIT_TEST(testTimevalAbsDiff_BigSecSmallSec_SmallUsecBigUsec);
  CPPUNIT_TEST(testTimevalAbsDiff_BigSecSmallSec_SmallUsecBigUsec_swapped);
  CPPUNIT_TEST(testTimevalAbsDiff_EqualSec_EqualUsec);
  CPPUNIT_TEST(testTimevalToDouble);
  CPPUNIT_TEST(testGetTimeOfDay);
  CPPUNIT_TEST_SUITE_END();
};

CPPUNIT_TEST_SUITE_REGISTRATION(UtilsTest);

} // namespace utils
} // namespace tape
} // namespace castor

#endif // TEST_UNITTEST_CASTOR_TAPE_UTILS_UTILSTEST_HPP
