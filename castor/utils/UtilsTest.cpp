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

#include "castor/utils/utils.hpp"

#include <gtest/gtest.h>
#include <list>
#include <stdlib.h>
#include <string>
#include <sys/time.h>
#include <unistd.h>
#include <vector>

namespace unitTests {

class castor_utils : public ::testing::Test {
protected:

  virtual void SetUp() {
  }

  virtual void TearDown() {
  }
};

/**
 * Tests the good day senario of passing a multi-column string to the
 * splitString() method.
 */
TEST_F(castor_utils, testGoodDaySplitString) {
  using namespace castor::utils;
  const std::string line("col0 col1 col2 col3 col4 col5 col6 col7");
  std::vector<std::string> columns;

  ASSERT_NO_THROW(splitString(line, ' ', columns));
  ASSERT_EQ((std::vector<std::string>::size_type)8, columns.size());
  ASSERT_EQ(std::string("col0"), columns[0]);
  ASSERT_EQ(std::string("col1"), columns[1]);
  ASSERT_EQ(std::string("col2"), columns[2]);
  ASSERT_EQ(std::string("col3"), columns[3]);
  ASSERT_EQ(std::string("col4"), columns[4]);
  ASSERT_EQ(std::string("col5"), columns[5]);
  ASSERT_EQ(std::string("col6"), columns[6]);
  ASSERT_EQ(std::string("col7"), columns[7]);
}

/**
 * Test the case of an empty string being passed to the splitString() method.
 */
TEST_F(castor_utils, testSplitStringWithEmptyString) {
  using namespace castor::utils;
  const std::string emptyString;
  std::vector<std::string> columns;

  ASSERT_NO_THROW(splitString(emptyString, ' ', columns));
  ASSERT_EQ((std::vector<std::string>::size_type)0, columns.size());
}

/**
 * Test the case of a non-empty string containing no separator character
 * passed to the splitString() method.
 */
TEST_F(castor_utils, testSplitStringWithNoSeparatorInString) {
  using namespace castor::utils;
  const std::string stringContainingNoSeparator =
    "stringContainingNoSeparator";
  std::vector<std::string> columns;

  ASSERT_NO_THROW(splitString(stringContainingNoSeparator, ' ', columns));
  ASSERT_EQ((std::vector<std::string>::size_type)1, columns.size());
  ASSERT_EQ(stringContainingNoSeparator, columns[0]);
}

TEST_F(castor_utils, testTrimStringWithEmptyString) {
  using namespace castor::utils;
  const std::string s;
  const std::string trimmedString = trimString(s);
  ASSERT_EQ(s, trimmedString);
}

TEST_F(castor_utils, testTrimStringContainingNoSpaces) {
  using namespace castor::utils;
  const std::string s("NO_SPACES");
  const std::string trimmedString = trimString(s);
  ASSERT_EQ(s, trimmedString);
}

TEST_F(castor_utils, testTrimStringContainingLeftSpace) {
  using namespace castor::utils;
  const std::string s(" VALUE");
  const std::string trimmedString = trimString(s);
  ASSERT_EQ(std::string("VALUE"), trimmedString);
}

TEST_F(castor_utils, testTrimStringContainingRightSpace) {
  using namespace castor::utils;
  const std::string s("VALUE ");
  const std::string trimmedString = trimString(s);
  ASSERT_EQ(std::string("VALUE"), trimmedString);
}

TEST_F(castor_utils, testTrimStringContainingLeftAndRightSpace) {
  using namespace castor::utils;
  const std::string s(" VALUE ");
  const std::string trimmedString = trimString(s);
  ASSERT_EQ(std::string("VALUE"), trimmedString);
}

TEST_F(castor_utils, testTrimStringContainingLeftTab) {
  using namespace castor::utils;
  const std::string s("\tVALUE");
  const std::string trimmedString = trimString(s);
  ASSERT_EQ(std::string("VALUE"), trimmedString);
}

TEST_F(castor_utils, testTrimStringContainingRightTab) {
  using namespace castor::utils;
  const std::string s("VALUE\t");
  const std::string trimmedString = trimString(s);
  ASSERT_EQ(std::string("VALUE"), trimmedString);
}

TEST_F(castor_utils, testTrimStringContainingLeftAndRightTab) {
  using namespace castor::utils;
  const std::string s("\tVALUE\t");
  const std::string trimmedString = trimString(s);
  ASSERT_EQ(std::string("VALUE"), trimmedString);
}

TEST_F(castor_utils, testTrimStringContainingLeftNewLine) {
  using namespace castor::utils;
  const std::string s("\nVALUE");
  const std::string trimmedString = trimString(s);
  ASSERT_EQ(std::string("VALUE"), trimmedString);
}

TEST_F(castor_utils, testTrimStringContainingRightNewLine) {
  using namespace castor::utils;
  const std::string s("VALUE\n");
  const std::string trimmedString = trimString(s);
  ASSERT_EQ(std::string("VALUE"), trimmedString);
}

TEST_F(castor_utils, testTrimStringContainingLeftAndRightNewLine) {
  using namespace castor::utils;
  const std::string s("\nVALUE\n");
  const std::string trimmedString = trimString(s);
  ASSERT_EQ(std::string("VALUE"), trimmedString);
}

TEST_F(castor_utils, testTrimStringContainingLeftAndRightWhiteSpace) {
  using namespace castor::utils;
  const std::string s("  \t\t\n\nVALUE  \t\t\n\n");
  const std::string trimmedString = trimString(s);
  ASSERT_EQ(std::string("VALUE"), trimmedString);
}

TEST_F(castor_utils, testTimevalGreaterThan_BigSecSmallSec_BigUsecSmallUsec) {
  using namespace castor::utils;
  const timeval bigger   = {6, 5};
  const timeval smaller  = {5, 4};
  const bool    expected = true;

  ASSERT_EQ(expected, timevalGreaterThan(bigger, smaller));
}

TEST_F(castor_utils, testTimevalGreaterThan_BigSecSmallSec_BigUsecSmallUsec_swapped) {
  using namespace castor::utils;
  const timeval bigger   = {6, 5};
  const timeval smaller  = {5, 4};
  const bool    expected = false;

  ASSERT_EQ(expected, timevalGreaterThan(smaller, bigger));
}

TEST_F(castor_utils, testTimevalGreaterThan_BigSecSmallSec_SmallUsecBigUsec) {
  using namespace castor::utils;
  const timeval bigger   = {4, 3};
  const timeval smaller  = {2, 7};
  const bool    expected = true;

  ASSERT_EQ(expected, timevalGreaterThan(bigger, smaller));
}

TEST_F(castor_utils, testTimevalGreaterThan_BigSecSmallSec_SmallUsecBigUsec_swapped) {
  using namespace castor::utils;
  const timeval bigger   = {4, 3};
  const timeval smaller  = {2, 7};
  const bool    expected = false;

  ASSERT_EQ(expected, timevalGreaterThan(smaller, bigger));
}

TEST_F(castor_utils, testTimevalGreaterThan_EqualSec_EqualUsec) {
  using namespace castor::utils;
  const timeval a         = {8, 9};
  const timeval b         = {8, 9};
  const bool    expected  = false;

  ASSERT_EQ(expected, timevalGreaterThan(a, b));
}

TEST_F(castor_utils, testTimevalAbsDiff_BigSecSmallSec_BigUsecSmallUsec) {
  using namespace castor::utils;
  const timeval bigger   = {6, 5};
  const timeval smaller  = {5, 4};
  const timeval expected = {1, 1};
  const timeval actual   = timevalAbsDiff(bigger,
    smaller);
  const bool    isAMatch = expected.tv_sec == actual.tv_sec &&
    expected.tv_usec == actual.tv_usec;

  ASSERT_EQ(true, isAMatch);
}

TEST_F(castor_utils, testTimevalAbsDiff_BigSecSmallSec_BigUsecSmallUsec_swapped) {
  using namespace castor::utils;
  const timeval bigger   = {6, 5};
  const timeval smaller  = {5, 4};
  const timeval expected = {1, 1};
  const timeval actual   = timevalAbsDiff(smaller,
    bigger);
  const bool    isAMatch = expected.tv_sec == actual.tv_sec &&
    expected.tv_usec == actual.tv_usec;

  ASSERT_EQ(true, isAMatch);
}

TEST_F(castor_utils, testTimevalAbsDiff_BigSecSmallSec_SmallUsecBigUsec) {
  using namespace castor::utils;
  const timeval bigger   = {4, 3};
  const timeval smaller  = {2, 7};
  const timeval expected = {1, 999996};
  const timeval actual   = timevalAbsDiff(bigger,
    smaller);
  const bool    isAMatch = expected.tv_sec == actual.tv_sec &&
    expected.tv_usec == actual.tv_usec;

  ASSERT_EQ(true, isAMatch);
}

TEST_F(castor_utils, testTimevalAbsDiff_BigSecSmallSec_SmallUsecBigUsec_swapped) {
  using namespace castor::utils;
  const timeval bigger   = {4, 3};
  const timeval smaller  = {2, 7};
  const timeval expected = {1, 999996};
  const timeval actual   = timevalAbsDiff(smaller,
    bigger);
  const bool    isAMatch = expected.tv_sec == actual.tv_sec &&
    expected.tv_usec == actual.tv_usec;

  ASSERT_EQ(true, isAMatch);
}

TEST_F(castor_utils, testTimevalAbsDiff_EqualSec_EqualUsec) {
  using namespace castor::utils;
  const timeval a        = {8, 9};
  const timeval b        = {8, 9};
  const timeval expected = {0, 0};
  const timeval actual   = timevalAbsDiff(a, b);
  const bool    isAMatch = expected.tv_sec == actual.tv_sec &&
    expected.tv_usec == actual.tv_usec;

  ASSERT_EQ(true, isAMatch);
}

TEST_F(castor_utils, testTimevalToDouble) {
  using namespace castor::utils;
  const timeval tv       = {1234, 999992};
  const double  expected = 1234.999992;
  const double  actual   = timevalToDouble(tv);

  ASSERT_EQ(expected, actual);
}

TEST_F(castor_utils, testCopyStringNullDst) {
  using namespace castor::utils;
  char dummy[6] = "Dummy";

  ASSERT_THROW(copyString(NULL, 0, dummy),
    castor::exception::Exception);
}

TEST_F(castor_utils, testCopyString) {
  using namespace castor::utils;
  char src[12]  = "Hello World";
  char dst[12];

  copyString(dst, src);
  ASSERT_EQ(0, strcmp(dst, src));
}

TEST_F(castor_utils, testCheckDgnSyntaxGoodDay) {
  using namespace castor::utils;

  std::ostringstream dgn;
  for(int i=0; i<CA_MAXDGNLEN; i++) {
    dgn << 'D';
  }

  ASSERT_NO_THROW(checkDgnSyntax(dgn.str().c_str()));
}

TEST_F(castor_utils, testCheckDgnSyntaxTooLong) {
  using namespace castor::utils;

  std::ostringstream dgn;
  for(int i=0; i<=CA_MAXDGNLEN; i++) {
    dgn << 'D';
  }

  ASSERT_THROW(checkDgnSyntax(dgn.str().c_str()),
    castor::exception::InvalidArgument);
}

TEST_F(castor_utils, testCheckDgnSyntaxInvalidCharacter) {
  using namespace castor::utils;

  std::ostringstream dgn;
  for(int i=0; i<CA_MAXDGNLEN; i++) {
    dgn << ' '; // Spaces are not allowed
  }

  ASSERT_THROW(checkDgnSyntax(dgn.str().c_str()),
    castor::exception::InvalidArgument);
}

TEST_F(castor_utils, testCheckVidSyntaxGoodDay) {
  using namespace castor::utils;

  std::ostringstream vid;
  for(int i=0; i<CA_MAXVIDLEN; i++) {
    vid << 'V';
  }

  ASSERT_NO_THROW(checkVidSyntax(vid.str().c_str()));
}

TEST_F(castor_utils, testCheckVidSyntaxTooLong) {
  using namespace castor::utils;

  std::ostringstream vid;
  for(int i=0; i<=CA_MAXVIDLEN; i++) {
    vid << 'V';
  }

  ASSERT_THROW(checkVidSyntax(vid.str().c_str()),
    castor::exception::InvalidArgument);
}

TEST_F(castor_utils, testCheckVidSyntaxInvalidCharacter) {
  using namespace castor::utils;

  std::ostringstream vid;
  for(int i=0; i<CA_MAXVIDLEN; i++) {
    vid << ' '; // Spaces are not allowed
  }

  ASSERT_THROW(checkVidSyntax(vid.str().c_str()),
    castor::exception::InvalidArgument);
}

} // namespace unitTests
