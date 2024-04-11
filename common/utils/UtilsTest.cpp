/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
 */

#include "common/utils/utils.hpp"
#include "common/exception/Exception.hpp"

#include <gtest/gtest.h>

namespace unitTests {

class cta_UtilsTest: public ::testing::Test {
protected:

  virtual void SetUp() {
  }

  virtual void TearDown() {
  }
};

TEST_F(cta_UtilsTest, trimSlashes_emptyString) {
  using namespace cta;

  const std::string s;
  const std::string trimmedString = utils::trimSlashes(s);
  ASSERT_EQ(s, trimmedString);
}

TEST_F(cta_UtilsTest, trimSlashes_noSlashes) {
  using namespace cta;

  const std::string s("NO_SLASHES");
  const std::string trimmedString = utils::trimSlashes(s);
  ASSERT_EQ(s, trimmedString);
}

TEST_F(cta_UtilsTest, trimSlashes_oneLeftSlash) {
  using namespace cta;

  const std::string s("/VALUE");
  const std::string trimmedString = utils::trimSlashes(s);
  ASSERT_EQ(std::string("VALUE"), trimmedString);
}

TEST_F(cta_UtilsTest, trimSlashes_twoLeftSlashes) {
  using namespace cta;

  const std::string s("//VALUE");
  const std::string trimmedString = utils::trimSlashes(s);
  ASSERT_EQ(std::string("VALUE"), trimmedString);
}

TEST_F(cta_UtilsTest, trimSlashes_oneRightSlash) {
  using namespace cta;

  const std::string s("VALUE/");
  const std::string trimmedString = utils::trimSlashes(s);
  ASSERT_EQ(std::string("VALUE"), trimmedString);
}

TEST_F(cta_UtilsTest, trimSlashes_twoRightSlashes) {
  using namespace cta;

  const std::string s("VALUE//");
  const std::string trimmedString = utils::trimSlashes(s);
  ASSERT_EQ(std::string("VALUE"), trimmedString);
}

TEST_F(cta_UtilsTest,
  trimSlashes_oneLeftAndOneRightSlash) {
  using namespace cta;

  const std::string s("/VALUE/");
  const std::string trimmedString = utils::trimSlashes(s);
  ASSERT_EQ(std::string("VALUE"), trimmedString);
}

TEST_F(cta_UtilsTest,
  trimSlashes_twoLeftAndTwoRightSlashes) {
  using namespace cta;

  const std::string s("//VALUE//");
  const std::string trimmedString = utils::trimSlashes(s);
  ASSERT_EQ(std::string("VALUE"), trimmedString);
}

TEST_F(cta_UtilsTest, getEnclosingPath_empty_string) {
  using namespace cta;

  const std::string dirPath = "";

  ASSERT_THROW(utils::getEnclosingPath(dirPath), std::exception);
}

TEST_F(cta_UtilsTest, getEnclosingPath_root) {
  using namespace cta;

  const std::string dirPath = "/";

  std::string enclosingPath;
  ASSERT_THROW(enclosingPath = utils::getEnclosingPath(dirPath),
    std::exception);
}

TEST_F(cta_UtilsTest, getEnclosingPath_grandparent) {
  using namespace cta;

  const std::string dirPath = "/grandparent";

  std::string enclosingPath;
  ASSERT_NO_THROW(enclosingPath = utils::getEnclosingPath(dirPath));
  ASSERT_EQ(std::string("/"), enclosingPath);
}

TEST_F(cta_UtilsTest,
  getEnclosingPath_grandparent_parent) {
  using namespace cta;

  const std::string dirPath = "/grandparent/parent";

  std::string enclosingPath;
  ASSERT_NO_THROW(enclosingPath = utils::getEnclosingPath(dirPath));
  ASSERT_EQ(std::string("/grandparent/"), enclosingPath);
}

TEST_F(cta_UtilsTest,
  getEnclosingPath_grandparent_parent_child) {
  using namespace cta;

  const std::string dirPath = "/grandparent/parent/child";

  std::string enclosingPath;
  ASSERT_NO_THROW(enclosingPath = utils::getEnclosingPath(dirPath));
  ASSERT_EQ(std::string("/grandparent/parent/"), enclosingPath);
}

TEST_F(cta_UtilsTest, getEnclosedName_just_enclosed_name) {
  using namespace cta;

  const std::string enclosedName = "child";
  std::string result;
  ASSERT_NO_THROW(result = utils::getEnclosedName(enclosedName));
  ASSERT_EQ(enclosedName, result);
}

TEST_F(cta_UtilsTest, getEnclosedName) {
  using namespace cta;

  const std::string enclosingPath = "/grandparent/parent/";
  const std::string enclosedName = "child";
  const std::string absoluteFilePath = enclosingPath + enclosedName;
  std::string result;
  ASSERT_NO_THROW(result = utils::getEnclosedName(absoluteFilePath));
  ASSERT_EQ(enclosedName, result);
}

TEST_F(cta_UtilsTest, getEnclosedNames) {
  using namespace cta;

  const std::string enclosingPath = "/grandparent/parent/";
  const std::string enclosedName1 = "child1";
  const std::string enclosedName2 = "child2";
  const std::string enclosedName3 = "child3";
  const std::string enclosedName4 = "child4";
  std::list<std::string> absoluteFilePaths;
  absoluteFilePaths.push_back(enclosingPath + enclosedName1);
  absoluteFilePaths.push_back(enclosingPath + enclosedName2);
  absoluteFilePaths.push_back(enclosingPath + enclosedName3);
  absoluteFilePaths.push_back(enclosingPath + enclosedName4);
  std::list<std::string> results;
  ASSERT_NO_THROW(results = utils::getEnclosedNames(absoluteFilePaths));
  ASSERT_EQ(4, results.size());
  std::set<std::string> resultSet;
  for(std::list<std::string>::const_iterator itor = results.begin();
    itor != results.end(); itor++) {
    resultSet.insert(*itor);
  }
  ASSERT_EQ(4, resultSet.size());
  ASSERT_FALSE(resultSet.find(enclosedName1) == resultSet.end());
  ASSERT_FALSE(resultSet.find(enclosedName2) == resultSet.end());
  ASSERT_FALSE(resultSet.find(enclosedName3) == resultSet.end());
  ASSERT_FALSE(resultSet.find(enclosedName4) == resultSet.end());
}

TEST_F(cta_UtilsTest, splitString_goodDay) {
  using namespace cta;
  const std::string line("col0 col1 col2 col3 col4 col5 col6 col7");
  std::vector<std::string> columns;

  ASSERT_NO_THROW(utils::splitString(line, ' ', columns));
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

TEST_F(cta_UtilsTest, splitString_emptyString) {
  using namespace cta;
  const std::string emptyString;
  std::vector<std::string> columns;


  ASSERT_NO_THROW(utils::splitString(emptyString, ' ', columns));
  ASSERT_EQ((std::vector<std::string>::size_type)0, columns.size());
}

TEST_F(cta_UtilsTest, splitString_noSeparatorInString) {
  using namespace cta;
  const std::string stringContainingNoSeparator =
    "stringContainingNoSeparator";
  std::vector<std::string> columns;

  ASSERT_NO_THROW(utils::splitString(stringContainingNoSeparator, ' ',
    columns));
  ASSERT_EQ((std::vector<std::string>::size_type)1, columns.size());
  ASSERT_EQ(stringContainingNoSeparator, columns[0]);
}

TEST_F(cta_UtilsTest, generateUuid) {
  using namespace cta;
  std::string uuid1;
  std::string uuid2;
  ASSERT_NO_THROW(uuid1 = utils::generateUuid());
  ASSERT_NO_THROW(uuid2 = utils::generateUuid());
  ASSERT_NE(uuid1, uuid2);
}

TEST_F(cta_UtilsTest, endsWith_slash_empty_string) {
  using namespace cta;
  const std::string str = "";
  ASSERT_FALSE(utils::endsWith(str, '/'));
}

TEST_F(cta_UtilsTest, endsWith_slash_non_empty_string_without_terminating_slash) {
  using namespace cta;
  const std::string str = "abcde";
  ASSERT_FALSE(utils::endsWith(str, '/'));
}

TEST_F(cta_UtilsTest, endsWith_slash_non_empty_string_with_terminating_slash) {
  using namespace cta;
  const std::string str = "abcde/";
  ASSERT_TRUE(utils::endsWith(str, '/'));
}

TEST_F(cta_UtilsTest, endsWith_slash_just_a_slash) {
  using namespace cta;
  const std::string str = "/";
  ASSERT_TRUE(utils::endsWith(str, '/'));
}

TEST_F(cta_UtilsTest, errnoToString_EACCESS) {
  using namespace cta;

  const std::string str = utils::errnoToString(EACCES);
  ASSERT_EQ(std::string("Permission denied"), str);
}

TEST_F(cta_UtilsTest, toUint8_123) {
  using namespace cta;

  uint8_t i = 0;

  ASSERT_NO_THROW(i = utils::toUint8("123"));
  ASSERT_EQ((uint8_t)123, i);
}

TEST_F(cta_UtilsTest, toUint8_zero) {
  using namespace cta;

  uint8_t i = 0;

  ASSERT_NO_THROW(i = utils::toUint8("0"));
  ASSERT_EQ((uint8_t)0, i);
}

TEST_F(cta_UtilsTest, toUint8_255) {
  using namespace cta;

  uint8_t i = 0;

  ASSERT_NO_THROW(i = utils::toUint8("255"));
  ASSERT_EQ((uint8_t)255, i);
}

TEST_F(cta_UtilsTest, toUint8_empty_string) {
  using namespace cta;

  ASSERT_THROW(utils::toUint8(""), std::exception);
}

TEST_F(cta_UtilsTest, toUint8_negative) {
  using namespace cta;

  ASSERT_THROW(utils::toUint8("-123"), std::exception);
}

TEST_F(cta_UtilsTest, toUint8_too_big) {
  using namespace cta;

  ASSERT_THROW(utils::toUint8("256"), std::exception);
}

TEST_F(cta_UtilsTest, toUint16_12345) {
  using namespace cta;

  uint16_t i = 0;

  ASSERT_NO_THROW(i = utils::toUint16("12345"));
  ASSERT_EQ((uint16_t)12345, i);
}

TEST_F(cta_UtilsTest, toUint16_zero) {
  using namespace cta;

  uint16_t i = 0;

  ASSERT_NO_THROW(i = utils::toUint16("0"));
  ASSERT_EQ((uint16_t)0, i);
}

TEST_F(cta_UtilsTest, toUint16_65535) {
  using namespace cta;

  uint16_t i = 0;

  ASSERT_NO_THROW(i = utils::toUint16("65535"));
  ASSERT_EQ((uint16_t)65535, i);
}

TEST_F(cta_UtilsTest, toUint16_empty_string) {
  using namespace cta;

  ASSERT_THROW(utils::toUint16(""), std::exception);
}

TEST_F(cta_UtilsTest, toUint16_negative) {
  using namespace cta;

  ASSERT_THROW(utils::toUint16("-12345"), std::exception);
}

TEST_F(cta_UtilsTest, toUint16_too_big) {
  using namespace cta;

  ASSERT_THROW(utils::toUint16("65536"), std::exception);
}

TEST_F(cta_UtilsTest, toUint32_12345) {
  using namespace cta;

  uint32_t i = 0;

  ASSERT_NO_THROW(i = utils::toUint32("12345"));
  ASSERT_EQ((uint32_t)12345, i);
}

TEST_F(cta_UtilsTest, toUint32_zero) {
  using namespace cta;

  uint32_t i = 0;

  ASSERT_NO_THROW(i = utils::toUint32("0"));
  ASSERT_EQ((uint32_t)0, i);
}

TEST_F(cta_UtilsTest, toUint32_4294967295) {
  using namespace cta;

  uint32_t i = 0;

  ASSERT_NO_THROW(i = utils::toUint32("4294967295"));
  ASSERT_EQ((uint32_t)4294967295, i);
}

TEST_F(cta_UtilsTest, toUint32_empty_string) {
  using namespace cta;

  ASSERT_THROW(utils::toUint32(""), std::exception);
}

TEST_F(cta_UtilsTest, toUint32_negative) {
  using namespace cta;

  ASSERT_THROW(utils::toUint32("-12345"), std::exception);
}

TEST_F(cta_UtilsTest, toUint32_too_big) {
  using namespace cta;

  ASSERT_THROW(utils::toUint32("4294967296"), std::exception);
}

TEST_F(cta_UtilsTest, toUid_12345) {
  using namespace cta;

  uid_t i = 0;

  ASSERT_NO_THROW(i = utils::toUid("12345"));
  ASSERT_EQ((uid_t)12345, i);
}

TEST_F(cta_UtilsTest, toUid_zero) {
  using namespace cta;

  uid_t i = 0;

  ASSERT_NO_THROW(i = utils::toUid("0"));
  ASSERT_EQ((uid_t)0, i);
}

TEST_F(cta_UtilsTest, toUid_max) {
  using namespace cta;

  std::ostringstream oss;
  oss << std::numeric_limits<uid_t>::max();

  uid_t i = 0;

  ASSERT_NO_THROW(i = utils::toUid(oss.str()));
  ASSERT_EQ(std::numeric_limits<uid_t>::max(), i);
}

TEST_F(cta_UtilsTest, toUid_empty_string) {
  using namespace cta;

  ASSERT_THROW(utils::toUid(""), std::exception);
}

TEST_F(cta_UtilsTest, toUid_negative) {
  using namespace cta;

  ASSERT_THROW(utils::toUid("-12345"), std::exception);
}

TEST_F(cta_UtilsTest, toUid_too_big) {
  using namespace cta;

  const uint64_t tooBig = (uint64_t)(std::numeric_limits<uid_t>::max()) +
    (uint64_t)1;

  std::ostringstream oss;
  oss << tooBig;

  ASSERT_THROW(utils::toUid(oss.str()), std::exception);
}

TEST_F(cta_UtilsTest, toGid_12345) {
  using namespace cta;

  gid_t i = 0;

  ASSERT_NO_THROW(i = utils::toGid("12345"));
  ASSERT_EQ((gid_t)12345, i);
}

TEST_F(cta_UtilsTest, toGid_zero) {
  using namespace cta;

  gid_t i = 0;

  ASSERT_NO_THROW(i = utils::toGid("0"));
  ASSERT_EQ((gid_t)0, i);
}

TEST_F(cta_UtilsTest, toGid_max) {
  using namespace cta;

  std::ostringstream oss;
  oss << std::numeric_limits<gid_t>::max();

  gid_t i = 0;

  ASSERT_NO_THROW(i = utils::toGid(oss.str()));
  ASSERT_EQ(std::numeric_limits<gid_t>::max(), i);
}

TEST_F(cta_UtilsTest, toGid_empty_string) {
  using namespace cta;

  ASSERT_THROW(utils::toGid(""), std::exception);
}

TEST_F(cta_UtilsTest, toGid_negative) {
  using namespace cta;

  ASSERT_THROW(utils::toGid("-12345"), std::exception);
}

TEST_F(cta_UtilsTest, toGid_too_big) {
  using namespace cta;

  const uint64_t tooBig = (uint64_t)(std::numeric_limits<gid_t>::max()) +
    (uint64_t)1;

  std::ostringstream oss;
  oss << tooBig;

  ASSERT_THROW(utils::toGid(oss.str()), std::exception);
}

TEST_F(cta_UtilsTest, isValidUInt) {
  using namespace cta;

  ASSERT_TRUE(utils::isValidUInt("12345"));
}

TEST_F(cta_UtilsTest, isValidUInt_empty_string) {
  using namespace cta;

  ASSERT_FALSE(utils::isValidUInt(""));
}

TEST_F(cta_UtilsTest, isValidUInt_negative) {
  using namespace cta;

  ASSERT_FALSE(utils::isValidUInt("-12345"));
}

TEST_F(cta_UtilsTest, isValidUInt_not_a_number) {
  using namespace cta;

  ASSERT_FALSE(utils::isValidUInt("one"));
}

TEST_F(cta_UtilsTest, toUint64) {
  using namespace cta;

  ASSERT_EQ((uint64_t)12345, utils::toUint64("12345"));
  ASSERT_EQ((uint64_t)18446744073709551615ULL, utils::toUint64("18446744073709551615"));
}

TEST_F(cta_UtilsTest, toUint64_too_big) {
  using namespace cta;

  ASSERT_THROW(utils::toUint64("18446744073709551616"), exception::Exception);
}

TEST_F(cta_UtilsTest, toUint64_empty_string) {
  using namespace cta;

  ASSERT_THROW(utils::toUint64(""), exception::Exception);
}

TEST_F(cta_UtilsTest, toUint64_minus_one) {
  using namespace cta;

  ASSERT_EQ((uint64_t)18446744073709551615UL, utils::toUint64("18446744073709551615"));
}

TEST_F(cta_UtilsTest, toUint64_not_a_number) {
  using namespace cta;

  ASSERT_THROW(utils::toUint64("one"), exception::Exception);
}

TEST_F(cta_UtilsTest, isValidDecimal) {
  using namespace cta;

  ASSERT_TRUE(utils::isValidDecimal("1.234"));
}

TEST_F(cta_UtilsTest, isValidDecimal_empty_string) {
  using namespace cta;

  ASSERT_FALSE(utils::isValidDecimal(""));
}

TEST_F(cta_UtilsTest, isValidDecimal_negative_double) {
  using namespace cta;

  ASSERT_TRUE(utils::isValidDecimal("-1.234"));
}

TEST_F(cta_UtilsTest, isValidDecimal_not_a_number) {
  using namespace cta;

  ASSERT_FALSE(utils::isValidDecimal("one"));
}

TEST_F(cta_UtilsTest, isValidDecimal_two_decimal_points) {
  using namespace cta;

  ASSERT_FALSE(utils::isValidDecimal("1.2.34"));
}

TEST_F(cta_UtilsTest, toDouble_double) {
  using namespace cta;

  ASSERT_EQ((double)1.234, utils::toDouble("1.234"));
}

TEST_F(cta_UtilsTest, toDouble_negative_double) {
  using namespace cta;

  ASSERT_EQ((double)-1.234, utils::toDouble("-1.234"));
}

TEST_F(cta_UtilsTest, toDouble_too_big) {
  using namespace cta;

  // std::numeric_limits<double>::max=1.79769e+308
  ASSERT_THROW(utils::toDouble("1.79770e+308"), exception::Exception);
}

TEST_F(cta_UtilsTest, toDouble_empty_string) {
  using namespace cta;

  ASSERT_THROW(utils::toDouble(""), exception::Exception);
}

TEST_F(cta_UtilsTest, toDouble_not_a_number) {
  using namespace cta;

  ASSERT_THROW(utils::toDouble("one"), exception::Exception);
}

TEST_F(cta_UtilsTest, adler32_empty_buf) {
  using namespace cta;

  // The adler32 of an empty buffer is 1
  ASSERT_EQ((uint32_t)1, utils::getAdler32(nullptr, 0));
}

TEST_F(cta_UtilsTest, adler32_buf_of_character_1) {
  using namespace cta;

  const uint8_t buf = '1';
  ASSERT_EQ((uint32_t)0x320032, utils::getAdler32(&buf, 1));
}

TEST_F(cta_UtilsTest, toUpper) {
  using namespace cta;

  std::string testStr = "testStr123-";

  ASSERT_NO_THROW(utils::toUpper(testStr));
  ASSERT_EQ(testStr, "TESTSTR123-");
}

TEST_F(cta_UtilsTest, toLower) {
  using namespace cta;

  std::string testStr = "TESTsTR123-";

  ASSERT_NO_THROW(utils::toLower(testStr));
  ASSERT_EQ(testStr, "teststr123-");
}

/**
 * Tests the good day senario of passing a multi-column string to the
 * splitString() method.
 */
TEST_F(cta_UtilsTest, testGoodDaySplitString) {
  using namespace cta;
  const std::string line("col0 col1 col2 col3 col4 col5 col6 col7");
  std::vector<std::string> columns;

  ASSERT_NO_THROW(utils::splitString(line, ' ', columns));
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
 * Tests passing only separators to the splitString() method.
 */
TEST_F(cta_UtilsTest, testSplitStringOnlySeparators) {
  using namespace cta;
  const std::string line(":::::");
  std::vector<std::string> columns;

  ASSERT_NO_THROW(utils::splitString(line, ':', columns));
  ASSERT_EQ((std::vector<std::string>::size_type)6, columns.size());
  for(uint32_t i = 0 ; i < 6; i++) {
    ASSERT_TRUE(columns[i].empty());
  }
}

/**
 * Test the case of an empty string being passed to the splitString() method.
 */
TEST_F(cta_UtilsTest, testSplitStringWithEmptyString) {
  using namespace cta;
  const std::string emptyString;
  std::vector<std::string> columns;

  ASSERT_NO_THROW(utils::splitString(emptyString, ' ', columns));
  ASSERT_EQ((std::vector<std::string>::size_type)0, columns.size());
}

/**
 * Test the case of a non-empty string containing no separator character
 * passed to the splitString() method.
 */
TEST_F(cta_UtilsTest, testSplitStringWithNoSeparatorInString) {
  using namespace cta;
  const std::string stringContainingNoSeparator =
    "stringContainingNoSeparator";
  std::vector<std::string> columns;

  ASSERT_NO_THROW(utils::splitString(stringContainingNoSeparator, ' ', columns));
  ASSERT_EQ((std::vector<std::string>::size_type)1, columns.size());
  ASSERT_EQ(stringContainingNoSeparator, columns[0]);
}

TEST_F(cta_UtilsTest, testTrimStringWithEmptyString) {
  using namespace cta;
  const std::string s;
  const std::string trimmedString = utils::trimString(s);
  ASSERT_EQ(s, trimmedString);
}

TEST_F(cta_UtilsTest, testTrimStringContainingNoSpaces) {
  using namespace cta;
  const std::string s("NO_SPACES");
  const std::string trimmedString = utils::trimString(s);
  ASSERT_EQ(s, trimmedString);
}

TEST_F(cta_UtilsTest, testTrimStringContainingLeftSpace) {
  using namespace cta;
  const std::string s(" VALUE");
  const std::string trimmedString = utils::trimString(s);
  ASSERT_EQ(std::string("VALUE"), trimmedString);
}

TEST_F(cta_UtilsTest, testTrimStringContainingRightSpace) {
  using namespace cta;
  const std::string s("VALUE ");
  const std::string trimmedString = utils::trimString(s);
  ASSERT_EQ(std::string("VALUE"), trimmedString);
}

TEST_F(cta_UtilsTest, testTrimStringContainingLeftAndRightSpace) {
  using namespace cta;
  const std::string s(" VALUE ");
  const std::string trimmedString = utils::trimString(s);
  ASSERT_EQ(std::string("VALUE"), trimmedString);
}

TEST_F(cta_UtilsTest, testTrimStringContainingLeftTab) {
  using namespace cta;
  const std::string s("\tVALUE");
  const std::string trimmedString = utils::trimString(s);
  ASSERT_EQ(std::string("VALUE"), trimmedString);
}

TEST_F(cta_UtilsTest, testTrimStringContainingRightTab) {
  using namespace cta;
  const std::string s("VALUE\t");
  const std::string trimmedString = utils::trimString(s);
  ASSERT_EQ(std::string("VALUE"), trimmedString);
}

TEST_F(cta_UtilsTest, testTrimStringContainingLeftAndRightTab) {
  using namespace cta;
  const std::string s("\tVALUE\t");
  const std::string trimmedString = utils::trimString(s);
  ASSERT_EQ(std::string("VALUE"), trimmedString);
}

TEST_F(cta_UtilsTest, testTrimStringContainingLeftNewLine) {
  using namespace cta;
  const std::string s("\nVALUE");
  const std::string trimmedString = utils::trimString(s);
  ASSERT_EQ(std::string("VALUE"), trimmedString);
}

TEST_F(cta_UtilsTest, testTrimStringContainingRightNewLine) {
  using namespace cta;
  const std::string s("VALUE\n");
  const std::string trimmedString = utils::trimString(s);
  ASSERT_EQ(std::string("VALUE"), trimmedString);
}

TEST_F(cta_UtilsTest, testTrimStringContainingLeftAndRightNewLine) {
  using namespace cta;
  const std::string s("\nVALUE\n");
  const std::string trimmedString = utils::trimString(s);
  ASSERT_EQ(std::string("VALUE"), trimmedString);
}

TEST_F(cta_UtilsTest, testTrimStringContainingLeftAndRightWhiteSpace) {
  using namespace cta;
  const std::string s("  \t\t\n\nVALUE  \t\t\n\n");
  const std::string trimmedString = utils::trimString(s);
  ASSERT_EQ(std::string("VALUE"), trimmedString);
}

TEST_F(cta_UtilsTest, testTrimStringContainingOnlySpace) {
  using namespace cta;
  const std::string s("     ");
  const std::string trimmedString = utils::trimString(s);
  ASSERT_EQ(std::string(""), trimmedString);
}

TEST_F(cta_UtilsTest, testTrimStringOneCharacter) {
  using namespace cta;
  const std::string s(" a ");
  const std::string trimmedString = utils::trimString(s);
  ASSERT_EQ(1,trimmedString.size());
  ASSERT_EQ(std::string("a"), trimmedString);
}

TEST_F(cta_UtilsTest, testCopyStringNullDst) {
  using namespace cta;
  char dummy[6] = "Dummy";

  ASSERT_THROW(utils::copyString(nullptr, 0, dummy),
    cta::exception::Exception);
}

TEST_F(cta_UtilsTest, testCopyString) {
  using namespace cta;
  char src[12]  = "Hello World";
  char dst[12];

  utils::copyString(dst, src);
  ASSERT_EQ(0, strcmp(dst, src));
}

TEST_F(cta_UtilsTest, ellipses) {
  using namespace cta::utils;

  ASSERT_EQ("1234567890", postEllipsis("1234567890", 12));
  ASSERT_EQ("1234567[...]", postEllipsis("1234567890ABCDEF", 12));
  ASSERT_EQ("1234567890", midEllipsis("1234567890", 12));
  ASSERT_EQ("123[...]CDEF", midEllipsis("1234567890ABCDEF", 12));
  ASSERT_EQ("1[...]ABCDEF", midEllipsis("1234567890ABCDEF", 12, 1));
  ASSERT_EQ("[...]0ABCDEF", preEllipsis("1234567890ABCDEF", 12));
}

TEST_F(cta_UtilsTest, DISABLED_currentTime) {
  /* This test is disabled as it prints our similar, yet slightly different dates,
   * so it would be complex to automate.
   * Just run with: "cta-unitTests --gtest_filter=*currentTime --gtest_also_run_disabled_tests" */
  using namespace cta::utils;
  ::system("date \"+%h %e %H:%M:%S.%N\" ");
  std::cout << getCurrentLocalTime() << std::endl;
}

TEST_F(cta_UtilsTest, searchAndReplace) {
  using namespace cta::utils;

  std::string str = "one two three four one two three four";
  const std::string search = "two";
  const std::string replacement = "replacement";

  searchAndReplace(str, search, replacement);

  ASSERT_EQ("one replacement three four one replacement three four", str);
}

TEST_F(cta_UtilsTest, appendParameterXRootFileURL){
  std::string fileURLTest = "root://ctaeos.cta.svc.cluster.local//eos/ctaeos/preprod/79fe26de-6b8b-437c-b507-06dbfe8d0a79/0/test00000171?eos.lfn=fxid:b2&eos.ruid=0&eos.rgid=0&eos.injection=1&eos.workflow=retrieve_written&eos.space=default&oss.asize=15360";
  std::string fileURL = fileURLTest;
  cta::utils::appendParameterXRootFileURL(fileURL,"oss.asize","145");
  //nothing should have changed
  ASSERT_EQ(fileURLTest,fileURL);

  fileURLTest = "root://ctaeos.cta.svc.cluster.local//eos/ctaeos/preprod/79fe26de-6b8b-437c-b507-06dbfe8d0a79/0/test00000171";
  fileURL = fileURLTest;
  cta::utils::appendParameterXRootFileURL(fileURL,"oss.asize","15360");
  ASSERT_EQ(fileURLTest+"?oss.asize=15360",fileURL);

  fileURLTest = "file://path_to_folder/path_to_file";
  fileURL = fileURLTest;
  cta::utils::appendParameterXRootFileURL(fileURL,"oss.asize","15360");
  ASSERT_EQ(fileURLTest,fileURL);

  fileURLTest = "root://ctaeos.cta.svc.cluster.local//eos/ctaeos/preprod/79fe26de-6b8b-437c-b507-06dbfe8d0a79/0/test00000171?eos.lfn=fxid:b2&eos.ruid=0&eos.rgid=0&eos.injection=1&eos.workflow=retrieve_written&eos.space=default";
  fileURL = fileURLTest;
  cta::utils::appendParameterXRootFileURL(fileURL,"oss.asize","15360");
  ASSERT_EQ(fileURLTest+"&oss.asize=15360",fileURL);
}

TEST_F(cta_UtilsTest, IsValidUUID) {
  // Test with valid UUID
  std::string validUUID = "123e4567-e89b-12d3-a456-426614174000";
  EXPECT_TRUE(cta::utils::isValidUUID(validUUID));

  // Test with invalid UUID (length not correct)
  std::string invalidUUID1 = "123e4567-e89b-12d3-a456-42661417400";
  EXPECT_FALSE(cta::utils::isValidUUID(invalidUUID1));

  // Test with invalid UUID (characters not correct)
  std::string invalidUUID2 = "123e4567-e89b-12d3-a456-42661417400G";
  EXPECT_FALSE(cta::utils::isValidUUID(invalidUUID2));
}

TEST_F(cta_UtilsTest, IsValidHex) {
  // Test with valid hexadecimal
  std::string validHex1 = "1A2B3C4D";
  EXPECT_TRUE(cta::utils::isValidHex(validHex1));

  // Test with valid hexadecimal
  std::string validHex2 = "0x1A2B3C4D";
  EXPECT_TRUE(cta::utils::isValidHex(validHex2));

  // Test with invalid hexadecimal (contains non-hex characters)
  std::string invalidHex1 = "1A2B3C4G";
  EXPECT_FALSE(cta::utils::isValidHex(invalidHex1));

  // Test with empty string
  std::string invalidHex2 = "";
  EXPECT_FALSE(cta::utils::isValidHex(invalidHex2));
}

TEST_F(cta_UtilsTest, IsValidID) {
  // Test with valid decimal ID
  std::string validDecimalID = "1234567890";
  EXPECT_TRUE(cta::utils::isValidID(validDecimalID));

  // Test with valid UUID ID
  std::string validUUIDID = "123e4567-e89b-12d3-a456-426614174000";
  EXPECT_TRUE(cta::utils::isValidID(validUUIDID));

  // Test with valid hexadecimal ID
  std::string validHexID = "0x1A2B3C4D";
  EXPECT_TRUE(cta::utils::isValidID(validHexID));

  // Test with invalid ID (contains non-decimal, non-hex characters)
  std::string invalidID1 = "1A2B3C4G";
  EXPECT_FALSE(cta::utils::isValidID(invalidID1));

  // Test with empty string
  std::string invalidID2 = "";
  EXPECT_FALSE(cta::utils::isValidID(invalidID2));
}

} // namespace unitTests
