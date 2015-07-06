/*
 * The CERN Tape Archive (CTA) project
 * Copyright (C) 2015  CERN
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "common/Utils.hpp"

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
  const std::string trimmedString = Utils::trimSlashes(s);
  ASSERT_EQ(s, trimmedString);
}

TEST_F(cta_UtilsTest, trimSlashes_noSlashes) {
  using namespace cta;

  const std::string s("NO_SLASHES");
  const std::string trimmedString = Utils::trimSlashes(s);
  ASSERT_EQ(s, trimmedString);
}

TEST_F(cta_UtilsTest, trimSlashes_oneLeftSlash) {
  using namespace cta;

  const std::string s("/VALUE");
  const std::string trimmedString = Utils::trimSlashes(s);
  ASSERT_EQ(std::string("VALUE"), trimmedString);
}

TEST_F(cta_UtilsTest, trimSlashes_twoLeftSlashes) {
  using namespace cta;

  const std::string s("//VALUE");
  const std::string trimmedString = Utils::trimSlashes(s);
  ASSERT_EQ(std::string("VALUE"), trimmedString);
}

TEST_F(cta_UtilsTest, trimSlashes_oneRightSlash) {
  using namespace cta;

  const std::string s("VALUE/");
  const std::string trimmedString = Utils::trimSlashes(s);
  ASSERT_EQ(std::string("VALUE"), trimmedString);
}

TEST_F(cta_UtilsTest, trimSlashes_twoRightSlashes) {
  using namespace cta;

  const std::string s("VALUE//");
  const std::string trimmedString = Utils::trimSlashes(s);
  ASSERT_EQ(std::string("VALUE"), trimmedString);
}

TEST_F(cta_UtilsTest,
  trimSlashes_oneLeftAndOneRightSlash) {
  using namespace cta;

  const std::string s("/VALUE/");
  const std::string trimmedString = Utils::trimSlashes(s);
  ASSERT_EQ(std::string("VALUE"), trimmedString);
}

TEST_F(cta_UtilsTest,
  trimSlashes_twoLeftAndTwoRightSlashes) {
  using namespace cta;

  const std::string s("//VALUE//");
  const std::string trimmedString = Utils::trimSlashes(s);
  ASSERT_EQ(std::string("VALUE"), trimmedString);
}

TEST_F(cta_UtilsTest, getEnclosingPath_empty_string) {
  using namespace cta;
    
  const std::string dirPath = "";

  ASSERT_THROW(Utils::getEnclosingPath(dirPath), std::exception);
}

TEST_F(cta_UtilsTest, getEnclosingPath_root) {
  using namespace cta;
    
  const std::string dirPath = "/";

  std::string enclosingPath;
  ASSERT_THROW(enclosingPath = Utils::getEnclosingPath(dirPath),
    std::exception);
}

TEST_F(cta_UtilsTest, getEnclosingPath_grandparent) {
  using namespace cta;

  const std::string dirPath = "/grandparent";
    
  std::string enclosingPath;
  ASSERT_NO_THROW(enclosingPath = Utils::getEnclosingPath(dirPath));
  ASSERT_EQ(std::string("/"), enclosingPath);
}

TEST_F(cta_UtilsTest,
  getEnclosingPath_grandparent_parent) {
  using namespace cta;

  const std::string dirPath = "/grandparent/parent";

  std::string enclosingPath;
  ASSERT_NO_THROW(enclosingPath = Utils::getEnclosingPath(dirPath));
  ASSERT_EQ(std::string("/grandparent/"), enclosingPath);
}

TEST_F(cta_UtilsTest,
  getEnclosingPath_grandparent_parent_child) {
  using namespace cta;

  const std::string dirPath = "/grandparent/parent/child";

  std::string enclosingPath;
  ASSERT_NO_THROW(enclosingPath = Utils::getEnclosingPath(dirPath));
  ASSERT_EQ(std::string("/grandparent/parent/"), enclosingPath);
}

TEST_F(cta_UtilsTest, getEnclosedName_just_enclosed_name) {
  using namespace cta;

  const std::string enclosedName = "child";
  std::string result;
  ASSERT_NO_THROW(result = Utils::getEnclosedName(enclosedName));
  ASSERT_EQ(enclosedName, result);
}

TEST_F(cta_UtilsTest, getEnclosedName) {
  using namespace cta;

  const std::string enclosingPath = "/grandparent/parent/";
  const std::string enclosedName = "child";
  const std::string absoluteFilePath = enclosingPath + enclosedName;
  std::string result;
  ASSERT_NO_THROW(result = Utils::getEnclosedName(absoluteFilePath));
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
  ASSERT_NO_THROW(results = Utils::getEnclosedNames(absoluteFilePaths));
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

  ASSERT_NO_THROW(Utils::splitString(line, ' ', columns));
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


  ASSERT_NO_THROW(Utils::splitString(emptyString, ' ', columns));
  ASSERT_EQ((std::vector<std::string>::size_type)0, columns.size());
}

TEST_F(cta_UtilsTest, splitString_noSeparatorInString) {
  using namespace cta;
  const std::string stringContainingNoSeparator =
    "stringContainingNoSeparator";
  std::vector<std::string> columns;

  ASSERT_NO_THROW(Utils::splitString(stringContainingNoSeparator, ' ',
    columns));
  ASSERT_EQ((std::vector<std::string>::size_type)1, columns.size());
  ASSERT_EQ(stringContainingNoSeparator, columns[0]);
}

TEST_F(cta_UtilsTest, generateUuid) {
  using namespace cta;
  std::string uuid1;
  std::string uuid2;
  ASSERT_NO_THROW(uuid1 = Utils::generateUuid());
  ASSERT_NO_THROW(uuid2 = Utils::generateUuid());
  ASSERT_NE(uuid1, uuid2);
}

TEST_F(cta_UtilsTest, endsWith_slash_empty_string) {
  using namespace cta;
  const std::string str = "";
  ASSERT_FALSE(Utils::endsWith(str, '/'));
}

TEST_F(cta_UtilsTest, endsWith_slash_non_empty_string_without_terminating_slash) {
  using namespace cta;
  const std::string str = "abcde";
  ASSERT_FALSE(Utils::endsWith(str, '/'));
} 

TEST_F(cta_UtilsTest, endsWith_slash_non_empty_string_with_terminating_slash) {
  using namespace cta;
  const std::string str = "abcde/";
  ASSERT_TRUE(Utils::endsWith(str, '/'));
}

TEST_F(cta_UtilsTest, endsWith_slash_just_a_slash) {
  using namespace cta;
  const std::string str = "/";
  ASSERT_TRUE(Utils::endsWith(str, '/'));
} 

TEST_F(cta_UtilsTest, errnoToString_EACCESS) {
  using namespace cta;

  const std::string str = Utils::errnoToString(EACCES);
  ASSERT_EQ(std::string("Permission denied"), str);
}

TEST_F(cta_UtilsTest, toUint16_uint16_t) {
  using namespace cta;

  uint16_t i = 0;

  ASSERT_NO_THROW(i = Utils::toUint16("12345"));
  ASSERT_EQ((uint16_t)12345, i);
}

TEST_F(cta_UtilsTest, toUint16_zero) {
  using namespace cta;

  uint16_t i = 0;

  ASSERT_NO_THROW(i = Utils::toUint16("0"));
  ASSERT_EQ((uint16_t)0, i);
}

TEST_F(cta_UtilsTest, toUint16_65535) {
  using namespace cta;

  uint16_t i = 0;

  ASSERT_NO_THROW(i = Utils::toUint16("65535"));
  ASSERT_EQ((uint16_t)65535, i);
}

TEST_F(cta_UtilsTest, toUint16_empty_string) {
  using namespace cta;

  ASSERT_THROW(Utils::toUint16(""), std::exception);
}

TEST_F(cta_UtilsTest, toUint16_negative) {
  using namespace cta;

  ASSERT_THROW(Utils::toUint16("-12345"), std::exception);
}

TEST_F(cta_UtilsTest, toUint16_too_big) {
  using namespace cta;

  ASSERT_THROW(Utils::toUint16("65536"), std::exception);
}

TEST_F(cta_UtilsTest, isValidUInt_unsigned_int) {
  using namespace cta;

  ASSERT_TRUE(Utils::isValidUInt("12345"));
}

TEST_F(cta_UtilsTest, isValidUInt_empty_string) {
  using namespace cta;

  ASSERT_FALSE(Utils::isValidUInt(""));
}

TEST_F(cta_UtilsTest, isValidUInt_signed_int) {
  using namespace cta;

  ASSERT_FALSE(Utils::isValidUInt("-12345"));
}

TEST_F(cta_UtilsTest, isValidUInt_not_a_number) {
  using namespace cta;

  ASSERT_FALSE(Utils::isValidUInt("one"));
}

} // namespace unitTests
