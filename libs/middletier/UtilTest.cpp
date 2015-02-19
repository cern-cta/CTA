#include "UtilsTest.hpp"

#include <gtest/gtest.h>

namespace unitTests {

class cta_client_UtilsTest: public ::testing::Test {
protected:

  virtual void SetUp() {
  }

  virtual void TearDown() {
  }
};

TEST_F(cta_client_UtilsTest, trimSlashes_emptyString) {
  using namespace cta;

  TestingMockMiddleTierAdmin api;
  const std::string s;
  const std::string trimmedString = api.trimSlashes(s);
  ASSERT_EQ(s, trimmedString);
}

TEST_F(cta_client_UtilsTest, trimSlashes_noSlashes) {
  using namespace cta;

  TestingMockMiddleTierAdmin api;
  const std::string s("NO_SLASHES");
  const std::string trimmedString = api.trimSlashes(s);
  ASSERT_EQ(s, trimmedString);
}

TEST_F(cta_client_UtilsTest, trimSlashes_oneLeftSlash) {
  using namespace cta;

  TestingMockMiddleTierAdmin api;
  const std::string s("/VALUE");
  const std::string trimmedString = api.trimSlashes(s);
  ASSERT_EQ(std::string("VALUE"), trimmedString);
}

TEST_F(cta_client_UtilsTest, trimSlashes_twoLeftSlashes) {
  using namespace cta;

  TestingMockMiddleTierAdmin api;
  const std::string s("//VALUE");
  const std::string trimmedString = api.trimSlashes(s);
  ASSERT_EQ(std::string("VALUE"), trimmedString);
}

TEST_F(cta_client_UtilsTest, trimSlashes_oneRightSlash) {
  using namespace cta;

  TestingMockMiddleTierAdmin api;
  const std::string s("VALUE/");
  const std::string trimmedString = api.trimSlashes(s);
  ASSERT_EQ(std::string("VALUE"), trimmedString);
}

TEST_F(cta_client_UtilsTest, trimSlashes_twoRightSlashes) {
  using namespace cta;

  TestingMockMiddleTierAdmin api;
  const std::string s("VALUE//");
  const std::string trimmedString = api.trimSlashes(s);
  ASSERT_EQ(std::string("VALUE"), trimmedString);
}

TEST_F(cta_client_UtilsTest,
  trimSlashes_oneLeftAndOneRightSlash) {
  using namespace cta;

  TestingMockMiddleTierAdmin api;
  const std::string s("/VALUE/");
  const std::string trimmedString = api.trimSlashes(s);
  ASSERT_EQ(std::string("VALUE"), trimmedString);
}

TEST_F(cta_client_UtilsTest,
  trimSlashes_twoLeftAndTwoRightSlashes) {
  using namespace cta;

  TestingMockMiddleTierAdmin api;
  const std::string s("//VALUE//");
  const std::string trimmedString = api.trimSlashes(s);
  ASSERT_EQ(std::string("VALUE"), trimmedString);
}

TEST_F(cta_client_UtilsTest, getEnclosingDirPath_empty_string) {
  using namespace cta;
    
  TestingMockMiddleTierAdmin api;
  const std::string dirPath = "";

  ASSERT_THROW(api.getEnclosingDirPath(dirPath), std::exception);
}

TEST_F(cta_client_UtilsTest, getEnclosingDirPath_root) {
  using namespace cta;
    
  TestingMockMiddleTierAdmin api;
  const std::string dirPath = "/";

  std::string enclosingDirPath;
  ASSERT_THROW(enclosingDirPath = api.getEnclosingDirPath(dirPath),
    std::exception);
}

TEST_F(cta_client_UtilsTest, getEnclosingDirPath_grandparent) {
  using namespace cta;

  TestingMockMiddleTierAdmin api;
  const std::string dirPath = "/grandparent";
    
  std::string enclosingDirPath;
  ASSERT_NO_THROW(enclosingDirPath = api.getEnclosingDirPath(dirPath));
  ASSERT_EQ(std::string("/"), enclosingDirPath);
}

TEST_F(cta_client_UtilsTest,
  getEnclosingDirPath_grandparent_parent) {
  using namespace cta;

  TestingMockMiddleTierAdmin api;
  const std::string dirPath = "/grandparent/parent";

  std::string enclosingDirPath;
  ASSERT_NO_THROW(enclosingDirPath = api.getEnclosingDirPath(dirPath));
  ASSERT_EQ(std::string("/grandparent/"), enclosingDirPath);
}

TEST_F(cta_client_UtilsTest,
  getEnclosingDirPath_grandparent_parent_child) {
  using namespace cta;

  TestingMockMiddleTierAdmin api;
  const std::string dirPath = "/grandparent/parent/child";

  std::string enclosingDirPath;
  ASSERT_NO_THROW(enclosingDirPath = api.getEnclosingDirPath(dirPath));
  ASSERT_EQ(std::string("/grandparent/parent/"), enclosingDirPath);
}

TEST_F(cta_client_UtilsTest, splitString_goodDay) {
  using namespace cta;
  const std::string line("col0 col1 col2 col3 col4 col5 col6 col7");
  std::vector<std::string> columns;

  TestingMockMiddleTierAdmin api;

  ASSERT_NO_THROW(api.splitString(line, ' ', columns));
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

TEST_F(cta_client_UtilsTest, splitString_emptyString) {
  using namespace cta;
  const std::string emptyString;
  std::vector<std::string> columns;

  TestingMockMiddleTierAdmin api;

  ASSERT_NO_THROW(api.splitString(emptyString, ' ', columns));
  ASSERT_EQ((std::vector<std::string>::size_type)0, columns.size());
}

TEST_F(cta_client_UtilsTest, splitString_noSeparatorInString) {
  using namespace cta;
  const std::string stringContainingNoSeparator =
    "stringContainingNoSeparator";
  std::vector<std::string> columns;

  TestingMockMiddleTierAdmin api;

  ASSERT_NO_THROW(api.splitString(stringContainingNoSeparator, ' ', columns));
  ASSERT_EQ((std::vector<std::string>::size_type)1, columns.size());
  ASSERT_EQ(stringContainingNoSeparator, columns[0]);
}

} // namespace unitTests
