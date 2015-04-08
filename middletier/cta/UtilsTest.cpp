#include "cta/Utils.hpp"

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

  const std::string s;
  const std::string trimmedString = Utils::trimSlashes(s);
  ASSERT_EQ(s, trimmedString);
}

TEST_F(cta_client_UtilsTest, trimSlashes_noSlashes) {
  using namespace cta;

  const std::string s("NO_SLASHES");
  const std::string trimmedString = Utils::trimSlashes(s);
  ASSERT_EQ(s, trimmedString);
}

TEST_F(cta_client_UtilsTest, trimSlashes_oneLeftSlash) {
  using namespace cta;

  const std::string s("/VALUE");
  const std::string trimmedString = Utils::trimSlashes(s);
  ASSERT_EQ(std::string("VALUE"), trimmedString);
}

TEST_F(cta_client_UtilsTest, trimSlashes_twoLeftSlashes) {
  using namespace cta;

  const std::string s("//VALUE");
  const std::string trimmedString = Utils::trimSlashes(s);
  ASSERT_EQ(std::string("VALUE"), trimmedString);
}

TEST_F(cta_client_UtilsTest, trimSlashes_oneRightSlash) {
  using namespace cta;

  const std::string s("VALUE/");
  const std::string trimmedString = Utils::trimSlashes(s);
  ASSERT_EQ(std::string("VALUE"), trimmedString);
}

TEST_F(cta_client_UtilsTest, trimSlashes_twoRightSlashes) {
  using namespace cta;

  const std::string s("VALUE//");
  const std::string trimmedString = Utils::trimSlashes(s);
  ASSERT_EQ(std::string("VALUE"), trimmedString);
}

TEST_F(cta_client_UtilsTest,
  trimSlashes_oneLeftAndOneRightSlash) {
  using namespace cta;

  const std::string s("/VALUE/");
  const std::string trimmedString = Utils::trimSlashes(s);
  ASSERT_EQ(std::string("VALUE"), trimmedString);
}

TEST_F(cta_client_UtilsTest,
  trimSlashes_twoLeftAndTwoRightSlashes) {
  using namespace cta;

  const std::string s("//VALUE//");
  const std::string trimmedString = Utils::trimSlashes(s);
  ASSERT_EQ(std::string("VALUE"), trimmedString);
}

TEST_F(cta_client_UtilsTest, getEnclosingDirPath_empty_string) {
  using namespace cta;
    
  const std::string dirPath = "";

  ASSERT_THROW(Utils::getEnclosingDirPath(dirPath), std::exception);
}

TEST_F(cta_client_UtilsTest, getEnclosingDirPath_root) {
  using namespace cta;
    
  const std::string dirPath = "/";

  std::string enclosingDirPath;
  ASSERT_THROW(enclosingDirPath = Utils::getEnclosingDirPath(dirPath),
    std::exception);
}

TEST_F(cta_client_UtilsTest, getEnclosingDirPath_grandparent) {
  using namespace cta;

  const std::string dirPath = "/grandparent";
    
  std::string enclosingDirPath;
  ASSERT_NO_THROW(enclosingDirPath = Utils::getEnclosingDirPath(dirPath));
  ASSERT_EQ(std::string("/"), enclosingDirPath);
}

TEST_F(cta_client_UtilsTest,
  getEnclosingDirPath_grandparent_parent) {
  using namespace cta;

  const std::string dirPath = "/grandparent/parent";

  std::string enclosingDirPath;
  ASSERT_NO_THROW(enclosingDirPath = Utils::getEnclosingDirPath(dirPath));
  ASSERT_EQ(std::string("/grandparent/"), enclosingDirPath);
}

TEST_F(cta_client_UtilsTest,
  getEnclosingDirPath_grandparent_parent_child) {
  using namespace cta;

  const std::string dirPath = "/grandparent/parent/child";

  std::string enclosingDirPath;
  ASSERT_NO_THROW(enclosingDirPath = Utils::getEnclosingDirPath(dirPath));
  ASSERT_EQ(std::string("/grandparent/parent/"), enclosingDirPath);
}

TEST_F(cta_client_UtilsTest, getEnclosedName) {
  using namespace cta;

  const std::string enclosingDirPath = "/grandparent/parent/";
  const std::string enclosedName = "child";
  const std::string absoluteFilePath = enclosingDirPath + enclosedName;
  std::string result;
  ASSERT_NO_THROW(result = Utils::getEnclosedName(absoluteFilePath));
  ASSERT_EQ(enclosedName, result);
}

TEST_F(cta_client_UtilsTest, getEnclosedNames) {
  using namespace cta;

  const std::string enclosingDirPath = "/grandparent/parent/";
  const std::string enclosedName1 = "child1";
  const std::string enclosedName2 = "child2";
  const std::string enclosedName3 = "child3";
  const std::string enclosedName4 = "child4";
  std::list<std::string> absoluteFilePaths;
  absoluteFilePaths.push_back(enclosingDirPath + enclosedName1);
  absoluteFilePaths.push_back(enclosingDirPath + enclosedName2);
  absoluteFilePaths.push_back(enclosingDirPath + enclosedName3);
  absoluteFilePaths.push_back(enclosingDirPath + enclosedName4);
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

TEST_F(cta_client_UtilsTest, splitString_goodDay) {
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

TEST_F(cta_client_UtilsTest, splitString_emptyString) {
  using namespace cta;
  const std::string emptyString;
  std::vector<std::string> columns;


  ASSERT_NO_THROW(Utils::splitString(emptyString, ' ', columns));
  ASSERT_EQ((std::vector<std::string>::size_type)0, columns.size());
}

TEST_F(cta_client_UtilsTest, splitString_noSeparatorInString) {
  using namespace cta;
  const std::string stringContainingNoSeparator =
    "stringContainingNoSeparator";
  std::vector<std::string> columns;

  ASSERT_NO_THROW(Utils::splitString(stringContainingNoSeparator, ' ',
    columns));
  ASSERT_EQ((std::vector<std::string>::size_type)1, columns.size());
  ASSERT_EQ(stringContainingNoSeparator, columns[0]);
}

} // namespace unitTests
