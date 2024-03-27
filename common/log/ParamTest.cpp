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

#include "common/log/Param.hpp"

#include <gtest/gtest.h>
#include <memory>

namespace unitTests {

class cta_log_ParamTest: public ::testing::Test {
protected:

  void SetUp() {
  }

  void TearDown() {
  }
}; // cta_log_ParamTest

TEST_F(cta_log_ParamTest, testConstructorWithAString) {
  using namespace cta::log;
  std::unique_ptr<Param> param;

  ASSERT_NO_THROW(param.reset(new Param("Name", "Value")));
  ASSERT_EQ(std::string("Name"), param->getName());
  ASSERT_EQ(std::string("Value"), param->getValue());
  ASSERT_EQ(std::string("\"Name\":\"Value\""), param->getKeyValueJSON());
}

TEST_F(cta_log_ParamTest, testConstructorWithAStringNoName) {
  using namespace cta::log;
  std::unique_ptr<Param> param;

  ASSERT_NO_THROW(param.reset(new Param("", "Value")));
  ASSERT_EQ(std::string(""), param->getName());
  ASSERT_EQ(std::string("Value"), param->getValue());
  ASSERT_EQ(std::string("\"\":\"Value\""), param->getKeyValueJSON());
}

TEST_F(cta_log_ParamTest, testConstructorWithAnInt) {
  using namespace cta::log;
  std::unique_ptr<Param> param;

  ASSERT_NO_THROW(param.reset(new Param("Name", 1234)));
  ASSERT_EQ(std::string("Name"), param->getName());
  ASSERT_EQ(std::string("1234"), param->getValue());
  ASSERT_EQ(std::string("\"Name\":1234"), param->getKeyValueJSON());
}

TEST_F(cta_log_ParamTest, testConstructorWithAFloat) {
  using namespace cta::log;
  std::unique_ptr<Param> param;

  ASSERT_NO_THROW(param.reset(new Param("Name", 1234.12f)));
  ASSERT_EQ(std::string("Name"), param->getName());
  ASSERT_EQ(std::string("1234.12"), param->getValue());
  ASSERT_EQ(std::string("\"Name\":1234.12"), param->getKeyValueJSON());
}

TEST_F(cta_log_ParamTest, testConstructorWithADouble1) {
  using namespace cta::log;
  std::unique_ptr<Param> param;

  ASSERT_NO_THROW(param.reset(new Param("Name", 1234.123456789)));
  ASSERT_EQ(std::string("Name"), param->getName());
  ASSERT_EQ(std::string("1234.123456789"), param->getValue());
  ASSERT_EQ(std::string("\"Name\":1234.123456789"), param->getKeyValueJSON());
}

TEST_F(cta_log_ParamTest, testConstructorWithADouble2) {
  using namespace cta::log;
  std::unique_ptr<Param> param;

  ASSERT_NO_THROW(param.reset(new Param("Name", 1234.0000)));
  ASSERT_EQ(std::string("Name"), param->getName());
  ASSERT_EQ(std::string("1234.0"), param->getValue());
  ASSERT_EQ(std::string("\"Name\":1234.0"), param->getKeyValueJSON());
}

TEST_F(cta_log_ParamTest, testConstructorWithADouble3) {
  using namespace cta::log;
  std::unique_ptr<Param> param;

  ASSERT_NO_THROW(param.reset(new Param("Name", 0.0000)));
  ASSERT_EQ(std::string("Name"), param->getName());
  ASSERT_EQ(std::string("0.0"), param->getValue());
  ASSERT_EQ(std::string("\"Name\":0.0"), param->getKeyValueJSON());
}

TEST_F(cta_log_ParamTest, testConstructorWithADouble4) {
  using namespace cta::log;
  std::unique_ptr<Param> param;

  ASSERT_NO_THROW(param.reset(new Param("Name", 1.234e-20)));
  ASSERT_EQ(std::string("Name"), param->getName());
  ASSERT_EQ(std::string("1.234e-20"), param->getValue());
  ASSERT_EQ(std::string("\"Name\":1.234e-20"), param->getKeyValueJSON());
}


TEST_F(cta_log_ParamTest, testConstructorWithADouble5) {
  using namespace cta::log;
  std::unique_ptr<Param> param;

  ASSERT_NO_THROW(param.reset(new Param("Name", 1e+30)));
  ASSERT_EQ(std::string("Name"), param->getName());
  ASSERT_EQ(std::string("1e+30"), param->getValue());
  ASSERT_EQ(std::string("\"Name\":1e+30"), param->getKeyValueJSON());
}

} // namespace unitTests
