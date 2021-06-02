/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2021 CERN
 * @license        This program is free software: you can redistribute it and/or modify
 *                 it under the terms of the GNU General Public License as published by
 *                 the Free Software Foundation, either version 3 of the License, or
 *                 (at your option) any later version.
 *
 *                 This program is distributed in the hope that it will be useful,
 *                 but WITHOUT ANY WARRANTY; without even the implied warranty of
 *                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *                 GNU General Public License for more details.
 *
 *                 You should have received a copy of the GNU General Public License
 *                 along with this program.  If not, see <http://www.gnu.org/licenses/>.
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
}

TEST_F(cta_log_ParamTest, testConstructorWithAnInt) {
  using namespace cta::log;
  std::unique_ptr<Param> param;

  ASSERT_NO_THROW(param.reset(new Param("Name", 1234)));
  ASSERT_EQ(std::string("Name"), param->getName());
  ASSERT_EQ(std::string("1234"), param->getValue());
}

} // namespace unitTests
