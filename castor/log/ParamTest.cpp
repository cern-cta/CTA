/******************************************************************************
 *                test/unittest/castor/log/ParamTest.hpp
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

#include "castor/log/Param.hpp"

#include <gtest/gtest.h>
#include <memory>

namespace unitTests {

class castor_log_ParamTest: public ::testing::Test {
protected:

  void SetUp() {
  }

  void TearDown() {
  }
}; // castor_log_ParamTest

TEST_F(castor_log_ParamTest, testConstructorWithAString) {
  using namespace castor::log;
  std::auto_ptr<Param> param;

  ASSERT_NO_THROW(param.reset(new Param("Name", "Value")));
  ASSERT_EQ(std::string("Name"), param->getName());
  ASSERT_EQ(std::string("Value"), param->getValue());
}

TEST_F(castor_log_ParamTest, testConstructorWithAnInt) {
  using namespace castor::log;
  std::auto_ptr<Param> param;

  ASSERT_NO_THROW(param.reset(new Param("Name", 1234)));
  ASSERT_EQ(std::string("Name"), param->getName());
  ASSERT_EQ(std::string("1234"), param->getValue());
}

} // namespace unitTests
