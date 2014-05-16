/******************************************************************************
 *         castor/tape/utils/TpconfigLineTest.cpp
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
 *
 * @author Steven.Murray@cern.ch
 *****************************************************************************/

#include "castor/exception/Exception.hpp"
#include "castor/tape/utils/TpconfigLine.hpp"

#include <gtest/gtest.h>

namespace unitTests {

class castor_tape_utils_TpconfigLineTest : public ::testing::Test {
protected:

  virtual void SetUp() {
  }

  virtual void TearDown() {
  }
};

TEST_F( castor_tape_utils_TpconfigLineTest, convertDownLowerCase) {
  using namespace castor::tape::utils;

  TpconfigLine::InitialState state = TpconfigLine::TPCONFIG_DRIVE_NONE;

  ASSERT_NO_THROW(state = TpconfigLine::str2InitialState("down"));
  ASSERT_EQ(TpconfigLine::TPCONFIG_DRIVE_DOWN, state);
}

TEST_F( castor_tape_utils_TpconfigLineTest, convertDownUpperCase) {
  using namespace castor::tape::utils;

  TpconfigLine::InitialState state = TpconfigLine::TPCONFIG_DRIVE_NONE;

  ASSERT_NO_THROW(state = TpconfigLine::str2InitialState("DOWN"));
  ASSERT_EQ(TpconfigLine::TPCONFIG_DRIVE_DOWN, state);
}

TEST_F( castor_tape_utils_TpconfigLineTest, convertDownMixedCase) {
  using namespace castor::tape::utils;

  TpconfigLine::InitialState state = TpconfigLine::TPCONFIG_DRIVE_NONE;

  ASSERT_NO_THROW(state = TpconfigLine::str2InitialState("Down"));
  ASSERT_EQ(TpconfigLine::TPCONFIG_DRIVE_DOWN, state);
}

TEST_F( castor_tape_utils_TpconfigLineTest, convertUpLowerCase) {
  using namespace castor::tape::utils;

  TpconfigLine::InitialState state = TpconfigLine::TPCONFIG_DRIVE_NONE;

  ASSERT_NO_THROW(state = TpconfigLine::str2InitialState("up"));
  ASSERT_EQ(TpconfigLine::TPCONFIG_DRIVE_UP, state);
}

TEST_F( castor_tape_utils_TpconfigLineTest, convertUpUpperCase) {
  using namespace castor::tape::utils;

  TpconfigLine::InitialState state = TpconfigLine::TPCONFIG_DRIVE_NONE;

  ASSERT_NO_THROW(state = TpconfigLine::str2InitialState("UP"));
  ASSERT_EQ(TpconfigLine::TPCONFIG_DRIVE_UP, state);
}

TEST_F( castor_tape_utils_TpconfigLineTest, convertUpMixedCase) {
  using namespace castor::tape::utils;

  TpconfigLine::InitialState state = TpconfigLine::TPCONFIG_DRIVE_NONE;

  ASSERT_NO_THROW(state = TpconfigLine::str2InitialState("Up"));
  ASSERT_EQ(TpconfigLine::TPCONFIG_DRIVE_UP, state);
}

TEST_F( castor_tape_utils_TpconfigLineTest, convertNonsense) {
  using namespace castor::tape::utils;

  ASSERT_THROW(TpconfigLine::str2InitialState("Nonsense"),
    castor::exception::Exception);
}

} // namespace unitTests
