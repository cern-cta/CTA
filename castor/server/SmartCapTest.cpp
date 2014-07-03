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
 * @author Steven.Murray@cern.ch
 *****************************************************************************/

#include "castor/exception/Exception.hpp"
#include "castor/server/SmartCap.hpp"

#include <gtest/gtest.h>

namespace unitTests {

class castor_server_SmartCapTest : public ::testing::Test {
protected:

  virtual void SetUp() {
  }

  virtual void TearDown() {
  }
};

TEST_F(castor_server_SmartCapTest, default_constructor) {
  castor::server::SmartCap smartPtr;
  ASSERT_EQ((cap_t)NULL, smartPtr.get());
}

TEST_F(castor_server_SmartCapTest, constructor) {
  cap_t cap = cap_get_proc();
  ASSERT_NE((cap_t)NULL, cap);

  castor::server::SmartCap smartPtr(cap);
  ASSERT_EQ(cap, smartPtr.get());
}

TEST_F(castor_server_SmartCapTest, reset) {
  castor::server::SmartCap smartPtr;
  ASSERT_EQ((cap_t)NULL, smartPtr.get());

  cap_t cap = cap_get_proc();
  ASSERT_NE((cap_t)NULL, cap);

  smartPtr.reset(cap);
  ASSERT_EQ(cap, smartPtr.get());
}

TEST_F(castor_server_SmartCapTest, assignment) {
  cap_t cap = cap_get_proc();
  ASSERT_NE((cap_t)NULL, cap);

  castor::server::SmartCap smartPtr1;
  castor::server::SmartCap smartPtr2;

  ASSERT_EQ((cap_t)NULL, smartPtr1.get());
  ASSERT_EQ((cap_t)NULL, smartPtr2.get());

  smartPtr1.reset(cap);
  ASSERT_EQ(cap, smartPtr1.get());

  smartPtr2 = smartPtr1;
  ASSERT_EQ((cap_t)NULL, smartPtr1.get());
  ASSERT_EQ(cap, smartPtr2.get());
}

TEST_F(castor_server_SmartCapTest, releaseNull) {
  castor::server::SmartCap smartPtr;
  ASSERT_THROW(smartPtr.release(), castor::exception::Exception);
}

} // namespace unitTests
