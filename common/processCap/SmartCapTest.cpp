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

#include "common/exception/Exception.hpp"
#include "common/processCap/SmartCap.hpp"

#include <gtest/gtest.h>

namespace unitTests {

class cta_server_SmartCapTest : public ::testing::Test {
protected:
  virtual void SetUp() {}

  virtual void TearDown() {}
};

TEST_F(cta_server_SmartCapTest, default_constructor) {
  cta::server::SmartCap smartPtr;
  ASSERT_EQ(nullptr, smartPtr.get());
}

TEST_F(cta_server_SmartCapTest, constructor) {
  cap_t cap = cap_get_proc();
  ASSERT_NE(nullptr, cap);

  cta::server::SmartCap smartPtr(cap);
  ASSERT_EQ(cap, smartPtr.get());
}

TEST_F(cta_server_SmartCapTest, reset) {
  cta::server::SmartCap smartPtr;
  ASSERT_EQ(nullptr, smartPtr.get());

  cap_t cap = cap_get_proc();
  ASSERT_NE(nullptr, cap);

  smartPtr.reset(cap);
  ASSERT_EQ(cap, smartPtr.get());
}

TEST_F(cta_server_SmartCapTest, assignment) {
  cap_t cap = cap_get_proc();
  ASSERT_NE(nullptr, cap);

  cta::server::SmartCap smartPtr1;
  cta::server::SmartCap smartPtr2;

  ASSERT_EQ(nullptr, smartPtr1.get());
  ASSERT_EQ(nullptr, smartPtr2.get());

  smartPtr1.reset(cap);
  ASSERT_EQ(cap, smartPtr1.get());

  smartPtr2 = smartPtr1;
  ASSERT_EQ(nullptr, smartPtr1.get());
  ASSERT_EQ(cap, smartPtr2.get());
}

TEST_F(cta_server_SmartCapTest, releaseNull) {
  cta::server::SmartCap smartPtr;
  ASSERT_THROW(smartPtr.release(), cta::exception::Exception);
}

}  // namespace unitTests
