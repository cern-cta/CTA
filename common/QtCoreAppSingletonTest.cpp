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

#include "common/QtCoreAppSingleton.hpp"

#include <gtest/gtest.h>

namespace unitTests {

class cta_common_QtCoreAppSingletonTest: public ::testing::Test {
protected:

  virtual void SetUp() {
  }

  virtual void TearDown() {
  }
};

TEST_F(cta_common_QtCoreAppSingletonTest, instance) {
  using namespace cta::common;

  ASSERT_NO_THROW(
    QtCoreAppSingleton& app1 = QtCoreAppSingleton::instance();
    QtCoreAppSingleton& app2 = QtCoreAppSingleton::instance();
    ASSERT_EQ(app1, app2););
}

TEST_F(cta_common_QtCoreAppSingletonTest, getApp) {
  using namespace cta::common;

  QtCoreAppSingleton& app = QtCoreAppSingleton::instance();
  ASSERT_NO_THROW(app.getApp());
} // namespace unitTests

} // namespace unitTests
