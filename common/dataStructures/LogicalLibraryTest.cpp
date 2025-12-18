/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "common/dataStructures/LogicalLibrary.hpp"

#include <algorithm>
#include <gtest/gtest.h>

namespace unitTests {

class cta_common_dataStructures_LogicalLibraryTest : public ::testing::Test {
protected:
  virtual void SetUp() {}

  virtual void TearDown() {}
};

TEST_F(cta_common_dataStructures_LogicalLibraryTest, constructor) {
  using namespace cta::common::dataStructures;

  LogicalLibrary logicalLibrary;
  ASSERT_FALSE(logicalLibrary.isDisabled);
}

}  // namespace unitTests
