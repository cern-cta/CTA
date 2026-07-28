/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "mediachanger/LibrarySlot.hpp"

#include "common/exception/Exception.hpp"

#include <gtest/gtest.h>
#include <memory>

namespace unitTests {

class cta_mediachanger_LibrarySlotTest : public ::testing::Test {
protected:
  virtual void SetUp() {}

  virtual void TearDown() {}
};

TEST_F(cta_mediachanger_LibrarySlotTest, goodDay) {
  using namespace cta::mediachanger;

  LibrarySlot slot(2);
  ASSERT_EQ(std::string("smc2"), slot.str());
  ASSERT_EQ((uint16_t) 2, slot.getDrvOrd());
  ASSERT_FALSE(slot.isDummy());
}

TEST_F(cta_mediachanger_LibrarySlotTest, dummySlot) {
  using namespace cta::mediachanger;

  LibrarySlot slot(2, true);
  ASSERT_EQ(std::string("smc2"), slot.str());
  ASSERT_EQ((uint16_t) 2, slot.getDrvOrd());
  ASSERT_TRUE(slot.isDummy());
}

}  // namespace unitTests
