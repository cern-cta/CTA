/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "mediachanger/LibrarySlotParser.hpp"

#include "common/exception/Exception.hpp"

#include <gtest/gtest.h>
#include <memory>

namespace unitTests {

class cta_mediachanger_LibrarySlotParserTest : public ::testing::Test {
protected:
  virtual void SetUp() {}

  virtual void TearDown() {}
};

TEST_F(cta_mediachanger_LibrarySlotParserTest, dummy) {
  using namespace cta::mediachanger;

  LibrarySlot slot = LibrarySlotParser::parse("dummy");
  ASSERT_TRUE(slot.isDummy());
}

TEST_F(cta_mediachanger_LibrarySlotParserTest, scsi) {
  using namespace cta::mediachanger;

  LibrarySlot slot = LibrarySlotParser::parse("smc1");
  ASSERT_EQ(slot.getDrvOrd(), 1);
  ASSERT_EQ(slot.str(), "smc1");
  ASSERT_FALSE(slot.isDummy());
}

TEST_F(cta_mediachanger_LibrarySlotParserTest, nonsense) {
  using namespace cta::mediachanger;

  ASSERT_THROW(LibrarySlotParser::parse("nonsense"), cta::exception::Exception);
}

}  // namespace unitTests
